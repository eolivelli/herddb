/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.indexing.vector;

import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory for bulk sequential {@link ReaderSupplier} instances used during
 * PQ retraining in streaming compaction (issue #599 Option B).
 *
 * <p>When {@link io.github.jbellis.jvector.graph.disk.PQRetrainer} calls
 * {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex#getView} on each
 * source segment, it normally goes through the segment's default
 * {@link ReaderSupplier} — which in production is backed by the
 * {@code SegmentBlockCache} / gRPC file-server stack. For N source segments and
 * ~4 096 sampled nodes each, this serialises ~53 248 16 KiB block-cache round-trips.
 *
 * <p>This class returns a {@link Function}{@code <OnDiskGraphIndex, ReaderSupplier>}
 * that, given a source graph, either:
 * <ul>
 *   <li><b>downloads the graph file once</b> to a temp file in
 *       {@link PersistentVectorStore#tmpDirectory()} (= the IS data directory) via
 *       {@link DataStorageManager#downloadMultipartIndexFile} when the storage manager
 *       supports a direct object-storage path
 *       ({@link DataStorageManager#supportsDirectMultipartDownload()} is {@code true});
 *       or</li>
 *   <li>opens a <b>sequential reader</b> via
 *       {@link DataStorageManager#multipartIndexReaderSupplier} (the existing
 *       file-server / in-memory path) when direct download is not available.</li>
 * </ul>
 * Either way, {@link io.github.jbellis.jvector.graph.disk.PQRetrainer} reads all
 * sampled vectors from the local file / in-memory buffer rather than issuing one
 * gRPC round-trip per sampled node.
 *
 * <p>Temp files created for the direct-download path are wrapped in a
 * {@link DeleteOnCloseReaderSupplier} so they are deleted as soon as the
 * {@code PQRetrainer} closes the supplier after extracting that source's vectors.
 */
final class SegmentPQReaderSupplier {

    private static final Logger LOGGER = Logger.getLogger(SegmentPQReaderSupplier.class.getName());

    private SegmentPQReaderSupplier() {
    }

    /**
     * Builds a reader-supplier factory for all source segments in a streaming
     * compaction. The factory maps each {@link OnDiskGraphIndex} to a
     * {@link ReaderSupplier} backed by a local file download or DSM sequential
     * reader, enabling bulk sequential reads during PQ retraining.
     *
     * <p>The factory is safe to call from multiple threads concurrently (each
     * invocation creates its own independent {@link ReaderSupplier} and, for the
     * direct-download path, its own temp file).
     *
     * @param store      the store that owns the segments; provides the storage
     *                   manager, tablespace UUID, and download directory
     * @param candidates the compaction input segments, positionally aligned with
     *                   {@code sources}
     * @param sources    the corresponding {@link OnDiskGraphIndex} objects, in the
     *                   same order as {@code candidates}
     * @return a factory that maps each {@link OnDiskGraphIndex} to a
     *         {@link ReaderSupplier} for its graph file; never {@code null}
     */
    static Function<OnDiskGraphIndex, ReaderSupplier> forSegments(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            List<OnDiskGraphIndex> sources) {

        DataStorageManager dsm = store.dataStorageManager();
        String tsUUID = store.tableSpaceUUID();
        Path downloadDir = store.tmpDirectory();

        // Build an identity-keyed map so that we can resolve VectorSegment from
        // the OnDiskGraphIndex reference without relying on equals/hashCode
        // (OnDiskGraphIndex does not override them).
        IdentityHashMap<OnDiskGraphIndex, VectorSegment> odgToSeg = new IdentityHashMap<>();
        for (int i = 0; i < sources.size(); i++) {
            odgToSeg.put(sources.get(i), candidates.get(i));
        }

        return odg -> {
            VectorSegment seg = odgToSeg.get(odg);
            if (seg == null) {
                throw new IllegalStateException(
                        "SegmentPQReaderSupplier: OnDiskGraphIndex not found in candidate set");
            }
            String segUuid = store.segmentStorageKey(seg);
            long graphFileSize = seg.graphFileSize;

            if (dsm.supportsDirectMultipartDownload()) {
                // Fast path: download directly from object storage (S3/GCS/MinIO),
                // bypassing the gRPC file-server. The temp file lives in the IS data
                // directory (store.tmpDirectory()) — the same location as all other
                // compaction scratch files (e.g. "herddb-vector-compact-graph-*.idx").
                Path tempFile = null;
                boolean success = false;
                try {
                    tempFile = Files.createTempFile(downloadDir, "herddb-pq-seg-", ".idx");
                    dsm.downloadMultipartIndexFile(tsUUID, segUuid, "graph", graphFileSize, tempFile);
                    ReaderSupplier mmap = ReaderSupplierFactory.open(tempFile);
                    success = true;
                    return new DeleteOnCloseReaderSupplier(mmap, tempFile);
                } catch (IOException e) {
                    deleteSilently(tempFile);
                    throw new UncheckedIOException(
                            "SegmentPQReaderSupplier: failed to download segment graph for PQ retraining"
                                    + " (tsUUID=" + tsUUID + ", segUuid=" + segUuid + ")", e);
                } catch (DataStorageManagerException e) {
                    deleteSilently(tempFile);
                    throw new RuntimeException(
                            "SegmentPQReaderSupplier: storage error while downloading segment graph for PQ retraining"
                                    + " (tsUUID=" + tsUUID + ", segUuid=" + segUuid + ")", e);
                } finally {
                    if (!success) {
                        deleteSilently(tempFile);
                    }
                }
            } else {
                // Fallback: sequential reader from the file server (or in-memory for
                // MemoryDataStorageManager). Still avoids per-node block-cache reads
                // because the whole file is served by a single ReaderSupplier whose
                // get() returns a reader over the complete file content.
                try {
                    return dsm.multipartIndexReaderSupplier(tsUUID, segUuid, "graph", graphFileSize);
                } catch (DataStorageManagerException e) {
                    throw new RuntimeException(
                            "SegmentPQReaderSupplier: failed to open multipart reader for PQ retraining"
                                    + " (tsUUID=" + tsUUID + ", segUuid=" + segUuid + ")", e);
                }
            }
        };
    }

    private static void deleteSilently(Path tempFile) {
        if (tempFile == null) {
            return;
        }
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete temp PQ segment file {0}", tempFile);
        }
    }
}
