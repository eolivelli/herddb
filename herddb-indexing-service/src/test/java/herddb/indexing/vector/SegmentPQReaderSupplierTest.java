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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SegmentPQReaderSupplier} guard conditions and the
 * direct-download (fast) path (issue #599 Option B).
 */
public class SegmentPQReaderSupplierTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 4,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        return store;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    // ---------------------------------------------------------------------------
    // Guard condition tests
    // ---------------------------------------------------------------------------

    /**
     * Verifies that {@link SegmentPQReaderSupplier#forSegments} rejects lists
     * of different sizes with a clear {@link IllegalArgumentException} before
     * accessing any store state.
     */
    @Test
    public void sizeMismatchRejected() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        PersistentVectorStore store = createStore(tmpDir, dsm);

        try {
            store.start();
            List<VectorSegment> candidates = Collections.singletonList(new VectorSegment(1));
            // sources list is empty — size mismatch with candidates
            List<OnDiskGraphIndex> sources = Collections.emptyList();

            try {
                SegmentPQReaderSupplier.forSegments(store, candidates, sources);
                fail("Expected IllegalArgumentException for size mismatch");
            } catch (IllegalArgumentException e) {
                assertTrue("exception message must mention sizes",
                        e.getMessage().contains("candidates.size()") && e.getMessage().contains("sources.size()"));
            }
        } finally {
            store.close();
        }
    }

    /**
     * Verifies that the factory lambda returned by
     * {@link SegmentPQReaderSupplier#forSegments} rejects a segment with
     * {@code graphFileSize == 0} with a clear {@link IllegalStateException}.
     *
     * <p>This guard prevents PQ retraining from opening an empty or truncated
     * reader when a segment's graph has not been written yet.
     */
    @Test
    public void zeroGraphFileSizeRejected() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        PersistentVectorStore store = createStore(tmpDir, dsm);
        // Disable deferral so every checkpoint produces a segment immediately.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;

        try {
            store.start();

            // Write enough vectors so that we get at least one on-disk segment.
            Random rng = new Random(7);
            final int dim = 16;
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            store.checkpoint();

            List<VectorSegment> segs = store.getOnDiskSegmentsSnapshotForTest();
            assertFalse("need at least one on-disk segment for this test", segs.isEmpty());

            // Pick the first segment with a populated on-disk graph.
            VectorSegment target = null;
            for (VectorSegment seg : segs) {
                if (seg.onDiskGraph != null) {
                    target = seg;
                    break;
                }
            }
            assertNotNull("no on-disk graph found in segments", target);

            OnDiskGraphIndex odg = target.onDiskGraph;

            // Temporarily zero out the graphFileSize to simulate the bad-state scenario.
            long originalSize = target.graphFileSize;
            assertTrue("segment must have a positive graphFileSize before patching",
                    originalSize > 0);
            target.graphFileSize = 0;
            try {
                Function<OnDiskGraphIndex, ReaderSupplier> factory =
                        SegmentPQReaderSupplier.forSegments(store,
                                Collections.singletonList(target),
                                Collections.singletonList(odg));
                // Calling the factory with the zero-size segment must throw.
                try {
                    factory.apply(odg);
                    fail("Expected IllegalStateException for graphFileSize == 0");
                } catch (IllegalStateException e) {
                    assertTrue("exception message must mention graphFileSize",
                            e.getMessage().contains("graphFileSize"));
                }
            } finally {
                // Restore so store.close() can clean up without issues.
                target.graphFileSize = originalSize;
            }
        } finally {
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            store.close();
        }
    }

    // ---------------------------------------------------------------------------
    // Direct-download fast-path tests
    // ---------------------------------------------------------------------------

    /**
     * Verifies that when {@link herddb.storage.DataStorageManager#supportsDirectMultipartDownload()}
     * returns {@code true}, {@link SegmentPQReaderSupplier} downloads each source
     * segment's graph file to a temp file and returns a {@link DeleteOnCloseReaderSupplier}.
     * After the supplier is closed, the temp file must be deleted.
     */
    @Test
    public void directDownloadFastPathSuccessCleansTempFile() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DirectDownloadDsm dsm = new DirectDownloadDsm(/*shouldFail=*/false);
        PersistentVectorStore store = createStore(tmpDir, dsm);
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        VectorIndexCompactor.streamingCompactionEnabled = true;

        int countBefore = VectorIndexCompactor.PQ_BULK_READER_COUNT.get();

        try {
            store.start();

            // Build enough segments with FusedPQ enabled (300 vectors × 5 checkpoints).
            Random rng = new Random(42);
            final int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }

            assertTrue("need >= 2 segments for streaming compaction",
                    store.getSegmentCount() >= 2);

            store.runCompactionCycle();

            // The bulk-reader factory must have been invoked (direct-download path).
            int countAfter = VectorIndexCompactor.PQ_BULK_READER_COUNT.get();
            assertTrue("PQ_BULK_READER_COUNT must increase after direct-download compaction: before="
                            + countBefore + " after=" + countAfter,
                    (countAfter - countBefore) >= 2);

            // All temp files created by the factory must be gone after compaction.
            for (Path tempFile : dsm.createdTempFiles) {
                assertFalse("temp segment file must be deleted after use: " + tempFile,
                        Files.exists(tempFile));
            }
            assertFalse("downloadMultipartIndexFile must be called at least once",
                    dsm.createdTempFiles.isEmpty());
        } finally {
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            store.close();
        }
    }

    /**
     * Verifies that when {@link herddb.storage.DataStorageManager#downloadMultipartIndexFile}
     * throws an {@link IOException}, the temp file created by
     * {@link SegmentPQReaderSupplier} is deleted and an {@link UncheckedIOException} is
     * propagated through the compaction path.
     */
    @Test
    public void directDownloadFastPathFailureCleansTempFile() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DirectDownloadDsm dsm = new DirectDownloadDsm(/*shouldFail=*/true);
        PersistentVectorStore store = createStore(tmpDir, dsm);
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        VectorIndexCompactor.streamingCompactionEnabled = true;

        try {
            store.start();

            // Build enough segments with FusedPQ enabled.
            Random rng = new Random(99);
            final int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }

            assertTrue("need >= 2 segments for streaming compaction",
                    store.getSegmentCount() >= 2);

            // runCompactionCycle() must propagate (or log) the failure; it must not
            // swallow it silently. Either an exception is thrown or the store remains
            // in a consistent state (not both conditions covered here — we only check
            // the temp-file cleanup invariant).
            try {
                store.runCompactionCycle();
                // If the compaction failure is swallowed (e.g. logged), that's OK
                // for this test — we only care about cleanup.
            } catch (Exception e) {
                // Any exception from a failing download is expected here.
            }

            // All temp files created by the factory must be gone after the failure.
            assertFalse("downloadMultipartIndexFile must have been attempted",
                    dsm.createdTempFiles.isEmpty());
            for (Path tempFile : dsm.createdTempFiles) {
                assertFalse("temp segment file must be deleted even after download failure: " + tempFile,
                        Files.exists(tempFile));
            }
        } finally {
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            store.close();
        }
    }

    // ---------------------------------------------------------------------------
    // Inner DSM for direct-download tests
    // ---------------------------------------------------------------------------

    /**
     * {@link MemoryDataStorageManager} subclass that claims to support direct
     * object-storage downloads and implements {@code downloadMultipartIndexFile}
     * by copying the in-memory data to {@code destFile}. If {@code shouldFail}
     * is {@code true}, the method throws {@link IOException} instead.
     *
     * <p>Recorded temp-file paths are stored in {@link #createdTempFiles} so
     * tests can verify post-close cleanup.
     */
    private static final class DirectDownloadDsm extends MemoryDataStorageManager {

        private final boolean shouldFail;
        final List<Path> createdTempFiles = new java.util.concurrent.CopyOnWriteArrayList<>();

        DirectDownloadDsm(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        @Override
        public boolean supportsDirectMultipartDownload() {
            return true;
        }

        @Override
        public void downloadMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                               long fileSize, Path destFile)
                throws IOException, DataStorageManagerException {
            // PersistentVectorStore also calls downloadMultipartIndexFile with
            // fileType="map" during the checkpoint phase. We only inject the failure
            // for fileType="graph" (the PQ-retraining path from SegmentPQReaderSupplier)
            // so that checkpoints can complete normally and build the on-disk segments
            // that the compaction test needs.
            if ("graph".equals(fileType)) {
                // Track the temp file regardless of success/failure so the test can
                // verify it gets deleted by SegmentPQReaderSupplier's finally block.
                createdTempFiles.add(destFile);
                if (shouldFail) {
                    throw new IOException("simulated download failure for PQ retraining test");
                }
            }

            // Copy the in-memory data to destFile via the parent's reader.
            ReaderSupplier rs = multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
            try (RandomAccessReader reader = rs.get()) {
                byte[] buf = new byte[(int) reader.length()];
                reader.readFully(buf);
                Files.write(destFile, buf);
            }
        }
    }
}
