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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage for the streaming merge path in
 * {@link RemoteSegmentGraphMerger}, the optimizer-pod side of issue #485.
 *
 * <p>Unlike {@code RemoteSegmentMergerTest}, this suite produces real
 * graph + map multipart files via {@link PersistentVectorStore} (so each
 * input has the full feature set the streaming compactor expects) and
 * exercises the dispatcher → {@code mergeStreaming} path with the flag in
 * its production-default state ({@code true}).
 *
 * <p>Asserted invariants:
 * <ul>
 *   <li>{@code mergeStreaming} produces a graph + map output addressable
 *       on the DSM.</li>
 *   <li>The merged graph re-loads via
 *       {@link OnDiskGraphIndex#load(ReaderSupplier)} and carries
 *       {@link FeatureId#INLINE_VECTORS}.</li>
 *   <li>The merged map file's PK set equals the union of inputs' surviving
 *       PKs (after tombstone + duplicate filtering).</li>
 *   <li>The flag-off legacy path on the same fixtures produces the same
 *       observable PK set (parity check).</li>
 *   <li>The new {@code graphFileSize} field is correctly derived as
 *       {@code sizeBytes - mapFileSize}.</li>
 * </ul>
 */
public class RemoteSegmentGraphMergerStreamingTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        // Streaming-compaction tests need the in-memory checkpoint deferral
        // gate disabled so small per-checkpoint shards are flushed.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds {@code numSegments} on-disk segments via a real
     * {@link PersistentVectorStore}, then snapshots each segment's
     * {@code (segUuid, segId, graphFileSize, mapFileSize, generation)}
     * tuple into a {@link RemoteSegmentGraphMerger.RemoteSegmentInput}
     * suitable for the merger.
     */
    private List<RemoteSegmentGraphMerger.RemoteSegmentInput> buildInputs(
            Path tmpDir, MemoryDataStorageManager dsm, int dim,
            int numSegments, int perSegment, long seed,
            List<Bytes> insertedPks) throws Exception {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "ts-streaming-merger", "tbl", "tsuuid", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {
            store.start();
            Random rng = new Random(seed);
            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < perSegment; i++) {
                    Bytes pk = Bytes.from_int(c * 10_000 + i);
                    insertedPks.add(pk);
                    store.addVector(pk, vec(rng, dim));
                }
                store.checkpoint();
            }
            assertEquals(numSegments, store.getSegmentCount());

            List<VectorSegment> segments = store.getOnDiskSegmentsSnapshotForTest();
            List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs =
                    new ArrayList<>(segments.size());
            String tableSpaceUuid = "tsuuid";
            String indexUuid = store.indexUUID();
            for (VectorSegment seg : segments) {
                inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        tableSpaceUuid, indexUuid, seg.segmentUuid != null ? seg.segmentUuid
                                : indexUuid + "_seg" + seg.segmentId,
                        seg.segmentId,
                        /* mapFileSize  */ seg.mapFileSize,
                        /* graphFileSize */ seg.graphFileSize,
                        /* generation  */ seg.generation,
                        /* tombstones  */ new int[0]));
            }
            return inputs;
        }
    }

    /**
     * Walks the produced map file and returns the union of PKs. The streaming
     * output must use the same wire format as the input — a sanity check
     * that the {@code (newOrd, pkLen, pk, dim, floats)} header layout is
     * compatible.
     */
    private Set<Bytes> readPksFromMapMultipart(MemoryDataStorageManager dsm,
                                               String tablespaceUuid,
                                               String multipartUuid,
                                               long mapFileSize,
                                               int expectedDim) throws Exception {
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                tablespaceUuid, multipartUuid, "map", mapFileSize);
        Path tempFile = Files.createTempFile(tmpFolder.newFolder().toPath(),
                "merger-out-", ".tmp");
        try (var reader = supplier.get();
             var fos = new java.io.FileOutputStream(tempFile.toFile());
             var bos = new java.io.BufferedOutputStream(fos, 65536)) {
            reader.seek(0L);
            byte[] buf = new byte[65536];
            long remaining = mapFileSize;
            while (remaining > 0L) {
                int toRead = (int) Math.min(buf.length, remaining);
                byte[] chunk = (toRead == buf.length) ? buf : new byte[toRead];
                reader.readFully(chunk);
                bos.write(chunk, 0, toRead);
                remaining -= toRead;
            }
        }
        Set<Bytes> pks = new HashSet<>();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(
                new FileInputStream(tempFile.toFile()), 65536))) {
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                dis.readInt(); // newOrdinal
                int pkLen = dis.readInt();
                byte[] pkRaw = new byte[pkLen];
                dis.readFully(pkRaw);
                int floatCount = dis.readInt();
                assertEquals("dim must match", expectedDim, floatCount);
                long skipBytes = (long) floatCount * Float.BYTES;
                long skipped = 0L;
                while (skipped < skipBytes) {
                    long s = dis.skip(skipBytes - skipped);
                    if (s <= 0) {
                        if (dis.read() < 0) {
                            throw new java.io.EOFException();
                        }
                        skipped++;
                    } else {
                        skipped += s;
                    }
                }
                pks.add(Bytes.from_array(pkRaw));
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
        return pks;
    }

    @Test
    public void streamingMergeProducesValidOnDiskGraphAndMatchingPkSet() throws Exception {
        int dim = 16;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        List<Bytes> insertedPks = new ArrayList<>();
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs =
                buildInputs(tmpDir, dsm, dim, /*numSegments*/ 3, /*perSegment*/ 200,
                        /*seed*/ 5L, insertedPks);
        // Sanity: every input has a real graphFileSize derived from the actual
        // multipart upload (the streaming dispatcher requires this).
        for (RemoteSegmentGraphMerger.RemoteSegmentInput in : inputs) {
            assertTrue("graphFileSize must be > 0 for streaming dispatch: "
                            + in.segmentUuid + " has " + in.graphFileSize,
                    in.graphFileSize > 0L);
        }

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, /* graphM */ 16, /* beamWidth */ 100,
                /* neighborOverflow */ 1.2f, /* alpha */ 1.4f,
                VectorSimilarityFunction.COSINE);

        long outSegmentId = 12345L;
        String outTablespaceUuid = "tsuuid";
        String outIndexUuid = inputs.get(0).indexUuid;
        RemoteSegmentGraphMerger.MergeOutput out = merger.merge(
                inputs, outTablespaceUuid, outIndexUuid, outSegmentId, dim);

        // (a) Output exists and is non-trivial.
        assertNotNull("merge must produce output for healthy inputs", out);
        assertTrue("merged graph must be non-empty", out.graphFileSize > 0L);
        assertTrue("merged map must be non-empty", out.mapFileSize > 0L);
        assertEquals("vectorCount = sum of input live vectors (no tombstones)",
                3L * 200L, out.vectorCount);
        assertEquals("no tombstones dropped", 0L, out.droppedTombstones);
        assertEquals("no duplicates dropped (PKs unique across inputs)",
                0L, out.droppedDuplicates);

        // (b) Merged graph re-loads via OnDiskGraphIndex.load and carries INLINE_VECTORS.
        String mergedMultipartUuid = outIndexUuid + "_seg" + outSegmentId;
        ReaderSupplier graphSupplier = dsm.multipartIndexReaderSupplier(
                outTablespaceUuid, mergedMultipartUuid, "graph", out.graphFileSize);
        try (graphSupplier) {
            OnDiskGraphIndex merged = OnDiskGraphIndex.load(graphSupplier);
            try {
                assertTrue("merged graph must carry INLINE_VECTORS",
                        merged.getFeatures().containsKey(FeatureId.INLINE_VECTORS));
                assertEquals("merged graph dimension must equal input dim",
                        dim, merged.getDimension());
            } finally {
                merged.close();
            }
        }

        // (c) Merged map's PK set equals union of inputs' PKs.
        Set<Bytes> mergedPks = readPksFromMapMultipart(dsm, outTablespaceUuid,
                mergedMultipartUuid, out.mapFileSize, dim);
        assertEquals("merged map PK count must equal vectorCount",
                out.vectorCount, mergedPks.size());
        assertEquals("merged map PK set must equal union of input PKs",
                new HashSet<>(insertedPks), mergedPks);
    }

    @Test
    public void streamingDispatchFallsBackToLegacyOnZeroGraphFileSize() throws Exception {
        int dim = 16;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        List<Bytes> insertedPks = new ArrayList<>();
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> realInputs =
                buildInputs(tmpDir, dsm, dim, 2, 100, 13L, insertedPks);

        // Synthesise a list with one input's graphFileSize zeroed: the dispatcher
        // must NOT call mergeStreaming (which would try to download the graph).
        // Use the legacy 7-arg constructor (graphFileSize defaults to 0).
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = new ArrayList<>();
        for (RemoteSegmentGraphMerger.RemoteSegmentInput real : realInputs) {
            inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    real.tablespaceUuid, real.indexUuid, real.segmentUuid,
                    real.segmentId, real.mapFileSize,
                    /* generation */ real.generation,
                    /* tombstonedOrdinals */ new int[0]));
        }
        for (RemoteSegmentGraphMerger.RemoteSegmentInput in : inputs) {
            assertEquals("synthesised inputs must have graphFileSize=0",
                    0L, in.graphFileSize);
        }

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 16, 100, 1.2f, 1.4f, VectorSimilarityFunction.COSINE);

        // Falling back to legacy: the merge should still succeed (the legacy
        // path doesn't need graphFileSize). Output is a fresh segment with
        // the union PK set.
        RemoteSegmentGraphMerger.MergeOutput out = merger.merge(
                inputs, "tsuuid", realInputs.get(0).indexUuid, 9999L, dim);
        assertNotNull("legacy fallback must produce output", out);
        Set<Bytes> mergedPks = readPksFromMapMultipart(dsm, "tsuuid",
                realInputs.get(0).indexUuid + "_seg" + 9999L, out.mapFileSize, dim);
        assertEquals(new HashSet<>(insertedPks), mergedPks);
    }

    @Test
    public void emptyAuthorityReturnsNullOnStreamingPath() throws Exception {
        // Build 2 segments + tombstone every PK to force authority.isEmpty() == true.
        int dim = 16;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        List<Bytes> insertedPks = new ArrayList<>();
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> realInputs =
                buildInputs(tmpDir, dsm, dim, 2, 50, 21L, insertedPks);

        // Re-emit each input with EVERY ordinal tombstoned.
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> tombstonedAll = new ArrayList<>();
        for (RemoteSegmentGraphMerger.RemoteSegmentInput in : realInputs) {
            int[] allOrds = new int[50];
            for (int i = 0; i < 50; i++) {
                allOrds[i] = i;
            }
            tombstonedAll.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    in.tablespaceUuid, in.indexUuid, in.segmentUuid,
                    in.segmentId, in.mapFileSize, in.graphFileSize,
                    in.generation, allOrds));
        }

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 16, 100, 1.2f, 1.4f, VectorSimilarityFunction.COSINE);
        RemoteSegmentGraphMerger.MergeOutput out = merger.merge(
                tombstonedAll, "tsuuid", realInputs.get(0).indexUuid, 8888L, dim);
        assertNull("merge must decline when every input vector is tombstoned", out);
    }
}
