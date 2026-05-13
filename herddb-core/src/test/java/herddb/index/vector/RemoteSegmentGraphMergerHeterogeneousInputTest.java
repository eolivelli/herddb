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
package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteSegmentGraphMerger} handles heterogeneous jvector
 * feature sets (issue #543: "Each source must have the same features") by
 * automatically falling back to the legacy in-memory rebuild path instead of
 * throwing an exception.
 *
 * <p>Test scenario:
 * <ol>
 *   <li>Build one segment with ≥ {@link PersistentVectorStore#MIN_VECTORS_FOR_FUSED_PQ}
 *       vectors → written with {@code FusedPQ + InlineVectors}.</li>
 *   <li>Build another segment with {@code < MIN_VECTORS_FOR_FUSED_PQ} vectors
 *       → written with {@code InlineVectors} only.</li>
 *   <li>Feed both as inputs to the merger (heterogeneous feature sets).</li>
 *   <li>Verify the merge completes without exception and produces a valid
 *       output with the correct PK count.</li>
 * </ol>
 *
 * <p>Additionally verifies that the threshold constant in
 * {@link RemoteSegmentGraphMerger} matches {@link PersistentVectorStore}
 * so the feature-set mismatch cannot re-appear from a constant skew alone.
 */
public class RemoteSegmentGraphMergerHeterogeneousInputTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private boolean savedStreamingFlag;

    @Before
    public void setUp() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        savedStreamingFlag = VectorIndexCompactor.streamingCompactionEnabled;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
        VectorIndexCompactor.streamingCompactionEnabled = savedStreamingFlag;
    }

    /**
     * The threshold constants in the two classes must be identical — a mismatch
     * is the root cause of issue #543.
     */
    @Test
    public void thresholdConstantsMatch() {
        assertEquals(
                "RemoteSegmentGraphMerger.MIN_VECTORS_FOR_FUSED_PQ must equal"
                        + " PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ",
                PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ,
                RemoteSegmentGraphMerger.MIN_VECTORS_FOR_FUSED_PQ);
    }

    /**
     * {@link RemoteSegmentGraphMerger#featureSetToStringList} must produce a
     * sorted, canonical list of feature-name strings.
     */
    @Test
    public void featureSetToStringListIsSorted() {
        java.util.Set<FeatureId> fs = new java.util.HashSet<>(
                Arrays.asList(FeatureId.INLINE_VECTORS, FeatureId.FUSED_PQ));
        List<String> names = RemoteSegmentGraphMerger.featureSetToStringList(fs);
        assertEquals(Arrays.asList("FUSED_PQ", "INLINE_VECTORS"), names);
    }

    // -------------------------------------------------------------------------
    // End-to-end fallback test
    // -------------------------------------------------------------------------

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds one segment with the given number of vectors in a fresh
     * {@link PersistentVectorStore} and returns the corresponding
     * {@link RemoteSegmentGraphMerger.RemoteSegmentInput} descriptor plus the
     * inserted PKs. The store uses the caller-supplied DSM and writes into the
     * given tablespace+index namespace so multiple stores can share one DSM.
     */
    private RemoteSegmentGraphMerger.RemoteSegmentInput buildOneSegment(
            Path tmpDir,
            MemoryDataStorageManager dsm,
            String tsUuid,
            String tblName,
            int dim,
            int vectorCount,
            long seedOffset,
            List<Bytes> collectedPks) throws Exception {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                tsUuid, tblName, tsUuid, "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {
            store.start();
            Random rng = new Random(seedOffset);
            for (int i = 0; i < vectorCount; i++) {
                Bytes pk = Bytes.from_long(seedOffset * 1_000_000L + i);
                collectedPks.add(pk);
                store.addVector(pk, vec(rng, dim));
            }
            store.checkpoint();
            assertEquals("expected exactly one segment after one checkpoint",
                    1, store.getSegmentCount());

            List<VectorSegment> segs = store.getOnDiskSegmentsSnapshotForTest();
            assertEquals(1, segs.size());
            VectorSegment seg = segs.get(0);
            String segUuid = seg.segmentUuid != null ? seg.segmentUuid
                    : store.indexUUID() + "_seg" + seg.segmentId;
            return new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    tsUuid, store.indexUUID(), segUuid,
                    seg.segmentId,
                    /* mapFileSize  */ seg.mapFileSize,
                    /* graphFileSize */ seg.graphFileSize,
                    /* generation  */ seg.generation,
                    /* tombstones  */ new int[0]);
        }
    }

    /**
     * Creates two segments with different jvector feature sets (one FusedPQ,
     * one InlineVectors only), merges them via the optimizer-side merger, and
     * asserts that:
     * <ul>
     *   <li>The merge completes successfully — no exception.</li>
     *   <li>The output PK count equals the sum of the two input counts (no
     *       tombstones or duplicates).</li>
     *   <li>{@code output.featureIds} is set and non-empty.</li>
     * </ul>
     */
    @Test
    public void heterogeneousInputsFallBackToLegacyAndSucceed() throws Exception {
        final int dim = 8;
        // 256 vectors → FusedPQ + InlineVectors (exactly at the threshold)
        final int largeCount = PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ;
        // 30 vectors → InlineVectors only (well below threshold)
        final int smallCount = 30;

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Path tmpDir = tmpFolder.newFolder().toPath();

        List<Bytes> pksFromLarge = new ArrayList<>();
        List<Bytes> pksFromSmall = new ArrayList<>();

        // Build the large (FusedPQ+InlineVectors) segment
        RemoteSegmentGraphMerger.RemoteSegmentInput largeInput =
                buildOneSegment(tmpDir, dsm, "ts-large", "tbl-large", dim,
                        largeCount, /* seedOffset */ 0L, pksFromLarge);

        // Build the small (InlineVectors-only) segment
        RemoteSegmentGraphMerger.RemoteSegmentInput smallInput =
                buildOneSegment(tmpDir, dsm, "ts-small", "tbl-small", dim,
                        smallCount, /* seedOffset */ 1L, pksFromSmall);

        // Both inputs share the same DSM but have distinct tablespace+index UUIDs —
        // their graph/map files are stored under different namespaces, so the merger
        // can read them independently.
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs =
                Arrays.asList(largeInput, smallInput);

        // Use an arbitrary output namespace (could match either input or be new)
        String outTs = "ts-merged";
        String outIdx = "idx-merged";
        long outSegId = 999L;

        // Enable streaming so the heterogeneous-detection code path in mergeStreaming
        // is exercised (it detects feature-set mismatch and falls back to mergeLegacy).
        VectorIndexCompactor.streamingCompactionEnabled = true;

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpFolder.newFolder().toPath(),
                /* graphM */ 16, /* beamWidth */ 100,
                /* neighborOverflow */ 1.2f, /* alpha */ 1.4f,
                VectorSimilarityFunction.COSINE);

        RemoteSegmentGraphMerger.MergeOutput output =
                merger.merge(inputs, outTs, outIdx, outSegId, dim);

        assertNotNull("merge must produce a non-null output", output);
        assertEquals("output should cover all input PKs",
                largeCount + smallCount, output.vectorCount);
        assertNotNull("output.featureIds must be set", output.featureIds);
        assertTrue("output.featureIds must be non-empty", !output.featureIds.isEmpty());

        // The legacy path rebuilds a fresh graph from the combined vector set; the
        // feature set it uses is determined by the total vector count.
        int totalVectors = largeCount + smallCount;
        if (totalVectors >= PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ) {
            assertTrue("combined count >= threshold → output must include FUSED_PQ",
                    output.featureIds.contains("FUSED_PQ"));
        } else {
            assertEquals("combined count < threshold → output must be INLINE_VECTORS only",
                    Arrays.asList("INLINE_VECTORS"), output.featureIds);
        }

        // Cleanup: delete the output so the temporary directory can be removed.
        merger.deleteOutput(output);
    }

    /**
     * Variant of the end-to-end fallback test where both input segments share
     * the same tablespace + index UUID — the production naming pattern. Uses a
     * single {@link PersistentVectorStore} that emits two checkpoints with
     * different vector counts so the segments land in the same DSM namespace
     * but carry different feature sets (below/above the FusedPQ threshold).
     */
    @Test
    public void heterogeneousInputsSameNamespaceFallBackToLegacy() throws Exception {
        final int dim = 8;
        final int smallCount = 30;  // below MIN_VECTORS_FOR_FUSED_PQ → InlineVectors only
        final int largeCount = PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ; // → FusedPQ+InlineVectors

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Path tmpDir = tmpFolder.newFolder().toPath();

        // Single store → both segments share tablespaceUuid + indexUuid.
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs;
        String sharedTs;
        String sharedIdx;

        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "ts-shared", "tbl", "ts-shared", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {
            store.start();
            sharedTs = "ts-shared";
            sharedIdx = store.indexUUID();

            Random rng = new Random(42L);
            // First checkpoint: smallCount vectors → InlineVectors only
            for (int i = 0; i < smallCount; i++) {
                store.addVector(Bytes.from_long(i), vec(rng, dim));
            }
            store.checkpoint();

            // Second checkpoint: largeCount vectors → FusedPQ + InlineVectors
            for (int i = 0; i < largeCount; i++) {
                store.addVector(Bytes.from_long(1_000_000L + i), vec(rng, dim));
            }
            store.checkpoint();

            assertEquals("expected exactly two segments", 2, store.getSegmentCount());

            List<VectorSegment> segs = store.getOnDiskSegmentsSnapshotForTest();
            inputs = new ArrayList<>(segs.size());
            for (VectorSegment seg : segs) {
                String segUuid = seg.segmentUuid != null ? seg.segmentUuid
                        : sharedIdx + "_seg" + seg.segmentId;
                inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        sharedTs, sharedIdx, segUuid,
                        seg.segmentId,
                        /* mapFileSize  */ seg.mapFileSize,
                        /* graphFileSize */ seg.graphFileSize,
                        /* generation  */ seg.generation,
                        /* tombstones  */ new int[0]));
            }
        }

        VectorIndexCompactor.streamingCompactionEnabled = true;

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpFolder.newFolder().toPath(),
                /* graphM */ 16, /* beamWidth */ 100,
                /* neighborOverflow */ 1.2f, /* alpha */ 1.4f,
                VectorSimilarityFunction.COSINE);

        RemoteSegmentGraphMerger.MergeOutput output =
                merger.merge(inputs, sharedTs, sharedIdx, 999L, dim);

        assertNotNull("merge must produce a non-null output", output);
        assertEquals("output must cover all input PKs",
                smallCount + largeCount, output.vectorCount);
        assertNotNull("output.featureIds must be set", output.featureIds);
        assertTrue("combined count >= threshold → output includes FUSED_PQ",
                output.featureIds.contains("FUSED_PQ"));

        merger.deleteOutput(output);
    }
}
