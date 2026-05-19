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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage for the streaming-compaction path introduced in
 * issue #485. Exercises {@link VectorIndexCompactor#rebuildSegmentStreaming}
 * via {@code runCompactionCycle()} on a {@link MemoryDataStorageManager}.
 *
 * <p>Asserted invariants:
 * <ul>
 *   <li><b>Deletion equivalence</b> — the merged segment's PK set equals
 *       the ground-truth set of authoritative survivors (computed by
 *       walking each input's {@code pkOffsets / pkLengths / pkData} and
 *       applying the same authority rule the compactor uses).</li>
 *   <li><b>Ordinal-remap density</b> — the merged segment's
 *       {@code pkOffsets} contains exactly {@code keptCount} non-{@code -1}
 *       entries at consecutive ordinals {@code 0..keptCount-1}; no holes.</li>
 *   <li><b>Search still works</b> — {@code store.search(...)} returns hits
 *       after the swap.</li>
 *   <li><b>{@code cachedPQ} invalidated</b> — the streaming-trained PQ
 *       does not poison the next checkpoint's reuse decision.</li>
 *   <li><b>Output reloadability</b> — the merged segment loads back through
 *       {@code OnDiskGraphIndex.load(...)} (already exercised by
 *       {@code preloadCompactedSegment}) and carries
 *       {@link FeatureId#INLINE_VECTORS}.</li>
 *   <li><b>Flag-off legacy parity</b> — flipping the flag off runs the
 *       in-memory rebuild and produces the same PK set on identical
 *       inputs.</li>
 * </ul>
 */
public class VectorIndexStreamingCompactionTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private boolean savedStreamingFlag;

    @Before
    public void disableCheckpointDeferral() {
        // Override the global deferral gate so tiny per-checkpoint shards
        // don't get deferred while the test is building segments.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        savedStreamingFlag = VectorIndexCompactor.streamingCompactionEnabled;
    }

    @After
    public void restoreCheckpointDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
        VectorIndexCompactor.streamingCompactionEnabled = savedStreamingFlag;
    }

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

    /**
     * Walks the on-disk segments and returns the union of their non-tombstoned
     * primary keys. Used as the "ground truth" against which the merged
     * segment's PK set is compared. Picks the highest-generation owner per PK
     * the same way {@link CompactionAuthorityMap} does.
     */
    private static Set<Bytes> authoritativePks(List<VectorSegment> segments) {
        // Map<PK, generation>: the highest-generation segment wins per PK.
        java.util.HashMap<Bytes, Long> bestGen = new java.util.HashMap<>();
        for (VectorSegment seg : segments) {
            int[] offsets = seg.pkOffsets;
            int[] lengths = seg.pkLengths;
            byte[] data = seg.pkData;
            if (offsets == null || lengths == null || data == null) {
                continue;
            }
            for (int ord = 0; ord < offsets.length; ord++) {
                int off = offsets[ord];
                if (off < 0) {
                    continue;
                }
                Bytes pk = Bytes.from_array(data, off, lengths[ord]);
                Long g = bestGen.get(pk);
                if (g == null || seg.generation > g) {
                    bestGen.put(pk, seg.generation);
                }
            }
        }
        return new HashSet<>(bestGen.keySet());
    }

    /** Returns the merged segment's PK set after compaction. */
    private static Set<Bytes> mergedSegmentPkSet(VectorSegment merged) {
        Set<Bytes> out = new HashSet<>();
        int[] offsets = merged.pkOffsets;
        int[] lengths = merged.pkLengths;
        byte[] data = merged.pkData;
        if (offsets == null || lengths == null || data == null) {
            return out;
        }
        for (int ord = 0; ord < offsets.length; ord++) {
            int off = offsets[ord];
            if (off < 0) {
                continue;
            }
            out.add(Bytes.from_array(data, off, lengths[ord]));
        }
        return out;
    }

    private static int writeSegments(PersistentVectorStore store, int dim, int perCheckpoint,
                                     int numCheckpoints, List<Bytes> insertedPks, long seed)
            throws Exception {
        Random rng = new Random(seed);
        for (int c = 0; c < numCheckpoints; c++) {
            for (int i = 0; i < perCheckpoint; i++) {
                Bytes pk = Bytes.from_int(c * 10_000 + i);
                insertedPks.add(pk);
                store.addVector(pk, vec(rng, dim));
            }
            store.checkpoint();
        }
        return store.getSegmentCount();
    }

    @Test
    public void streamingDeletionEquivalenceAndDenseOrdinals() throws Exception {
        VectorIndexCompactor.streamingCompactionEnabled = true;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            int dim = 16;
            int perCheckpoint = 300;
            int numCheckpoints = 5;
            List<Bytes> allPks = new ArrayList<>();
            int segmentsBefore = writeSegments(store, dim, perCheckpoint, numCheckpoints, allPks, 1);
            assertTrue("need >= 4 segments to exercise compaction, got " + segmentsBefore,
                    segmentsBefore >= 4);

            // Tombstone every 3rd inserted PK before compaction so authority
            // filtering kicks in.
            int tombstoned = 0;
            for (int i = 0; i < allPks.size(); i += 3) {
                store.removeVector(allPks.get(i));
                tombstoned++;
            }

            // Snapshot the pre-compaction segments so we can compute the ground-truth
            // surviving PK set.
            List<VectorSegment> preSegments = store.getOnDiskSegmentsSnapshotForTest();
            Set<Bytes> expectedSurvivors = authoritativePks(preSegments);

            long genBefore = store.getCurrentIndexStatusGeneration();
            store.runCompactionCycle();
            long genAfter = store.getCurrentIndexStatusGeneration();

            assertTrue("generation must advance after streaming compaction", genAfter > genBefore);
            assertTrue("segment count must shrink",
                    store.getSegmentCount() < segmentsBefore);
            assertEquals(1, store.getCompactionSuccessesTotal());
            assertEquals(0, store.getCompactionConsecutiveFailures());

            // Locate the merged segment (highest generation in the post-compaction snapshot).
            List<VectorSegment> postSegments = store.getOnDiskSegmentsSnapshotForTest();
            VectorSegment merged = postSegments.stream()
                    .max(java.util.Comparator.comparingLong(s -> s.generation))
                    .orElseThrow(() -> new AssertionError("no merged segment"));

            // (a) Deletion equivalence: every authoritative pre-compaction PK
            // is in the merged segment, and no extra ones.
            Set<Bytes> mergedPks = mergedSegmentPkSet(merged);
            assertEquals("merged PK set must equal authoritative survivors",
                    expectedSurvivors, mergedPks);

            // (b) Ordinal-remap density: dense 0..keptCount-1, no holes.
            int[] offsets = merged.pkOffsets;
            assertNotNull(offsets);
            int kept = 0;
            int firstHole = -1;
            for (int ord = 0; ord < offsets.length; ord++) {
                if (offsets[ord] >= 0) {
                    kept++;
                } else if (firstHole < 0) {
                    firstHole = ord;
                }
            }
            assertEquals("merged segment must contain exactly keptCount entries",
                    mergedPks.size(), kept);
            // Either no holes at all, or all holes follow the dense prefix
            // (there should be none in a dense streaming output).
            assertTrue("merged segment ordinals must be dense (no internal holes); firstHole="
                            + firstHole + " kept=" + kept,
                    firstHole < 0 || firstHole >= kept);

            // (c) Search still works: pick any authoritative PK, build a query
            // vector from its position, expect a non-empty result.
            List<Map.Entry<Bytes, Float>> hits = store.search(vec(new Random(2), dim), 5);
            assertNotNull(hits);
            assertFalse(hits.isEmpty());

            // (d) cachedPQ invalidated: the streaming engine retrained internally
            // and called invalidateCachedPq() on success.
            assertNull("cachedPQ must be invalidated after streaming compaction",
                    store.getCachedPqForTest());

            // (e) Output reloadability: the merged segment carries INLINE_VECTORS.
            assertTrue("merged segment must carry INLINE_VECTORS",
                    merged.onDiskGraph.getFeatures().containsKey(FeatureId.INLINE_VECTORS));

            // Sanity: tombstoned PK count was recorded by the compaction's
            // live-PK filter telemetry.
            assertTrue("compaction live-PK filter must have dropped the tombstoned PKs",
                    store.getCompactionLivePkFilteredTotal() >= tombstoned);
        }
    }

    @Test
    public void legacyPathStillWorksWhenFlagIsOff() throws Exception {
        VectorIndexCompactor.streamingCompactionEnabled = false;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            List<Bytes> allPks = new ArrayList<>();
            int segmentsBefore = writeSegments(store, 16, 300, 5, allPks, 7);
            assertTrue(segmentsBefore >= 4);

            // Snapshot ground truth before compaction.
            Set<Bytes> expectedSurvivors =
                    authoritativePks(store.getOnDiskSegmentsSnapshotForTest());

            store.runCompactionCycle();
            assertEquals(1, store.getCompactionSuccessesTotal());

            // Locate merged segment by generation.
            VectorSegment merged = store.getOnDiskSegmentsSnapshotForTest().stream()
                    .max(java.util.Comparator.comparingLong(s -> s.generation))
                    .orElseThrow(() -> new AssertionError("no merged segment"));

            // Same surviving PK set as the ground truth: this is the
            // observable equivalence between the legacy and streaming paths.
            assertEquals(expectedSurvivors, mergedSegmentPkSet(merged));
        }
    }

    /**
     * Issue #485 review item B.3#1: the {@code < 2 candidates} fallback to
     * the legacy in-memory rebuild must trigger when the policy hands a
     * single-segment cycle to the streaming entry point. Build exactly one
     * segment + a non-trivial live shard, force a 1-segment compaction
     * candidate by tightening {@code minCount=1}, and observe the legacy
     * synthetic-shard path firing via {@link
     * VectorIndexCompactor#syntheticShardObserverForTest}.
     */
    @Test
    public void streamingFallsBackToLegacyForSingleCandidate() throws Exception {
        VectorIndexCompactor.streamingCompactionEnabled = true;
        long fallbackBefore = VectorIndexCompactor.getStreamingFallbackToLegacyTotal();
        AtomicReference<PersistentVectorStore.LiveGraphShard> observed =
                new AtomicReference<>();
        VectorIndexCompactor.syntheticShardObserverForTest = observed::set;
        try {
            Path tmpDir = tmpFolder.newFolder().toPath();
            MemoryDataStorageManager dsm = new MemoryDataStorageManager();
            MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
            try (PersistentVectorStore store = new PersistentVectorStore(
                    "testidx", "testtable", "tstblspace", "vector_col",
                    tmpDir, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    /*compactionIntervalMs*/ Long.MAX_VALUE)) {
                // minCount=1, maxCount=1: hand the streaming entry point a
                // single-candidate cycle so the < 2 sources fallback fires.
                store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE,
                        /*minCount*/ 1, /*maxCount*/ Integer.MAX_VALUE,
                        /*retentionMs*/ 0);
                store.start();
                Random rng = new Random(11);
                List<Bytes> insertedPks = new ArrayList<>();
                // First segment: 200 rows -> checkpoint -> exactly one on-disk segment.
                for (int i = 0; i < 200; i++) {
                    Bytes pk = Bytes.from_int(i);
                    insertedPks.add(pk);
                    store.addVector(pk, vec(rng, 16));
                }
                store.checkpoint();
                // Live shard: 50 more rows still in memory (will be folded in via
                // authority-by-PK-from-live-shards during rebuild).
                for (int i = 200; i < 250; i++) {
                    Bytes pk = Bytes.from_int(i);
                    insertedPks.add(pk);
                    store.addVector(pk, vec(rng, 16));
                }
                assertEquals("setup must produce exactly one on-disk segment",
                        1, store.getSegmentCount());

                store.runCompactionCycle();

                // The fallback metric must have advanced (this is the only way
                // the single-candidate path can run without throwing).
                long fallbackAfter = VectorIndexCompactor.getStreamingFallbackToLegacyTotal();
                assertTrue("fallback-to-legacy counter must advance: before="
                                + fallbackBefore + " after=" + fallbackAfter,
                        fallbackAfter == fallbackBefore + 1);

                // The synthetic-shard observer must have been invoked: that's
                // the legacy path's signature event.
                assertNotNull("legacy synthetic-shard path must have run on fallback",
                        observed.get());

                // Compaction succeeded (the single-segment "rebuild" repackages
                // the segment by walking authoritative PKs).
                assertEquals(1, store.getCompactionSuccessesTotal());
                assertEquals(0, store.getCompactionConsecutiveFailures());
            }
        } finally {
            VectorIndexCompactor.syntheticShardObserverForTest = null;
        }
    }

    /**
     * Issue #485 review item B.3#2: the {@code INLINE_VECTORS} guard must
     * fire as {@code CompactionException(CORRUPTION, ...)} when a candidate's
     * graph lacks the feature. HerdDB writers always include {@code
     * INLINE_VECTORS} today, so the guard is dormant in production — but a
     * regression in any of the three writer sites would silently produce a
     * corrupt streaming output without this assertion in place.
     *
     * <p>The guard is exposed as a small package-private helper
     * ({@link VectorIndexCompactor#requireInlineVectorsFeature(int, java.util.Map)})
     * so the test does not need to mock a full {@link
     * io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex} instance —
     * {@code OnDiskGraphIndex}'s only constructor is {@code private}, which
     * makes a non-reflection mock impossible. Testing the guard at this level
     * still locks in the invariant: the helper is the single call site
     * referenced by {@code rebuildSegmentStreaming}.
     */
    @Test
    public void streamingRejectsCandidateWithoutInlineVectors() throws Exception {
        // Empty feature map → guard must throw CORRUPTION.
        try {
            VectorIndexCompactor.requireInlineVectorsFeature(42, java.util.Collections.emptyMap());
            org.junit.Assert.fail("requireInlineVectorsFeature must throw on empty feature map");
        } catch (VectorIndexCompactor.CompactionException expected) {
            assertEquals(VectorIndexCompactor.FailureReason.CORRUPTION, expected.reason);
            assertTrue("error message must reference INLINE_VECTORS, got: "
                            + expected.getMessage(),
                    expected.getMessage() != null
                            && expected.getMessage().contains("INLINE_VECTORS"));
            assertTrue("error message must reference the segment id, got: "
                            + expected.getMessage(),
                    expected.getMessage().contains("42"));
        }

        // null map → guard must throw CORRUPTION (defensive against jvector
        // contract drift; same outcome as empty map for symmetry).
        try {
            VectorIndexCompactor.requireInlineVectorsFeature(7, null);
            org.junit.Assert.fail("requireInlineVectorsFeature must throw on null feature map");
        } catch (VectorIndexCompactor.CompactionException expected) {
            assertEquals(VectorIndexCompactor.FailureReason.CORRUPTION, expected.reason);
        }

        // Map containing INLINE_VECTORS → guard must NOT throw. Use the FUSED_PQ
        // FeatureId as a noise key alongside INLINE_VECTORS to confirm the guard
        // looks for the right key, not any-key-present.
        java.util.Map<io.github.jbellis.jvector.graph.disk.feature.FeatureId, Object> ok =
                new java.util.HashMap<>();
        ok.put(io.github.jbellis.jvector.graph.disk.feature.FeatureId.INLINE_VECTORS,
                new Object()); // sentinel value — guard only consults containsKey()
        VectorIndexCompactor.requireInlineVectorsFeature(0, ok); // no exception
        ok.put(io.github.jbellis.jvector.graph.disk.feature.FeatureId.FUSED_PQ,
                new Object());
        VectorIndexCompactor.requireInlineVectorsFeature(0, ok); // still no exception

        // Map containing only FUSED_PQ → guard must throw (a single non-INLINE
        // feature must not pass the guard).
        java.util.Map<io.github.jbellis.jvector.graph.disk.feature.FeatureId, Object> wrongKey =
                new java.util.HashMap<>();
        wrongKey.put(io.github.jbellis.jvector.graph.disk.feature.FeatureId.FUSED_PQ,
                new Object());
        try {
            VectorIndexCompactor.requireInlineVectorsFeature(99, wrongKey);
            org.junit.Assert.fail("requireInlineVectorsFeature must throw when only FUSED_PQ is present");
        } catch (VectorIndexCompactor.CompactionException expected) {
            assertEquals(VectorIndexCompactor.FailureReason.CORRUPTION, expected.reason);
        }
    }

    /**
     * Issue #485 review item B.1#1 (orphan tracking on partial upload failure).
     *
     * <p>The streaming path uploads the merged graph and map files in two
     * sequential {@code writeMultipartIndexFile} calls. A failure in either
     * call must route both candidate orphan paths through {@code pendingDeletes}
     * so the retention reaper can sweep them. Pre-fix the rebuild method
     * rethrew the raw {@code IOException} / {@code DataStorageManagerException}
     * and the outer cycle's {@code catch (IOException)} arm did not consult
     * the orphans list.
     *
     * <p>This test forces the FIRST multipart write (graph) to fail,
     * which means nothing is uploaded — but the orphan-tracking code is
     * conservative and queues both anyway. A future enhancement could make
     * the orphan list precise; locking in the current behaviour as the
     * regression gate.
     */
    @Test
    public void streamingFailedUploadQueuesOrphansForRetentionSweep() throws Exception {
        VectorIndexCompactor.streamingCompactionEnabled = true;
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            // Build 5 segments so the streaming path runs (>= 2 sources).
            Random rng = new Random(33);
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 200; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, 16));
                }
                store.checkpoint();
            }
            int pendingBefore = store.getPendingDeletesSnapshot().size();

            // Arm the DSM to fail the next multipart write (the graph upload).
            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();

            // Failure metric must have advanced.
            assertTrue("compaction failure metric must have advanced",
                    store.getCompactionFailuresWriteIoTotal()
                            + store.getCompactionFailuresMetadataIoTotal() > 0);

            // Both orphan multipart entries (graph + map) must be queued for
            // retention-aware deletion regardless of which upload step failed.
            List<PersistentVectorStore.PendingDelete> pendingAfter =
                    store.getPendingDeletesSnapshot();
            assertTrue("orphan tracking must queue both graph and map entries:"
                            + " pendingBefore=" + pendingBefore
                            + " pendingAfter.size=" + pendingAfter.size(),
                    pendingAfter.size() >= pendingBefore + 2);
        }
    }

    /** Minimal failure-injection DSM. Counts the next N {@code writeMultipartIndexFile} calls and throws. */
    private static final class FailingDsm extends MemoryDataStorageManager {
        final AtomicInteger failNextMultipartWrites = new AtomicInteger(0);

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid,
                                              String fileType, Path tempFile,
                                              LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (failNextMultipartWrites.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IOException("injected multipart write failure (issue #485 test)");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }
    }

    @Test
    public void searchResultsConvergeAcrossPaths() throws Exception {
        // Run an identical workload twice — once streaming, once legacy —
        // then probe both with the same query. We don't expect bit-identical
        // recall (the two engines build the graph differently) but the top-K
        // for a clearly-clustered point should overlap meaningfully.
        Path tmpDirA = tmpFolder.newFolder().toPath();
        Path tmpDirB = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsmA = new MemoryDataStorageManager();
        MemoryDataStorageManager dsmB = new MemoryDataStorageManager();
        int dim = 16;
        int topK = 10;
        // Build a small clustered dataset so the closest neighbours are
        // unambiguous: 200 vectors near the origin + 100 vectors offset by 100.
        Random rng = new Random(42);
        List<float[]> dataset = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
                v[d] = rng.nextFloat();
            }
            dataset.add(v);
        }
        for (int i = 0; i < 100; i++) {
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) {
                v[d] = 100f + rng.nextFloat();
            }
            dataset.add(v);
        }

        Set<Bytes> streamingTopK;
        Set<Bytes> legacyTopK;
        try {
            VectorIndexCompactor.streamingCompactionEnabled = true;
            try (PersistentVectorStore store = createStore(tmpDirA, dsmA)) {
                store.start();
                for (int c = 0; c < 5; c++) {
                    for (int i = 0; i < 60; i++) {
                        int idx = c * 60 + i;
                        if (idx >= dataset.size()) {
                            break;
                        }
                        store.addVector(Bytes.from_int(idx), dataset.get(idx));
                    }
                    store.checkpoint();
                }
                store.runCompactionCycle();
                List<Map.Entry<Bytes, Float>> hits = store.search(dataset.get(0), topK);
                streamingTopK = new HashSet<>();
                for (Map.Entry<Bytes, Float> h : hits) {
                    streamingTopK.add(h.getKey());
                }
            }

            VectorIndexCompactor.streamingCompactionEnabled = false;
            try (PersistentVectorStore store = createStore(tmpDirB, dsmB)) {
                store.start();
                for (int c = 0; c < 5; c++) {
                    for (int i = 0; i < 60; i++) {
                        int idx = c * 60 + i;
                        if (idx >= dataset.size()) {
                            break;
                        }
                        store.addVector(Bytes.from_int(idx), dataset.get(idx));
                    }
                    store.checkpoint();
                }
                store.runCompactionCycle();
                List<Map.Entry<Bytes, Float>> hits = store.search(dataset.get(0), topK);
                legacyTopK = new HashSet<>();
                for (Map.Entry<Bytes, Float> h : hits) {
                    legacyTopK.add(h.getKey());
                }
            }
        } finally {
            VectorIndexCompactor.streamingCompactionEnabled = savedStreamingFlag;
        }

        // Intersection should be substantial — any near-zero overlap signals
        // that the streaming output is materially worse than the legacy path
        // (the whole point of the deletion-equivalence test is parity, but a
        // clustered query has to land on cluster #1, not the offset cluster).
        Set<Bytes> intersection = new HashSet<>(streamingTopK);
        intersection.retainAll(legacyTopK);
        assertTrue("streaming top-K must overlap legacy top-K substantially "
                        + "(streaming=" + streamingTopK + " legacy=" + legacyTopK + ")",
                intersection.size() >= Math.min(topK / 2, 3));
    }
}
