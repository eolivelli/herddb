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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reproducer + fix verification for issue #538: PR #536 (issue #535) is
 * incomplete. On a real bigann-100M run with the external optimizer
 * enabled, segments deprecated by the optimizer's own merges end up in
 * {@link PersistentVectorStore#segments segments} with
 * {@code onDiskGraph == null} — leading to the same persistent-failure
 * loop in {@link VectorIndexCompactor#rebuildSegmentStreaming} that
 * issue #535 was supposed to fix.
 *
 * <h2>Root cause</h2>
 *
 * {@link PersistentVectorStore#atomicSwapCompactionResult} runs in three
 * stages:
 *
 * <ol>
 *   <li><b>Stage 1</b> (under {@link PersistentVectorStore#checkpointLock}):
 *       snapshot {@code current = this.segments}; build
 *       {@code newSegments = current - inputs + mergedOutput}.</li>
 *   <li><b>Stage 2</b> (under {@code checkpointLock}, no writeLock):
 *       {@code persistIndexStatusMultiSegment} — the slow I/O.</li>
 *   <li><b>Stage 3</b> (under {@code stateLock.writeLock()}):
 *       publish {@code this.segments = newSegments}.</li>
 * </ol>
 *
 * Between Stages 1 and 3, a concurrent
 * {@link PersistentVectorStore#dropSegmentByUuid} (typically fired by the
 * optimizer-watcher's {@code onSegmentReleased} when the optimizer
 * deprecates a NON-input segment) can:
 *
 * <ul>
 *   <li>Remove the dropped segment from {@code this.segments} under the
 *       writeLock — the segment is now absent.</li>
 *   <li>Enqueue the segment on
 *       {@link PersistentVectorStore#pendingSegmentCloses} for the
 *       deferred close added by PR #536.</li>
 * </ul>
 *
 * Stage 3 then publishes the <b>stale</b> {@code newSegments} (which
 * still contains the just-dropped segment), <b>re-inserting</b> the
 * dropped segment into {@code this.segments}. The cycle's
 * {@code finally} block then drains
 * {@code pendingSegmentCloses}, calling {@code seg.close()} on the
 * re-inserted segment — which nulls {@code seg.onDiskGraph}.
 *
 * <p>Result: the dropped segment is back in {@code this.segments} with
 * {@code onDiskGraph == null}. Every subsequent {@code runCompactionCycle}
 * picks it as a candidate, hits the null-check at
 * {@code VectorIndexCompactor.rebuildSegmentStreaming:780}, and throws
 * {@code CompactionException(CORRUPTION, "candidate segment N has no
 * on-disk graph (streaming compaction)")} — exactly the production
 * symptom of issues #535 and #538.
 *
 * <h2>Heap-dump evidence (from issue #538)</h2>
 *
 * The 3.1 GiB heap dump captured during a stuck IS run shows:
 * <ul>
 *   <li>{@code PersistentVectorStore.segments} (a
 *       {@code CopyOnWriteArrayList}, 85 entries) contains 5 segments
 *       with {@code onDiskGraph == null} (segmentIds 15, 33, 39, 42,
 *       45 — all IS-locally-produced).</li>
 *   <li>{@code pendingSegmentCloses} is empty (the deferred-close drain
 *       has already run).</li>
 *   <li>Path-to-GC-roots confirms all 5 are in
 *       {@code this.segments[…]}.</li>
 * </ul>
 *
 * The Stage-3 republish race is the only path that can produce this
 * combination of in-{@code segments} + closed + drained.
 *
 * <h2>The fix</h2>
 *
 * Stage 3 rebuilds {@code newSegments} from the <b>current</b>
 * {@code this.segments} (read under writeLock), not from Stage 1's
 * snapshot. A segment dropped between Stage 1 and Stage 3 is therefore
 * absent from {@code currentAtStage3} and is correctly NOT republished.
 *
 * <h2>What this test proves</h2>
 *
 * The reproducer uses the existing
 * {@link PersistentVectorStore#setAtomicSwapPostPersistHookForTest} hook
 * (which fires AFTER Stage 2 persist but BEFORE Stage 3 publish) to
 * synchronously call {@code dropSegmentByUuid} on a NON-input segment.
 * Post-fix, the dropped segment must NOT be present in
 * {@code this.segments} after the cycle completes.
 */
public class Issue538Stage3RepublishRaceTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 16;

    @Before
    public void disableDeferral() {
        // Allow small live shards to seal into on-disk segments, so the
        // test can produce enough segments quickly.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private static float[] vec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore newStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, // disable the auto compaction loop
                VectorSimilarityFunction.EUCLIDEAN);
        // maxTotalBytes = 25,000 with the 5 seeded segments (≈4.5k / 9k /
        // 13.5k / 18k / 22.5k = 67.5k total) DETERMINISTICALLY restricts
        // chooseSegmentsToMerge to picking the 2 smallest segments as
        // candidates (4.5k + 9k = 13.5k fits; adding the next 13.5k would
        // overflow 25k). The remaining 3 LARGER segments are non-inputs
        // and are the only valid targets for the Stage-3 race.
        //
        // minCount=2 lets the byte-trigger fire on those 2 candidates.
        // minBytes=1 makes every cycle qualify regardless of size.
        // maxCount=Integer.MAX_VALUE — irrelevant given the byte-trigger.
        store.configureCompaction(Long.MAX_VALUE, 1L, 25_000L, 2, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * Seeds {@code n} on-disk segments via add+checkpoint cycles, with
     * different vector counts so they have different
     * {@code estimatedSizeBytes} — that way
     * {@link VectorIndexCompactor#chooseSegmentsToMerge} (which sorts by
     * size ascending and respects {@code maxCount}) deterministically
     * picks the smallest ones as candidates and leaves the larger ones
     * as non-candidates (the drop victims).
     */
    private void seedSegments(PersistentVectorStore store, int n) throws Exception {
        Random rng = new Random(42);
        for (int c = 0; c < n; c++) {
            // Each subsequent checkpoint adds more vectors so the segments
            // have monotonically increasing sizes; the policy's
            // sort-ascending + maxCount=2 then deterministically picks the
            // first two segments as candidates.
            int n_vectors = 20 + c * 20;
            for (int i = 0; i < n_vectors; i++) {
                store.addVector(Bytes.from_int(c * 1000 + i), vec(rng));
            }
            store.checkpoint();
        }
    }

    /** Stamps a UUID on every segment that doesn't already carry one. */
    private void stampMissingUuids(List<VectorSegment> segs) {
        for (VectorSegment seg : segs) {
            if (seg.segmentUuid == null) {
                seg.segmentUuid = UUID.randomUUID().toString();
            }
        }
    }

    /**
     * <b>The fix verification.</b> Fires {@code dropSegmentByUuid} on a
     * NON-INPUT segment AFTER Stage 2 persist but BEFORE Stage 3 publish.
     * Pre-fix the segment is re-inserted by Stage 3's stale snapshot;
     * post-fix Stage 3 rebuilds from {@code this.segments} and the
     * segment stays dropped.
     */
    @Test(timeout = 30_000)
    public void dropSegmentByUuidBetweenStage2AndStage3IsNotRepublishedByStage3()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-stage3-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            // Seed 5 segments with increasing sizes. With maxCount=2 the
            // policy will pick the 2 smallest as candidates, leaving the
            // other 3 as drop victims.
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertTrue("setup must have at least 5 segments — got " + initial.size(),
                    initial.size() >= 5);
            stampMissingUuids(initial);

            // The candidates the policy will pick (2 smallest) are at the
            // start when sorted by size ascending. To pick a NON-input,
            // grab the LARGEST segment — the policy with maxCount=2 will
            // never select it.
            VectorSegment largest = initial.get(0);
            for (VectorSegment s : initial) {
                if (s.estimatedSizeBytes > largest.estimatedSizeBytes) {
                    largest = s;
                }
            }
            final VectorSegment victim = largest;
            final String victimUuid = victim.segmentUuid;
            final int victimId = victim.segmentId;
            assertNotNull("victim must have a UUID stamped", victimUuid);
            assertNotNull("victim must have onDiskGraph populated", victim.onDiskGraph);

            // Drop the victim AFTER Stage 2 persist but BEFORE Stage 3
            // publish. This is the precise race window where #538 fires.
            store.setAtomicSwapPostPersistHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            long successesBefore = store.getCompactionSuccessesTotal();

            store.runCompactionCycle();

            // ----- Post-fix assertions -----

            // The hook removed the victim from this.segments under writeLock,
            // BEFORE Stage 3 published. Post-fix, Stage 3 must rebuild from
            // the current this.segments (which doesn't have the victim) and
            // must NOT re-insert the victim.
            //
            // Pre-fix (master @ 9ebb106b), Stage 3 re-inserts the victim:
            // the next compaction cycle then sees seg.onDiskGraph == null
            // and throws CompactionException(CORRUPTION) — exactly the
            // production failure mode in issue #538.
            List<VectorSegment> after = store.getOnDiskSegmentsSnapshotForTest();
            for (VectorSegment s : after) {
                assertFalse("victim must NOT be republished by Stage 3 — that was"
                        + " issue #538's exact production failure mode (Stage 3"
                        + " rebuilt newSegments from a stale Stage-1 snapshot,"
                        + " re-inserting a segment a concurrent dropSegmentByUuid"
                        + " had just removed)",
                        s.segmentId == victimId);
            }

            // The compaction succeeded: a merged output was published.
            assertEquals("the compaction must have completed successfully",
                    successesBefore + 1L, store.getCompactionSuccessesTotal());

            // No CORRUPTION error was recorded. (This cycle wouldn't be the
            // one that fires CORRUPTION — that happens on the NEXT cycle,
            // when the orphan is picked as a candidate. We assert no
            // corruption to keep the test focused on the Stage-3 fix.)
            assertEquals("no CORRUPTION failure on the racing cycle",
                    corruptionBefore, store.getCompactionFailuresCorruptionTotal());

            // The deferred close drained the victim: onDiskGraph is null.
            // (This holds both pre-fix and post-fix; it's the
            // PR #536 / #535 contract.)
            assertNull("victim.onDiskGraph must be null after the deferred-close drain",
                    victim.onDiskGraph);
        }
    }

    /**
     * Strict follow-up: a cycle following the racing one must NOT fail with
     * CORRUPTION on the (no-longer-orphan) victim. Pre-fix the victim would
     * be back in this.segments with onDiskGraph==null and the next cycle
     * would throw {@code candidate segment N has no on-disk graph
     * (streaming compaction)}. Post-fix the victim is gone and the next
     * cycle proceeds normally on remaining segments.
     *
     * <p>This test also asserts the second cycle actually executed work
     * (compaction success counter advanced), to catch a regression where
     * the policy silently finds no candidates and the "no corruption"
     * assertion passes vacuously.
     */
    @Test(timeout = 30_000)
    public void noCorruptionOnNextCycleAfterRacingCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-stage3-race-next").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            stampMissingUuids(initial);

            VectorSegment largest = initial.get(0);
            for (VectorSegment s : initial) {
                if (s.estimatedSizeBytes > largest.estimatedSizeBytes) {
                    largest = s;
                }
            }
            final String victimUuid = largest.segmentUuid;

            store.setAtomicSwapPostPersistHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            // First cycle: the race fires.
            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            long successesBefore = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            long corruptionAfterFirst = store.getCompactionFailuresCorruptionTotal();
            assertEquals("first cycle must not record CORRUPTION (the race shifts"
                    + " the symptom to the NEXT cycle, not this one)",
                    corruptionBefore, corruptionAfterFirst);

            // Disarm the hook so the second cycle runs without the race.
            store.setAtomicSwapPostPersistHookForTest(null);

            // Loosen the policy so the second cycle compacts everything still
            // present — including the (post-fix) merged output from cycle 1
            // and the remaining non-input segments. Pre-fix the orphaned
            // victim would be republished and selected here, tripping the
            // no-on-disk-graph check. Post-fix the orphan is gone and the
            // cycle merges the remaining healthy segments cleanly.
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);

            store.runCompactionCycle();

            assertEquals(
                "no CORRUPTION on the cycle following the race — that was"
                + " the persistent-failure loop reported in issue #538",
                corruptionBefore, store.getCompactionFailuresCorruptionTotal());

            // Catch the silent "no candidates" trap: the second cycle MUST
            // have done useful work (i.e. a compaction succeeded), otherwise
            // the no-corruption assertion above is vacuous.
            assertTrue("the second cycle must have produced at least one"
                    + " successful compaction (regression guard against"
                    + " 'no candidates' masking a real failure)",
                    store.getCompactionSuccessesTotal() > successesBefore);
        }
    }

    /**
     * Coverage gap (pr-reviewer follow-up): exercises a drop landing in
     * the Stage 1 → Stage 2 sub-window (via {@code atomicSwapPostBuildHook},
     * which fires AFTER Stage 1 build and BEFORE Stage 2 persist). The fix
     * must protect the FULL Stage 1 → Stage 3 window, not just the
     * immediate pre-Stage-3 sub-window. Post-fix the dropped segment is
     * NOT republished regardless of which sub-window the drop landed in.
     *
     * <p>The three sub-windows (Stage 1 → Stage 2, during Stage 2 persist,
     * Stage 2 → Stage 3) are equivalent under the fix because Stage 3
     * rebuilds {@code finalNewSegments} from a fresh read of
     * {@code this.segments} under the writeLock — so the test only needs
     * to cover one of them; the other two share the same code path.
     */
    @Test(timeout = 30_000)
    public void dropBeforeStage2PersistIsNotRepublishedByStage3() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-stage2-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            stampMissingUuids(initial);

            VectorSegment largest = initial.get(0);
            for (VectorSegment s : initial) {
                if (s.estimatedSizeBytes > largest.estimatedSizeBytes) {
                    largest = s;
                }
            }
            final int victimId = largest.segmentId;
            final String victimUuid = largest.segmentUuid;

            // PostBuildHook fires AFTER Stage 1 builds newSegments and BEFORE
            // Stage 2 starts the slow persist. Drop the victim here. With the
            // fix, Stage 3 re-reads this.segments and excludes the dropped
            // segment.
            store.setAtomicSwapPostBuildHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            store.runCompactionCycle();

            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                assertFalse("victim dropped during Stage 1 → Stage 2 must NOT"
                        + " be republished by Stage 3 (issue #538: the fix"
                        + " must protect the full Stage 1 → Stage 3 window,"
                        + " not just the immediate pre-Stage-3 sub-window)",
                        s.segmentId == victimId);
            }
            assertEquals("no CORRUPTION recorded",
                    corruptionBefore, store.getCompactionFailuresCorruptionTotal());
        }
    }

    /** Baseline regression guard: a normal compaction succeeds. */
    @Test(timeout = 30_000)
    public void noRaceMeansNormalCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-baseline").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            long before = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            long after = store.getCompactionSuccessesTotal();

            assertEquals("baseline compaction must succeed", before + 1L, after);
            assertEquals("no CORRUPTION on the baseline", 0L,
                    store.getCompactionFailuresCorruptionTotal());
        }
    }

    /**
     * Pr-reviewer follow-up: locks down the compaction Stage 3 comment's
     * claim that adoptions landing in the Stage 1 → Stage 3 window are
     * preserved by the rebuild. The hook fires between Stage 2 persist
     * and Stage 3, simulating a concurrent {@code adoptExternalSegment}
     * by directly mutating {@code this.segments} (via reflection — full
     * {@code adoptExternalSegment} requires preexisting multipart files
     * keyed by externalStorageKey, which would dominate the test
     * setup). Asserts the synthetic adopted segment is present in
     * {@code this.segments} after the cycle returns.
     *
     * <p>Mirrors the {@code currentAtStage3}-driven rebuild semantics:
     * any segment in {@code this.segments} when Stage 3 acquires the
     * writeLock survives the publish, regardless of whether it was in
     * Stage 1's snapshot.
     */
    @Test(timeout = 30_000)
    public void adoptionDuringCompactionStage1To3MustBePreservedByStage3()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-compaction-adopt").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            stampMissingUuids(initial);

            // Synthetic adopted segment. The Stage 3 publish only iterates
            // and filters by segmentId, so a minimal VectorSegment with a
            // unique high ID is sufficient to verify preservation. Real
            // adoptions land via {@code adoptExternalSegment} from the
            // optimizer-watcher's {@code onSegmentAssigned} callback; the
            // observable effect on `this.segments` is identical to the
            // reflection-driven mutation below.
            final int adoptedId = 1_000_000_001;
            final VectorSegment adopted = new VectorSegment(adoptedId);
            adopted.segmentUuid = UUID.randomUUID().toString();
            adopted.generation = 9999L;

            // Hook fires BETWEEN Stage 2 persist and Stage 3 publish — same
            // window where a concurrent adoption could land in production.
            store.setAtomicSwapPostPersistHookForTest(() -> {
                try {
                    addSegmentViaReflection(store, adopted);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });

            long successesBefore = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            long successesAfter = store.getCompactionSuccessesTotal();

            assertEquals("compaction must succeed", successesBefore + 1L, successesAfter);

            // The adopted segment is in this.segments after Stage 3 publish.
            boolean foundAdopted = false;
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                if (s.segmentId == adoptedId) {
                    foundAdopted = true;
                    break;
                }
            }
            assertTrue("adoption landed in the Stage 1 → Stage 3 window must"
                    + " be PRESERVED by the Stage 3 rebuild (the rebuild"
                    + " iterates `currentAtStage3` and excludes only inputs;"
                    + " adoptions are not inputs)", foundAdopted);
        }
    }

    /**
     * Pr-reviewer follow-up: companion test for the checkpoint side. With
     * the fix the Phase C Stage 2 rebuild uses
     * {@code currentAtStage2 + preloadedSegments}, so an adoption landing
     * in the Phase A → Stage 2 window is preserved. Pre-fix the rebuild
     * filtered by membership in {@code sealedSegments + mergeableSegments
     * + preloadedSegments} — a Phase-A-time snapshot — and silently
     * dropped any adoption that arrived in the window. This test fails
     * pre-fix and passes post-fix.
     */
    @Test(timeout = 60_000)
    public void adoptionDuringCheckpointPhaseAToStage2MustNotBeDroppedByStage2()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-checkpoint-adopt").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            // Seed a few on-disk segments so Phase A captures a non-empty
            // sealed/mergeable list (the bug only fires when the
            // Phase-A snapshot is non-empty so the rebuild has somewhere
            // to filter against).
            seedSegments(store, 3);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertTrue("seedSegments must produce at least 2 segments",
                    initial.size() >= 2);
            stampMissingUuids(initial);

            final int adoptedId = 1_000_000_002;
            final VectorSegment adopted = new VectorSegment(adoptedId);
            adopted.segmentUuid = UUID.randomUUID().toString();
            adopted.generation = 9999L;

            // Hook fires after Stage 1's pending-deletes apply and BEFORE
            // Stage 2's writeLock. This is the Phase A → Stage 2 lock-free
            // window where real adoptions can land in production (the
            // optimizer-watcher's onSegmentAssigned does not hold
            // checkpointLock).
            store.setPhaseCPostDeletesApplyHookForTest(() -> {
                try {
                    addSegmentViaReflection(store, adopted);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });

            // Trigger a checkpoint. The hook will fire inside Phase C,
            // adding the synthetic adopted segment to this.segments. With
            // the fix, Stage 2's rebuild reads `currentAtStage2` and
            // preserves the adoption. Pre-fix the rebuild iterates only
            // Phase-A's snapshot and silently drops the adoption.
            store.addVector(Bytes.from_int(8888), vec(new Random(8888)));
            store.checkpoint();

            // The adopted segment MUST be in this.segments after the
            // checkpoint publishes.
            boolean foundAdopted = false;
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                if (s.segmentId == adoptedId) {
                    foundAdopted = true;
                    break;
                }
            }
            assertTrue("adoption landed in the Phase A → Stage 2 window must"
                    + " be PRESERVED by the Stage 2 rebuild (issue #538"
                    + " follow-up: the checkpoint-side rebuild must"
                    + " iterate `currentAtStage2` directly, not"
                    + " sealed+mergeable+preloaded with currentIds filter)",
                    foundAdopted);
        }
    }

    /**
     * Reflection-driven equivalent of {@code adoptExternalSegment}'s
     * end-effect on {@code this.segments}: writes a fresh
     * {@code CopyOnWriteArrayList} containing the current entries plus
     * the supplied segment back to the private {@code segments} field.
     * Used by adoption-window tests where a full
     * {@code adoptExternalSegment} setup (pre-written multipart files,
     * BLink storage init, jvector graph load) would dominate the test
     * boilerplate without exercising the bug.
     *
     * <p>Production-vs-test divergence: real
     * {@link PersistentVectorStore#adoptExternalSegment} acquires
     * {@code stateLock.writeLock()} before mutating {@code segments}.
     * This helper does NOT — it relies on the test running the hook
     * callback on the same thread as the cycle (which is the case for
     * every hook in this test file), so there is no concurrent writer
     * and no visibility concern.
     */
    private static void addSegmentViaReflection(PersistentVectorStore store,
            VectorSegment seg) throws ReflectiveOperationException {
        java.lang.reflect.Field f =
                PersistentVectorStore.class.getDeclaredField("segments");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<VectorSegment> current = (List<VectorSegment>) f.get(store);
        java.util.concurrent.CopyOnWriteArrayList<VectorSegment> newList =
                new java.util.concurrent.CopyOnWriteArrayList<>(current);
        newList.add(seg);
        f.set(store, newList);
    }

    /**
     * Test-only {@link VectorSegment} subclass that records every
     * {@code deletePk} invocation in a thread-safe list and returns
     * {@code false} so the reapply loop continues to subsequent segments.
     * Used by {@link
     * #stage2ReapplyMustCoverAdoptedSegmentsForPendingCheckpointDeletes}
     * to verify that the Stage-2 late-deletes reapply DOES iterate
     * adopted segments (a regression introduced by the round-3 fix
     * that preserves adoptions in the published list).
     */
    private static final class TrackingVectorSegment extends VectorSegment {
        final java.util.List<Bytes> deletePkInvocations =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        TrackingVectorSegment(int segmentId) {
            super(segmentId);
        }

        @Override
        boolean deletePk(Bytes key) {
            deletePkInvocations.add(key);
            // Return false so the reapply loop continues — we are a
            // pure observer, not an absorber.
            return false;
        }
    }

    /**
     * Test-only {@link VectorSegment} subclass that THROWS on every
     * {@code deletePk} invocation. Used by
     * {@link #stage2ReapplyThrowMustNotLeavePartialCounterUpdate} to
     * verify that a throw from the late-deletes reapply unwinds
     * cleanly: {@code onDiskSegmentsEstimatedMemoryBytes} stays at its
     * pre-Stage-2 value (preloadedSegments were not yet registered)
     * AND {@code this.segments} stays at the OLD list (publish did
     * not happen). Locks in the "reapply → register → publish"
     * ordering invariant.
     */
    private static final class ThrowingVectorSegment extends VectorSegment {
        ThrowingVectorSegment(int segmentId) {
            super(segmentId);
        }

        @Override
        boolean deletePk(Bytes key) {
            throw new RuntimeException(
                    "synthetic deletePk failure (issue #538 pr-reviewer pass-4 test)");
        }
    }

    /**
     * Coverage gap (pr-reviewer follow-up): the symmetric race in
     * {@link PersistentVectorStore#doCheckpointFusedPQThreePhase Phase C
     * Stage 2}. Structurally identical to the compaction Stage-3 race:
     * Phase A snapshots {@code segments} into
     * {@code sealedSegments + mergeableSegments} under writeLock, then
     * releases writeLock for the slow Phase B I/O and Phase C-prep loads.
     * Phase C Stage 2 re-acquires writeLock and publishes
     * {@code this.segments = newSegments} (the Phase-A snapshot +
     * preloadedSegments).
     *
     * <p>Between Phase A and Phase C Stage 2, a concurrent
     * {@code dropSegmentByUuid} can:
     *
     * <ol>
     *   <li>remove a segment from {@code this.segments} under writeLock,</li>
     *   <li>opportunistically drain {@code pendingSegmentCloses} (the
     *       checkpoint does NOT hold {@code compactionLock}, only
     *       {@code checkpointLock}, so the drain's tryLock succeeds),</li>
     *   <li>close the dropped segment ({@code onDiskGraph = null}).</li>
     * </ol>
     *
     * Phase C Stage 2 then republishes the dropped (now-closed) segment
     * into {@code this.segments} — same observable failure mode as #538.
     *
     * <p>This test fires the drop from {@code phaseCPostDeletesApplyHook}
     * (already exists at {@code PersistentVectorStore.java:1315}). The
     * hook runs inside Phase C, AFTER Stage 1's pending-deletes apply and
     * BEFORE Stage 2's writeLock acquisition.
     */
    @Test(timeout = 60_000)
    public void dropSegmentByUuidDuringCheckpointMustNotBeRepublishedByPhaseCStage2()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-checkpoint-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            // Seed via direct addVector + checkpoint (the existing
            // seedSegments uses this pattern); the checkpoint we'll race
            // is the one triggered by the next addVector+checkpoint pair.
            seedSegments(store, 4);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertTrue("seedSegments must produce at least 3 segments",
                    initial.size() >= 3);
            stampMissingUuids(initial);

            // Pick the smallest segment as the drop victim. Any segment
            // would do (the bug doesn't care about size for checkpoint),
            // but the smallest is a stable choice.
            VectorSegment smallest = initial.get(0);
            for (VectorSegment s : initial) {
                if (s.estimatedSizeBytes < smallest.estimatedSizeBytes) {
                    smallest = s;
                }
            }
            final VectorSegment victim = smallest;
            final String victimUuid = victim.segmentUuid;
            final int victimId = victim.segmentId;
            assertNotNull("victim must have a UUID stamped", victimUuid);
            assertNotNull("victim must have onDiskGraph populated", victim.onDiskGraph);

            // Install the hook BEFORE triggering the next checkpoint. The
            // hook fires inside Phase C, in the lock-free window before
            // Stage 2's writeLock acquisition.
            store.setPhaseCPostDeletesApplyHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            // Add one more vector + checkpoint to trigger a new checkpoint
            // cycle. The checkpoint's Phase A snapshots the current segment
            // list (which includes the victim); during Phase C the hook
            // drops the victim; Phase C Stage 2 must NOT republish it.
            store.addVector(Bytes.from_int(99999), vec(new Random(99)));
            store.checkpoint();

            // Post-fix assertions.
            List<VectorSegment> after = store.getOnDiskSegmentsSnapshotForTest();
            for (VectorSegment s : after) {
                assertFalse("victim must NOT be republished by Phase C Stage 2"
                        + " — this is the symmetric checkpoint twin of"
                        + " issue #538's compaction Stage-3 republish bug",
                        s.segmentId == victimId);
            }

            // The deferred-close drain ran during the dropSegmentByUuid's
            // opportunistic call (compactionLock was free during checkpoint),
            // so the victim's onDiskGraph is null.
            assertNull("victim.onDiskGraph must be null after the deferred-close"
                    + " drain that the checkpoint-window drop opportunistically"
                    + " triggered",
                    victim.onDiskGraph);

            // Pr-reviewer pass-3 follow-up: the dirty bit MUST remain true
            // after a race-affected checkpoint, even when
            // `totalLiveSize() == 0`. Pre-fix-3 the Stage-2 OVERWRITE
            // `dirty.set(totalLiveSize() > 0)` clobbered the `dirty=true`
            // signal that {@code dropSegmentByUuid} set in the Phase A →
            // Stage 2 window, leaving the next periodic checkpoint to skip
            // (it would observe `dirty == false`) and the IndexStatus-vs-
            // segments drift introduced by the rebuild would persist
            // indefinitely. Post-fix-3 the Stage-2 OR pattern
            // `dirty.set(dirty.get() || totalLiveSize() > 0)` preserves
            // the signal so the next checkpoint repersists IndexStatus
            // and heals the drift.
            assertTrue("dirty bit must be preserved past the race-affected"
                    + " checkpoint so the next periodic checkpoint runs"
                    + " and repersists IndexStatus (issue #538 pr-reviewer"
                    + " pass-3: Stage-2 must OR-merge the `dirty=true`"
                    + " signal from the in-window dropSegmentByUuid /"
                    + " adoptExternalSegment, not overwrite it with"
                    + " totalLiveSize()>0)",
                    store.isDirty());
        }
    }

    /**
     * Pr-reviewer pass-3 follow-up: with the round-3 fix preserving
     * adopted segments in {@code this.segments} past the checkpoint,
     * the Stage-2 late-deletes reapply must ALSO iterate those adopted
     * segments. Otherwise a {@code removeVector(P)} that arrived during
     * the checkpoint (placing {@code P} in
     * {@code pendingCheckpointDeletes}) BEFORE the adoption would
     * leave {@code P} live in the adopted segment after the checkpoint
     * — a stale-data window.
     *
     * <p>This test injects a {@link TrackingVectorSegment} via reflection
     * inside {@code phaseCPostDeletesApplyHook} (the Phase A → Stage 2
     * lock-free window), then calls {@code removeVector} from the same
     * hook so that {@code P} is added to {@code pendingCheckpointDeletes}.
     * Post-fix the Stage-2 reapply iterates {@code finalNewSegments},
     * which includes the adopted tracking segment, calling its
     * {@code deletePk(P)}. The test asserts the tracking segment's
     * invocation log contains {@code P} at least twice — once from
     * {@code removeVector}'s direct iteration of {@code this.segments}
     * and once from the Stage-2 reapply.
     *
     * <p>Pre-fix-3 the reapply iterates {@code preloadedSegments} only;
     * the tracking segment is silently bypassed, so the invocation count
     * is 1 instead of 2.
     */
    @Test(timeout = 60_000)
    public void stage2ReapplyMustCoverAdoptedSegmentsForPendingCheckpointDeletes()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-reapply-adopted").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 3);

            // The pk we will remove during the checkpoint. Pick a value
            // that's NOT in the seeded segments (so removeVector does not
            // find it in any real segment — keeps the test focused on the
            // reapply path). Any non-empty Bytes works.
            final Bytes pkToRemove = Bytes.from_int(999_999_001);

            final TrackingVectorSegment tracker = new TrackingVectorSegment(1_000_000_003);
            tracker.segmentUuid = UUID.randomUUID().toString();
            tracker.generation = 9999L;

            store.setPhaseCPostDeletesApplyHookForTest(() -> {
                try {
                    // 1. Inject the tracking segment into this.segments so
                    //    Stage 2's rebuild of finalNewSegments will include
                    //    it (currentAtStage2 + preloadedSegments).
                    addSegmentViaReflection(store, tracker);
                    // 2. Call removeVector(pkToRemove). Since a checkpoint
                    //    is in progress (pendingCheckpointDeletes != null),
                    //    the pk goes onto pendingCheckpointDeletes. The
                    //    pk is also tried against every segment via
                    //    removeVector's iteration of this.segments —
                    //    this includes the tracking segment, so the
                    //    tracker sees a removeVector-direct invocation.
                    store.removeVector(pkToRemove);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });

            // Trigger the checkpoint cycle.
            store.addVector(Bytes.from_int(7777), vec(new Random(7777)));
            store.checkpoint();

            // Confirm the tracker is in the published segment list (the
            // round-3 fix preserves adoptions; precondition for the
            // reapply to have a chance to iterate it).
            boolean trackerInSegments = false;
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                if (s.segmentId == tracker.segmentId) {
                    trackerInSegments = true;
                    break;
                }
            }
            assertTrue("tracker must be in this.segments post-publish "
                    + "(round-3 fix); precondition for the reapply assertion",
                    trackerInSegments);

            // CORE assertion: the tracker's deletePk was invoked at least
            // TWICE for pkToRemove — once by removeVector's direct
            // iteration of this.segments, AND once by Stage 2's late-
            // deletes reapply over finalNewSegments. Pre-fix-3 the reapply
            // iterated `preloadedSegments` only (which does not contain
            // the tracker), so the invocation count would be 1.
            int hits = 0;
            for (Bytes b : tracker.deletePkInvocations) {
                if (pkToRemove.equals(b)) {
                    hits++;
                }
            }
            assertTrue("Stage-2 late-deletes reapply must iterate adopted "
                    + "segments — tracker.deletePk(pkToRemove) must be "
                    + "invoked at least twice (once by removeVector's direct "
                    + "iteration, once by the reapply over finalNewSegments). "
                    + "Got " + hits + " invocations; expected >= 2.",
                    hits >= 2);
        }
    }

    /**
     * Pr-reviewer pass-5 follow-up: symmetric compaction-side test for
     * the reapply-covers-adopted-segments invariant. Mirrors
     * {@link #stage2ReapplyMustCoverAdoptedSegmentsForPendingCheckpointDeletes}
     * but exercises {@code atomicSwapCompactionResult} Stage 3 with
     * {@code pendingCompactionDeletes} (the compaction-cycle counterpart
     * to {@code pendingCheckpointDeletes}).
     *
     * <p>Pre-fix-5 the Stage 3 reapply targeted ONLY {@code mergedOutput}
     * — a {@code removeVector(P)} that ran during the cycle, followed
     * by an {@code adoptExternalSegment} landing in the Stage 1 →
     * Stage 3 window, would leave {@code P} live in the adopted
     * segment after the cycle. Post-fix-5 the reapply iterates the
     * entire {@code finalNewSegments} (currentAtStage3 - inputs +
     * mergedOutput), so the adopted segment receives the pending
     * delete and the {@code TrackingVectorSegment} observes the
     * invocation.
     */
    @Test(timeout = 30_000)
    public void stage3CompactionReapplyMustCoverAdoptedSegmentsForPendingCompactionDeletes()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-compaction-reapply").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            stampMissingUuids(initial);

            final Bytes pkToRemove = Bytes.from_int(999_999_003);
            final TrackingVectorSegment tracker = new TrackingVectorSegment(1_000_000_005);
            tracker.segmentUuid = UUID.randomUUID().toString();
            tracker.generation = 9999L;

            // Use atomicSwapPostBuildHook (fires AFTER Stage 1 build,
            // BEFORE Stage 2 persist). Order matters: removeVector FIRST
            // so the pk lands in pendingCompactionDeletes without
            // touching the tracker (tracker not yet injected); then
            // inject the tracker so Stage 3's reapply iterates it.
            store.setAtomicSwapPostBuildHookForTest(() -> {
                try {
                    store.removeVector(pkToRemove);
                    addSegmentViaReflection(store, tracker);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });

            store.runCompactionCycle();

            // Precondition: the tracker is in the published segments
            // (the round-1 fix preserves adoptions through Stage 3).
            boolean trackerInSegments = false;
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                if (s.segmentId == tracker.segmentId) {
                    trackerInSegments = true;
                    break;
                }
            }
            assertTrue("tracker must be in this.segments post-publish "
                    + "(round-1 fix preserves adoptions)", trackerInSegments);

            // CORE assertion: the tracker's deletePk(pkToRemove) MUST
            // have been called at least once by the Stage-3 reapply.
            // (We never invoked it via removeVector — the tracker was
            // injected AFTER removeVector.) Pre-fix-5 the reapply
            // targeted only mergedOutput, so the tracker would NOT see
            // pkToRemove. Post-fix-5 the reapply iterates
            // finalNewSegments which includes the tracker.
            int hits = 0;
            for (Bytes b : tracker.deletePkInvocations) {
                if (pkToRemove.equals(b)) {
                    hits++;
                }
            }
            assertTrue("Stage-3 compaction reapply must iterate adopted "
                    + "segments — tracker.deletePk(pkToRemove) must be "
                    + "invoked at least once by the reapply over "
                    + "finalNewSegments. Got " + hits + " invocations; "
                    + "expected >= 1. Pre-fix-5 the reapply targeted "
                    + "only mergedOutput, leaving pendingCompactionDeletes "
                    + "unapplied to adopted segments (issue #538 pr-reviewer "
                    + "pass-5).",
                    hits >= 1);
        }
    }

    /**
     * Pr-reviewer pass-4 follow-up: locks in the "reapply → register →
     * publish" ordering invariant in Phase C Stage 2. If
     * {@code seg.deletePk} throws during the reapply, the failure must
     * unwind cleanly: {@code onDiskSegmentsEstimatedMemoryBytes}
     * unchanged (preloadedSegments NOT yet registered) and
     * {@code this.segments} unchanged (publish did not happen).
     *
     * <p>Pre-fix-4 the order was "register → reapply → publish", so a
     * reapply throw left the counter incremented for every preloaded
     * segment while {@code this.segments} still pointed at the OLD
     * list — a half-updated state. Post-fix-4 the register runs AFTER
     * the reapply, so a reapply throw never advances the counter.
     */
    @Test(timeout = 60_000)
    public void stage2ReapplyThrowMustNotLeavePartialCounterUpdate()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue538-reapply-throw").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 3);

            final Bytes pkToRemove = Bytes.from_int(999_999_002);
            final ThrowingVectorSegment thrower = new ThrowingVectorSegment(1_000_000_004);
            thrower.segmentUuid = UUID.randomUUID().toString();
            thrower.generation = 9999L;

            store.setPhaseCPostDeletesApplyHookForTest(() -> {
                try {
                    // Order matters: call removeVector FIRST so the pk is
                    // added to pendingCheckpointDeletes (removeVector also
                    // iterates this.segments — at this point thrower is
                    // NOT yet injected, so removeVector does not hit the
                    // thrower). Then inject thrower so Stage 2's reapply
                    // will iterate it and trigger the throw.
                    store.removeVector(pkToRemove);
                    addSegmentViaReflection(store, thrower);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });

            long counterBefore = store.getOnDiskSegmentsEstimatedMemoryBytes();
            List<VectorSegment> segmentsBefore = store.getOnDiskSegmentsSnapshotForTest();

            // Trigger checkpoint. The Phase C Stage 2 reapply will call
            // thrower.deletePk(pk) and throw. The checkpoint must
            // propagate the exception, but with the post-fix-4 ordering
            // the counter and this.segments are unchanged.
            store.addVector(Bytes.from_int(6666), vec(new Random(6666)));
            boolean checkpointThrew = false;
            try {
                store.checkpoint();
            } catch (Exception e) {
                checkpointThrew = true;
            }
            assertTrue("checkpoint must propagate the synthetic"
                    + " thrower's RuntimeException from the reapply path",
                    checkpointThrew);

            // CORE assertion: the counter did NOT advance during the
            // failed Stage 2. This proves the register-after-reapply
            // ordering is in effect.
            assertEquals("on-disk-segments memory counter must be unchanged"
                    + " when the Stage-2 reapply throws — preloadedSegments"
                    + " must NOT have been registered before the reapply"
                    + " (post-fix-4 ordering: reapply → register → publish)",
                    counterBefore, store.getOnDiskSegmentsEstimatedMemoryBytes());

            // this.segments did not advance either: the publish was after
            // the (failed) reapply.
            List<VectorSegment> segmentsAfter = store.getOnDiskSegmentsSnapshotForTest();
            // The thrower was injected via reflection, so it IS in the
            // current segments list (we don't undo that). But the
            // preloaded segments (from this checkpoint's Phase B) must
            // NOT have been published — verify by checking the size
            // didn't grow past `before.size() + 1` (the +1 is the
            // reflection-injected thrower; the preloaded segment would
            // be +2).
            assertEquals("this.segments must not include the preloaded segment"
                    + " when the Stage-2 reapply threw (publish did not run)",
                    segmentsBefore.size() + 1, segmentsAfter.size());
        }
    }
}
