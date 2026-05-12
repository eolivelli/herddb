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
     * Coverage gap (pr-reviewer follow-up): exercises a drop landing during
     * Stage 2's slow {@code persistIndexStatusMultiSegment} I/O (using
     * {@code atomicSwapPostBuildHook}, which fires AFTER Stage 1 build but
     * BEFORE Stage 2 persist — i.e. the long lock-free window). The fix
     * must protect this window too, not just the immediate pre-Stage-3
     * window. Post-fix the dropped segment is NOT republished regardless
     * of which sub-window the drop landed in.
     */
    @Test(timeout = 30_000)
    public void dropDuringStage2PersistIsNotRepublishedByStage3() throws Exception {
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
        }
    }
}
