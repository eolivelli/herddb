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
 * Regression test for issue #535: the IS-local fallback compaction
 * repeatedly fails with
 *
 * <pre>
 *   CompactionException: candidate segment N has no on-disk graph (streaming compaction)
 *       at VectorIndexCompactor.rebuildSegmentStreaming(VectorIndexCompactor.java:782)
 * </pre>
 *
 * on the SAME segment for tens of minutes during a bigann-100M k3s
 * benchmark with the optimizer enabled.
 *
 * <h2>Root cause (pre-fix)</h2>
 *
 * {@link PersistentVectorStore#runCompactionCycle} snapshots
 * {@code this.segments} at the start of a cycle <b>without holding
 * {@link PersistentVectorStore#stateLock}'s readLock</b>. The cycle then
 * iterates the snapshot in
 * {@link VectorIndexCompactor#rebuildSegmentStreaming} reading
 * {@code seg.onDiskGraph}. If a concurrent
 * {@link PersistentVectorStore#dropSegmentByUuid} call lands between the
 * snapshot and the iteration (fired by the optimizer-watcher's
 * {@code onSegmentReleased} when the external optimizer deprecates an
 * IS-produced segment), it
 *
 * <ol>
 *   <li>removes the segment from {@code this.segments} under the writeLock,
 *       and</li>
 *   <li>calls {@code found.close()} on the segment <b>while the writeLock
 *       is still held</b> — which sets {@code seg.onDiskGraph = null}
 *       (the only place that field is ever assigned to {@code null},
 *       per {@link VectorSegment#close} line 340).</li>
 * </ol>
 *
 * The compaction's snapshot still holds a reference to the (now closed)
 * {@code VectorSegment} object. When
 * {@link VectorIndexCompactor#rebuildSegmentStreaming} iterates the
 * candidates, it sees {@code seg.onDiskGraph == null} and throws
 * {@code CompactionException(FailureReason.CORRUPTION,
 *   "candidate segment N has no on-disk graph (streaming compaction)")}.
 *
 * <h2>The fix</h2>
 *
 * {@link PersistentVectorStore#dropSegmentByUuid} no longer closes the
 * segment synchronously. It removes the segment from {@code this.segments}
 * under the writeLock and enqueues the {@code VectorSegment} on
 * {@code pendingSegmentCloses}. The actual {@link VectorSegment#close}
 * call is deferred to either
 *
 * <ul>
 *   <li>an opportunistic drain right after the writeLock release (fast
 *       path: no concurrent compaction → close immediately), or</li>
 *   <li>the {@link PersistentVectorStore#runCompactionCycle}'s finally
 *       block (after the cycle's snapshot is no longer being iterated).</li>
 * </ul>
 *
 * Both drain paths hold {@code compactionLock} so the close can never
 * happen while a cycle is iterating its candidate list.
 *
 * <h2>What this test proves</h2>
 *
 * <ol>
 *   <li>A {@code dropSegmentByUuid} call fired DURING a compaction cycle
 *       (after candidates are picked, before the on-disk-graph iteration
 *       starts) <b>no longer corrupts the cycle's snapshot</b>. The
 *       cycle does not throw {@code CompactionException(CORRUPTION,
 *       "no on-disk graph (streaming compaction)")}.</li>
 *   <li>The cycle still surfaces the drift cleanly:
 *       {@link VectorIndexCompactor.FailureReason#ABORTED_INPUT_GONE}
 *       fires from {@code atomicSwapCompactionResult} when it discovers
 *       the dropped segment is missing from {@code this.segments} —
 *       which is the correct, documented response to "an input segment
 *       disappeared under us".</li>
 *   <li>The dropped segment's resources are still released — the
 *       deferred close runs in the cycle's finally block, so
 *       {@code victim.onDiskGraph} is observed to be {@code null} AFTER
 *       the cycle completes.</li>
 *   <li>The baseline (no race) cycle is unaffected: it still merges all
 *       candidates and produces one merged output.</li>
 * </ol>
 */
public class Issue535DropSegmentByUuidRaceTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 16;

    @Before
    public void disableDeferral() {
        // Allow checkpoints to seal small live shards into on-disk segments
        // even with very few vectors per shard — keeps the test fast.
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
                Long.MAX_VALUE, // compactionIntervalMs — disable the auto loop
                VectorSimilarityFunction.EUCLIDEAN);
        // Aggressive policy: 2+ segments → merge. Min bytes = 1 so every cycle
        // qualifies regardless of segment size.
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                Integer.MAX_VALUE, 0);
        return store;
    }

    /** Seeds {@code n} on-disk segments via add+checkpoint cycles. */
    private void seedSegments(PersistentVectorStore store, int n) throws Exception {
        Random rng = new Random(42);
        for (int c = 0; c < n; c++) {
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(c * 1000 + i), vec(rng));
            }
            store.checkpoint();
        }
    }

    /**
     * Stamp a UUID on every segment that does not already carry one. With the
     * publisher disabled (no {@link herddb.index.vector.SegmentPublisher}
     * wired into the store) {@code segmentUuid} is not populated by the
     * normal compaction/checkpoint paths, so the test pre-stamps the field
     * directly via package-private access — exactly mirroring what
     * production sees once the optimizer is enabled and segments get
     * stamped via {@code atomicSwapCompactionResult} (line 2588-2590).
     */
    private void stampMissingUuids(List<VectorSegment> segs) {
        for (VectorSegment seg : segs) {
            if (seg.segmentUuid == null) {
                seg.segmentUuid = UUID.randomUUID().toString();
            }
        }
    }

    /**
     * <b>The fix verification.</b> Runs a single compaction cycle and,
     * between the candidate selection and the on-disk graph iteration,
     * calls {@link PersistentVectorStore#dropSegmentByUuid} to drop one
     * of the candidate segments. Pre-fix, the cycle's iteration of
     * {@code candidates} surfaced {@code CompactionException(CORRUPTION,
     * "candidate segment N has no on-disk graph (streaming compaction)")}.
     * Post-fix, the close is deferred and the cycle either completes
     * normally (if the cycle's swap also drifts gracefully) or aborts
     * with {@link
     * VectorIndexCompactor.FailureReason#ABORTED_INPUT_GONE} —
     * <b>not</b> {@code CORRUPTION}.
     */
    @Test(timeout = 30_000)
    public void dropSegmentByUuidDuringCompactionMustNotCauseCorruption()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue535-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertTrue("setup must produce at least 2 on-disk segments — got "
                    + initial.size(), initial.size() >= 2);

            stampMissingUuids(initial);

            VectorSegment victim = initial.get(0);
            final String victimUuid = victim.segmentUuid;
            final int victimId = victim.segmentId;
            assertNotNull("victim must have a UUID stamped", victimUuid);
            assertNotNull("victim must have onDiskGraph populated before the race",
                    victim.onDiskGraph);

            // Wire the hook. It runs INSIDE runCompactionCycle, AFTER the
            // candidate list is built, BEFORE rebuildSegment reads
            // seg.onDiskGraph. Synchronously dropping the victim's UUID
            // simulates the optimizer-watcher race observed in production.
            store.setCompactionPostCandidateSelectionHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            long abortedBefore = store.getCompactionFailuresAbortedInputGoneTotal();

            // Run the cycle under the race.
            store.runCompactionCycle();

            // ---------- Post-fix assertions ----------

            // The victim was removed from `this.segments` under the writeLock
            // before the hook returned, so the post-cycle list must not
            // contain it.
            List<VectorSegment> after = store.getOnDiskSegmentsSnapshotForTest();
            for (VectorSegment s : after) {
                assertFalse("victim must be removed from segments by dropSegmentByUuid",
                        s.segmentId == victimId);
            }

            // CORE INVARIANT (the fix): NO CORRUPTION failure must have
            // been recorded — the cycle's iteration of `candidates` saw a
            // non-null `onDiskGraph` on every candidate because the close
            // was deferred past the iteration window.
            assertEquals("no CORRUPTION failure must be recorded — that was "
                    + "issue #535's exact production failure mode",
                    corruptionBefore, store.getCompactionFailuresCorruptionTotal());

            // Drift-aware behaviour: the cycle's atomicSwapCompactionResult
            // Stage 1 detects that an input segment vanished and aborts
            // with ABORTED_INPUT_GONE. This is the EXPECTED, documented
            // outcome of a race between compaction and segment retirement,
            // not a regression. (If multiple races accumulate, the counter
            // increases by at least one per drift event.)
            assertTrue(
                    "either compaction succeeded (drift detected pre-rebuild) or "
                    + "aborted with ABORTED_INPUT_GONE (drift detected at swap); "
                    + "both are documented graceful outcomes, neither is "
                    + "CORRUPTION (issue #535)",
                    store.getCompactionFailuresAbortedInputGoneTotal() > abortedBefore
                    || store.getCompactionSuccessesTotal() > 0);

            // The deferred close ran in the cycle's finally block: by the
            // time runCompactionCycle returned, the victim's onDiskGraph
            // had been nulled out (resources were released).
            assertNull("deferred close must have run in runCompactionCycle's finally — "
                    + "victim.onDiskGraph should be null after the cycle ends",
                    victim.onDiskGraph);
        }
    }

    /**
     * Companion regression test: same scenario but the victim is one of
     * EXACTLY 2 candidates the policy can match (the minimum count for a
     * streaming merge). Pre-fix this also produced
     * {@code CompactionException(CORRUPTION)}; post-fix the cycle must
     * surface only ABORTED_INPUT_GONE (or succeed) — never CORRUPTION.
     */
    @Test(timeout = 30_000)
    public void dropDuringTwoCandidateCycleMustNotCauseCorruption() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue535-race-two-candidates").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            // Seed exactly 2 segments — the minimum for streaming compaction
            // to attempt a real merge (fewer than 2 falls back to the legacy
            // in-memory path which has the SAME null-check but a different
            // error message).
            seedSegments(store, 2);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertEquals(2, initial.size());
            stampMissingUuids(initial);

            VectorSegment victim = initial.get(0);
            final String victimUuid = victim.segmentUuid;

            store.setCompactionPostCandidateSelectionHookForTest(() -> {
                store.dropSegmentByUuid(victimUuid);
            });

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            store.runCompactionCycle();
            long corruptionAfter = store.getCompactionFailuresCorruptionTotal();

            assertEquals(
                "race must NOT produce a CORRUPTION failure (issue #535 was the "
                + "candidate segment N has no on-disk graph error)",
                corruptionBefore, corruptionAfter);

            assertNull("deferred close must have run by the time runCompactionCycle returned",
                    victim.onDiskGraph);
        }
    }

    /**
     * Without the race (hook is a no-op) the same setup must NOT produce a
     * CORRUPTION failure AND must complete the compaction successfully —
     * proves the fix does not regress the happy path.
     */
    @Test(timeout = 30_000)
    public void noRaceMeansSuccessfulCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue535-no-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            stampMissingUuids(initial);

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();
            store.runCompactionCycle();
            long corruptionAfter = store.getCompactionFailuresCorruptionTotal();

            assertEquals("baseline compaction must NOT produce CORRUPTION failures",
                    corruptionBefore, corruptionAfter);
            assertEquals("baseline compaction must complete successfully",
                    1L, store.getCompactionSuccessesTotal());
        }
    }

    /**
     * Verifies the opportunistic-drain path: when {@code dropSegmentByUuid}
     * is called while NO compaction is running, the segment is closed
     * synchronously (preserving the original "close immediately when
     * safe" behaviour for the common idle-IS case).
     */
    @Test(timeout = 30_000)
    public void dropWhileIdleClosesSegmentSynchronously() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue535-drop-idle").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 3);

            List<VectorSegment> initial = store.getOnDiskSegmentsSnapshotForTest();
            assertTrue(initial.size() >= 1);
            stampMissingUuids(initial);

            VectorSegment victim = initial.get(0);
            assertNotNull("victim must have onDiskGraph populated before the drop",
                    victim.onDiskGraph);

            store.dropSegmentByUuid(victim.segmentUuid);

            // No compaction running → opportunistic drain succeeded
            // synchronously inside dropSegmentByUuid. The victim's
            // onDiskGraph must already be null.
            assertNull("idle drop must close the segment synchronously via the "
                    + "opportunistic drain",
                    victim.onDiskGraph);

            // Sanity: the segment is also removed from `segments`.
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                assertFalse(s.segmentId == victim.segmentId);
            }
        }
    }
}
