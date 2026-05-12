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
            store.runCompactionCycle();

            // Disarm the hook so subsequent cycles run normally.
            store.setAtomicSwapPostPersistHookForTest(null);

            // Second cycle: must NOT trip the no-on-disk-graph check.
            // Pre-fix this is where issue #538's persistent failure loop
            // would start; post-fix the cycle either compacts the remaining
            // healthy segments or exits with no candidates — either way no
            // CORRUPTION is recorded.
            store.runCompactionCycle();

            assertEquals(
                "no CORRUPTION on the cycle following the race — that was"
                + " the persistent-failure loop reported in issue #538",
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
}
