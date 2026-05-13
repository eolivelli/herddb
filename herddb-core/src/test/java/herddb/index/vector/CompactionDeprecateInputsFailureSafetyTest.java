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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #551 (Root Cause B): zombie segments caused
 * by the retention reaper deleting input files while ZK still shows them
 * as ACTIVE.
 *
 * <p>Pre-fix code in {@link PersistentVectorStore#atomicSwapCompactionResult}
 * queued input segments for retention-aware deletion in Stage 1 (BEFORE
 * {@code persistIndexStatusMultiSegment}), independent of whether the
 * post-swap {@code deprecateInputs} call subsequently succeeded. If
 * {@code deprecateInputs} threw (e.g. transient ZK error), the inputs
 * remained ACTIVE in ZK but the retention reaper would still physically
 * delete their files after the retention deadline elapsed —
 * the textbook zombie-segment failure mode.
 *
 * <p>Post-fix, {@code queueSegmentPendingDelete} runs <em>only</em> in
 * the post-swap block when {@code deprecateInputs} succeeds. If
 * {@code deprecateInputs} throws, the inputs are NOT queued: their
 * files stay in MinIO as orphans (intact but unreferenced by
 * IndexStatus), which is strictly safer than a zombie.
 *
 * <p>This test installs a {@link SegmentPublisher} that throws on
 * {@code deprecateInputs} and asserts that {@code pendingDeletes} stays
 * empty after a successful local compaction merge.
 */
public class CompactionDeprecateInputsFailureSafetyTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * THE FIX: when {@code deprecateInputs} throws, input segment files
     * must NOT be added to {@code pendingDeletes}. Without this guard the
     * retention reaper deletes them after the retention deadline while
     * ZK still shows them as ACTIVE → zombie.
     */
    @Test(timeout = 30_000)
    public void deprecateInputsFailureMustNotQueueInputFilesForDeletion() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue551-deprecateInputs").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        FailingDeprecatePublisher publisher = new FailingDeprecatePublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();

            Random rng = new Random(551);
            int dim = 16;
            // Produce enough segments to give the compactor candidates.
            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must produce >= 2 segments to allow a merge",
                    segmentsBefore >= 2);

            // Pre-condition: pendingDeletes is empty at this point. The
            // checkpoints above did NOT call queueSegmentPendingDelete on
            // any segment (queue happens only via compaction, not via plain
            // checkpoint).
            assertEquals("baseline: no pending deletes before compaction",
                    0, store.getPendingDeletesSnapshot().size());

            // Run a compaction cycle. atomicSwapCompactionResult will:
            //   - stage merged output PROVISIONAL (publisher.stageNewSegments — no-op)
            //   - revalidate inputs ACTIVE (publisher.revalidateInputsActive — true)
            //   - persist IndexStatus (Stage 2 — succeeds)
            //   - swap this.segments (Stage 3 — succeeds)
            //   - commit merged output ACTIVE (publisher.commitStagedSegments — no-op)
            //   - deprecate inputs (publisher.deprecateInputs — THROWS)
            //
            // Pre-fix: the Stage-1 queueSegmentPendingDelete loop has already
            // populated pendingDeletes; the deprecateInputs throw is logged
            // but the queued entries remain → zombie hazard.
            // Post-fix: queueSegmentPendingDelete runs ONLY after
            // deprecateInputs succeeds, so a throw here leaves pendingDeletes
            // empty.
            store.runCompactionCycle();

            // ----- Post-fix assertions -----

            // (A.1) The compactor must have SELECTED candidates this cycle.
            // pr-reviewer pass on #552: without this, a regression where
            // configureCompaction() parameters silently stop matching the
            // candidates would degrade the test into "compaction did nothing,
            // pendingDeletes is empty for the wrong reason" and the
            // assertion below would pass for an unrelated reason.
            assertEquals("compaction must have run exactly one cycle",
                    1, store.getCompactionRunsTotal());
            assertTrue("compactor must have selected >= 2 inputs (got "
                            + store.getCompactionLastInputSegments() + ")",
                    store.getCompactionLastInputSegments() >= 2);

            // (A.2) The compaction must have actually succeeded (the throw is
            //       in the post-swap best-effort block, NOT in the swap
            //       proper). If the compactor itself didn't run, the test
            //       isn't exercising the right path.
            assertEquals("compaction must have completed successfully",
                    1, store.getCompactionSuccessesTotal());
            assertTrue("segment count must shrink after a successful merge — "
                            + "before=" + segmentsBefore
                            + ", after=" + store.getSegmentCount(),
                    store.getSegmentCount() < segmentsBefore);

            // (B) deprecateInputs must have been called (and thrown).
            //     If it wasn't called, the test path is broken — the
            //     compaction took a different branch.
            assertEquals("publisher.deprecateInputs must have been called"
                            + " exactly once (deprecate-on-merge path)",
                    1, publisher.deprecateInputsCalls.get());

            // (C) THE FIX: pendingDeletes must be empty. Pre-fix it would
            //     hold exactly 2 entries per input segment (graph + map);
            //     post-fix it stays empty because the deprecate throw
            //     prevents the queue.
            //
            //     A non-empty pendingDeletes here is the precise zombie-
            //     creation hazard: the next reapExpiredPendingDeletes pass
            //     after the retention deadline would physically delete the
            //     input files while ZK still shows them as ACTIVE.
            List<PersistentVectorStore.PendingDelete> pending =
                    store.getPendingDeletesSnapshot();
            assertEquals("pendingDeletes must be EXACTLY empty when"
                            + " deprecateInputs failed (issue #551 root cause B)."
                            + " Found entries: " + pending,
                    0, pending.size());
        }
    }

    /**
     * THE COMPLEMENTARY CASE: when {@code deprecateInputs} succeeds, input
     * segment files MUST be queued for retention-aware deletion. This
     * pins the fix on the happy path so a future change cannot
     * accidentally over-correct and leak files in the steady state.
     */
    @Test(timeout = 30_000)
    public void deprecateInputsSuccessQueuesInputFilesForDeletion() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue551-deprecateInputs-ok").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        SuccessfulPublisher publisher = new SuccessfulPublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();

            Random rng = new Random(552);
            int dim = 16;
            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must produce >= 2 segments to allow a merge",
                    segmentsBefore >= 2);
            assertEquals(0, store.getPendingDeletesSnapshot().size());

            store.runCompactionCycle();

            assertEquals(1, store.getCompactionSuccessesTotal());
            assertTrue("segment count must shrink",
                    store.getSegmentCount() < segmentsBefore);
            assertEquals("deprecateInputs must have been called exactly once",
                    1, publisher.deprecateInputsCalls.get());

            // Compactor must have actually selected candidates this cycle.
            long selected = store.getCompactionLastInputSegments();
            assertTrue("compactor must have selected >= 2 inputs (got "
                            + selected + ")",
                    selected >= 2);

            // Happy path: pendingDeletes must hold EXACTLY 2 entries per
            // selected input (graph + map). pr-reviewer pass on #552: the
            // previous `>= 2` assertion was too loose — a regression that
            // queued only the graph (or only the first input) would still
            // pass. The deterministic expectation is `2 * selected`.
            List<PersistentVectorStore.PendingDelete> pending =
                    store.getPendingDeletesSnapshot();
            assertEquals("pendingDeletes must hold EXACTLY 2 entries per"
                            + " selected input (graph + map) when deprecateInputs"
                            + " succeeds — selected=" + selected
                            + ", pending=" + pending,
                    2L * selected, (long) pending.size());
        }
    }

    /**
     * pr-reviewer pass on #552 follow-up #2: empty-result compaction
     * (rebuild returns {@code null}) with a publisher attached must NOT
     * queue input files for deletion, because {@code deprecateInputs}
     * was not called on them (ZK still shows ACTIVE). Pre-fix this
     * branch fell through to the `else` and queued the files — same
     * zombie-segment hazard as the {@code deprecateInputs}-fails case.
     *
     * <p>The test forces every PK to be tombstoned before running the
     * compactor, so {@code VectorIndexCompactor.rebuildSegment} returns
     * {@code null} and the IS takes the empty-result branch in
     * {@code runCompactionCycle}.
     */
    @Test(timeout = 30_000)
    public void emptyResultCompactionWithPublisherMustNotQueueInputsForDeletion()
            throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue551-emptyResult").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        TrackingPublisher publisher = new TrackingPublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();

            Random rng = new Random(553);
            int dim = 16;
            java.util.List<Bytes> allPks = new java.util.ArrayList<>();
            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 200; i++) {
                    Bytes pk = Bytes.from_int(c * 10_000 + i);
                    allPks.add(pk);
                    store.addVector(pk, vec(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must produce >= 2 segments",
                    segmentsBefore >= 2);
            assertEquals(0, store.getPendingDeletesSnapshot().size());

            // Tombstone EVERY PK so the rebuild produces an empty result.
            for (Bytes pk : allPks) {
                store.removeVector(pk);
            }

            // Compaction must take the empty-result branch (rebuild ==
            // null) → atomicSwapCompactionResult is called with
            // mergedOutput == null → stagedInfo == null → the new
            // `else if (stagedInfo == null)` branch fires.
            store.runCompactionCycle();

            // The compactor ran and succeeded (empty-result is still a
            // success — the inputs were swapped out).
            assertEquals(1, store.getCompactionRunsTotal());
            assertEquals(1, store.getCompactionSuccessesTotal());
            assertEquals("empty-result compaction must produce zero output"
                            + " segments",
                    0, store.getCompactionLastOutputSegments());
            long selected = store.getCompactionLastInputSegments();
            assertTrue("empty-result compactor must have selected >= 2"
                            + " inputs (got " + selected + ")",
                    selected >= 2);

            // Publisher contract: deprecateInputs was NEVER called (the
            // empty-result path skips the stage/commit/deprecate sequence
            // entirely because mergedOutput == null).
            assertEquals("publisher.deprecateInputs must NOT have been"
                            + " called on the empty-result path",
                    0, publisher.deprecateInputsCalls.get());

            // THE FIX: pendingDeletes must be empty. Pre-fix the `else`
            // branch would have flipped inputsSafeToDelete = true and
            // queued the input files even though their ZK znodes were
            // never deprecated → zombie hazard.
            List<PersistentVectorStore.PendingDelete> pending =
                    store.getPendingDeletesSnapshot();
            assertEquals("pendingDeletes must be empty on the empty-result"
                            + " path with publisher attached (issue #551"
                            + " pr-reviewer follow-up #1). Found entries: "
                            + pending,
                    0, pending.size());
        }
    }

    /**
     * Minimal {@link SegmentPublisher} that always throws on
     * {@code deprecateInputs}. Other methods are best-effort no-ops:
     * stage/commit return cleanly (mirroring the ZK fast path) and
     * revalidate returns {@code true} so the local compactor doesn't
     * abort the merge for a different reason.
     */
    private static final class FailingDeprecatePublisher implements SegmentPublisher {

        final AtomicInteger deprecateInputsCalls = new AtomicInteger();

        @Override
        public void stageNewSegments(List<NewSegmentInfo> segments) {
            // no-op: success
        }

        @Override
        public void commitStagedSegments(List<NewSegmentInfo> segments) {
            // no-op: success
        }

        @Override
        public boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
            return true;
        }

        @Override
        public void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                    long retentionUntilEpochMillis) {
            deprecateInputsCalls.incrementAndGet();
            throw new RuntimeException(
                    "test-injected deprecateInputs failure (issue #551 root cause B)");
        }
    }

    /** Companion publisher that always succeeds (used by the happy-path test). */
    private static final class SuccessfulPublisher implements SegmentPublisher {

        final AtomicInteger deprecateInputsCalls = new AtomicInteger();

        @Override
        public void stageNewSegments(List<NewSegmentInfo> segments) {
        }

        @Override
        public void commitStagedSegments(List<NewSegmentInfo> segments) {
        }

        @Override
        public boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
            return true;
        }

        @Override
        public void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                    long retentionUntilEpochMillis) {
            deprecateInputsCalls.incrementAndGet();
            // no throw — happy path
        }
    }

    /**
     * Tracking publisher used by the empty-result-compaction test: counts
     * every relevant call so the test can assert that {@code deprecateInputs}
     * was NEVER invoked on that path.
     */
    private static final class TrackingPublisher implements SegmentPublisher {

        final AtomicInteger stageCalls = new AtomicInteger();
        final AtomicInteger commitCalls = new AtomicInteger();
        final AtomicInteger deprecateInputsCalls = new AtomicInteger();

        @Override
        public void stageNewSegments(List<NewSegmentInfo> segments) {
            stageCalls.incrementAndGet();
        }

        @Override
        public void commitStagedSegments(List<NewSegmentInfo> segments) {
            commitCalls.incrementAndGet();
        }

        @Override
        public boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
            return true;
        }

        @Override
        public void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                    long retentionUntilEpochMillis) {
            deprecateInputsCalls.incrementAndGet();
        }
    }
}
