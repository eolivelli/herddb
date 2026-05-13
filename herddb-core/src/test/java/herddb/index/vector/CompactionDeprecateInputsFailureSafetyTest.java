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

            // (A) The compaction must have actually succeeded (the throw is
            //     in the post-swap best-effort block, NOT in the swap proper).
            //     If the compactor itself didn't run, the test isn't
            //     exercising the right path.
            assertEquals("compaction must have completed successfully",
                    1, store.getCompactionSuccessesTotal());
            assertTrue("segment count must shrink after a successful merge — "
                            + "before=" + segmentsBefore
                            + ", after=" + store.getSegmentCount(),
                    store.getSegmentCount() < segmentsBefore);

            // (B) deprecateInputs must have been called (and thrown).
            //     If it wasn't called, the test path is broken — the
            //     compaction took a different branch.
            assertTrue("publisher.deprecateInputs must have been called at"
                            + " least once (deprecate-on-merge path)",
                    publisher.deprecateInputsCalls.get() >= 1);

            // (C) THE FIX: pendingDeletes must be empty. Pre-fix it would
            //     hold at least 2 entries per input segment (graph + map);
            //     post-fix it stays empty because the deprecate throw
            //     prevents the queue.
            //
            //     A non-empty pendingDeletes here is the precise zombie-
            //     creation hazard: the next reapExpiredPendingDeletes pass
            //     after the retention deadline would physically delete the
            //     input files while ZK still shows them as ACTIVE.
            List<PersistentVectorStore.PendingDelete> pending =
                    store.getPendingDeletesSnapshot();
            assertEquals("pendingDeletes must be empty when deprecateInputs"
                            + " failed (issue #551 root cause B). Found entries: "
                            + pending,
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
            assertTrue("deprecateInputs must have been called",
                    publisher.deprecateInputsCalls.get() >= 1);

            // Happy path: pendingDeletes must hold entries for the input
            // files (graph + map per input). Without this, the fix would
            // be over-correct and leak files in steady state.
            List<PersistentVectorStore.PendingDelete> pending =
                    store.getPendingDeletesSnapshot();
            assertTrue("pendingDeletes must hold the swapped-out input files"
                            + " when deprecateInputs succeeds — got " + pending.size(),
                    pending.size() >= 2);
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
}
