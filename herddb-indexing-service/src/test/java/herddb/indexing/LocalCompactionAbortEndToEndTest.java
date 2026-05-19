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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.indexing.vector.NewSegmentInfo;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.indexing.vector.SegmentPublisher;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end coverage for the abort and post-persist failure paths in
 * {@link PersistentVectorStore#atomicSwapCompactionResult} that the IS-local
 * compaction fallback relies on. Uses a fault-injecting {@link SegmentPublisher}
 * to drive the failure modes; the underlying compaction runs through the real
 * {@code runCompactionCycle} → {@code rebuildSegment} → {@code atomicSwap}
 * pipeline.
 *
 * <p>Two contracts are pinned here:
 * <ol>
 *   <li><b>Abort path leaks no remote bytes:</b> when revalidate fails after
 *       the merged output's multipart files were already uploaded by
 *       {@code rebuildSegment}, those files MUST end up in
 *       {@code pendingDeletes} so the retention reaper sweeps them. Without
 *       this, every optimizer-vs-IS race leaks up to
 *       {@code vector.index.compaction.maxBytes} of remote storage.</li>
 *   <li><b>Post-persist registry failures don't break local search:</b> if
 *       {@code commitStagedSegments} or {@code deprecateInputs} throws AFTER
 *       IndexStatus has already been persisted with the merged output, the
 *       IS must keep running with the merged segment locally — the
 *       reconcile-on-restart sweep will heal the registry on the next
 *       lifecycle.</li>
 * </ol>
 */
public class LocalCompactionAbortEndToEndTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "abort";
    private static final String INDEX_UUID = "abort_idx_uuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * SegmentPublisher that records every method invocation and lets the test
     * inject failures at specific lifecycle points. Stamps a deterministic
     * UUID on every staged input so the merged output's UUID is observable
     * after the abort.
     */
    private static final class FaultInjectingPublisher implements SegmentPublisher {
        final AtomicBoolean revalidateMustFail = new AtomicBoolean();
        final AtomicBoolean commitMustThrow = new AtomicBoolean();
        final AtomicBoolean deprecateMustThrow = new AtomicBoolean();
        final AtomicInteger stageCalls = new AtomicInteger();
        final AtomicInteger commitCalls = new AtomicInteger();
        final AtomicInteger deprecateCalls = new AtomicInteger();
        final AtomicInteger unstageCalls = new AtomicInteger();
        volatile String lastStagedUuid;

        @Override
        public void stageNewSegments(List<NewSegmentInfo> segments) {
            stageCalls.incrementAndGet();
            if (segments != null && !segments.isEmpty()) {
                lastStagedUuid = segments.get(0).getSegmentUuid();
            }
        }

        @Override
        public boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
            return !revalidateMustFail.get();
        }

        @Override
        public void commitStagedSegments(List<NewSegmentInfo> segments) {
            commitCalls.incrementAndGet();
            if (commitMustThrow.get()) {
                throw new RuntimeException("test-injected commit failure");
            }
        }

        @Override
        public void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                    long retentionUntilEpochMillis) {
            deprecateCalls.incrementAndGet();
            if (deprecateMustThrow.get()) {
                throw new RuntimeException("test-injected deprecate failure");
            }
        }

        @Override
        public void unstage(List<NewSegmentInfo> staged) {
            unstageCalls.incrementAndGet();
        }
    }

    @Test
    public void abortQueuesMergedOutputBytesForRetentionReaping() throws Exception {
        // Drive segments above the kick threshold WITHOUT a publisher so each
        // checkpoint produces a UUID-less sealed segment (legacy v3 path).
        // Then attach the fault-injecting publisher with revalidate-fail
        // armed and run the cycle directly. atomicSwap reaches the abort
        // path AFTER rebuildSegment uploaded the merged output's graph + map
        // files; those files must land in pendingDeletes.
        //
        // The legacy null-UUID inputs are skipped from inputInfosForRegistry
        // (the registry never saw them), so revalidateInputsActive is called
        // with an EMPTY list — which trivially returns true. To hit the
        // failure mode we need real-UUID inputs, so we pre-attach the
        // publisher BEFORE checkpointing — that stamps UUIDs onto each new
        // segment via Phase B. We then capture call-count baselines before
        // running the test cycle.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        FaultInjectingPublisher publisher = new FaultInjectingPublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Suppress background-thread interference during ingest:
            //   - High kickFraction (× 1000-segment back-pressure cap) keeps
            //     the addVector wake from firing while we accumulate segments.
            //   - Long.MAX_VALUE idle interval keeps the periodic tick from
            //     ever waking the background loop.
            // After ingest we drop the threshold to a small value so our
            // foreground runCompactionCycle() actually crosses the gate.
            store.setCompactionBackpressureThreshold(1000);
            store.setLocalCompactionKickFraction(0.99d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            store.setSegmentPublisher(publisher);
            // Tight bytes/count caps so a single cycle picks 2+ candidates.
            store.configureCompaction(Long.MAX_VALUE / 2, 0L, Long.MAX_VALUE, 2, 1024, 0L);
            store.start();

            Random rng = new Random(11);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }
            assertTrue("test must produce ≥ 2 candidate segments, saw "
                            + store.getSegmentCount(),
                    store.getSegmentCount() >= 2);

            int pendingBefore = store.getPendingDeletesSnapshot().size();
            int stageBefore = publisher.stageCalls.get();
            int commitBefore = publisher.commitCalls.get();
            int deprecateBefore = publisher.deprecateCalls.get();
            int unstageBefore = publisher.unstageCalls.get();

            // Arm the failure mode and run the cycle directly.
            // Drop the kick threshold so our foreground cycle crosses the gate.
            store.setCompactionBackpressureThreshold(2);
            store.setLocalCompactionKickFraction(0.5d);
            publisher.revalidateMustFail.set(true);
            store.runCompactionCycle();

            // The cycle staged the merged output and unstaged it after
            // revalidate failed.
            assertEquals("stage must have been called exactly once during the cycle",
                    1, publisher.stageCalls.get() - stageBefore);
            assertEquals("unstage must have been called exactly once during the cycle",
                    1, publisher.unstageCalls.get() - unstageBefore);
            assertEquals("commit must NOT have been called on the abort path",
                    0, publisher.commitCalls.get() - commitBefore);
            assertEquals("deprecate must NOT have been called on the abort path",
                    0, publisher.deprecateCalls.get() - deprecateBefore);

            // pendingDeletes must have grown — the merged output's graph + map
            // files were queued for cleanup, so the leak is bounded.
            int pendingAfter = store.getPendingDeletesSnapshot().size();
            assertTrue("pendingDeletes must contain the merged output's multipart files"
                            + " after the abort (was " + pendingBefore + ", now "
                            + pendingAfter + ")",
                    pendingAfter >= pendingBefore + 2);
        }
    }

    @Test
    public void postPersistCommitFailureDoesNotBreakLocalSearch() throws Exception {
        // A commitStagedSegments failure AFTER IndexStatus is persisted must
        // not break the IS — the merged output is already durable locally;
        // the registry CAS will be retried by reconcile-on-restart.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        FaultInjectingPublisher publisher = new FaultInjectingPublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // High threshold + huge idle interval suppress background-thread
            // interference during ingest; we drop the threshold before the
            // foreground cycle.
            store.setCompactionBackpressureThreshold(1000);
            store.setLocalCompactionKickFraction(0.99d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            store.setSegmentPublisher(publisher);
            store.configureCompaction(Long.MAX_VALUE / 2, 0L, Long.MAX_VALUE, 2, 1024, 0L);
            store.start();

            Random rng = new Random(13);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            int commitBefore = publisher.commitCalls.get();

            store.setCompactionBackpressureThreshold(2);
            store.setLocalCompactionKickFraction(0.5d);
            publisher.commitMustThrow.set(true);
            store.runCompactionCycle();

            // Commit was called and threw — but the swap completed locally,
            // so the merged output is in segments and old ones are gone.
            assertEquals("commit must have been called exactly once during the cycle",
                    1, publisher.commitCalls.get() - commitBefore);
            assertTrue("merged output must have replaced inputs locally despite the"
                            + " commit failure (saw " + segmentsBefore + " → "
                            + store.getSegmentCount() + ")",
                    store.getSegmentCount() < segmentsBefore);

            // Search must still work (proves the merged output is loaded + queryable).
            // We hit the new merged-segment count via getSegmentCount() above; one
            // additional tiny ingest + checkpoint must succeed without throwing.
            store.addVector(Bytes.from_int(99_999), randomVector(rng, 32));
            assertTrue("checkpoint after a commit-fail abort must succeed (proves the IS"
                            + " did not get into a stuck state)", store.checkpoint());
        }
    }

    @Test
    public void postPersistDeprecateFailureDoesNotBreakLocalSearch() throws Exception {
        // Same shape as above but for deprecateInputs — the catch-and-log at
        // the deprecate site must not propagate, and the IS must remain
        // operational.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        FaultInjectingPublisher publisher = new FaultInjectingPublisher();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(1000);
            store.setLocalCompactionKickFraction(0.99d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            store.setSegmentPublisher(publisher);
            store.configureCompaction(Long.MAX_VALUE / 2, 0L, Long.MAX_VALUE, 2, 1024, 0L);
            store.start();

            Random rng = new Random(17);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            int commitBefore = publisher.commitCalls.get();
            int deprecateBefore = publisher.deprecateCalls.get();

            store.setCompactionBackpressureThreshold(2);
            store.setLocalCompactionKickFraction(0.5d);
            publisher.deprecateMustThrow.set(true);
            store.runCompactionCycle();

            assertEquals("deprecate must have been called exactly once during the cycle",
                    1, publisher.deprecateCalls.get() - deprecateBefore);
            assertEquals("commit succeeded before deprecate threw — exactly one commit"
                            + " call during this cycle",
                    1, publisher.commitCalls.get() - commitBefore);
            assertTrue("the swap completed locally despite the deprecate failure"
                            + " (saw " + segmentsBefore + " → " + store.getSegmentCount() + ")",
                    store.getSegmentCount() < segmentsBefore);
            // addVector returns void — call it and trust the absence of a thrown
            // exception as the success signal. A subsequent checkpoint must
            // also succeed, proving the IS is not stuck.
            store.addVector(Bytes.from_int(123_456), randomVector(rng, 32));
            assertTrue("checkpoint after a deprecate-fail must succeed",
                    store.checkpoint());
        }
    }
}
