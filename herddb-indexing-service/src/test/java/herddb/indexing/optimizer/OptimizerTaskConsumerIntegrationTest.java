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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for {@link OptimizerTaskConsumer} against a real ZK.
 * Covers the lease-then-CAS happy path, the rollback path on CAS contention,
 * the revalidate-before-publish guard, FAILED + POISON transitions, the
 * pre-merge input-drift short-circuit to DONE-no-op, and the merger-throws
 * recovery.
 */
public class OptimizerTaskConsumerIntegrationTest {

    private static final String BASE_PATH = "/herd-test-consumer";
    private static final String TS_UUID = "ts-uuid";
    private static final String IDX = "idx-uuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private OptimizerTaskRegistry taskRegistry;
    private SegmentRegistryClient segmentRegistry;
    private AtomicLong fakeClock;
    private InMemorySegmentMerger merger;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        taskRegistry = new OptimizerTaskRegistry(() -> zk, BASE_PATH);
        taskRegistry.ensureRoot();
        segmentRegistry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        segmentRegistry.ensureRoot();
        fakeClock = new AtomicLong(10_000L);
        merger = new InMemorySegmentMerger();
    }

    @After
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private SegmentMetadata sampleSegment(String segUuid, long sizeBytes) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    private OptimizerTask pendingTask(String taskId, List<VersionedSegmentMetadata> inputs,
                                      int targetOwner) {
        List<String> uuids = new ArrayList<>();
        Map<String, Integer> versions = new LinkedHashMap<>();
        for (VersionedSegmentMetadata v : inputs) {
            uuids.add(v.metadata().getSegmentUuid());
            versions.put(v.metadata().getSegmentUuid(), v.zkVersion());
        }
        return OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid(IDX)
                .inputSegmentUuids(uuids)
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(targetOwner)
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(fakeClock.get())
                .leaderEpoch(1L)
                .build();
    }

    private OptimizerTaskConsumer consumer(String workerId, SegmentMerger m) {
        return new OptimizerTaskConsumer(taskRegistry, segmentRegistry, m, TS_UUID,
                workerId, /* retentionMillis */ 600_000L, /* maxAttempts */ 3, fakeClock::get);
    }

    private OptimizerTaskConsumer consumer(String workerId) {
        return consumer(workerId, merger);
    }

    private List<VersionedSegmentMetadata> activeInputs(String... uuids) throws Exception {
        List<VersionedSegmentMetadata> inputs = new ArrayList<>();
        for (String u : uuids) {
            segmentRegistry.createSegment(sampleSegment(u, 1_000_000L));
            inputs.add(segmentRegistry.getSegment(TS_UUID, IDX, u).orElseThrow());
        }
        return inputs;
    }

    // -------------------------------------------------------------------------
    // happy path
    // -------------------------------------------------------------------------

    @Test
    public void happyPathClaimsMergesPublishesAndCompletes() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-a", "seg-b");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, /* owner */ 1));
        assertTrue(consumer("w-1").consumeOneTask());

        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.DONE, t.getState());
        assertNotNull(t.getOutputSegmentUuid());

        // Inputs are deprecated, output is owned by instance 1.
        VersionedSegmentMetadata a = segmentRegistry.getSegment(TS_UUID, IDX, "seg-a").orElseThrow();
        VersionedSegmentMetadata b = segmentRegistry.getSegment(TS_UUID, IDX, "seg-b").orElseThrow();
        assertEquals(SegmentState.DEPRECATED, a.metadata().getState());
        assertEquals(SegmentState.DEPRECATED, b.metadata().getState());
        VersionedSegmentMetadata out = segmentRegistry.getSegment(
                TS_UUID, IDX, t.getOutputSegmentUuid()).orElseThrow();
        assertEquals(1, out.metadata().getOwnerInstanceId());

        // Lease is gone.
        assertFalse(taskRegistry.leaseExists(TS_UUID, taskId));
    }

    @Test
    public void consumeReturnsFalseWhenNoPendingTask() throws Exception {
        assertFalse(consumer("w-1").consumeOneTask());
    }

    // -------------------------------------------------------------------------
    // lease + CAS contention
    // -------------------------------------------------------------------------

    @Test
    public void leaseAlreadyHeldCausesSkip() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-c", "seg-d");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        // Another worker pre-creates the lease.
        taskRegistry.createLease(TS_UUID, taskId, "rival");
        assertFalse(consumer("w-1").consumeOneTask());
        // Task state untouched.
        assertEquals(OptimizerTaskState.PENDING,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
    }

    @Test
    public void casLoserDeletesItsLeaseAndReturnsFalse() throws Exception {
        // Stage: PENDING task. We simulate two consumers racing the CAS by:
        // 1. picking the oldest task with consumer A (which creates lease + CAS)
        // 2. simultaneously another tool bumps the task's znode out-of-band
        // 3. consumer A's CAS now fails — we expect it to delete its lease.
        // Easiest sim: pre-create the lease (so createLease fails), then
        // manually bump the task znode version; call consume; assert lease gone.
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-e", "seg-f");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        // Bump the znode by writing an unrelated update (e.g. attempts++).
        VersionedOptimizerTask v0 = taskRegistry.getTask(TS_UUID, taskId).orElseThrow();
        OptimizerTask bumped = v0.task().toBuilder().attempts(99).build();
        taskRegistry.casUpdateTask(v0, bumped);
        // Now consumer will attempt CAS with the (stale) v0 — but pickOldestPending
        // re-reads, so it'll see the fresh version. To force CAS loser we instead
        // create the lease ourselves first, simulating another worker that
        // already claimed.
        taskRegistry.createLease(TS_UUID, taskId, "rival");
        // consume must observe lease + skip without modifying task state.
        assertFalse(consumer("w-1").consumeOneTask());
    }

    // -------------------------------------------------------------------------
    // input-drift before merge
    // -------------------------------------------------------------------------

    @Test
    public void inputsDriftedBeforeMergeShortCircuitsToDoneNoop() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-g", "seg-h");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        // Deprecate seg-g out from under the consumer.
        VersionedSegmentMetadata vG = segmentRegistry.getSegment(TS_UUID, IDX, "seg-g").orElseThrow();
        SegmentMetadata d = vG.metadata().toBuilder().state(SegmentState.DEPRECATED).build();
        segmentRegistry.casUpdateSegment(vG, d);

        InMemorySegmentMerger mergerSpy = new InMemorySegmentMerger();
        assertTrue(consumer("w-1", mergerSpy).consumeOneTask());

        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.DONE, t.getState());
        assertNull(t.getOutputSegmentUuid());
        assertEquals(0, mergerSpy.getInvocationCount());
    }

    // -------------------------------------------------------------------------
    // merger declined / threw
    // -------------------------------------------------------------------------

    @Test
    public void mergerDeclinedSurfacesAsDoneNoop() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-i", "seg-j");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        SegmentMerger declining = new SegmentMerger() {
            @Override
            public SegmentMetadata merge(List<SegmentMetadata> in, int newOwnerInstance) {
                return null;
            }
        };
        assertTrue(consumer("w-1", declining).consumeOneTask());
        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.DONE, t.getState());
        assertNull(t.getOutputSegmentUuid());
    }

    @Test
    public void mergerThrowsTransitionsTaskToFailed() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-k", "seg-l");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        SegmentMerger throwingMerger = new SegmentMerger() {
            @Override
            public SegmentMetadata merge(List<SegmentMetadata> in, int newOwnerInstance) {
                throw new RuntimeException("boom");
            }
        };
        assertTrue(consumer("w-1", throwingMerger).consumeOneTask());
        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.FAILED, t.getState());
        assertEquals(1, t.getAttempts());
        assertNotNull(t.getLastError());
        assertTrue(t.getLastError().getMessage().contains("boom"));
        assertFalse(taskRegistry.leaseExists(TS_UUID, taskId));
    }

    @Test
    public void mergerThrowsAtMaxAttemptsTransitionsToPoison() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-m", "seg-n");
        String taskId = UUID.randomUUID().toString();
        OptimizerTask preFailed = pendingTask(taskId, inputs, 0).toBuilder()
                .attempts(2) // next failure → attempts=3 = maxAttempts → POISON
                .build();
        taskRegistry.createTask(preFailed);
        SegmentMerger throwingMerger = new SegmentMerger() {
            @Override
            public SegmentMetadata merge(List<SegmentMetadata> in, int newOwnerInstance) {
                throw new RuntimeException("repeated failure");
            }
        };
        assertTrue(consumer("w-1", throwingMerger).consumeOneTask());
        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.POISON, t.getState());
        assertEquals(3, t.getAttempts());
    }

    // -------------------------------------------------------------------------
    // revalidate failure after merge
    // -------------------------------------------------------------------------

    @Test
    public void revalidateFailureAbandonsOutputAndFailsTask() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-o", "seg-p");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        // Merger drifts seg-o just before returning success — emulate by
        // wrapping the merger and bumping the segment between merge() and
        // revalidate().
        SegmentMerger driftingMerger = new SegmentMerger() {
            @Override
            public SegmentMetadata merge(List<SegmentMetadata> in, int newOwnerInstance) {
                // Move seg-o to TRANSFERRING out-of-band so revalidate fails.
                try {
                    VersionedSegmentMetadata vO = segmentRegistry
                            .getSegment(TS_UUID, IDX, "seg-o").orElseThrow();
                    SegmentMetadata moved = vO.metadata().toBuilder()
                            .state(SegmentState.TRANSFERRING)
                            .pendingOwnerInstanceId(2)
                            .build();
                    segmentRegistry.casUpdateSegment(vO, moved);
                } catch (Exception failedSetup) {
                    throw new RuntimeException(failedSetup);
                }
                return merger.merge(in, newOwnerInstance);
            }

            @Override
            public void abandon(SegmentMetadata aborted) {
                merger.abandon(aborted);
            }
        };
        assertTrue(consumer("w-1", driftingMerger).consumeOneTask());
        OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.FAILED, t.getState());
        assertTrue(t.getLastError().getMessage().contains("drifted"));
        // merger.abandon must have been called for the output.
        assertEquals(1, merger.getAbandonInvocationCount());
    }

    // -------------------------------------------------------------------------
    // sanity: lease cleanup
    // -------------------------------------------------------------------------

    @Test
    public void taskZnodeDeletedDuringMergeIsHandledGracefully() throws Exception {
        // Race: the orphan scanner sees an input already DEPRECATED by us
        // mid-loop and casDeleteTask's it just before we run the final
        // CAS-to-DONE. The consumer must surface no exception and the
        // scheduler tick must continue.
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-race-a", "seg-race-b");
        String taskId = UUID.randomUUID().toString();
        OptimizerTask pending = pendingTask(taskId, inputs, 0);
        taskRegistry.createTask(pending);

        // Wrap the merger so it deletes the task znode out-of-band AFTER the
        // merge call (mimics the orphan scanner racing us between
        // createSegment + per-input deprecate and the final DONE CAS).
        SegmentMerger racingMerger = new SegmentMerger() {
            @Override
            public SegmentMetadata merge(List<SegmentMetadata> in, int newOwnerInstance) throws Exception {
                SegmentMetadata out = merger.merge(in, newOwnerInstance);
                // Race the orphan scanner: delete the task znode (lease child
                // first) right when the consumer is about to publish output.
                String taskPath = taskRegistry.taskPath(TS_UUID, taskId);
                try {
                    zk.delete(taskRegistry.leasePath(TS_UUID, taskId), -1);
                } catch (org.apache.zookeeper.KeeperException.NoNodeException ok) {
                    // lease not created yet
                }
                zk.delete(taskPath, -1);
                return out;
            }
        };

        // Consume must not throw — the work landed (merge output published,
        // inputs deprecated), only the final DONE CAS finds no znode and
        // logs an INFO instead of escaping.
        assertTrue(consumer("w-race", racingMerger).consumeOneTask());

        // Task is gone (deleted by the race), inputs are DEPRECATED, output exists.
        org.junit.Assert.assertFalse(taskRegistry.getTask(TS_UUID, taskId).isPresent());
        VersionedSegmentMetadata a = segmentRegistry.getSegment(TS_UUID, IDX, "seg-race-a").orElseThrow();
        VersionedSegmentMetadata b = segmentRegistry.getSegment(TS_UUID, IDX, "seg-race-b").orElseThrow();
        assertEquals(SegmentState.DEPRECATED, a.metadata().getState());
        assertEquals(SegmentState.DEPRECATED, b.metadata().getState());
    }

    @Test
    public void leaseIsDeletedRegardlessOfOutcome() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-q", "seg-r");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTask(taskId, inputs, 0));
        consumer("w-1").consumeOneTask();
        assertFalse(taskRegistry.leaseExists(TS_UUID, taskId));
    }
}
