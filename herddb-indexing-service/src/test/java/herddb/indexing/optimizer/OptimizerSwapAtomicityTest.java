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
 * Issue #555 happy path: stage PROVISIONAL → all expected acks arrive →
 * the consumer commits an atomic ZooKeeper multi-op that flips the
 * output to ACTIVE AND every input to DEPRECATED in a single transaction.
 * No watcher snapshot can observe one transition without the other.
 */
public class OptimizerSwapAtomicityTest {

    private static final String BASE_PATH = "/herd-test-555-atomic";
    private static final String TS_UUID = "ts-uuid-555";
    private static final String IDX = "idx-uuid-555";

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
        zk = new ZooKeeper(zkServer.getConnectString(), 30_000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
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

    private List<VersionedSegmentMetadata> activeInputs(String... uuids) throws Exception {
        List<VersionedSegmentMetadata> inputs = new ArrayList<>();
        for (String u : uuids) {
            segmentRegistry.createSegment(sampleSegment(u, 1_000_000L));
            inputs.add(segmentRegistry.getSegment(TS_UUID, IDX, u).orElseThrow());
        }
        return inputs;
    }

    private OptimizerTask pendingTaskWithAcks(String taskId, List<VersionedSegmentMetadata> inputs,
                                              int targetOwner, List<String> expectedAcks) {
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
                .expectedAckServiceIds(expectedAcks)
                .build();
    }

    private OptimizerTaskConsumer consumer(String workerId, long ackTimeoutMs) {
        return new OptimizerTaskConsumer(taskRegistry, segmentRegistry, merger,
                TS_UUID, workerId,
                /* retentionMillis */ 600_000L,
                /* maxAttempts */ 3,
                /* swapAckTimeoutMillis */ ackTimeoutMs,
                fakeClock::get);
    }

    @Test
    public void stageThenAckThenSwapCommitsAtomically() throws Exception {
        // Setup: 2 inputs, 1 expected ack ("is-0").
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-in-1", "seg-in-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, /* owner */ 0,
                List.of("is-0")));
        OptimizerTaskConsumer c = consumer("w-1", /* ackTimeoutMs */ 60_000L);

        // Stage 1: PENDING → CLAIMED → AWAITING_ACK (PROVISIONAL output published).
        assertTrue(c.consumeOneTask());
        OptimizerTask afterStage = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.AWAITING_ACK, afterStage.getState());
        String outputUuid = afterStage.getOutputSegmentUuid();
        assertNotNull(outputUuid);
        assertEquals(SegmentState.PROVISIONAL,
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow()
                        .metadata().getState());
        // Inputs still ACTIVE.
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-in-1").orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-in-2").orElseThrow()
                        .metadata().getState());

        // Stage 2: ack has not yet arrived → consumer parks the task.
        assertFalse("no progress expected before ack lands", c.consumeOneTask());
        assertEquals(OptimizerTaskState.AWAITING_ACK,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());

        // Simulate the IS pod adopting and writing the ephemeral ack znode.
        segmentRegistry.createSwapAckNode(outputUuid, "is-0");

        // Stage 3: ack present → consumer commits the atomic multi-op.
        assertTrue(c.consumeOneTask());

        OptimizerTask afterSwap = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.DONE, afterSwap.getState());
        assertEquals(outputUuid, afterSwap.getOutputSegmentUuid());

        // Both inputs are now DEPRECATED with replacedBy = [output] (and the
        // output is ACTIVE) — observed in a single snapshot.
        VersionedSegmentMetadata a = segmentRegistry.getSegment(TS_UUID, IDX, "seg-in-1").orElseThrow();
        VersionedSegmentMetadata b = segmentRegistry.getSegment(TS_UUID, IDX, "seg-in-2").orElseThrow();
        VersionedSegmentMetadata out = segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow();
        assertEquals(SegmentState.DEPRECATED, a.metadata().getState());
        assertEquals(SegmentState.DEPRECATED, b.metadata().getState());
        assertEquals(SegmentState.ACTIVE, out.metadata().getState());
        assertEquals(List.of(outputUuid), a.metadata().getReplacedBy());
        assertEquals(List.of(outputUuid), b.metadata().getReplacedBy());
        // The retention timer is set on the deprecated inputs.
        assertTrue("retention timer set", a.metadata().getRetentionUntilEpochMillis() > 0L);

        // Acks tree is torn down post-commit.
        assertTrue("acks tree should be deleted after commit",
                segmentRegistry.listSwapAcks(outputUuid, null).isEmpty());

        // Observability.
        assertEquals(1, c.getTasksStagedProvisionalTotal());
        assertEquals(1, c.getTasksSwapCommittedTotal());
    }

    @Test
    public void swapWaitsForEveryShadowAckBeforeCommitting() throws Exception {
        // Setup: 2 inputs, 2 expected acks (primary + 1 shadow).
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-w-1", "seg-w-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, /* owner */ 0,
                List.of("primary-is-0", "shadow-is-0-a")));
        OptimizerTaskConsumer c = consumer("w-1", /* ackTimeoutMs */ 60_000L);

        // Stage to AWAITING_ACK.
        assertTrue(c.consumeOneTask());
        String outputUuid = taskRegistry.getTask(TS_UUID, taskId)
                .orElseThrow().task().getOutputSegmentUuid();

        // Primary acks; consumer must NOT commit because the shadow ack is missing.
        segmentRegistry.createSwapAckNode(outputUuid, "primary-is-0");
        assertFalse("consumer must wait for the shadow ack",
                c.consumeOneTask());
        assertEquals(OptimizerTaskState.AWAITING_ACK,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-w-1").orElseThrow()
                        .metadata().getState());

        // Shadow acks; multi-op fires.
        segmentRegistry.createSwapAckNode(outputUuid, "shadow-is-0-a");
        assertTrue(c.consumeOneTask());

        assertEquals(OptimizerTaskState.DONE,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.DEPRECATED,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-w-1").orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.DEPRECATED,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-w-2").orElseThrow()
                        .metadata().getState());
    }

    @Test
    public void ackArrivedAlreadyCommitsOnFirstAwaitingDrain() throws Exception {
        // Same as the basic happy path but acks land BEFORE the consumer's
        // second poll: the consumer's single subsequent consumeOneTask call
        // commits the multi-op without an intermediate "no progress" tick.
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-fast-1", "seg-fast-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, /* owner */ 0,
                List.of("is-eager")));
        OptimizerTaskConsumer c = consumer("w-eager", /* ackTimeoutMs */ 60_000L);

        // Stage 1.
        assertTrue(c.consumeOneTask());
        String outputUuid = taskRegistry.getTask(TS_UUID, taskId)
                .orElseThrow().task().getOutputSegmentUuid();

        // Ack lands before the next poll.
        segmentRegistry.createSwapAckNode(outputUuid, "is-eager");

        // Single consume drives AWAITING_ACK → DONE.
        assertTrue(c.consumeOneTask());
        assertEquals(OptimizerTaskState.DONE,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
    }

    @Test
    public void ackDisappearsBetweenCheckAndSwapStillCommits() throws Exception {
        // Issue #555 corner case: a shadow's ZK session expires in the window
        // between the consumer's listSwapAcks() observation and the atomic
        // multi-op. The swap MUST still commit — atomicSwap operates only on
        // segment znodes, never on the acks subtree, so a vanishing ack
        // cannot stop a swap the consumer already decided on. This pins the
        // deliberate "commit anyway" semantic.
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-vanish-1", "seg-vanish-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, /* owner */ 0,
                List.of("flaky-shadow")));
        OptimizerTaskConsumer c = consumer("w-vanish", /* ackTimeoutMs */ 60_000L);

        assertTrue(c.consumeOneTask()); // → AWAITING_ACK
        String outputUuid = taskRegistry.getTask(TS_UUID, taskId)
                .orElseThrow().task().getOutputSegmentUuid();
        segmentRegistry.createSwapAckNode(outputUuid, "flaky-shadow");

        // Hook: between the ack check and the multi-op, delete the ack
        // ephemeral (simulating the IS pod's session expiring).
        c.postAckCheckPreSwapHookForTests = () -> {
            try {
                zk.delete(segmentRegistry.ackPath(outputUuid, "flaky-shadow"), -1);
            } catch (org.apache.zookeeper.KeeperException.NoNodeException ok) {
                // already gone — fine
            } catch (Exception e) {
                throw new RuntimeException("hook failed to delete ack", e);
            }
        };

        assertTrue(c.consumeOneTask()); // → DONE despite the vanished ack

        assertEquals(OptimizerTaskState.DONE,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.DEPRECATED,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-vanish-1").orElseThrow()
                        .metadata().getState());
        assertEquals(1, c.getTasksSwapCommittedTotal());
    }

    @Test
    public void twoConsumersRacingTheSwapExactlyOneCommits() throws Exception {
        // Issue #555 concurrency: two optimizer pods drain the same
        // AWAITING_ACK task once acks land. The atomic swap is CAS-ordered —
        // exactly one consumer's multi-op wins; the other observes the
        // output already ACTIVE (or loses the CAS) and absorbs it cleanly.
        // End state: task DONE, inputs DEPRECATED exactly once, output ACTIVE.
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-race-1", "seg-race-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, /* owner */ 0,
                List.of("is-0")));

        OptimizerTaskConsumer c1 = consumer("w-race-1", /* ackTimeoutMs */ 60_000L);
        OptimizerTaskConsumer c2 = consumer("w-race-2", /* ackTimeoutMs */ 60_000L);

        // c1 stages the task to AWAITING_ACK.
        assertTrue(c1.consumeOneTask());
        String outputUuid = taskRegistry.getTask(TS_UUID, taskId)
                .orElseThrow().task().getOutputSegmentUuid();
        segmentRegistry.createSwapAckNode(outputUuid, "is-0");

        // Both consumers race to commit the swap, each looping until the
        // task reaches a terminal state.
        java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
        Runnable drain = () -> {
            try {
                start.await();
                for (int i = 0; i < 50; i++) {
                    OptimizerTask t = taskRegistry.getTask(TS_UUID, taskId)
                            .map(VersionedOptimizerTask::task).orElse(null);
                    if (t == null || t.getState().isTerminal()) {
                        return;
                    }
                    (Thread.currentThread().getName().endsWith("1") ? c1 : c2).consumeOneTask();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Thread t1 = new Thread(drain, "race-thread-1");
        Thread t2 = new Thread(drain, "race-thread-2");
        t1.start();
        t2.start();
        start.countDown();
        t1.join(30_000L);
        t2.join(30_000L);

        // Exactly one consumer committed the multi-op.
        assertEquals("exactly one consumer must commit the atomic swap",
                1, c1.getTasksSwapCommittedTotal() + c2.getTasksSwapCommittedTotal());
        // End state is consistent: task DONE, output ACTIVE, inputs DEPRECATED.
        assertEquals(OptimizerTaskState.DONE,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.DEPRECATED,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-race-1").orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.DEPRECATED,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-race-2").orElseThrow()
                        .metadata().getState());
    }
}
