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
 * Issue #555 abort path: when the expected acknowledgements do not arrive
 * within the configured swap-ack timeout, the consumer aborts the staged
 * swap — deletes the multipart files (via {@code merger.abandon}) plus the
 * PROVISIONAL znode plus the acks subtree, leaves inputs ACTIVE, and
 * transitions the task to FAILED (then POISON on retry exhaustion).
 */
public class OptimizerSwapAckTimeoutAbortTest {

    private static final String BASE_PATH = "/herd-test-555-timeout";
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

    private SegmentMetadata sampleSegment(String segUuid) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1_000_000L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    private List<VersionedSegmentMetadata> activeInputs(String... uuids) throws Exception {
        List<VersionedSegmentMetadata> inputs = new ArrayList<>();
        for (String u : uuids) {
            segmentRegistry.createSegment(sampleSegment(u));
            inputs.add(segmentRegistry.getSegment(TS_UUID, IDX, u).orElseThrow());
        }
        return inputs;
    }

    private OptimizerTask pendingTaskWithAcks(String taskId, List<VersionedSegmentMetadata> inputs,
                                              List<String> expectedAcks) {
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
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(fakeClock.get())
                .leaderEpoch(1L)
                .expectedAckServiceIds(expectedAcks)
                .build();
    }

    private OptimizerTaskConsumer consumer(String workerId, long ackTimeoutMs, int maxAttempts) {
        return new OptimizerTaskConsumer(taskRegistry, segmentRegistry, merger,
                TS_UUID, workerId,
                /* retentionMillis */ 600_000L,
                /* maxAttempts */ maxAttempts,
                /* swapAckTimeoutMillis */ ackTimeoutMs,
                fakeClock::get);
    }

    @Test
    public void ackTimeoutAbortsSwapAndKeepsInputsActive() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-t-1", "seg-t-2");
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskWithAcks(taskId, inputs, List.of("never-acks-IS")));
        OptimizerTaskConsumer c = consumer("w-timeout", /* ackTimeoutMs */ 30_000L,
                /* maxAttempts */ 3);

        // Stage to AWAITING_ACK.
        assertTrue(c.consumeOneTask());
        String outputUuid = taskRegistry.getTask(TS_UUID, taskId)
                .orElseThrow().task().getOutputSegmentUuid();
        assertNotNull(outputUuid);
        assertEquals(SegmentState.PROVISIONAL,
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).orElseThrow()
                        .metadata().getState());

        // Within the ack window: no progress.
        fakeClock.addAndGet(10_000L);
        assertFalse(c.consumeOneTask());
        assertEquals(OptimizerTaskState.AWAITING_ACK,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());

        // Past the ack window: abort fires.
        fakeClock.addAndGet(25_000L);
        assertTrue(c.consumeOneTask());

        OptimizerTask afterAbort = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.FAILED, afterAbort.getState());
        assertEquals(1, afterAbort.getAttempts());
        assertNotNull(afterAbort.getLastError());
        assertTrue("lastError should mention the ack timeout",
                afterAbort.getLastError().getMessage().contains("ack timeout"));

        // Inputs are still ACTIVE — the producer's next tick can retry.
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-t-1").orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-t-2").orElseThrow()
                        .metadata().getState());
        // The staged output and its acks tree are gone.
        assertFalse("PROVISIONAL znode must be deleted on abort",
                segmentRegistry.getSegment(TS_UUID, IDX, outputUuid).isPresent());
        assertTrue("acks tree must be deleted on abort",
                segmentRegistry.listSwapAcks(outputUuid, null).isEmpty());
        // merger.abandon must have been called for the staged output.
        assertEquals(1, merger.getAbandonInvocationCount());
        assertEquals(outputUuid, merger.getAbandonedUuids().get(0));

        // Observability.
        assertEquals(1, c.getTasksAckTimeoutTotal());
        assertEquals(1, c.getTasksFailedTotal());
        assertEquals(0, c.getTasksSwapCommittedTotal());
    }

    @Test
    public void ackTimeoutAtMaxAttemptsTransitionsToPoison() throws Exception {
        List<VersionedSegmentMetadata> inputs = activeInputs("seg-p-1", "seg-p-2");
        String taskId = UUID.randomUUID().toString();
        // Pre-set attempts so the abort transitions to POISON (next attempt = maxAttempts).
        OptimizerTask pending = pendingTaskWithAcks(taskId, inputs,
                List.of("never-acks-IS")).toBuilder()
                .attempts(2)
                .build();
        taskRegistry.createTask(pending);
        OptimizerTaskConsumer c = consumer("w-poison", /* ackTimeoutMs */ 5_000L,
                /* maxAttempts */ 3);

        // Stage to AWAITING_ACK.
        assertTrue(c.consumeOneTask());

        // Past timeout.
        fakeClock.addAndGet(10_000L);
        assertTrue(c.consumeOneTask());

        OptimizerTask afterAbort = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.POISON, afterAbort.getState());
        assertEquals(3, afterAbort.getAttempts());
        assertEquals(1, c.getTasksPoisonedTotal());
    }
}
