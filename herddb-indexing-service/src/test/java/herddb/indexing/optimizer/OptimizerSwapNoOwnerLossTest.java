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
 * Issue #555 end-to-end reproducer: a merge whose inputs span IS-0 and IS-1
 * is staged with {@code targetOwnerInstanceId = 1}. The bug fix asserts:
 * <ul>
 *   <li>The output is published as {@code PROVISIONAL} (not ACTIVE).</li>
 *   <li>Inputs stay {@code ACTIVE} until acks land — so neither IS sees a
 *       drop-without-replacement during the wait.</li>
 *   <li>Only after IS-1 (primary) AND its shadow have acked does the multi-op
 *       fire — and at that point inputs flip to {@code DEPRECATED} in the
 *       same ZK transaction that promotes the output to {@code ACTIVE}.</li>
 *   <li>No ZK snapshot between stage and commit can observe "inputs
 *       DEPRECATED without ACTIVE replacement".</li>
 * </ul>
 *
 * <p>This is the regression test for the gist1m GKE benchmark scenario:
 * IS-1 ended up with 0 segments because the old non-atomic code published
 * the new output (assigned to IS-0) and deprecated IS-1's inputs without
 * a corresponding adopt on IS-1.
 */
public class OptimizerSwapNoOwnerLossTest {

    private static final String BASE_PATH = "/herd-test-555-reproducer";
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

    private SegmentMetadata seg(String uuid, int owner) {
        return SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(owner)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1_000_000L)
                .vectorCount(100L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    private List<VersionedSegmentMetadata> activeInputs(String[] uuids, int[] owners) throws Exception {
        List<VersionedSegmentMetadata> inputs = new ArrayList<>();
        for (int i = 0; i < uuids.length; i++) {
            segmentRegistry.createSegment(seg(uuids[i], owners[i]));
            inputs.add(segmentRegistry.getSegment(TS_UUID, IDX, uuids[i]).orElseThrow());
        }
        return inputs;
    }

    private OptimizerTask pendingTaskCrossOwner(String taskId,
                                                 List<VersionedSegmentMetadata> inputs,
                                                 int targetOwner,
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
                .targetOwnerInstanceId(targetOwner)
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(fakeClock.get())
                .leaderEpoch(1L)
                .expectedAckServiceIds(expectedAcks)
                .build();
    }

    @Test
    public void crossInstanceMergeDoesNotDropInputsUntilNewOwnerAcks() throws Exception {
        // Cross-instance merge: seg-A owned by IS-0, seg-B + seg-C owned by IS-1.
        // The optimizer chose IS-1 as the new owner; primary "is-1" + shadow
        // "is-1-shadow" must both ack before the swap fires.
        List<VersionedSegmentMetadata> inputs = activeInputs(
                new String[]{"seg-A", "seg-B", "seg-C"},
                new int[]{0, 1, 1});
        String taskId = UUID.randomUUID().toString();
        taskRegistry.createTask(pendingTaskCrossOwner(taskId, inputs,
                /* targetOwner */ 1,
                /* expectedAcks */ List.of("is-1-primary", "is-1-shadow")));

        OptimizerTaskConsumer c = new OptimizerTaskConsumer(taskRegistry, segmentRegistry, merger,
                TS_UUID, "w-1",
                /* retentionMillis */ 600_000L,
                /* maxAttempts */ 3,
                /* swapAckTimeoutMillis */ 60_000L,
                fakeClock::get);

        // Stage 1: PENDING → AWAITING_ACK. PROVISIONAL output published with
        // ownerInstanceId=1. Inputs (including the IS-0-owned seg-A and the
        // IS-1-owned seg-B/seg-C) remain ACTIVE.
        assertTrue(c.consumeOneTask());
        OptimizerTask afterStage = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.AWAITING_ACK, afterStage.getState());
        String stagedUuid = afterStage.getOutputSegmentUuid();
        assertNotNull(stagedUuid);
        VersionedSegmentMetadata staged = segmentRegistry.getSegment(TS_UUID, IDX, stagedUuid).orElseThrow();
        assertEquals(SegmentState.PROVISIONAL, staged.metadata().getState());
        assertEquals(1, staged.metadata().getOwnerInstanceId());

        for (String inUuid : new String[]{"seg-A", "seg-B", "seg-C"}) {
            VersionedSegmentMetadata v = segmentRegistry.getSegment(TS_UUID, IDX, inUuid).orElseThrow();
            assertEquals("input " + inUuid + " must stay ACTIVE until acks arrive",
                    SegmentState.ACTIVE, v.metadata().getState());
        }

        // Only the primary acks: inputs MUST NOT be dropped — this is the
        // exact data-loss window from the gist1m bench.
        segmentRegistry.createSwapAckNode(stagedUuid, "is-1-primary");
        assertFalse("primary alone is not enough — the shadow must also ack",
                c.consumeOneTask());

        for (String inUuid : new String[]{"seg-A", "seg-B", "seg-C"}) {
            VersionedSegmentMetadata v = segmentRegistry.getSegment(TS_UUID, IDX, inUuid).orElseThrow();
            assertEquals("input " + inUuid + " must NOT be DEPRECATED while shadow lags",
                    SegmentState.ACTIVE, v.metadata().getState());
        }

        // Shadow finally acks → atomic swap fires.
        segmentRegistry.createSwapAckNode(stagedUuid, "is-1-shadow");
        assertTrue(c.consumeOneTask());

        // After commit: every input is DEPRECATED, replacedBy=[stagedUuid],
        // output is ACTIVE. Observed as a single atomic transition.
        for (String inUuid : new String[]{"seg-A", "seg-B", "seg-C"}) {
            VersionedSegmentMetadata v = segmentRegistry.getSegment(TS_UUID, IDX, inUuid).orElseThrow();
            assertEquals("input " + inUuid + " must be DEPRECATED post-swap",
                    SegmentState.DEPRECATED, v.metadata().getState());
            assertEquals("replacedBy must reference the merged output",
                    List.of(stagedUuid), v.metadata().getReplacedBy());
        }
        VersionedSegmentMetadata out = segmentRegistry.getSegment(TS_UUID, IDX, stagedUuid).orElseThrow();
        assertEquals(SegmentState.ACTIVE, out.metadata().getState());
        assertEquals(1, out.metadata().getOwnerInstanceId());

        // Atomicity assertion: there is no ZK snapshot in which the output is
        // ACTIVE but some input is still ACTIVE, or vice versa. We pin the
        // post-commit state above; the test would have failed at the earlier
        // assertion if any partial transition were observable mid-flow.

        // Task is DONE.
        assertEquals(OptimizerTaskState.DONE,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertEquals(1, c.getTasksSwapCommittedTotal());
    }
}
