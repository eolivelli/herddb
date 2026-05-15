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
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
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
 * Issue #555 crash-recovery: when an optimizer pod dies after the
 * CLAIMED → AWAITING_ACK transition (i.e. PROVISIONAL output is on ZK
 * but no other pod is around to drive it to commit), the orphan scanner
 * aborts the task after {@code provisionalGcMs} has elapsed and cleans up
 * the staged PROVISIONAL znode + acks subtree. The task transitions to
 * FAILED / POISON; the inputs remain ACTIVE so the producer can retry
 * with a fresh task.
 */
public class OptimizerSwapCrashAfterProvisionalTest {

    private static final String BASE_PATH = "/herd-test-555-crash";
    private static final String TS_UUID = "ts-uuid-555";
    private static final String IDX = "idx-uuid-555";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private OptimizerTaskRegistry taskRegistry;
    private SegmentRegistryClient segmentRegistry;
    private AtomicLong fakeClock;

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

    private void seedActive(String segUuid) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
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
        segmentRegistry.createSegment(m);
    }

    private void seedProvisional(String segUuid, long stagedAt) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX)
                .indexName("docs_v1")
                .state(SegmentState.PROVISIONAL)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(2_000_000L)
                .vectorCount(20L)
                .generation(2L)
                .createdAtEpochMillis(stagedAt)
                .provisionalCreatedAtEpochMillis(stagedAt)
                .build();
        segmentRegistry.createSegment(m);
        segmentRegistry.createSwapAckParent(segUuid);
    }

    @Test
    public void orphanScannerAbortsStaleAwaitingAckTask() throws Exception {
        seedActive("seg-c-1");
        seedActive("seg-c-2");
        VersionedSegmentMetadata in1 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-c-1").orElseThrow();
        VersionedSegmentMetadata in2 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-c-2").orElseThrow();

        String stagedUuid = UUID.randomUUID().toString();
        seedProvisional(stagedUuid, /* stagedAt */ 10_000L);

        // Pre-seed an AWAITING_ACK task referencing the staged output. The
        // optimizer that staged it is "dead" (no lease, no other consumer pod).
        String taskId = UUID.randomUUID().toString();
        Map<String, Integer> versions = new LinkedHashMap<>();
        versions.put("seg-c-1", in1.zkVersion());
        versions.put("seg-c-2", in2.zkVersion());
        OptimizerTask awaiting = OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid(IDX)
                .inputSegmentUuids(List.of("seg-c-1", "seg-c-2"))
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.AWAITING_ACK)
                .outputSegmentUuid(stagedUuid)
                .provisionalOutputCreatedAtEpochMillis(10_000L)
                .createdAtEpochMillis(10_000L)
                .leaderEpoch(1L)
                .expectedAckServiceIds(List.of("never-acks"))
                .build();
        taskRegistry.createTask(awaiting);

        OptimizerOrphanScanner scanner = new OptimizerOrphanScanner(
                taskRegistry, segmentRegistry, TS_UUID,
                /* maxAttempts */ 3,
                /* orphanResetMs */ 1_000L,
                /* terminalRetentionMs */ 60_000L,
                /* poisonRetentionMs */ 60_000L,
                /* provisionalGcMs */ 30_000L,
                fakeClock::get);

        // Within the provisional-GC window: nothing happens.
        fakeClock.set(20_000L);
        OptimizerOrphanScanner.ScanResult resultEarly = scanner.scan();
        assertEquals("no abort while inside provisional GC window",
                0, resultEarly.awaitingAckAborted);
        assertEquals(OptimizerTaskState.AWAITING_ACK,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
        assertTrue(segmentRegistry.getSegment(TS_UUID, IDX, stagedUuid).isPresent());

        // Past the window: scanner aborts.
        fakeClock.set(60_000L);
        OptimizerOrphanScanner.ScanResult resultLate = scanner.scan();
        assertEquals("scanner aborted the stale AWAITING_ACK task",
                1, resultLate.awaitingAckAborted);

        OptimizerTask afterAbort = taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task();
        assertEquals(OptimizerTaskState.FAILED, afterAbort.getState());
        assertEquals(1, afterAbort.getAttempts());
        assertTrue(afterAbort.getLastError().getMessage().contains("AWAITING_ACK task aborted"));
        // Staged PROVISIONAL znode + acks tree gone.
        assertFalse("PROVISIONAL znode must be deleted by the scanner",
                segmentRegistry.getSegment(TS_UUID, IDX, stagedUuid).isPresent());
        assertTrue("acks tree must be deleted by the scanner",
                segmentRegistry.listSwapAcks(stagedUuid, null).isEmpty());
        // Inputs remain ACTIVE for the producer's next tick.
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-c-1").orElseThrow()
                        .metadata().getState());
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-c-2").orElseThrow()
                        .metadata().getState());
    }

    @Test
    public void scannerSweepsOrphanProvisionalWithNoTaskReference() throws Exception {
        // Issue #555: a pod that crashed BETWEEN createSegment(PROVISIONAL)
        // and the CLAIMED → AWAITING_ACK CAS leaves a PROVISIONAL znode that
        // NO task references (the task, still CLAIMED, never recorded the
        // output UUID — and the orphan scanner reset it to PENDING). The
        // orphan-PROVISIONAL sweep must reclaim that dangling znode.
        seedActive("seg-keep-active");
        String orphanUuid = UUID.randomUUID().toString();
        seedProvisional(orphanUuid, /* stagedAt */ 10_000L);
        // No task in the registry references orphanUuid at all.

        OptimizerOrphanScanner scanner = new OptimizerOrphanScanner(
                taskRegistry, segmentRegistry, TS_UUID,
                /* maxAttempts */ 3,
                /* orphanResetMs */ 1_000L,
                /* terminalRetentionMs */ 60_000L,
                /* poisonRetentionMs */ 60_000L,
                /* provisionalGcMs */ 30_000L,
                fakeClock::get);

        // Within the GC window: the orphan PROVISIONAL znode is left alone.
        fakeClock.set(20_000L);
        OptimizerOrphanScanner.ScanResult early = scanner.scan();
        assertEquals(0, early.orphanProvisionalSwept);
        assertTrue(segmentRegistry.getSegment(TS_UUID, IDX, orphanUuid).isPresent());

        // Past the GC window: the sweep deletes it.
        fakeClock.set(60_000L);
        OptimizerOrphanScanner.ScanResult late = scanner.scan();
        assertEquals("orphan PROVISIONAL znode must be swept",
                1, late.orphanProvisionalSwept);
        assertFalse("orphan PROVISIONAL znode must be gone",
                segmentRegistry.getSegment(TS_UUID, IDX, orphanUuid).isPresent());
        assertTrue("acks subtree of the orphan must be gone",
                segmentRegistry.listSwapAcks(orphanUuid, null).isEmpty());
        // An unrelated ACTIVE segment is never touched by the sweep.
        assertEquals(SegmentState.ACTIVE,
                segmentRegistry.getSegment(TS_UUID, IDX, "seg-keep-active").orElseThrow()
                        .metadata().getState());
    }

    @Test
    public void scannerSweepDoesNotTouchProvisionalBackedByAwaitingAckTask() throws Exception {
        // The sweep must NOT delete a PROVISIONAL znode that IS referenced
        // by a live AWAITING_ACK task — handleAwaitingAck owns that one.
        seedActive("seg-ref-1");
        seedActive("seg-ref-2");
        VersionedSegmentMetadata in1 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-ref-1").orElseThrow();
        VersionedSegmentMetadata in2 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-ref-2").orElseThrow();
        String stagedUuid = UUID.randomUUID().toString();
        seedProvisional(stagedUuid, /* stagedAt */ 10_000L);

        String taskId = UUID.randomUUID().toString();
        Map<String, Integer> versions = new LinkedHashMap<>();
        versions.put("seg-ref-1", in1.zkVersion());
        versions.put("seg-ref-2", in2.zkVersion());
        // AWAITING_ACK task referencing the staged output, but NOT yet stale
        // (provisionalCreatedAt is recent relative to provisionalGcMs).
        OptimizerTask awaiting = OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid(IDX)
                .inputSegmentUuids(List.of("seg-ref-1", "seg-ref-2"))
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.AWAITING_ACK)
                .outputSegmentUuid(stagedUuid)
                .provisionalOutputCreatedAtEpochMillis(10_000L)
                .createdAtEpochMillis(10_000L)
                .leaderEpoch(1L)
                .expectedAckServiceIds(List.of("is-0"))
                .requiresAcks(true)
                .build();
        taskRegistry.createTask(awaiting);

        OptimizerOrphanScanner scanner = new OptimizerOrphanScanner(
                taskRegistry, segmentRegistry, TS_UUID,
                /* maxAttempts */ 3,
                /* orphanResetMs */ 1_000L,
                /* terminalRetentionMs */ 60_000L,
                /* poisonRetentionMs */ 60_000L,
                /* provisionalGcMs */ 30_000L,
                fakeClock::get);

        // Even well past provisionalGcMs, the sweep must not touch a
        // PROVISIONAL znode referenced by an AWAITING_ACK task — the
        // handleAwaitingAck branch is what aborts that one (and it also
        // fires here, so the task ends FAILED, but the znode delete is
        // done by handleAwaitingAck's abort, counted as awaitingAckAborted,
        // NOT as an orphan sweep).
        fakeClock.set(60_000L);
        OptimizerOrphanScanner.ScanResult result = scanner.scan();
        assertEquals("the staged znode is owned by handleAwaitingAck, not the orphan sweep",
                0, result.orphanProvisionalSwept);
        assertEquals(1, result.awaitingAckAborted);
    }

    @Test
    public void scannerHonorsProvisionalGcWindowForActiveTaskWithLease() throws Exception {
        // Sanity: a recently-staged AWAITING_ACK task that's still within the
        // window must NOT be touched by the scanner — only ack arrival, ack
        // timeout, or pod death past the window should drive it forward.
        seedActive("seg-fresh-1");
        seedActive("seg-fresh-2");
        VersionedSegmentMetadata in1 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-fresh-1").orElseThrow();
        VersionedSegmentMetadata in2 = segmentRegistry.getSegment(TS_UUID, IDX, "seg-fresh-2").orElseThrow();

        String stagedUuid = UUID.randomUUID().toString();
        seedProvisional(stagedUuid, 10_000L);

        String taskId = UUID.randomUUID().toString();
        Map<String, Integer> versions = new LinkedHashMap<>();
        versions.put("seg-fresh-1", in1.zkVersion());
        versions.put("seg-fresh-2", in2.zkVersion());
        OptimizerTask awaiting = OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid(IDX)
                .inputSegmentUuids(List.of("seg-fresh-1", "seg-fresh-2"))
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.AWAITING_ACK)
                .outputSegmentUuid(stagedUuid)
                .provisionalOutputCreatedAtEpochMillis(10_000L)
                .createdAtEpochMillis(10_000L)
                .leaderEpoch(1L)
                .expectedAckServiceIds(List.of("is-0"))
                .build();
        taskRegistry.createTask(awaiting);

        OptimizerOrphanScanner scanner = new OptimizerOrphanScanner(
                taskRegistry, segmentRegistry, TS_UUID,
                /* maxAttempts */ 3,
                /* orphanResetMs */ 1_000L,
                /* terminalRetentionMs */ 60_000L,
                /* poisonRetentionMs */ 60_000L,
                /* provisionalGcMs */ 30_000L,
                fakeClock::get);

        // Just inside the window.
        fakeClock.set(35_000L);
        OptimizerOrphanScanner.ScanResult result = scanner.scan();
        assertEquals(0, result.awaitingAckAborted);
        assertEquals(OptimizerTaskState.AWAITING_ACK,
                taskRegistry.getTask(TS_UUID, taskId).orElseThrow().task().getState());
    }
}
