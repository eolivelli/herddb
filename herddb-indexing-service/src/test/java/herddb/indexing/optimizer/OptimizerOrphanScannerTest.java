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
import herddb.log.LogSequenceNumber;
import java.util.List;
import java.util.Optional;
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
 * Exhaustive state-transition matrix for {@link OptimizerOrphanScanner}.
 * Uses a real ZK (curator-test TestingServer) so the lease-ephemeral and
 * task-znode CAS semantics are exercised end-to-end.
 */
public class OptimizerOrphanScannerTest {

    private static final String BASE_PATH = "/herd-test-orphan-scanner";
    private static final String TS_UUID = "ts-uuid";
    private static final String IDX_UUID = "idx-uuid";
    private static final int MAX_ATTEMPTS = 3;
    private static final long ORPHAN_RESET_MS = 1_000L;
    private static final long TERMINAL_RETENTION_MS = 5_000L;
    private static final long POISON_RETENTION_MS = 60_000L;

    private TestingServer zkServer;
    private ZooKeeper zk;
    private OptimizerTaskRegistry taskRegistry;
    private SegmentRegistryClient segmentRegistry;
    private AtomicLong fakeClock;
    private OptimizerOrphanScanner scanner;

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
        scanner = new OptimizerOrphanScanner(taskRegistry, segmentRegistry, TS_UUID,
                MAX_ATTEMPTS, ORPHAN_RESET_MS, TERMINAL_RETENTION_MS, POISON_RETENTION_MS,
                fakeClock::get);
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

    private OptimizerTask claimedTask(String taskId, int attempts, long claimedAt,
                                      List<String> inputs) {
        return OptimizerTask.builder()
                .taskId(taskId)
                .tablespaceUuid(TS_UUID)
                .indexUuid(IDX_UUID)
                .inputSegmentUuids(inputs)
                .targetOwnerInstanceId(0)
                .state(OptimizerTaskState.CLAIMED)
                .attempts(attempts)
                .claim(new OptimizerTask.WorkerClaim("worker-x", claimedAt))
                .createdAtEpochMillis(claimedAt - 100L)
                .leaderEpoch(1L)
                .build();
    }

    private void createSegment(String segUuid, SegmentState state) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(state)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1000L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .retentionUntilEpochMillis(state == SegmentState.DEPRECATED ? Long.MAX_VALUE : -1L)
                .build();
        segmentRegistry.createSegment(m);
    }

    @Test
    public void claimedWithLiveLeaseIsUntouched() throws Exception {
        taskRegistry.createTask(claimedTask("t-live", 0, 1_000L, List.of("seg-a")));
        taskRegistry.createLease(TS_UUID, "t-live", "worker-x");
        fakeClock.set(1_000_000L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(0, r.orphansReset);
        assertEquals(0, r.orphansPoisoned);
        assertEquals(OptimizerTaskState.CLAIMED,
                taskRegistry.getTask(TS_UUID, "t-live").orElseThrow().task().getState());
    }

    @Test
    public void claimedWithMissingLeaseButWithinWindowIsUntouched() throws Exception {
        taskRegistry.createTask(claimedTask("t-young", 0, 1_000L, List.of("seg-a")));
        // No lease created. Age = now - claimedAt = 500 ms — under ORPHAN_RESET_MS = 1000.
        fakeClock.set(1_500L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(0, r.orphansReset);
    }

    @Test
    public void orphanedClaimedResetsToPendingWithIncrementedAttempts() throws Exception {
        taskRegistry.createTask(claimedTask("t-orphan", 1, 1_000L, List.of("seg-a")));
        createSegment("seg-a", SegmentState.ACTIVE);
        fakeClock.set(1_000_000L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(1, r.orphansReset);
        OptimizerTask updated = taskRegistry.getTask(TS_UUID, "t-orphan").orElseThrow().task();
        assertEquals(OptimizerTaskState.PENDING, updated.getState());
        assertEquals(2, updated.getAttempts());
        assertEquals(null, updated.getClaim());
    }

    @Test
    public void orphanedClaimedAtMaxAttemptsTransitionsToPoison() throws Exception {
        taskRegistry.createTask(claimedTask("t-poison", MAX_ATTEMPTS - 1, 1_000L, List.of("seg-a")));
        createSegment("seg-a", SegmentState.ACTIVE);
        fakeClock.set(1_000_000L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(1, r.orphansPoisoned);
        OptimizerTask updated = taskRegistry.getTask(TS_UUID, "t-poison").orElseThrow().task();
        assertEquals(OptimizerTaskState.POISON, updated.getState());
        assertEquals(MAX_ATTEMPTS, updated.getAttempts());
    }

    @Test
    public void orphanedClaimedWithDeprecatedInputIsDeleted() throws Exception {
        taskRegistry.createTask(claimedTask("t-already-done", 0, 1_000L,
                List.of("seg-a", "seg-b")));
        createSegment("seg-a", SegmentState.DEPRECATED);
        createSegment("seg-b", SegmentState.ACTIVE);
        fakeClock.set(1_000_000L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(1, r.orphansDeletedAfterDeprecate);
        Optional<VersionedOptimizerTask> after = taskRegistry.getTask(TS_UUID, "t-already-done");
        assertFalse(after.isPresent());
    }

    @Test
    public void terminalDoneTaskOlderThanRetentionIsGCd() throws Exception {
        OptimizerTask done = OptimizerTask.builder()
                .taskId("t-done")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_UUID)
                .inputSegmentUuids(List.of("seg-a"))
                .state(OptimizerTaskState.DONE)
                .outputSegmentUuid("seg-out")
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(done);
        fakeClock.set(1_000L + TERMINAL_RETENTION_MS + 100L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(1, r.terminalGcCount);
        assertFalse(taskRegistry.getTask(TS_UUID, "t-done").isPresent());
    }

    @Test
    public void terminalFailedTaskOlderThanRetentionIsGCd() throws Exception {
        OptimizerTask failed = OptimizerTask.builder()
                .taskId("t-failed")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_UUID)
                .inputSegmentUuids(List.of("seg-a"))
                .state(OptimizerTaskState.FAILED)
                .attempts(2)
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(failed);
        fakeClock.set(1_000L + TERMINAL_RETENTION_MS + 100L);
        OptimizerOrphanScanner.ScanResult r = scanner.scan();
        assertEquals(1, r.terminalGcCount);
        assertFalse(taskRegistry.getTask(TS_UUID, "t-failed").isPresent());
    }

    @Test
    public void poisonTaskUsesLongerRetention() throws Exception {
        OptimizerTask poison = OptimizerTask.builder()
                .taskId("t-poison")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_UUID)
                .inputSegmentUuids(List.of("seg-a"))
                .state(OptimizerTaskState.POISON)
                .attempts(MAX_ATTEMPTS)
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(poison);
        // After terminal retention but before poison retention: NOT GC'd.
        fakeClock.set(1_000L + TERMINAL_RETENTION_MS + 100L);
        OptimizerOrphanScanner.ScanResult mid = scanner.scan();
        assertEquals(0, mid.terminalGcCount);
        assertNotNull(taskRegistry.getTask(TS_UUID, "t-poison").orElse(null));
        // After poison retention: GC'd.
        fakeClock.set(1_000L + POISON_RETENTION_MS + 100L);
        OptimizerOrphanScanner.ScanResult late = scanner.scan();
        assertEquals(1, late.terminalGcCount);
    }

    @Test
    public void scanIsIdempotent() throws Exception {
        taskRegistry.createTask(claimedTask("t-idem", 0, 1_000L, List.of("seg-a")));
        createSegment("seg-a", SegmentState.ACTIVE);
        fakeClock.set(1_000_000L);
        scanner.scan();
        OptimizerOrphanScanner.ScanResult second = scanner.scan();
        // After the first scan reset the task to PENDING, second scan must not
        // re-process it (CLAIMED → PENDING is one-way for the orphan logic).
        assertEquals(0, second.orphansReset);
    }
}
