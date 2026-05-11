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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * End-to-end tests for {@link OptimizerTaskProducer} against a real ZK.
 * Exercises happy-path enqueue, in-flight-input exclusion, leader-epoch
 * fencing, orphan recovery, multi-index ticks, and the no-leader / no-live-
 * instances skip paths.
 */
public class OptimizerTaskProducerIntegrationTest {

    private static final String BASE_PATH = "/herd-test-producer";
    private static final String TS_UUID = "ts-uuid";
    private static final String IDX_A = "idx-a";
    private static final String IDX_B = "idx-b";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private OptimizerTaskRegistry taskRegistry;
    private SegmentRegistryClient segmentRegistry;
    private OptimizerLeaderLock leaderLock;
    private LeaderEpoch leaderEpoch;
    private OptimizerOrphanScanner orphanScanner;
    private AtomicLong fakeClock;

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
        leaderLock = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);
        leaderLock.ensureRoot();
        leaderEpoch = new LeaderEpoch(() -> zk, BASE_PATH, TS_UUID);
        fakeClock = new AtomicLong(10_000L);
        orphanScanner = new OptimizerOrphanScanner(taskRegistry, segmentRegistry, TS_UUID,
                3, 1_000L, 5_000L, 60_000L, fakeClock::get);
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

    private void seedActive(String indexUuid, String segUuid, long sizeBytes) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(indexUuid)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(100L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        segmentRegistry.createSegment(m);
    }

    private OptimizerTaskProducer newProducer(MergePolicy policy, OwnerSelector selector) {
        return new OptimizerTaskProducer(taskRegistry, segmentRegistry, TS_UUID,
                policy, selector, leaderLock, leaderEpoch, orphanScanner, fakeClock::get);
    }

    @Test
    public void happyPathCreatesOneTaskPerIndex() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        seedActive(IDX_B, "seg-b-1", 1_000_000L);
        seedActive(IDX_B, "seg-b-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertTrue(r.ranAsLeader);
        assertEquals(2, r.tasksCreated);
        assertEquals(2, taskRegistry.listTasks(TS_UUID).size());
    }

    @Test
    public void excludesInputsReferencedByExistingPendingTask() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        seedActive(IDX_A, "seg-a-3", 1_000_000L);
        // Pre-seed a PENDING task that holds seg-a-1 + seg-a-2.
        OptimizerTask existing = OptimizerTask.builder()
                .taskId("t-existing")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_A)
                .inputSegmentUuids(List.of("seg-a-1", "seg-a-2"))
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(existing);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        // Only one ACTIVE segment (seg-a-3) is not blocked — policy needs ≥ 2.
        assertEquals(0, r.tasksCreated);
        assertEquals(1, taskRegistry.listTasks(TS_UUID).size());
    }

    @Test
    public void epochBumpsOncePerTick() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        producer.produceTasks();
        producer.produceTasks();
        assertEquals(2L, leaderEpoch.currentEpoch());
    }

    @Test
    public void staleLeaderCannotBumpEpochAfterAnotherLeaderTakesOver() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        producer.produceTasks(); // epoch = 1

        // Out-of-band stale-stat write to mimic the BadVersion case.
        org.apache.zookeeper.data.Stat staleStat = zk.exists(leaderEpoch.getPath(), false);
        assertNotNull(staleStat);
        // Another leader bumps to 2.
        leaderEpoch.bumpEpoch();
        // Our cached stale stat is now invalid — direct setData fails. This
        // demonstrates the fencing primitive the producer relies on.
        try {
            zk.setData(leaderEpoch.getPath(), "999".getBytes(), staleStat.getVersion());
            org.junit.Assert.fail("expected BadVersionException");
        } catch (org.apache.zookeeper.KeeperException.BadVersionException ok) {
            // expected — stale leader path is fenced via znode version
        }
    }

    @Test
    public void orphanScannerResetsClaimedWithMissingLease() throws Exception {
        // Pre-seed a CLAIMED task whose claim is older than orphan reset window
        OptimizerTask orphan = OptimizerTask.builder()
                .taskId("t-orphan")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_A)
                .inputSegmentUuids(List.of("seg-a-1", "seg-a-2"))
                .state(OptimizerTaskState.CLAIMED)
                .attempts(0)
                .claim(new OptimizerTask.WorkerClaim("worker-x", 1_000L))
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(orphan);
        // No lease znode created. Advance clock well past the orphan window.
        fakeClock.set(1_000_000L);
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertEquals(1, r.orphansReset);
        OptimizerTask after = taskRegistry.getTask(TS_UUID, "t-orphan").orElseThrow().task();
        assertEquals(OptimizerTaskState.PENDING, after.getState());
        // Producer also created a new task… but those same inputs (seg-a-1+2) are now
        // blocked by the still-non-terminal "t-orphan" PENDING task, so 0 new tasks.
        assertEquals(0, r.tasksCreated);
    }

    @Test
    public void orphanScannerDeletesClaimedWithDeprecatedInput() throws Exception {
        // CLAIMED task whose first input is already DEPRECATED — the worker
        // already finished the merge but died before deleting the task znode.
        OptimizerTask t = OptimizerTask.builder()
                .taskId("t-done-leak")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_A)
                .inputSegmentUuids(List.of("seg-a-1", "seg-a-2"))
                .state(OptimizerTaskState.CLAIMED)
                .attempts(0)
                .claim(new OptimizerTask.WorkerClaim("worker-x", 1_000L))
                .createdAtEpochMillis(1_000L)
                .leaderEpoch(1L)
                .targetOwnerInstanceId(0)
                .build();
        taskRegistry.createTask(t);
        SegmentMetadata deprecated = SegmentMetadata.builder()
                .segmentUuid("seg-a-1")
                .tablespaceUuid(TS_UUID).indexUuid(IDX_A).tableName("docs").indexName("docs_v1")
                .state(SegmentState.DEPRECATED)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1_000_000L).vectorCount(10L).generation(1L)
                .createdAtEpochMillis(0L)
                .retentionUntilEpochMillis(Long.MAX_VALUE)
                .build();
        segmentRegistry.createSegment(deprecated);
        fakeClock.set(1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertEquals(1, r.orphansDeletedAfterDeprecate);
        assertFalse(taskRegistry.getTask(TS_UUID, "t-done-leak").isPresent());
    }

    @Test
    public void losingTheLeaderLockSkipsTheTickWithoutBumpingEpoch() throws Exception {
        // First, have another holder grab the lock.
        ZooKeeper otherZk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {});
        try {
            // Wait for connect
            for (int i = 0; i < 50 && otherZk.getState() != ZooKeeper.States.CONNECTED; i++) {
                Thread.sleep(50);
            }
            OptimizerLeaderLock otherLock = new OptimizerLeaderLock(() -> otherZk, BASE_PATH, TS_UUID);
            assertTrue(otherLock.tryAcquire());
            OptimizerTaskProducer producer = newProducer(
                    new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                    new Fixed0OwnerSelector());
            OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
            assertFalse("producer must not have acted as leader", r.ranAsLeader);
            // Epoch must not have advanced (initial 0).
            assertEquals(0L, leaderEpoch.currentEpoch());
        } finally {
            otherZk.close();
        }
    }

    @Test
    public void noLiveInstancesPreventsTaskCreation() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new LeastLoadedOwnerSelector((ts, ix) -> new long[0]));
        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertTrue(r.ranAsLeader);
        assertEquals(0, r.tasksCreated);
    }

    @Test
    public void targetOwnerSetFromSelector() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new StaticAssignmentOwnerSelector(new int[]{2}));
        producer.produceTasks();
        List<VersionedOptimizerTask> tasks = taskRegistry.listTasks(TS_UUID);
        assertEquals(1, tasks.size());
        assertEquals(2, tasks.get(0).task().getTargetOwnerInstanceId());
    }

    @Test
    public void taskCarriesCurrentLeaderEpochAndInputVersions() throws Exception {
        seedActive(IDX_A, "seg-a-1", 1_000_000L);
        seedActive(IDX_A, "seg-a-2", 1_000_000L);
        OptimizerTaskProducer producer = newProducer(
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector());
        producer.produceTasks();
        List<VersionedOptimizerTask> tasks = taskRegistry.listTasks(TS_UUID);
        assertEquals(1, tasks.size());
        OptimizerTask t = tasks.get(0).task();
        assertEquals(1L, t.getLeaderEpoch());
        Set<String> uuids = new HashSet<>(t.getInputSegmentUuids());
        assertTrue(uuids.contains("seg-a-1"));
        assertTrue(uuids.contains("seg-a-2"));
        assertEquals(2, t.getInputSegmentExpectedVersions().size());
    }
}
