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
import herddb.log.LogSequenceNumber;
import java.util.List;
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
 * Issue #555: pins the production-shape producer wiring. A producer built
 * WITH an {@link IndexingServiceInstanceDirectory} (as {@link IndexOptimizerMain}
 * always does) must:
 * <ul>
 *   <li>snapshot the resolved ack-target {@code serviceId}s onto each task
 *       ({@code expectedAckServiceIds}) and mark the task {@code requiresAcks};</li>
 *   <li>fail-closed — skip task creation entirely — when the directory
 *       cannot resolve any ack target (ZK blip / no IS pod registered),
 *       so the consumer never commits an atomic swap with zero acks.</li>
 * </ul>
 */
public class OptimizerTaskProducerAckTargetsTest {

    private static final String BASE_PATH = "/herd-test-555-acktargets";
    private static final String TS_UUID = "ts-uuid";
    private static final String IDX = "idx-a";

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
                3, 1_000L, 5_000L, 60_000L, 600_000L, fakeClock::get);
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

    private void seedActive(String segUuid, int owner) throws Exception {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(segUuid)
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
        segmentRegistry.createSegment(m);
    }

    /** Stub directory returning a fixed ack-target list for every effective instance. */
    private static final class StubDirectory implements IndexingServiceInstanceDirectory {
        private final List<String> serviceIds;

        StubDirectory(List<String> serviceIds) {
            this.serviceIds = serviceIds;
        }

        @Override
        public int[] liveInstanceOrdinals() {
            return new int[]{0};
        }

        @Override
        public List<String> serviceIdsForEffectiveInstance(int effectiveInstanceId) {
            return serviceIds;
        }
    }

    private OptimizerTaskProducer producerWithDirectory(IndexingServiceInstanceDirectory dir) {
        return new OptimizerTaskProducer(taskRegistry, segmentRegistry, TS_UUID,
                new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector(), leaderLock, leaderEpoch, orphanScanner,
                fakeClock::get, dir);
    }

    @Test
    public void producerSnapshotsExpectedAckServiceIdsFromDirectory() throws Exception {
        seedActive("seg-a-1", 0);
        seedActive("seg-a-2", 0);
        OptimizerTaskProducer producer = producerWithDirectory(
                new StubDirectory(List.of("svc-primary-0", "svc-shadow-0")));

        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertEquals(1, r.tasksCreated);
        assertEquals(0, r.tasksSkippedNoAckTargets);

        List<VersionedOptimizerTask> tasks = taskRegistry.listTasks(TS_UUID);
        assertEquals(1, tasks.size());
        OptimizerTask t = tasks.get(0).task();
        assertTrue("a directory-backed task must require acks", t.isRequiresAcks());
        assertEquals("expected-acks must be the directory's snapshot",
                List.of("svc-primary-0", "svc-shadow-0"), t.getExpectedAckServiceIds());
    }

    @Test
    public void producerSkipsTaskWhenDirectoryReturnsNoAckTargets() throws Exception {
        seedActive("seg-b-1", 0);
        seedActive("seg-b-2", 0);
        // Directory cannot resolve any ack target (ZK blip / scale-down race).
        OptimizerTaskProducer producer = producerWithDirectory(
                new StubDirectory(List.of()));

        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        // Fail-closed: NO task is created — the consumer would otherwise
        // commit the atomic swap with zero acks (the issue #555 data-loss
        // window).
        assertEquals(0, r.tasksCreated);
        assertEquals(1, r.tasksSkippedNoAckTargets);
        assertTrue("no task may be emitted when ack targets cannot be resolved",
                taskRegistry.listTasks(TS_UUID).isEmpty());
    }

    @Test
    public void directorylessProducerEmitsTaskThatDoesNotRequireAcks() throws Exception {
        // The package-private directory-less constructor (used only by
        // in-package tests) emits requiresAcks=false tasks — the consumer
        // commits without acks. This pins the test-only escape hatch.
        seedActive("seg-c-1", 0);
        seedActive("seg-c-2", 0);
        OptimizerTaskProducer producer = new OptimizerTaskProducer(taskRegistry, segmentRegistry,
                TS_UUID, new MergePolicy.AggressivePolicy(Long.MAX_VALUE, Long.MAX_VALUE, 100, 8),
                new Fixed0OwnerSelector(), leaderLock, leaderEpoch, orphanScanner, fakeClock::get);

        OptimizerTaskProducer.ProduceResult r = producer.produceTasks();
        assertEquals(1, r.tasksCreated);
        assertEquals(0, r.tasksSkippedNoAckTargets);
        OptimizerTask t = taskRegistry.listTasks(TS_UUID).get(0).task();
        assertFalse("a directory-less task must not require acks", t.isRequiresAcks());
        assertTrue("a directory-less task carries no expected-acks",
                t.getExpectedAckServiceIds().isEmpty());
    }
}
