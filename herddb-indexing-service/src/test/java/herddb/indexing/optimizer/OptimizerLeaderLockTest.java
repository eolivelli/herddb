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
 * Validates fix C2: the optimizer leader lock prevents two engines from running
 * simultaneously against the same registry. During a Helm rolling upgrade or a
 * misconfigured {@code replicas} setting, only the lock holder makes progress;
 * the other tries to acquire on each tick and silently no-ops until its turn.
 */
public class OptimizerLeaderLockTest {

    private static final String BASE_PATH = "/herd-test-C2";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
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
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();
        fakeClock = new AtomicLong(0L);
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
                .indexUuid(IDX_UUID)
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

    private IndexOptimizerEngine engineWithLock(OptimizerLeaderLock lock) {
        return new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get, /* dsm */ null, lock);
    }

    @Test
    public void firstAcquireWins() {
        OptimizerLeaderLock lockA = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);
        OptimizerLeaderLock lockB = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);

        assertTrue(lockA.tryAcquire());
        assertTrue(lockA.isHeld());
        assertFalse(lockB.tryAcquire());
        assertFalse(lockB.isHeld());

        // Reacquire by the same holder is a no-op success.
        assertTrue(lockA.tryAcquire());
    }

    @Test
    public void releaseAllowsAnotherToAcquire() {
        OptimizerLeaderLock lockA = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);
        OptimizerLeaderLock lockB = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);

        assertTrue(lockA.tryAcquire());
        lockA.release();
        assertFalse(lockA.isHeld());
        assertTrue(lockB.tryAcquire());
    }

    @Test
    public void differentTablespacesDoNotBlockEachOther() {
        OptimizerLeaderLock lockTsA = new OptimizerLeaderLock(() -> zk, BASE_PATH, "ts-a");
        OptimizerLeaderLock lockTsB = new OptimizerLeaderLock(() -> zk, BASE_PATH, "ts-b");
        assertTrue(lockTsA.tryAcquire());
        assertTrue(lockTsB.tryAcquire());
        // Distinct paths.
        assertNotNull(lockTsA.getLockPath());
        assertNotNull(lockTsB.getLockPath());
        assertFalse(lockTsA.getLockPath().equals(lockTsB.getLockPath()));
    }

    @Test
    public void onlyLeaderEnginePerformsWork() throws Exception {
        // Pre-seed candidates so the merge would normally fire.
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 100L));
        }

        OptimizerLeaderLock lockA = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);
        OptimizerLeaderLock lockB = new OptimizerLeaderLock(() -> zk, BASE_PATH, TS_UUID);

        IndexOptimizerEngine engineA = engineWithLock(lockA);
        IndexOptimizerEngine engineB = engineWithLock(lockB);

        // engineA goes first → wins the lock → merges.
        engineA.runOnce();
        // engineB tries → loses the lock → no-ops.
        engineB.runOnce();

        // Final state: 1 active output + 3 deprecated, exactly what one merge produces.
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals(4, all.size());
        long active = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.ACTIVE).count();
        long deprecated = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.DEPRECATED).count();
        assertEquals(1, active);
        assertEquals(3, deprecated);

        assertEquals(1, engineA.getSegmentsMerged());
        assertEquals(0, engineB.getSegmentsMerged());
        assertEquals("engine B should have skipped the tick", 1, engineB.getTicksSkippedNotLeader());
        assertEquals("engine A should not skip", 0, engineA.getTicksSkippedNotLeader());
    }

    @Test
    public void expiredSessionAllowsAnotherToAcquire() throws Exception {
        // Review-item R6 from the second pr-reviewer pass: when lockA's ZK session
        // expires (e.g. network partition), the ephemeral znode is automatically
        // removed by ZK. lockA's cached held=true should be detected as stale on
        // the next tryAcquire — the verification path in tryAcquire reads the znode
        // and clears the cache when it's gone. Meanwhile lockB (a separate ZK
        // client) can acquire fresh.
        ZooKeeper zkA = freshZkClient();
        try {
            OptimizerLeaderLock lockA = new OptimizerLeaderLock(() -> zkA, BASE_PATH, TS_UUID);
            assertTrue(lockA.tryAcquire());
            assertTrue(lockA.isHeld());

            // Forcibly close zkA — its ephemeral lock node disappears.
            zkA.close();

            // A fresh lockB on a separate ZK session can now acquire.
            ZooKeeper zkB = freshZkClient();
            try {
                OptimizerLeaderLock lockB = new OptimizerLeaderLock(() -> zkB, BASE_PATH, TS_UUID);
                // Wait briefly for the ephemeral to drop server-side.
                long deadline = System.currentTimeMillis() + 5000L;
                while (System.currentTimeMillis() < deadline && !lockB.tryAcquire()) {
                    Thread.sleep(50);
                }
                assertTrue("lockB must be able to acquire after lockA's session ended",
                        lockB.isHeld());
            } finally {
                zkB.close();
            }
        } finally {
            // zkA already closed; nothing else to do.
        }
    }

    private ZooKeeper freshZkClient() throws Exception {
        java.util.concurrent.CountDownLatch connected = new java.util.concurrent.CountDownLatch(1);
        ZooKeeper newZk = new ZooKeeper(zkServer.getConnectString(), 5000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        return newZk;
    }

    @Test
    public void engineWithoutLockBehavesLikeBefore() throws Exception {
        // Sanity: an engine with no lock (legacy ctor) keeps the original behaviour.
        for (int i = 0; i < 3; i++) {
            registry.createSegment(sampleSegment("seg-" + i, 100L));
        }
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get);
        engine.runOnce();
        assertEquals(1, engine.getSegmentsMerged());
        assertEquals(0, engine.getTicksSkippedNotLeader());
    }
}
