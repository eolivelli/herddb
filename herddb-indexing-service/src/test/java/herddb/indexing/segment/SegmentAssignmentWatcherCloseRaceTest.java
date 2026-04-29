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
package herddb.indexing.segment;

import static org.junit.Assert.assertTrue;
import herddb.log.LogSequenceNumber;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Validates fix B1: a {@link SegmentAssignmentWatcher} that has already been
 * closed must NOT propagate a {@link java.util.concurrent.RejectedExecutionException}
 * back to the ZK event thread when a stale watcher fires after close.
 */
public class SegmentAssignmentWatcherCloseRaceTest {

    private static final String BASE_PATH = "/herd-test-B1";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

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

    private SegmentMetadata sample() {
        return SegmentMetadata.builder()
                .segmentUuid("seg-A").tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void closedWatcherIgnoresStaleWatcherFires() throws Exception {
        // Review-item B5 / P3-3: deterministically exercise the close-then-fire race.
        // Calling dispatchScan directly on the closed watcher is the most reliable
        // signal: it goes through the same code path the ZK Watcher.process() lambda
        // takes when it fires after close(), AND any exception propagates to the
        // calling thread (the JUnit test thread) rather than being swallowed by the
        // ZK EventThread. This eliminates the Thread.sleep + UEH-on-event-thread
        // dance the previous version relied on, which the reviewer correctly flagged
        // as both flaky AND under-asserting.
        registry.createSegment(sample());

        java.util.concurrent.CountDownLatch initialAssigned = new java.util.concurrent.CountDownLatch(1);
        SegmentAssignmentListener listener = new SegmentAssignmentListener() {
            @Override
            public void onSegmentAssigned(VersionedSegmentMetadata segment) {
                initialAssigned.countDown();
            }
        };

        SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(registry, 0, listener);
        watcher.watchIndex(TS_UUID, IDX_UUID);
        // Wait deterministically for the initial scan to land before we close.
        assertTrue("initial onSegmentAssigned must fire",
                initialAssigned.await(10, TimeUnit.SECONDS));

        // Close — dispatchExecutor is now shut down.
        watcher.close();

        // Direct invocation of the package-private dispatchScan: this is the same
        // method the ZK Watcher lambda invokes. Without the B1 fix, this submits to
        // the closed executor and throws RejectedExecutionException to OUR thread.
        // With the B1 fix, the catch swallows it.
        SegmentAssignmentWatcher.IndexKey key = new SegmentAssignmentWatcher.IndexKey(
                TS_UUID, IDX_UUID);
        for (int i = 0; i < 10; i++) {
            // Must NOT throw — that's the entire B1 contract.
            watcher.dispatchScan(key);
        }
    }
}
