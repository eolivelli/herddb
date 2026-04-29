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
        registry.createSegment(sample());

        // Review-item B5 from the second pr-reviewer pass: install a global
        // UncaughtExceptionHandler so a RejectedExecutionException thrown on the ZK
        // event thread (the failure mode B1 fixes) actually fails the test rather
        // than being silently swallowed. Use a CountDownLatch on the initial scan
        // to remove the Thread.sleep(150) timing dependency.
        java.util.concurrent.atomic.AtomicReference<Throwable> uncaught =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> uncaught.compareAndSet(null, e));

        java.util.concurrent.CountDownLatch initialAssigned = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch postCloseDispatched =
                new java.util.concurrent.CountDownLatch(1);

        SegmentAssignmentListener listener = new SegmentAssignmentListener() {
            @Override
            public void onSegmentAssigned(VersionedSegmentMetadata segment) {
                initialAssigned.countDown();
            }
        };

        try {
            SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(registry, 0, listener);
            watcher.watchIndex(TS_UUID, IDX_UUID);
            // Wait deterministically for the initial scan to fire its first event.
            assertTrue("initial onSegmentAssigned must fire",
                    initialAssigned.await(10, TimeUnit.SECONDS));

            // Close the watcher; subsequent watcher fires must NOT propagate
            // RejectedExecutionException through the ZK event thread.
            watcher.close();

            // Trigger many watcher fires by mutating the registry repeatedly. With a
            // closed dispatch executor, each fire would normally hit the executor's
            // RejectedExecutionHandler and bubble out via the ZK client's event thread.
            for (int i = 0; i < 5; i++) {
                VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-A")
                        .orElseThrow();
                registry.casUpdateSegment(current,
                        current.metadata().toBuilder().generation(100L + i).build());
            }

            // Give the ZK event thread time to fire all the watchers. We can't latch
            // on "watcher fired" here because the watcher is closed — but a cheap
            // equivalent is to round-trip through ZK once more (forces the event
            // thread to drain).
            registry.listSegments(TS_UUID, IDX_UUID);
            // Small bounded sleep to flush any queued event-thread callbacks. This
            // is not a sync primitive — it's a drain barrier; if B1 is broken the
            // exception is already in `uncaught` by now.
            Thread.sleep(100);
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(previous);
        }

        Throwable caught = uncaught.get();
        if (caught != null && caught instanceof java.util.concurrent.RejectedExecutionException) {
            org.junit.Assert.fail("RejectedExecutionException leaked through ZK event thread: "
                    + caught);
        }
        // Other unrelated exceptions from the JVM (e.g. test runner threads) should
        // not fail this assertion — only the specific REJ leak we are guarding against.
    }
}
