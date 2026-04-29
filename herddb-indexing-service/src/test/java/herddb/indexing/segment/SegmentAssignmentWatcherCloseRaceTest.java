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
    public void watcherFiresAgainstShutDownExecutorAreSwallowed() throws Exception {
        // Review-item B5 / P3-3 / P4-1: deterministically exercise the
        // RejectedExecutionException catch in SegmentAssignmentWatcher.dispatchScan.
        //
        // The earlier P3-3 design called watcher.close() and then invoked dispatchScan
        // directly — but close() sets `closed = true` BEFORE shutting down the
        // executor, so dispatchScan short-circuits at its early-return without
        // reaching the catch. The reviewer correctly flagged this as a
        // deterministic-but-vacuous test (it would still pass even if we deleted the
        // catch).
        //
        // The fix: use the package-private constructor with a custom executor
        // wrapper that ACCEPTS scheduleAtFixedRate (so watchIndex's heartbeat
        // arming succeeds) but REJECTS execute (so the dispatchScan path that
        // submits scan tasks hits the RejectedExecutionException catch). The
        // watcher's `closed` field stays false (we never call close()), so
        // dispatchScan does not short-circuit and the catch is the only thing
        // standing between the rejection and the ZK event thread.
        registry.createSegment(sample());

        java.util.concurrent.atomic.AtomicLong rejectedExecutes = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.ScheduledExecutorService rejectingExecutor = newRejectingExecutor(rejectedExecutes);
        try {
            SegmentAssignmentListener listener = new SegmentAssignmentListener() { };
            SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                    registry, 0, listener, /* refreshIntervalMs */ 30_000L, rejectingExecutor);

            watcher.watchIndex(TS_UUID, IDX_UUID);

            java.util.concurrent.atomic.AtomicReference<Throwable> uncaught =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread.UncaughtExceptionHandler previous = Thread.getDefaultUncaughtExceptionHandler();
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> uncaught.compareAndSet(null, e));
            try {
                // Each registry mutation arms the data watcher → fires once.
                // dispatchScan tries to submit to the rejecting executor; the catch
                // swallows the RejectedExecutionException. Without the B1 catch,
                // the exception would bubble up the ZK event thread and trip the UEH.
                for (int i = 0; i < 10; i++) {
                    VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, "seg-A")
                            .orElseThrow();
                    registry.casUpdateSegment(current,
                            current.metadata().toBuilder().generation(100L + i).build());
                }
                // Round-trip through ZK once more to drain the event thread's queue.
                registry.listSegments(TS_UUID, IDX_UUID);
            } finally {
                Thread.setDefaultUncaughtExceptionHandler(previous);
            }

            // The catch must have fired at least once (proves the test reaches it).
            assertTrue("the test must have actually triggered at least one rejected"
                    + " execute submission — otherwise the B1 catch is untested. Saw "
                    + rejectedExecutes.get() + " rejections.",
                    rejectedExecutes.get() > 0);

            Throwable caught = uncaught.get();
            if (caught instanceof java.util.concurrent.RejectedExecutionException) {
                org.junit.Assert.fail("RejectedExecutionException leaked through ZK event thread"
                        + " — the catch in SegmentAssignmentWatcher.dispatchScan was bypassed: "
                        + caught);
            }
        } finally {
            rejectingExecutor.shutdownNow();
        }
    }

    /**
     * Subclasses {@link java.util.concurrent.ScheduledThreadPoolExecutor} and overrides
     * only {@code execute} to always throw {@link java.util.concurrent.RejectedExecutionException}.
     * The pool itself stays running so the watcher's {@code scheduleAtFixedRate}
     * heartbeat-refresher arm succeeds — it's only the explicit {@code execute}
     * calls from {@code dispatchScan} that must blow up. That's exactly the
     * failure mode the B1 catch exists to handle.
     */
    private static java.util.concurrent.ScheduledExecutorService newRejectingExecutor(
            java.util.concurrent.atomic.AtomicLong rejectionCount) {
        return new java.util.concurrent.ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "segment-watcher-test-executor");
            t.setDaemon(true);
            return t;
        }) {
            @Override
            public void execute(Runnable command) {
                rejectionCount.incrementAndGet();
                throw new java.util.concurrent.RejectedExecutionException(
                        "test wrapper deliberately rejecting execute");
            }
        };
    }
}
