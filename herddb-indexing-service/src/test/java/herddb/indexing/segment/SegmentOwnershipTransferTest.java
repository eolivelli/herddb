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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
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
 * Two-IS-instance integration test for the segment ownership transfer protocol.
 *
 * <p>Sets up a {@link SegmentAssignmentWatcher} per IS instance backed by the
 * same ZK registry, drives the transfer state machine via
 * {@link OwnershipTransfer}, and asserts that the watchers fire the right
 * events in the right order. Simulates the "open segment locally" / "close
 * segment locally" actions with simple counters rather than real
 * {@link herddb.indexing.vector.PersistentVectorStore} integration — that wiring
 * is exercised in step 5+.
 */
public class SegmentOwnershipTransferTest {

    private static final String BASE_PATH = "/herd-test-step4";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String SEG_UUID = "segabc";

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
        assertTrue("ZK connect timed out", connected.await(30, TimeUnit.SECONDS));
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

    private SegmentMetadata sampleSegment(int owner) {
        return SegmentMetadata.builder()
                .segmentUuid(SEG_UUID)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(owner)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(1_700_000_000L)
                .build();
    }

    /** Recording listener: appends event tags so the test can assert on the trace. */
    private static final class RecordingListener implements SegmentAssignmentListener {
        final ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();

        @Override
        public void onPendingAssignment(VersionedSegmentMetadata segment) {
            events.add("PENDING:" + segment.metadata().getSegmentUuid());
        }

        @Override
        public void onSegmentAssigned(VersionedSegmentMetadata segment) {
            events.add("ASSIGNED:" + segment.metadata().getSegmentUuid());
        }

        @Override
        public void onSegmentReleased(SegmentMetadata previous) {
            events.add("RELEASED:" + (previous == null ? "<deleted>" : previous.getSegmentUuid()));
        }

        List<String> snapshot() {
            return new ArrayList<>(events);
        }
    }

    /**
     * Polls {@code predicate} until it becomes true or the deadline elapses.
     *
     * <p>The deadline is generous (30s) because the watcher's single-thread
     * dispatch executor must process several ZK watcher fires (children +
     * per-segment data) and re-list the index between each transition, and
     * on a contended CI runner those round-trips occasionally exceed a tighter
     * deadline — see issue #608. A healthy run completes in milliseconds, so
     * a 30s exhaustion still signals a real regression.
     *
     * <p>{@code desc} is included in the failure message so the test reports
     * exactly which predicate did not stabilise (mirrors the helper in
     * {@code SegmentTransferCrashRecoveryTest}).
     */
    private static void waitFor(java.util.function.BooleanSupplier predicate, String desc)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            if (predicate.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("predicate did not become true within 30s: " + desc);
    }

    @Test
    public void initialOwnerSeesAssignedEvent() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID),
                    "instance 0 sees initial ASSIGNED");
        }
        assertEquals(Collections.singletonList("ASSIGNED:" + SEG_UUID), l0.snapshot());
    }

    @Test
    public void transferProtocolMovesOwnershipFromZeroToOne() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        RecordingListener l1 = new RecordingListener();

        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0);
             SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, l1)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            w1.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID),
                    "instance 0 sees initial ASSIGNED");

            // 1) Optimizer initiates transfer to instance 1.
            VersionedSegmentMetadata v0 = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, v0, /* newOwner= */ 1);
            assertEquals(SegmentState.TRANSFERRING, transferring.metadata().getState());
            assertEquals(1, transferring.metadata().getPendingOwnerInstanceId());

            // 2) Watcher 1 sees the pending assignment.
            waitFor(() -> l1.events.contains("PENDING:" + SEG_UUID),
                    "instance 1 sees PENDING after initiate");

            // 3) New owner finishes opening locally and CAS-completes the transfer.
            VersionedSegmentMetadata afterComplete = OwnershipTransfer.complete(registry,
                    transferring, /* newOwner= */ 1);
            assertEquals(SegmentState.ACTIVE, afterComplete.metadata().getState());
            assertEquals(1, afterComplete.metadata().getOwnerInstanceId());
            assertEquals(SegmentMetadata.NO_INSTANCE,
                    afterComplete.metadata().getPendingOwnerInstanceId());

            // 4) Watcher 1 now sees ASSIGNED, and watcher 0 sees RELEASED.
            waitFor(() -> l1.events.contains("ASSIGNED:" + SEG_UUID),
                    "instance 1 sees ASSIGNED after complete");
            waitFor(() -> l0.events.contains("RELEASED:" + SEG_UUID),
                    "instance 0 sees RELEASED after complete");
        }

        // Final trace: instance 0 went ASSIGNED → RELEASED; instance 1 went PENDING → ASSIGNED.
        // (Middle reordering is allowed — the watcher coalesces events on the dispatch thread.)
        assertTrue("0 must have seen ASSIGNED then RELEASED",
                l0.events.contains("ASSIGNED:" + SEG_UUID)
                        && l0.events.contains("RELEASED:" + SEG_UUID));
        assertTrue("1 must have seen PENDING then ASSIGNED",
                l1.events.contains("PENDING:" + SEG_UUID)
                        && l1.events.contains("ASSIGNED:" + SEG_UUID));
    }

    @Test
    public void initiateRejectsNonActiveSegment() throws Exception {
        registry.createSegment(sampleSegment(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        // Directly mutate the znode state to TRANSFERRING; subsequent initiate must be rejected.
        VersionedSegmentMetadata transferring = registry.casUpdateSegment(current,
                current.metadata().toBuilder()
                        .state(SegmentState.TRANSFERRING).pendingOwnerInstanceId(1).build());
        try {
            OwnershipTransfer.initiate(registry, transferring, 2);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("TRANSFERRING"));
        }
    }

    @Test
    public void initiateRejectsSelfTransfer() throws Exception {
        registry.createSegment(sampleSegment(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        try {
            OwnershipTransfer.initiate(registry, current, 0);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("already owned"));
        }
    }

    @Test
    public void completeRejectsWrongNewOwner() throws Exception {
        registry.createSegment(sampleSegment(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, current, 1);
        try {
            OwnershipTransfer.complete(registry, transferring, /* wrong owner */ 2);
            fail("expected IllegalStateException");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("pending owner"));
        }
    }

    @Test
    public void deletedSegmentTriggersReleasedForOwner() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID),
                    "instance 0 sees initial ASSIGNED");

            VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            registry.casDeleteSegment(current);
            // The watcher passes the cached pre-deletion metadata (not null) so that
            // downstream listeners (e.g. IndexingServiceEngine) can identify which segment
            // was removed and call dropSegmentByUuid on it.
            waitFor(() -> l0.events.contains("RELEASED:" + SEG_UUID),
                    "instance 0 sees RELEASED after delete");
        }
    }

    /**
     * Stress run with many ownership transitions. Tightened post-review (item C3) to
     * assert the final terminal-state event counts EXACTLY rather than a lenient
     * lower bound. The watcher's diff-and-emit design coalesces intermediate states
     * within a single scan (so {@code onPendingAssignment} may or may not fire
     * depending on timing) but the terminal {@code onSegmentAssigned} on the
     * destination and {@code onSegmentReleased} on the source MUST fire exactly once
     * per cycle.
     */
    @Test
    public void repeatedTransfersFireTerminalEventsExactlyOncePerCycle() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        RecordingListener l1 = new RecordingListener();
        final int cycles = 5;

        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0);
             SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, l1)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            w1.watchIndex(TS_UUID, IDX_UUID);
            // Initial assignment to instance 0 — wait for it to land before starting cycles.
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID),
                    "instance 0 sees initial ASSIGNED before cycles");

            for (int i = 0; i < cycles; i++) {
                int target = (i % 2 == 0) ? 1 : 0;
                VersionedSegmentMetadata v = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID)
                        .orElseThrow();
                v = OwnershipTransfer.initiate(registry, v, target);
                v = OwnershipTransfer.complete(registry, v, target);

                // Wait for both ends of the transition to be observed by the watchers
                // before starting the next cycle. This serialisation is what gives us
                // exactness: if we kept piling on transitions, the watcher could compress
                // multiple cycles into a single scan and lose intermediate events.
                final int t = target;
                final int s = 1 - target;
                final int expectedAssignsTarget = (t == 0)
                        ? 1 + (i + 2) / 2          // instance 0: initial + every odd cycle
                        : (i + 2) / 2;             // instance 1: every even cycle
                final int expectedReleasesSource = (s == 0)
                        ? (i + 2) / 2              // instance 0 released every even cycle
                        : (i + 1) / 2;             // instance 1 released every odd cycle
                waitFor(() -> {
                    RecordingListener targetL = (t == 0) ? l0 : l1;
                    RecordingListener sourceL = (s == 0) ? l0 : l1;
                    long assigns = targetL.events.stream()
                            .filter(e -> e.equals("ASSIGNED:" + SEG_UUID)).count();
                    long releases = sourceL.events.stream()
                            .filter(e -> e.equals("RELEASED:" + SEG_UUID)).count();
                    return assigns == expectedAssignsTarget && releases == expectedReleasesSource;
                }, "cycle " + i + ": target=" + t + " expects "
                        + expectedAssignsTarget + " ASSIGNED, source=" + s + " expects "
                        + expectedReleasesSource + " RELEASED");
            }

            // Final exact counts:
            //   instance 0 was the owner at cycles {start, 1, 3} → 3 ASSIGNED, 2 RELEASED
            //   instance 1 was the owner at cycles {0, 2, 4}     → 3 ASSIGNED, 3 RELEASED...
            // Wait — 5 cycles starting from owner=0:
            //   cycle 0: 0→1 (1 release for 0, 1 assign for 1)
            //   cycle 1: 1→0 (1 release for 1, 1 assign for 0)
            //   cycle 2: 0→1
            //   cycle 3: 1→0
            //   cycle 4: 0→1
            // After: 0 had ASSIGNED at start + cycle 1 + cycle 3 = 3 assigns; 0 had RELEASED at
            //        cycles 0, 2, 4 = 3 releases. 1 had ASSIGNED at cycles 0, 2, 4 = 3 assigns;
            //        1 had RELEASED at cycles 1, 3 = 2 releases.
            long assigns0 = l0.events.stream().filter(e -> e.equals("ASSIGNED:" + SEG_UUID)).count();
            long releases0 = l0.events.stream().filter(e -> e.equals("RELEASED:" + SEG_UUID)).count();
            long assigns1 = l1.events.stream().filter(e -> e.equals("ASSIGNED:" + SEG_UUID)).count();
            long releases1 = l1.events.stream().filter(e -> e.equals("RELEASED:" + SEG_UUID)).count();
            assertEquals(3, assigns0);
            assertEquals(3, releases0);
            assertEquals(3, assigns1);
            assertEquals(2, releases1);
        }
    }
}
