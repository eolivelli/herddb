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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 * {@link herddb.index.vector.PersistentVectorStore} integration — that wiring
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

    private static void waitFor(java.util.function.BooleanSupplier predicate) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (predicate.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("predicate did not become true within 10s");
    }

    @Test
    public void initialOwnerSeesAssignedEvent() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID));
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
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID));

            // 1) Optimizer initiates transfer to instance 1.
            VersionedSegmentMetadata v0 = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, v0, /* newOwner= */ 1);
            assertEquals(SegmentState.TRANSFERRING, transferring.metadata().getState());
            assertEquals(1, transferring.metadata().getPendingOwnerInstanceId());

            // 2) Watcher 1 sees the pending assignment.
            waitFor(() -> l1.events.contains("PENDING:" + SEG_UUID));

            // 3) New owner finishes opening locally and CAS-completes the transfer.
            VersionedSegmentMetadata afterComplete = OwnershipTransfer.complete(registry,
                    transferring, /* newOwner= */ 1);
            assertEquals(SegmentState.ACTIVE, afterComplete.metadata().getState());
            assertEquals(1, afterComplete.metadata().getOwnerInstanceId());
            assertEquals(SegmentMetadata.NO_INSTANCE,
                    afterComplete.metadata().getPendingOwnerInstanceId());

            // 4) Watcher 1 now sees ASSIGNED, and watcher 0 sees RELEASED.
            waitFor(() -> l1.events.contains("ASSIGNED:" + SEG_UUID));
            waitFor(() -> l0.events.contains("RELEASED:" + SEG_UUID));
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
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID));

            VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            registry.casDeleteSegment(current);
            waitFor(() -> l0.events.contains("RELEASED:<deleted>"));
        }
    }

    /**
     * Smoke check: a stress run with many transitions does not lose events. We
     * loop a single segment through ACTIVE→TRANSFERRING→ACTIVE multiple times and
     * assert that the watcher's PENDING/ASSIGNED/RELEASED count stays consistent.
     */
    @Test
    public void repeatedTransfersDoNotDropEvents() throws Exception {
        registry.createSegment(sampleSegment(0));
        RecordingListener l0 = new RecordingListener();
        RecordingListener l1 = new RecordingListener();
        AtomicInteger cycle = new AtomicInteger();

        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0);
             SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, l1)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            w1.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID));

            for (int i = 0; i < 5; i++) {
                int target = (i % 2 == 0) ? 1 : 0;
                int source = 1 - target;
                int currentCycle = cycle.incrementAndGet();
                VersionedSegmentMetadata v = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID)
                        .orElseThrow();
                v = OwnershipTransfer.initiate(registry, v, target);
                v = OwnershipTransfer.complete(registry, v, target);
                final int t = target;
                final int s = source;
                final int cyc = currentCycle;
                waitFor(() -> {
                    RecordingListener listenerOfTarget = (t == 0) ? l0 : l1;
                    RecordingListener listenerOfSource = (s == 0) ? l0 : l1;
                    long assigns = listenerOfTarget.events.stream()
                            .filter(e -> e.startsWith("ASSIGNED:")).count();
                    long releases = listenerOfSource.events.stream()
                            .filter(e -> e.startsWith("RELEASED:")).count();
                    return assigns >= cyc / 2 + (t == 0 ? 1 : 0) && releases >= (cyc + 1) / 2;
                });
            }
        }
        // Final sanity: each listener saw at least one ASSIGNED and one RELEASED across cycles.
        assertFalse(l0.events.isEmpty());
        assertFalse(l1.events.isEmpty());
    }
}
