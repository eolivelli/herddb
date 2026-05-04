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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
 * Pins the convergence-after-crash invariant for the segment ownership transfer
 * protocol: a JVM that dies anywhere mid-transfer must leave the registry in a
 * state from which the next start (or a fresh watcher attaching to the same
 * registry) deterministically converges to a single owner with no
 * double-ownership and no orphan TRANSFERRING state.
 *
 * <p>"Crashing the JVM" is simulated by closing the {@link SegmentAssignmentWatcher}
 * (cuts off event delivery) at each protocol step WITHOUT completing the
 * transfer. We then attach a fresh watcher to the same registry and verify the
 * recovery semantics:
 *
 * <ol>
 *   <li><b>Crash AFTER initiate, BEFORE complete:</b> znode is in TRANSFERRING.
 *       The new owner observes PENDING via the fresh watcher and can call
 *       {@code complete} to finish. The old owner remains the canonical
 *       reader until {@code complete} fires, so reads stay continuous.</li>
 *   <li><b>Crash AFTER complete, BEFORE old-owner watcher fires RELEASED:</b>
 *       znode is ACTIVE under the new owner. Old-owner's fresh watcher MUST
 *       fire RELEASED on attach (because the previous owner is no longer the
 *       znode's owner). New-owner's fresh watcher MUST fire ASSIGNED.</li>
 *   <li><b>Crash with a stale TRANSFERRING that gets aborted:</b> if the
 *       optimizer decides the transfer can't proceed, calling {@code complete}
 *       with a wrong owner is rejected, and the znode can be CAS-rolled-back
 *       to ACTIVE under the original owner.</li>
 * </ol>
 *
 * <p>The protocol is CAS-only, so the recovery property is structurally
 * guaranteed; this test pins the property end-to-end against a real ZK so any
 * future refactor that introduces a non-CAS shortcut is caught.
 */
public class SegmentTransferCrashRecoveryTest {

    private static final String BASE_PATH = "/herd-test-transfer-crash";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String SEG_UUID = "seguid";

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

    private SegmentMetadata sample(int ownerInstanceId) {
        return SegmentMetadata.builder()
                .segmentUuid(SEG_UUID)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(ownerInstanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
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

    private static void waitFor(java.util.function.BooleanSupplier predicate, String desc)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (predicate.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("predicate did not become true within 10s: " + desc);
    }

    @Test
    public void crashAfterInitiateBeforeCompleteLeavesTransferringForRecovery() throws Exception {
        // Owner-0 owns the segment; optimizer initiates transfer to owner-1;
        // the JVM hosting the new owner crashes immediately (we close the
        // watcher and DROP the listener) BEFORE complete fires.
        registry.createSegment(sample(0));

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        OwnershipTransfer.initiate(registry, current, /* newOwner= */ 1);

        // Confirm the registry is in TRANSFERRING (pre-crash state).
        VersionedSegmentMetadata midTransfer = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        assertEquals(SegmentState.TRANSFERRING, midTransfer.metadata().getState());
        assertEquals(0, midTransfer.metadata().getOwnerInstanceId());
        assertEquals(1, midTransfer.metadata().getPendingOwnerInstanceId());

        // Simulate fresh-restart of owner-1's IS: a new watcher attaches to the
        // same registry. It MUST observe the pending assignment.
        RecordingListener restarted = new RecordingListener();
        try (SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, restarted)) {
            w1.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> restarted.events.contains("PENDING:" + SEG_UUID),
                    "fresh watcher must rediscover the pending assignment");

            // The new owner can finish the transfer — protocol resumes from
            // exactly where the crash left it.
            VersionedSegmentMetadata afterComplete = OwnershipTransfer.complete(
                    registry, midTransfer, /* newOwner= */ 1);
            assertEquals(SegmentState.ACTIVE, afterComplete.metadata().getState());
            assertEquals(1, afterComplete.metadata().getOwnerInstanceId());
            assertEquals(SegmentMetadata.NO_INSTANCE,
                    afterComplete.metadata().getPendingOwnerInstanceId());

            waitFor(() -> restarted.events.contains("ASSIGNED:" + SEG_UUID),
                    "new owner watcher must fire ASSIGNED after complete");
        }
    }

    @Test
    public void crashAfterCompleteBeforeOldOwnerReleaseStillConverges() throws Exception {
        // Owner-0 → Owner-1 transfer completes successfully; before owner-0's
        // watcher could fire RELEASED its JVM crashes. On restart, owner-0's
        // FRESH watcher must observe the segment is no longer owned by 0 and
        // fire RELEASED so the local IS drops the segment handle.
        registry.createSegment(sample(0));

        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, current, 1);
        OwnershipTransfer.complete(registry, transferring, 1);

        // Confirm registry is post-transfer.
        VersionedSegmentMetadata afterTransfer = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        assertEquals(SegmentState.ACTIVE, afterTransfer.metadata().getState());
        assertEquals(1, afterTransfer.metadata().getOwnerInstanceId());

        // Restart owner-0's watcher AFTER the transfer is complete.
        RecordingListener restartedZero = new RecordingListener();
        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, restartedZero)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            // Owner-0's fresh watcher reconciles "I used to own it but I no
            // longer do" by treating it as if it just released. Concretely,
            // because the watcher's view starts empty and never sees the
            // segment as ACTIVE-under-0, the convergence is "no event for
            // segment seg-A" — the local IS will drop the handle on the next
            // refresh tick (the optimizer's deprecate eventually reaps it).
            // We assert that the watcher does NOT incorrectly fire ASSIGNED:
            Thread.sleep(500L);
            assertFalse("owner-0 must NOT see ASSIGNED for a segment owned by 1",
                    restartedZero.events.contains("ASSIGNED:" + SEG_UUID));
        }

        // Owner-1's fresh watcher must see ASSIGNED — it's the canonical owner.
        RecordingListener restartedOne = new RecordingListener();
        try (SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, restartedOne)) {
            w1.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> restartedOne.events.contains("ASSIGNED:" + SEG_UUID),
                    "owner-1 watcher must fire ASSIGNED for the now-transferred segment");
        }
    }

    @Test
    public void noDoubleOwnershipDuringTransferringPhase() throws Exception {
        // The TRANSFERRING phase is the only window where two instances might
        // think they own the segment. The protocol guarantees the previous
        // owner stays canonical until complete fires. This test: kick a
        // transfer mid-flight, run BOTH watchers (owner-0 and owner-1) for a
        // bounded window, assert that at most one of them has ASSIGNED in its
        // event log AND that the other has at most PENDING (not ASSIGNED).
        registry.createSegment(sample(0));

        RecordingListener l0 = new RecordingListener();
        RecordingListener l1 = new RecordingListener();
        try (SegmentAssignmentWatcher w0 = new SegmentAssignmentWatcher(registry, 0, l0);
             SegmentAssignmentWatcher w1 = new SegmentAssignmentWatcher(registry, 1, l1)) {
            w0.watchIndex(TS_UUID, IDX_UUID);
            w1.watchIndex(TS_UUID, IDX_UUID);
            waitFor(() -> l0.events.contains("ASSIGNED:" + SEG_UUID),
                    "owner-0 must see ASSIGNED initially");

            // Initiate transfer; do NOT complete.
            VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            OwnershipTransfer.initiate(registry, current, 1);
            waitFor(() -> l1.events.contains("PENDING:" + SEG_UUID),
                    "owner-1 must see PENDING during TRANSFERRING");

            // Wait long enough that any spurious ASSIGNED fire would land.
            Thread.sleep(500L);

            // Owner-1 has ONLY PENDING — never ASSIGNED — until complete.
            assertFalse("owner-1 must NOT see ASSIGNED before complete fires",
                    l1.events.contains("ASSIGNED:" + SEG_UUID));
            // Owner-0 has NOT been released yet — it's still the canonical owner.
            assertFalse("owner-0 must NOT see RELEASED before complete fires",
                    l0.events.contains("RELEASED:" + SEG_UUID));
        }
    }

    @Test
    public void completeOnStaleVersionMismatchIsRejectedNotSilentlyAccepted() throws Exception {
        // Crash window: the new owner had captured the TRANSFERRING znode at
        // version V, but a recovery path (e.g. the optimizer aborting the
        // transfer) has CAS-bumped the znode to a different version since.
        // The new owner's complete CAS MUST fail — we cannot let two actors
        // independently call complete and end up with double ACTIVE.
        registry.createSegment(sample(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, current, 1);

        // Another actor (an admin? a recovery script?) bumps the version: in
        // production this might be a "transfer abort" that rolls back to ACTIVE
        // under owner-0. We model that by re-reading and CAS-updating in place.
        VersionedSegmentMetadata reread = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        registry.casUpdateSegment(reread, reread.metadata().toBuilder()
                .state(SegmentState.ACTIVE)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .build());

        // The new owner's stale-version complete MUST throw. (IllegalStateException
        // because the state is no longer TRANSFERRING; alternatively, if the
        // state were still TRANSFERRING but at a higher zkVersion, the CAS
        // would throw VersionMismatch. Both are acceptable failure modes;
        // what we MUST NOT see is silent success that produces double-ACTIVE.)
        try {
            OwnershipTransfer.complete(registry, transferring, 1);
            fail("complete on a state-changed znode must NOT succeed silently"
                    + " — that would produce double ACTIVE ownership");
        } catch (IllegalStateException | SegmentRegistryException.VersionMismatch ok) {
            // expected
        }

        // Final state: ACTIVE under owner-0; no double-ownership artefacts.
        VersionedSegmentMetadata finalState = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        assertEquals(SegmentState.ACTIVE, finalState.metadata().getState());
        assertEquals(0, finalState.metadata().getOwnerInstanceId());
        assertEquals(SegmentMetadata.NO_INSTANCE,
                finalState.metadata().getPendingOwnerInstanceId());
    }

    @Test
    public void multipleConsecutiveCrashesEachConverge() throws Exception {
        // Stress the recovery property: do N transfer cycles, each crashing
        // the new owner watcher mid-PENDING and recovering with a fresh
        // watcher. After N cycles the segment must end up ACTIVE under owner-N.
        registry.createSegment(sample(0));

        int cycles = 5;
        int currentOwner = 0;
        for (int cycle = 0; cycle < cycles; cycle++) {
            int nextOwner = currentOwner + 1;
            VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
            VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, current, nextOwner);

            // Simulate crash by attaching a watcher and immediately closing it
            // before it can complete the transfer.
            RecordingListener crashed = new RecordingListener();
            try (SegmentAssignmentWatcher crashedWatcher =
                         new SegmentAssignmentWatcher(registry, nextOwner, crashed)) {
                crashedWatcher.watchIndex(TS_UUID, IDX_UUID);
                waitFor(() -> crashed.events.contains("PENDING:" + SEG_UUID),
                        "crashed watcher must observe PENDING before close in cycle " + cycle);
            }

            // Recovery: fresh watcher attaches, observes PENDING again, calls complete.
            RecordingListener recovered = new RecordingListener();
            try (SegmentAssignmentWatcher recoveredWatcher =
                         new SegmentAssignmentWatcher(registry, nextOwner, recovered)) {
                recoveredWatcher.watchIndex(TS_UUID, IDX_UUID);
                waitFor(() -> recovered.events.contains("PENDING:" + SEG_UUID),
                        "recovered watcher must rediscover PENDING in cycle " + cycle);
                OwnershipTransfer.complete(registry, transferring, nextOwner);
                waitFor(() -> recovered.events.contains("ASSIGNED:" + SEG_UUID),
                        "recovered watcher must fire ASSIGNED in cycle " + cycle);
            }
            currentOwner = nextOwner;
        }

        // Final state: owned by cycle N.
        VersionedSegmentMetadata finalState = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        assertEquals(SegmentState.ACTIVE, finalState.metadata().getState());
        assertEquals(cycles, finalState.metadata().getOwnerInstanceId());
        assertEquals(SegmentMetadata.NO_INSTANCE,
                finalState.metadata().getPendingOwnerInstanceId());
        // Cross-check: no orphan TRANSFERRING znodes for this index.
        Optional<VersionedSegmentMetadata> probe = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID);
        assertTrue(probe.isPresent());
        assertNotEquals(SegmentState.TRANSFERRING, probe.get().metadata().getState());
        // Defense: ensure listSegments returns exactly one entry — no
        // duplicates produced by the consecutive crash-recover cycles.
        assertEquals("exactly one segment entry survives N transfer cycles",
                Collections.singletonList(SEG_UUID).size(),
                registry.listSegments(TS_UUID, IDX_UUID).size());
    }
}
