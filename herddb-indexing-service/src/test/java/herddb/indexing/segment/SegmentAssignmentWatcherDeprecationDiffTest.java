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
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.List;
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
 * Unit tests for {@link SegmentAssignmentWatcher} focusing on:
 * <ol>
 *   <li>An owned ACTIVE segment that transitions to DEPRECATED triggers
 *       {@code onSegmentReleased} even though the ownerInstanceId has not
 *       changed (issue #514 fix in {@code collectDiff}).</li>
 *   <li>When a scan sees new ACTIVE segments <em>and</em> existing DEPRECATED
 *       segments in the same tick, all {@code onSegmentAssigned} events are
 *       delivered before any {@code onSegmentReleased} event — preventing a
 *       zero-segment search window.</li>
 * </ol>
 *
 * <p>Uses a real {@code TestingServer} ZooKeeper — no mocking. No BookKeeper,
 * no ZKTestEnv, so no {@code @Category(ClusterTest.class)} needed.
 */
public class SegmentAssignmentWatcherDeprecationDiffTest {

    private static final String BASE_PATH = "/herd-test-deprecation-diff";
    private static final String TS_UUID = "ts-514-diff";
    private static final String IDX_UUID = "idx-514-diff";
    private static final int OWNER = 1;

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30_000, ev -> {
            if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Build an ACTIVE segment owned by {@link #OWNER} in the default index. */
    private static SegmentMetadata activeSegment(String uuid, long segId) {
        return buildSegment(uuid, segId, TS_UUID, IDX_UUID);
    }

    /** Build an ACTIVE segment owned by {@link #OWNER} in the given tablespace/index. */
    private static SegmentMetadata buildSegment(String uuid, long segId,
            String tsUuid, String idxUuid) {
        return SegmentMetadata.builder()
                .segmentUuid(uuid).tablespaceUuid(tsUuid).tableName("docs")
                .indexUuid(idxUuid).indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(OWNER)
                .segmentId(segId)
                .graphPath(idxUuid + "_seg" + segId + "_graph")
                .mapPath(idxUuid + "_seg" + segId + "_map")
                .sizeBytes(1000L).mapFileSize(100L).vectorCount(10L)
                .generation(1L).createdAtEpochMillis(0L)
                .baseLsn(new LogSequenceNumber(1L, segId))
                .build();
    }

    private static SegmentMetadata deprecate(SegmentMetadata m) {
        return m.toBuilder().state(SegmentState.DEPRECATED).build();
    }

    private VersionedSegmentMetadata getVersioned(String segUuid) throws Exception {
        return registry.getSegment(TS_UUID, IDX_UUID, segUuid).orElseThrow();
    }

    private VersionedSegmentMetadata getVersioned(String tsUuid, String idxUuid,
            String segUuid) throws Exception {
        return registry.getSegment(tsUuid, idxUuid, segUuid).orElseThrow();
    }

    // -------------------------------------------------------------------------
    // Test 1: ACTIVE → DEPRECATED triggers onSegmentReleased
    // -------------------------------------------------------------------------

    /**
     * A single owned ACTIVE segment that transitions to DEPRECATED must fire
     * {@code onSegmentReleased}, even though the {@code ownerInstanceId} does
     * not change.
     */
    @Test
    public void deprecatedTransitionFiresRelease() throws Exception {
        SegmentMetadata seg = activeSegment("seg-dep-1", 101L);
        registry.createSegment(seg);

        List<String> assigned = new ArrayList<>();
        List<String> released = new ArrayList<>();

        SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                registry, OWNER,
                new SegmentAssignmentListener() {
                    @Override
                    public void onSegmentAssigned(VersionedSegmentMetadata vsm) {
                        assigned.add(vsm.metadata().getSegmentUuid());
                    }
                    @Override
                    public void onSegmentReleased(SegmentMetadata previous) {
                        released.add(previous == null ? "<null>" : previous.getSegmentUuid());
                    }
                });
        try {
            // Initial scan: segment is ACTIVE, owned by OWNER → onSegmentAssigned fires.
            watcher.watchIndex(TS_UUID, IDX_UUID);
            assertEquals("initial scan: 1 assignment", 1, assigned.size());
            assertEquals("seg-dep-1", assigned.get(0));
            assertEquals("initial scan: 0 releases", 0, released.size());

            // Transition the segment to DEPRECATED.
            VersionedSegmentMetadata v = getVersioned("seg-dep-1");
            registry.casUpdateSegment(v, deprecate(seg));

            // Re-scan (as a watcher fire would do).
            watcher.scanIndex(new SegmentAssignmentWatcher.IndexKey(TS_UUID, IDX_UUID));

            assertEquals("after deprecation: still 1 assignment (no new segment)", 1, assigned.size());
            assertEquals("after deprecation: 1 release must fire", 1, released.size());
            assertEquals("seg-dep-1", released.get(0));
        } finally {
            watcher.close();
        }
    }

    // -------------------------------------------------------------------------
    // Test 2: assignments fire before releases in the same scan
    // -------------------------------------------------------------------------

    /**
     * When a scan tick sees both a new ACTIVE merged segment and the deprecated
     * inputs in one go, all {@code onSegmentAssigned} callbacks must be
     * delivered before any {@code onSegmentReleased} callback.
     *
     * <p>The test manufactures this scenario by:
     * <ol>
     *   <li>Creating 3 ACTIVE segments and doing an initial scan so the watcher
     *       populates its {@code known} map (as if those segments had been
     *       adopted previously).</li>
     *   <li>Closing the watcher's background executor so no async watcher fires
     *       can race with the ZK state mutations that follow.</li>
     *   <li>Deprecating the 3 inputs and creating 1 new ACTIVE merged segment
     *       — all <em>without</em> any scan running.</li>
     *   <li>Calling {@link SegmentAssignmentWatcher#scanIndex} directly
     *       (one synchronous call) and asserting that ALL
     *       {@code onSegmentAssigned} events precede ALL
     *       {@code onSegmentReleased} events in the delivered order.</li>
     * </ol>
     *
     * <p>This test uses index-key "idx-order" so it does not share ZK state
     * with the other tests (which use {@link #IDX_UUID}).
     */
    @Test
    public void assignmentsFireBeforeReleasesInSameScan() throws Exception {
        final String ts  = "ts-order";
        final String idx = "idx-order";

        // Create 3 ACTIVE owned segments in ZK.
        SegmentMetadata seg1 = buildSegment("seg-ord-1", 201L, ts, idx);
        SegmentMetadata seg2 = buildSegment("seg-ord-2", 202L, ts, idx);
        SegmentMetadata seg3 = buildSegment("seg-ord-3", 203L, ts, idx);
        registry.createSegment(seg1);
        registry.createSegment(seg2);
        registry.createSegment(seg3);

        List<String> eventOrder = new ArrayList<>();
        List<String> assigned = new ArrayList<>();
        List<String> released = new ArrayList<>();

        SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                registry, OWNER,
                new SegmentAssignmentListener() {
                    @Override
                    public void onSegmentAssigned(VersionedSegmentMetadata vsm) {
                        assigned.add(vsm.metadata().getSegmentUuid());
                        eventOrder.add("ASSIGN:" + vsm.metadata().getSegmentUuid());
                    }
                    @Override
                    public void onSegmentReleased(SegmentMetadata previous) {
                        String uuid = previous == null ? "<null>" : previous.getSegmentUuid();
                        released.add(uuid);
                        eventOrder.add("RELEASE:" + uuid);
                    }
                });
        try {
            // Initial scan: 3 ACTIVE, all owned → 3 assignments; populates known.
            watcher.watchIndex(ts, idx);
            assertEquals("initial scan: 3 assignments", 3, assigned.size());
            assertEquals("initial scan: 0 releases", 0, released.size());
            eventOrder.clear();

            // Close the watcher so no async ZK watcher fires can run between
            // the state mutations below and the direct scanIndex call.
            // After close(), dispatchScan short-circuits (closed=true), so
            // the ZK events from casUpdate fire but are silently discarded.
            // scanIndex itself does NOT check closed, so the direct call below
            // still executes.
            watcher.close();

            // Simulate what the optimizer does atomically from the watcher's
            // point of view: deprecate 3 inputs, create 1 new merged segment.
            VersionedSegmentMetadata v1 = getVersioned(ts, idx, "seg-ord-1");
            VersionedSegmentMetadata v2 = getVersioned(ts, idx, "seg-ord-2");
            VersionedSegmentMetadata v3 = getVersioned(ts, idx, "seg-ord-3");
            registry.casUpdateSegment(v1, deprecate(seg1));
            registry.casUpdateSegment(v2, deprecate(seg2));
            registry.casUpdateSegment(v3, deprecate(seg3));

            SegmentMetadata merged = buildSegment("seg-merged", 204L, ts, idx);
            registry.createSegment(merged);

            // Single direct scan — no async interference.
            watcher.scanIndex(new SegmentAssignmentWatcher.IndexKey(ts, idx));

            // Verify: 1 new assignment (the merged segment) happened.
            assertTrue("merged segment must be assigned", assigned.contains("seg-merged"));
            // Verify: 3 releases.
            assertEquals("3 deprecated segments must be released", 3, released.size());

            // CRITICAL ordering: all ASSIGN events must precede all RELEASE events.
            int lastAssignIdx = -1;
            int firstReleaseIdx = Integer.MAX_VALUE;
            for (int i = 0; i < eventOrder.size(); i++) {
                String e = eventOrder.get(i);
                if (e.startsWith("ASSIGN:")) {
                    lastAssignIdx = i;
                } else if (e.startsWith("RELEASE:")) {
                    if (i < firstReleaseIdx) {
                        firstReleaseIdx = i;
                    }
                }
            }
            assertTrue("all ASSIGN events must come before any RELEASE event; order was: "
                            + eventOrder,
                    lastAssignIdx < firstReleaseIdx);
        } finally {
            // close() is idempotent; watcher may already be closed above.
            try {
                watcher.close();
            } catch (Exception ignored) {
                // Intentional: second close() after deliberate first close() above.
            }
        }
    }

    /**
     * Corner case: a segment that is NOT owned by this instance transitioning
     * to DEPRECATED must NOT fire onSegmentReleased.
     */
    @Test
    public void deprecationOfUnownedSegmentDoesNotFireRelease() throws Exception {
        int otherOwner = OWNER + 1;
        SegmentMetadata seg = SegmentMetadata.builder()
                .segmentUuid("seg-other").tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(otherOwner)   // owned by a DIFFERENT instance
                .segmentId(301L)
                .graphPath(IDX_UUID + "_seg301_graph")
                .mapPath(IDX_UUID + "_seg301_map")
                .sizeBytes(500L).mapFileSize(50L).vectorCount(5L)
                .generation(1L).createdAtEpochMillis(0L)
                .baseLsn(new LogSequenceNumber(1L, 301L))
                .build();
        registry.createSegment(seg);

        List<String> released = new ArrayList<>();

        SegmentAssignmentWatcher watcher = new SegmentAssignmentWatcher(
                registry, OWNER,  // instance OWNER watches, but seg is owned by otherOwner
                new SegmentAssignmentListener() {
                    @Override
                    public void onSegmentReleased(SegmentMetadata previous) {
                        released.add(previous == null ? "<null>" : previous.getSegmentUuid());
                    }
                });
        try {
            watcher.watchIndex(TS_UUID, IDX_UUID);

            // Transition unowned segment to DEPRECATED.
            VersionedSegmentMetadata v = getVersioned("seg-other");
            registry.casUpdateSegment(v,
                    seg.toBuilder().state(SegmentState.DEPRECATED).build());
            watcher.scanIndex(new SegmentAssignmentWatcher.IndexKey(TS_UUID, IDX_UUID));

            assertEquals("unowned segment deprecation must NOT fire onSegmentReleased",
                    0, released.size());
        } finally {
            watcher.close();
        }
    }
}
