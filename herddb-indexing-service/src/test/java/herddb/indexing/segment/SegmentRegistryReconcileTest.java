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
import herddb.indexing.vector.NewSegmentInfo;
import herddb.log.LogSequenceNumber;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
 * Validates the staged-publish + reconcile-on-restart machinery introduced for
 * review item A1+A3.
 *
 * <p>Scenarios:
 * <ol>
 *   <li>Crash between stage and IndexStatus persist → orphan PROVISIONAL znode is
 *       NOT in IndexStatus → reconcile drops it.</li>
 *   <li>Crash between IndexStatus persist and commit → PROVISIONAL znode IS in
 *       IndexStatus → reconcile promotes it to ACTIVE.</li>
 *   <li>Successful checkpoint with subsequent commit → segment is ACTIVE; reconcile
 *       on next start is a no-op for it.</li>
 *   <li>IndexStatus has a UUID for which there is no znode at all → reconcile
 *       re-registers it as ACTIVE (transient publisher failure recovery).</li>
 *   <li>commitStagedSegments retried after success → idempotent (no-op).</li>
 * </ol>
 */
public class SegmentRegistryReconcileTest {

    private static final String BASE_PATH = "/herd-test-A1A3";
    private static final String TABLE_SPACE = "tsuid";
    private static final String TABLE_NAME = "docs";
    private static final String INDEX_UUID = "idxuid";
    private static final String INDEX_NAME = "docs_v1";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private SegmentRegistryPublisher publisher;

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
        publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, /* instanceId= */ 0);
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

    private static NewSegmentInfo info(String uuid, int segmentId) {
        return new NewSegmentInfo(segmentId, uuid,
                "g/" + uuid, 100L, "m/" + uuid, 100L, 200L, 10L, 1L,
                new LogSequenceNumber(1L, 100L), /* jvectorFeatureIds */ null);
    }

    @Test
    public void stageThenCommitTransitionsThroughProvisionalToActive() throws Exception {
        NewSegmentInfo info = info("seg-A", 1);
        publisher.stageNewSegments(Collections.singletonList(info));
        Optional<VersionedSegmentMetadata> staged = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-A");
        assertTrue(staged.isPresent());
        assertEquals(SegmentState.PROVISIONAL, staged.get().metadata().getState());

        publisher.commitStagedSegments(Collections.singletonList(info));
        Optional<VersionedSegmentMetadata> committed = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-A");
        assertTrue(committed.isPresent());
        assertEquals(SegmentState.ACTIVE, committed.get().metadata().getState());
    }

    @Test
    public void reconcileDropsOrphanProvisionalNotInIndexStatus() throws Exception {
        // Simulate "crash between stage and IndexStatus persist": PROVISIONAL znode
        // exists but the segment is NOT in IndexStatus. Reconcile must drop it.
        NewSegmentInfo orphan = info("seg-orphan", 1);
        publisher.stageNewSegments(Collections.singletonList(orphan));
        assertEquals(SegmentState.PROVISIONAL,
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-orphan").orElseThrow()
                        .metadata().getState());

        // IndexStatus contains nothing → orphan is dropped.
        publisher.reconcileWithIndexStatus(Collections.emptyList());

        assertFalse("orphan PROVISIONAL must be dropped",
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-orphan").isPresent());
    }

    @Test
    public void reconcilePromotesProvisionalThatIsInIndexStatus() throws Exception {
        // Simulate "crash between IndexStatus persist and commit": PROVISIONAL znode
        // exists AND its UUID is in IndexStatus. Reconcile must promote it.
        NewSegmentInfo persisted = info("seg-recovered", 1);
        publisher.stageNewSegments(Collections.singletonList(persisted));

        publisher.reconcileWithIndexStatus(Collections.singletonList(persisted));

        VersionedSegmentMetadata after = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-recovered")
                .orElseThrow();
        assertEquals(SegmentState.ACTIVE, after.metadata().getState());
    }

    @Test
    public void reconcileReRegistersIndexStatusUuidWithNoZnode() throws Exception {
        // Simulate "publisher failed transiently": IndexStatus has a UUID but the
        // znode was never created. Reconcile must register it as ACTIVE.
        NewSegmentInfo recovered = info("seg-needs-create", 7);
        // No prior stageNewSegments call; ZK is empty for this segment.
        assertFalse(registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-needs-create").isPresent());

        publisher.reconcileWithIndexStatus(Collections.singletonList(recovered));

        VersionedSegmentMetadata after = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-needs-create")
                .orElseThrow();
        assertEquals(SegmentState.ACTIVE, after.metadata().getState());
        assertEquals("g/seg-needs-create", after.metadata().getGraphPath());
    }

    @Test
    public void reconcileLeavesActiveSegmentsUntouched() throws Exception {
        NewSegmentInfo committed = info("seg-stable", 1);
        publisher.stageNewSegments(Collections.singletonList(committed));
        publisher.commitStagedSegments(Collections.singletonList(committed));
        VersionedSegmentMetadata before = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-stable")
                .orElseThrow();

        publisher.reconcileWithIndexStatus(Collections.singletonList(committed));

        VersionedSegmentMetadata after = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-stable")
                .orElseThrow();
        assertEquals(SegmentState.ACTIVE, after.metadata().getState());
        // Version unchanged because reconcile did not touch this znode.
        assertEquals(before.zkVersion(), after.zkVersion());
    }

    @Test
    public void commitRetryAfterSuccessIsIdempotent() throws Exception {
        NewSegmentInfo info = info("seg-A", 1);
        publisher.stageNewSegments(Collections.singletonList(info));
        publisher.commitStagedSegments(Collections.singletonList(info));
        VersionedSegmentMetadata afterFirstCommit = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-A")
                .orElseThrow();
        // Second commit (e.g. retry from a stuck thread) must not bump anything.
        publisher.commitStagedSegments(Collections.singletonList(info));
        VersionedSegmentMetadata afterSecondCommit = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-A")
                .orElseThrow();
        assertEquals(afterFirstCommit.zkVersion(), afterSecondCommit.zkVersion());
        assertEquals(SegmentState.ACTIVE, afterSecondCommit.metadata().getState());
    }

    @Test
    public void commitWithoutStageCreatesActiveDirectly() throws Exception {
        // Single-phase legacy caller: commitStagedSegments invoked without a prior
        // stageNewSegments. The publisher must still leave the registry in ACTIVE state.
        NewSegmentInfo info = info("seg-direct", 5);
        publisher.commitStagedSegments(Collections.singletonList(info));
        assertEquals(SegmentState.ACTIVE,
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-direct").orElseThrow()
                        .metadata().getState());
    }

    @Test
    public void reconcileMultipleScenariosInOneCall() throws Exception {
        // A: PROVISIONAL with no IndexStatus entry → drop
        NewSegmentInfo orphan = info("seg-A", 1);
        publisher.stageNewSegments(Collections.singletonList(orphan));
        // B: PROVISIONAL with IndexStatus entry → promote
        NewSegmentInfo recovered = info("seg-B", 2);
        publisher.stageNewSegments(Collections.singletonList(recovered));
        // C: missing znode with IndexStatus entry → create ACTIVE
        NewSegmentInfo needsCreate = info("seg-C", 3);
        // D: ACTIVE with IndexStatus entry → leave alone
        NewSegmentInfo stable = info("seg-D", 4);
        publisher.stageNewSegments(Collections.singletonList(stable));
        publisher.commitStagedSegments(Collections.singletonList(stable));

        // IndexStatus = {B, C, D}. Reconcile.
        publisher.reconcileWithIndexStatus(Arrays.asList(recovered, needsCreate, stable));

        assertFalse(registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-A").isPresent());
        assertEquals(SegmentState.ACTIVE,
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-B").orElseThrow().metadata().getState());
        assertEquals(SegmentState.ACTIVE,
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-C").orElseThrow().metadata().getState());
        assertEquals(SegmentState.ACTIVE,
                registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-D").orElseThrow().metadata().getState());

        List<VersionedSegmentMetadata> all = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertEquals(3, all.size());
    }
}
