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
 * Pins what happens when DROP_INDEX or TRUNCATE_TABLE fires while a segment is
 * actively in the middle of an ownership transfer. The new owner that has
 * already started the takeover protocol must NOT be able to {@code complete}
 * onto a segment whose registry znode has been swept under it. The
 * registry-cleanup path on the old owner must remove ALL znodes for the
 * index regardless of state — including TRANSFERRING ones — and the new
 * owner's CAS-complete must observe that the znode is gone and abort with a
 * documented failure.
 */
public class TruncateDuringTransferTest {

    private static final String BASE_PATH = "/herd-test-truncate-transfer";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String SEG_UUID = "seguid";
    private static final String IDX_NAME = "docs_v1";
    private static final String TBL_NAME = "docs";

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
                .tableName(TBL_NAME)
                .indexUuid(IDX_UUID)
                .indexName(IDX_NAME)
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(ownerInstanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void dropAllSegmentsReapsTransferringSegment() throws Exception {
        // Setup: segment owned by 0, transfer initiated to 1 (now TRANSFERRING).
        registry.createSegment(sample(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        OwnershipTransfer.initiate(registry, current, /* newOwner= */ 1);

        VersionedSegmentMetadata midTransfer = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        assertEquals(SegmentState.TRANSFERRING, midTransfer.metadata().getState());

        // DROP_INDEX fires on the IS that owned the segment. The publisher's
        // sweep must remove the TRANSFERRING znode too.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, /* instanceId= */ 0);
        publisher.dropAllSegmentsForIndex();

        Optional<VersionedSegmentMetadata> after = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID);
        assertFalse("DROP_INDEX must reap TRANSFERRING znodes — leaving them would let a"
                        + " new owner complete the transfer onto an index that no longer exists",
                after.isPresent());
    }

    @Test
    public void newOwnerCannotCompleteAfterDropIndexSwept() throws Exception {
        // Setup: transfer in flight; new owner has captured the TRANSFERRING
        // znode reference but has NOT yet called complete.
        registry.createSegment(sample(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        VersionedSegmentMetadata transferring = OwnershipTransfer.initiate(registry, current, 1);

        // DROP fires on the original owner.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);
        publisher.dropAllSegmentsForIndex();

        // The new owner now tries to complete the transfer using its
        // pre-captured stale reference. The CAS MUST fail — there is no
        // znode at that path anymore. We accept either a clean
        // VersionMismatch (znode missing detected by the CAS layer) or
        // a SegmentRegistryException with NoNodeException-like cause —
        // anything but silent success that would resurrect a dropped
        // index's segment.
        try {
            OwnershipTransfer.complete(registry, transferring, 1);
            fail("complete() must NOT succeed against a swept registry — that would"
                    + " resurrect a segment whose multipart files have just been deleted");
        } catch (SegmentRegistryException expected) {
            // OK — registry-layer rejection.
        } catch (RuntimeException expected) {
            // OK — defensive layer rejection.
        }

        // Final state: registry stays empty for the index.
        assertTrue(registry.listSegments(TS_UUID, IDX_UUID).isEmpty());
    }

    @Test
    public void dropAllSegmentsLeavesPendingOwnerNoChannel() throws Exception {
        // Adversarial: optimizer initiates transfer, IS-side DROP fires,
        // the new owner (instance 1) gets nothing. There must be no
        // surviving registry channel through which instance 1 could load
        // the segment — neither in TRANSFERRING nor in any other state.
        registry.createSegment(sample(0));
        VersionedSegmentMetadata current = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
        OwnershipTransfer.initiate(registry, current, 1);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);
        publisher.dropAllSegmentsForIndex();

        // The new owner's listSegments shows nothing.
        assertTrue("new owner must not see any segment for the dropped index",
                registry.listSegments(TS_UUID, IDX_UUID).isEmpty());
    }

    @Test
    public void truncateDuringMultiSegmentTransferReapsAll() throws Exception {
        // Multi-segment scenario: segment A is ACTIVE, segment B is in
        // TRANSFERRING, segment C is DEPRECATED (just after the optimizer
        // merged it). TRUNCATE fires; ALL three znodes must go.
        registry.createSegment(SegmentMetadata.builder()
                .segmentUuid("A").tablespaceUuid(TS_UUID).tableName(TBL_NAME)
                .indexUuid(IDX_UUID).indexName(IDX_NAME)
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 1L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build());
        registry.createSegment(SegmentMetadata.builder()
                .segmentUuid("B").tablespaceUuid(TS_UUID).tableName(TBL_NAME)
                .indexUuid(IDX_UUID).indexName(IDX_NAME)
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 2L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build());
        registry.createSegment(SegmentMetadata.builder()
                .segmentUuid("C").tablespaceUuid(TS_UUID).tableName(TBL_NAME)
                .indexUuid(IDX_UUID).indexName(IDX_NAME)
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 3L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build());

        VersionedSegmentMetadata bv = registry.getSegment(TS_UUID, IDX_UUID, "B").orElseThrow();
        OwnershipTransfer.initiate(registry, bv, 1);
        VersionedSegmentMetadata cv = registry.getSegment(TS_UUID, IDX_UUID, "C").orElseThrow();
        registry.casUpdateSegment(cv, cv.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(java.util.Collections.singletonList("merged"))
                .retentionUntilEpochMillis(System.currentTimeMillis() + 600_000L)
                .build());

        assertEquals(3, registry.listSegments(TS_UUID, IDX_UUID).size());

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);
        publisher.dropAllSegmentsForIndex();

        assertTrue("TRUNCATE must reap every znode regardless of state (ACTIVE, "
                        + "TRANSFERRING, DEPRECATED — all three must be gone)",
                registry.listSegments(TS_UUID, IDX_UUID).isEmpty());
    }

    @Test
    public void recreateAfterTruncateStartsCleanRegistry() throws Exception {
        // After TRUNCATE_TABLE, the IS engine re-creates the vector store
        // with a FRESH storage UUID (issue #383 review). The new store can
        // attach a publisher and start registering segments under the SAME
        // indexUuid (the index-name-and-uuid mapping in SchemaTracker is
        // unchanged across truncate). Verify that:
        //
        // 1. The post-sweep registry is empty.
        // 2. A subsequent createSegment lands cleanly.
        // 3. listSegments returns ONLY the new segment, not any ghosts.
        registry.createSegment(sample(0));

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TS_UUID, TBL_NAME, IDX_UUID, IDX_NAME, 0);
        publisher.dropAllSegmentsForIndex();
        assertTrue(registry.listSegments(TS_UUID, IDX_UUID).isEmpty());

        // Re-create with a different segmentUuid (the new store's first
        // checkpoint produces a fresh UUID — same indexUuid scope).
        registry.createSegment(SegmentMetadata.builder()
                .segmentUuid("post-truncate")
                .tablespaceUuid(TS_UUID).tableName(TBL_NAME)
                .indexUuid(IDX_UUID).indexName(IDX_NAME)
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(2L, 1L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build());

        assertEquals(1, registry.listSegments(TS_UUID, IDX_UUID).size());
        assertEquals("post-truncate",
                registry.listSegments(TS_UUID, IDX_UUID).get(0).metadata().getSegmentUuid());
    }
}
