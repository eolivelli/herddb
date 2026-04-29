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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.file.FileDataStorageManager;
import herddb.log.LogSequenceNumber;
import java.nio.file.Path;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test for {@link TombstoneOverlayManager}: drives a real
 * {@link FileDataStorageManager} (which fully implements the multipart API)
 * and a curator-test {@link TestingServer} ZooKeeper, then verifies overlay
 * upload + znode CAS + reload roundtrip.
 */
public class TombstoneOverlayManagerTest {

    private static final String BASE_PATH = "/herd-test-step3";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";
    private static final String SEG_UUID = "seguid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private FileDataStorageManager dsm;
    private Path tmpDirectory;

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

        Path baseDir = tmpFolder.newFolder("data").toPath();
        dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TS_UUID);
        tmpDirectory = tmpFolder.newFolder("tmp").toPath();
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

    private VersionedSegmentMetadata createActiveSegment() throws Exception {
        SegmentMetadata segment = SegmentMetadata.builder()
                .segmentUuid(SEG_UUID)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .graphPath(TS_UUID + "/" + IDX_UUID + "_seg/" + SEG_UUID + "/graph")
                .mapPath(TS_UUID + "/" + IDX_UUID + "_seg/" + SEG_UUID + "/map")
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(1_700_000_000L)
                .build();
        registry.createSegment(segment);
        return registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
    }

    @Test
    public void emptyManagerWithNoTombstonesReturnsEmptyFlush() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        Optional<VersionedSegmentMetadata> result = mgr.flush(current);
        assertFalse("empty flush should be a no-op", result.isPresent());
    }

    @Test
    public void flushUploadsOverlayAndUpdatesRegistry() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        mgr.markDeleted(3, new LogSequenceNumber(1L, 200L));
        mgr.markDeleted(17, new LogSequenceNumber(1L, 250L));
        mgr.markDeleted(3, new LogSequenceNumber(1L, 220L)); // duplicate, idempotent
        mgr.markDeleted(99, new LogSequenceNumber(2L, 0L));

        Optional<VersionedSegmentMetadata> result = mgr.flush(current);
        assertTrue("flush must produce a new version", result.isPresent());

        VersionedSegmentMetadata updated = result.get();
        assertNotNull(updated.metadata().getTombstonePath());
        assertEquals(2L, updated.metadata().getTombstoneLsnLedgerId());
        assertEquals(0L, updated.metadata().getTombstoneLsnOffset());
        assertEquals(1L, mgr.getLastPublishedGeneration());
        assertEquals(current.zkVersion() + 1, updated.zkVersion());

        // Read the overlay back from S3-equivalent storage and verify ordinals.
        TombstoneOverlay overlay = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, 1L);
        assertEquals(SEG_UUID, overlay.getSegmentUuid());
        assertEquals(1L, overlay.getOverlayGeneration());
        assertEquals(2L, overlay.getTombstoneLsn().ledgerId);
        assertEquals(0L, overlay.getTombstoneLsn().offset);
        assertArrayEquals(new int[]{3, 17, 99}, overlay.getTombstonedOrdinals());
    }

    @Test
    public void multipleFlushesIncreaseGenerationAndPathChanges() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        mgr.markDeleted(1, new LogSequenceNumber(1L, 10L));
        VersionedSegmentMetadata afterFirst = mgr.flush(current).orElseThrow();
        String firstPath = afterFirst.metadata().getTombstonePath();
        assertEquals(1L, mgr.getLastPublishedGeneration());

        mgr.markDeleted(2, new LogSequenceNumber(1L, 20L));
        VersionedSegmentMetadata afterSecond = mgr.flush(afterFirst).orElseThrow();
        String secondPath = afterSecond.metadata().getTombstonePath();
        assertEquals(2L, mgr.getLastPublishedGeneration());

        // Each generation writes to a distinct multipart fileType, so paths differ.
        assertFalse("each flush must produce a new path", firstPath.equals(secondPath));

        // After review item A4: a successful gen-2 flush physically deletes gen-1's
        // overlay file to prevent leaks. Loading gen-1 must therefore fail; gen-2
        // contains the cumulative tombstone set.
        try {
            TombstoneOverlayManager.loadOverlay(dsm, TS_UUID, IDX_UUID, SEG_UUID, 1L);
            org.junit.Assert.fail("expected gen-1 overlay file to be deleted by review item A4");
        } catch (herddb.storage.DataStorageManagerException expectedDeleted) {
            // good — A4 removes the previous generation file after a successful flush
        }
        TombstoneOverlay gen2 = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, 2L);
        assertArrayEquals(new int[]{1, 2}, gen2.getTombstonedOrdinals());
    }

    @Test
    public void replaceFromOverlayRestoresStateForNewOwner() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager owner = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        owner.markDeleted(7, new LogSequenceNumber(5L, 5L));
        owner.markDeleted(42, new LogSequenceNumber(5L, 6L));
        VersionedSegmentMetadata afterFlush = owner.flush(current).orElseThrow();

        // Simulate the new owner: fresh manager, fetch overlay from S3, reload.
        TombstoneOverlayManager newOwner = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        TombstoneOverlay loaded = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, 1L);
        newOwner.replaceFromOverlay(loaded);

        assertArrayEquals(new int[]{7, 42}, newOwner.snapshotOrdinals());
        assertEquals(5L, newOwner.getWatermarkLsn().ledgerId);
        assertEquals(6L, newOwner.getWatermarkLsn().offset);
        assertEquals(1L, newOwner.getLastPublishedGeneration());

        // New owner can continue from the loaded state and flush gen2.
        newOwner.markDeleted(100, new LogSequenceNumber(6L, 0L));
        VersionedSegmentMetadata gen2 = newOwner.flush(afterFlush).orElseThrow();
        assertEquals(2L, newOwner.getLastPublishedGeneration());
        TombstoneOverlay reloaded = TombstoneOverlayManager.loadOverlay(
                dsm, TS_UUID, IDX_UUID, SEG_UUID, 2L);
        assertArrayEquals(new int[]{7, 42, 100}, reloaded.getTombstonedOrdinals());
    }

    @Test
    public void flushWithStaleVersionFailsAndCleansUpOverlay() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // Concurrently bump the znode (simulating another writer / state transition).
        registry.casUpdateSegment(current,
                current.metadata().toBuilder().pendingOwnerInstanceId(1).build());

        mgr.markDeleted(1, new LogSequenceNumber(1L, 1L));
        try {
            mgr.flush(current);
            fail("flush with stale version must throw VersionMismatch");
        } catch (SegmentRegistryException.VersionMismatch ok) {
            // Expected — and the orphan multipart file should have been cleaned up
            // (best-effort). Verify the registry znode still has no tombstonePath
            // attached because the failed CAS never committed.
            VersionedSegmentMetadata after = registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID)
                    .orElseThrow();
            assertEquals(null, after.metadata().getTombstonePath());
        }
        // Internal state should NOT consider generation 1 as published — the next
        // flush attempt (with the now-current znode) should retry as generation 1.
        assertEquals(0L, mgr.getLastPublishedGeneration());
    }

    @Test
    public void rejectsNegativeOrdinal() throws Exception {
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        try {
            mgr.markDeleted(-1, new LogSequenceNumber(1L, 1L));
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }
}
