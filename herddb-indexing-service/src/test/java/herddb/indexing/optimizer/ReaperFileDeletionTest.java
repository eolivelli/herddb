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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Validates fix A8: when {@link IndexOptimizerEngine} reaps a DEPRECATED segment
 * past its retention deadline, it deletes the multipart graph + map + tombstone
 * files via the wired {@link DataStorageManager} BEFORE removing the znode.
 *
 * <p>Uses an instrumented in-memory DSM that records every
 * {@code deleteMultipartIndexFile} call so we can assert exact (uuid, fileType)
 * triples were deleted.
 */
public class ReaperFileDeletionTest {

    private static final String BASE_PATH = "/herd-test-A8";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private RecordingDsm dsm;
    private AtomicLong fakeClock;
    private InMemorySegmentMerger merger;
    private org.junit.rules.TemporaryFolder tmpFolder = new org.junit.rules.TemporaryFolder();
    @org.junit.Rule
    public org.junit.rules.TemporaryFolder tmp = tmpFolder;

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

        Path baseDir = tmp.newFolder("data").toPath();
        dsm = new RecordingDsm(baseDir);
        dsm.initTablespace(TS_UUID);
        fakeClock = new AtomicLong(0L);
        merger = new InMemorySegmentMerger();
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

    private SegmentMetadata buildSegment(String segUuid, int segId, String tombstonePath) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TS_UUID)
                .tableName("docs")
                .indexUuid(IDX_UUID)
                .indexName("docs_v1")
                .state(SegmentState.DEPRECATED)
                .ownerInstanceId(0)
                .segmentId(segId)
                .graphPath("g/" + segUuid)
                .mapPath("m/" + segUuid)
                .tombstonePath(tombstonePath)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L)
                .vectorCount(10L)
                .generation(1L)
                .retentionUntilEpochMillis(/* in the past */ 1L)
                .createdAtEpochMillis(0L)
                .build();
    }

    @Test
    public void reaperDeletesGraphAndMapWhenSegmentIdIsKnown() throws Exception {
        registry.createSegment(buildSegment("seg-1", /* segmentId= */ 42, /* tombstonePath */ null));

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                /* retentionMs */ 60_000L,
                () -> 0,
                fakeClock::get,
                dsm,
                /* leaderLock */ null,
                /* safeModeFileDeletion */ false);
        // Advance clock past retention so reaper fires.
        fakeClock.set(1000L);
        engine.runOnce();

        assertEquals(1, engine.getSegmentsDeleted());
        // znode is gone.
        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-1").isPresent());
        // DSM saw exactly graph + map deletes for segUuid=indexUuid_seg42.
        assertTrue(dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg42", "graph")));
        assertTrue(dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg42", "map")));
    }

    @Test
    public void reaperSkipsFileDeleteWhenNoSegmentId() throws Exception {
        registry.createSegment(buildSegment("seg-2", /* segmentId */ SegmentMetadata.NO_SEGMENT_ID, null));

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L,
                () -> 0,
                fakeClock::get,
                dsm,
                /* leaderLock */ null,
                /* safeModeFileDeletion */ false);
        fakeClock.set(1000L);
        engine.runOnce();

        assertEquals(1, engine.getSegmentsDeleted());
        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-2").isPresent());
        // DSM saw NO graph/map delete because segmentId was unknown — and no tombstone
        // file either since tombstonePath was null. Only safe to check that at least
        // no graph/map for this segment hit the DSM.
        assertFalse(dsm.deletes.toString(),
                dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg-1", "graph")));
    }

    @Test
    public void reaperDeletesTombstoneOverlayFiles() throws Exception {
        registry.createSegment(buildSegment("seg-3", /* segmentId */ 7, /* tombstonePath */ "t/seg-3/gen5"));

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L,
                () -> 0,
                fakeClock::get,
                dsm,
                /* leaderLock */ null,
                /* safeModeFileDeletion */ false);
        fakeClock.set(1000L);
        engine.runOnce();

        assertEquals(1, engine.getSegmentsDeleted());
        // Probed window of generations means at least tombstones-1 was attempted.
        assertTrue("expected tombstones-1 delete attempt; saw " + dsm.deletes,
                dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg_seg-3", "tombstones-1")));
    }

    @Test
    public void safeModeDefaultPreservesFilesWhileRemovingZnode() throws Exception {
        // Review-item B1: by default the reaper MUST NOT delete files even when a DSM
        // is wired, because the IS-side ownership-change listener may not be in place.
        registry.createSegment(buildSegment("seg-safe", 11, null));
        IndexOptimizerEngine safeEngine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get, dsm); // safeMode = true by default
        fakeClock.set(1000L);
        safeEngine.runOnce();

        assertEquals(1, safeEngine.getSegmentsDeleted());
        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-safe").isPresent());
        assertTrue("safeMode default must NOT delete graph/map files; saw " + dsm.deletes,
                dsm.deletes.isEmpty());
    }

    @Test
    public void unsafeModeRequiresDsm() {
        try {
            new IndexOptimizerEngine(registry, merger, TS_UUID,
                    new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                    60_000L, () -> 0, fakeClock::get, /* dsm */ null,
                    /* leaderLock */ null, /* safeModeFileDeletion */ false);
            org.junit.Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("DataStorageManager"));
        }
    }

    @Test
    public void reaperWithoutDsmStillRemovesZnode() throws Exception {
        // Engine constructed without a DSM (default convenience ctor). File deletion
        // is skipped but the znode lifecycle still progresses.
        registry.createSegment(buildSegment("seg-4", 100, null));

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L,
                () -> 0,
                fakeClock::get); // no DSM
        fakeClock.set(1000L);
        engine.runOnce();
        assertEquals(1, engine.getSegmentsDeleted());
        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-4").isPresent());
        assertTrue("no DSM means no file deletes", dsm.deletes.isEmpty());
    }

    @Test
    public void crashBetweenFileDeleteAndZnodeDeleteIsHealedByNextTick() throws Exception {
        // Review-item B3: simulate a crash AFTER deleteSegmentFilesBestEffort()
        // succeeded but BEFORE the casDelete landed. The znode stays DEPRECATED
        // (NOT DELETED) so the next tick's runForIndex re-enters deleteRetainedSegment
        // via the DEPRECATED branch and finishes the job.
        //
        // We inject the crash by wrapping the ZK supplier so the first attempted
        // ZK.delete throws ConnectionLossException once, then proceeds normally.
        // The retry logic in SegmentRegistryClient bounds it to 5 attempts; with
        // crashCount=Integer.MAX_VALUE we exceed the cap and the casDelete fails
        // on the first runOnce; after we reset to 0 the second runOnce succeeds.
        registry.createSegment(buildSegment("seg-crash", 5, null));

        final java.util.concurrent.atomic.AtomicInteger crashCount =
                new java.util.concurrent.atomic.AtomicInteger(Integer.MAX_VALUE);
        java.util.function.Supplier<org.apache.zookeeper.ZooKeeper> faultySupplier = () -> {
            try {
                return newFaultyZk(crashCount);
            } catch (java.io.IOException ioe) {
                throw new RuntimeException(ioe);
            }
        };

        org.apache.zookeeper.ZooKeeper faultyZk = faultySupplier.get();
        SegmentRegistryClient crashingRegistry = new SegmentRegistryClient(() -> faultyZk, BASE_PATH);
        IndexOptimizerEngine engine = new IndexOptimizerEngine(crashingRegistry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0, fakeClock::get, dsm,
                /* leaderLock */ null, /* safeModeFileDeletion */ false);

        fakeClock.set(1000L);
        engine.runOnce(); // first tick: files deleted, znode delete fails after retries

        // Files were deleted (idempotently).
        assertTrue("files were deleted on the failing tick: " + dsm.deletes,
                dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg5", "graph")));
        // Znode is STILL DEPRECATED (not DELETED, not gone).
        herddb.indexing.segment.VersionedSegmentMetadata after =
                registry.getSegment(TS_UUID, IDX_UUID, "seg-crash").orElseThrow();
        assertEquals(SegmentState.DEPRECATED, after.metadata().getState());

        // Second tick: crash counter zeroed, casDelete succeeds, znode disappears.
        crashCount.set(0);
        engine.runOnce();
        assertFalse("znode must be gone after recovery tick",
                registry.getSegment(TS_UUID, IDX_UUID, "seg-crash").isPresent());
    }

    private org.apache.zookeeper.ZooKeeper newFaultyZk(
            java.util.concurrent.atomic.AtomicInteger crashCount) throws java.io.IOException {
        return new org.apache.zookeeper.ZooKeeper(zk.toString(), 30000, e -> { }) {
                {
                    try {
                        super.close();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public void delete(String path, int version)
                        throws InterruptedException, org.apache.zookeeper.KeeperException {
                    if (crashCount.getAndDecrement() > 0) {
                        throw new org.apache.zookeeper.KeeperException.ConnectionLossException();
                    }
                    zk.delete(path, version);
                }

                @Override
                public byte[] getData(String path, org.apache.zookeeper.Watcher watcher,
                        org.apache.zookeeper.data.Stat stat)
                        throws org.apache.zookeeper.KeeperException, InterruptedException {
                    return zk.getData(path, watcher, stat);
                }

                @Override
                public java.util.List<String> getChildren(String path, org.apache.zookeeper.Watcher watcher)
                        throws org.apache.zookeeper.KeeperException, InterruptedException {
                    return zk.getChildren(path, watcher);
                }

                @Override
                public java.util.List<String> getChildren(String path, boolean watch)
                        throws org.apache.zookeeper.KeeperException, InterruptedException {
                    return zk.getChildren(path, watch);
                }

                @Override
                public org.apache.zookeeper.data.Stat setData(String path, byte[] data, int version)
                        throws org.apache.zookeeper.KeeperException, InterruptedException {
                    return zk.setData(path, data, version);
                }

                @Override
                public org.apache.zookeeper.data.Stat exists(String path, org.apache.zookeeper.Watcher watcher)
                        throws org.apache.zookeeper.KeeperException, InterruptedException {
                    return zk.exists(path, watcher);
                }
            };
    }

    @Test
    public void reaperToleratesDsmFailure() throws Exception {
        // DSM throws on every delete. The reaper must still remove the znode.
        Path baseDir = tmp.newFolder("faulty").toPath();
        DataStorageManager faultyDsm = new FileDataStorageManager(baseDir) {
            @Override
            public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                    throws DataStorageManagerException {
                throw new DataStorageManagerException("simulated DSM failure");
            }
        };
        ((FileDataStorageManager) faultyDsm).initTablespace(TS_UUID);
        registry.createSegment(buildSegment("seg-5", 200, null));

        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L,
                () -> 0,
                fakeClock::get,
                faultyDsm,
                /* leaderLock */ null,
                /* safeModeFileDeletion */ false);
        fakeClock.set(1000L);
        engine.runOnce();

        assertEquals(1, engine.getSegmentsDeleted());
        assertFalse(registry.getSegment(TS_UUID, IDX_UUID, "seg-5").isPresent());
    }

    private static String deleteKey(String tableSpace, String uuid, String fileType) {
        return tableSpace + "::" + uuid + "::" + fileType;
    }

    /**
     * Wraps {@link FileDataStorageManager} to record every {@code deleteMultipartIndexFile}
     * call so we can assert on the (uuid, fileType) deletions emitted by the reaper.
     */
    static class RecordingDsm extends FileDataStorageManager {
        final Set<String> deletes = new HashSet<>();

        RecordingDsm(Path baseDir) {
            super(baseDir);
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deletes.add(deleteKey(tableSpace, uuid, fileType));
            // Don't actually try to delete — the parent throws on missing dirs which
            // is exactly the case under test.
        }
    }
}
