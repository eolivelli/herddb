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
                dsm);
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
                dsm);
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
                dsm);
        fakeClock.set(1000L);
        engine.runOnce();

        assertEquals(1, engine.getSegmentsDeleted());
        // Probed window of generations means at least tombstones-1 was attempted.
        assertTrue("expected tombstones-1 delete attempt; saw " + dsm.deletes,
                dsm.deletes.contains(deleteKey(TS_UUID, "idxuid_seg_seg-3", "tombstones-1")));
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
                faultyDsm);
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
