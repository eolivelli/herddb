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
import herddb.file.FileDataStorageManager;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Path;
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
 * Validates fix A4: each successful tombstone-overlay flush deletes the previous
 * generation's multipart file so we don't leak one orphan overlay directory per
 * flush.
 */
public class TombstoneOverlayLeakTest {

    private static final String BASE_PATH = "/herd-test-A4";
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
    private Path baseDir;

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

        baseDir = tmpFolder.newFolder("data").toPath();
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
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L)
                .vectorCount(10L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        registry.createSegment(segment);
        return registry.getSegment(TS_UUID, IDX_UUID, SEG_UUID).orElseThrow();
    }

    /**
     * Helper: probe whether a previously-uploaded multipart overlay file still exists.
     * Trying to read it via {@code multipartIndexReaderSupplier} either returns
     * happily (file present) or throws {@link DataStorageManagerException} when the
     * underlying directory is missing.
     */
    private boolean overlayFileExists(long generation) {
        try {
            dsm.multipartIndexReaderSupplier(TS_UUID,
                    TombstoneOverlayManager.multipartUuidFor(IDX_UUID, SEG_UUID),
                    TombstoneOverlayManager.fileTypeFor(generation),
                    Long.MAX_VALUE).get().close();
            return true;
        } catch (DataStorageManagerException notFound) {
            return false;
        } catch (Exception other) {
            fail("unexpected exception probing overlay file: " + other);
            return false;
        }
    }

    @Test
    public void successiveFlushesDeleteThePreviousOverlayFile() throws Exception {
        VersionedSegmentMetadata current = createActiveSegment();
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                dsm, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);

        // Flush #1 — generation 1 lands.
        mgr.markDeleted(1, new LogSequenceNumber(1L, 1L));
        VersionedSegmentMetadata after1 = mgr.flush(current).orElseThrow();
        assertTrue("gen 1 file must exist after first flush", overlayFileExists(1L));

        // Flush #2 — generation 2 lands; gen 1 must be deleted.
        mgr.markDeleted(2, new LogSequenceNumber(1L, 2L));
        VersionedSegmentMetadata after2 = mgr.flush(after1).orElseThrow();
        assertTrue("gen 2 file must exist after second flush", overlayFileExists(2L));
        assertEquals("gen 1 file must be deleted after gen 2 flush succeeds — review item A4",
                false, overlayFileExists(1L));

        // Flush #3 — gen 3 lands; gen 2 deleted; gen 1 still gone.
        mgr.markDeleted(3, new LogSequenceNumber(1L, 3L));
        mgr.flush(after2).orElseThrow();
        assertTrue("gen 3 file must exist after third flush", overlayFileExists(3L));
        assertEquals("gen 2 file must be deleted", false, overlayFileExists(2L));
        assertEquals("gen 1 file must still be gone", false, overlayFileExists(1L));
    }

    @Test
    public void cleanupFailureOnPreviousFileDoesNotBreakCurrentFlush() throws Exception {
        // Wrap the FileDataStorageManager so deleteMultipartIndexFile throws on the
        // PREVIOUS generation but lets the current one through. The flush must still
        // succeed and the current generation's file must still be uploaded + the znode
        // updated.
        VersionedSegmentMetadata current = createActiveSegment();

        // Custom DSM throws exclusively on tombstones-1 deletion.
        FileDataStorageManager wrapped = new FileDataStorageManager(baseDir) {
            @Override
            public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                    throws DataStorageManagerException {
                if (fileType.equals("tombstones-1")) {
                    throw new DataStorageManagerException("simulated cleanup failure");
                }
                super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
            }
        };
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                wrapped, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        mgr.markDeleted(1, new LogSequenceNumber(1L, 1L));
        VersionedSegmentMetadata after1 = mgr.flush(current).orElseThrow();

        mgr.markDeleted(2, new LogSequenceNumber(1L, 2L));
        // Must NOT throw despite the swallowed cleanup failure.
        VersionedSegmentMetadata after2 = mgr.flush(after1).orElseThrow();

        // Gen 2 published successfully even though gen 1 cleanup failed.
        assertEquals(SegmentState.ACTIVE, after2.metadata().getState());
        assertEquals(2L, mgr.getLastPublishedGeneration());
    }

    @Test
    public void noPreviousGenerationOnFirstFlush() throws Exception {
        // First-ever flush has no previous generation to delete; must not call delete.
        VersionedSegmentMetadata current = createActiveSegment();
        // Custom DSM throws on ANY delete — first flush must not invoke it.
        FileDataStorageManager wrapped = new FileDataStorageManager(baseDir) {
            @Override
            public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                    throws DataStorageManagerException {
                throw new DataStorageManagerException(
                        "FIRST FLUSH MUST NOT DELETE: " + fileType);
            }
        };
        TombstoneOverlayManager mgr = new TombstoneOverlayManager(
                wrapped, registry, TS_UUID, IDX_UUID, SEG_UUID, tmpDirectory);
        mgr.markDeleted(1, new LogSequenceNumber(1L, 1L));
        // Must not throw — first flush has no previous-generation to delete.
        mgr.flush(current).orElseThrow();
    }
}
