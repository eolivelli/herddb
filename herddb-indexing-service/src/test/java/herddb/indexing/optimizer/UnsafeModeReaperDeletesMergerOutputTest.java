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
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManagerException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates fix R7 (second pr-reviewer pass) and review-pass-3 P3-6: drive the
 * optimizer through a full merge → deprecate → reap cycle using a
 * {@link SegmentMerger} that produces metadata with a REAL (non-{@link
 * SegmentMetadata#NO_SEGMENT_ID}) {@code segmentId} AND uploads actual
 * graph/map files via {@link FileDataStorageManager}. The reaper deletes the
 * files only when {@code safeModeFileDeletion = false}.
 *
 * <p><b>Important:</b> this test exercises the UNSAFE-mode reaper path. Default
 * production deployments run with {@code indexoptimizer.safeMode.fileDeletion=true}
 * (see VECTOR.md "Production prerequisites") and therefore do NOT delete files
 * — the znode lifecycle still progresses but graph/map/tombstone bytes remain
 * on remote storage. The class name advertises this distinction so a future
 * reader does not mistake a green run here for "file deletion works in
 * production".
 */
public class UnsafeModeReaperDeletesMergerOutputTest {

    private static final String BASE_PATH = "/herd-test-R7";
    private static final String TS_UUID = "tsuid";
    private static final String IDX_UUID = "idxuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private FileDataStorageManager dsm;
    private Path baseDir;
    private AtomicLong fakeClock;

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
        fakeClock = new AtomicLong(0L);
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

    private SegmentMetadata sampleSegment(String uuid, int segmentId) {
        return SegmentMetadata.builder()
                .segmentUuid(uuid).tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid(IDX_UUID).indexName("docs_v1").state(SegmentState.ACTIVE)
                .ownerInstanceId(0).segmentId(segmentId)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(100L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    /**
     * A tiny "production-shaped" merger: each call uploads a fake graph + map file
     * via the wired DSM and produces metadata with a real (non-sentinel)
     * {@code segmentId} so the reaper can reconstruct the multipart-uuid for
     * deletion.
     */
    private static final class FileBackedMerger implements SegmentMerger {
        private final FileDataStorageManager dsm;
        private final Path tmpDir;
        private final AtomicLong nextSegmentId = new AtomicLong(1000L);

        FileBackedMerger(FileDataStorageManager dsm, Path tmpDir) {
            this.dsm = dsm;
            this.tmpDir = tmpDir;
        }

        @Override
        public SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance)
                throws Exception {
            String mergedUuid = UUID.randomUUID().toString();
            int mergedId = (int) nextSegmentId.getAndIncrement();
            String multipartUuid = inputs.get(0).getIndexUuid() + "_seg" + mergedId;

            Path graph = Files.createTempFile(tmpDir, "graph-", ".bin");
            Files.write(graph, new byte[]{1, 2, 3, 4});
            String graphPath = dsm.writeMultipartIndexFile(
                    inputs.get(0).getTablespaceUuid(), multipartUuid, "graph", graph, null);
            Files.deleteIfExists(graph);

            Path map = Files.createTempFile(tmpDir, "map-", ".bin");
            Files.write(map, new byte[]{5, 6, 7, 8});
            String mapPath = dsm.writeMultipartIndexFile(
                    inputs.get(0).getTablespaceUuid(), multipartUuid, "map", map, null);
            Files.deleteIfExists(map);

            return SegmentMetadata.builder()
                    .segmentUuid(mergedUuid)
                    .tablespaceUuid(inputs.get(0).getTablespaceUuid())
                    .tableName(inputs.get(0).getTableName())
                    .indexUuid(inputs.get(0).getIndexUuid())
                    .indexName(inputs.get(0).getIndexName())
                    .state(SegmentState.ACTIVE)
                    .ownerInstanceId(newOwnerInstance)
                    .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                    .segmentId(mergedId)
                    .graphPath(graphPath)
                    .mapPath(mapPath)
                    .baseLsn(inputs.get(0).baseLsn())
                    .sizeBytes(8L).vectorCount(20L).generation(99L)
                    .createdAtEpochMillis(System.currentTimeMillis())
                    .build();
        }
    }

    @Test
    public void mergeOutputFilesAreDeletedAtRetention() throws Exception {
        Path tmpDir = tmpFolder.newFolder("merger-tmp").toPath();
        // Pre-seed three input segments AND write real graph/map files for each
        // (under the indexUUID + "_seg" + segmentId multipart path the reaper will
        // probe). This makes the "files deleted at reap" assertion below meaningful.
        for (int i = 0; i < 3; i++) {
            int segId = 100 + i;
            registry.createSegment(sampleSegment("input-" + i, segId));
            String multipartUuid = IDX_UUID + "_seg" + segId;
            Path graph = Files.createTempFile(tmpDir, "input-graph-", ".bin");
            Files.write(graph, new byte[]{(byte) i, 0, 0, 0});
            dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "graph", graph, null);
            Files.deleteIfExists(graph);
            Path map = Files.createTempFile(tmpDir, "input-map-", ".bin");
            Files.write(map, new byte[]{(byte) i, 1, 0, 0});
            dsm.writeMultipartIndexFile(TS_UUID, multipartUuid, "map", map, null);
            Files.deleteIfExists(map);
        }

        // Sanity: the input files exist before the merge.
        for (int i = 0; i < 3; i++) {
            String multipartUuid = IDX_UUID + "_seg" + (100 + i);
            try (var rdr = dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "graph", 4L).get()) {
                // file present
            }
        }

        FileBackedMerger merger = new FileBackedMerger(dsm, tmpDir);
        IndexOptimizerEngine engine = new IndexOptimizerEngine(registry, merger, TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 2, Long.MAX_VALUE, Long.MAX_VALUE),
                /* retentionMs */ 60_000L, () -> 0, fakeClock::get, dsm,
                /* leaderLock */ null, /* safeModeFileDeletion */ false);

        // === Tick 1: merge fires, output is published with real files.
        engine.runOnce();
        assertEquals("merger ran exactly once", 1, engine.getSegmentsMerged());
        List<VersionedSegmentMetadata> all = registry.listSegments(TS_UUID, IDX_UUID);
        VersionedSegmentMetadata mergedOutput = all.stream()
                .filter(v -> v.metadata().getState() == SegmentState.ACTIVE)
                .findFirst().orElseThrow();
        int mergedSegmentId = mergedOutput.metadata().getSegmentId();
        assertTrue("merger must produce a real segmentId, not the sentinel",
                mergedSegmentId != SegmentMetadata.NO_SEGMENT_ID);

        // The merger's multipart files exist on disk now. Probe via DSM.
        String multipartUuid = IDX_UUID + "_seg" + mergedSegmentId;
        try (var rdr = dsm.multipartIndexReaderSupplier(TS_UUID, multipartUuid, "graph", 4L).get()) {
            // success — file exists
        }

        // === Tick 2: advance clock past retention; deprecated input files must be
        //  reaped + their znodes removed. The merged output is still ACTIVE.
        // Manually deprecate inputs with retentionUntil in the past so the reap
        // fires immediately on the next tick (the engine already deprecated them
        // with a future retention, but for test speed we rewrite the metadata).
        for (VersionedSegmentMetadata v : registry.listSegments(TS_UUID, IDX_UUID)) {
            if (v.metadata().getState() == SegmentState.DEPRECATED) {
                registry.casUpdateSegment(v,
                        v.metadata().toBuilder().retentionUntilEpochMillis(1L).build());
            }
        }
        fakeClock.set(2000L);
        engine.runOnce();

        // All three inputs are reaped. Merged output is still in the registry.
        all = registry.listSegments(TS_UUID, IDX_UUID);
        assertEquals("only the merged output should remain", 1, all.size());
        assertEquals(SegmentState.ACTIVE, all.get(0).metadata().getState());
        assertEquals(3, engine.getSegmentsDeleted());

        // Each input's multipart files must be GONE from the DSM.
        for (int i = 0; i < 3; i++) {
            String inputMultipartUuid = IDX_UUID + "_seg" + (100 + i);
            try {
                dsm.multipartIndexReaderSupplier(TS_UUID, inputMultipartUuid, "graph", 4L).get();
                org.junit.Assert.fail("input " + i + " graph file must have been deleted");
            } catch (DataStorageManagerException expected) {
                // good — file removed
            }
        }
    }
}
