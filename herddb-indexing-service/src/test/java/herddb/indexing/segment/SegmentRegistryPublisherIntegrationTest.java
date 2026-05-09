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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
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
 * End-to-end test for step 2 of the index-optimizer plan: when a
 * {@link PersistentVectorStore} runs a checkpoint with a
 * {@link SegmentRegistryPublisher} attached, every freshly-emitted segment must
 * appear in the ZooKeeper segment registry as an ACTIVE segment owned by the
 * local instance, with file paths and sizes matching the multipart files
 * actually written.
 */
public class SegmentRegistryPublisherIntegrationTest {

    private static final String BASE_PATH = "/herd-test-step2";
    private static final String TABLE_SPACE = "tstblspace";
    private static final String TABLE_NAME = "docs";
    private static final String INDEX_NAME = "docs_v1";
    private static final String INDEX_UUID = "testidx_testtable_fixed";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

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

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, TABLE_NAME, TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void publisherRegistersAllSegmentsEmittedByCheckpoint() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, /* instanceId= */ 0);

        int dim = 32;
        // Need >= MIN_VECTORS_FOR_FUSED_PQ (256) for FusedPQ path to actually emit
        // a multipart segment with non-null graphFilePath/mapFilePath.
        int numVectors = 300;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
        }

        // Verify ZK now contains exactly one ACTIVE segment for the index.
        List<VersionedSegmentMetadata> registered = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertFalse("expected at least one segment registered after checkpoint",
                registered.isEmpty());

        for (VersionedSegmentMetadata v : registered) {
            SegmentMetadata m = v.metadata();
            assertEquals(SegmentState.ACTIVE, m.getState());
            assertEquals(0, m.getOwnerInstanceId());
            assertEquals(SegmentMetadata.NO_INSTANCE, m.getPendingOwnerInstanceId());
            assertEquals(TABLE_SPACE, m.getTablespaceUuid());
            assertEquals(INDEX_UUID, m.getIndexUuid());
            assertEquals(TABLE_NAME, m.getTableName());
            assertEquals(INDEX_NAME, m.getIndexName());
            assertNotNull("graphPath must be set", m.getGraphPath());
            assertNotNull("mapPath must be set", m.getMapPath());
            // store.checkpoint() (no-arg) flows START_OF_TIME (-1,-1) through to the publisher
            // because no commit-log LSN is plumbed in unit-test mode; we just assert the
            // publisher faithfully forwarded what the store gave it.
            assertEquals(-1L, m.getBaseLsnLedgerId());
            assertEquals(-1L, m.getBaseLsnOffset());
            assertTrue("sizeBytes must be > 0", m.getSizeBytes() > 0);
            assertTrue("generation must be >= 1", m.getGeneration() >= 1);
            // Issue #484 (round-2 review): mapFileSize must be plumbed from
            // NewSegmentInfo into the znode so the optimizer-side merger
            // can drive the multipart reader correctly. Without this the
            // merger falls into a legacy size-hint fallback that's broken
            // on the production direct-multipart-download path.
            assertTrue("mapFileSize must be > 0 (issue #484): " + m.getMapFileSize(),
                    m.getMapFileSize() > 0L);
            assertTrue("mapFileSize (" + m.getMapFileSize() + ") must be <= sizeBytes ("
                    + m.getSizeBytes() + ") since sizeBytes aggregates graph + map",
                    m.getMapFileSize() <= m.getSizeBytes());
        }

        long totalRegisteredVectors = 0L;
        for (VersionedSegmentMetadata v : registered) {
            totalRegisteredVectors += v.metadata().getVectorCount();
        }
        assertEquals("total vectors across registered segments must equal inserted count",
                numVectors, totalRegisteredVectors);
    }

    @Test
    public void noPublisherMeansNoZkRegistration() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        int dim = 32;
        int numVectors = 300;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Note: no setSegmentPublisher() call — legacy mode.
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
        }

        // ZK should be empty — legacy mode does not touch the registry.
        List<VersionedSegmentMetadata> registered = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertTrue("legacy mode must not register any segment in ZK", registered.isEmpty());
    }

    @Test
    public void publisherFailureDoesNotBreakCheckpoint() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        // A publisher that always blows up. The checkpoint must still succeed.
        herddb.index.vector.SegmentPublisher faulty = new herddb.index.vector.SegmentPublisher() {
            @Override
            public void stageNewSegments(java.util.List<herddb.index.vector.NewSegmentInfo> segments) {
                throw new RuntimeException("simulated publisher failure");
            }

            @Override
            public void commitStagedSegments(java.util.List<herddb.index.vector.NewSegmentInfo> segments) {
                throw new RuntimeException("simulated publisher failure");
            }
        };

        int dim = 32;
        int numVectors = 300;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(faulty);
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            // Must not throw — publisher failures are swallowed by design.
            store.checkpoint();
            // Local segments must still be visible.
            assertTrue("checkpoint must still produce local segments",
                    store.getSegmentCount() > 0);
        }

        // Registry stays empty since the publisher always failed.
        List<VersionedSegmentMetadata> registered = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertTrue("registry should be empty when publisher always fails", registered.isEmpty());
    }
}
