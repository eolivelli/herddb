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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryPublisher;
import herddb.indexing.segment.VersionedSegmentMetadata;
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
 * Validates fix G3: opt-in to segmented-v2 is per-index, not cluster-wide.
 * One IS process can host two {@link PersistentVectorStore} instances at the
 * same time — one with a publisher attached (segmented-v2), one without
 * (legacy) — and they don't interfere. The legacy index's checkpoint must NOT
 * touch the registry; the v2 index's checkpoint must.
 */
public class MixedModeIndexesTest {

    private static final String BASE_PATH = "/herd-test-G3";
    private static final String TABLE_SPACE = "tstblspace";

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

    private PersistentVectorStore createStore(String indexName, String indexUuid, Path tmpDir,
                                               FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(indexName, "testtable", TABLE_SPACE,
                "vector_col", indexUuid, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void v2IndexRegistersInRegistryAndV1IndexDoesNot() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        // === Legacy index (no publisher attached): checkpoint must not touch ZK.
        String legacyUuid = "legacy_idx_uuid";
        try (PersistentVectorStore legacy = createStore("legacy", legacyUuid, tmpDir, dsm)) {
            legacy.start();
            for (int i = 0; i < 300; i++) {
                legacy.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            legacy.checkpoint();
        }

        // === v2 index (publisher attached): checkpoint must register a segment.
        String v2Uuid = "v2_idx_uuid";
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, "testtable", v2Uuid, "v2", /* instanceId= */ 0);
        try (PersistentVectorStore v2 = createStore("v2", v2Uuid, tmpDir, dsm)) {
            v2.setSegmentPublisher(publisher);
            v2.start();
            for (int i = 0; i < 300; i++) {
                v2.addVector(Bytes.from_int(1000 + i), randomVector(new Random(i), 32));
            }
            v2.checkpoint();
        }

        // The legacy index left no traces in the registry.
        List<VersionedSegmentMetadata> legacyEntries = registry.listSegments(TABLE_SPACE, legacyUuid);
        assertTrue("legacy (no-publisher) index must NOT register anything in ZK",
                legacyEntries.isEmpty());

        // The v2 index registered at least one segment.
        List<VersionedSegmentMetadata> v2Entries = registry.listSegments(TABLE_SPACE, v2Uuid);
        assertFalse("v2 index must register segments in ZK", v2Entries.isEmpty());
        for (VersionedSegmentMetadata v : v2Entries) {
            assertNotNull(v.metadata().getSegmentUuid());
            assertEquals(v2Uuid, v.metadata().getIndexUuid());
        }
    }

    @Test
    public void externalCompactionFlagIsPerStore() throws Exception {
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(tmpFolder.newFolder("data").toPath());
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore legacy = createStore("legacy", "legacy_uuid", tmpDir, dsm);
             PersistentVectorStore v2 = createStore("v2", "v2_uuid", tmpDir, dsm)) {
            // Per-index toggle: legacy keeps its in-IS compaction loop; v2 disables it.
            v2.setExternalCompactionEnabled(true);
            assertEquals(false, legacy.isExternalCompactionEnabled());
            assertEquals(true, v2.isExternalCompactionEnabled());
        }
    }
}
