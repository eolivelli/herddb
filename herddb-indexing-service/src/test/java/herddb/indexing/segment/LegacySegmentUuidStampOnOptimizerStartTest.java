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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
 * Regression test for issue #499 Bug 2: legacy v3-format segments (produced by
 * an IS that ran without the external optimizer) must be stamped with fresh UUIDs
 * and registered in the external ZK segment registry on the first optimizer-enabled
 * IS startup.
 *
 * <p>Scenario:
 * <ol>
 *   <li>Boot a {@link PersistentVectorStore} WITHOUT a {@link SegmentRegistryPublisher}
 *       (legacy IS mode, no optimizer). Insert vectors and checkpoint — produces a v3
 *       IndexStatus with no per-segment UUIDs.</li>
 *   <li>Reopen the same store WITH a publisher (first optimizer-enabled start).
 *       Assert that all previously-null-UUID segments are now stamped, registered in ZK
 *       as ACTIVE, and that the updated IndexStatus is persisted to disk immediately
 *       (so the stamps survive the next restart without a checkpoint).</li>
 *   <li>Reopen a second time with a fresh publisher instance. Assert idempotency: the
 *       same UUIDs are reused, no duplicate registrations are created, and the registry
 *       still contains exactly the same entries as after step 2.</li>
 * </ol>
 */
public class LegacySegmentUuidStampOnOptimizerStartTest {

    private static final String BASE_PATH = "/herd-test-499";
    private static final String TABLE_SPACE = "tstblspace";
    private static final String TABLE_NAME = "docs";
    private static final String INDEX_NAME = "docs_vidx";
    private static final String INDEX_UUID = "testidx-499";

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

    /** Creates a store backed by a real {@link FileDataStorageManager} for IndexStatus persistence. */
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

    /**
     * Main scenario: v3 index (no UUID) → first optimizer-enabled start stamps and
     * registers all segments → second start is idempotent.
     */
    @Test
    public void legacySegmentsStampedAndRegisteredOnFirstOptimizerStart() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        int dim = 32;
        int vectorCount = 400;

        // === Step 1: boot WITHOUT publisher — produces v3 IndexStatus (no UUIDs).
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // No setSegmentPublisher → no UUID stamping during checkpoint.
            store.start();
            for (int i = 0; i < vectorCount; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
        }

        // Confirm ZK is empty — no publisher was active during the first boot.
        List<VersionedSegmentMetadata> afterLegacyBoot = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertTrue("ZK must be empty after legacy (no-publisher) boot; got: " + afterLegacyBoot,
                afterLegacyBoot.isEmpty());

        // === Step 2: reopen WITH publisher (first optimizer-enabled start).
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, /* instanceId= */ 0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
            // start() must have stamped UUIDs on the legacy segments AND persisted the
            // updated IndexStatus AND registered them in ZK via reconcileWithIndexStatus().
        }

        List<VersionedSegmentMetadata> afterFirstOptimizerStart =
                registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertFalse("ZK must contain segments after the first optimizer-enabled start",
                afterFirstOptimizerStart.isEmpty());

        // All registered segments must be ACTIVE.
        for (VersionedSegmentMetadata v : afterFirstOptimizerStart) {
            assertEquals("segment must be ACTIVE after reconcile; got: " + v.metadata(),
                    SegmentState.ACTIVE, v.metadata().getState());
            assertNotNull("segment UUID must not be null", v.metadata().getSegmentUuid());
        }

        Set<String> uuidsAfterFirst = new HashSet<>();
        for (VersionedSegmentMetadata v : afterFirstOptimizerStart) {
            assertTrue("duplicate UUID detected: " + v.metadata().getSegmentUuid(),
                    uuidsAfterFirst.add(v.metadata().getSegmentUuid()));
        }
        int segCountAfterFirst = afterFirstOptimizerStart.size();

        // === Step 3: second start — must be idempotent (same UUIDs, same count).
        SegmentRegistryPublisher publisher2 = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher2);
            store.start();
            // No new inserts or checkpoints — only reconcile runs.
        }

        List<VersionedSegmentMetadata> afterSecondStart =
                registry.listSegments(TABLE_SPACE, INDEX_UUID);

        // The count must be unchanged — reconcile must not create duplicate entries.
        assertEquals("segment count must be unchanged after the second optimizer-enabled start;"
                        + " first=" + segCountAfterFirst + " second=" + afterSecondStart.size(),
                segCountAfterFirst, afterSecondStart.size());

        // The same UUIDs must still be present — no new UUIDs were issued.
        Set<String> uuidsAfterSecond = new HashSet<>();
        for (VersionedSegmentMetadata v : afterSecondStart) {
            uuidsAfterSecond.add(v.metadata().getSegmentUuid());
        }
        assertEquals("UUID set must be identical after the second optimizer-enabled start",
                uuidsAfterFirst, uuidsAfterSecond);

        // All segments must still be ACTIVE.
        for (VersionedSegmentMetadata v : afterSecondStart) {
            assertEquals("segment must still be ACTIVE after second start; got: " + v.metadata(),
                    SegmentState.ACTIVE, v.metadata().getState());
        }
    }

    /**
     * A store that never had any segments (empty IndexStatus) must produce no ZK
     * entries on the first optimizer-enabled start.
     */
    @Test
    public void emptyIndexProducesNoZkEntriesOnOptimizerStart() throws Exception {
        Path baseDir = tmpFolder.newFolder("data-empty").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp-empty").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        // Boot without publisher (no checkpoint → no IndexStatus written at all).
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            // no addVector, no checkpoint
        }

        // First optimizer-enabled start on a completely empty store.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
        }

        assertTrue("empty index must produce no ZK entries",
                registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
    }
}
