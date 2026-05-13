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
 * Validates fix A2: per-segment UUIDs are stamped at checkpoint time, persisted in
 * the v4 IndexStatus payload, and re-used across restarts so the segment registry
 * never sees a duplicate registration of the same physical segment.
 *
 * <p>Scenario:
 * <ol>
 *   <li>Boot a {@link PersistentVectorStore} with a {@link SegmentRegistryPublisher}.</li>
 *   <li>Insert vectors, checkpoint → ZK now has N entries with N UUIDs.</li>
 *   <li>Close the store, then re-open it pointing at the same data dir + DSM.</li>
 *   <li>Insert more vectors, checkpoint again. The previously-existing on-disk
 *       segments must NOT be re-registered (their UUIDs are preserved); only the
 *       freshly-emitted shard segment(s) get fresh UUIDs.</li>
 *   <li>The registry must still contain exactly the union of unique UUIDs.</li>
 * </ol>
 *
 * <p>Performance note: the UUID-stamping path adds one {@code UUID.randomUUID()} per
 * new segment per checkpoint, which is ~µs on modern JVMs and runs lock-free outside
 * the live-shard write path. This is far below the noise floor of Phase B's graph
 * build + multipart upload — no measurable hot-path impact.
 */
public class SegmentUuidRestartDurabilityTest {

    private static final String BASE_PATH = "/herd-test-A2";
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
    public void uuidStableAcrossRestart_noDuplicateRegistrationsOnSecondCheckpoint() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, /* instanceId= */ 0);

        int dim = 32;
        int firstBatch = 300;

        // === First boot: ingest + checkpoint → ZK gets N entries.
        Set<String> uuidsAfterFirstCheckpoint;
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
            for (int i = 0; i < firstBatch; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
        }
        List<VersionedSegmentMetadata> afterFirst = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertFalse("first checkpoint must register at least one segment", afterFirst.isEmpty());
        uuidsAfterFirstCheckpoint = new HashSet<>();
        for (VersionedSegmentMetadata v : afterFirst) {
            uuidsAfterFirstCheckpoint.add(v.metadata().getSegmentUuid());
        }

        // === Reboot: same data dir, same DSM, same publisher. The store reloads
        // IndexStatus (now v4) and recovers the per-segment UUIDs from the payload.
        SegmentRegistryPublisher publisher2 = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        int secondBatch = 300;
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher2);
            store.start();
            for (int i = firstBatch; i < firstBatch + secondBatch; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
        }

        List<VersionedSegmentMetadata> afterSecond = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        Set<String> uuidsAfterSecond = new HashSet<>();
        for (VersionedSegmentMetadata v : afterSecond) {
            uuidsAfterSecond.add(v.metadata().getSegmentUuid());
        }

        // The original UUIDs MUST still be in the registry — they were not duplicated
        // under different ids when the second checkpoint ran.
        assertTrue("post-restart registry must still contain the original segment UUIDs;"
                        + " original=" + uuidsAfterFirstCheckpoint + " current=" + uuidsAfterSecond,
                uuidsAfterSecond.containsAll(uuidsAfterFirstCheckpoint));

        // No two znodes can refer to the same physical (graphPath, mapPath) pair —
        // the bug we are guarding against produced exactly that kind of duplicate.
        Set<String> graphPaths = new HashSet<>();
        for (VersionedSegmentMetadata v : afterSecond) {
            assertNotNull(v.metadata().getGraphPath());
            assertTrue("duplicate graphPath registration: " + v.metadata().getGraphPath()
                            + " — UUID re-issue bug returned",
                    graphPaths.add(v.metadata().getGraphPath()));
        }

        // The merge of unique UUIDs equals the size of the registry.
        assertEquals(afterSecond.size(), uuidsAfterSecond.size());
    }

    @Test
    public void emptyOpenWithoutCheckpointDoesNotProduceUuid() throws Exception {
        // Sanity check: starting and stopping without a checkpoint should not register
        // any segment in ZK, regardless of the publisher being attached.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
            // no addVector, no checkpoint
        }

        assertTrue(registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
    }

    @Test
    public void publisherRejectsNullUuid() {
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);
        herddb.index.vector.NewSegmentInfo invalid = new herddb.index.vector.NewSegmentInfo(
                42, /* segmentUuid */ null,
                "g/x", 100L, "m/x", 100L, 200L, 10L, 1L,
                new herddb.log.LogSequenceNumber(1L, 100L), /* jvectorFeatureIds */ null);
        try {
            // The two-phase entry points must reject a null UUID just as the
            // legacy single-phase one did. We test stage; commit goes through the
            // same requireUuid() helper.
            publisher.stageNewSegments(java.util.Collections.singletonList(invalid));
            org.junit.Assert.fail("expected IllegalStateException for null UUID");
        } catch (IllegalStateException ok) {
            assertTrue(ok.getMessage().contains("stamped UUID"));
        }
    }

    @Test
    public void publisherIdempotentOnSegmentAlreadyExists() throws Exception {
        // Simulates a crash-and-retry: stage + commit then a re-stage + re-commit.
        // The second pair must silently no-op rather than throwing.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);
        String stableUuid = "stable-uuid-1";
        herddb.index.vector.NewSegmentInfo info = new herddb.index.vector.NewSegmentInfo(
                7, stableUuid,
                "g/x", 100L, "m/x", 100L, 200L, 10L, 1L,
                new herddb.log.LogSequenceNumber(1L, 100L), /* jvectorFeatureIds */ null);
        publisher.stageNewSegments(java.util.Collections.singletonList(info));
        publisher.commitStagedSegments(java.util.Collections.singletonList(info));
        // Second pair (retry) must not throw.
        publisher.stageNewSegments(java.util.Collections.singletonList(info));
        publisher.commitStagedSegments(java.util.Collections.singletonList(info));
        assertEquals(1, registry.listSegments(TABLE_SPACE, INDEX_UUID).size());
    }
}
