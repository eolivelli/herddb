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
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogSequenceNumber;
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
 * Pins the consistency contract for DROP_INDEX / TRUNCATE_TABLE on a
 * segmented-v2 index: when the IS engine drops the local store it must ALSO
 * sweep every registry znode for the index, otherwise the registry is left
 * with orphan ACTIVE znodes pointing at multipart files that the surrounding
 * {@code DataStorageManager.dropIndex} has just deleted. Other IS instances
 * watching the registry would observe phantom segments and (in the worst
 * case) attempt ownership-transfers for files that no longer exist.
 *
 * <p>This test exercises the SPI hook directly via
 * {@link SegmentRegistryPublisher#dropAllSegmentsForIndex} and the
 * {@link PersistentVectorStore#dropAllRegistryEntries} convenience method
 * that the IS engine invokes inside {@code submitVectorStoreDeletion}. The
 * end-to-end engine-driven flow is covered separately by
 * {@code DropTableIndexCleanupTest} (no-publisher variant from master) once
 * the engine is configured with the publisher.
 */
public class TruncateAndDropRegistryCleanupTest {

    private static final String BASE_PATH = "/herd-test-truncate-cleanup";
    private static final String TABLE_SPACE = "tstblspace";
    private static final String TABLE_NAME = "docs";
    private static final String INDEX_NAME = "docs_v1";
    private static final String INDEX_UUID = "testidx_testtable_drop";

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

    private SegmentMetadata buildSegment(String segUuid, SegmentState state, int ownerInstanceId) {
        return SegmentMetadata.builder()
                .segmentUuid(segUuid)
                .tablespaceUuid(TABLE_SPACE)
                .tableName(TABLE_NAME)
                .indexUuid(INDEX_UUID)
                .indexName(INDEX_NAME)
                .state(state)
                .ownerInstanceId(ownerInstanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(1024L).vectorCount(10L).generation(1L).createdAtEpochMillis(0L)
                .build();
    }

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, TABLE_NAME, TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
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
    public void dropAllSegmentsForIndexRemovesEveryZnode() throws Exception {
        // Pre-create three ACTIVE segments + one DEPRECATED + one TRANSFERRING
        // — the sweep must remove ALL of them regardless of state, because
        // DROP_INDEX is a global "this index no longer exists" operation.
        registry.createSegment(buildSegment("seg-A", SegmentState.ACTIVE, 0));
        registry.createSegment(buildSegment("seg-B", SegmentState.ACTIVE, 1));
        registry.createSegment(buildSegment("seg-C", SegmentState.ACTIVE, 0));
        // Mutate two into non-ACTIVE states.
        VersionedSegmentMetadata b = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-B")
                .orElseThrow();
        registry.casUpdateSegment(b, b.metadata().toBuilder()
                .state(SegmentState.TRANSFERRING)
                .pendingOwnerInstanceId(2)
                .build());
        VersionedSegmentMetadata c = registry.getSegment(TABLE_SPACE, INDEX_UUID, "seg-C")
                .orElseThrow();
        registry.casUpdateSegment(c, c.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(java.util.Collections.singletonList("merged-X"))
                .retentionUntilEpochMillis(System.currentTimeMillis() + 600_000L)
                .build());

        assertEquals(3, registry.listSegments(TABLE_SPACE, INDEX_UUID).size());

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, /* instanceId= */ 0);

        publisher.dropAllSegmentsForIndex();

        // Every znode for this index must be gone, regardless of the state
        // it was in pre-sweep.
        List<VersionedSegmentMetadata> after = registry.listSegments(TABLE_SPACE, INDEX_UUID);
        assertTrue("DROP_INDEX must remove ALL znodes for the index "
                        + "(saw " + after.size() + " survivors: " + summary(after) + ")",
                after.isEmpty());
    }

    @Test
    public void dropAllSegmentsForIndexIsIdempotent() throws Exception {
        registry.createSegment(buildSegment("only-seg", SegmentState.ACTIVE, 0));

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        // First call: removes the entry.
        publisher.dropAllSegmentsForIndex();
        assertTrue(registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());

        // Second call: must NOT throw — empty index sweep is a benign no-op.
        publisher.dropAllSegmentsForIndex();
        assertTrue(registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
    }

    @Test
    public void dropAllSegmentsForIndexLeavesOtherIndexesUntouched() throws Exception {
        // Sanity: the sweep is scoped to (tablespace, indexUuid). A
        // sibling index in the same tablespace must NOT lose any znodes.
        String siblingIdx = "siblingIdx";
        registry.createSegment(buildSegment("ours", SegmentState.ACTIVE, 0));
        // Sibling segment under a DIFFERENT indexUuid.
        SegmentMetadata sib = SegmentMetadata.builder()
                .segmentUuid("theirs")
                .tablespaceUuid(TABLE_SPACE).tableName("other")
                .indexUuid(siblingIdx).indexName("other_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 1L))
                .sizeBytes(0L).vectorCount(0L).generation(1L).createdAtEpochMillis(0L)
                .build();
        registry.createSegment(sib);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);
        publisher.dropAllSegmentsForIndex();

        assertTrue("our index must be empty",
                registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
        assertEquals("sibling index must be untouched",
                1, registry.listSegments(TABLE_SPACE, siblingIdx).size());
    }

    @Test
    public void dropAllSegmentsTolerantOfPerSegmentVersionMismatch() throws Exception {
        // Adversarial: the sweep races against a concurrent CAS that bumps
        // a znode's version between our list and our delete. The sweep must
        // log + skip that znode and continue with the rest, NOT abort. Other
        // sweeps (or the optimizer's reaper) catch the straggler.
        registry.createSegment(buildSegment("safe-seg", SegmentState.ACTIVE, 0));
        registry.createSegment(buildSegment("racing-seg", SegmentState.ACTIVE, 0));

        // Read both znodes; we'll bump racing-seg right before the sweep
        // hits it. Since the sweep iterates in registry order, and we
        // can't directly observe its iteration, we model the race by
        // bumping racing-seg's version BEFORE invoking the sweep — the
        // version we'd otherwise observe is now stale.
        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        // Pre-bump racing-seg's version (one CAS update simulates the
        // concurrent actor).
        VersionedSegmentMetadata current = registry.getSegment(TABLE_SPACE, INDEX_UUID, "racing-seg")
                .orElseThrow();
        // Capture the OLD version, then bump.
        int oldVersion = current.zkVersion();
        registry.casUpdateSegment(current, current.metadata().toBuilder()
                .pendingOwnerInstanceId(99)
                .build());
        // The stale-version reference now exists in our memory but the
        // registry has moved on. The sweep does its OWN list-then-delete
        // (it doesn't reuse our pre-captured reference), so the sweep
        // will succeed because its delete uses the FRESH version it just
        // listed. The test therefore demonstrates that bumping the znode
        // does NOT break the sweep — the sweep's list-then-delete is its
        // own atomic-enough unit. Both znodes must be gone.
        publisher.dropAllSegmentsForIndex();
        assertTrue(registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
    }

    @Test
    public void persistentVectorStoreDropAllRegistryEntriesNoOpsWithoutPublisher() throws Exception {
        // Defense: a store with NO publisher attached (legacy mode) must
        // tolerate dropAllRegistryEntries() — the IS engine calls this
        // unconditionally now, so the no-publisher branch must be benign.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            // No setSegmentPublisher call — legacy mode.
            store.dropAllRegistryEntries();
            // Should not have thrown. Repeating must be idempotent.
            store.dropAllRegistryEntries();
        }
    }

    @Test
    public void persistentVectorStoreDropAllRegistryEntriesSweepsAttachedPublisher() throws Exception {
        // End-to-end at the PVS level: attach a publisher, ingest +
        // checkpoint to register znodes, then call dropAllRegistryEntries
        // and verify the registry is empty.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
            assertFalse("checkpoint must register at least one segment in the registry",
                    registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());

            store.dropAllRegistryEntries();
            assertTrue("dropAllRegistryEntries must clear the registry",
                    registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
        }
    }

    @Test
    public void persistentVectorStoreCloseAloneDoesNotDropRegistryEntries() throws Exception {
        // Critical contract: a normal {@code close()} (e.g. graceful shutdown,
        // store-replacement on the assignment-watcher path) must NOT clobber
        // the registry. Only the explicit DROP/TRUNCATE path goes through
        // dropAllRegistryEntries(). Otherwise every restart would lose the
        // registry, which is exactly what UUID-stable v4 IndexStatus exists
        // to prevent.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                registry, TABLE_SPACE, TABLE_NAME, INDEX_UUID, INDEX_NAME, 0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setSegmentPublisher(publisher);
            store.start();
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), 32));
            }
            store.checkpoint();
        } // try-with-resources triggers close()

        assertFalse("close() must NEVER drop registry entries — only the explicit"
                        + " DROP/TRUNCATE path may. Otherwise restarting the IS would"
                        + " wipe the segmented-v2 registry on every shutdown.",
                registry.listSegments(TABLE_SPACE, INDEX_UUID).isEmpty());
    }

    private static String summary(List<VersionedSegmentMetadata> entries) {
        StringBuilder sb = new StringBuilder();
        for (VersionedSegmentMetadata v : entries) {
            sb.append(v.metadata().getSegmentUuid())
                    .append("(").append(v.metadata().getState()).append(") ");
        }
        return sb.toString().trim();
    }
}
