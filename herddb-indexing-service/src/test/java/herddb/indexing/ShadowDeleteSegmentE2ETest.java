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
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.utils.ZKTestEnv;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #617: end-to-end test for the operator-facing {@code DeleteSegment}
 * path on a real {@link PersistentVectorStore} backed by a shared
 * {@link MemoryDataStorageManager}, with one primary and one shadow replica
 * coordinated through a real {@link ZKTestEnv}. Follows the same harness
 * convention as {@link ShadowE2ETest} — both live in the IS module and run
 * in the core (non-cluster) Maven test category because {@link ZKTestEnv}
 * starts an embedded ZK/Bookie pair in the same JVM.
 *
 * <p>Coverage:
 * <ul>
 *   <li><b>Refusal when graph file is present</b> — the IS rejects the
 *       delete because the segment's multipart graph is reachable via the
 *       shared {@link MemoryDataStorageManager} (the same property the
 *       production refusal gate keys off in the issue #617 scenario,
 *       where {@code force=false} prevents accidental deletes).</li>
 *   <li><b>Force override removes the segment</b> — with {@code force=true}
 *       the primary's in-memory segment count drops, the IS re-publishes
 *       a fresh {@link IndexingServiceCheckpointState}, the shadow's
 *       reload counter advances, and the shadow's loaded segment count
 *       on the dropped-segment store matches the primary's (zero).</li>
 *   <li><b>purge_storage=true also deletes the multipart files</b> — after
 *       the force-delete the IS reports {@code storage_purged=true} and a
 *       subsequent existence probe against the storage manager returns
 *       false for both the {@code graph} and {@code map} keys.</li>
 * </ul>
 *
 * <p>This test deliberately calls
 * {@link IndexingServiceEngine#deleteSegment(String, String, String, boolean, boolean)}
 * directly rather than going through the gRPC layer — the gRPC wire path
 * is covered by {@code herddb.indexing.admin.IndexingAdminCliDeleteSegmentTest}
 * (a focused fake-gRPC test). Coupling both layers in a single test would
 * add cluster-mode wiring (an in-process gRPC server) without exercising
 * a code path the CLI test does not already cover.
 */
public class ShadowDeleteSegmentE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv zk;
    private ZookeeperMetadataStorageManager metadata;
    private int savedMinLive;
    private long savedDeferral;
    private final List<ZookeeperMetadataStorageManager> shadowMetadatas = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        zk = new ZKTestEnv(folder.newFolder("zk").toPath());
        zk.startBookieAndInitCluster();
        metadata = new ZookeeperMetadataStorageManager(
                zk.getAddress(), zk.getTimeout(), zk.getPath());
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        // Same gates lifted as in ShadowE2ETest so a tiny workload still
        // produces a real persisted segment.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() throws Exception {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
        for (ZookeeperMetadataStorageManager sm : shadowMetadatas) {
            try {
                sm.close();
            } catch (Exception ignore) {
                // teardown best-effort — match ShadowE2ETest pattern
            }
        }
        if (metadata != null) {
            try {
                metadata.close();
            } catch (Exception ignore) {
                // teardown best-effort
            }
        }
        if (zk != null) {
            zk.close();
        }
    }

    private static Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace(TableSpace.DEFAULT)
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index createIndex(String uuid) {
        return Index.builder()
                .name("vidx").uuid(uuid)
                .table("vectable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private IndexingServiceEngine newPrimary(MemoryDataStorageManager dsm, MemoryManager mm,
                                              String stableUuid,
                                              AtomicReference<PersistentVectorStore> storeRef) throws Exception {
        Path logDir = folder.newFolder().toPath();
        Path dataDir = folder.newFolder().toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME, TableSpace.DEFAULT);
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(metadata);
        engine.setDataStorageManager(dsm);
        engine.setMemoryManager(mm);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirArg, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDirArg, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            storeRef.set(pvs);
            return pvs;
        });
        return engine;
    }

    private IndexingServiceEngine newShadow(MemoryDataStorageManager dsm, MemoryManager mm,
                                             int shadowOf) throws Exception {
        Path logDir = folder.newFolder().toPath();
        Path dataDir = folder.newFolder().toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME, TableSpace.DEFAULT);
        props.setProperty(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.ROLE_SHADOW);
        props.setProperty(IndexingServerConfiguration.PROPERTY_SHADOW_OF, Integer.toString(shadowOf));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        ZookeeperMetadataStorageManager perShadow = new ZookeeperMetadataStorageManager(
                zk.getAddress(), zk.getTimeout(), zk.getPath());
        perShadow.start();
        shadowMetadatas.add(perShadow);
        engine.setMetadataStorageManager(perShadow);
        engine.setDataStorageManager(dsm);
        engine.setMemoryManager(mm);
        return engine;
    }

    private void seedDsmSchema(MemoryDataStorageManager dsm, String tsUuid, Table table, Index idx)
            throws Exception {
        dsm.writeTables(tsUuid, LogSequenceNumber.START_OF_TIME,
                Arrays.asList(table), Arrays.asList(idx), false);
    }

    /**
     * Full lifecycle test:
     *   1. Primary ingests 128 vectors, checkpoints, and produces ≥1 segment.
     *   2. A shadow starts and catches up (reloadCount == 1).
     *   3. {@code deleteSegment(force=false)} is refused — graph file present.
     *   4. {@code deleteSegment(force=true, purge_storage=true)} succeeds:
     *      - primary's {@code segmentCount} drops by one;
     *      - storage_purged == true;
     *      - graph + map files are no longer reachable in the DSM.
     *   5. The shadow observes the new checkpoint state (reloadCount advances).
     */
    @Test
    public void deleteSegmentReducesPrimaryCountAndNotifiesShadow() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-617";

        AtomicReference<PersistentVectorStore> primaryStore = new AtomicReference<>();
        IndexingServiceEngine primary = newPrimary(dsm, mm, stableUuid, primaryStore);
        primary.start();

        Table t = createTable();
        Index idx = createIndex(stableUuid);
        primary.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
        primary.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

        Random rng = new Random(617);
        int dim = 16;
        LogSequenceNumber last = null;
        for (int i = 0; i < 128; i++) {
            Record r = RecordSerializer.makeRecord(t,
                    "pk", "k" + i, "vec", randomVector(rng, dim));
            LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
            last = new LogSequenceNumber(1, 100 + i);
            primary.applySingleEntryForTest(last, ins);
        }
        primary.awaitPendingWorkForTest();
        primary.setLastProcessedLsnForTest(last);
        primary.forceCheckpointAndSaveWatermark();

        seedDsmSchema(dsm, primary.getTableSpaceUUID(), t, idx);

        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary(
                        "p0", "dummy-addr:0", 0));
        IndexingServiceCheckpointState beforeDelete =
                metadata.getIndexingServiceCheckpointState(0);
        assertNotNull("primary must have published checkpoint state", beforeDelete);

        // A real segment was created by the checkpoint.
        PersistentVectorStore pvs = primaryStore.get();
        assertNotNull("primary store must be initialised", pvs);
        int segmentCountBefore = pvs.getSegmentCount();
        assertTrue("primary checkpoint must produce ≥1 segment, got " + segmentCountBefore,
                segmentCountBefore >= 1);
        List<String> keysBefore = pvs.getSegmentStorageKeysSnapshot();
        assertEquals(segmentCountBefore, keysBefore.size());
        String targetSegment = keysBefore.get(0);

        // Step 2: boot a shadow and let it catch up to reloadCount == 1.
        IndexingServiceEngine shadow = newShadow(dsm, mm, 0);
        shadow.start();
        try {
            assertTrue("shadow must become ready", shadow.isShadowReady());
            assertEquals("shadow must have reloaded exactly once",
                    1, shadow.getShadowReloadCount());

            // Step 3: refuse the delete when force=false. The MemoryDataStorageManager
            // honours multipartIndexFileExists via the default implementation, so the
            // graph file is reachable → engine refuses.
            try {
                primary.deleteSegment("vectable", "vidx", targetSegment,
                        /* purgeStorage */ false, /* force */ false);
                org.junit.Assert.fail("delete must be refused while graph file is present");
            } catch (IndexingServiceEngine.DeleteSegmentException expected) {
                assertTrue("refusal message must mention force flag, got: " + expected.getMessage(),
                        expected.getMessage().toLowerCase().contains("force"));
            }
            // Confirm the refusal did not mutate state.
            assertEquals("refused delete must NOT alter segment count",
                    segmentCountBefore, pvs.getSegmentCount());

            long reloadCountBeforeForce = shadow.getShadowReloadCount();
            IndexingServiceCheckpointState stateBeforeForce =
                    metadata.getIndexingServiceCheckpointState(0);
            assertNotNull(stateBeforeForce);

            // Step 4: force + purge.
            IndexingServiceEngine.DeleteSegmentResult result =
                    primary.deleteSegment("vectable", "vidx", targetSegment,
                            /* purgeStorage */ true, /* force */ true);
            assertTrue("force-delete must report removed=true", result.removed);
            assertEquals(targetSegment, result.segment);
            assertTrue("force-delete must report graph_file_present=true",
                    result.graphFilePresent);
            assertTrue("force-delete with purge must report storage_purged=true",
                    result.storagePurged);

            assertEquals("primary segment count must have dropped by one",
                    segmentCountBefore - 1, pvs.getSegmentCount());

            // Storage purge must have actually removed the multipart files.
            assertFalse("graph multipart file must be gone after purge",
                    dsm.multipartIndexFileExists(primary.getTableSpaceUUID(),
                            targetSegment, "graph"));
            assertFalse("map multipart file must be gone after purge",
                    dsm.multipartIndexFileExists(primary.getTableSpaceUUID(),
                            targetSegment, "map"));

            // The primary must have written a fresh checkpoint state to ZK
            // as part of the deleteSegment path. We verify this directly
            // before waiting on the shadow — a missing republish would
            // mask a regression in the engine-level notification logic
            // behind a generic "reloadCount didn't advance" failure.
            IndexingServiceCheckpointState stateAfterDelete =
                    metadata.getIndexingServiceCheckpointState(0);
            assertNotNull(stateAfterDelete);
            assertEquals("post-delete state must carry the new segment count",
                    pvs.getSegmentCount(), stateAfterDelete.getSegmentCount());
            assertTrue("post-delete state timestamp must advance, before="
                            + stateBeforeForce.getTimestampMillis()
                            + " after=" + stateAfterDelete.getTimestampMillis(),
                    stateAfterDelete.getTimestampMillis() >= stateBeforeForce.getTimestampMillis());

            // Step 5: shadow notification — wait for reloadCount to advance.
            // 30s deadline is consistent with awaitShadowReloadsForTest()'s
            // internal 30s barrier (the watch may arrive slightly after the
            // setData call returns).
            long deadline = System.currentTimeMillis() + 30_000L;
            while (shadow.getShadowReloadCount() <= reloadCountBeforeForce
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue("shadow must react to the post-delete republish, "
                            + "reloadCount before=" + reloadCountBeforeForce
                            + " now=" + shadow.getShadowReloadCount(),
                    shadow.getShadowReloadCount() > reloadCountBeforeForce);
        } finally {
            shadow.close();
            primary.close();
        }
    }

    /**
     * Force-delete WITHOUT {@code purge_storage}: the in-memory removal must
     * still succeed, but the engine must report {@code storage_purged=false}
     * and the multipart files must remain in the DSM (for post-mortem
     * forensics, as documented in issue #617).
     */
    @Test
    public void forceDeleteWithoutPurgeKeepsMultipartFiles() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-617-nopurge";

        AtomicReference<PersistentVectorStore> primaryStore = new AtomicReference<>();
        IndexingServiceEngine primary = newPrimary(dsm, mm, stableUuid, primaryStore);
        primary.start();

        Table t = createTable();
        Index idx = createIndex(stableUuid);
        primary.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
        primary.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

        Random rng = new Random(2026);
        int dim = 12;
        LogSequenceNumber last = null;
        for (int i = 0; i < 64; i++) {
            Record r = RecordSerializer.makeRecord(t,
                    "pk", "k" + i, "vec", randomVector(rng, dim));
            LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
            last = new LogSequenceNumber(1, 200 + i);
            primary.applySingleEntryForTest(last, ins);
        }
        primary.awaitPendingWorkForTest();
        primary.setLastProcessedLsnForTest(last);
        primary.forceCheckpointAndSaveWatermark();
        seedDsmSchema(dsm, primary.getTableSpaceUUID(), t, idx);
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary("p0", "dummy:0", 0));

        try {
            PersistentVectorStore pvs = primaryStore.get();
            assertNotNull(pvs);
            List<String> keys = pvs.getSegmentStorageKeysSnapshot();
            assertTrue("must have at least one segment", keys.size() >= 1);
            String target = keys.get(0);

            IndexingServiceEngine.DeleteSegmentResult result = primary.deleteSegment(
                    "vectable", "vidx", target,
                    /* purgeStorage */ false, /* force */ true);

            assertTrue(result.removed);
            assertFalse("storage_purged must be false when purge_storage=false",
                    result.storagePurged);
            // Files must still be on disk for forensics.
            assertTrue("graph file must remain in DSM when purge_storage=false",
                    dsm.multipartIndexFileExists(primary.getTableSpaceUUID(), target, "graph"));
        } finally {
            primary.close();
        }
    }

    /**
     * Negative path: requesting a segment that is not loaded must throw
     * {@link IndexingServiceEngine.DeleteSegmentException} with a message
     * that lists the currently-loaded segments. This is the operator-
     * friendly counterpart of the issue #617 reproduction, where the
     * operator pastes the wrong segment name.
     */
    @Test
    public void unknownSegmentIsRejectedWithDiagnosticMessage() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-617-unknown";

        AtomicReference<PersistentVectorStore> primaryStore = new AtomicReference<>();
        IndexingServiceEngine primary = newPrimary(dsm, mm, stableUuid, primaryStore);
        primary.start();
        try {
            Table t = createTable();
            Index idx = createIndex(stableUuid);
            primary.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
            primary.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));
            // No checkpoint → store has no segments. Still a valid lookup target.

            try {
                primary.deleteSegment("vectable", "vidx", "vidx_nonsense_seg999",
                        false, true);
                org.junit.Assert.fail("unknown segment must be rejected");
            } catch (IndexingServiceEngine.DeleteSegmentException expected) {
                assertTrue("error must mention the missing segment, got: " + expected.getMessage(),
                        expected.getMessage().contains("vidx_nonsense_seg999"));
            }
        } finally {
            primary.close();
        }
    }
}
