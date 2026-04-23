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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the shadow-replica boot + reload wiring on
 * {@link IndexingServiceEngine}:
 * <ul>
 *     <li>A shadow engine discovers vector indexes from the shared storage
 *         (no commit log), creates {@link ReadOnlyVectorStore} instances, and
 *         performs an initial reload to reach
 *         {@code shadowReady == true}.</li>
 *     <li>Publishing a fresh checkpoint state from the primary's side fires
 *         the ZK watch and drives a reload on the shadow; loaded LSN and
 *         reload count advance accordingly.</li>
 * </ul>
 */
public class ShadowReloadTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    /**
     * In-memory metadata manager that supports the shadow-replica APIs:
     * stores the latest checkpoint state per instanceId, invokes registered
     * watchers on every publish, and pre-registers CREATE_TABLE / CREATE_INDEX
     * via loadTables / loadIndexes so the shadow's boot discovery succeeds.
     */
    private static final class ShadowAwareMemoryMetadata extends MemoryMetadataStorageManager {
        final java.util.Map<Integer, IndexingServiceCheckpointState> state = new java.util.HashMap<>();
        final java.util.Map<Integer, List<Consumer<IndexingServiceCheckpointState>>> watchers = new java.util.HashMap<>();

        @Override
        public synchronized void publishIndexingServiceCheckpointState(IndexingServiceCheckpointState s)
                throws MetadataStorageManagerException {
            state.put(s.getInstanceId(), s);
            List<Consumer<IndexingServiceCheckpointState>> cbs = watchers.get(s.getInstanceId());
            if (cbs != null) {
                for (Consumer<IndexingServiceCheckpointState> cb : cbs) {
                    try {
                        cb.accept(s);
                    } catch (Exception ignored) {
                        // test-only stub; swallow to match real impl
                    }
                }
            }
        }

        @Override
        public synchronized IndexingServiceCheckpointState getIndexingServiceCheckpointState(int instanceId)
                throws MetadataStorageManagerException {
            return state.get(instanceId);
        }

        @Override
        public synchronized void watchIndexingServiceCheckpointState(
                int instanceId, Consumer<IndexingServiceCheckpointState> callback)
                throws MetadataStorageManagerException {
            watchers.computeIfAbsent(instanceId, k -> new ArrayList<>()).add(callback);
        }
    }

    private static Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index createIndex(String uuid) {
        return Index.builder()
                .name("vidx")
                .uuid(uuid)
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

    @Test
    public void shadowBootsDiscoversIndexAndReloads() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        // --- Primary side: create table + index, ingest some vectors, checkpoint
        Path primaryLogDir = folder.newFolder("primary-log").toPath();
        Path primaryDataDir = folder.newFolder("primary-data").toPath();
        Properties primaryProps = new Properties();
        primaryProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration primaryConfig = new IndexingServerConfiguration(primaryProps);
        IndexingServiceEngine primary = new IndexingServiceEngine(primaryLogDir, primaryDataDir, primaryConfig);
        primary.setMetadataStorageManager(metadata);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Primary uses a deterministic index UUID so the shadow can find the
        // same on-disk segments in the shared DSM. We use the engine's
        // resolved tableSpaceUUID so that the primary's IndexStatus ends up at
        // the same DSM key the shadow later queries.
        AtomicReference<PersistentVectorStore> primaryStoreRef = new AtomicReference<>();
        final String stableUuid = "shared-uuid-step5";
        primary.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDir, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, primary.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDir, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            primaryStoreRef.set(pvs);
            return pvs;
        });
        primary.start();

        Table t = createTable();
        primary.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
        Index idx = createIndex(stableUuid);
        primary.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

        Random rng = new Random(13);
        int dim = 16;
        LogSequenceNumber lastLsn = null;
        for (int i = 0; i < 256; i++) {
            Record r = RecordSerializer.makeRecord(t,
                    "pk", "k" + i, "vec", randomVector(rng, dim));
            LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
            lastLsn = new LogSequenceNumber(1, 100 + i);
            primary.applySingleEntryForTest(lastLsn, ins);
        }
        primary.awaitPendingWorkForTest();
        primary.setLastProcessedLsnForTest(lastLsn);
        primary.forceCheckpointAndSaveWatermark();
        // Primary should have published a state entry.
        assertNotNull(metadata.getIndexingServiceCheckpointState(0));

        // The shadow discovers indexes via dsm.loadIndexes; populate the DSM
        // with the primary's table + index definitions. In production this is
        // done by the herddb-core TableSpaceManager's checkpoint path; in
        // this engine-level test we do it manually.
        dsm.writeTables(
                primary.getTableSpaceUUID(), LogSequenceNumber.START_OF_TIME,
                java.util.Arrays.asList(t),
                java.util.Arrays.asList(idx), false);

        // --- Shadow side: same dsm + metadata, role=shadow, shadowOf=0.
        Path shadowLogDir = folder.newFolder("shadow-log").toPath();
        Path shadowDataDir = folder.newFolder("shadow-data").toPath();
        Properties shadowProps = new Properties();
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.ROLE_SHADOW);
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_SHADOW_OF, "0");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
        // isShadow's validateRoleAndShadow is NOT called here (that runs in
        // IndexingServer.start(), not IndexingServiceEngine.start()); the
        // engine-level config still allows storage.type=memory for tests.
        IndexingServerConfiguration shadowConfig = new IndexingServerConfiguration(shadowProps);
        IndexingServiceEngine shadow = new IndexingServiceEngine(shadowLogDir, shadowDataDir, shadowConfig);
        shadow.setMetadataStorageManager(metadata);
        shadow.setDataStorageManager(dsm);
        shadow.setMemoryManager(mm);
        // Shadow's factory is unused — startAsShadow creates ReadOnlyVectorStore
        // directly using the index.uuid from loadIndexes.

        try {
            shadow.start();
            assertTrue("shadow must become ready after initial reload", shadow.isShadowReady());
            assertNotNull(shadow.getShadowLoadedLsn());
            assertEquals(1, shadow.getShadowReloadCount());

            // Shadow search returns sensible results.
            float[] q = randomVector(rng, dim);
            List<java.util.Map.Entry<herddb.utils.Bytes, Float>> results =
                    shadow.search("default", "vectable", "vidx", q, 5);
            assertEquals(5, results.size());

            // Primary produces a second checkpoint — shadow watcher fires,
            // reload count advances.
            for (int i = 0; i < 128; i++) {
                Record r = RecordSerializer.makeRecord(t,
                        "pk", "k" + (256 + i), "vec", randomVector(rng, dim));
                LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
                lastLsn = new LogSequenceNumber(1, 500 + i);
                primary.applySingleEntryForTest(lastLsn, ins);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(lastLsn);
            primary.forceCheckpointAndSaveWatermark();

            // Wait up to 10s for the shadow reload to have picked up the
            // new state — the watch fires on the test's synchronous
            // publishIndexingServiceCheckpointState path, and the shadow
            // executor runs asynchronously.
            long deadline = System.currentTimeMillis() + 10_000L;
            while (shadow.getShadowReloadCount() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue("shadow reload count must advance after new primary checkpoint",
                    shadow.getShadowReloadCount() >= 2);
        } finally {
            shadow.close();
            primary.close();
        }
    }
}
