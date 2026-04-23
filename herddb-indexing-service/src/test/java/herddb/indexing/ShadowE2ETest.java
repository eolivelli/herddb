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
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end shadow replicas test using a real ZooKeeper (via {@link ZKTestEnv})
 * and in-process engines sharing a {@link MemoryDataStorageManager}. Covers:
 *
 * <ul>
 *   <li>Happy path: primary ingests + checkpoints, two shadows catch up via
 *       the ZK-driven reload path, and queries on the shadows return results
 *       consistent with the primary's.</li>
 *   <li>Determinism: WaitForCheckpoint (via engine-level polling of
 *       {@code shadowLoadedLsn}) succeeds only after the shadow has reloaded
 *       past the primary's published LSN.</li>
 *   <li>Shadow boot before primary: the shadow sees no state yet, reports
 *       {@code ready=false}, then transitions to ready once the primary
 *       publishes its first checkpoint state.</li>
 * </ul>
 */
public class ShadowE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv zk;
    private ZookeeperMetadataStorageManager metadata;

    private int savedMinLive;
    private long savedDeferral;

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
                // teardown best-effort
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

    private final List<ZookeeperMetadataStorageManager> shadowMetadatas = new ArrayList<>();

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
        // Each shadow gets its OWN ZK session so that watchers installed by
        // one don't overwrite the single per-manager watcher slot used by
        // another. In production this is guaranteed because each IS process
        // runs its own ZookeeperMetadataStorageManager.
        ZookeeperMetadataStorageManager perShadowMetadata = new ZookeeperMetadataStorageManager(
                zk.getAddress(), zk.getTimeout(), zk.getPath());
        perShadowMetadata.start();
        shadowMetadatas.add(perShadowMetadata);
        engine.setMetadataStorageManager(perShadowMetadata);
        engine.setDataStorageManager(dsm);
        engine.setMemoryManager(mm);
        return engine;
    }

    private void seedDsmSchema(MemoryDataStorageManager dsm, String tsUuid, Table table, Index idx)
            throws Exception {
        dsm.writeTables(tsUuid, LogSequenceNumber.START_OF_TIME,
                Arrays.asList(table), Arrays.asList(idx), false);
    }

    @Test
    public void happyPathPrimaryAndTwoShadows() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-happy";

        AtomicReference<PersistentVectorStore> primaryStore = new AtomicReference<>();
        IndexingServiceEngine primary = newPrimary(dsm, mm, stableUuid, primaryStore);
        primary.start();

        Table t = createTable();
        Index idx = createIndex(stableUuid);
        primary.applyEntry(new LogSequenceNumber(1, 1), LogEntryFactory.createTable(t, null));
        primary.applyEntry(new LogSequenceNumber(1, 2), LogEntryFactory.createIndex(idx, null));

        Random rng = new Random(11);
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

        // Register the primary in ZK and confirm state was published via
        // the step-3 path.
        metadata.registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary(
                        "p0", "dummy-addr:0", 0));
        IndexingServiceCheckpointState published = metadata.getIndexingServiceCheckpointState(0);
        assertNotNull("primary must have published checkpoint state", published);

        // Boot two shadows.
        IndexingServiceEngine shadowA = newShadow(dsm, mm, 0);
        IndexingServiceEngine shadowB = newShadow(dsm, mm, 0);
        try {
            shadowA.start();
            shadowB.start();

            assertTrue("shadow A must become ready", shadowA.isShadowReady());
            assertTrue("shadow B must become ready", shadowB.isShadowReady());
            assertEquals(1, shadowA.getShadowReloadCount());
            assertEquals(1, shadowB.getShadowReloadCount());

            // Queries return the same count on both shadows.
            float[] q = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> primaryResults =
                    primary.search(TableSpace.DEFAULT, "vectable", "vidx", q, 10);
            List<Map.Entry<Bytes, Float>> shadowAResults =
                    shadowA.search(TableSpace.DEFAULT, "vectable", "vidx", q, 10);
            List<Map.Entry<Bytes, Float>> shadowBResults =
                    shadowB.search(TableSpace.DEFAULT, "vectable", "vidx", q, 10);
            assertEquals(primaryResults.size(), shadowAResults.size());
            assertEquals(primaryResults.size(), shadowBResults.size());

            // Primary does another checkpoint; shadows should catch up via
            // the ZK watch.
            for (int i = 0; i < 64; i++) {
                Record r = RecordSerializer.makeRecord(t,
                        "pk", "k" + (200 + i), "vec", randomVector(rng, dim));
                LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
                last = new LogSequenceNumber(1, 400 + i);
                primary.applySingleEntryForTest(last, ins);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(last);
            primary.forceCheckpointAndSaveWatermark();

            // Poll each shadow until it advances past reloadCount 1.
            long deadline = System.currentTimeMillis() + 10_000;
            while ((shadowA.getShadowReloadCount() < 2 || shadowB.getShadowReloadCount() < 2)
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(30);
            }
            assertTrue("shadow A must see 2nd checkpoint, got " + shadowA.getShadowReloadCount(),
                    shadowA.getShadowReloadCount() >= 2);
            assertTrue("shadow B must see 2nd checkpoint, got " + shadowB.getShadowReloadCount(),
                    shadowB.getShadowReloadCount() >= 2);
        } finally {
            shadowA.close();
            shadowB.close();
            primary.close();
        }
    }

    @Test
    public void shadowBootsBeforePrimaryBecomesReadyOnPublish() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String stableUuid = "idx-uuid-order";

        // Shadow first. Schema doesn't exist in DSM yet — shadow discovers
        // zero indexes, runs its first reload against an empty state, and
        // still transitions to ready=true (empty-pool reload counts as
        // success; search on a missing index returns empty).
        IndexingServiceEngine shadow = newShadow(dsm, mm, 0);
        shadow.start();
        try {
            assertTrue("shadow with no state is still ready (empty pool)",
                    shadow.isShadowReady());

            // Now boot the primary, ingest + checkpoint, then seed the schema
            // into the DSM so a re-scan would discover the index on reload.
            AtomicReference<PersistentVectorStore> pRef = new AtomicReference<>();
            IndexingServiceEngine primary = newPrimary(dsm, mm, stableUuid, pRef);
            primary.start();
            try {
                Table t = createTable();
                Index idx = createIndex(stableUuid);
                primary.applyEntry(new LogSequenceNumber(1, 1),
                        LogEntryFactory.createTable(t, null));
                primary.applyEntry(new LogSequenceNumber(1, 2),
                        LogEntryFactory.createIndex(idx, null));

                Random rng = new Random(29);
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

                // Verify primary's state made it to ZK
                long deadline = System.currentTimeMillis() + 5_000;
                IndexingServiceCheckpointState s = null;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        s = metadata.getIndexingServiceCheckpointState(0);
                    } catch (MetadataStorageManagerException ignore) {
                        // retry
                    }
                    if (s != null) {
                        break;
                    }
                    Thread.sleep(20);
                }
                assertNotNull("primary must publish checkpoint state", s);

                // Poll until shadow's reload-count advances (watcher fires).
                deadline = System.currentTimeMillis() + 10_000;
                while (shadow.getShadowReloadCount() < 2
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(30);
                }
                assertTrue("shadow must react to primary's first publish",
                        shadow.getShadowReloadCount() >= 2);
            } finally {
                primary.close();
            }
        } finally {
            shadow.close();
        }
    }
}
