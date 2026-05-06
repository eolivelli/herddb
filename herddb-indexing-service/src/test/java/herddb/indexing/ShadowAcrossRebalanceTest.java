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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.IndexingServiceRebalanceDescriptor;
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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Shadow-replica behaviour across a {@code INDEXING_SERVICE_REBALANCE}.
 *
 * <p>Shadows do not tail the commit log, so REBALANCE entries are irrelevant
 * to them — they continue to mirror the on-disk checkpoint state of their
 * paired primary, identified by {@code shadowOf}. This test verifies:
 * <ul>
 *   <li>An existing shadow keeps serving across a REBALANCE applied on its
 *       paired primary; checkpoint reloads are still triggered by primary
 *       state updates and not perturbed by the REBALANCE.</li>
 *   <li>A shadow whose paired primary has not yet published any checkpoint
 *       state still boots cleanly (no exception, {@code shadowReady=false}
 *       or empty), and becomes ready when the primary later publishes via
 *       the existing watcher chain.</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class ShadowAcrossRebalanceTest {

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
                        // test-only stub
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

    private static Table makeTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index makeIndex(String uuid) {
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

    /**
     * Existing shadow of primary 0 keeps reloading correctly when the primary
     * checkpoints a fresh state, even if a REBALANCE entry was applied on
     * the primary in between (the entry never reaches the shadow because the
     * shadow doesn't tail the log).
     */
    @Test
    public void existingShadowKeepsReloadingAcrossRebalance() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        Path primaryLogDir = folder.newFolder("primary-log").toPath();
        Path primaryDataDir = folder.newFolder("primary-data").toPath();
        Properties primaryProps = new Properties();
        primaryProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration primaryConfig = new IndexingServerConfiguration(primaryProps);
        IndexingServiceEngine primary = new IndexingServiceEngine(
                primaryLogDir, primaryDataDir, primaryConfig);
        primary.setMetadataStorageManager(metadata);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        final String stableUuid = "shared-uuid-shadow-rebalance";
        primary.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDir, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, primary.getTableSpaceUUID(), vectorColumnName,
                    stableUuid, dataDir, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });
        primary.start();

        Table t = makeTable();
        primary.applyEntry(new LogSequenceNumber(1, 1),
                LogEntryFactory.createTable(t, null));
        Index idx = makeIndex(stableUuid);
        primary.applyEntry(new LogSequenceNumber(1, 2),
                LogEntryFactory.createIndex(idx, null));

        Random rng = new Random(7);
        int dim = 16;
        LogSequenceNumber lastLsn = null;
        for (int i = 0; i < 64; i++) {
            Record r = RecordSerializer.makeRecord(t,
                    "pk", "k" + i, "vec", randomVector(rng, dim));
            lastLsn = new LogSequenceNumber(1, 100 + i);
            primary.applySingleEntryForTest(lastLsn,
                    LogEntryFactory.insert(t, r.key, r.value, null));
        }
        primary.awaitPendingWorkForTest();
        primary.setLastProcessedLsnForTest(lastLsn);
        primary.forceCheckpointAndSaveWatermark();
        assertNotNull(metadata.getIndexingServiceCheckpointState(0));

        dsm.writeTables(primary.getTableSpaceUUID(), LogSequenceNumber.START_OF_TIME,
                Collections.singletonList(t), Collections.singletonList(idx), false);

        // --- Shadow side
        Path shadowLogDir = folder.newFolder("shadow-log").toPath();
        Path shadowDataDir = folder.newFolder("shadow-data").toPath();
        Properties shadowProps = new Properties();
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.ROLE_SHADOW);
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_SHADOW_OF, "0");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
        IndexingServerConfiguration shadowConfig = new IndexingServerConfiguration(shadowProps);
        IndexingServiceEngine shadow = new IndexingServiceEngine(
                shadowLogDir, shadowDataDir, shadowConfig);
        shadow.setMetadataStorageManager(metadata);
        shadow.setDataStorageManager(dsm);
        shadow.setMemoryManager(mm);

        try {
            shadow.start();
            assertTrue("shadow must be ready after initial reload", shadow.isShadowReady());
            long initialReloadCount = shadow.getShadowReloadCount();

            // Now apply a REBALANCE entry on the PRIMARY engine. The shadow
            // does NOT tail the log, so this is a no-op for the shadow path.
            IndexingServiceRebalanceDescriptor descriptor = new IndexingServiceRebalanceDescriptor(
                    System.currentTimeMillis(), 4,
                    Collections.singletonList(t),
                    Collections.singletonList(idx));
            primary.applySingleEntryForTest(new LogSequenceNumber(1, 1000),
                    LogEntryFactory.indexingServiceRebalance(descriptor));
            primary.awaitPendingWorkForTest();
            // Primary recorded the entry but neither produced a new checkpoint
            // state nor disrupted shadow's existing pairing.
            assertEquals(initialReloadCount, shadow.getShadowReloadCount());

            // Drive a fresh primary checkpoint — shadow's watcher must fire
            // and reload count advances by exactly one.
            for (int i = 64; i < 96; i++) {
                Record r = RecordSerializer.makeRecord(t,
                        "pk", "k" + i, "vec", randomVector(rng, dim));
                lastLsn = new LogSequenceNumber(1, 200 + i);
                primary.applySingleEntryForTest(lastLsn,
                        LogEntryFactory.insert(t, r.key, r.value, null));
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(lastLsn);
            primary.forceCheckpointAndSaveWatermark();

            long deadline = System.currentTimeMillis() + 5000;
            while (shadow.getShadowReloadCount() <= initialReloadCount
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue("shadow must observe a new reload after REBALANCE was applied"
                    + " on primary and a fresh checkpoint was published",
                    shadow.getShadowReloadCount() > initialReloadCount);
        } finally {
            shadow.close();
            primary.close();
        }
    }

    /**
     * A shadow whose paired primary has not yet published any state must
     * still boot cleanly (no exception). Once the primary publishes, the
     * existing watcher chain fires a reload on the shadow.
     */
    @Test
    public void shadowBootsBeforePrimaryPublishesState() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        // No primary state has been published for instance 99
        assertNull(metadata.getIndexingServiceCheckpointState(99));

        Path shadowLogDir = folder.newFolder("shadow-log").toPath();
        Path shadowDataDir = folder.newFolder("shadow-data").toPath();
        Properties shadowProps = new Properties();
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.ROLE_SHADOW);
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_SHADOW_OF, "99");
        shadowProps.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "100");
        IndexingServerConfiguration shadowConfig = new IndexingServerConfiguration(shadowProps);
        IndexingServiceEngine shadow = new IndexingServiceEngine(
                shadowLogDir, shadowDataDir, shadowConfig);
        shadow.setMetadataStorageManager(metadata);
        shadow.setDataStorageManager(dsm);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        shadow.setMemoryManager(mm);

        try {
            shadow.start();
            // Boot succeeds even though primary 99 has nothing published.
            // No vector indexes have been registered with the DSM either, so
            // the shadow has nothing to reload yet — that is the desired
            // wait-for-primary behaviour.
            long beforeReloads = shadow.getShadowReloadCount();
            assertFalse("shadow must not claim a checkpoint LSN with no primary state",
                    shadow.getShadowLoadedLsn() != null
                            && shadow.getShadowLoadedLsn().after(LogSequenceNumber.START_OF_TIME));

            // Now publish a fresh primary state: the watcher chain must
            // fire and the shadow's reload count must advance.
            metadata.publishIndexingServiceCheckpointState(
                    new IndexingServiceCheckpointState(99, 7L, 42L, 0,
                            System.currentTimeMillis(),
                            System.currentTimeMillis() - 100));

            long deadline = System.currentTimeMillis() + 5000;
            while (shadow.getShadowReloadCount() <= beforeReloads
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertTrue("shadow must observe a reload once primary publishes its state",
                    shadow.getShadowReloadCount() > beforeReloads);
        } finally {
            shadow.close();
        }
    }
}
