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
 * Issue #423: a shadow replica must surface the freshness of the data it can
 * serve. The primary publishes
 * {@link IndexingServiceCheckpointState#getLastEntryTimestampMillis()} after
 * every successful checkpoint; the shadow picks it up on every reload and
 * exposes it via {@link IndexingServiceEngine#getShadowLoadedEntryTimestamp()}
 * (and, downstream, {@code GetShadowStatus.loaded_entry_timestamp_ms}).
 *
 * <p>This is the "freshness for shadows" half of the feature, complementing
 * the primary-side coverage in {@link IndexingServiceEngineLagTimestampsTest}.
 */
public class ShadowLoadedEntryTimestampTest {

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
     * In-memory metadata manager identical to the one used by
     * {@link ShadowReloadTest}: stores per-instance checkpoint state and
     * fires registered watchers on every publish.
     */
    private static final class ShadowAwareMemoryMetadata extends MemoryMetadataStorageManager {
        final java.util.Map<Integer, IndexingServiceCheckpointState> state = new java.util.HashMap<>();
        final java.util.Map<Integer, List<Consumer<IndexingServiceCheckpointState>>> watchers =
                new java.util.HashMap<>();

        @Override
        public synchronized void publishIndexingServiceCheckpointState(
                IndexingServiceCheckpointState s)
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
        public synchronized IndexingServiceCheckpointState getIndexingServiceCheckpointState(
                int instanceId) throws MetadataStorageManagerException {
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

    /**
     * End-to-end: run a primary engine that processes a hand-crafted
     * timestamp-stamped LogEntry, force a checkpoint (so the primary
     * publishes its checkpoint state with the LogEntry timestamp), and then
     * verify a shadow attached to the same DSM/metadata picks up the
     * timestamp on reload and surfaces it via
     * {@link IndexingServiceEngine#getShadowLoadedEntryTimestamp()}.
     */
    @Test
    public void shadowReportsLastEntryTimestampPublishedByPrimary() throws Exception {
        ShadowAwareMemoryMetadata metadata = new ShadowAwareMemoryMetadata();
        metadata.start();
        metadata.ensureDefaultTableSpace("local", "local", 0, 1);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        // --- Primary side
        Path primaryLogDir = folder.newFolder("primary-log").toPath();
        Path primaryDataDir = folder.newFolder("primary-data").toPath();
        Properties primaryProps = new Properties();
        primaryProps.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration primaryConfig = new IndexingServerConfiguration(primaryProps);
        IndexingServiceEngine primary = new IndexingServiceEngine(
                primaryLogDir, primaryDataDir, primaryConfig);
        primary.setMetadataStorageManager(metadata);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        AtomicReference<PersistentVectorStore> primaryStoreRef = new AtomicReference<>();
        final String stableUuid = "shadow-lag-test-uuid";
        primary.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                        dataDir, indexProperties) -> {
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

        Random rng = new Random(7);
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
        // Pretend the primary's most recently processed LogEntry carries
        // this hand-crafted timestamp. The checkpoint must propagate it
        // through the WatermarkSnapshot AND through
        // IndexingServiceCheckpointState (issue #423).
        long primaryEntryTs = 1_700_000_321_000L;
        primary.setLastProcessedEntryTimestampForTest(primaryEntryTs);
        primary.forceCheckpointAndSaveWatermark();

        IndexingServiceCheckpointState published =
                metadata.getIndexingServiceCheckpointState(0);
        assertNotNull("primary must publish checkpoint state", published);
        assertEquals("checkpoint state must carry the LogEntry timestamp",
                primaryEntryTs, published.getLastEntryTimestampMillis());

        // Hydrate the DSM with table/index defs so the shadow's
        // loadIndexes-driven discovery succeeds (same trick as ShadowReloadTest).
        dsm.writeTables(
                primary.getTableSpaceUUID(), LogSequenceNumber.START_OF_TIME,
                java.util.Arrays.asList(t),
                java.util.Arrays.asList(idx), false);

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
            assertTrue("shadow must become ready after initial reload", shadow.isShadowReady());
            assertNotNull(shadow.getShadowLoadedLsn());
            assertEquals("shadow must surface the primary's advertised LogEntry timestamp",
                    primaryEntryTs, shadow.getShadowLoadedEntryTimestamp());

            // Drive a second checkpoint with a NEW timestamp — the watcher
            // chain must fire on the shadow side and the freshness must
            // advance.
            for (int i = 0; i < 64; i++) {
                Record r = RecordSerializer.makeRecord(t,
                        "pk", "k" + (256 + i), "vec", randomVector(rng, dim));
                LogEntry ins = LogEntryFactory.insert(t, r.key, r.value, null);
                lastLsn = new LogSequenceNumber(1, 500 + i);
                primary.applySingleEntryForTest(lastLsn, ins);
            }
            primary.awaitPendingWorkForTest();
            primary.setLastProcessedLsnForTest(lastLsn);
            long primaryEntryTs2 = primaryEntryTs + 60_000L;
            primary.setLastProcessedEntryTimestampForTest(primaryEntryTs2);
            primary.forceCheckpointAndSaveWatermark();

            // Wait for the shadow reload count to advance OR for the
            // freshness to update — the watcher is fired synchronously on
            // publish, but the reload runs on an executor.
            long deadline = System.currentTimeMillis() + 10_000L;
            while (shadow.getShadowLoadedEntryTimestamp() < primaryEntryTs2
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("shadow freshness must track the latest primary checkpoint",
                    primaryEntryTs2, shadow.getShadowLoadedEntryTimestamp());
        } finally {
            shadow.close();
            primary.close();
        }
    }
}
