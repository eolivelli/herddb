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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.index.vector.AbstractVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #368: the Indexing Service must be able to recover
 * its schema (table and vector-index definitions) from the watermark snapshot
 * after a restart, even when the early BookKeeper ledgers that contained the
 * original {@code CREATE_TABLE} / {@code CREATE_INDEX} DDL entries have been
 * trimmed by the server's retention policy.
 *
 * <p>The test verifies three related properties:
 * <ol>
 *   <li>A checkpoint saves the current schema alongside the watermark LSN in
 *       the {@link WatermarkSnapshot}.</li>
 *   <li>An engine that starts with a schema-carrying snapshot hydrates its
 *       {@link SchemaTracker} and vector stores from the snapshot before the
 *       tailer starts.  The tailer resumes from the <em>watermark LSN</em>
 *       (not from {@code START_OF_TIME}), so only entries that arrived after
 *       the checkpoint need to be replayed.  This is critical when early
 *       BookKeeper ledgers have been trimmed: the engine must never attempt
 *       to open a ledger that is no longer present.</li>
 *   <li>When a vector store supports UUID-based checkpoint recovery
 *       ({@link AbstractVectorStore#getStoreUUID()} is non-null), the UUID is
 *       embedded in the watermark snapshot's index properties under the key
 *       {@link IndexingServiceEngine#PROP_IS_STORE_UUID}.  A restarting engine
 *       reads that UUID and passes it to the vector store factory so the new
 *       store instance re-attaches to the same on-disk checkpoint without full
 *       DML replay.</li>
 * </ol>
 *
 * <p>A companion negative test confirms that an engine started with an
 * empty-schema snapshot (no DDL replayed either) silently drops all DML
 * entries, demonstrating the bug that this fix addresses.
 *
 * @author enrico.olivelli
 */
public class WatermarkSchemaRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int DIM = 8;

    // ---------- helpers -----------------------------------------------------

    private static Table buildTable() {
        return Table.builder()
                .name("t1")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index buildVectorIndex() {
        return Index.builder()
                .name("vidx")
                .table("t1")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private static float[] randomVec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    /**
     * Watermark store whose {@link #load()} returns a pre-baked snapshot —
     * used to inject a recovered watermark into a freshly created engine.
     */
    private static final class PreloadedWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> current;

        PreloadedWatermarkStore(WatermarkSnapshot initial) {
            this.current = new AtomicReference<>(initial);
        }

        @Override
        public WatermarkSnapshot load() {
            return current.get();
        }

        @Override
        public void save(WatermarkSnapshot snapshot) throws IOException {
            current.set(snapshot);
        }
    }

    // ---------- tests --------------------------------------------------------

    /**
     * Verifies the full schema-recovery path and implicitly validates the
     * BK-ledger-trimming scenario:
     * <ol>
     *   <li>Engine A applies DDL + 50 DML entries and checkpoints — the
     *       snapshot saved by the watermark store must carry the schema.</li>
     *   <li>Engine B starts with that snapshot (no DDL is applied to it,
     *       simulating that the BK ledgers carrying CREATE_TABLE / CREATE_INDEX
     *       have been trimmed).  It must hydrate its {@link SchemaTracker} from
     *       the snapshot so that DML entries can be routed correctly.</li>
     *   <li>Engine B only indexes the 20 new post-watermark DML entries
     *       (vector count == 20), proving that the tailer started from the
     *       watermark LSN, not from {@code START_OF_TIME}.  If the tailer had
     *       started from {@code START_OF_TIME} it would have re-indexed all
     *       50 original entries as well, making the count 70 instead of 20.</li>
     * </ol>
     */
    @Test
    public void testSchemaRecoveredFromWatermarkAfterRestart() throws Exception {
        Path logDir  = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        AtomicReference<WatermarkSnapshot> capturedSnapshot = new AtomicReference<>();
        WatermarkStore capturingStore = new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }
            @Override
            public void save(WatermarkSnapshot s) {
                capturedSnapshot.set(s);
            }
        };

        Table  table = buildTable();
        Index  index = buildVectorIndex();
        Random rng   = new Random(1);

        // ---- Phase 1: Engine A ----
        MemoryMetadataStorageManager meta = newMeta();
        IndexingServiceEngine engineA =
                new IndexingServiceEngine(logDir, dataDir, config);
        engineA.setMetadataStorageManager(meta);
        engineA.setWatermarkStore(capturingStore);
        try {
            engineA.start();

            // Apply DDL
            engineA.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engineA.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            // Apply 50 DML entries
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < 50; i++) {
                Record rec = RecordSerializer.makeRecord(
                        table, "pk", "k" + i, "vec", randomVec(rng));
                LogEntry ins = LogEntryFactory.insert(table, rec.key, rec.value, null);
                lastLsn = new LogSequenceNumber(1, 100 + i);
                engineA.applySingleEntryForTest(lastLsn, ins);
            }
            engineA.awaitPendingWorkForTest();
            engineA.setLastProcessedLsnForTest(lastLsn);

            // Checkpoint — this must persist the schema in the watermark snapshot.
            engineA.forceCheckpointAndSaveWatermark();
        } finally {
            engineA.close();
        }

        // Assertions on the saved snapshot.
        WatermarkSnapshot snap = capturedSnapshot.get();
        assertNotNull("checkpoint must have saved a watermark snapshot", snap);
        assertTrue("snapshot must carry schema (issue #368)", snap.hasSchema());
        assertEquals("exactly one table in schema", 1, snap.tables.size());
        assertEquals("table name preserved", "t1", snap.tables.get(0).name);
        assertEquals("exactly one vector index in schema", 1, snap.vectorIndexes.size());
        assertEquals("index name preserved", "vidx", snap.vectorIndexes.get(0).name);
        assertEquals("watermark must be at the last processed LSN",
                new LogSequenceNumber(1, 149), snap.lsn);

        // ---- Phase 2: Engine B ---- (simulates IS pod restart after BK trim)
        // Engine B is injected with the saved watermark that includes schema.
        // NO DDL is applied — simulates trimmed early BK ledgers where the
        // original CREATE_TABLE / CREATE_INDEX entries are no longer available.
        // The engine must recover schema from the watermark and correctly route
        // DML entries even without DDL in the commit log.
        IndexingServiceEngine engineB =
                new IndexingServiceEngine(logDir, dataDir, config);
        engineB.setMetadataStorageManager(meta);
        engineB.setWatermarkStore(new PreloadedWatermarkStore(snap));
        try {
            engineB.start(); // pre-populates SchemaTracker from watermark snapshot

            // Verify the index is visible (schema hydrated)
            List<IndexingServiceEngine.IndexDescriptor> indexes = engineB.listIndexes();
            assertEquals("engine B must see the index after schema recovery", 1, indexes.size());
            assertEquals("index name must match", "vidx", indexes.get(0).getIndex());

            // Apply 20 new DML entries with LSNs AFTER the watermark.
            // These must be indexed because the schema is available.
            LogSequenceNumber newLsn = null;
            for (int i = 0; i < 20; i++) {
                Record rec = RecordSerializer.makeRecord(
                        table, "pk", "new_k" + i, "vec", randomVec(rng));
                LogEntry ins = LogEntryFactory.insert(table, rec.key, rec.value, null);
                newLsn = new LogSequenceNumber(1, 200 + i);
                engineB.applySingleEntryForTest(newLsn, ins);
            }
            engineB.awaitPendingWorkForTest();

            // The 20 new vectors must be searchable.
            float[] queryVec = randomVec(rng);
            List<Map.Entry<Bytes, Float>> results =
                    engineB.search("default", "t1", "vidx", queryVec, 5);
            assertTrue(
                    "engine B must have indexed DML entries after schema recovery from watermark "
                            + "(issue #368); got " + results.size() + " results",
                    results.size() > 0);

            // Confirm the vector count matches the 20 newly inserted records.
            long vectorCount = engineB.getIndexStatus("default", "t1", "vidx").getVectorCount();
            assertEquals("all 20 newly inserted vectors must be indexed", 20, vectorCount);
        } finally {
            engineB.close();
        }
    }

    /**
     * Negative test — demonstrates the bug that issue #368 fixes:
     *
     * <p>An engine started with {@link WatermarkSnapshot#START_OF_TIME} (no
     * schema) that does not replay DDL entries silently drops every DML entry
     * because its {@link SchemaTracker} is empty. The vector count must be
     * zero after applying DML without prior DDL.
     *
     * <p>This test documents the pre-fix behaviour and acts as a guard to
     * ensure the schema-hydration path is distinguishable from a genuine
     * empty state.
     */
    @Test
    public void testDmlDroppedWhenNoSchemaNoDdlReplay() throws Exception {
        Path logDir  = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        Table  table = buildTable();
        Random rng   = new Random(99);

        MemoryMetadataStorageManager meta = newMeta();
        IndexingServiceEngine engine =
                new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(meta);
        // Inject START_OF_TIME (no schema, no previous watermark).
        engine.setWatermarkStore(new PreloadedWatermarkStore(WatermarkSnapshot.START_OF_TIME));
        try {
            engine.start();

            // Apply DML WITHOUT any DDL — this is the "BK ledgers trimmed" scenario.
            for (int i = 0; i < 20; i++) {
                Record rec = RecordSerializer.makeRecord(
                        table, "pk", "k" + i, "vec", randomVec(rng));
                LogEntry ins = LogEntryFactory.insert(table, rec.key, rec.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i), ins);
            }
            engine.awaitPendingWorkForTest();

            // With no schema, no vector store exists — search returns empty.
            float[] queryVec = randomVec(rng);
            List<Map.Entry<Bytes, Float>> results =
                    engine.search("default", "t1", "vidx", queryVec, 5);
            assertEquals(
                    "DML must be silently dropped when schema is unknown (no DDL replayed)",
                    0, results.size());
        } finally {
            engine.close();
        }
    }

    /**
     * Verifies that after schema is hydrated from the watermark snapshot,
     * DDL entries that arrive AFTER the watermark (e.g. a new index created
     * between the last checkpoint and the restart) are still applied normally
     * via the tailer (or equivalent direct apply) and produce working indexes.
     *
     * <p>Concretely: engine A applies DDL for "t1"+"vidx", checkpoints, then
     * applies DDL for a SECOND table "t2"+"vidx2" (not yet checkpointed).
     * Engine B starts from the watermark snapshot (has "t1"+"vidx"), then
     * receives the CREATE_TABLE/CREATE_INDEX for "t2"+"vidx2" via the tailer
     * (simulated with applySingleEntryForTest). Both indexes must end up
     * loaded on engine B.
     */
    @Test
    public void testNewDdlAfterWatermarkAppliedOnRecovery() throws Exception {
        Path logDir  = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        AtomicReference<WatermarkSnapshot> capturedSnapshot = new AtomicReference<>();
        WatermarkStore capturingStore = new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }
            @Override
            public void save(WatermarkSnapshot s) {
                capturedSnapshot.set(s);
            }
        };

        Table  t1   = buildTable();                          // t1 / vidx
        Table  t2   = Table.builder()
                .name("t2")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("v2", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
        Index  idx1 = buildVectorIndex();                    // vidx  on t1
        Index  idx2 = Index.builder()
                .name("vidx2")
                .table("t2")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("v2", ColumnTypes.FLOATARRAY)
                .build();

        Random rng = new Random(42);

        // ---- Engine A: checkpoint covers only t1/vidx ----
        MemoryMetadataStorageManager meta = newMeta();
        IndexingServiceEngine engineA =
                new IndexingServiceEngine(logDir, dataDir, config);
        engineA.setMetadataStorageManager(meta);
        engineA.setWatermarkStore(capturingStore);
        try {
            engineA.start();
            engineA.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(t1, null));
            engineA.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(idx1, null));

            LogSequenceNumber lastLsn = new LogSequenceNumber(1, 50);
            engineA.setLastProcessedLsnForTest(lastLsn);
            engineA.forceCheckpointAndSaveWatermark();
            // Snapshot now has t1+vidx; t2+vidx2 are not yet in it.
        } finally {
            engineA.close();
        }

        WatermarkSnapshot snap = capturedSnapshot.get();
        assertNotNull(snap);
        assertEquals("snapshot covers t1 only", 1, snap.tables.size());
        assertEquals("snapshot covers vidx only", 1, snap.vectorIndexes.size());

        // ---- Engine B: starts from snapshot, then receives post-watermark DDL ----
        IndexingServiceEngine engineB =
                new IndexingServiceEngine(logDir, dataDir, config);
        engineB.setMetadataStorageManager(meta);
        engineB.setWatermarkStore(new PreloadedWatermarkStore(snap));
        try {
            engineB.start();

            // At this point engine B knows about t1/vidx from the snapshot.
            // The tailer (simulated) delivers t2/vidx2 DDL entries after watermark.
            engineB.applyEntry(new LogSequenceNumber(1, 60),
                    LogEntryFactory.createTable(t2, null));
            engineB.applyEntry(new LogSequenceNumber(1, 61),
                    LogEntryFactory.createIndex(idx2, null));

            // Both indexes must now be loaded.
            List<IndexingServiceEngine.IndexDescriptor> indexes = engineB.listIndexes();
            assertEquals("both indexes must be loaded after recovery + post-watermark DDL",
                    2, indexes.size());

            // DML for both tables must be routed correctly.
            Record r1 = RecordSerializer.makeRecord(t1, "pk", "a", "vec", randomVec(rng));
            engineB.applySingleEntryForTest(new LogSequenceNumber(1, 70),
                    LogEntryFactory.insert(t1, r1.key, r1.value, null));

            Record r2 = RecordSerializer.makeRecord(t2, "pk", "b", "v2", randomVec(rng));
            engineB.applySingleEntryForTest(new LogSequenceNumber(1, 71),
                    LogEntryFactory.insert(t2, r2.key, r2.value, null));

            engineB.awaitPendingWorkForTest();

            assertEquals("t1/vidx must have 1 vector",
                    1L, engineB.getIndexStatus("default", "t1", "vidx").getVectorCount());
            assertEquals("t2/vidx2 must have 1 vector",
                    1L, engineB.getIndexStatus("default", "t2", "vidx2").getVectorCount());
        } finally {
            engineB.close();
        }
    }

    // ======================================================================
    // UUID round-trip test (issue #368 — PROP_IS_STORE_UUID persistence)
    // ======================================================================

    /**
     * An {@link InMemoryVectorStore} subclass that reports a fixed UUID via
     * {@link AbstractVectorStore#getStoreUUID()}.  Used to test the
     * {@link IndexingServiceEngine#PROP_IS_STORE_UUID} embedding path without
     * requiring a full {@link herddb.index.vector.PersistentVectorStore}.
     */
    private static final class UUIDReportingVectorStore extends InMemoryVectorStore {
        private final String uuid;

        UUIDReportingVectorStore(String vectorColumnName, String uuid) {
            super(vectorColumnName);
            this.uuid = uuid;
        }

        @Override
        public String getStoreUUID() {
            return uuid;
        }
    }

    /**
     * Verifies the {@link IndexingServiceEngine#PROP_IS_STORE_UUID} round-trip:
     * <ol>
     *   <li>Engine A uses a vector store factory that returns
     *       {@link UUIDReportingVectorStore} instances with a known UUID for
     *       each index.  After checkpoint the watermark snapshot must carry
     *       {@code _is.store.uuid} in the index's properties.</li>
     *   <li>Engine B uses a factory that records the UUID it receives via
     *       {@code indexProperties}.  The recorded UUID must equal the one
     *       embedded in the snapshot by Engine A — proving that a restarting
     *       engine passes the persisted UUID to the vector store factory so
     *       that a {@link herddb.index.vector.PersistentVectorStore} can
     *       locate its existing S3 checkpoint.</li>
     * </ol>
     */
    @Test
    public void testStoreUUIDRoundTripThroughWatermarkSnapshot() throws Exception {
        Path logDir  = folder.newFolder("log-uuid").toPath();
        Path dataDir = folder.newFolder("data-uuid").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        final String engineAUUID = "engine-a-store-uuid-12345";

        AtomicReference<WatermarkSnapshot> capturedSnapshot = new AtomicReference<>();

        // --- Engine A: uses a factory that returns stores with a known UUID ---
        MemoryMetadataStorageManager meta = newMeta();
        IndexingServiceEngine engineA = new IndexingServiceEngine(logDir, dataDir, config);
        engineA.setMetadataStorageManager(meta);
        engineA.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }
            @Override
            public void save(WatermarkSnapshot s) {
                capturedSnapshot.set(s);
            }
        });
        // Inject a factory that returns UUIDReportingVectorStore instances.
        engineA.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory2, indexProperties) ->
                new UUIDReportingVectorStore(vectorColumnName, engineAUUID));

        Table  table = buildTable();
        Index  index = buildVectorIndex();
        Random rng   = new Random(7);

        try {
            engineA.start();
            engineA.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engineA.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            // Apply a few DML entries so there is something to checkpoint.
            for (int i = 0; i < 10; i++) {
                Record rec = RecordSerializer.makeRecord(table, "pk", "k" + i, "vec", randomVec(rng));
                LogEntry ins = LogEntryFactory.insert(table, rec.key, rec.value, null);
                engineA.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), ins);
            }
            engineA.awaitPendingWorkForTest();
            engineA.setLastProcessedLsnForTest(new LogSequenceNumber(1, 19));
            engineA.forceCheckpointAndSaveWatermark();
        } finally {
            engineA.close();
        }

        WatermarkSnapshot snap = capturedSnapshot.get();
        assertNotNull("engine A must have saved a watermark snapshot", snap);
        assertTrue("snapshot must carry schema", snap.hasSchema());
        assertEquals("one index in snapshot", 1, snap.vectorIndexes.size());

        Index savedIdx = snap.vectorIndexes.get(0);
        assertEquals("PROP_IS_STORE_UUID must be embedded in snapshot",
                engineAUUID, savedIdx.properties.get(IndexingServiceEngine.PROP_IS_STORE_UUID));

        // --- Engine B: records the UUID received from indexProperties ---
        AtomicReference<String> uuidReceivedByEngineB = new AtomicReference<>();
        AtomicReference<WatermarkSnapshot> snapBRef = new AtomicReference<>();
        // A capturing watermark store that seeds Engine B with the snapshot
        // saved by Engine A and records every subsequent save().
        WatermarkStore storeBWrapped = new WatermarkStore() {
            private WatermarkSnapshot current = snap;
            @Override
            public WatermarkSnapshot load() {
                return current;
            }
            @Override
            public void save(WatermarkSnapshot s) {
                current = s;
                snapBRef.set(s);
            }
        };

        IndexingServiceEngine engineB = new IndexingServiceEngine(logDir, dataDir, config);
        engineB.setMetadataStorageManager(meta);
        engineB.setWatermarkStore(storeBWrapped);
        engineB.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory2, indexProperties) -> {
            String receivedUUID = indexProperties != null
                    ? indexProperties.get(IndexingServiceEngine.PROP_IS_STORE_UUID) : null;
            uuidReceivedByEngineB.set(receivedUUID);
            return new InMemoryVectorStore(vectorColumnName);
        });

        try {
            engineB.start(); // installSchemaFromSnapshot → factory called with PROP_IS_STORE_UUID

            assertNotNull("engine B factory must have been called for the index", uuidReceivedByEngineB.get());
            assertEquals(
                    "engine B factory must receive the UUID that engine A embedded in the snapshot",
                    engineAUUID, uuidReceivedByEngineB.get());

            // Sanity: no UUID property is set for an index backed by a store whose
            // getStoreUUID() returns null (InMemoryVectorStore).
            engineB.setLastProcessedLsnForTest(new LogSequenceNumber(1, 19));
            engineB.forceCheckpointAndSaveWatermark();
            WatermarkSnapshot snapB = snapBRef.get();
            assertNotNull("engine B must have saved a checkpoint", snapB);
            assertNull("InMemoryVectorStore has no getStoreUUID(); PROP_IS_STORE_UUID must not appear",
                    snapB.vectorIndexes.get(0).properties.get(IndexingServiceEngine.PROP_IS_STORE_UUID));
        } finally {
            engineB.close();
        }
    }
}
