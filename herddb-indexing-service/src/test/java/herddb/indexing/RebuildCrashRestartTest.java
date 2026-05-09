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
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies the crash-restart contract: a rebuild that
 * fails on engine N must re-run on engine N+1 (where N+1 boots from
 * the same persisted watermark, which stayed anchored before the
 * failed CREATE_INDEX because the engine entered FAILED state).
 *
 * <p>Two test scenarios:
 * <ul>
 *   <li>Failed rebuild on engine 1 leaves watermark at LSN before
 *       CREATE_INDEX. Engine 2, given the same watermark and a
 *       working DSM, replays CREATE_INDEX from the tailer and the
 *       rebuild succeeds.</li>
 *   <li>Successful rebuild on engine 1 advances the watermark past
 *       CREATE_INDEX. Engine 2, hydrating from the persisted
 *       snapshot, recreates the vector store via {@code
 *       installSchemaFromSnapshot} (the schema-only path) and does
 *       NOT re-fire the rebuild.</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class RebuildCrashRestartTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private static final LogSequenceNumber REBUILD_LSN = new LogSequenceNumber(7L, 13L);

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createIndex() {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_REBUILD, "true")
                .property(VectorIndexManager.PROP_REBUILD_LSN,
                        VectorIndexManager.encodeRebuildLsn(REBUILD_LSN))
                .build();
    }

    private IndexingServiceEngine buildEngine(MemoryDataStorageManager dsm,
                                              WatermarkStore watermarkStore) throws Exception {
        Path logDir = folder.newFolder().toPath();
        Path dataDir = folder.newFolder().toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        MemoryMetadataStorageManager memMeta = new MemoryMetadataStorageManager();
        memMeta.start();
        memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(memMeta);
        engine.setDataStorageManager(dsm);
        if (watermarkStore != null) {
            engine.setWatermarkStore(watermarkStore);
        }
        engine.start();
        return engine;
    }

    @Test
    public void failedRebuildOnFirstEngine_isReplayedOnSecondEngine() throws Exception {
        Table table = createTable();
        Index index = createIndex();
        // Pre-stage 10 records in the SHARED record list (both
        // engines see the same data, simulating production where
        // two IS pods share a remote storage).
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records.add(RecordSerializer.makeRecord(table,
                    "pk", "key" + i,
                    "vec", new float[]{i * 0.1f, 0f, 0f}));
        }

        // Shared in-memory watermark store — engine 1 writes its
        // watermark, engine 2 reads it on start. Mirrors the
        // production WatermarkStore contract (file or S3-backed).
        InMemoryWatermarkStore sharedWatermark =
                new InMemoryWatermarkStore(WatermarkSnapshot.START_OF_TIME);

        // Engine 1: failing DSM. The rebuild's fullTableScan throws,
        // triggerRebuildIfNeeded escalates to FAILED, watermark
        // STAYS at the CREATE_TABLE LSN. The watermark store is
        // never updated past that point because checkpointAndSaveWatermark
        // never fires (we never call forceCheckpointAndSaveWatermark
        // and the periodic interval doesn't elapse in the test).
        FailingFakeDsm failingDsm = new FailingFakeDsm(REBUILD_LSN, records);
        failingDsm.failNextScans.set(1);
        IndexingServiceEngine engine1 = buildEngine(failingDsm, sharedWatermark);
        try {
            LogSequenceNumber createTableLsn = new LogSequenceNumber(1, 1);
            engine1.processEntryForTest(createTableLsn,
                    LogEntryFactory.createTable(table, null));
            engine1.processEntryForTest(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            assertEquals("engine 1 must be FAILED after rebuild scan failure",
                    IndexingServiceEngine.EngineStatus.FAILED, engine1.getEngineStatus());
            assertEquals("engine 1 watermark must NOT advance past CREATE_INDEX",
                    createTableLsn, engine1.getLastProcessedLsn());

            // Force the watermark save — this simulates a periodic
            // checkpoint cycle that fired before the crash. Even on
            // FAILED the engine's existing checkpoint code persists
            // whatever lastProcessedLsn currently is, which is the
            // pre-CREATE_INDEX value. No data loss because the
            // CREATE_INDEX entry itself is not in the snapshot.
            engine1.forceCheckpointAndSaveWatermark();

            WatermarkSnapshot persistedAfterFailure = sharedWatermark.load();
            assertEquals("persisted watermark must equal the pre-CREATE_INDEX LSN",
                    createTableLsn, persistedAfterFailure.lsn);
        } finally {
            engine1.close();
        }

        // Engine 2: working DSM, same shared watermark. On start
        // it hydrates schema from the snapshot — note that the
        // snapshot DOES include BOTH the CREATE_TABLE AND the
        // failed CREATE_INDEX, because applyEntry's switch case
        // for CREATE_INDEX runs schemaTracker.applyEntry and
        // createVectorStoreIfNeeded BEFORE triggerRebuildIfNeeded.
        // So at the moment engine 1 enters FAILED, the schema state
        // is already mutated and the snapshot captures it.
        // installSchemaFromSnapshot on engine 2 re-installs the
        // index via createVectorStoreIfNeeded (the schema-only
        // path, no rebuild). We then re-deliver the same
        // CREATE_INDEX entry through processEntryForTest
        // (simulating BookKeeper redelivering the entry from the
        // commit log starting at the persisted watermark LSN); the
        // engine's applyEntry treats it as a duplicate at the
        // schema level (createVectorStoreIfNeeded sees the existing
        // store and skips) but still fires triggerRebuildIfNeeded —
        // which is what re-runs the back-fill on a working DSM.
        WorkingFakeDsm workingDsm = new WorkingFakeDsm(REBUILD_LSN, records);
        IndexingServiceEngine engine2 = buildEngine(workingDsm, sharedWatermark);
        try {
            assertEquals("engine 2 must boot in ACTIVE state (FAILED is per-process)",
                    IndexingServiceEngine.EngineStatus.ACTIVE, engine2.getEngineStatus());
            engine2.processEntryForTest(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            assertEquals("engine 2 must NOT enter FAILED on the replay (DSM works)",
                    IndexingServiceEngine.EngineStatus.ACTIVE, engine2.getEngineStatus());
            assertEquals("engine 2 watermark must advance past CREATE_INDEX after replay",
                    new LogSequenceNumber(1, 2), engine2.getLastProcessedLsn());
            // The rebuild must have populated the vector store.
            assertEquals("engine 2 must have indexed every record on the replay",
                    10L, engine2.getRebuildMetricsForTest().recordsIndexed.sum());
            assertEquals("engine 2 fullTableScan must have fired exactly once",
                    1, workingDsm.fullTableScanInvocations.get());
        } finally {
            engine2.close();
        }
    }

    @Test
    public void successfulRebuildOnFirstEngine_isNotReplayedOnSecondEngine() throws Exception {
        // Symmetric to the failure-replay test: when engine 1
        // succeeds and the watermark advances past CREATE_INDEX,
        // engine 2 must NOT re-fire the rebuild on startup. The
        // installSchemaFromSnapshot path creates the vector store
        // via createVectorStoreIfNeeded (schema-only), but does
        // NOT enter the live applyEntry path, so triggerRebuildIfNeeded
        // is not called.
        Table table = createTable();
        Index index = createIndex();
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(RecordSerializer.makeRecord(table,
                    "pk", "key" + i,
                    "vec", new float[]{i * 0.1f, 0f, 0f}));
        }

        // Engine 1 first: succeed, then close. The shared watermark
        // store is now at the CREATE_INDEX LSN.
        WorkingFakeDsm engine1Dsm = new WorkingFakeDsm(REBUILD_LSN, records);
        InMemoryWatermarkStore sharedWatermark =
                new InMemoryWatermarkStore(WatermarkSnapshot.START_OF_TIME);

        IndexingServiceEngine engine1 = buildEngine(engine1Dsm, sharedWatermark);
        try {
            engine1.processEntryForTest(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine1.processEntryForTest(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));
            assertEquals("engine 1 must be ACTIVE after successful rebuild",
                    IndexingServiceEngine.EngineStatus.ACTIVE, engine1.getEngineStatus());
            assertEquals("engine 1 watermark must advance past CREATE_INDEX",
                    new LogSequenceNumber(1, 2), engine1.getLastProcessedLsn());
            engine1.forceCheckpointAndSaveWatermark();
            assertEquals("persisted watermark must match the post-CREATE_INDEX LSN",
                    new LogSequenceNumber(1, 2), sharedWatermark.load().lsn);
        } finally {
            engine1.close();
        }

        // Engine 2: FRESH DSM (not shared with engine 1, whose
        // close() called dsm.close() which clears
        // MemoryDataStorageManager state). The shared watermark
        // store carries the post-CREATE_INDEX snapshot. Engine 2
        // boots, hydrates schema via installSchemaFromSnapshot
        // (the schema-only path), and must NOT fire any
        // fullTableScan because the live applyEntry path is not
        // re-entered for the snapshot's synthetic CREATE_INDEX.
        WorkingFakeDsm engine2Dsm = new WorkingFakeDsm(REBUILD_LSN, records);
        IndexingServiceEngine engine2 = buildEngine(engine2Dsm, sharedWatermark);
        try {
            assertEquals("snapshot replay must NOT fire fullTableScan on engine 2",
                    0, engine2Dsm.fullTableScanInvocations.get());
            assertEquals("engine 2 must boot ACTIVE",
                    IndexingServiceEngine.EngineStatus.ACTIVE,
                    engine2.getEngineStatus());
            assertEquals("engine 2 must report zero rebuild activity",
                    0L, engine2.getRebuildMetricsForTest().recordsScanned.sum());
        } finally {
            engine2.close();
        }
    }

    /** In-memory watermark store; used to share state across two engine instances. */
    private static final class InMemoryWatermarkStore implements WatermarkStore {
        private WatermarkSnapshot snapshot;

        InMemoryWatermarkStore(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public synchronized WatermarkSnapshot load() {
            return snapshot;
        }

        @Override
        public synchronized void save(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }

    /** Working fake DSM that emits the staged record list as one page. */
    private static final class WorkingFakeDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;
        final AtomicInteger fullTableScanInvocations = new AtomicInteger(0);

        WorkingFakeDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = Objects.requireNonNull(expectedLsn);
            this.records = new ArrayList<>(records);
        }

        @Override
        public void fullTableScan(String tableSpace, String uuid,
                                  LogSequenceNumber sequenceNumber,
                                  FullTableScanConsumer consumer)
                throws DataStorageManagerException {
            fullTableScanInvocations.incrementAndGet();
            TableStatus status = new TableStatus("vectable", expectedLsn,
                    Bytes.longToByteArray(0L), 1L, new HashMap<>());
            consumer.acceptTableStatus(status);
            consumer.acceptPage(0L, Arrays.asList(records.toArray(new Record[0])));
            consumer.endTable();
        }
    }

    /** Failing fake DSM — first N scans throw, then it works. */
    private static final class FailingFakeDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;
        final AtomicInteger failNextScans = new AtomicInteger(0);

        FailingFakeDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = Objects.requireNonNull(expectedLsn);
            this.records = new ArrayList<>(records);
        }

        @Override
        public void fullTableScan(String tableSpace, String uuid,
                                  LogSequenceNumber sequenceNumber,
                                  FullTableScanConsumer consumer)
                throws DataStorageManagerException {
            if (failNextScans.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new DataStorageManagerException(
                        "injected scan failure for crash-restart test");
            }
            TableStatus status = new TableStatus("vectable", expectedLsn,
                    Bytes.longToByteArray(0L), 1L, new HashMap<>());
            consumer.acceptTableStatus(status);
            consumer.acceptPage(0L, Arrays.asList(records.toArray(new Record[0])));
            consumer.endTable();
        }
    }
}
