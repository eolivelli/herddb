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
import static org.junit.Assert.assertNotEquals;
import herddb.codec.RecordSerializer;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #364: the IS must publish its <em>durable</em> recovery LSN — the
 * LSN of the most recent checkpoint whose watermark has been persisted to
 * remote storage — so that the server's commit-log retention floor never
 * drops a ledger the IS would still need to replay on a restart. The
 * volatile in-memory tailer position ({@code lastProcessedLsn}) advances
 * on every applied entry and is therefore unsafe as a retention floor.
 *
 * <p>This test exercises the engine end-to-end:
 * <ol>
 *   <li>{@code lastDurableLsn} is initialized from the loaded
 *       {@link WatermarkSnapshot#lsn} on engine start.</li>
 *   <li>It does NOT advance simply because entries were applied — only the
 *       tailer position advances when a successful save has not yet
 *       happened.</li>
 *   <li>It DOES advance after a successful
 *       {@link IndexingServiceEngine#forceCheckpointAndSaveWatermark()}.</li>
 *   <li>If the watermark save fails (I/O error from the
 *       {@link WatermarkStore}), {@code lastDurableLsn} stays anchored at
 *       the previous durable value while {@code lastProcessedLsn} keeps
 *       advancing.</li>
 *   <li>{@code getIndexStatus(...)} reports both LSNs distinctly so the
 *       gRPC layer can serialize them as {@code tailer_lsn_*} and
 *       {@code durable_lsn_*}.</li>
 * </ol>
 */
public class IndexingServiceEngineDurableLsnTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private int savedMinLive;
    private long savedDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        // Disable the min-live-vectors gate so checkpoints reach Phase B
        // even with a small number of test vectors.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedDeferral;
    }

    /**
     * In-memory {@link WatermarkStore} whose {@code save()} can be flipped
     * into a permanent failure mode: a single test can observe the engine's
     * behaviour both when the save succeeds and when it fails, on the same
     * engine instance.
     */
    private static final class FailableWatermarkStore implements WatermarkStore {
        private final AtomicReference<WatermarkSnapshot> saved =
                new AtomicReference<>(WatermarkSnapshot.START_OF_TIME);
        private volatile boolean failOnNextSave;

        FailableWatermarkStore(WatermarkSnapshot seed) {
            saved.set(seed);
        }

        void setFailOnNextSave(boolean fail) {
            this.failOnNextSave = fail;
        }

        @Override
        public synchronized WatermarkSnapshot load() {
            return saved.get();
        }

        @Override
        public synchronized void save(WatermarkSnapshot snapshot) throws IOException {
            if (failOnNextSave) {
                throw new IOException("simulated watermark save failure");
            }
            saved.set(snapshot);
        }
    }

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private Table createTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds an engine with a real {@link PersistentVectorStore} factory
     * and a custom {@link WatermarkStore}. Returns the engine ready to
     * receive DDL.
     */
    private IndexingServiceEngine buildEngine(WatermarkStore watermark) throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        engine.setWatermarkStore(watermark);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(
                128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, "tstblspace", vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE,
                    VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });
        return engine;
    }

    /**
     * On a fresh engine (no persisted watermark), {@code lastDurableLsn} is
     * {@link LogSequenceNumber#START_OF_TIME}. Applying entries advances
     * the tailer position only — durable stays at START_OF_TIME until a
     * checkpoint succeeds.
     */
    @Test
    public void freshEngineHasStartOfTimeDurableLsn() throws Exception {
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(WatermarkSnapshot.START_OF_TIME);
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();
            assertEquals("fresh engine durable LSN must be START_OF_TIME",
                    LogSequenceNumber.START_OF_TIME, engine.getLastDurableLsn());
            assertEquals("tailer LSN must also start at START_OF_TIME",
                    LogSequenceNumber.START_OF_TIME, engine.getLastProcessedLsn());

            // Apply a few entries — tailer advances, durable does not.
            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.setLastProcessedLsnForTest(new LogSequenceNumber(1, 1));

            assertEquals("durable LSN must NOT advance just because the tailer applied entries",
                    LogSequenceNumber.START_OF_TIME, engine.getLastDurableLsn());
            assertEquals("tailer LSN must reflect the last applied entry",
                    new LogSequenceNumber(1, 1), engine.getLastProcessedLsn());
        } finally {
            engine.close();
        }
    }

    /**
     * The engine resumes its durable LSN from the persisted snapshot — this
     * is the recovery floor the server's retention pin must respect.
     */
    @Test
    public void durableLsnRecoveredFromWatermarkSnapshot() throws Exception {
        LogSequenceNumber persisted = new LogSequenceNumber(7, 113);
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(new WatermarkSnapshot(persisted, 1));
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();
            assertEquals("durable LSN must equal the loaded watermark snapshot",
                    persisted, engine.getLastDurableLsn());
            assertEquals("tailer LSN is initialized from the same snapshot",
                    persisted, engine.getLastProcessedLsn());
        } finally {
            engine.close();
        }
    }

    /**
     * After a successful {@code forceCheckpointAndSaveWatermark()}, the
     * durable LSN advances to match the captured checkpoint LSN — that
     * value is now the safe recovery floor for the server.
     */
    @Test
    public void durableLsnAdvancesAfterSuccessfulCheckpoint() throws Exception {
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(WatermarkSnapshot.START_OF_TIME);
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();

            // Apply DDL and enough vectors that Phase B has work to do.
            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            Index index = Index.builder()
                    .name("vidx")
                    .table("vectable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            Random rng = new Random(101);
            int numVectors = 256;
            int dim = 16;
            long baseLsn = 100;
            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < numVectors; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(lastLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(lastLsn);

            assertEquals("before checkpoint, durable still START_OF_TIME",
                    LogSequenceNumber.START_OF_TIME, engine.getLastDurableLsn());

            engine.forceCheckpointAndSaveWatermark();

            assertEquals("durable LSN must advance to the checkpoint LSN",
                    lastLsn, engine.getLastDurableLsn());
            assertEquals("durable LSN equals tailer LSN once the checkpoint succeeds",
                    engine.getLastProcessedLsn(), engine.getLastDurableLsn());
        } finally {
            engine.close();
        }
    }

    /**
     * Issue #364 core invariant: when {@code watermarkStore.save()} fails,
     * the durable LSN must stay anchored at the previous durable value,
     * even though the in-memory tailer position has advanced past the
     * failed checkpoint LSN. The server's retention floor must never use
     * the tailer position as a recovery point.
     */
    @Test
    public void durableLsnDoesNotAdvanceIfWatermarkSaveFails() throws Exception {
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(WatermarkSnapshot.START_OF_TIME);
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();

            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            Index index = Index.builder()
                    .name("vidx")
                    .table("vectable")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .build();
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            // First (successful) checkpoint — establishes a baseline durable LSN.
            Random rng = new Random(202);
            int dim = 16;
            long baseLsn = 100;
            LogSequenceNumber firstLsn = null;
            for (int i = 0; i < 256; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                firstLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(firstLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(firstLsn);
            engine.forceCheckpointAndSaveWatermark();
            LogSequenceNumber baselineDurable = engine.getLastDurableLsn();
            assertEquals("baseline durable LSN must equal first checkpoint LSN",
                    firstLsn, baselineDurable);

            // Second batch of inserts — tailer advances past baseline.
            LogSequenceNumber secondLsn = null;
            for (int i = 0; i < 200; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + (1000 + i),
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                secondLsn = new LogSequenceNumber(2, i + 1);
                engine.applySingleEntryForTest(secondLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(secondLsn);

            // Second checkpoint with the watermark save failing — durable
            // LSN must stay at the baseline, NOT advance to secondLsn.
            watermark.setFailOnNextSave(true);
            engine.forceCheckpointAndSaveWatermark();

            assertEquals("durable LSN must stay anchored when the save fails",
                    baselineDurable, engine.getLastDurableLsn());
            assertNotEquals(
                    "tailer LSN has advanced past the durable LSN — exactly the "
                            + "scenario where reporting tailer LSN to the server "
                            + "would break recovery (issue #364)",
                    engine.getLastProcessedLsn(), engine.getLastDurableLsn());
            assertEquals("tailer reflects the last applied entry",
                    secondLsn, engine.getLastProcessedLsn());

            // Recover: the next save succeeds and the durable LSN catches up.
            watermark.setFailOnNextSave(false);
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("durable LSN catches up after the next successful save",
                    secondLsn, engine.getLastDurableLsn());
        } finally {
            engine.close();
        }
    }

    /**
     * Verifies {@link IndexingServiceEngine#getIndexStatus} carries the two
     * LSNs as distinct fields — this is what the gRPC server then maps onto
     * {@code tailer_lsn_*} and {@code durable_lsn_*} on the wire.
     */
    @Test
    public void getIndexStatusReportsBothLsnsDistinctly() throws Exception {
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(WatermarkSnapshot.START_OF_TIME);
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();

            Table table = createTable();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.setLastProcessedLsnForTest(new LogSequenceNumber(5, 42));

            IndexingServiceEngine.IndexStatusInfo info =
                    engine.getIndexStatus("local", "", "");
            assertEquals("tailer ledger from getIndexStatus",
                    5, info.getTailerLsnLedger());
            assertEquals("tailer offset from getIndexStatus",
                    42, info.getTailerLsnOffset());
            assertEquals("durable ledger from getIndexStatus is START_OF_TIME ledger (-1)",
                    LogSequenceNumber.START_OF_TIME.ledgerId,
                    info.getDurableLsnLedger());
            assertEquals("durable offset from getIndexStatus is START_OF_TIME offset (-1)",
                    LogSequenceNumber.START_OF_TIME.offset,
                    info.getDurableLsnOffset());
        } finally {
            engine.close();
        }
    }
}
