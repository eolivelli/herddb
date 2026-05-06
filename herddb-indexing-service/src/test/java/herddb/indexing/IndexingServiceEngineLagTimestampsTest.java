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
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #423: the indexing-service engine reports the wall-clock timestamp of
 * the last processed {@link LogEntry} (so dashboards can compute
 * {@code now - timestamp = tailer_lag_ms}) and the timestamp at the durable
 * watermark LSN (for {@code durable_lag_ms}).
 *
 * <p>These tests cover:
 * <ol>
 *   <li>{@code lastProcessedEntryTimestamp} advances on every entry that flows
 *       through the tailer's {@code processEntry} path.</li>
 *   <li>{@code lastDurableEntryTimestamp} only moves after a successful
 *       {@code forceCheckpointAndSaveWatermark()}.</li>
 *   <li>It stays anchored at the previous durable value when the watermark
 *       save fails — same invariant as the durable LSN itself.</li>
 *   <li>Both timestamps are exposed via {@code getIndexStatus()}.</li>
 *   <li>The persisted watermark snapshot carries the durable timestamp, so a
 *       restarting engine re-hydrates {@code lastDurableEntryTimestamp} from
 *       the snapshot rather than starting at 0.</li>
 * </ol>
 */
public class IndexingServiceEngineLagTimestampsTest {

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
     * into a permanent failure mode. Mirrors the helper used in
     * {@link IndexingServiceEngineDurableLsnTest}.
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

        synchronized WatermarkSnapshot peek() {
            return saved.get();
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
     * The engine's tailer freshness clock must match the LogEntry timestamp
     * fed into {@code processEntry}, and {@link IndexingServiceEngine.IndexStatusInfo}
     * must surface both that and the durable freshness via the same accessor
     * the gRPC layer reads.
     */
    @Test
    public void tailerTimestampAdvancesViaProcessEntryAndIsExposedOnGetIndexStatus() throws Exception {
        FailableWatermarkStore watermark =
                new FailableWatermarkStore(WatermarkSnapshot.START_OF_TIME);
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();

            assertEquals("fresh engine reports tailer timestamp as unknown (0)",
                    0L, engine.getLastProcessedEntryTimestamp());

            // Drive an entry through the same path as the live tailer
            // (processEntryForTest -> processEntry).
            Table table = createTable();
            long entryTimestamp = 1_700_000_001_234L;
            LogEntry create = LogEntryFactory.createTable(table, null);
            // LogEntryFactory.createTable already stamps the timestamp from
            // System.currentTimeMillis(); for a deterministic assertion we
            // replace it with a hand-crafted one.
            LogEntry stamped = new LogEntry(entryTimestamp,
                    create.type, create.transactionId, create.tableId,
                    create.key, create.value);
            engine.processEntryForTest(new LogSequenceNumber(1, 1), stamped);

            assertEquals("tailer timestamp must advance to the LogEntry timestamp",
                    entryTimestamp, engine.getLastProcessedEntryTimestamp());

            // And it must surface on getIndexStatus.
            IndexingServiceEngine.IndexStatusInfo info =
                    engine.getIndexStatus("local", "", "");
            assertEquals("getIndexStatus.tailer_lsn_timestamp must match",
                    entryTimestamp, info.getTailerLsnTimestamp());
            assertEquals("getIndexStatus.durable_lsn_timestamp must still be 0 (no checkpoint)",
                    0L, info.getDurableLsnTimestamp());
        } finally {
            engine.close();
        }
    }

    /**
     * After a successful {@code forceCheckpointAndSaveWatermark()}, the
     * durable timestamp must advance to the timestamp captured at the same
     * instant as the checkpoint LSN, and the persisted snapshot must carry
     * it.
     */
    @Test
    public void durableTimestampAdvancesAfterSuccessfulCheckpoint() throws Exception {
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

            Random rng = new Random(303);
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
            // Pretend the very last LogEntry the tailer saw had this timestamp.
            long lastEntryTs = 1_700_000_500_000L;
            engine.setLastProcessedEntryTimestampForTest(lastEntryTs);

            assertEquals("before checkpoint, durable timestamp still 0",
                    0L, engine.getLastDurableEntryTimestamp());

            engine.forceCheckpointAndSaveWatermark();

            assertEquals("durable timestamp must advance to the captured value",
                    lastEntryTs, engine.getLastDurableEntryTimestamp());
            assertEquals("persisted watermark must carry the timestamp",
                    lastEntryTs, watermark.peek().lastEntryTimestamp);
            // Equally, getIndexStatus exposes both fields.
            IndexingServiceEngine.IndexStatusInfo info =
                    engine.getIndexStatus("local", "vectable", "vidx");
            assertEquals("getIndexStatus.durable_lsn_timestamp",
                    lastEntryTs, info.getDurableLsnTimestamp());
            assertEquals("getIndexStatus.tailer_lsn_timestamp",
                    lastEntryTs, info.getTailerLsnTimestamp());
        } finally {
            engine.close();
        }
    }

    /**
     * Mirror of {@code durableLsnDoesNotAdvanceIfWatermarkSaveFails}: when the
     * watermark save fails, the durable timestamp must stay anchored at the
     * previous durable value even though the tailer timestamp has moved on.
     */
    @Test
    public void durableTimestampDoesNotAdvanceIfWatermarkSaveFails() throws Exception {
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

            // First successful checkpoint establishes a baseline.
            Random rng = new Random(404);
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
            long firstTs = 1_700_000_100_000L;
            engine.setLastProcessedEntryTimestampForTest(firstTs);
            engine.forceCheckpointAndSaveWatermark();
            assertEquals("baseline durable timestamp set", firstTs,
                    engine.getLastDurableEntryTimestamp());

            // Now the second checkpoint will fail to save: the tailer
            // timestamp keeps advancing, but the durable timestamp must NOT.
            watermark.setFailOnNextSave(true);
            LogSequenceNumber secondLsn = null;
            for (int i = 256; i < 384; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "k" + i,
                        "vec", randomVector(rng, dim));
                LogEntry insert =
                        LogEntryFactory.insert(table, record.key, record.value, null);
                secondLsn = new LogSequenceNumber(1, baseLsn + i);
                engine.applySingleEntryForTest(secondLsn, insert);
            }
            engine.awaitPendingWorkForTest();
            engine.setLastProcessedLsnForTest(secondLsn);
            long secondTs = 1_700_000_200_000L;
            engine.setLastProcessedEntryTimestampForTest(secondTs);
            engine.forceCheckpointAndSaveWatermark();

            assertEquals("durable timestamp must stay anchored at the previous durable value",
                    firstTs, engine.getLastDurableEntryTimestamp());
            assertNotEquals("tailer timestamp must have moved on",
                    firstTs, engine.getLastProcessedEntryTimestamp());
            assertEquals("tailer timestamp tracks the most recently processed entry",
                    secondTs, engine.getLastProcessedEntryTimestamp());
        } finally {
            engine.close();
        }
    }

    /**
     * Issue #423 + #364: a restarting engine must re-hydrate the durable
     * freshness from the persisted watermark snapshot — otherwise dashboards
     * would briefly show a "0 (unknown)" durable lag every time a pod
     * restarts, even though the engine in fact recovers from a known
     * checkpoint.
     */
    @Test
    public void durableTimestampRecoveredFromWatermarkSnapshot() throws Exception {
        LogSequenceNumber persisted = new LogSequenceNumber(7, 113);
        long persistedTs = 1_700_000_777_000L;
        FailableWatermarkStore watermark = new FailableWatermarkStore(
                new WatermarkSnapshot(persisted, 1, persistedTs,
                        Collections.emptyList(), Collections.emptyList()));
        IndexingServiceEngine engine = buildEngine(watermark);
        try {
            engine.start();
            assertEquals("durable timestamp re-hydrated from persisted snapshot",
                    persistedTs, engine.getLastDurableEntryTimestamp());
            // The tailer freshness is also pre-seeded so the very first
            // GetIndexStatus after boot already carries a meaningful value.
            assertEquals("tailer timestamp pre-seeded from persisted snapshot",
                    persistedTs, engine.getLastProcessedEntryTimestamp());
        } finally {
            engine.close();
        }
    }
}
