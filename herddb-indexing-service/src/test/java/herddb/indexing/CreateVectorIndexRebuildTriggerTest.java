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
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies that the engine's CREATE_INDEX path fires the
 * back-fill rebuild only when the live tailer delivers a CREATE_INDEX
 * entry with {@code rebuild=true}, and never on the schema-replay
 * paths ({@code installSchemaFromSnapshot} /
 * {@code installSchemaFromDescriptor}) that synthesise CREATE_INDEX
 * entries from a persisted snapshot or rebalance descriptor.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexRebuildTriggerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);

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

    private Index createIndexWithRebuild() {
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

    private Index createIndexWithoutRebuild() {
        return Index.builder()
                .name("vidx")
                .table("vectable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    private IndexingServiceEngine buildEngine(CountingFakeDsm dsm) throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        MemoryMetadataStorageManager memMeta = new MemoryMetadataStorageManager();
        memMeta.start();
        memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(memMeta);
        engine.setDataStorageManager(dsm);
        // Use the default vectorStoreFactory (InMemoryVectorStore) — the
        // store under test does not need persistence; we just need
        // observable size() and search() to verify the rebuild
        // populated it.
        engine.start();
        return engine;
    }

    @Test
    public void liveCreateIndexWithRebuildTrue_firesRebuilder() throws Exception {
        Table table = createTable();
        Index index = createIndexWithRebuild();
        // Pre-stage a few records in the fake DSM at the rebuild LSN.
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            float[] vec = new float[]{i * 0.1f, i * 0.2f, i * 0.3f};
            records.add(RecordSerializer.makeRecord(table,
                    "pk", "key" + i, "vec", vec));
        }
        CountingFakeDsm dsm = new CountingFakeDsm(REBUILD_LSN, records);

        IndexingServiceEngine engine = buildEngine(dsm);
        try {
            // Live tailer path: applyEntry CREATE_TABLE + CREATE_INDEX.
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            assertEquals("fullTableScan must fire exactly once for rebuild=true",
                    1, dsm.fullTableScanInvocations.get());
            assertEquals("rebuild metrics must reflect 10 scanned records",
                    10L, engine.getRebuildMetricsForTest().recordsScanned.sum());
            assertEquals("rebuild metrics must reflect 10 indexed records (predicate accepts all)",
                    10L, engine.getRebuildMetricsForTest().recordsIndexed.sum());
            assertTrue("rebuild start time must be recorded",
                    engine.getRebuildMetricsForTest().lastRebuildStartTimeMillis > 0);
            assertTrue("rebuild end time must be at-or-after start time",
                    engine.getRebuildMetricsForTest().lastRebuildEndTimeMillis
                            >= engine.getRebuildMetricsForTest().lastRebuildStartTimeMillis);
        } finally {
            engine.close();
        }
    }

    @Test
    public void liveCreateIndexWithoutRebuildFlag_doesNotFireRebuilder() throws Exception {
        Table table = createTable();
        Index indexNoRebuild = createIndexWithoutRebuild();
        CountingFakeDsm dsm = new CountingFakeDsm(REBUILD_LSN, Collections.emptyList());

        IndexingServiceEngine engine = buildEngine(dsm);
        try {
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(indexNoRebuild, null));

            assertEquals("fullTableScan must NOT fire when rebuild flag is absent",
                    0, dsm.fullTableScanInvocations.get());
            assertEquals("rebuild metrics must remain at zero",
                    0L, engine.getRebuildMetricsForTest().recordsScanned.sum());
        } finally {
            engine.close();
        }
    }

    @Test
    public void liveCreateIndexNonVectorWithRebuildFlag_doesNotFireRebuilder() throws Exception {
        // Defensive: the rebuild flag is meaningful only on vector
        // indexes. A non-vector index that happens to carry the flag
        // (e.g. a future feature) must not accidentally fire the
        // rebuilder, which would crash on the
        // "vector index must have exactly one column" check.
        Table table = Table.builder()
                .name("plaintable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("n", ColumnTypes.INTEGER)
                .primaryKey("pk")
                .build();
        Index hashWithRebuild = Index.builder()
                .name("hidx")
                .table("plaintable")
                .tablespace("default")
                .type(Index.TYPE_HASH)
                .column("n", ColumnTypes.INTEGER)
                .property(VectorIndexManager.PROP_REBUILD, "true")
                .property(VectorIndexManager.PROP_REBUILD_LSN,
                        VectorIndexManager.encodeRebuildLsn(REBUILD_LSN))
                .build();
        CountingFakeDsm dsm = new CountingFakeDsm(REBUILD_LSN, Collections.emptyList());

        IndexingServiceEngine engine = buildEngine(dsm);
        try {
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(hashWithRebuild, null));

            assertEquals("fullTableScan must NOT fire for non-vector index",
                    0, dsm.fullTableScanInvocations.get());
        } finally {
            engine.close();
        }
    }

    @Test
    public void snapshotReplayWithRebuildTrueIndex_doesNotFireRebuilder() throws Exception {
        // Snapshot replay path: the engine starts with a non-empty
        // WatermarkSnapshot containing an index with rebuild=true.
        // installSchemaFromSnapshot calls createVectorStoreIfNeeded
        // directly — it MUST NOT re-enter applyEntry, otherwise a
        // restart would re-fire the rebuild every single time the
        // engine boots, even after the original rebuild had already
        // populated the store and the watermark had advanced past
        // CREATE_INDEX.
        Table table = createTable();
        Index index = createIndexWithRebuild();

        CountingFakeDsm dsm = new CountingFakeDsm(REBUILD_LSN,
                Collections.singletonList(
                        RecordSerializer.makeRecord(table,
                                "pk", "key0", "vec", new float[]{1f, 2f, 3f})));

        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        MemoryMetadataStorageManager memMeta = new MemoryMetadataStorageManager();
        memMeta.start();
        memMeta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(memMeta);
        engine.setDataStorageManager(dsm);
        // Inject a watermark snapshot that already carries the table
        // and the rebuild=true index — simulates "the engine already
        // ran a successful rebuild before the previous shutdown, the
        // watermark snapshot was persisted, and we are now booting
        // back up against that snapshot".
        WatermarkSnapshot snapshot = new WatermarkSnapshot(
                new LogSequenceNumber(1, 100), 1, 0L,
                Arrays.asList(table),
                Arrays.asList(index));
        engine.setWatermarkStore(new InMemoryWatermarkStore(snapshot));
        try {
            engine.start();
            assertEquals("snapshot replay must NOT fire fullTableScan — "
                            + "createVectorStoreIfNeeded is the schema-only path",
                    0, dsm.fullTableScanInvocations.get());
            // Sanity: the vector store was created (just not back-filled).
            assertNotNull("snapshot replay must create the vector store",
                    engine.search("default", "vectable", "vidx",
                            new float[]{1f, 2f, 3f}, 1));
        } finally {
            engine.close();
        }
    }

    /**
     * Minimal {@link MemoryDataStorageManager} subclass that counts
     * calls to {@link #fullTableScan(String, String, LogSequenceNumber,
     * FullTableScanConsumer)} and synthesises a single-page response
     * carrying the staged records.
     */
    private static final class CountingFakeDsm extends MemoryDataStorageManager {
        private final LogSequenceNumber expectedLsn;
        private final List<Record> records;
        final AtomicInteger fullTableScanInvocations = new AtomicInteger(0);

        CountingFakeDsm(LogSequenceNumber expectedLsn, List<Record> records) {
            this.expectedLsn = expectedLsn;
            this.records = records;
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

    /**
     * In-memory {@link WatermarkStore} preloaded with a single
     * snapshot, used to simulate engine startup against a persisted
     * watermark.
     */
    private static final class InMemoryWatermarkStore implements WatermarkStore {
        private WatermarkSnapshot snapshot;

        InMemoryWatermarkStore(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public WatermarkSnapshot load() {
            return snapshot;
        }

        @Override
        public void save(WatermarkSnapshot snapshot) {
            this.snapshot = snapshot;
        }
    }
}
