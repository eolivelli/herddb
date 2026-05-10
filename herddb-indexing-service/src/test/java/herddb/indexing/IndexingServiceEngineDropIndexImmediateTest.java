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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #509: unit tests for {@link IndexingServiceEngine#dropIndexImmediate}
 * — the eager server-triggered cleanup path that removes a vector store from
 * the IS's tracking maps and submits ZK + file cleanup without waiting for the
 * commit-log tailer to reach the matching {@code DROP_INDEX} log entry.
 *
 * <p>Tests:
 * <ol>
 *   <li>Removes the store from {@code vectorStores}, submits deletion, and
 *       dataStorageManager.dropIndex is called once (via
 *       awaitPendingDeletionsForTest).</li>
 *   <li>Idempotent: a second call for the same key is a harmless no-op (no
 *       extra dropIndex call).</li>
 *   <li>Tailer path after eager: a later {@code applyEntry(DROP_INDEX)} is a
 *       no-op because the store is already gone (no double deletion).</li>
 *   <li>Unknown key: calling {@code dropIndexImmediate} for an index that was
 *       never created is a harmless no-op.</li>
 * </ol>
 */
public class IndexingServiceEngineDropIndexImmediateTest {

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

    private static final int DIM = 4;

    private static Table buildTable(String name) {
        int h = name.hashCode() & 0x7FFFFFFF;
        return Table.builder()
                .name(name)
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .tableId(h == 0 ? 1 : h)
                .build();
    }

    private static Index buildVectorIndex(String name, String tableName) {
        return Index.builder()
                .name(name)
                .table(tableName)
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    /**
     * DSM wrapper that counts {@code dropIndex} calls (excluding the
     * {@code _tmp_pkset_*} internal BLink drops that happen during checkpoint).
     */
    private static final class CountingDsm extends ForwardingDataStorageManager {
        final AtomicInteger dropIndexCount = new AtomicInteger(0);
        final List<String[]> dropIndexCalls = new ArrayList<>();

        CountingDsm(DataStorageManager delegate) {
            super(delegate);
        }

        @Override
        public synchronized void dropIndex(String tableSpace, String name)
                throws DataStorageManagerException {
            if (!name.contains("_tmp_pkset_")) {
                dropIndexCount.incrementAndGet();
                dropIndexCalls.add(new String[]{tableSpace, name});
            }
            super.dropIndex(tableSpace, name);
        }
    }

    /** Close-tracking PersistentVectorStore subclass (same pattern as DropTableIndexCleanupTest). */
    private static final class CloseSpy extends PersistentVectorStore {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        CloseSpy(String indexName, String tableName, String tableSpaceUUID,
                 String vectorColumnName, Path tmpDirectory,
                 DataStorageManager dataStorageManager, MemoryManager memoryManager,
                 int m, int beamWidth, float neighborOverflow, float alpha,
                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                 long compactionIntervalMs, VectorSimilarityFunction similarityFunction,
                 long maxVectorMemoryBytes) {
            super(indexName, tableName, tableSpaceUUID, vectorColumnName,
                    tmpDirectory, dataStorageManager, memoryManager,
                    m, beamWidth, neighborOverflow, alpha, fusedPQ,
                    maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                    similarityFunction, maxVectorMemoryBytes);
        }

        @Override
        public void close() throws Exception {
            closed.set(true);
            super.close();
        }

        boolean wasClosed() {
            return closed.get();
        }
    }

    private IndexingServerConfiguration configuration() {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        return new IndexingServerConfiguration(props);
    }

    private MemoryMetadataStorageManager newMeta() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    /**
     * Normal eager-drop path: {@code dropIndexImmediate} removes the store,
     * triggers deletion, the store is closed and local data erased.
     */
    @Test
    public void dropIndexImmediateClosesStoreAndSubmitsDeletion() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        CountingDsm dsm = new CountingDsm(backing);
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        List<CloseSpy> spies = new ArrayList<>();
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());
        engine.setDataStorageManager(dsm);

        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            CloseSpy spy = new CloseSpy(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN, Long.MAX_VALUE);
            try {
                spy.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            synchronized (spies) {
                spies.add(spy);
            }
            return spy;
        });

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        Table table = buildTable("t1");
        Index ix = buildVectorIndex("vidx", "t1");

        try {
            engine.start();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            engine.awaitPendingWorkForTest();

            CloseSpy spy;
            synchronized (spies) {
                assertEquals("one vector store should have been created", 1, spies.size());
                spy = spies.get(0);
            }

            // Eager drop via RPC path (table + indexName computed by the gRPC handler).
            engine.dropIndexImmediate("t1", "vidx");
            engine.awaitPendingDeletionsForTest();

            // Store must have been closed.
            assertTrue("dropIndexImmediate must close the vector store", spy.wasClosed());

            // dataStorageManager.dropIndex must have been called exactly once.
            assertEquals("dropIndexImmediate must call dsm.dropIndex exactly once",
                    1, dsm.dropIndexCount.get());
            assertEquals("dsm.dropIndex tableSpace must be the engine's tablespace UUID",
                    engine.getTableSpaceUUID(), dsm.dropIndexCalls.get(0)[0]);
        } finally {
            engine.close();
        }
    }

    /**
     * Idempotency: calling {@code dropIndexImmediate} a second time for the same
     * (table, index) pair is a no-op — no additional DSM drops.
     */
    @Test
    public void dropIndexImmediateIsIdempotent() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        CountingDsm dsm = new CountingDsm(backing);
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());
        engine.setDataStorageManager(dsm);

        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        Table table = buildTable("t2");
        Index ix = buildVectorIndex("vidx2", "t2");

        try {
            engine.start();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            engine.awaitPendingWorkForTest();

            // First eager drop.
            engine.dropIndexImmediate("t2", "vidx2");
            engine.awaitPendingDeletionsForTest();
            int afterFirst = dsm.dropIndexCount.get();
            assertEquals("first dropIndexImmediate must call dsm.dropIndex once",
                    1, afterFirst);

            // Second eager drop — store is already gone.
            engine.dropIndexImmediate("t2", "vidx2");
            engine.awaitPendingDeletionsForTest();
            assertEquals("second dropIndexImmediate must be a no-op",
                    afterFirst, dsm.dropIndexCount.get());
        } finally {
            engine.close();
        }
    }

    /**
     * Tailer path after eager drop: when the commit-log tailer later processes
     * the {@code DROP_INDEX} log entry for an index that was already removed by
     * {@code dropIndexImmediate}, the tailer must be a no-op (no double deletion).
     */
    @Test
    public void tailerDropIndexAfterEagerDropIsNoOp() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        CountingDsm dsm = new CountingDsm(backing);
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());
        engine.setDataStorageManager(dsm);

        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName,
                                       dataDirectory, indexProperties) -> {
            PersistentVectorStore pvs = new PersistentVectorStore(
                    indexName, tableName, engine.getTableSpaceUUID(), vectorColumnName,
                    dataDirectory, dsm, mm,
                    16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                    Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);
            try {
                pvs.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return pvs;
        });

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        Table table = buildTable("t3");
        Index ix = buildVectorIndex("vidx3", "t3");

        try {
            engine.start();
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(ix, null));
            engine.awaitPendingWorkForTest();

            // Eager drop via the new RPC path.
            engine.dropIndexImmediate("t3", "vidx3");
            engine.awaitPendingDeletionsForTest();
            int afterEager = dsm.dropIndexCount.get();
            assertEquals("eager drop must call dsm.dropIndex once", 1, afterEager);

            // Now the tailer processes the DROP_INDEX log entry (simulating the
            // commit-log catching up to the server's DROP_INDEX entry).
            engine.applyEntry(new LogSequenceNumber(1, 999),
                    LogEntryFactory.dropIndex("vidx3", null));
            engine.awaitPendingWorkForTest();
            engine.awaitPendingDeletionsForTest();

            // The tailer must NOT have triggered another deletion.
            assertEquals("tailer DROP_INDEX after eager drop must be a no-op",
                    afterEager, dsm.dropIndexCount.get());
        } finally {
            engine.close();
        }
    }

    /**
     * Unknown-key path: calling {@code dropIndexImmediate} for an index that
     * was never created (or was already removed) is a harmless no-op.
     */
    @Test
    public void dropIndexImmediateForUnknownKeyIsNoOp() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        MemoryDataStorageManager backing = new MemoryDataStorageManager();
        CountingDsm dsm = new CountingDsm(backing);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, configuration());
        engine.setMetadataStorageManager(newMeta());
        engine.setDataStorageManager(dsm);

        engine.setWatermarkStore(new WatermarkStore() {
            @Override
            public WatermarkSnapshot load() {
                return WatermarkSnapshot.START_OF_TIME;
            }

            @Override
            public void save(WatermarkSnapshot snapshot) throws IOException {
            }
        });

        try {
            engine.start();

            // Call for an index that was never registered — must not throw.
            engine.dropIndexImmediate("nonexistent_table", "nonexistent_index");
            engine.awaitPendingDeletionsForTest();

            assertEquals("dropIndexImmediate for unknown key must not call dsm.dropIndex",
                    0, dsm.dropIndexCount.get());
        } finally {
            engine.close();
        }
    }
}
