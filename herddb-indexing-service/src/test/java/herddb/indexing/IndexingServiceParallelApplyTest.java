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
import herddb.codec.RecordSerializer;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests parallel DML entry processing in IndexingServiceEngine.
 */
public class IndexingServiceParallelApplyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Table createTable() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static MemoryMetadataStorageManager createTestMetadata() throws Exception {
        MemoryMetadataStorageManager m = new MemoryMetadataStorageManager();
        m.start();
        m.ensureDefaultTableSpace("local", "local", 0, 1);
        return m;
    }

    private IndexingServiceEngine createEngine(Path logDir, Path dataDir, int parallelism) throws Exception {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM, String.valueOf(parallelism));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        engine.start();
        return engine;
    }

    /**
     * Sets up table and vector index using the synchronous applyEntry (DDL path).
     */
    private void setupTableAndIndex(IndexingServiceEngine engine, Table table) {
        LogEntry createTable = LogEntryFactory.createTable(table, null);
        engine.applyEntry(new LogSequenceNumber(1, 1), createTable);

        Index index = Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
        LogEntry createIndex = LogEntryFactory.createIndex(index, null);
        engine.applyEntry(new LogSequenceNumber(1, 2), createIndex);
    }

    @Test
    public void testParallelInsertsDifferentKeys() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        IndexingServiceEngine engine = createEngine(logDir, dataDir, 4);
        try {
            Table table = createTable();
            setupTableAndIndex(engine, table);

            // Insert many records with different keys via async path
            int numRecords = 100;
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
            }

            // Drain all async work before checking results
            engine.awaitPendingWorkForTest();

            List<?> results = engine.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            assertEquals(numRecords, results.size());
        } finally {
            engine.close();
        }
    }

    @Test
    public void testSameKeyOrdering() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // Use a recording store to verify operation order
        List<String> operations = Collections.synchronizedList(new ArrayList<>());

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM, "4");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            return new InMemoryVectorStore(vectorColumnName) {
                @Override
                public void addVector(Bytes pk, float[] vector) {
                    operations.add("add:" + pk);
                    super.addVector(pk, vector);
                }

                @Override
                public void removeVector(Bytes pk) {
                    operations.add("remove:" + pk);
                    super.removeVector(pk);
                }
            };
        });

        try {
            engine.start();

            Table table = createTable();
            setupTableAndIndex(engine, table);

            // INSERT then UPDATE then DELETE for the same key via async path
            Record record1 = RecordSerializer.makeRecord(table,
                    "pk", "samekey",
                    "vec", new float[]{1.0f, 2.0f, 3.0f});
            Record record2 = RecordSerializer.makeRecord(table,
                    "pk", "samekey",
                    "vec", new float[]{4.0f, 5.0f, 6.0f});

            engine.applySingleEntryForTest(new LogSequenceNumber(1, 10),
                    LogEntryFactory.insert(table, record1.key, record1.value, null));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 11),
                    LogEntryFactory.update(table, record2.key, record2.value, null));
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 12),
                    LogEntryFactory.delete(table, record1.key, null));

            // Drain workers before checking
            engine.awaitPendingWorkForTest();

            // Verify order: add, remove, add, remove (insert, then update=remove+add, then delete=remove)
            assertEquals(4, operations.size());
            assertTrue(operations.get(0).startsWith("add:"));
            assertTrue(operations.get(1).startsWith("remove:"));
            assertTrue(operations.get(2).startsWith("add:"));
            assertTrue(operations.get(3).startsWith("remove:"));
        } finally {
            engine.close();
        }
    }

    @Test
    public void testDdlDrainsPendingDml() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM, "4");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());

        try {
            engine.start();

            Table table = createTable();
            setupTableAndIndex(engine, table);

            // Insert some records via async path
            for (int i = 0; i < 50; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
            }

            // DDL via applySingleEntryForTest drains pending DML before applying
            LogEntry dropTable = LogEntryFactory.dropTable("mytable", null);
            engine.applySingleEntryForTest(new LogSequenceNumber(1, 100), dropTable);

            // After DDL, all inserts were applied before the drop
            // The table is dropped, so search should return empty
            List<?> results = engine.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, 100);
            assertTrue(results.isEmpty());
        } finally {
            engine.close();
        }
    }

    @Test
    public void testParallelismWithSingleWorker() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // parallelism=1 should work correctly (degenerate case)
        IndexingServiceEngine engine = createEngine(logDir, dataDir, 1);
        try {
            Table table = createTable();
            setupTableAndIndex(engine, table);

            for (int i = 0; i < 20; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
            }

            engine.awaitPendingWorkForTest();

            List<?> results = engine.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, 20);
            assertEquals(20, results.size());
        } finally {
            engine.close();
        }
    }

    @Test
    public void testMultipleWorkersUsed() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Set<String> threadNames = ConcurrentHashMap.newKeySet();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM, "4");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        engine.setVectorStoreFactory((indexName, tableName, vectorColumnName, dataDirectory, indexProperties) -> {
            return new InMemoryVectorStore(vectorColumnName) {
                @Override
                public void addVector(Bytes pk, float[] vector) {
                    threadNames.add(Thread.currentThread().getName());
                    super.addVector(pk, vector);
                }
            };
        });

        try {
            engine.start();

            Table table = createTable();
            setupTableAndIndex(engine, table);

            // Insert many records with diverse keys via async path to hit different stripes
            for (int i = 0; i < 200; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "diversekey" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
            }

            engine.awaitPendingWorkForTest();

            // With 200 different keys and 4 workers, we should see multiple worker threads
            assertTrue("Expected multiple worker threads but got: " + threadNames,
                    threadNames.size() > 1);
            for (String name : threadNames) {
                assertTrue("Unexpected thread name: " + name,
                        name.startsWith("indexing-apply-worker-"));
            }
        } finally {
            engine.close();
        }
    }

    @Test
    public void testAutoDetectParallelism() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        // parallelism=0 means auto-detect
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        // Don't set PROPERTY_APPLY_PARALLELISM, use default (0 = auto)
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(createTestMetadata());
        try {
            engine.start();
            // Engine should start without errors with auto-detected parallelism
        } finally {
            engine.close();
        }
    }
}
