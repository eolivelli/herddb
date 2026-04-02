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

import static org.junit.Assert.*;

import herddb.mem.MemoryMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.XXHash64Utils;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests shard assignment logic in IndexingServiceEngine.
 */
public class ShardAssignmentTest {

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

    private IndexingServiceEngine createEngine(Path logDir, Path dataDir,
            int instanceId, int numInstances) throws Exception {
        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, String.valueOf(instanceId));
        props.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, String.valueOf(numInstances));
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);
        IndexingServiceEngine engine = new IndexingServiceEngine(logDir, dataDir, config);
        MemoryMetadataStorageManager meta = new MemoryMetadataStorageManager();
        meta.start();
        meta.ensureDefaultTableSpace("local", "local", 0, 1);
        engine.setMetadataStorageManager(meta);
        engine.start();
        return engine;
    }

    private void setupTableAndIndex(IndexingServiceEngine engine, Table table, int numShards) {
        LogEntry createTable = LogEntryFactory.createTable(table, null);
        engine.applyEntry(new LogSequenceNumber(1, 1), createTable);

        Index index = Index.builder()
                .name("vidx")
                .table("mytable")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property("numShards", String.valueOf(numShards))
                .build();
        LogEntry createIndex = LogEntryFactory.createIndex(index, null);
        engine.applyEntry(new LogSequenceNumber(1, 2), createIndex);
    }

    @Test
    public void testShardToInstanceMapping() throws Exception {
        Path logDir0 = folder.newFolder("log0").toPath();
        Path dataDir0 = folder.newFolder("data0").toPath();
        Path logDir1 = folder.newFolder("log1").toPath();
        Path dataDir1 = folder.newFolder("data1").toPath();

        Table table = createTable();
        int numRecords = 100;
        int numShards = 4;

        IndexingServiceEngine engine0 = createEngine(logDir0, dataDir0, 0, 2);
        IndexingServiceEngine engine1 = createEngine(logDir1, dataDir1, 1, 2);
        try {
            setupTableAndIndex(engine0, table, numShards);
            setupTableAndIndex(engine1, table, numShards);

            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                engine0.applySingleEntryForTest(lsn, insert);
                engine1.applySingleEntryForTest(lsn, insert);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            List<?> results0 = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1 = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);

            // Union should cover all records, intersection should be empty
            Set<String> keys0 = new HashSet<>();
            for (Object r : results0) {
                keys0.add(r.toString());
            }
            Set<String> keys1 = new HashSet<>();
            for (Object r : results1) {
                keys1.add(r.toString());
            }

            // Both engines together should have all records
            assertEquals("Union of both engines should have all records",
                    numRecords, keys0.size() + keys1.size());

            // No key should appear in both engines
            Set<String> intersection = new HashSet<>(keys0);
            intersection.retainAll(keys1);
            assertTrue("No key should be in both engines, but found: " + intersection,
                    intersection.isEmpty());
        } finally {
            engine0.close();
            engine1.close();
        }
    }

    @Test
    public void testSingleInstanceAcceptsAll() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Table table = createTable();
        int numRecords = 50;

        IndexingServiceEngine engine = createEngine(logDir, dataDir, 0, 1);
        try {
            setupTableAndIndex(engine, table, 4);

            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
            }

            engine.awaitPendingWorkForTest();

            List<?> results = engine.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            assertEquals("Single instance should accept all records", numRecords, results.size());
        } finally {
            engine.close();
        }
    }

    @Test
    public void testSingleShardAcceptsAll() throws Exception {
        Path logDir0 = folder.newFolder("log0").toPath();
        Path dataDir0 = folder.newFolder("data0").toPath();
        Path logDir1 = folder.newFolder("log1").toPath();
        Path dataDir1 = folder.newFolder("data1").toPath();

        Table table = createTable();
        int numRecords = 50;

        IndexingServiceEngine engine0 = createEngine(logDir0, dataDir0, 0, 2);
        IndexingServiceEngine engine1 = createEngine(logDir1, dataDir1, 1, 2);
        try {
            setupTableAndIndex(engine0, table, 1);
            setupTableAndIndex(engine1, table, 1);

            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                engine0.applySingleEntryForTest(lsn, insert);
                engine1.applySingleEntryForTest(lsn, insert);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            List<?> results0 = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1 = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);

            assertEquals("With numShards=1 both engines should accept all records",
                    numRecords, results0.size());
            assertEquals("With numShards=1 both engines should accept all records",
                    numRecords, results1.size());
        } finally {
            engine0.close();
            engine1.close();
        }
    }

    @Test
    public void testHashDeterminism() {
        byte[] keyBytes = "deterministic-key".getBytes();
        long hash1 = XXHash64Utils.hash(keyBytes, 0, keyBytes.length);
        long hash2 = XXHash64Utils.hash(keyBytes, 0, keyBytes.length);
        assertEquals("Hashing the same bytes must produce the same result", hash1, hash2);
    }

    @Test
    public void testHashDistribution() {
        int numShards = 8;
        int numKeys = 10000;
        int[] counts = new int[numShards];
        Random random = new Random(42);

        for (int i = 0; i < numKeys; i++) {
            byte[] keyBytes = ("random-pk-" + random.nextLong()).getBytes();
            long hash = XXHash64Utils.hash(keyBytes, 0, keyBytes.length);
            int shardId = Math.floorMod((int) hash, numShards);
            counts[shardId]++;
        }

        double lowerBound = numKeys * 0.08;
        double upperBound = numKeys * 0.17;
        for (int s = 0; s < numShards; s++) {
            assertTrue("Shard " + s + " got " + counts[s] + " keys, expected between "
                            + (int) lowerBound + " and " + (int) upperBound,
                    counts[s] >= lowerBound && counts[s] <= upperBound);
        }
    }

    @Test
    public void testDeleteNotFiltered() throws Exception {
        Path logDir0 = folder.newFolder("log0").toPath();
        Path dataDir0 = folder.newFolder("data0").toPath();
        Path logDir1 = folder.newFolder("log1").toPath();
        Path dataDir1 = folder.newFolder("data1").toPath();

        Table table = createTable();
        int numRecords = 20;
        int numShards = 4;

        IndexingServiceEngine engine0 = createEngine(logDir0, dataDir0, 0, 2);
        IndexingServiceEngine engine1 = createEngine(logDir1, dataDir1, 1, 2);
        try {
            setupTableAndIndex(engine0, table, numShards);
            setupTableAndIndex(engine1, table, numShards);

            // Insert records into both engines
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                engine0.applySingleEntryForTest(lsn, insert);
                engine1.applySingleEntryForTest(lsn, insert);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            // Verify records were distributed
            List<?> results0Before = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1Before = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            int totalBefore = results0Before.size() + results1Before.size();
            assertEquals(numRecords, totalBefore);

            // Delete all records from both engines
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry delete = LogEntryFactory.delete(table, record.key, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 100 + i);
                engine0.applySingleEntryForTest(lsn, delete);
                engine1.applySingleEntryForTest(lsn, delete);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            // Both engines should have zero records after delete
            List<?> results0After = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1After = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);

            assertEquals("Engine 0 should have no records after delete", 0, results0After.size());
            assertEquals("Engine 1 should have no records after delete", 0, results1After.size());
        } finally {
            engine0.close();
            engine1.close();
        }
    }

    @Test
    public void testUpdateNotFiltered() throws Exception {
        Path logDir0 = folder.newFolder("log0").toPath();
        Path dataDir0 = folder.newFolder("data0").toPath();
        Path logDir1 = folder.newFolder("log1").toPath();
        Path dataDir1 = folder.newFolder("data1").toPath();

        Table table = createTable();
        int numRecords = 20;
        int numShards = 4;

        IndexingServiceEngine engine0 = createEngine(logDir0, dataDir0, 0, 2);
        IndexingServiceEngine engine1 = createEngine(logDir1, dataDir1, 1, 2);
        try {
            setupTableAndIndex(engine0, table, numShards);
            setupTableAndIndex(engine1, table, numShards);

            // Insert records into both engines
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, i * 2.0f, i * 3.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                engine0.applySingleEntryForTest(lsn, insert);
                engine1.applySingleEntryForTest(lsn, insert);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            // Verify initial distribution
            List<?> results0Before = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1Before = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            int totalBefore = results0Before.size() + results1Before.size();
            assertEquals(numRecords, totalBefore);

            // Update all records in both engines with new vector values
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 10.0f, i * 20.0f, i * 30.0f});
                LogEntry update = LogEntryFactory.update(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 100 + i);
                engine0.applySingleEntryForTest(lsn, update);
                engine1.applySingleEntryForTest(lsn, update);
            }

            engine0.awaitPendingWorkForTest();
            engine1.awaitPendingWorkForTest();

            // After update, updates are NOT filtered by shard, so the update (remove+add)
            // re-adds vectors on both engines. Each engine should now have all records.
            List<?> results0After = engine0.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);
            List<?> results1After = engine1.search("default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords);

            // Both engines should have all records after unfiltered updates
            assertEquals("Engine 0 should have all records after unfiltered update",
                    numRecords, results0After.size());
            assertEquals("Engine 1 should have all records after unfiltered update",
                    numRecords, results1After.size());
        } finally {
            engine0.close();
            engine1.close();
        }
    }
}
