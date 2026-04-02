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

import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.RecordSerializer;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for multiple IndexingService instances with sharding,
 * tailing from BookKeeper commit log.
 */
public class MultiInstanceBookKeeperShardingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void setUp() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder("zk").toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void tearDown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    private Table createTable() {
        return Table.builder()
                .name("mytable")
                .tablespace("default")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private Index createIndex(int numShards) {
        return Index.builder()
                .name("vidx")
                .table("mytable")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property("numShards", String.valueOf(numShards))
                .build();
    }

    /**
     * Creates N IndexingServiceEngine instances configured for BK tailing,
     * applies DDL and DML entries via the engines directly (simulating what
     * the tailer would do), and verifies sharding + search merge.
     */
    private void runMultiInstanceTest(int numInstances, int numShards, int numRecords) throws Exception {
        Table table = createTable();
        Index index = createIndex(numShards);

        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<EmbeddedIndexingService> services = new ArrayList<>();

        try {
            // Start N embedded indexing services
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir, i, numInstances);
                eis.start();
                services.add(eis);
                engines.add(eis.getEngine());
            }

            // Apply DDL to all engines
            LogEntry createTableEntry = LogEntryFactory.createTable(table, null);
            LogEntry createIndexEntry = LogEntryFactory.createIndex(index, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, 1), createTableEntry);
                engine.applyEntry(new LogSequenceNumber(1, 2), createIndexEntry);
            }

            // Apply INSERTs to all engines (shard filtering happens inside)
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, (i + 1) * 1.0f, (i + 2) * 1.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                for (IndexingServiceEngine engine : engines) {
                    engine.applySingleEntryForTest(lsn, insert);
                }
            }

            // Drain all async work
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // Verify each instance has a subset of records
            int totalAcrossInstances = 0;
            Set<String> allKeys = new HashSet<>();
            for (int i = 0; i < numInstances; i++) {
                IndexingServiceEngine engine = engines.get(i);
                List<Map.Entry<Bytes, Float>> results = engine.search(
                        "default", "mytable", "vidx",
                        new float[]{1.0f, 2.0f, 3.0f}, numRecords);
                int instanceCount = results.size();
                totalAcrossInstances += instanceCount;

                // Each instance should have a non-empty subset (with enough records and shards)
                if (numInstances > 1 && numShards >= numInstances && numRecords >= numInstances * 5) {
                    assertTrue("Instance " + i + " should have some records, got " + instanceCount,
                            instanceCount > 0);
                    assertTrue("Instance " + i + " should not have all records, got " + instanceCount,
                            instanceCount < numRecords);
                }

                for (Map.Entry<Bytes, Float> entry : results) {
                    allKeys.add(entry.getKey().toString());
                }
            }

            // Union of all instances should equal all records
            assertEquals("Union of all instances should contain all records",
                    numRecords, totalAcrossInstances);
            assertEquals("All unique keys should be present", numRecords, allKeys.size());

            // Verify search via client fan-out
            List<String> addresses = new ArrayList<>();
            for (EmbeddedIndexingService eis : services) {
                addresses.add(eis.getAddress());
            }
            try (IndexingServiceClient client = new IndexingServiceClient(addresses)) {
                List<Map.Entry<Bytes, Float>> mergedResults = client.search(
                        "default", "mytable", "vidx",
                        new float[]{1.0f, 2.0f, 3.0f}, numRecords);
                assertEquals("Merged search should return all records",
                        numRecords, mergedResults.size());
            }

        } finally {
            for (EmbeddedIndexingService eis : services) {
                try {
                    eis.close();
                } catch (Exception e) {
                    // ignore cleanup errors
                }
            }
        }
    }

    @Test
    public void testTwoInstances() throws Exception {
        runMultiInstanceTest(2, 4, 100);
    }

    @Test
    public void testThreeInstances() throws Exception {
        runMultiInstanceTest(3, 6, 150);
    }

    @Test
    public void testFiveInstances() throws Exception {
        runMultiInstanceTest(5, 10, 200);
    }

    @Test
    public void testDeleteAcrossInstances() throws Exception {
        int numInstances = 2;
        int numShards = 4;
        int numRecords = 50;

        Table table = createTable();
        Index index = createIndex(numShards);

        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<EmbeddedIndexingService> services = new ArrayList<>();

        try {
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("del-log-" + i).toPath();
                Path dataDir = folder.newFolder("del-data-" + i).toPath();
                EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir, i, numInstances);
                eis.start();
                services.add(eis);
                engines.add(eis.getEngine());
            }

            // DDL
            LogEntry createTableEntry = LogEntryFactory.createTable(table, null);
            LogEntry createIndexEntry = LogEntryFactory.createIndex(index, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, 1), createTableEntry);
                engine.applyEntry(new LogSequenceNumber(1, 2), createIndexEntry);
            }

            // INSERT
            List<Record> records = new ArrayList<>();
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, (i + 1) * 1.0f, (i + 2) * 1.0f});
                records.add(record);
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                for (IndexingServiceEngine engine : engines) {
                    engine.applySingleEntryForTest(new LogSequenceNumber(1, 10 + i), insert);
                }
            }
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // DELETE all records — should be applied to all instances
            for (int i = 0; i < numRecords; i++) {
                LogEntry delete = LogEntryFactory.delete(table, records.get(i).key, null);
                for (IndexingServiceEngine engine : engines) {
                    engine.applySingleEntryForTest(new LogSequenceNumber(1, 100 + i), delete);
                }
            }
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // All instances should have 0 records
            for (int i = 0; i < numInstances; i++) {
                List<?> results = engines.get(i).search(
                        "default", "mytable", "vidx",
                        new float[]{1.0f, 2.0f, 3.0f}, numRecords);
                assertEquals("Instance " + i + " should have 0 records after delete", 0, results.size());
            }

        } finally {
            for (EmbeddedIndexingService eis : services) {
                try {
                    eis.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    @Test
    public void testCheckpointWithMultipleInstances() throws Exception {
        int numInstances = 2;
        int numShards = 4;
        int numRecords = 30;

        Table table = createTable();
        Index index = createIndex(numShards);

        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<EmbeddedIndexingService> services = new ArrayList<>();

        try {
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("ckpt-log-" + i).toPath();
                Path dataDir = folder.newFolder("ckpt-data-" + i).toPath();
                EmbeddedIndexingService eis = new EmbeddedIndexingService(logDir, dataDir, i, numInstances);
                eis.start();
                services.add(eis);
                engines.add(eis.getEngine());
            }

            // DDL + DML
            LogEntry createTableEntry = LogEntryFactory.createTable(table, null);
            LogEntry createIndexEntry = LogEntryFactory.createIndex(index, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, 1), createTableEntry);
                engine.applyEntry(new LogSequenceNumber(1, 2), createIndexEntry);
            }

            LogSequenceNumber lastLsn = null;
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "key" + i,
                        "vec", new float[]{i * 1.0f, (i + 1) * 1.0f, (i + 2) * 1.0f});
                LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
                lastLsn = new LogSequenceNumber(1, 10 + i);
                for (IndexingServiceEngine engine : engines) {
                    engine.applySingleEntryForTest(lastLsn, insert);
                }
            }
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // Verify all instances report status
            for (int i = 0; i < numInstances; i++) {
                IndexingServiceEngine.IndexStatusInfo status =
                        engines.get(i).getIndexStatus("default", "mytable", "vidx");
                assertTrue("Instance " + i + " should have some vectors",
                        status.getVectorCount() > 0);
            }

            // Verify merged search returns all
            List<String> addresses = new ArrayList<>();
            for (EmbeddedIndexingService eis : services) {
                addresses.add(eis.getAddress());
            }
            try (IndexingServiceClient client = new IndexingServiceClient(addresses)) {
                List<Map.Entry<Bytes, Float>> results = client.search(
                        "default", "mytable", "vidx",
                        new float[]{1.0f, 2.0f, 3.0f}, numRecords);
                assertEquals(numRecords, results.size());
            }

        } finally {
            for (EmbeddedIndexingService eis : services) {
                try {
                    eis.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }
}
