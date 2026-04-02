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
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for multiple IndexingService instances with sharded vector indexes.
 * Each instance gets a subset of the data based on key hashing, and the
 * {@link IndexingServiceClient} fans out searches to all instances and merges results.
 *
 * @author enrico.olivelli
 */
public class MultiInstanceFileShardingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final int VECTOR_DIM = 3;

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
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(numShards))
                .build();
    }

    /**
     * Applies DDL (CREATE TABLE + CREATE INDEX) and DML (INSERTs) to all engines.
     * Each engine's sharding logic determines which records it actually indexes.
     */
    private void applyEntries(List<IndexingServiceEngine> engines, Table table, Index index, int numRecords)
            throws Exception {
        // DDL: apply to all engines
        LogEntry createTable = LogEntryFactory.createTable(table, null);
        LogEntry createIndex = LogEntryFactory.createIndex(index, null);
        for (IndexingServiceEngine engine : engines) {
            engine.applyEntry(new LogSequenceNumber(1, 1), createTable);
            engine.applyEntry(new LogSequenceNumber(1, 2), createIndex);
        }

        // DML: apply inserts to all engines (sharding logic inside each engine filters)
        for (int i = 0; i < numRecords; i++) {
            float[] vec = new float[]{i * 1.0f, i * 2.0f, i * 3.0f};
            Record record = RecordSerializer.makeRecord(table,
                    "pk", "key" + i,
                    "vec", vec);
            LogEntry insert = LogEntryFactory.insert(table, record.key, record.value, null);
            LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
            for (IndexingServiceEngine engine : engines) {
                engine.applySingleEntryForTest(lsn, insert);
            }
        }

        // Wait for all async work to complete
        for (IndexingServiceEngine engine : engines) {
            engine.awaitPendingWorkForTest();
        }
    }

    private void runMultiInstanceTest(int numInstances, int numShards, int numRecords) throws Exception {
        List<EmbeddedIndexingService> services = new ArrayList<>();
        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<String> addresses = new ArrayList<>();

        try {
            // Start N instances, each with its own logDir and dataDir
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("log-" + i).toPath();
                Path dataDir = folder.newFolder("data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, i, numInstances);
                svc.start();
                services.add(svc);
                engines.add(svc.getEngine());
                addresses.add(svc.getAddress());
            }

            Table table = createTable();
            Index index = createIndex(numShards);

            applyEntries(engines, table, index, numRecords);

            // Create a client that fans out to all instances
            try (IndexingServiceClient client = new IndexingServiceClient(addresses)) {
                // Search with a high limit to get all records
                float[] queryVec = new float[]{1.0f, 2.0f, 3.0f};
                List<Map.Entry<Bytes, Float>> results = client.search(
                        "default", "mytable", "vidx", queryVec, numRecords);

                assertEquals("Fan-out search should find all " + numRecords + " records",
                        numRecords, results.size());

                // Verify all keys are present
                Set<String> foundKeys = new HashSet<>();
                for (Map.Entry<Bytes, Float> entry : results) {
                    foundKeys.add(entry.getKey().toString());
                }
                for (int i = 0; i < numRecords; i++) {
                    assertTrue("Missing key" + i, foundKeys.contains(
                            Bytes.from_string("key" + i).toString()));
                }

                // Verify each individual instance has a subset (not all records)
                for (int i = 0; i < numInstances; i++) {
                    IndexingServiceEngine engine = engines.get(i);
                    List<?> localResults = engine.search("default", "mytable", "vidx",
                            queryVec, numRecords);
                    assertTrue("Instance " + i + " should have some records but had " + localResults.size(),
                            localResults.size() > 0);
                    assertTrue("Instance " + i + " should have a subset (" + localResults.size()
                                    + ") not all " + numRecords + " records",
                            localResults.size() < numRecords);
                }
            }
        } finally {
            for (EmbeddedIndexingService svc : services) {
                try {
                    svc.close();
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
    public void testCheckpointWithMultipleInstances() throws Exception {
        int numInstances = 2;
        int numShards = 4;
        int numRecords = 80;

        List<EmbeddedIndexingService> services = new ArrayList<>();
        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<String> addresses = new ArrayList<>();

        try {
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("cp-log-" + i).toPath();
                Path dataDir = folder.newFolder("cp-data-" + i).toPath();
                EmbeddedIndexingService svc = new EmbeddedIndexingService(logDir, dataDir, i, numInstances);
                svc.start();
                services.add(svc);
                engines.add(svc.getEngine());
                addresses.add(svc.getAddress());
            }

            Table table = createTable();
            Index index = createIndex(numShards);

            applyEntries(engines, table, index, numRecords);

            try (IndexingServiceClient client = new IndexingServiceClient(addresses)) {
                // Verify all records are found via fan-out search
                float[] queryVec = new float[]{1.0f, 2.0f, 3.0f};
                List<Map.Entry<Bytes, Float>> results = client.search(
                        "default", "mytable", "vidx", queryVec, numRecords);
                assertEquals("Should find all " + numRecords + " records after catch-up",
                        numRecords, results.size());

                // Verify total across all instances equals numRecords
                int totalLocalRecords = 0;
                for (int i = 0; i < numInstances; i++) {
                    List<?> localResults = engines.get(i).search("default", "mytable", "vidx",
                            queryVec, numRecords);
                    totalLocalRecords += localResults.size();
                }
                assertEquals("Sum of local records across instances should equal total",
                        numRecords, totalLocalRecords);
            }
        } finally {
            for (EmbeddedIndexingService svc : services) {
                try {
                    svc.close();
                } catch (Exception e) {
                    // ignore cleanup errors
                }
            }
        }
    }
}
