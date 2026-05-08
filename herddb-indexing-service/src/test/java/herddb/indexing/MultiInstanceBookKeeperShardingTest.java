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
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(addresses, 30)) {
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
            try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(addresses, 30)) {
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

    /**
     * Issue #463: with 2 instances and {@code numShards=4}, every replica
     * sees every INSERT entry (so {@code tailer_inserts} matches the workload
     * size on each), but the shard filter rejects roughly half of them on
     * each instance — those rejections must show up in the new
     * {@code tailer_entries_shard_filtered} counter and the totals across
     * the cluster must add up to exactly the workload size (every INSERT was
     * rejected on exactly one of the two replicas). Drives the
     * non-transactional fast path (each INSERT applied directly via
     * {@code applySingleEntryForTest}) to keep this test isolated from the
     * transaction-buffer code path covered by
     * {@link #testTwoInstancesTransactionalInsertsBumpShardFilteredCounter}.
     */
    @Test
    public void testTwoInstancesNonTransactionalInsertsBumpShardFilteredCounter()
            throws Exception {
        int numInstances = 2;
        int numShards = 4;
        int numRecords = 100;

        Table table = createTable();
        Index index = createIndex(numShards);

        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<EmbeddedIndexingService> services = new ArrayList<>();

        try {
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("sf-nontx-log-" + i).toPath();
                Path dataDir = folder.newFolder("sf-nontx-data-" + i).toPath();
                EmbeddedIndexingService eis = new EmbeddedIndexingService(
                        logDir, dataDir, i, numInstances);
                eis.start();
                services.add(eis);
                engines.add(eis.getEngine());
            }

            // DDL on every replica.
            LogEntry createTableEntry = LogEntryFactory.createTable(table, null);
            LogEntry createIndexEntry = LogEntryFactory.createIndex(index, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, 1), createTableEntry);
                engine.applyEntry(new LogSequenceNumber(1, 2), createIndexEntry);
            }

            // 100 non-transactional INSERTs delivered to BOTH replicas via
            // processEntryForTest — same entrypoint the real tailer thread
            // uses. processEntry() runs classifyForMetrics() (bumps
            // tailer_inserts) AND dispatches to the apply pipeline (where
            // applyInsert() bumps tailer_entries_shard_filtered for entries
            // the shard filter rejects). Going through applySingleEntryForTest
            // would bypass classifyForMetrics and leave tailer_inserts at 0.
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "sf-nontx-" + i,
                        "vec", new float[]{i * 1.0f, (i + 1) * 1.0f, (i + 2) * 1.0f});
                LogEntry insert = LogEntryFactory.insert(table,
                        record.key, record.value, null);
                LogSequenceNumber lsn = new LogSequenceNumber(1, 10 + i);
                for (IndexingServiceEngine engine : engines) {
                    engine.processEntryForTest(lsn, insert);
                }
            }
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            long sf0 = engines.get(0).getTailerEntriesShardFiltered();
            long sf1 = engines.get(1).getTailerEntriesShardFiltered();
            assertEquals(
                    "every INSERT must have been shard-filtered on exactly one replica",
                    numRecords, sf0 + sf1);
            assertTrue("instance 0 must shard-filter at least one INSERT, got " + sf0,
                    sf0 > 0);
            assertTrue("instance 0 must accept at least one INSERT (sf0 < " + numRecords + "), got " + sf0,
                    sf0 < numRecords);
            assertTrue("instance 1 must shard-filter at least one INSERT, got " + sf1,
                    sf1 > 0);
            assertTrue("instance 1 must accept at least one INSERT (sf1 < " + numRecords + "), got " + sf1,
                    sf1 < numRecords);
            // tailer_inserts is bumped at classifyForMetrics time (intent) so
            // it should match the workload size on EVERY replica regardless
            // of the shard filter outcome.
            assertEquals("instance 0 must see every INSERT in tailer_inserts",
                    numRecords, engines.get(0).getTailerInserts());
            assertEquals("instance 1 must see every INSERT in tailer_inserts",
                    numRecords, engines.get(1).getTailerInserts());

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

    /**
     * Issue #463: VectorBench commits each batch of INSERTs in a single
     * transaction (BEGIN + many transactional INSERTs + COMMIT), which routes
     * through {@code transactionBuffer} → {@code applyBufferedEntries} →
     * {@code submitDmlAsync} → {@code applyEntry} → {@code applyInsert}. This
     * is a different code path from
     * {@link #testTwoInstancesNonTransactionalInsertsBumpShardFilteredCounter},
     * which exercises the non-tx fast path. We need to verify the shard
     * filter (and its new {@code tailer_entries_shard_filtered} counter)
     * still fire correctly when the entries arrive inside a transaction
     * envelope — which is what the VectorBench production path actually does.
     *
     * <p>Splits 100 INSERTs across two transactions of 50 each, with keys
     * deliberately chosen so each transaction contains a mix of shard-0/1/2/3
     * keys (sequential PKs do this naturally). Drives the full BEGIN → 50
     * INSERTs → COMMIT envelope through {@code processEntryForTest} on both
     * replicas, then asserts the same cluster-level invariants as the non-tx
     * test plus a search-fan-out check that proves the on-disk state is
     * actually disjoint (i.e., the actual filter ran, not just the metric).
     */
    @Test
    public void testTwoInstancesTransactionalInsertsBumpShardFilteredCounter()
            throws Exception {
        int numInstances = 2;
        int numShards = 4;
        int numRecords = 100;
        int batchSize = 50;
        long[] txIds = new long[]{42L, 43L};

        Table table = createTable();
        Index index = createIndex(numShards);

        List<IndexingServiceEngine> engines = new ArrayList<>();
        List<EmbeddedIndexingService> services = new ArrayList<>();

        try {
            for (int i = 0; i < numInstances; i++) {
                Path logDir = folder.newFolder("sf-tx-log-" + i).toPath();
                Path dataDir = folder.newFolder("sf-tx-data-" + i).toPath();
                EmbeddedIndexingService eis = new EmbeddedIndexingService(
                        logDir, dataDir, i, numInstances);
                eis.start();
                services.add(eis);
                engines.add(eis.getEngine());
            }

            // DDL on every replica. CREATE_TABLE / CREATE_INDEX flow through
            // applyEntry directly, not the transactional path — they're not
            // part of the transaction envelope we're testing.
            LogEntry createTableEntry = LogEntryFactory.createTable(table, null);
            LogEntry createIndexEntry = LogEntryFactory.createIndex(index, null);
            for (IndexingServiceEngine engine : engines) {
                engine.applyEntry(new LogSequenceNumber(1, 1), createTableEntry);
                engine.applyEntry(new LogSequenceNumber(1, 2), createIndexEntry);
            }

            // Drive 100 INSERTs across 2 transactions (50 each). Each INSERT
            // has txId == txIds[batch], wrapped in BEGINTRANSACTION/COMMIT.
            // Routed through processEntryForTest on BOTH replicas — same
            // entrypoint the real BookKeeper tailer uses, so the
            // transactionBuffer + applyBufferedEntries + submitDmlAsync path
            // is exercised end-to-end.
            long lsnOff = 10;
            Set<String> allKeys = new HashSet<>();
            for (int batch = 0; batch < numRecords / batchSize; batch++) {
                long txId = txIds[batch];
                LogEntry begin = LogEntryFactory.beginTransaction(txId);
                LogSequenceNumber beginLsn = new LogSequenceNumber(1, lsnOff++);
                for (IndexingServiceEngine engine : engines) {
                    engine.processEntryForTest(beginLsn, begin);
                }
                for (int j = 0; j < batchSize; j++) {
                    int globalIdx = batch * batchSize + j;
                    String pk = "sf-tx-" + globalIdx;
                    allKeys.add(pk);
                    Record record = RecordSerializer.makeRecord(table,
                            "pk", pk,
                            "vec", new float[]{globalIdx * 1.0f,
                                    (globalIdx + 1) * 1.0f, (globalIdx + 2) * 1.0f});
                    // Build a transactional INSERT directly — LogEntryFactory.insert
                    // takes a herddb.model.Transaction, but we only need the txId
                    // for the tailer's BEGIN/COMMIT fan-out, so we construct the
                    // LogEntry ourselves with a non-zero transactionId.
                    LogEntry insert = new LogEntry(System.currentTimeMillis(),
                            LogEntryType.INSERT, txId, table.tableId,
                            record.key, record.value);
                    LogSequenceNumber insertLsn = new LogSequenceNumber(1, lsnOff++);
                    for (IndexingServiceEngine engine : engines) {
                        engine.processEntryForTest(insertLsn, insert);
                    }
                }
                LogEntry commit = LogEntryFactory.commitTransaction(txId);
                LogSequenceNumber commitLsn = new LogSequenceNumber(1, lsnOff++);
                for (IndexingServiceEngine engine : engines) {
                    engine.processEntryForTest(commitLsn, commit);
                }
            }
            // Drain the async DML pipeline on both replicas — applyBufferedEntries
            // submits work to submitDmlAsync, which runs on the apply-worker pool.
            for (IndexingServiceEngine engine : engines) {
                engine.awaitPendingWorkForTest();
            }

            // Metric invariants — same shape as the non-tx test, proves the
            // shard filter fires from the transactional code path too.
            long sf0 = engines.get(0).getTailerEntriesShardFiltered();
            long sf1 = engines.get(1).getTailerEntriesShardFiltered();
            assertEquals(
                    "every transactional INSERT must have been shard-filtered "
                            + "on exactly one replica (got sf0=" + sf0 + ", sf1=" + sf1 + ")",
                    numRecords, sf0 + sf1);
            assertTrue("instance 0 must shard-filter at least one INSERT, got " + sf0,
                    sf0 > 0);
            assertTrue("instance 0 must accept at least one INSERT, got " + sf0,
                    sf0 < numRecords);
            assertTrue("instance 1 must shard-filter at least one INSERT, got " + sf1,
                    sf1 > 0);
            assertTrue("instance 1 must accept at least one INSERT, got " + sf1,
                    sf1 < numRecords);
            assertEquals("instance 0 must see every INSERT in tailer_inserts",
                    numRecords, engines.get(0).getTailerInserts());
            assertEquals("instance 1 must see every INSERT in tailer_inserts",
                    numRecords, engines.get(1).getTailerInserts());

            // Storage-level invariant: each replica's local index must
            // physically hold ONLY the keys it accepted (i.e., not the
            // shard-filtered ones), and the union across replicas must equal
            // every key we inserted. Proves the actual filter — not just the
            // metric — ran through the transaction-buffer path.
            Set<String> keys0 = new HashSet<>();
            for (Map.Entry<Bytes, Float> e : engines.get(0).search(
                    "default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords)) {
                keys0.add(new String(e.getKey().to_array(),
                        java.nio.charset.StandardCharsets.UTF_8));
            }
            Set<String> keys1 = new HashSet<>();
            for (Map.Entry<Bytes, Float> e : engines.get(1).search(
                    "default", "mytable", "vidx",
                    new float[]{1.0f, 2.0f, 3.0f}, numRecords)) {
                keys1.add(new String(e.getKey().to_array(),
                        java.nio.charset.StandardCharsets.UTF_8));
            }
            // |keys0| + |keys1| == numRecords ⇒ disjoint union (no key was
            // accepted by both replicas — which would mean the shard filter
            // failed open).
            assertEquals(
                    "no key may be present on both replicas (shard filter must "
                            + "send each key to exactly one owner)",
                    numRecords, keys0.size() + keys1.size());
            Set<String> union = new HashSet<>(keys0);
            union.addAll(keys1);
            assertEquals(
                    "union of per-replica keys must cover every inserted key",
                    allKeys, union);
            // Per-replica counts must match the negation of the shard-filter
            // counter on the same replica: locally accepted == numRecords - shardFiltered.
            assertEquals(
                    "instance 0 local key set size must equal numRecords - shardFiltered0",
                    numRecords - sf0, keys0.size());
            assertEquals(
                    "instance 1 local key set size must equal numRecords - shardFiltered1",
                    numRecords - sf1, keys1.size());

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
