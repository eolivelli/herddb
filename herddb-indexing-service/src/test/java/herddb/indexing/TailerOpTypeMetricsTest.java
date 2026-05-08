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
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;
import herddb.utils.XXHash64Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #459: per-operation-type tailer counters. Drives one entry of each
 * relevant {@code LogEntryType} through {@code processEntryForTest()} and
 * asserts the matching engine getter advanced by exactly 1, that the
 * accepted/skipped split lines up with the issue's contract, and that
 * {@code tailer_entries_processed = tailer_entries_accepted + tailer_entries_skipped}.
 *
 * <p>Counts are checked at the {@link IndexingServiceEngine} layer (where the
 * counters live); the gRPC + admin-CLI plumbing is exercised indirectly by
 * the existing diagnostics test.
 */
public class TailerOpTypeMetricsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private EmbeddedIndexingService service;

    @Before
    public void setUp() throws Exception {
        service = new EmbeddedIndexingService(
                folder.newFolder("log").toPath(),
                folder.newFolder("data").toPath());
        service.start();
    }

    @After
    public void tearDown() throws Exception {
        if (service != null) {
            service.close();
        }
    }

    private static Table testTable() {
        return Table.builder()
                .name("vectable")
                .tablespace("local")
                .column("pk", ColumnTypes.STRING)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("pk")
                .build();
    }

    private static Index testIndex(String table) {
        return Index.builder()
                .name("vidx")
                .table(table)
                .tablespace("local")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .build();
    }

    @Test
    public void freshEngineReportsAllZero() {
        IndexingServiceEngine e = service.getEngine();
        assertEquals(0L, e.getTailerEntriesProcessed());
        assertEquals(0L, e.getTailerEntriesAccepted());
        assertEquals(0L, e.getTailerEntriesSkipped());
        assertEquals(0L, e.getTailerEntriesShardFiltered());
        assertEquals(0L, e.getTailerInserts());
        assertEquals(0L, e.getTailerUpdates());
        assertEquals(0L, e.getTailerDeletes());
        assertEquals(0L, e.getTailerDdl());
        assertEquals(0L, e.getTailerBatchesProcessed());
    }

    /**
     * Drive one entry of every classifiable {@link herddb.log.LogEntryType}
     * through {@code processEntryForTest} and assert that:
     * <ul>
     *   <li>each per-op counter advanced by exactly 1,</li>
     *   <li>{@code accepted} = inserts + updates + deletes,</li>
     *   <li>{@code skipped} = ddl + every non-DML/non-DDL entry (NOOP,
     *       REBALANCE, BEGIN/COMMIT/ROLLBACKTRANSACTION),</li>
     *   <li>{@code accepted + skipped} matches the total processed
     *       (the issue's contract).</li>
     * </ul>
     */
    @Test
    public void perOpCountersAdvanceOnceForEachEntry() throws Exception {
        IndexingServiceEngine engine = service.getEngine();

        Table table = testTable();
        Index index = testIndex("vectable");
        long lsnOff = 1;

        // --- DDL: CREATE_TABLE, CREATE_INDEX, ALTER_TABLE, DROP_INDEX,
        //          TRUNCATE_TABLE, DROP_TABLE.
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.createTable(table, null));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.createIndex(index, null));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.alterTable(table, null));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.truncate(table, null));
        // DROP_INDEX is dispatched via the entry.value payload (index name).
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                new LogEntry(System.currentTimeMillis(),
                        herddb.log.LogEntryType.DROP_INDEX, 0L, 0,
                        null, Bytes.from_string("vidx")));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.dropTable(table, null));

        // --- DML: 3 INSERT, 2 UPDATE, 1 DELETE on a freshly seeded table.
        for (int i = 0; i < 3; i++) {
            engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                    LogEntryFactory.insert(table, Bytes.from_string("k" + i),
                            Bytes.from_string("v" + i), null));
        }
        for (int i = 0; i < 2; i++) {
            engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                    LogEntryFactory.update(table, Bytes.from_string("k" + i),
                            Bytes.from_string("v_new" + i), null));
        }
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.delete(table, Bytes.from_string("k0"), null));

        // --- Non-DML/non-DDL: NOOP, BEGIN/COMMIT/ROLLBACKTRANSACTION,
        // TABLE_CONSISTENCY_CHECK, INDEXING_SERVICE_REBALANCE. All counted
        // as "skipped" (no graph mutation at this entry's arrival). Including
        // the latter two explicitly so reordering the classifier's `default`
        // branch can't silently miss a real LogEntryType (review feedback
        // on PR #460).
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.noop());
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.beginTransaction(42L));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.commitTransaction(42L));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                LogEntryFactory.rollbackTransaction(43L));
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                new LogEntry(System.currentTimeMillis(),
                        herddb.log.LogEntryType.TABLE_CONSISTENCY_CHECK,
                        0L, table.tableId, null, null));
        // INDEXING_SERVICE_REBALANCE with a null payload is valid here:
        // handleRebalanceEntry() logs and returns when entry.value is null,
        // so the engine treats it as a no-op — but the classifier must still
        // count it as one skipped entry, which is what we want to assert.
        engine.processEntryForTest(new LogSequenceNumber(1, lsnOff++),
                new LogEntry(System.currentTimeMillis(),
                        herddb.log.LogEntryType.INDEXING_SERVICE_REBALANCE,
                        0L, 0, null, null));

        // --- Per-op assertions. The DML / DDL totals must be unaffected by
        // the non-DML/non-DDL entries above.
        assertEquals("inserts", 3L, engine.getTailerInserts());
        assertEquals("updates", 2L, engine.getTailerUpdates());
        assertEquals("deletes", 1L, engine.getTailerDeletes());
        assertEquals("ddl: CREATE_TABLE+CREATE_INDEX+ALTER_TABLE+TRUNCATE_TABLE+DROP_INDEX+DROP_TABLE",
                6L, engine.getTailerDdl());

        // accepted == sum of inserts/updates/deletes; skipped == ddl + 6
        // control entries (NOOP, BEGIN, COMMIT, ROLLBACK,
        // TABLE_CONSISTENCY_CHECK, INDEXING_SERVICE_REBALANCE).
        assertEquals("accepted", 6L, engine.getTailerEntriesAccepted());
        assertEquals("skipped: 6 DDL + NOOP + BEGIN + COMMIT + ROLLBACK + "
                + "TABLE_CONSISTENCY_CHECK + INDEXING_SERVICE_REBALANCE",
                12L, engine.getTailerEntriesSkipped());

        // The issue's primary contract: accepted + skipped == every entry
        // the tailer classified.
        long classified = engine.getTailerEntriesAccepted()
                + engine.getTailerEntriesSkipped();
        assertEquals("accepted + skipped must equal the number of entries classified",
                18L, classified);
    }

    /**
     * A second, independent INSERT on its own must bump only
     * {@code tailer_inserts} and {@code tailer_entries_accepted} — proves the
     * classifier doesn't double-count or leak across categories.
     */
    @Test
    public void singleInsertOnlyTouchesInsertAndAccepted() throws Exception {
        IndexingServiceEngine engine = service.getEngine();
        Table table = testTable();
        engine.processEntryForTest(new LogSequenceNumber(1, 1),
                LogEntryFactory.insert(table, Bytes.from_string("k"),
                        Bytes.from_string("v"), null));

        assertEquals(1L, engine.getTailerInserts());
        assertEquals(0L, engine.getTailerUpdates());
        assertEquals(0L, engine.getTailerDeletes());
        assertEquals(0L, engine.getTailerDdl());
        assertEquals(1L, engine.getTailerEntriesAccepted());
        assertEquals(0L, engine.getTailerEntriesSkipped());
    }

    /**
     * A single DDL entry on its own must bump only {@code tailer_ddl} and
     * {@code tailer_entries_skipped} — DDL is "skipped" in the issue's
     * terminology because it doesn't mutate the HNSW graph (it just updates
     * the in-engine schema tracker).
     */
    @Test
    public void singleDdlOnlyTouchesDdlAndSkipped() throws Exception {
        IndexingServiceEngine engine = service.getEngine();
        Table table = testTable();
        engine.processEntryForTest(new LogSequenceNumber(1, 1),
                LogEntryFactory.createTable(table, null));

        assertEquals(0L, engine.getTailerInserts());
        assertEquals(0L, engine.getTailerUpdates());
        assertEquals(0L, engine.getTailerDeletes());
        assertEquals(1L, engine.getTailerDdl());
        assertEquals(0L, engine.getTailerEntriesAccepted());
        assertEquals(1L, engine.getTailerEntriesSkipped());
    }

    /**
     * Issue #463: an INSERT on a table with no vector index must NOT bump
     * {@code tailer_entries_shard_filtered}. The early return inside
     * {@code applyInsert} (before the per-index shard-filter loop) signals a
     * different reason than shard-filter rejection — operators distinguish
     * "this replica chose not to index because it doesn't own the shard"
     * (counted) from "the table is non-vector" (not counted).
     */
    @Test
    public void singleInsertOnTableWithNoVectorIndexDoesNotBumpShardFiltered()
            throws Exception {
        IndexingServiceEngine engine = service.getEngine();
        Table table = testTable();

        // Register the table so applyInsert resolves the schema, but do NOT
        // create a vector index for it. The applyInsert() body will hit the
        // `vectorIndexes.isEmpty()` early return.
        engine.processEntryForTest(new LogSequenceNumber(1, 1),
                LogEntryFactory.createTable(table, null));
        engine.processEntryForTest(new LogSequenceNumber(1, 2),
                LogEntryFactory.insert(table, Bytes.from_string("k"),
                        Bytes.from_string("v"), null));
        engine.awaitPendingWorkForTest();

        assertEquals("INSERT must still bump tailer_inserts",
                1L, engine.getTailerInserts());
        assertEquals("INSERT on table without vector index must NOT count "
                        + "as shard-filtered",
                0L, engine.getTailerEntriesShardFiltered());
    }

    /**
     * Issue #467 item 2: a table with TWO vector indexes that have
     * <em>different</em> {@code numShards} values (2 and 3) can produce
     * per-key split decisions — a key whose hash puts it on an odd shard
     * for {@code numShards=2} but an even shard for {@code numShards=3}
     * will be <em>accepted by the second index</em> even though the first
     * rejected it. The documented contract is:
     * <ul>
     *   <li>{@code tailer_entries_shard_filtered} is incremented only when
     *       <em>every</em> applicable vector index rejected the key.</li>
     *   <li>Keys accepted by at least one index are NOT counted.</li>
     * </ul>
     * The test sets up instance 0 of 2 with one table + two indexes
     * ({@code numShards=2} and {@code numShards=3}) then drives 120 INSERTs
     * (a multiple of lcm(2,3)=6, so hash buckets are well-represented).
     * Before driving the INSERTs it pre-classifies each key using the same
     * {@link XXHash64Utils#hash} + {@code Math.floorMod} formula used by
     * production, counts how many are rejected by <em>both</em> indexes,
     * and asserts the engine counter matches. It additionally asserts that
     * some keys triggered the "mixed acceptance" path (one index accepts,
     * one rejects) — proving the test is not vacuous.
     */
    @Test
    public void mixedAcceptanceInsertDoesNotBumpShardFiltered() throws Exception {
        int numRecords = 120; // multiple of lcm(2, 3) = 6
        int numShards2 = 2;
        int numShards3 = 3;

        // Instance 0 of 2 — shard filter fires for non-trivial numShards.
        EmbeddedIndexingService multiService = new EmbeddedIndexingService(
                folder.newFolder("ma-log").toPath(),
                folder.newFolder("ma-data").toPath(),
                0, 2);
        multiService.start();
        try {
            IndexingServiceEngine engine = multiService.getEngine();
            Table table = Table.builder()
                    .name("matbl")
                    .tablespace("local")
                    .column("pk", ColumnTypes.STRING)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .primaryKey("pk")
                    .build();

            // Two indexes with different numShards on the same table.
            Index idxA = Index.builder()
                    .name("vidxA")
                    .table("matbl")
                    .tablespace("local")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .property("numShards", String.valueOf(numShards2))
                    .build();
            Index idxB = Index.builder()
                    .name("vidxB")
                    .table("matbl")
                    .tablespace("local")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .property("numShards", String.valueOf(numShards3))
                    .build();

            // DDL via applyEntry so the schema tracker is seeded without
            // touching the per-op metric counters we are about to assert.
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(idxA, null));
            engine.applyEntry(new LogSequenceNumber(1, 3),
                    LogEntryFactory.createIndex(idxB, null));

            // Pre-classify every key using the same hash + floorMod formula
            // as IndexingServiceEngine.isAcceptedLocally. n=2, instanceId=0.
            int expectedShardFiltered = 0;
            int mixedAcceptanceCount = 0;
            for (int i = 0; i < numRecords; i++) {
                Bytes key = Bytes.from_string("ma-" + i);
                long hash = XXHash64Utils.hash(
                        key.getBuffer(), key.getOffset(), key.getLength());
                // idxA: numShards=2 → accepted if (hash%2)%2==0 → shard==0
                boolean acceptedByA = Math.floorMod((int) hash, numShards2) % 2 == 0;
                // idxB: numShards=3 → accepted if (hash%3)%2==0 → shard ∈ {0,2}
                boolean acceptedByB = Math.floorMod((int) hash, numShards3) % 2 == 0;
                if (!acceptedByA && !acceptedByB) {
                    expectedShardFiltered++;
                }
                if (acceptedByA != acceptedByB) {
                    mixedAcceptanceCount++;
                }
            }

            // Sanity-check the test is actually exercising mixed decisions
            // (otherwise the two-index setup adds nothing over a single-index
            // test and we would not be testing the "at-least-one-accepts"
            // branch at all).
            assertTrue("with numShards=2 and numShards=3 across 120 keys, at "
                    + "least one key must have divergent per-index decisions",
                    mixedAcceptanceCount > 0);
            // Similarly, at least one key must be rejected by both so the
            // counter is non-trivially tested.
            assertTrue("at least one key must be rejected by both indexes",
                    expectedShardFiltered > 0);

            // Drive all INSERTs through processEntryForTest (the path the
            // real tailer uses) so classifyForMetrics fires on each one.
            // The record value must be a properly serialised row (schema-aware
            // bytes), not a raw string — applyInsert feeds it through
            // RecordSerializer.accessRawDataFromValue to extract the vector.
            for (int i = 0; i < numRecords; i++) {
                Record record = RecordSerializer.makeRecord(table,
                        "pk", "ma-" + i,
                        "vec", new float[]{i * 1.0f, (i + 1) * 1.0f, (i + 2) * 1.0f});
                engine.processEntryForTest(new LogSequenceNumber(1, 10 + i),
                        LogEntryFactory.insert(table, record.key, record.value, null));
            }
            engine.awaitPendingWorkForTest();

            assertEquals("shard-filter counter must match the pre-classified "
                    + "both-reject count",
                    expectedShardFiltered, engine.getTailerEntriesShardFiltered());
            assertEquals("tailer_inserts must count every INSERT regardless of "
                    + "shard-filter outcome",
                    numRecords, engine.getTailerInserts());
        } finally {
            multiService.close();
        }
    }

    /**
     * Issue #467 item 5: UPDATE and DELETE entries on a sharded vector index
     * must NOT increment {@code tailer_entries_shard_filtered}. The shard
     * filter is INSERT-only: DELETE is always broadcast (so stale copies on
     * non-owners are removed), and UPDATE's remove is also broadcast while
     * only the add side is filtered — but neither bumps the counter. A
     * focused single-instance test locks this documented scope explicitly so a
     * future change that accidentally widens the filter to UPDATE or DELETE
     * would fail loudly here rather than at the broader multi-instance tests.
     */
    @Test
    public void updateAndDeleteOnShardedTableDoNotBumpShardFiltered() throws Exception {
        // Instance 0 of 2, numShards=4 — shard filter is active.
        EmbeddedIndexingService shardedService = new EmbeddedIndexingService(
                folder.newFolder("ud-log").toPath(),
                folder.newFolder("ud-data").toPath(),
                0, 2);
        shardedService.start();
        try {
            IndexingServiceEngine engine = shardedService.getEngine();
            Table table = Table.builder()
                    .name("udtbl")
                    .tablespace("local")
                    .column("pk", ColumnTypes.STRING)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .primaryKey("pk")
                    .build();
            Index index = Index.builder()
                    .name("udidx")
                    .table("udtbl")
                    .tablespace("local")
                    .type(Index.TYPE_VECTOR)
                    .column("vec", ColumnTypes.FLOATARRAY)
                    .property("numShards", "4")
                    .build();

            // DDL via applyEntry so counters start at zero before the DML.
            engine.applyEntry(new LogSequenceNumber(1, 1),
                    LogEntryFactory.createTable(table, null));
            engine.applyEntry(new LogSequenceNumber(1, 2),
                    LogEntryFactory.createIndex(index, null));

            // Build a properly serialised record so the async applyUpdate path
            // can decode the vector column without throwing "bad column type".
            Record record = RecordSerializer.makeRecord(table,
                    "pk", "ud-key",
                    "vec", new float[]{1.0f, 2.0f, 3.0f});

            // One UPDATE and one DELETE through the full tailer path.
            engine.processEntryForTest(new LogSequenceNumber(1, 3),
                    LogEntryFactory.update(table, record.key, record.value, null));
            engine.processEntryForTest(new LogSequenceNumber(1, 4),
                    LogEntryFactory.delete(table, record.key, null));
            engine.awaitPendingWorkForTest();

            assertEquals("tailer_updates must be 1", 1L, engine.getTailerUpdates());
            assertEquals("tailer_deletes must be 1", 1L, engine.getTailerDeletes());
            assertEquals("UPDATE and DELETE must NOT bump tailer_entries_shard_filtered",
                    0L, engine.getTailerEntriesShardFiltered());
            // tailer_inserts must remain zero — nothing was inserted.
            assertEquals("tailer_inserts must remain 0", 0L, engine.getTailerInserts());
        } finally {
            shardedService.close();
        }
    }
}
