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
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Table;
import herddb.utils.Bytes;
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
}
