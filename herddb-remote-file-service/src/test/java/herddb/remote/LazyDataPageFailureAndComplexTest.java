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

package herddb.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for the lazy data-page load path covering failure
 * injection, multi-row scans, joins, transactions, and checkpoint compaction.
 *
 * <p>A single JVM is used per test; each test creates its own temporary
 * directory and remote file server so state is never shared across tests.
 */
public class LazyDataPageFailureAndComplexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Test client that counts range reads and optionally throws for "value"
     * reads — specifically the third and subsequent range reads per data-page
     * path. The first two reads for a given page are the fixed header and
     * the index section; everything after that is a value fetch triggered
     * by {@link LazyDataPage#get(herddb.utils.Bytes)}. Used to simulate
     * "remote storage can't serve value bytes" without breaking page-load
     * bootstrap.
     */
    private static class FailableCountingClient extends RemoteFileServiceClient {
        final AtomicLong readCalls = new AtomicLong();
        final AtomicLong readBytes = new AtomicLong();
        final AtomicBoolean failValueReads = new AtomicBoolean(false);
        /** When set, every read is failed, including header and index. */
        final AtomicBoolean failAllReads = new AtomicBoolean(false);
        /** Per-path read counter used to discriminate header/index from value reads. */
        final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>
                perPathReads = new java.util.concurrent.ConcurrentHashMap<>();

        FailableCountingClient(List<String> servers) {
            super(servers);
        }

        @Override
        public byte[] readFileRange(String path, long offset, int length, int blockSize) {
            readCalls.incrementAndGet();
            if (failAllReads.get()) {
                throw new RuntimeException("simulated remote read failure (all)");
            }
            int perPath = perPathReads
                    .computeIfAbsent(path, k -> new java.util.concurrent.atomic.AtomicInteger())
                    .incrementAndGet();
            // First two reads per path are header (perPath==1) and index (perPath==2);
            // anything after that is a value fetch.
            if (failValueReads.get() && path.contains("/data/") && perPath >= 3) {
                throw new RuntimeException("simulated remote read failure (value)");
            }
            byte[] bytes = super.readFileRange(path, offset, length, blockSize);
            if (bytes != null) {
                readBytes.addAndGet(bytes.length);
            }
            return bytes;
        }

        void reset() {
            readCalls.set(0);
            readBytes.set(0);
            failValueReads.set(false);
            failAllReads.set(false);
            perPathReads.clear();
        }
    }

    private static final String TS = "ts1";
    private static final String NODE = "localhost";

    /** Persistent state that must survive across DBManager reopens within a test. */
    private static class RemoteEnv implements AutoCloseable {
        final RemoteFileServer server;
        final Path dataPath;
        final Path logsPath;
        final Path metadataPath;
        final Path tmpPath;

        RemoteEnv(TemporaryFolder folder) throws Exception {
            this.server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
            this.server.start();
            this.dataPath = folder.newFolder("data").toPath();
            this.logsPath = folder.newFolder("logs").toPath();
            this.metadataPath = folder.newFolder("metadata").toPath();
            this.tmpPath = folder.newFolder("tmp").toPath();
        }

        @Override
        public void close() throws Exception {
            server.close();
        }
    }

    /** One DBManager + test client; short-lived — open/close around each phase. */
    private static class Embedded implements AutoCloseable {
        final FailableCountingClient client;
        final DBManager manager;

        Embedded(RemoteEnv env) throws Exception {
            this.client = new FailableCountingClient(Arrays.asList(
                    "localhost:" + env.server.getPort()));
            this.manager = new DBManager(NODE,
                    new FileMetadataStorageManager(env.metadataPath),
                    new RemoteFileDataStorageManager(env.dataPath, env.tmpPath, 1000, client,
                            new LazyValueCache(2L * 1024L * 1024L)),
                    new FileCommitLogManager(env.logsPath),
                    env.tmpPath, null);
            this.manager.start();
        }

        @Override
        public void close() throws Exception {
            manager.close();
            client.close();
        }
    }

    private static void createTablespace(DBManager manager, String ts) throws Exception {
        manager.executeStatement(
                new CreateTableSpaceStatement(ts, Collections.singleton(NODE), NODE, 1, 0, 0),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        assertTrue(manager.waitForTablespace(ts, 10000));
    }

    private static void createIntStringTable(DBManager manager, String ts, String name) throws Exception {
        Table table = Table.builder()
                .tablespace(ts)
                .name(name)
                .column("id", ColumnTypes.INTEGER)
                .column("n", ColumnTypes.STRING)
                .primaryKey("id")
                .build();
        manager.executeStatement(new CreateTableStatement(table),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
    }

    private static long beginTx(DBManager manager, String ts) throws Exception {
        herddb.model.TransactionResult res = (herddb.model.TransactionResult) manager.executeStatement(
                new BeginTransactionStatement(ts),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        return res.getTransactionId();
    }

    private static void commit(DBManager manager, String ts, long tx) throws Exception {
        manager.executeStatement(
                new CommitTransactionStatement(ts, tx),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
    }

    private static void rollback(DBManager manager, String ts, long tx) throws Exception {
        manager.executeStatement(
                new RollbackTransactionStatement(ts, tx),
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
    }

    // ------------------------------------------------------------------
    // 1) Failure injection tests
    // ------------------------------------------------------------------

    @Test
    public void selectFailsCleanlyWhenValueReadFails() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        // Phase 1: populate + checkpoint with a normal client.
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "t1");
            for (int i = 0; i < 20; i++) {
                RemoteTestUtils.executeUpdate(e.manager,
                        "INSERT INTO " + TS + ".t1(id, n) VALUES(?, ?)",
                        Arrays.asList(i, "name-" + i));
            }
            e.manager.checkpoint();
        }
        // Phase 2: reopen, inject value-read failures, attempt SELECT.
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            e.client.failValueReads.set(true);
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(7))) {
                ds.consume();
                fail("expected SELECT to fail when the remote value read fails");
            } catch (Exception expected) {
                // Surface contains our simulated failure somewhere in the cause chain.
                assertTrue("exception chain must contain simulated failure; got " + expected,
                        containsCauseMessage(expected, "simulated remote read failure"));
            }
        }
        }
    }

    // NOTE: a symmetrical "UPDATE fails cleanly when value read fails" test
    // is intentionally omitted — see issue #181. A remote value-fetch
    // failure raised from inside TableManager during an UPDATE leaves a
    // StampedLock held in TableSpaceManager, so DBManager.close() blocks
    // indefinitely on Activator join. The same lock leak exists with the
    // legacy eager readPage() path, so it is not a lazy-page regression;
    // once #181 is fixed, add the symmetrical test here.

    @Test
    public void retryAfterFailureSucceeds() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "t1");
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".t1(id, n) VALUES(1, 'foo')", Collections.emptyList());
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            e.client.failValueReads.set(true);
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(1))) {
                ds.consume();
                fail("expected failure");
            } catch (Exception ignored) {
                // expected
            }
            // Disable failure, retry the same query.
            e.client.failValueReads.set(false);
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(1))) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                assertEquals("foo", rows.get(0).get("n").toString());
            }
        }
        }
    }

    // ------------------------------------------------------------------
    // 2) Scan and join
    // ------------------------------------------------------------------

    @Test
    public void multiPageScanReturnsEveryRow() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        final int numRows = 500;
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            // Force several pages by using largeish values.
            Table table = Table.builder()
                    .tablespace(TS)
                    .name("t1")
                    .column("id", ColumnTypes.INTEGER)
                    .column("body", ColumnTypes.BYTEARRAY)
                    .primaryKey("id")
                    .build();
            e.manager.executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            for (int i = 0; i < numRows; i++) {
                byte[] body = new byte[2048];
                body[0] = (byte) (i & 0xff);
                body[1] = (byte) ((i >> 8) & 0xff);
                RemoteTestUtils.executeUpdate(e.manager,
                        "INSERT INTO " + TS + ".t1(id, body) VALUES(?, ?)",
                        Arrays.asList(i, body));
            }
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT id FROM " + TS + ".t1",
                    Collections.emptyList())) {
                List<DataAccessor> rows = ds.consume();
                Set<Integer> seen = new HashSet<>();
                for (DataAccessor r : rows) {
                    seen.add(((Number) r.get("id")).intValue());
                }
                assertEquals(numRows, seen.size());
                for (int i = 0; i < numRows; i++) {
                    assertTrue("missing row id=" + i, seen.contains(i));
                }
            }
        }
        }
    }

    @Test
    public void innerJoinReturnsExpectedRows() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "customer");
            Table order = Table.builder()
                    .tablespace(TS)
                    .name("orders")
                    .column("oid", ColumnTypes.INTEGER)
                    .column("cid", ColumnTypes.INTEGER)
                    .column("amount", ColumnTypes.INTEGER)
                    .primaryKey("oid")
                    .build();
            e.manager.executeStatement(new CreateTableStatement(order),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".customer(id, n) VALUES(?, ?)", Arrays.asList(1, "Alice"));
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".customer(id, n) VALUES(?, ?)", Arrays.asList(2, "Bob"));
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".orders(oid, cid, amount) VALUES(?, ?, ?)",
                    Arrays.asList(100, 1, 50));
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".orders(oid, cid, amount) VALUES(?, ?, ?)",
                    Arrays.asList(101, 1, 20));
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".orders(oid, cid, amount) VALUES(?, ?, ?)",
                    Arrays.asList(102, 2, 30));
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT c.n, o.amount FROM " + TS + ".customer c"
                            + " INNER JOIN " + TS + ".orders o ON c.id = o.cid"
                            + " ORDER BY o.oid",
                    Collections.emptyList())) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(3, rows.size());
                assertEquals("Alice", rows.get(0).get("n").toString());
                assertEquals(50, ((Number) rows.get(0).get("amount")).intValue());
                assertEquals("Alice", rows.get(1).get("n").toString());
                assertEquals(20, ((Number) rows.get(1).get("amount")).intValue());
                assertEquals("Bob", rows.get(2).get("n").toString());
                assertEquals(30, ((Number) rows.get(2).get("amount")).intValue());
            }
        }
        }
    }

    // ------------------------------------------------------------------
    // 3) Transactions
    // ------------------------------------------------------------------

    @Test
    public void updateAndSelectInSameTransactionSeeTheirOwnWrites() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "t1");
            for (int i = 0; i < 10; i++) {
                RemoteTestUtils.executeUpdate(e.manager,
                        "INSERT INTO " + TS + ".t1(id, n) VALUES(?, ?)",
                        Arrays.asList(i, "original-" + i));
            }
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            long tx = beginTx(e.manager, TS);
            TransactionContext txCtx = new TransactionContext(tx);
            // Update inside the transaction.
            RemoteTestUtils.executeUpdateWithContext(e.manager,
                    "UPDATE " + TS + ".t1 SET n = ? WHERE id = ?",
                    Arrays.asList("tx-3", 3),
                    txCtx);
            // Select inside the same transaction — must see own write.
            try (DataScanner ds = RemoteTestUtils.scanWithContext(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(3), txCtx)) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                assertEquals("tx-3", rows.get(0).get("n").toString());
            }
            commit(e.manager, TS, tx);

            // Select outside the transaction — must see the committed write.
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(3))) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                assertEquals("tx-3", rows.get(0).get("n").toString());
            }
        }
        }
    }

    @Test
    public void rollbackDiscardsChangesAgainstLazyPages() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "t1");
            for (int i = 0; i < 5; i++) {
                RemoteTestUtils.executeUpdate(e.manager,
                        "INSERT INTO " + TS + ".t1(id, n) VALUES(?, ?)",
                        Arrays.asList(i, "original-" + i));
            }
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            long tx = beginTx(e.manager, TS);
            TransactionContext txCtx = new TransactionContext(tx);
            RemoteTestUtils.executeUpdateWithContext(e.manager,
                    "UPDATE " + TS + ".t1 SET n = ? WHERE id = ?",
                    Arrays.asList("will-rollback", 2),
                    txCtx);
            rollback(e.manager, TS, tx);

            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(2))) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                assertEquals("original value must be restored after rollback",
                        "original-2", rows.get(0).get("n").toString());
            }
        }
        }
    }

    @Test
    public void txSelectFailsWhenValueReadFails() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            createIntStringTable(e.manager, TS, "t1");
            RemoteTestUtils.executeUpdate(e.manager,
                    "INSERT INTO " + TS + ".t1(id, n) VALUES(?, ?)",
                    Arrays.asList(1, "only-row"));
            e.manager.checkpoint();
        }
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            long tx = beginTx(e.manager, TS);
            TransactionContext txCtx = new TransactionContext(tx);
            e.client.failValueReads.set(true);
            try (DataScanner ds = RemoteTestUtils.scanWithContext(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(1),
                    txCtx)) {
                ds.consume();
                fail("expected value read failure inside transaction");
            } catch (Exception expected) {
                assertTrue("got " + expected,
                        containsCauseMessage(expected, "simulated remote read failure"));
            }
            // Reading should work again once the failure is cleared.
            e.client.failValueReads.set(false);
            try (DataScanner ds = RemoteTestUtils.scanWithContext(e.manager,
                    "SELECT n FROM " + TS + ".t1 WHERE id = ?",
                    Arrays.asList(1),
                    txCtx)) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                assertEquals("only-row", rows.get(0).get("n").toString());
            }
            rollback(e.manager, TS, tx);
        }
        }
    }

    // ------------------------------------------------------------------
    // 4) Checkpoint with compaction
    // ------------------------------------------------------------------

    @Test
    public void checkpointAfterDeletionsReadsLazyPagesForCompaction() throws Exception {
        try (RemoteEnv env = new RemoteEnv(folder)) {
        final int numRows = 200;
        try (Embedded e = new Embedded(env)) {
            createTablespace(e.manager, TS);
            // Build pages that are fat enough to make compaction meaningful.
            Table table = Table.builder()
                    .tablespace(TS)
                    .name("t1")
                    .column("id", ColumnTypes.INTEGER)
                    .column("payload", ColumnTypes.BYTEARRAY)
                    .primaryKey("id")
                    .build();
            e.manager.executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            byte[] filler = new byte[8 * 1024];
            for (int i = 0; i < numRows; i++) {
                filler[0] = (byte) (i & 0xff);
                RemoteTestUtils.executeUpdate(e.manager,
                        "INSERT INTO " + TS + ".t1(id, payload) VALUES(?, ?)",
                        Arrays.asList(i, filler.clone()));
            }
            e.manager.checkpoint();
        }
        // Phase 2: reopen, delete many rows to force compaction, checkpoint.
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            // Delete every row where id is odd (~half the rows).
            for (int i = 1; i < numRows; i += 2) {
                RemoteTestUtils.executeUpdate(e.manager,
                        "DELETE FROM " + TS + ".t1 WHERE id = ?",
                        Arrays.asList(i));
            }
            // Now checkpoint — this exercises the compaction path, which reads
            // old lazy pages via getRecordsForFlush() to rebuild merged pages.
            e.manager.checkpoint();

            // Survivors: every even id must still be present.
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT id FROM " + TS + ".t1",
                    Collections.emptyList())) {
                List<DataAccessor> rows = ds.consume();
                Set<Integer> ids = new HashSet<>();
                for (DataAccessor r : rows) {
                    ids.add(((Number) r.get("id")).intValue());
                }
                assertEquals(numRows / 2, ids.size());
                for (int i = 0; i < numRows; i += 2) {
                    assertTrue("survivor " + i + " missing", ids.contains(i));
                }
                for (int i = 1; i < numRows; i += 2) {
                    assertFalse("deleted " + i + " still present", ids.contains(i));
                }
            }
        }
        // Phase 3: final reopen, verify durability after compaction.
        try (Embedded e = new Embedded(env)) {
            assertTrue(e.manager.waitForBootOfLocalTablespaces(10000));
            try (DataScanner ds = RemoteTestUtils.scan(e.manager,
                    "SELECT COUNT(*) AS c FROM " + TS + ".t1",
                    Collections.emptyList())) {
                List<DataAccessor> rows = ds.consume();
                assertEquals(1, rows.size());
                // Count column name is implementation-defined; just fetch the first numeric column.
                Object count = rows.get(0).get("c");
                assertNotNull(count);
                assertEquals(numRows / 2, ((Number) count).intValue());
            }
        }
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static boolean containsCauseMessage(Throwable t, String needle) {
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && msg.contains(needle)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
