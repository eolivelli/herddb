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

package herddb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.server.Server;
import herddb.server.StaticClientSideMetadataProvider;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic client testing
 *
 * @author enrico.olivelli
 */
public class BatchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBatch() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     PreparedStatement statementCreatePage = con.prepareStatement("CREATE TABLE mytable (n1 int primary key auto_increment, name string)");
                     PreparedStatement statement = con.prepareStatement("INSERT INTO mytable (name) values(?)")) {
                    statementCreatePage.addBatch();

                    // use executeBatch for DDL, like in MySQL import scripts
                    int[] resultsCreate = statementCreatePage.executeBatch();
                    assertEquals(1, resultsCreate[0]);

                    {
                        for (int i = 0; i < 100; i++) {
                            statement.setString(1, "v" + i);
                            statement.addBatch();
                        }
                        int[] results = statement.executeBatch();
                        for (int i = 0; i < 100; i++) {
                            assertEquals(1, results[i]);
                        }

                        try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                            int count = 0;
                            while (rs.next()) {
                                assertEquals("v" + count, rs.getString("name"));
                                assertEquals(count + 1, rs.getInt("n1"));
                                count++;
                            }
                            assertEquals(100, count);
                        }
                    }

                    int next_id;
                    try (ResultSet rs = statement.executeQuery("SELECT MAX(n1) FROM mytable")) {
                        assertTrue(rs.next());
                        next_id = rs.getInt(1) + 1;
                        assertEquals(101, next_id);
                    }

                    statement.executeUpdate("DELETE FROM mytable");
                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(0, rs.getInt(1));
                    }

                    // transactions
                    con.setAutoCommit(false);

                    {
                        for (int i = 0; i < 100; i++) {
                            statement.setString(1, "v" + i);
                            statement.addBatch();
                        }
                        int[] results = statement.executeBatch();
                        for (int i = 0; i < 100; i++) {
                            assertEquals(1, results[i]);
                        }

                        try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                            int count = 0;
                            while (rs.next()) {
                                assertEquals("v" + count, rs.getString("name"));
                                assertEquals(count + next_id, rs.getInt("n1"));
                                count++;
                            }
                            assertEquals(100, count);
                        }
                    }
                    con.commit();
                }
            }
        }

    }

    /**
     * Issue #251 regression: {@code executeBatchAsync} used to clear the
     * internal {@code batch} field inside an async {@code whenComplete}
     * callback. The callback ran on a netty thread <em>after</em>
     * {@code res.complete(results)} unblocked the caller's {@code .get()},
     * so a fast caller that called {@code addBatch(...)} immediately
     * afterwards could mutate {@code this.batch} <em>before</em> the
     * callback wiped it. The bench's ingestion worker hit this race and
     * silently dropped 9 rows out of a 100 M batch ingest.
     *
     * <p>The fix snapshots the batch on the calling thread <em>before</em>
     * submitting the request and clears {@code this.batch} synchronously,
     * mirroring {@link java.sql.PreparedStatement#executeBatch()}'s
     * {@code finally}-block clear. After this fix:
     * <ul>
     *   <li>{@code .get()}'s {@code int[]} result has length equal to the
     *       <em>snapshot</em> size, never inflated by a concurrent
     *       {@code addBatch}.</li>
     *   <li>A row added between {@code executeBatchAsync()} and
     *       {@code .get()} is preserved in {@code this.batch} for the
     *       <em>next</em> flush — never wiped by the previous async
     *       callback.</li>
     * </ul>
     */
    @Test
    public void executeBatchAsyncClearsBatchSynchronouslyOnCallerThread() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     PreparedStatement create = con.prepareStatement("CREATE TABLE mytable (n1 int primary key, val int)");
                     PreparedStatement stmt = con.prepareStatement("INSERT INTO mytable (n1, val) values(?, ?)")) {
                    create.executeUpdate();

                    PreparedStatementAsync async = stmt.unwrap(PreparedStatementAsync.class);
                    Field batchField = HerdDBPreparedStatement.class.getDeclaredField("batch");
                    batchField.setAccessible(true);
                    @SuppressWarnings("unchecked")
                    List<List<Object>> internalBatch = (List<List<Object>>) batchField.get(stmt);

                    // Stage row 1 and start the async send. With the fix,
                    // executeBatchAsync snapshots `this.batch` and clears it
                    // BEFORE returning — so the field is empty by the time we
                    // resume on this thread.
                    stmt.setInt(1, 1);
                    stmt.setInt(2, 100);
                    stmt.addBatch();
                    assertEquals("addBatch must populate the buffer", 1, internalBatch.size());

                    CompletableFuture<int[]> future = async.executeBatchAsync();
                    assertEquals("executeBatchAsync must clear `this.batch` synchronously "
                                    + "before returning to the caller (issue #251)",
                            0, internalBatch.size());

                    // Now race with the (still in-flight) async callback by
                    // adding row 2. With the bug, the callback's read of
                    // `batch.size()` would see 2 instead of 1, and its later
                    // `batch.clear()` would wipe row 2 — losing it from the
                    // next flush. With the fix, this addBatch lands in an
                    // empty `this.batch` and is preserved for the next call.
                    stmt.setInt(1, 2);
                    stmt.setInt(2, 200);
                    stmt.addBatch();

                    int[] results1 = future.get();
                    assertEquals("first flush sent exactly 1 row (the snapshot taken before "
                                    + "the racy addBatch); a length of 2 would mean the callback "
                                    + "read this.batch.size() AFTER the racy addBatch — the bug",
                            1, results1.length);
                    assertEquals(1, results1[0]);

                    // The racy addBatch must still be in the buffer — verify
                    // by flushing. With the bug, the previous callback's
                    // batch.clear() would have wiped it after our addBatch.
                    assertEquals("row added during the in-flight batch must survive — "
                                    + "the previous callback must not wipe it",
                            1, internalBatch.size());

                    int[] results2 = async.executeBatchAsync().get();
                    assertEquals(1, results2.length);
                    assertEquals(1, results2[0]);

                    try (ResultSet rs = stmt.executeQuery("SELECT n1 FROM mytable ORDER BY n1")) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1));
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1));
                        assertFalse("exactly 2 rows persisted — no rows lost to the race",
                                rs.next());
                    }
                }
            }
        }
    }

    /**
     * Issue #251 regression — stress-style guard against the
     * executeBatchAsync race. The bench's ingestion worker reuses a single
     * {@link PreparedStatement} across many sequential batches; with the
     * previous code, a tight loop that committed N batches could lose
     * occasional rows because the async callback's {@code batch.clear()}
     * raced with the worker's next {@code addBatch}. After the fix, all
     * rows must land in the table.
     */
    @Test
    public void executeBatchAsyncTightLoopLosesNoRows() throws Exception {
        final int batches = 500;
        final int rowsPerBatch = 10;
        final long expected = (long) batches * rowsPerBatch;

        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     PreparedStatement create = con.prepareStatement("CREATE TABLE mytable (n1 int primary key, val int)");
                     PreparedStatement stmt = con.prepareStatement("INSERT INTO mytable (n1, val) values(?, ?)")) {
                    create.executeUpdate();
                    con.setAutoCommit(false);

                    PreparedStatementAsync async = stmt.unwrap(PreparedStatementAsync.class);
                    AtomicLong nextId = new AtomicLong(1);
                    for (int b = 0; b < batches; b++) {
                        for (int r = 0; r < rowsPerBatch; r++) {
                            stmt.setInt(1, (int) nextId.getAndIncrement());
                            stmt.setInt(2, r);
                            stmt.addBatch();
                        }
                        int[] results = async.executeBatchAsync().get();
                        assertEquals(rowsPerBatch, results.length);
                        for (int n : results) {
                            // INSERT must always confirm 1 row per batch entry
                            assertEquals(1, n);
                        }
                        con.commit();
                    }

                    try (Statement s = con.createStatement();
                         ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals("all rows from " + batches + " batches must be persisted",
                                expected, rs.getLong(1));
                    }
                }
            }
        }
    }

    @Test
    public void testBatchAsync() throws Exception {
        try (Server server = new Server(TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath()))) {
            server.start();
            server.waitForStandaloneBoot();
            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()))) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                try (BasicHerdDBDataSource dataSource = new BasicHerdDBDataSource(client);
                     Connection con = dataSource.getConnection();
                     PreparedStatementAsync statementCreatePage = con.prepareStatement("CREATE TABLE mytable (n1 int primary key auto_increment, name string)")
                             .unwrap(PreparedStatementAsync.class);
                     PreparedStatementAsync statement = con.prepareStatement("INSERT INTO mytable (name) values(?)")
                             .unwrap(PreparedStatementAsync.class)) {
                    statementCreatePage.addBatch();

                    // use executeBatch for DDL, like in MySQL import scripts
                    int[] resultsCreate = statementCreatePage.executeBatchAsync().get();
                    assertEquals(1, resultsCreate[0]);

                    {
                        for (int i = 0; i < 100; i++) {
                            statement.setString(1, "v" + i);
                            statement.addBatch();
                        }
                        int[] results = statement.executeBatchAsync().get();
                        for (int i = 0; i < 100; i++) {
                            assertEquals(1, results[i]);
                        }

                        try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                            int count = 0;
                            while (rs.next()) {
                                assertEquals("v" + count, rs.getString("name"));
                                assertEquals(count + 1, rs.getInt("n1"));
                                count++;
                            }
                            assertEquals(100, count);
                        }
                    }

                    int next_id;
                    try (ResultSet rs = statement.executeQuery("SELECT MAX(n1) FROM mytable")) {
                        assertTrue(rs.next());
                        next_id = rs.getInt(1) + 1;
                        assertEquals(101, next_id);
                    }

                    statement.executeUpdate("DELETE FROM mytable");
                    try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM mytable")) {
                        assertTrue(rs.next());
                        assertEquals(0, rs.getInt(1));
                    }

                    // transactions
                    con.setAutoCommit(false);

                    {
                        for (int i = 0; i < 100; i++) {
                            statement.setString(1, "v" + i);
                            statement.addBatch();
                        }
                        int[] results = statement.executeBatchAsync().get();
                        for (int i = 0; i < 100; i++) {
                            assertEquals(1, results[i]);
                        }

                        try (ResultSet rs = statement.executeQuery("SELECT * FROM mytable ORDER BY n1")) {
                            int count = 0;
                            while (rs.next()) {
                                assertEquals("v" + count, rs.getString("name"));
                                assertEquals(count + next_id, rs.getInt("n1"));
                                count++;
                            }
                            assertEquals(100, count);
                        }
                    }
                    con.commit();
                }
            }
        }

    }

}
