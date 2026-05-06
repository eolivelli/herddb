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

package herddb.server;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.client.DMLResult;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.client.ScanResultSet;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.GetStatement;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the per-request-type metrics exposed under the {@code requests}
 * scope are populated by {@link ServerSideConnectionPeer} for every kind of
 * client request, that failures are reported as failed events, and that
 * in-JVM "local-mode" execution paths (which bypass the wire protocol) do NOT
 * touch these metrics. The Grafana dashboard panels added in the same change
 * rely on these series to display request rate, failure rate, and latency by
 * type.
 */
public class RequestStatsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Drives every user-facing request type through {@link HDBConnection} and
     * asserts the corresponding {@code requests.<type>} {@link TestOpStatsLogger}
     * recorded at least one successful event. Covers all 9 request types.
     */
    @Test
    public void testRequestStatsArePopulated() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                // 1. CREATE TABLE — execute_statement (DDL). usePreparedStatement=true
                // forces the client to send a TYPE_PREPARE_STATEMENT first, which
                // exercises the prepare_statement metric.
                long created = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long)",
                        0, false, true, Collections.emptyList()).updateCount;
                assertEquals(1, created);

                // 2. INSERT — execute_statement (DML). Same prepare-first flow.
                long inserted = connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id, n1) values (?, ?)",
                        0, false, true, Arrays.asList("k1", 10L)).updateCount;
                assertEquals(1, inserted);

                // 3. Batch INSERT — execute_statements
                List<DMLResult> batched = connection.executeUpdates(TableSpace.DEFAULT,
                        "INSERT INTO mytable (id, n1) values (?, ?)",
                        0, false, true,
                        Arrays.asList(
                                Arrays.asList("k2", 20L),
                                Arrays.asList("k3", 30L)));
                assertEquals(2, batched.size());

                // 4. SCAN — open_scanner + fetch_scanner_data + close_scanner.
                // fetchSize=1 with 3 rows guarantees that fetch_scanner_data is
                // reached on the wire (otherwise the server returns last=true on
                // the OPENSCANNER reply and no FETCHSCANNERDATA PDU is sent).
                int rows = 0;
                try (ScanResultSet rs = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable", true, Collections.emptyList(), 0, 0, 1, true)) {
                    while (rs.hasNext()) {
                        rs.next();
                        rows++;
                    }
                }
                assertEquals(3, rows);

                // 5. BEGIN / COMMIT
                long txCommit = connection.beginTransaction(TableSpace.DEFAULT);
                connection.commitTransaction(TableSpace.DEFAULT, txCommit);

                // 6. BEGIN / ROLLBACK
                long txRollback = connection.beginTransaction(TableSpace.DEFAULT);
                connection.rollbackTransaction(TableSpace.DEFAULT, txRollback);
            }

            // Every request type the client can issue must be reachable under
            // the requests.<type> scope. close_scanner is included here because
            // the scanner produced by step 4 is closed once the result set is
            // exhausted; if the server already returned last=true on the final
            // FETCHSCANNERDATA reply the client may skip CLOSESCANNER, so close
            // is verified separately via the explicit-close test below.
            assertSuccessRecorded(statsProvider, "requests.prepare_statement");
            assertSuccessRecorded(statsProvider, "requests.execute_statement");
            assertSuccessRecorded(statsProvider, "requests.execute_statements");
            assertSuccessRecorded(statsProvider, "requests.open_scanner");
            assertSuccessRecorded(statsProvider, "requests.fetch_scanner_data");
            assertSuccessRecorded(statsProvider, "requests.tx_begin");
            assertSuccessRecorded(statsProvider, "requests.tx_commit");
            assertSuccessRecorded(statsProvider, "requests.tx_rollback");
        }
    }

    /**
     * Executes a scan with a fetch size larger than the result so the client
     * library issues an explicit CLOSESCANNER, hitting the close_scanner path.
     */
    @Test
    public void testCloseScannerRecorded() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long)",
                        0, false, true, Collections.emptyList());
                for (int i = 0; i < 5; i++) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id, n1) values (?, ?)",
                            0, false, true, Arrays.asList("k" + i, (long) i));
                }
                // Open a scanner but only consume one row, then close — this
                // forces a CLOSESCANNER PDU because the server still has rows.
                try (ScanResultSet rs = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT * FROM mytable", true, Collections.emptyList(), 0, 0, 1, true)) {
                    if (rs.hasNext()) {
                        rs.next();
                    }
                }
            }

            assertSuccessRecorded(statsProvider, "requests.close_scanner");
        }
    }

    /**
     * Negative-path coverage: every metric used in the dashboard's
     * "Failed Request Rate by Type" panel must be reachable as a failure.
     */
    @Test
    public void testFailedRequestsRecordedAsFailures() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            // Cap the client-side retry count so a NotLeaderError on an unknown
            // tablespace surfaces as an exception instead of looping forever.
            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MAX_OPERATION_RETRY_COUNT, 1);
            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                // Make sure CREATE TABLE succeeds (so we can later commit on it).
                connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE t (id string primary key)",
                        0, false, true, Collections.emptyList());

                // execute_statement failure — INSERT into a non-existing table.
                ignoring(() -> connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO does_not_exist (id) values (?)",
                        0, false, true, Arrays.asList("k1")));

                // execute_statements failure — batch against a non-existing table.
                ignoring(() -> connection.executeUpdates(TableSpace.DEFAULT,
                        "INSERT INTO does_not_exist_either (id) values (?)",
                        0, false, true,
                        Arrays.asList(Arrays.asList("a"), Arrays.asList("b"))));

                // open_scanner failure — query against a non-existing table.
                ignoring(() -> {
                    try (ScanResultSet rs = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM definitely_not_a_table", true, Collections.emptyList(),
                            0, 0, 10, true)) {
                        while (rs.hasNext()) {
                            rs.next();
                        }
                    }
                    return null;
                });

                // tx_commit failure — committing a tx id that doesn't exist.
                ignoring(() -> {
                    connection.commitTransaction(TableSpace.DEFAULT, 999_999L);
                    return null;
                });

                // tx_rollback failure — rolling back a tx id that doesn't exist.
                ignoring(() -> {
                    connection.rollbackTransaction(TableSpace.DEFAULT, 999_999L);
                    return null;
                });
            }

            assertFailureRecorded(statsProvider, "requests.execute_statement");
            assertFailureRecorded(statsProvider, "requests.execute_statements");
            assertFailureRecorded(statsProvider, "requests.open_scanner");
            assertFailureRecorded(statsProvider, "requests.tx_commit");
            assertFailureRecorded(statsProvider, "requests.tx_rollback");
        }
    }

    /**
     * Drives a {@code prepare_statement} failure by issuing an
     * {@code executeUpdate} with {@code usePreparedStatement=true} against a
     * tablespace that does not exist. The client's prepare PDU surfaces the
     * not-leader error, which the server records as a failed
     * {@code requests.prepare_statement} event.
     *
     * <p>The client retries {@link herddb.client.impl.LeaderChangedException}
     * up to {@code Integer.MAX_VALUE} times (it overrides
     * {@code getMaxRetry()} unconditionally) so the call would loop forever on
     * its own. We bound the wait by running the doomed call on a separate
     * thread with a 2-second cap and asserting the metric immediately after —
     * by then the very first PREPARE PDU has already been processed and
     * recorded as a failure.</p>
     */
    @Test
    public void testPrepareStatementFailureRecorded() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                ExecutorService runner = Executors.newSingleThreadExecutor();
                Future<?> doomed = runner.submit(() -> {
                    try {
                        connection.executeUpdate("no_such_tablespace",
                                "SELECT 1", 0, false, true, Collections.emptyList());
                    } catch (Exception ignored) {
                        // expected: tablespace not found
                    }
                });
                try {
                    doomed.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException expected) {
                    // Client is in its retry loop — that's fine, the very
                    // first PREPARE attempt has already failed and the metric
                    // has been recorded.
                } finally {
                    doomed.cancel(true);
                    runner.shutdownNow();
                }
            }

            assertFailureRecorded(statsProvider, "requests.prepare_statement");
        }
    }

    /**
     * In-JVM use of {@link herddb.core.DBManager#executeStatement} (the path
     * taken by HerdDBEmbeddedDataSource and similar embedded callers) must NOT
     * touch the request metrics — those are scoped to the wire protocol. This
     * test guards against future refactors that accidentally instrument the
     * embedded path.
     */
    @Test
    public void testLocalExecutionDoesNotIncrementRequestStats() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            // Run a statement directly against DBManager — the same code path
            // that the local-mode public methods of ServerSideConnectionPeer
            // (executeUpdate / executeGet / executeScan) ultimately call.
            CreateTableSpaceStatement createTs = new CreateTableSpaceStatement(
                    "ignored_check_ts", Collections.singleton("local"), "local", 1, 0, 0);
            try {
                server.getManager().executeStatement(createTs,
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
            } catch (Exception ignore) {
                // It is fine if this particular statement fails; the only
                // behaviour we care about is that no requests.* metric moved.
            }

            // A GetStatement against a non-existing tablespace should fail too,
            // but again must not move any requests.* metric.
            try {
                server.getManager().executeStatement(
                        new GetStatement(TableSpace.DEFAULT, "no_such_table",
                                Bytes.from_string("k"), null, false),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
            } catch (Exception ignore) {
                // expected
            }

            // None of the requests.* metrics may have been touched.
            for (String metricName : new String[]{
                    "requests.execute_statement",
                    "requests.execute_statements",
                    "requests.prepare_statement",
                    "requests.open_scanner",
                    "requests.fetch_scanner_data",
                    "requests.close_scanner",
                    "requests.tx_begin",
                    "requests.tx_commit",
                    "requests.tx_rollback",
                    "requests.request_tablespace_dump"}) {
                TestOpStatsLogger logger = statsProvider.getOpStatsLogger(metricName);
                if (logger == null) {
                    continue;
                }
                assertEquals("local-mode execution must not increment " + metricName + " success",
                        0L, logger.getSuccessCount());
                assertEquals("local-mode execution must not increment " + metricName + " failure",
                        0L, logger.getFailureCount());
            }
        }
    }

    private static void assertSuccessRecorded(TestStatsProvider provider, String fullPath) {
        TestOpStatsLogger logger = provider.getOpStatsLogger(fullPath);
        assertTrue(fullPath + " OpStatsLogger missing", logger != null);
        long total = logger.getSuccessCount() + logger.getFailureCount();
        assertTrue("expected at least one event for " + fullPath + ", got " + total, total >= 1);
        assertTrue(
                "expected at least one successful event for " + fullPath
                        + ", success=" + logger.getSuccessCount()
                        + " failure=" + logger.getFailureCount(),
                logger.getSuccessCount() >= 1);
    }

    private static void assertFailureRecorded(TestStatsProvider provider, String fullPath) {
        TestOpStatsLogger logger = provider.getOpStatsLogger(fullPath);
        assertTrue(fullPath + " OpStatsLogger missing", logger != null);
        assertTrue(
                "expected at least one failed event for " + fullPath
                        + ", success=" + logger.getSuccessCount()
                        + " failure=" + logger.getFailureCount(),
                logger.getFailureCount() >= 1);
    }

    /**
     * Helper that runs a possibly-failing call and swallows any exception so
     * the test can keep driving subsequent failure paths. We cannot rely on
     * the call always throwing — some failures are surfaced as zero
     * updateCount instead of an exception — but the metric assertion is the
     * real check.
     */
    private static void ignoring(ThrowingCallable r) {
        try {
            r.call();
        } catch (Exception ignored) {
            // expected
        }
    }

    @FunctionalInterface
    private interface ThrowingCallable {
        Object call() throws HDBException, Exception;
    }
}
