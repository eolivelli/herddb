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
import herddb.client.ScanResultSet;
import herddb.model.TableSpace;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the per-request-type metrics exposed under the {@code requests}
 * scope are populated by {@link ServerSideConnectionPeer} for every kind of
 * client request. The Grafana dashboard panels added in the same change rely
 * on these series to display request rate and latency by type.
 */
public class RequestStatsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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

                // 1. CREATE TABLE — execute_statement (DDL)
                long created = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE mytable (id string primary key, n1 long)",
                        0, false, true, Collections.emptyList()).updateCount;
                assertEquals(1, created);

                // 2. INSERT — execute_statement (DML)
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

                // 4. SCAN — open_scanner + (potentially) fetch_scanner_data + close_scanner.
                // fetchSize=1 with 3 rows guarantees that fetch_scanner_data is reached.
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

            // Each handler must be reachable under the requests.<type> scope.
            assertSuccessRecorded(statsProvider, "requests.execute_statement");
            assertSuccessRecorded(statsProvider, "requests.execute_statements");
            assertSuccessRecorded(statsProvider, "requests.open_scanner");
            assertSuccessRecorded(statsProvider, "requests.tx_begin");
            assertSuccessRecorded(statsProvider, "requests.tx_commit");
            assertSuccessRecorded(statsProvider, "requests.tx_rollback");
        }
    }

    @Test
    public void testFailedRequestRecordedAsFailure() throws Exception {
        TestStatsProvider statsProvider = new TestStatsProvider();
        ServerConfiguration config = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(config, statsProvider)) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder().toPath()));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));

                // Send an INSERT that targets a non-existing table — the planner
                // will reject it and the handler must record a failed event.
                try {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO does_not_exist (id) values (?)",
                            0, false, true, Arrays.asList("k1"));
                    // Some failures are surfaced as zero updateCount instead of
                    // an exception; the metric assertion below is the real check.
                } catch (Exception expected) {
                    // expected: missing table
                }
            }

            TestOpStatsLogger logger = statsProvider.getOpStatsLogger("requests.execute_statement");
            // The logger MUST exist (the request reached the handler).
            assertTrue("execute_statement OpStatsLogger missing", logger != null);
            long total = logger.getSuccessCount() + logger.getFailureCount();
            assertTrue("expected at least one execute_statement event, got total=" + total, total >= 1);
            assertTrue(
                    "expected at least one failed execute_statement event, success="
                            + logger.getSuccessCount() + " failure=" + logger.getFailureCount(),
                    logger.getFailureCount() >= 1);
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
}
