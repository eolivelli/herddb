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

package herddb.mysql.protocol;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Integration tests for the MySQL protocol implementation using the real MySQL JDBC driver.
 */
public class MySQLProtocolTest {

    private MySQLServer server;
    private int port;

    private final MySQLCommandHandler handler = new MySQLCommandHandler() {
        @Override
        public boolean authenticate(String username, byte[] scrambledPassword, byte[] challenge, String database) {
            return true;
        }

        @Override
        public CompletableFuture<QueryResult> executeQuery(long connectionId, String sql) {
            String trimmed = sql.trim();
            String upper = trimmed.toUpperCase();

            if (trimmed.equalsIgnoreCase("SELECT 1")) {
                ResultSetResponse rs = new ResultSetResponse(
                        Collections.singletonList(
                                new ColumnDef("1", "", "", MySQLColumnType.MYSQL_TYPE_LONGLONG, 1)
                        ),
                        Collections.singletonList(new Object[]{"1"})
                );
                return CompletableFuture.completedFuture(rs);
            } else if (upper.startsWith("INSERT")) {
                return CompletableFuture.completedFuture(
                        new OkResult(1, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT));
            } else if (upper.startsWith("SET ")) {
                return CompletableFuture.completedFuture(
                        new OkResult(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT));
            } else if (upper.startsWith("SELECT")) {
                // Handle driver init queries like SELECT @@session.auto_increment_increment, etc.
                // Return a single-column single-row result with a reasonable default value
                String value = "";
                if (upper.contains("AUTO_INCREMENT_INCREMENT")) {
                    value = "1";
                } else if (upper.contains("TRANSACTION_READ_ONLY") || upper.contains("TX_READ_ONLY")) {
                    value = "0";
                } else if (upper.contains("TX_ISOLATION") || upper.contains("TRANSACTION_ISOLATION")) {
                    value = "REPEATABLE-READ";
                }
                ResultSetResponse rs = new ResultSetResponse(
                        Collections.singletonList(
                                new ColumnDef(trimmed, "", "", MySQLColumnType.MYSQL_TYPE_VAR_STRING, 255)
                        ),
                        Collections.singletonList(new Object[]{value})
                );
                return CompletableFuture.completedFuture(rs);
            } else if (upper.startsWith("SHOW ")) {
                // Return empty result set for SHOW commands
                ResultSetResponse rs = new ResultSetResponse(
                        Collections.singletonList(
                                new ColumnDef("Value", "", "", MySQLColumnType.MYSQL_TYPE_VAR_STRING, 255)
                        ),
                        Collections.emptyList()
                );
                return CompletableFuture.completedFuture(rs);
            } else {
                // Default: return OK for any unrecognized statement
                return CompletableFuture.completedFuture(
                        new OkResult(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT));
            }
        }

        @Override
        public void useDatabase(long connectionId, String database) {
            // no-op
        }

        @Override
        public void connectionClosed(long connectionId) {
            // no-op
        }
    };

    @Before
    public void setUp() throws Exception {
        server = new MySQLServer("localhost", 0, handler);
        server.start();
        port = server.getPort();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    private String jdbcUrl() {
        return "jdbc:mysql://localhost:" + port + "/test?useSSL=false&allowPublicKeyRetrieval=true&useServerPrepStmts=false";
    }

    @Test
    public void testConnectionSucceeds() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl(), "root", "")) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testSelect1() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl(), "root", "")) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    assertTrue(rs.next());
                    assertEquals("1", rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testInsertReturnsUpdateCount() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl(), "root", "")) {
            try (Statement stmt = conn.createStatement()) {
                int count = stmt.executeUpdate("INSERT INTO t VALUES (1)");
                assertEquals(1, count);
            }
        }
    }

    @Test
    public void testPing() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl(), "root", "")) {
            assertTrue(conn.isValid(1));
        }
    }

    @Test
    public void testConnectionClose() throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl(), "root", "");
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }
}
