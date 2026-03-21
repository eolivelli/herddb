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

package herddb.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import herddb.server.Server;
import herddb.server.ServerConfiguration;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test: HerdDB with MySQL protocol transport.
 * Uses the official MySQL Connector/J to connect.
 */
public class MySQLBasicTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ServerConfiguration newConfig(Path baseDir) {
        return new ServerConfiguration(baseDir)
                .set(ServerConfiguration.PROPERTY_PORT, 0)
                .set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE)
                .set(MySQLTransport.PROPERTY_MYSQL_ENABLED, true)
                .set(MySQLTransport.PROPERTY_MYSQL_PORT, 0)  // random port
                .set(MySQLTransport.PROPERTY_MYSQL_HOST, "localhost");
    }

    private int getMySQLPort(Server server) {
        // Find the MySQLTransport among custom transports
        // Use reflection since customTransports is private
        try {
            java.lang.reflect.Field field = Server.class.getDeclaredField("customTransports");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            java.util.List<herddb.server.CustomTransport> transports =
                    (java.util.List<herddb.server.CustomTransport>) field.get(server);
            for (herddb.server.CustomTransport t : transports) {
                if (t instanceof MySQLTransport) {
                    return ((MySQLTransport) t).getPort();
                }
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException("MySQLTransport not found");
    }

    private String jdbcUrl(int port) {
        return "jdbc:mysql://localhost:" + port + "/herd"
                + "?useSSL=false&allowPublicKeyRetrieval=true"
                + "&useServerPrepStmts=false&connectTimeout=5000"
                + "&defaultAuthenticationPlugin=com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin";
    }

    @Test
    public void testConnectionAndPing() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newConfig(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            int mysqlPort = getMySQLPort(server);
            try (Connection conn = DriverManager.getConnection(
                    jdbcUrl(mysqlPort), "sa", "hdb")) {
                assertTrue(conn.isValid(2));
            }
        }
    }

    @Test
    public void testCreateTableInsertSelectUpdateDelete() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newConfig(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            int mysqlPort = getMySQLPort(server);
            try (Connection conn = DriverManager.getConnection(
                    jdbcUrl(mysqlPort), "sa", "hdb");
                 Statement stmt = conn.createStatement()) {

                // CREATE TABLE
                stmt.executeUpdate("CREATE TABLE t1 (id int primary key, name string, val double)");

                // INSERT
                int inserted = stmt.executeUpdate("INSERT INTO t1 (id, name, val) VALUES (1, 'hello', 3.14)");
                assertEquals(1, inserted);

                inserted = stmt.executeUpdate("INSERT INTO t1 (id, name, val) VALUES (2, 'world', 2.72)");
                assertEquals(1, inserted);

                // SELECT
                try (ResultSet rs = stmt.executeQuery("SELECT id, name, val FROM t1 ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("hello", rs.getString("name"));
                    assertEquals(3.14, rs.getDouble("val"), 0.001);

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt("id"));
                    assertEquals("world", rs.getString("name"));
                    assertEquals(2.72, rs.getDouble("val"), 0.001);

                    assertFalse(rs.next());
                }

                // UPDATE
                int updated = stmt.executeUpdate("UPDATE t1 SET name='updated' WHERE id=1");
                assertEquals(1, updated);

                try (ResultSet rs = stmt.executeQuery("SELECT name FROM t1 WHERE id=1")) {
                    assertTrue(rs.next());
                    assertEquals("updated", rs.getString("name"));
                }

                // DELETE
                int deleted = stmt.executeUpdate("DELETE FROM t1 WHERE id=2");
                assertEquals(1, deleted);

                try (ResultSet rs = stmt.executeQuery("SELECT count(*) as cnt FROM t1")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("cnt"));
                }
            }
        }
    }

    @Test
    public void testTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newConfig(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            int mysqlPort = getMySQLPort(server);
            try (Connection conn = DriverManager.getConnection(
                    jdbcUrl(mysqlPort), "sa", "hdb");
                 Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE t2 (id int primary key, name string)");

                // BEGIN + INSERT + COMMIT
                stmt.execute("BEGIN");
                stmt.executeUpdate("INSERT INTO t2 (id, name) VALUES (1, 'tx1')");
                stmt.execute("COMMIT");

                try (ResultSet rs = stmt.executeQuery("SELECT name FROM t2 WHERE id=1")) {
                    assertTrue(rs.next());
                    assertEquals("tx1", rs.getString("name"));
                }

                // BEGIN + INSERT + ROLLBACK
                stmt.execute("BEGIN");
                stmt.executeUpdate("INSERT INTO t2 (id, name) VALUES (2, 'tx2')");
                stmt.execute("ROLLBACK");

                try (ResultSet rs = stmt.executeQuery("SELECT count(*) as cnt FROM t2")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("cnt"));
                }
            }
        }
    }

    @Test
    public void testSelectVariable() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newConfig(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            int mysqlPort = getMySQLPort(server);
            try (Connection conn = DriverManager.getConnection(
                    jdbcUrl(mysqlPort), "sa", "hdb");
                 Statement stmt = conn.createStatement()) {

                try (ResultSet rs = stmt.executeQuery("SELECT @@version_comment")) {
                    assertTrue(rs.next());
                    assertEquals("HerdDB", rs.getString(1));
                }
            }
        }
    }

    @Test
    public void testAuthenticationFailure() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        try (Server server = new Server(newConfig(baseDir))) {
            server.start();
            server.waitForStandaloneBoot();

            int mysqlPort = getMySQLPort(server);
            try {
                Connection conn = DriverManager.getConnection(
                        jdbcUrl(mysqlPort), "sa", "wrong_password");
                conn.close();
                // Should not reach here
                assertTrue("Expected authentication failure", false);
            } catch (java.sql.SQLException e) {
                // Expected — authentication failure
                assertTrue(e.getMessage().contains("denied") || e.getMessage().contains("Access")
                        || e.getMessage().contains("authentication") || e.getMessage().contains("refused"));
            }
        }
    }
}
