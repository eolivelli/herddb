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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.ZKTestEnv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that both the legacy {@code jdbc:herddb:zookeeper:…} URL
 * (ZooKeeper-based discovery) and the new {@code jdbc:herddb:server:…} URL
 * (server-based discovery via system tables) can successfully connect to and
 * query a HerdDB cluster server backed by ZooKeeper + BookKeeper.
 *
 * <p>Both test methods share the same embedded ZK + BK + Server setup so the
 * only variable is the JDBC URL / discovery mechanism.
 */
public class JdbcDriverClusterDiscoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;
    private Server server;

    @Before
    public void setup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookie();

        ServerConfiguration serverConfig =
                TestUtils.newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverConfig.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverConfig.set(ServerConfiguration.PROPERTY_MODE,
                ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverConfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS,
                testEnv.getAddress());
        serverConfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverConfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT,
                testEnv.getTimeout());

        server = new Server(serverConfig);
        server.start();
        server.waitForStandaloneBoot();
    }

    @After
    public void teardown() throws Exception {
        try {
            if (server != null) {
                server.close();
            }
        } finally {
            if (testEnv != null) {
                testEnv.close();
            }
        }
    }

    /**
     * Legacy discovery path: {@code jdbc:herddb:zookeeper:…} — the client
     * connects to ZooKeeper to resolve the tablespace leader.
     */
    @Test
    public void connectViaZookeeperUrl() throws Exception {
        String url = "jdbc:herddb:zookeeper:" + testEnv.getAddress() + testEnv.getPath();
        assertSystemTablesAccessible(url);
    }

    /**
     * New server-based discovery path: {@code jdbc:herddb:server:…} — the
     * client connects to the server's configured endpoint and resolves the
     * leader by querying {@code sysnodes} / {@code systablespaces}.
     *
     * <p>In cluster mode {@link Server#getJdbcUrl()} returns a ZooKeeper URL;
     * the {@code server:} form must therefore be constructed from the server's
     * advertised host/port.  This is the URL style used by the Kubernetes
     * tools pod and is the primary motivation for issue #309.
     */
    @Test
    public void connectViaServerUrl() throws Exception {
        // Construct the server-based URL from the actual advertised address —
        // server.getJdbcUrl() returns a ZooKeeper URL in cluster mode.
        String host = server.getServerHostData().getHost();
        int port = server.getServerHostData().getPort();
        assertNotNull("Server host must not be null", host);
        assertTrue("Server port must be positive", port > 0);
        String url = "jdbc:herddb:server:" + host + ":" + port;
        assertSystemTablesAccessible(url);
    }

    // -------------------------------------------------------------------------
    // Shared assertion helper
    // -------------------------------------------------------------------------

    private void assertSystemTablesAccessible(String url) throws Exception {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT * FROM SYSTABLES")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertTrue("Expected at least one system table, found: " + count, count > 0);
        }
    }

    @AfterClass
    public static void deregisterDrivers() throws Exception {
        for (Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
                drivers.hasMoreElements(); ) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
    }
}
