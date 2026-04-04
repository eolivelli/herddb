/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.client;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.network.ServerHostData;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for follower reads routing logic.
 */
public class FollowerReadsRoutingTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testFollowerReadsDisabledByDefault() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        assertEquals(false, config.getBoolean(
                ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS,
                ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS_DEFAULT));
    }

    @Test
    public void testFollowerReadsConfigFromJdbcUrl() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.readJdbcUrl("jdbc:herddb:server:localhost:7000?client.allowReadsFromFollowers=true");
        assertEquals(true, config.getBoolean(
                ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS,
                ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS_DEFAULT));
    }

    @Test
    public void testFollowerReadsEnabledRoutesToReplica() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfig = newServerConfigurationWithAutoPort(baseDir);

        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS, serverConfig.getString(ServerConfiguration.PROPERTY_HOST, "localhost"));
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT, serverConfig.getInt(ServerConfiguration.PROPERTY_PORT, 0));
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);

            try (HDBClient client = new HDBClient(clientConfig)) {
                assertTrue(client.isAllowReadsFromFollowers());

                // Set up a metadata provider that reports replicas
                String nodeId = server.getNodeId();
                StaticClientSideMetadataProvider staticProvider = new StaticClientSideMetadataProvider(server);
                // Use a wrapper that reports replicas
                TrackingMetadataProvider trackingProvider = new TrackingMetadataProvider(staticProvider, nodeId);
                client.setClientSideMetadataProvider(trackingProvider);

                try (HDBConnection connection = client.openConnection()) {
                    // Create a table
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE t1 (k int primary key, v string)", 0, false, true, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO t1 (k,v) VALUES (1,'test')", 0, false, true, Collections.emptyList());

                    // Execute scan with tx=0 (autocommit) - should use follower reads path
                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10, false)) {
                        assertTrue(scan.hasNext());
                        assertNotNull(scan.next());
                    }

                    // Verify replicas were queried (since allowReadsFromFollowers is true and tx == 0)
                    assertTrue("getTableSpaceReplicas should have been called",
                            trackingProvider.replicasQueriedCount.get() > 0);
                }
            }
        }
    }

    @Test
    public void testFollowerReadsDisabledRoutesToLeader() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfig = newServerConfigurationWithAutoPort(baseDir);

        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS, serverConfig.getString(ServerConfiguration.PROPERTY_HOST, "localhost"));
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT, serverConfig.getInt(ServerConfiguration.PROPERTY_PORT, 0));
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, false);

            try (HDBClient client = new HDBClient(clientConfig)) {
                String nodeId = server.getNodeId();
                StaticClientSideMetadataProvider staticProvider = new StaticClientSideMetadataProvider(server);
                TrackingMetadataProvider trackingProvider = new TrackingMetadataProvider(staticProvider, nodeId);
                client.setClientSideMetadataProvider(trackingProvider);

                try (HDBConnection connection = client.openConnection()) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE t1 (k int primary key, v string)", 0, false, true, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO t1 (k,v) VALUES (1,'test')", 0, false, true, Collections.emptyList());

                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM t1", true, Collections.emptyList(), 0, 0, 10, false)) {
                        assertTrue(scan.hasNext());
                    }

                    // Verify replicas were NOT queried (since allowReadsFromFollowers is false)
                    assertEquals("getTableSpaceReplicas should not have been called",
                            0, trackingProvider.replicasQueriedCount.get());
                }
            }
        }
    }

    @Test
    public void testFollowerReadsNotUsedInTransaction() throws Exception {
        Path baseDir = folder.newFolder().toPath();
        ServerConfiguration serverConfig = newServerConfigurationWithAutoPort(baseDir);

        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            ClientConfiguration clientConfig = new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS, serverConfig.getString(ServerConfiguration.PROPERTY_HOST, "localhost"));
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT, serverConfig.getInt(ServerConfiguration.PROPERTY_PORT, 0));
            clientConfig.set(ClientConfiguration.PROPERTY_ALLOW_READS_FROM_FOLLOWERS, true);

            try (HDBClient client = new HDBClient(clientConfig)) {
                String nodeId = server.getNodeId();
                StaticClientSideMetadataProvider staticProvider = new StaticClientSideMetadataProvider(server);
                TrackingMetadataProvider trackingProvider = new TrackingMetadataProvider(staticProvider, nodeId);
                client.setClientSideMetadataProvider(trackingProvider);

                try (HDBConnection connection = client.openConnection()) {
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE t1 (k int primary key, v string)", 0, false, true, Collections.emptyList());
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO t1 (k,v) VALUES (1,'test')", 0, false, true, Collections.emptyList());

                    // Begin a transaction
                    long tx = connection.beginTransaction(TableSpace.DEFAULT);
                    assertTrue(tx > 0);

                    trackingProvider.replicasQueriedCount.set(0);

                    // Execute scan inside transaction - should NOT use follower reads
                    try (ScanResultSet scan = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT * FROM t1", true, Collections.emptyList(), tx, 0, 10, false)) {
                        assertTrue(scan.hasNext());
                    }

                    // Verify replicas were NOT queried (even with allowReadsFromFollowers=true, tx != 0)
                    assertEquals("getTableSpaceReplicas should not have been called inside a transaction",
                            0, trackingProvider.replicasQueriedCount.get());

                    connection.commitTransaction(TableSpace.DEFAULT, tx);
                }
            }
        }
    }

    /**
     * A metadata provider wrapper that tracks calls to getTableSpaceReplicas.
     */
    private static class TrackingMetadataProvider implements ClientSideMetadataProvider {
        private final ClientSideMetadataProvider delegate;
        private final String nodeId;
        final AtomicInteger replicasQueriedCount = new AtomicInteger();

        TrackingMetadataProvider(ClientSideMetadataProvider delegate, String nodeId) {
            this.delegate = delegate;
            this.nodeId = nodeId;
        }

        @Override
        public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
            return delegate.getTableSpaceLeader(tableSpace);
        }

        @Override
        public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
            return delegate.getServerHostData(nodeId);
        }

        @Override
        public Set<String> getTableSpaceReplicas(String tableSpace) throws ClientSideMetadataProviderException {
            replicasQueriedCount.incrementAndGet();
            return new HashSet<>(Arrays.asList(nodeId));
        }

        @Override
        public void requestMetadataRefresh(Exception err) throws ClientSideMetadataProviderException {
            delegate.requestMetadataRefresh(err);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
