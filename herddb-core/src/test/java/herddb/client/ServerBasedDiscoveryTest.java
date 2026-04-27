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
package herddb.client;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.auth.oidc.TestOidcServer;
import herddb.model.TableSpace;
import herddb.network.ServerHostData;
import herddb.security.sasl.SaslUtils;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link ServerBasedClientSideMetadataProvider}: the ZooKeeper-less
 * cluster topology discovery mechanism wired in as the default provider for
 * {@code jdbc:herddb:server:…} URLs.
 */
public class ServerBasedDiscoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // -------------------------------------------------------------------------
    // Basic discovery tests
    // -------------------------------------------------------------------------

    /**
     * A standalone server is fully discovered through its system tables and
     * basic DML operations work end-to-end using the new provider.
     */
    @Test
    public void standaloneServerDiscoveredAndDmlWorks() throws Exception {
        ServerConfiguration serverConfig =
                newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            // Use the actual bound address — serverConfig.getInt(PROPERTY_PORT)
            // still returns 0 after auto-binding; only server.getServerHostData()
            // carries the real ephemeral port.
            ServerHostData serverAddr = server.getServerHostData();

            ClientConfiguration clientConfig =
                    new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                    ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                    serverAddr.getHost());
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT,
                    serverAddr.getPort());

            try (HDBClient client = new HDBClient(clientConfig);
                 HDBConnection connection = client.openConnection()) {

                // The default provider must be the new server-based one
                assertTrue("Expected ServerBasedClientSideMetadataProvider, got: "
                        + client.getClientSideMetadataProvider().getClass().getSimpleName(),
                        client.getClientSideMetadataProvider()
                                instanceof ServerBasedClientSideMetadataProvider);

                connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE discovery_test (id int primary key, v string)",
                        0, false, true, Collections.emptyList());
                connection.executeUpdate(TableSpace.DEFAULT,
                        "INSERT INTO discovery_test (id, v) VALUES (1, 'hello')",
                        0, false, true, Collections.emptyList());

                try (ScanResultSet rs = connection.executeScan(TableSpace.DEFAULT,
                        "SELECT id, v FROM discovery_test WHERE id = 1",
                        true, Collections.emptyList(), 0, 0, 10, false)) {
                    assertTrue(rs.hasNext());
                    assertEquals("hello", rs.next().get("v").toString());
                }
            }
        }
    }

    /**
     * After a successful discovery refresh the provider caches the correct
     * leader node-id and its matching {@link ServerHostData}, which can then
     * be used for routing.
     */
    @Test
    public void leaderAndNodeAddressDiscoveredCorrectly() throws Exception {
        ServerConfiguration serverConfig =
                newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            // Use the actual bound address.
            ServerHostData serverAddr = server.getServerHostData();

            ClientConfiguration clientConfig =
                    new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                    ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                    serverAddr.getHost());
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT,
                    serverAddr.getPort());

            try (HDBClient client = new HDBClient(clientConfig)) {
                ClientSideMetadataProvider provider = client.getClientSideMetadataProvider();

                // Trigger discovery
                String leader = provider.getTableSpaceLeader(TableSpace.DEFAULT);
                assertNotNull("Leader must not be null after discovery", leader);

                ServerHostData hostData = provider.getServerHostData(leader);
                assertNotNull("ServerHostData for leader must not be null", hostData);
                assertEquals("Discovered port must match server advertised port",
                        serverAddr.getPort(), hostData.getPort());
            }
        }
    }

    /**
     * After {@link ClientSideMetadataProvider#requestMetadataRefresh} is called
     * the cache is cleared and the provider successfully re-discovers from the
     * server on the next request.
     */
    @Test
    public void metadataCacheIsRefreshedOnRequest() throws Exception {
        ServerConfiguration serverConfig =
                newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            ServerHostData serverAddr = server.getServerHostData();

            ClientConfiguration clientConfig =
                    new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                    ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                    serverAddr.getHost());
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT,
                    serverAddr.getPort());

            try (HDBClient client = new HDBClient(clientConfig)) {
                ClientSideMetadataProvider provider = client.getClientSideMetadataProvider();

                // First discovery
                String leaderFirst = provider.getTableSpaceLeader(TableSpace.DEFAULT);
                assertNotNull(leaderFirst);

                // Force a full refresh
                provider.requestMetadataRefresh(new RuntimeException("simulated error"));

                // Re-discover — must succeed and return the same leader
                String leaderAfterRefresh = provider.getTableSpaceLeader(TableSpace.DEFAULT);
                assertNotNull(leaderAfterRefresh);
                assertEquals("Leader must be stable across refreshes",
                        leaderFirst, leaderAfterRefresh);
            }
        }
    }

    /**
     * After the first successful refresh the provider's fallback list contains
     * the addresses returned by {@code sysnodes}, enabling failover in a
     * multi-node cluster without relying on ZooKeeper.
     */
    @Test
    public void knownNodesPopulatedAfterFirstRefresh() throws Exception {
        ServerConfiguration serverConfig =
                newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        try (Server server = new Server(serverConfig)) {
            server.start();
            server.waitForStandaloneBoot();

            ServerHostData serverAddr = server.getServerHostData();

            ClientConfiguration clientConfig =
                    new ClientConfiguration(folder.newFolder().toPath());
            clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                    ClientConfiguration.PROPERTY_MODE_STANDALONE);
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                    serverAddr.getHost());
            clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT,
                    serverAddr.getPort());

            try (HDBClient client = new HDBClient(clientConfig)) {
                ServerBasedClientSideMetadataProvider provider =
                        (ServerBasedClientSideMetadataProvider) client.getClientSideMetadataProvider();

                // Trigger discovery
                provider.getTableSpaceLeader(TableSpace.DEFAULT);

                // knownNodes must contain at least the single standalone server
                assertTrue("knownNodes must be non-empty after first refresh",
                        !provider.knownNodes.isEmpty());
                boolean found = provider.knownNodes.stream()
                        .anyMatch(h -> h.getPort() == serverAddr.getPort());
                assertTrue("Bootstrap server must appear in knownNodes", found);
            }
        }
    }

    // -------------------------------------------------------------------------
    // OIDC authentication test
    // -------------------------------------------------------------------------

    /**
     * When the server requires OIDC / OAUTHBEARER authentication the
     * discovery queries executed by the internal bootstrap client must also
     * present a valid token. This test verifies that all OIDC properties are
     * forwarded from the outer {@link ClientConfiguration} to the bootstrap
     * client, so both the topology discovery and subsequent DML operations
     * succeed without any explicit {@code setClientSideMetadataProvider} call.
     */
    @Test
    public void discoveryWorksWithOidcAuth() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("herddb-jdbc", "jdbc-secret");

            ServerConfiguration serverConfig =
                    newServerConfigurationWithAutoPort(folder.newFolder("srv").toPath());
            serverConfig.set(ServerConfiguration.PROPERTY_OIDC_ENABLED, "true");
            serverConfig.set(ServerConfiguration.PROPERTY_OIDC_ISSUER_URL, idp.getIssuerUrl());

            try (Server server = new Server(serverConfig)) {
                server.start();
                server.waitForStandaloneBoot();

                ServerHostData serverAddr = server.getServerHostData();

                ClientConfiguration clientConfig =
                        new ClientConfiguration(folder.newFolder("cli").toPath());
                clientConfig.set(ClientConfiguration.PROPERTY_MODE,
                        ClientConfiguration.PROPERTY_MODE_STANDALONE);
                clientConfig.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS,
                        serverAddr.getHost());
                clientConfig.set(ClientConfiguration.PROPERTY_SERVER_PORT,
                        serverAddr.getPort());
                // OIDC / OAUTHBEARER settings — must be forwarded to the
                // internal bootstrap client that runs the discovery queries
                clientConfig.set(ClientConfiguration.PROPERTY_AUTH_MECH,
                        SaslUtils.AUTH_OAUTHBEARER);
                clientConfig.set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL,
                        idp.getIssuerUrl());
                clientConfig.set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, "herddb-jdbc");
                clientConfig.set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, "jdbc-secret");
                // The authzid presented by the client must match the
                // preferred_username claim in the token (which the stub IDP
                // sets to the client_id).
                clientConfig.set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "herddb-jdbc");

                try (HDBClient client = new HDBClient(clientConfig);
                     HDBConnection connection = client.openConnection()) {

                    // No explicit setClientSideMetadataProvider call —
                    // ServerBasedClientSideMetadataProvider is the default.
                    assertTrue(client.getClientSideMetadataProvider()
                            instanceof ServerBasedClientSideMetadataProvider);

                    // DDL + DML must both succeed (discovery auth + regular auth)
                    long created = connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE oidc_test (id int primary key, v string)",
                            0, false, true, Collections.emptyList()).updateCount;
                    assertEquals(1, created);

                    connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO oidc_test (id, v) VALUES (?, ?)",
                            0, false, true, Arrays.asList(42, "oidc-value"));

                    try (ScanResultSet rs = connection.executeScan(TableSpace.DEFAULT,
                            "SELECT v FROM oidc_test WHERE id = ?",
                            true, Collections.singletonList(42), 0, 0, 10, false)) {
                        assertTrue(rs.hasNext());
                        assertEquals("oidc-value", rs.next().get("v").toString());
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // qualifyHostname unit tests
    // -------------------------------------------------------------------------

    /**
     * A short hostname is qualified by appending the domain portion of the
     * known FQDN — the primary use case for Kubernetes StatefulSet pods.
     */
    @Test
    public void qualifyHostname_shortNameGetsQualified() {
        assertEquals(
                "server-0.headless-svc.namespace.svc.cluster.local",
                ServerBasedClientSideMetadataProvider.qualifyHostname(
                        "server-0",
                        "server-0.headless-svc.namespace.svc.cluster.local"));
    }

    /**
     * A short hostname for a DIFFERENT pod in the same cluster is correctly
     * qualified using the domain suffix extracted from the bootstrap FQDN.
     */
    @Test
    public void qualifyHostname_differentPodGetsQualified() {
        assertEquals(
                "server-1.headless-svc.namespace.svc.cluster.local",
                ServerBasedClientSideMetadataProvider.qualifyHostname(
                        "server-1",
                        "server-0.headless-svc.namespace.svc.cluster.local"));
    }

    /**
     * An already-qualified FQDN is returned unchanged even if the bootstrap
     * host is also an FQDN.
     */
    @Test
    public void qualifyHostname_fqdnPassedThrough() {
        assertEquals(
                "already.qualified.host.example.com",
                ServerBasedClientSideMetadataProvider.qualifyHostname(
                        "already.qualified.host.example.com",
                        "bootstrap.headless-svc.namespace.svc.cluster.local"));
    }

    /**
     * When the bootstrap host is itself a short name (no dots), the discovered
     * hostname is returned unchanged — there is no domain to borrow.
     */
    @Test
    public void qualifyHostname_shortBootstrapNoOp() {
        assertEquals(
                "server-0",
                ServerBasedClientSideMetadataProvider.qualifyHostname(
                        "server-0",
                        "bootstrap-short"));
    }

    /**
     * A plain IP address (has dots) is returned unchanged.
     */
    @Test
    public void qualifyHostname_ipAddressPassedThrough() {
        assertEquals(
                "10.42.0.5",
                ServerBasedClientSideMetadataProvider.qualifyHostname(
                        "10.42.0.5",
                        "bootstrap.headless-svc.namespace.svc.cluster.local"));
    }
}
