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
package herddb.server.security;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.auth.oidc.TestOidcServer;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.model.TableSpace;
import herddb.security.sasl.SaslUtils;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.server.StaticClientSideMetadataProvider;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end JDBC client &rarr; embedded HerdDB server with OIDC / OAUTHBEARER authentication.
 */
public class OidcAuthTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ServerConfiguration oidcServerConfig(TestOidcServer idp, String baseDir) throws Exception {
        ServerConfiguration cfg = newServerConfigurationWithAutoPort(folder.newFolder(baseDir).toPath());
        cfg.set(ServerConfiguration.PROPERTY_OIDC_ENABLED, "true");
        cfg.set(ServerConfiguration.PROPERTY_OIDC_ISSUER_URL, idp.getIssuerUrl());
        return cfg;
    }

    @Test
    public void validClientCredentialsAuthenticates() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("herddb-jdbc", "jdbc-secret");
            try (Server server = new Server(oidcServerConfig(idp, "srv"))) {
                server.start();

                try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("cli").toPath())
                        .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                        .set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL, idp.getIssuerUrl())
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, "herddb-jdbc")
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, "jdbc-secret")
                        // the authzid sent by the client should match the principal from the token
                        .set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "herddb-jdbc"));
                     HDBConnection connection = client.openConnection()) {
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    long r = connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key, n1 long)",
                            0, false, true, Collections.emptyList()).updateCount;
                    assertEquals(1, r);
                    assertEquals(1, connection.executeUpdate(TableSpace.DEFAULT,
                            "INSERT INTO mytable (id,n1) values(?,?)", 0, false, true,
                            Arrays.asList("a", 7L)).updateCount);
                }
            }
        }
    }

    @Test
    public void wrongClientSecretIsRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("herddb-jdbc", "jdbc-secret");
            try (Server server = new Server(oidcServerConfig(idp, "srv"))) {
                server.start();

                try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("cli").toPath())
                        .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                        .set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL, idp.getIssuerUrl())
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, "herddb-jdbc")
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, "wrong-secret"));
                     HDBConnection connection = client.openConnection()) {
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key)",
                            0, false, true, Collections.emptyList());
                    fail("expected auth failure");
                } catch (HDBException | RuntimeException e) {
                    // expected - token acquisition fails
                }
            }
        }
    }

    @Test
    public void tokenFromDifferentIssuerIsRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer();
             TestOidcServer other = new TestOidcServer()) {
            other.registerClient("other-client", "other-secret");
            try (Server server = new Server(oidcServerConfig(idp, "srv"))) {
                server.start();

                // point client at the wrong issuer
                try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("cli").toPath())
                        .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                        .set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL, other.getIssuerUrl())
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, "other-client")
                        .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, "other-secret"));
                     HDBConnection connection = client.openConnection()) {
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key)",
                            0, false, true, Collections.emptyList());
                    fail("expected auth failure - token from wrong issuer");
                } catch (HDBException e) {
                    assertTrue(e.getMessage().toLowerCase().contains("auth"));
                }
            }
        }
    }

    @Test
    public void serverStartFailsWithoutIssuerUrl() throws Exception {
        ServerConfiguration cfg = newServerConfigurationWithAutoPort(folder.newFolder("bad").toPath());
        cfg.set(ServerConfiguration.PROPERTY_OIDC_ENABLED, "true");
        // no issuer URL
        try {
            new Server(cfg);
            fail("expected startup failure");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage() + "", e.getMessage().toLowerCase().contains("oidc"));
        }
    }

    @Test
    public void preAcquiredTokenWorks() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            String token = idp.issueToken("static-user",
                    Collections.<String, Object>singletonMap("preferred_username", "static-user"), 300);
            try (Server server = new Server(oidcServerConfig(idp, "srv"))) {
                server.start();

                try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("cli").toPath())
                        .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                        .set(ClientConfiguration.PROPERTY_OIDC_TOKEN, token)
                        .set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "static-user"));
                     HDBConnection connection = client.openConnection()) {
                    client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                    long r = connection.executeUpdate(TableSpace.DEFAULT,
                            "CREATE TABLE mytable (id string primary key)",
                            0, false, true, Collections.emptyList()).updateCount;
                    assertEquals(1, r);
                }
            }
        }
    }
}
