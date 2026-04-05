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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.client.ClientConfiguration;
import herddb.client.HDBClient;
import herddb.client.HDBConnection;
import herddb.client.HDBException;
import herddb.model.TableSpace;
import herddb.security.sasl.SaslUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * End-to-end test using a real Keycloak instance in a Testcontainer.
 * <p>
 * Spins up Keycloak, creates a realm + client with client_credentials grant via
 * the admin REST API, and verifies that a HerdDB server configured with OIDC
 * accepts JDBC clients that authenticate via OAUTHBEARER.
 *
 * <p>Requires Docker.
 */
public class KeycloakOidcE2ETest {

    private static final String REALM = "herddb";
    private static final String CLIENT_ID = "herddb-jdbc";
    private static final String CLIENT_SECRET = "jdbc-secret-aaaaaaaaaaaaaaaa";

    private static GenericContainer<?> keycloak;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static String issuerUrl;

    @BeforeClass
    public static void configureRealm() throws Exception {
        Assume.assumeTrue("Docker is required for this test",
                DockerClientFactory.instance().isDockerAvailable());
        keycloak = new GenericContainer<>("quay.io/keycloak/keycloak:25.0")
                .withExposedPorts(8080)
                .withEnv("KEYCLOAK_ADMIN", "admin")
                .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
                .withCommand("start-dev")
                .waitingFor(Wait.forHttp("/realms/master").forStatusCode(200)
                        .withStartupTimeout(java.time.Duration.ofMinutes(3)));
        keycloak.start();
        String host = keycloak.getHost();
        int port = keycloak.getMappedPort(8080);
        String base = "http://" + host + ":" + port;
        issuerUrl = base + "/realms/" + REALM;

        String adminToken = fetchAdminToken(base);
        createRealm(base, adminToken);
        createClient(base, adminToken);
        addUsernameMapper(base, adminToken);
    }

    @AfterClass
    public static void teardown() {
        if (keycloak != null) {
            keycloak.stop();
        }
    }

    private Server buildOidcServer(Path dir) throws Exception {
        ServerConfiguration cfg = new ServerConfiguration(dir);
        cfg.set(ServerConfiguration.PROPERTY_NODEID, "test-node");
        cfg.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        cfg.set(ServerConfiguration.PROPERTY_PORT, 0);
        cfg.set(ServerConfiguration.PROPERTY_HOST, "127.0.0.1");
        cfg.set(ServerConfiguration.PROPERTY_OIDC_ENABLED, "true");
        cfg.set(ServerConfiguration.PROPERTY_OIDC_ISSUER_URL, issuerUrl);
        return new Server(cfg);
    }

    @Test
    public void jdbcClientWithClientCredentialsWorks() throws Exception {
        try (Server server = buildOidcServer(folder.newFolder("s").toPath())) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("c").toPath())
                    .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                    .set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL, issuerUrl)
                    .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, CLIENT_ID)
                    .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, CLIENT_SECRET)
                    // Keycloak service account username is "service-account-<client-id>"
                    .set(ClientConfiguration.PROPERTY_CLIENT_USERNAME, "service-account-" + CLIENT_ID));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                long r = connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE t1 (id string primary key)", 0, false, true,
                        Collections.emptyList()).updateCount;
                assertEquals(1, r);
            }
        }
    }

    @Test
    public void wrongSecretIsRejected() throws Exception {
        try (Server server = buildOidcServer(folder.newFolder("s").toPath())) {
            server.start();
            server.waitForStandaloneBoot();

            try (HDBClient client = new HDBClient(new ClientConfiguration(folder.newFolder("c").toPath())
                    .set(ClientConfiguration.PROPERTY_AUTH_MECH, SaslUtils.AUTH_OAUTHBEARER)
                    .set(ClientConfiguration.PROPERTY_OIDC_ISSUER_URL, issuerUrl)
                    .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_ID, CLIENT_ID)
                    .set(ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET, "definitely-not-the-secret"));
                 HDBConnection connection = client.openConnection()) {
                client.setClientSideMetadataProvider(new StaticClientSideMetadataProvider(server));
                connection.executeUpdate(TableSpace.DEFAULT,
                        "CREATE TABLE t1 (id string primary key)", 0, false, true,
                        Collections.emptyList());
                fail("expected auth failure");
            } catch (HDBException | RuntimeException e) {
                assertTrue(e.getMessage() != null);
            }
        }
    }

    // --- Keycloak admin API helpers ---

    private static String fetchAdminToken(String baseUrl) throws Exception {
        String url = baseUrl + "/realms/master/protocol/openid-connect/token";
        String body = "grant_type=password&client_id=admin-cli&username=admin&password=admin";
        String json = postForm(url, body, null);
        int idx = json.indexOf("\"access_token\":\"");
        if (idx < 0) {
            throw new IOException("admin token response: " + json);
        }
        int start = idx + "\"access_token\":\"".length();
        int end = json.indexOf('"', start);
        return json.substring(start, end);
    }

    private static void createRealm(String baseUrl, String adminToken) throws Exception {
        String url = baseUrl + "/admin/realms";
        String json = "{\"realm\":\"" + REALM + "\",\"enabled\":true,\"accessTokenLifespan\":300}";
        int status = postJson(url, json, adminToken);
        if (status != 201 && status != 409) {
            throw new IOException("createRealm status=" + status);
        }
    }

    private static void createClient(String baseUrl, String adminToken) throws Exception {
        String url = baseUrl + "/admin/realms/" + REALM + "/clients";
        String json = "{"
                + "\"clientId\":\"" + CLIENT_ID + "\","
                + "\"secret\":\"" + CLIENT_SECRET + "\","
                + "\"enabled\":true,"
                + "\"publicClient\":false,"
                + "\"serviceAccountsEnabled\":true,"
                + "\"standardFlowEnabled\":false,"
                + "\"directAccessGrantsEnabled\":false,"
                + "\"protocol\":\"openid-connect\""
                + "}";
        int status = postJson(url, json, adminToken);
        if (status != 201 && status != 409) {
            throw new IOException("createClient status=" + status);
        }
    }

    private static void addUsernameMapper(String baseUrl, String adminToken) throws Exception {
        // Look up the internal client UUID
        String getClientsUrl = baseUrl + "/admin/realms/" + REALM + "/clients?clientId=" + CLIENT_ID;
        String clientsJson = getWithToken(getClientsUrl, adminToken);
        int idIdx = clientsJson.indexOf("\"id\":\"");
        if (idIdx < 0) {
            throw new IOException("client list: " + clientsJson);
        }
        int start = idIdx + "\"id\":\"".length();
        int end = clientsJson.indexOf('"', start);
        String clientUuid = clientsJson.substring(start, end);
        // Keycloak 25 already includes 'preferred_username' via the profile scope by default
        // for service accounts, so nothing more to do here.
        // Left as a placeholder to verify client exists.
        if (clientUuid.isEmpty()) {
            throw new IOException("empty clientUuid");
        }
    }

    private static String postForm(String url, String body, String bearer) throws Exception {
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        if (bearer != null) {
            con.setRequestProperty("Authorization", "Bearer " + bearer);
        }
        try (OutputStream out = con.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return readBody(con);
    }

    private static int postJson(String url, String json, String bearer) throws Exception {
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Authorization", "Bearer " + bearer);
        try (OutputStream out = con.getOutputStream()) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
        int code = con.getResponseCode();
        try (InputStream in = code >= 400 ? con.getErrorStream() : con.getInputStream()) {
            if (in != null) {
                byte[] buf = new byte[4096];
                while (in.read(buf) > 0) { }
            }
        }
        return code;
    }

    private static String getWithToken(String url, String bearer) throws Exception {
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Authorization", "Bearer " + bearer);
        con.setRequestProperty("Accept", "application/json");
        return readBody(con);
    }

    private static String readBody(HttpURLConnection con) throws IOException {
        int code = con.getResponseCode();
        InputStream in = code >= 400 ? con.getErrorStream() : con.getInputStream();
        if (in == null) {
            return "";
        }
        try {
            byte[] buf = new byte[8192];
            int read = in.read(buf);
            return read <= 0 ? "" : new String(buf, 0, read, StandardCharsets.UTF_8);
        } finally {
            in.close();
        }
    }

    @SuppressWarnings("unused")
    private static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
