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
package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import herddb.auth.oidc.OidcBootstrap;
import herddb.auth.oidc.OidcConfiguration;
import herddb.auth.oidc.OidcTokenProvider;
import herddb.auth.oidc.TestOidcServer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteFileServer} rejects unauthenticated requests
 * and accepts requests carrying a valid JWT bearer token when OIDC is
 * enabled. Issue #425 swapped the gRPC OIDC interceptor for an
 * OAUTHBEARER SASL handshake on the new Netty transport: the server now
 * uses {@link herddb.auth.oidc.sasl.OAuthBearerSaslServer} during
 * connection setup, and the client supplies the bearer token via a
 * {@code Supplier<String>} passed to {@link RemoteFileServiceClient}.
 */
public class RemoteFileServerOidcTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static Properties oidcProps(TestOidcServer idp) {
        Properties p = new Properties();
        p.setProperty(OidcBootstrap.PROP_ENABLED, "true");
        p.setProperty(OidcBootstrap.PROP_ISSUER_URL, idp.getIssuerUrl());
        return p;
    }

    @Test
    public void validTokenAllowsReadWrite() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("file-client", "file-secret");
            try (RemoteFileServer server = new RemoteFileServer(
                    "127.0.0.1", 0, folder.newFolder("data").toPath(),
                    2, oidcProps(idp))) {
                server.start();

                OidcConfiguration cfg = new OidcConfiguration(idp.getIssuerUrl()).discover();
                OidcTokenProvider tp = new OidcTokenProvider(cfg, "file-client", "file-secret", null);
                try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()),
                        Collections.emptyMap(),
                        () -> {
                            try {
                                return tp.getToken();
                            } catch (Exception e) {
                                // Broad catch: the token provider declares a checked
                                // OidcAuthException that the SASL handshake path does not
                                // surface; any failure here propagates as the SASL
                                // handshake failing the connect.
                                throw new RuntimeException(e);
                            }
                        })) {
                    byte[] content = "hello OIDC".getBytes(StandardCharsets.UTF_8);
                    client.writeFile("ts/u/data/1.page", content);
                    byte[] read = client.readFile("ts/u/data/1.page");
                    assertNotNull(read);
                    assertArrayEquals(content, read);
                }
            }
        }
    }

    @Test
    public void missingTokenIsRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            try (RemoteFileServer server = new RemoteFileServer(
                    "127.0.0.1", 0, folder.newFolder("data").toPath(),
                    2, oidcProps(idp))) {
                server.start();
                // no token supplier → no SASL handshake → server rejects the
                // first data-plane request with "authentication required".
                try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()))) {
                    client.writeFile("ts/u/data/1.page", new byte[]{0x01});
                    fail("expected authentication failure");
                } catch (RuntimeException e) {
                    // expected — server replied with TYPE_ERROR
                }
            }
        }
    }

    @Test
    public void invalidTokenIsRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            try (RemoteFileServer server = new RemoteFileServer(
                    "127.0.0.1", 0, folder.newFolder("data").toPath(),
                    2, oidcProps(idp))) {
                server.start();
                try (RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()),
                        Collections.emptyMap(),
                        () -> "not.a.real.jwt")) {
                    client.writeFile("ts/u/data/1.page", new byte[]{0x01});
                    fail("expected authentication failure");
                } catch (RuntimeException e) {
                    // expected — SASL handshake fails, connection is closed
                }
            }
        }
    }
}
