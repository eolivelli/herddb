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
import herddb.auth.oidc.grpc.JwtAuthClientInterceptor;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteFileServer} rejects unauthenticated requests and accepts
 * requests with a valid JWT bearer token when OIDC is enabled.
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
                RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()),
                        Collections.emptyMap(),
                        new JwtAuthClientInterceptor(() -> {
                            try {
                                return tp.getToken();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                try {
                    byte[] content = "hello OIDC".getBytes(StandardCharsets.UTF_8);
                    client.writeFile("ts/u/data/1.page", content);
                    byte[] read = client.readFile("ts/u/data/1.page");
                    assertNotNull(read);
                    assertArrayEquals(content, read);
                } finally {
                    client.close();
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

                // no interceptor → no Authorization header
                RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()));
                try {
                    client.writeFile("ts/u/data/1.page", new byte[]{0x01});
                    fail("expected UNAUTHENTICATED");
                } catch (RuntimeException e) {
                    // expected - gRPC status exception propagates
                } finally {
                    client.close();
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

                RemoteFileServiceClient client = new RemoteFileServiceClient(
                        Arrays.asList("localhost:" + server.getPort()),
                        Collections.emptyMap(),
                        new JwtAuthClientInterceptor(() -> "not.a.real.jwt"));
                try {
                    client.writeFile("ts/u/data/1.page", new byte[]{0x01});
                    fail("expected UNAUTHENTICATED");
                } catch (RuntimeException e) {
                    // expected
                } finally {
                    client.close();
                }
            }
        }
    }
}
