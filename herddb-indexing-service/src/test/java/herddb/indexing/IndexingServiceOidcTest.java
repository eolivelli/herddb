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
package herddb.indexing;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import herddb.auth.oidc.OidcBootstrap;
import herddb.auth.oidc.OidcConfiguration;
import herddb.auth.oidc.OidcTokenProvider;
import herddb.auth.oidc.TestOidcServer;
import herddb.auth.oidc.grpc.JwtAuthClientInterceptor;
import herddb.index.vector.RemoteVectorIndexService;
import java.util.Arrays;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IndexingServiceOidcTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static IndexingServerConfiguration oidcConfig(TestOidcServer idp) {
        Properties p = new Properties();
        p.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        p.setProperty(IndexingServerConfiguration.PROPERTY_INSTANCE_ID, "0");
        p.setProperty(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES, "1");
        p.setProperty(OidcBootstrap.PROP_ENABLED, "true");
        p.setProperty(OidcBootstrap.PROP_ISSUER_URL, idp.getIssuerUrl());
        return new IndexingServerConfiguration(p);
    }

    @Test
    public void validTokenAllowsStatusCall() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("index-client", "secret");
            try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                    folder.newFolder("log").toPath(),
                    folder.newFolder("data").toPath(),
                    oidcConfig(idp))) {
                svc.start();

                OidcConfiguration cfg = new OidcConfiguration(idp.getIssuerUrl()).discover();
                OidcTokenProvider tp = new OidcTokenProvider(cfg, "index-client", "secret", null);
                IndexingServiceClient client = new IndexingServiceClient(
                        Arrays.asList(svc.getAddress()),
                        30,
                        new JwtAuthClientInterceptor(() -> {
                            try {
                                return tp.getToken();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }));
                try {
                    RemoteVectorIndexService.IndexStatusInfo s = client.getIndexStatus(
                            "default", "t", "i");
                    assertNotNull(s);
                } finally {
                    client.close();
                }
            }
        }
    }

    @Test
    public void missingTokenRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                    folder.newFolder("log").toPath(),
                    folder.newFolder("data").toPath(),
                    oidcConfig(idp))) {
                svc.start();

                IndexingServiceClient client = new IndexingServiceClient(
                        Arrays.asList(svc.getAddress()));
                try {
                    client.getIndexStatus("default", "t", "i");
                    fail("expected UNAUTHENTICATED");
                } catch (Exception e) {
                    // expected
                } finally {
                    client.close();
                }
            }
        }
    }

    @Test
    public void invalidTokenRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            try (EmbeddedIndexingService svc = new EmbeddedIndexingService(
                    folder.newFolder("log").toPath(),
                    folder.newFolder("data").toPath(),
                    oidcConfig(idp))) {
                svc.start();

                IndexingServiceClient client = new IndexingServiceClient(
                        Arrays.asList(svc.getAddress()),
                        30,
                        new JwtAuthClientInterceptor(() -> "invalid.jwt.here"));
                try {
                    client.getIndexStatus("default", "t", "i");
                    fail("expected UNAUTHENTICATED");
                } catch (Exception e) {
                    // expected
                } finally {
                    client.close();
                }
            }
        }
    }
}
