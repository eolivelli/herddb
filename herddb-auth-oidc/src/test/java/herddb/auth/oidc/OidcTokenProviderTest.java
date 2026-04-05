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
package herddb.auth.oidc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

public class OidcTokenProviderTest {

    @Test
    public void fetchesTokenWithClientCredentials() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            server.registerClient("svc-a", "secret-a");
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "svc-a", "secret-a", null);
            String token = p.getToken();
            assertNotNull(token);
            assertEquals(1, server.tokenRequests.get());
        }
    }

    @Test
    public void cachesTokenAcrossCalls() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            server.registerClient("svc-a", "secret-a");
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "svc-a", "secret-a", null);
            String t1 = p.getToken();
            String t2 = p.getToken();
            String t3 = p.getToken();
            assertEquals(t1, t2);
            assertEquals(t1, t3);
            assertEquals("should have fetched only once", 1, server.tokenRequests.get());
        }
    }

    @Test
    public void invalidateForcesRefresh() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            server.registerClient("svc-a", "secret-a");
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "svc-a", "secret-a", null);
            p.getToken();
            p.invalidate();
            p.getToken();
            assertEquals(2, server.tokenRequests.get());
        }
    }

    @Test
    public void wrongSecretFails() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            server.registerClient("svc-a", "secret-a");
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "svc-a", "wrong-secret", null);
            try {
                p.getToken();
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("401"));
            }
        }
    }

    @Test
    public void unknownClientFails() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "unknown", "x", null);
            try {
                p.getToken();
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("401"));
            }
        }
    }

    @Test
    public void tokenIsVerifiable() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            server.registerClient("svc-a", "secret-a");
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            OidcTokenProvider p = new OidcTokenProvider(cfg, "svc-a", "secret-a", null);
            String token = p.getToken();
            OidcTokenValidator v = new OidcTokenValidator(cfg, null, new PrincipalExtractor());
            assertEquals("svc-a", v.validate(token));
        }
    }
}
