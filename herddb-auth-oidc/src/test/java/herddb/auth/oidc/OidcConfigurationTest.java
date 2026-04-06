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

public class OidcConfigurationTest {

    @Test
    public void discoveryPopulatesJwksAndTokenEndpoint() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl()).discover();
            assertEquals(server.getJwksUri(), cfg.getJwksUri());
            assertEquals(server.getTokenEndpoint(), cfg.getTokenEndpoint());
        }
    }

    @Test
    public void explicitValuesNotOverriddenByDiscovery() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcConfiguration cfg = new OidcConfiguration(server.getIssuerUrl())
                    .setJwksUri("http://example/jwks-explicit")
                    .setTokenEndpoint("http://example/token-explicit")
                    .discover();
            assertEquals("http://example/jwks-explicit", cfg.getJwksUri());
            assertEquals("http://example/token-explicit", cfg.getTokenEndpoint());
        }
    }

    @Test
    public void trailingSlashIsNormalizedInIssuer() {
        OidcConfiguration cfg = new OidcConfiguration("http://example.com/realms/foo///");
        assertEquals("http://example.com/realms/foo", cfg.getIssuerUrl());
    }

    @Test
    public void requireJwksUriFailsBeforeDiscovery() {
        OidcConfiguration cfg = new OidcConfiguration("http://example.com");
        try {
            cfg.requireJwksUri();
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jwksUri"));
        }
    }

    @Test
    public void discoveryFailsOnUnreachableIssuer() {
        OidcConfiguration cfg = new OidcConfiguration("http://127.0.0.1:1")
                .setConnectTimeoutMillis(500)
                .setReadTimeoutMillis(500);
        try {
            cfg.discover();
            fail("expected IOException");
        } catch (Exception e) {
            assertNotNull(e);
        }
    }
}
