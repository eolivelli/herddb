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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class OidcTokenValidatorTest {

    private OidcConfiguration discoveredConfig(TestOidcServer server) throws Exception {
        return new OidcConfiguration(server.getIssuerUrl()).discover();
    }

    @Test
    public void validTokenResolvesUsername() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor());
            String token = server.issueToken("alice",
                    Collections.<String, Object>singletonMap("preferred_username", "alice"), 300);
            assertEquals("alice", v.validate(token));
        }
    }

    @Test
    public void fallsBackToSubWhenClaimMissing() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor("custom_claim"));
            String token = server.issueToken("bob", null, 300);
            assertEquals("bob", v.validate(token));
        }
    }

    @Test
    public void customClaimIsPreferred() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor("email"));
            Map<String, Object> claims = new HashMap<>();
            claims.put("email", "carol@example.com");
            claims.put("preferred_username", "carol");
            String token = server.issueToken("carol-sub", claims, 300);
            assertEquals("carol@example.com", v.validate(token));
        }
    }

    @Test
    public void expiredTokenIsRejected() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor());
            String token = server.issueToken("alice", null, -60);
            try {
                v.validate(token);
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("exp"));
            }
        }
    }

    @Test
    public void wrongIssuerIsRejected() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor());
            String token = server.issueToken("alice", null, 300, "http://other-issuer.example");
            try {
                v.validate(token);
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("iss"));
            }
        }
    }

    @Test
    public void audienceMismatchIsRejected() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), "required-aud", new PrincipalExtractor());
            String token = server.issueToken("alice",
                    Collections.<String, Object>singletonMap("aud", "other-aud"), 300);
            try {
                v.validate(token);
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                // expected
            }
        }
    }

    @Test
    public void audienceMatchAccepted() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), "my-service", new PrincipalExtractor());
            Map<String, Object> claims = new HashMap<>();
            claims.put("aud", "my-service");
            claims.put("preferred_username", "alice");
            String token = server.issueToken("alice", claims, 300);
            assertEquals("alice", v.validate(token));
        }
    }

    @Test
    public void malformedTokenIsRejected() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor());
            try {
                v.validate("not.a.jwt");
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                // expected
            }
        }
    }

    @Test
    public void emptyTokenIsRejected() throws Exception {
        try (TestOidcServer server = new TestOidcServer()) {
            OidcTokenValidator v = new OidcTokenValidator(
                    discoveredConfig(server), null, new PrincipalExtractor());
            try {
                v.validate("");
                fail("expected OidcAuthException");
            } catch (OidcAuthException e) {
                assertTrue(e.getMessage().toLowerCase().contains("empty"));
            }
        }
    }
}
