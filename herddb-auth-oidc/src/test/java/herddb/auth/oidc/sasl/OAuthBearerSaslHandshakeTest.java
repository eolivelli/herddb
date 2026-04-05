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
package herddb.auth.oidc.sasl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.auth.oidc.OidcAuthException;
import herddb.auth.oidc.OidcConfiguration;
import herddb.auth.oidc.OidcTokenProvider;
import herddb.auth.oidc.OidcTokenValidator;
import herddb.auth.oidc.PrincipalExtractor;
import herddb.auth.oidc.TestOidcServer;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.junit.BeforeClass;
import org.junit.Test;

public class OAuthBearerSaslHandshakeTest {

    @BeforeClass
    public static void installProvider() {
        OAuthBearerSaslServerProvider.install();
    }

    private static CallbackHandler handlerBackedBy(TokenAuthenticator auth) {
        return new CallbackHandler() {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback cb : callbacks) {
                    if (cb instanceof OAuthBearerCallback) {
                        OAuthBearerCallback oauth = (OAuthBearerCallback) cb;
                        try {
                            String principal = auth.authenticate(oauth.getToken());
                            oauth.setAuthenticatedPrincipal(principal);
                        } catch (IOException e) {
                            oauth.setValidationError(e.getMessage());
                        }
                    } else {
                        throw new UnsupportedCallbackException(cb);
                    }
                }
            }
        };
    }

    @Test
    public void roundTripSucceedsWithValidToken() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            idp.registerClient("client-x", "secret-x");
            OidcConfiguration cfg = new OidcConfiguration(idp.getIssuerUrl()).discover();
            OidcTokenProvider tp = new OidcTokenProvider(cfg, "client-x", "secret-x", null);
            OidcTokenValidator tv = new OidcTokenValidator(cfg, null, new PrincipalExtractor());

            OAuthBearerSaslClient client = new OAuthBearerSaslClient(() -> {
                try {
                    return tp.getToken();
                } catch (OidcAuthException e) {
                    throw new RuntimeException(e);
                }
            });

            // use direct instantiation for the server (provider installation tested separately)
            SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(tv::validate));

            byte[] initial = client.evaluateChallenge(new byte[0]);
            byte[] reply = server.evaluateResponse(initial);
            assertNotNull(reply);
            assertTrue(client.isComplete());
            assertTrue(server.isComplete());
            assertEquals("client-x", server.getAuthorizationID());
        }
    }

    @Test
    public void expiredTokenIsRejected() throws Exception {
        try (TestOidcServer idp = new TestOidcServer()) {
            OidcConfiguration cfg = new OidcConfiguration(idp.getIssuerUrl()).discover();
            OidcTokenValidator tv = new OidcTokenValidator(cfg, null, new PrincipalExtractor());
            String expired = idp.issueToken("alice",
                    Collections.<String, Object>singletonMap("preferred_username", "alice"), -300);

            OAuthBearerSaslClient client = new OAuthBearerSaslClient(() -> expired);
            SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(tv::validate));
            byte[] initial = client.evaluateChallenge(new byte[0]);
            try {
                server.evaluateResponse(initial);
                fail("expected SaslException");
            } catch (SaslException e) {
                // expected
            }
        }
    }

    @Test
    public void rejectsMalformedInitialResponse() throws Exception {
        SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(t -> "ignored"));
        try {
            server.evaluateResponse("no-separator".getBytes("UTF-8"));
            fail("expected SaslException");
        } catch (SaslException e) {
            // expected
        }
    }

    @Test
    public void rejectsWrongAuthScheme() throws Exception {
        String bad = "n,,\u0001auth=Basic dXNlcjpwYXNz\u0001\u0001";
        SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(t -> "ignored"));
        try {
            server.evaluateResponse(bad.getBytes("UTF-8"));
            fail("expected SaslException");
        } catch (SaslException e) {
            assertTrue(e.getMessage().toLowerCase().contains("bearer"));
        }
    }

    @Test
    public void rejectsMissingToken() throws Exception {
        String bad = "n,,\u0001\u0001";
        SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(t -> "ignored"));
        try {
            server.evaluateResponse(bad.getBytes("UTF-8"));
            fail("expected SaslException");
        } catch (SaslException e) {
            // expected
        }
    }

    @Test
    public void providerInstallationWorksViaJvmSasl() throws Exception {
        // After install() we should be able to create the server via the standard API.
        OAuthBearerSaslServerProvider.install();
        Map<String, Object> props = Collections.emptyMap();
        SaslServer server = Sasl.createSaslServer("OAUTHBEARER", "herddb", null, props,
                handlerBackedBy(t -> "user-x"));
        assertNotNull(server);

        OAuthBearerSaslClient client = new OAuthBearerSaslClient(() -> "any-token");
        byte[] initial = client.evaluateChallenge(new byte[0]);
        byte[] reply = server.evaluateResponse(initial);
        assertTrue(server.isComplete());
        assertEquals("user-x", server.getAuthorizationID());
        assertNotNull(reply);
    }

    @Test
    public void authzidMismatchRejected() throws Exception {
        SaslServer server = new OAuthBearerSaslServer(handlerBackedBy(t -> "realuser"));
        String msg = "n,a=impostor,\u0001auth=Bearer some.token.here\u0001\u0001";
        try {
            server.evaluateResponse(msg.getBytes("UTF-8"));
            fail("expected SaslException");
        } catch (SaslException e) {
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("authzid"));
        }
    }
}
