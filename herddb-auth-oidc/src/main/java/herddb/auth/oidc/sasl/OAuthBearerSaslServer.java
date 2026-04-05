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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Server-side SASL implementation of the OAUTHBEARER mechanism (RFC 7628).
 * <p>
 * Client initial response format:
 * <pre>
 *   gs2-header \x01 "auth=Bearer " token \x01 \x01
 * </pre>
 * where {@code gs2-header} is typically {@code n,,} or {@code n,a=authzid,}.
 * <p>
 * This implementation completes in a single round-trip: on success returns an empty
 * server response; on failure throws a {@link SaslException}.
 */
public final class OAuthBearerSaslServer implements SaslServer {

    public static final String MECHANISM = "OAUTHBEARER";

    private final CallbackHandler callbackHandler;
    private boolean complete;
    private String authorizationId;

    public OAuthBearerSaslServer(CallbackHandler callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    @Override
    public String getMechanismName() {
        return MECHANISM;
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        if (response == null || response.length == 0) {
            throw new AuthenticationException("empty OAUTHBEARER response");
        }
        String raw = new String(response, StandardCharsets.UTF_8);
        ParsedInitialResponse parsed = parseInitialResponse(raw);

        OAuthBearerCallback cb = new OAuthBearerCallback();
        cb.setToken(parsed.token);
        try {
            callbackHandler.handle(new Callback[]{cb});
        } catch (IOException | UnsupportedCallbackException e) {
            throw new AuthenticationException("token validation failed: " + e.getMessage(), e);
        }
        if (cb.getValidationError() != null) {
            throw new AuthenticationException(cb.getValidationError());
        }
        String principal = cb.getAuthenticatedPrincipal();
        if (principal == null || principal.isEmpty()) {
            throw new AuthenticationException("token validation did not yield a principal");
        }
        // If client supplied an authzid, it must match the authenticated principal.
        if (parsed.authzid != null && !parsed.authzid.isEmpty() && !parsed.authzid.equals(principal)) {
            throw new AuthenticationException("authzid '" + parsed.authzid
                    + "' does not match authenticated principal '" + principal + "'");
        }
        this.authorizationId = principal;
        this.complete = true;
        return new byte[0];
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getAuthorizationID() {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return authorizationId;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        byte[] copy = new byte[len];
        System.arraycopy(incoming, offset, copy, 0, len);
        return copy;
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        byte[] copy = new byte[len];
        System.arraycopy(outgoing, offset, copy, 0, len);
        return copy;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return null;
    }

    @Override
    public void dispose() {
        complete = false;
        authorizationId = null;
    }

    /**
     * Parses an OAUTHBEARER client initial response per RFC 7628 section 3.1.
     * Returns the extracted bearer token and optional authzid.
     */
    static ParsedInitialResponse parseInitialResponse(String s) throws SaslException {
        int firstSep = s.indexOf('\u0001');
        if (firstSep < 0) {
            throw new AuthenticationException("missing gs2 header separator");
        }
        String gs2Header = s.substring(0, firstSep);
        String rest = s.substring(firstSep + 1);

        // gs2-header is "<saslname>,<optional authzid>," — commonly "n,," or "n,a=user,"
        String authzid = null;
        int firstComma = gs2Header.indexOf(',');
        int secondComma = firstComma < 0 ? -1 : gs2Header.indexOf(',', firstComma + 1);
        if (firstComma >= 0 && secondComma > firstComma) {
            String middle = gs2Header.substring(firstComma + 1, secondComma);
            if (middle.startsWith("a=")) {
                authzid = middle.substring(2);
            }
        }

        // rest is a series of attr=value pairs separated by 0x01, ending with 0x01 0x01
        String token = null;
        for (String pair : rest.split("\u0001")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = pair.substring(0, eq);
            String value = pair.substring(eq + 1);
            if ("auth".equals(key)) {
                if (!value.regionMatches(true, 0, "Bearer ", 0, 7)) {
                    throw new AuthenticationException("auth attribute must use Bearer scheme");
                }
                token = value.substring(7).trim();
            }
        }
        if (token == null || token.isEmpty()) {
            throw new AuthenticationException("missing Bearer token in OAUTHBEARER response");
        }
        return new ParsedInitialResponse(token, authzid);
    }

    static final class ParsedInitialResponse {
        final String token;
        final String authzid;

        ParsedInitialResponse(String token, String authzid) {
            this.token = token;
            this.authzid = authzid;
        }
    }
}
