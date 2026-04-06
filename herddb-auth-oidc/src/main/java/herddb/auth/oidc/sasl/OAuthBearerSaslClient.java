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

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Client-side SASL implementation of the OAUTHBEARER mechanism (RFC 7628).
 * The bearer token is provided by a {@link Supplier} so callers can integrate with
 * their own token-acquisition/caching logic.
 */
public final class OAuthBearerSaslClient implements SaslClient {

    public static final String MECHANISM = "OAUTHBEARER";

    private final Supplier<String> tokenSupplier;
    private final String authzid;
    private boolean complete;

    public OAuthBearerSaslClient(Supplier<String> tokenSupplier) {
        this(tokenSupplier, null);
    }

    public OAuthBearerSaslClient(Supplier<String> tokenSupplier, String authzid) {
        this.tokenSupplier = tokenSupplier;
        this.authzid = authzid;
    }

    @Override
    public String getMechanismName() {
        return MECHANISM;
    }

    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
        if (complete) {
            // Per RFC 7628, server may send an error followed by empty, client returns 0x01
            return new byte[]{0x01};
        }
        String token = tokenSupplier.get();
        if (token == null || token.isEmpty()) {
            throw new SaslException("OAUTHBEARER token supplier returned empty token");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("n,");
        if (authzid != null && !authzid.isEmpty()) {
            sb.append("a=").append(authzid);
        }
        sb.append(',');
        sb.append('\u0001');
        sb.append("auth=Bearer ").append(token);
        sb.append('\u0001');
        sb.append('\u0001');
        complete = true;
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        byte[] copy = new byte[len];
        System.arraycopy(incoming, offset, copy, 0, len);
        return copy;
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        byte[] copy = new byte[len];
        System.arraycopy(outgoing, offset, copy, 0, len);
        return copy;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        return null;
    }

    @Override
    public void dispose() {
        complete = false;
    }
}
