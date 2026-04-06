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

import java.io.IOException;
import java.util.Properties;

/**
 * Convenience helpers for bootstrapping OIDC components from a {@link Properties} bag,
 * used by the file server and indexing service.
 * <p>
 * Recognized property keys:
 * <ul>
 *     <li>{@code auth.oidc.enabled} (default {@code false})</li>
 *     <li>{@code auth.oidc.issuer.url} (required when enabled)</li>
 *     <li>{@code auth.oidc.jwks.uri} (optional - overrides discovery)</li>
 *     <li>{@code auth.oidc.audience} (optional)</li>
 *     <li>{@code auth.oidc.username.claim} (optional, default {@code preferred_username})</li>
 *     <li>{@code auth.oidc.client.id} (required for clients)</li>
 *     <li>{@code auth.oidc.client.secret} (required for clients)</li>
 *     <li>{@code auth.oidc.scope} (optional)</li>
 * </ul>
 */
public final class OidcBootstrap {

    public static final String PROP_ENABLED = "auth.oidc.enabled";
    public static final String PROP_ISSUER_URL = "auth.oidc.issuer.url";
    public static final String PROP_JWKS_URI = "auth.oidc.jwks.uri";
    public static final String PROP_AUDIENCE = "auth.oidc.audience";
    public static final String PROP_USERNAME_CLAIM = "auth.oidc.username.claim";
    public static final String PROP_CLIENT_ID = "auth.oidc.client.id";
    public static final String PROP_CLIENT_SECRET = "auth.oidc.client.secret";
    public static final String PROP_SCOPE = "auth.oidc.scope";

    private OidcBootstrap() {
    }

    public static boolean isEnabled(Properties props) {
        return Boolean.parseBoolean(props.getProperty(PROP_ENABLED, "false"));
    }

    /**
     * Builds an {@link OidcTokenValidator} from the given properties. Performs OIDC
     * discovery unless {@code auth.oidc.jwks.uri} is set.
     */
    public static OidcTokenValidator buildValidator(Properties props) throws IOException {
        String issuer = props.getProperty(PROP_ISSUER_URL, "");
        if (issuer.isEmpty()) {
            throw new IOException(PROP_ISSUER_URL + " is required when " + PROP_ENABLED + "=true");
        }
        String jwksUri = props.getProperty(PROP_JWKS_URI, "");
        String audience = props.getProperty(PROP_AUDIENCE, "");
        String usernameClaim = props.getProperty(PROP_USERNAME_CLAIM, "");
        OidcConfiguration cfg = new OidcConfiguration(issuer);
        if (!jwksUri.isEmpty()) {
            cfg.setJwksUri(jwksUri);
        } else {
            cfg.discover();
        }
        return new OidcTokenValidator(cfg, audience.isEmpty() ? null : audience,
                new PrincipalExtractor(usernameClaim));
    }

    /**
     * Builds an {@link OidcTokenProvider} (client_credentials) from the given properties.
     */
    public static OidcTokenProvider buildProvider(Properties props) throws IOException {
        String issuer = props.getProperty(PROP_ISSUER_URL, "");
        String clientId = props.getProperty(PROP_CLIENT_ID, "");
        String clientSecret = props.getProperty(PROP_CLIENT_SECRET, "");
        if (issuer.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new IOException(PROP_ISSUER_URL + ", " + PROP_CLIENT_ID + ", and "
                    + PROP_CLIENT_SECRET + " are all required to build an OidcTokenProvider");
        }
        String scope = props.getProperty(PROP_SCOPE, "");
        OidcConfiguration cfg = new OidcConfiguration(issuer).discover();
        return new OidcTokenProvider(cfg, clientId, clientSecret, scope.isEmpty() ? null : scope);
    }
}
