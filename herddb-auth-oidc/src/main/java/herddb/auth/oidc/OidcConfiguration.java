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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Objects;

/**
 * OIDC provider configuration.
 * <p>
 * The issuer URL points at an OIDC provider (e.g. {@code https://keycloak.example/realms/myrealm}).
 * When {@link #discover()} is called, the well-known discovery document at
 * {@code <issuer>/.well-known/openid-configuration} is fetched and the {@code jwks_uri} and
 * {@code token_endpoint} are resolved. Individual endpoints may be explicitly overridden.
 *
 * @author enrico.olivelli
 */
public final class OidcConfiguration {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String issuerUrl;
    private String jwksUri;
    private String tokenEndpoint;
    private int connectTimeoutMillis = 5000;
    private int readTimeoutMillis = 5000;

    public OidcConfiguration(String issuerUrl) {
        this.issuerUrl = Objects.requireNonNull(issuerUrl, "issuerUrl").replaceAll("/+$", "");
    }

    public String getIssuerUrl() {
        return issuerUrl;
    }

    public String getJwksUri() {
        return jwksUri;
    }

    public OidcConfiguration setJwksUri(String jwksUri) {
        this.jwksUri = jwksUri;
        return this;
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    public OidcConfiguration setTokenEndpoint(String tokenEndpoint) {
        this.tokenEndpoint = tokenEndpoint;
        return this;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public OidcConfiguration setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public OidcConfiguration setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    /**
     * Performs OIDC discovery by fetching {@code <issuer>/.well-known/openid-configuration}
     * and populating {@code jwksUri} and {@code tokenEndpoint} if they have not been
     * explicitly set.
     */
    public OidcConfiguration discover() throws IOException {
        String url = issuerUrl + "/.well-known/openid-configuration";
        JsonNode doc = fetchJson(url);
        if (jwksUri == null) {
            JsonNode n = doc.get("jwks_uri");
            if (n == null || n.isNull()) {
                throw new IOException("Discovery document at " + url + " does not contain jwks_uri");
            }
            jwksUri = n.asText();
        }
        if (tokenEndpoint == null) {
            JsonNode n = doc.get("token_endpoint");
            if (n != null && !n.isNull()) {
                tokenEndpoint = n.asText();
            }
        }
        JsonNode iss = doc.get("issuer");
        if (iss != null && !iss.isNull()) {
            String docIssuer = iss.asText().replaceAll("/+$", "");
            if (!docIssuer.equals(issuerUrl)) {
                throw new IOException("Discovery issuer mismatch: expected " + issuerUrl + " got " + docIssuer);
            }
        }
        return this;
    }

    private JsonNode fetchJson(String url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
        con.setConnectTimeout(connectTimeoutMillis);
        con.setReadTimeout(readTimeoutMillis);
        con.setRequestMethod("GET");
        con.setRequestProperty("Accept", "application/json");
        int status = con.getResponseCode();
        if (status < 200 || status >= 300) {
            throw new IOException("Failed to fetch " + url + " (HTTP " + status + ")");
        }
        try (InputStream in = con.getInputStream()) {
            return MAPPER.readTree(in);
        } finally {
            con.disconnect();
        }
    }

    /**
     * Validates that required fields are populated. Call after {@link #discover()} or after
     * manually setting {@code jwksUri}/{@code tokenEndpoint}.
     */
    public void requireJwksUri() {
        if (jwksUri == null || jwksUri.isEmpty()) {
            throw new IllegalStateException("jwksUri is not set (did you call discover()?)");
        }
    }

    public void requireTokenEndpoint() {
        if (tokenEndpoint == null || tokenEndpoint.isEmpty()) {
            throw new IllegalStateException("tokenEndpoint is not set (did you call discover()?)");
        }
    }

    @Override
    public String toString() {
        return "OidcConfiguration{issuer=" + issuerUrl + ", jwks=" + jwksUri + ", tokenEndpoint=" + tokenEndpoint + '}';
    }
}
