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
import com.nimbusds.jwt.JWTParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Acquires JWT access tokens via the OAuth2 client_credentials grant and caches them
 * until shortly before expiry.
 * <p>
 * Thread-safe. Concurrent callers while a refresh is in progress may trigger at most a
 * single network call; subsequent callers wait on the shared in-flight refresh.
 *
 * @author enrico.olivelli
 */
public final class OidcTokenProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long REFRESH_SKEW_SECONDS = 30;

    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    private final String scope;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;

    private final AtomicReference<CachedToken> cache = new AtomicReference<>();
    private final Object refreshLock = new Object();

    public OidcTokenProvider(OidcConfiguration config, String clientId, String clientSecret, String scope) {
        config.requireTokenEndpoint();
        this.tokenEndpoint = config.getTokenEndpoint();
        this.clientId = Objects.requireNonNull(clientId, "clientId");
        this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret");
        this.scope = scope;
        this.connectTimeoutMillis = config.getConnectTimeoutMillis();
        this.readTimeoutMillis = config.getReadTimeoutMillis();
    }

    /**
     * Returns a currently-valid access token. Fetches a new one if there is no cached token
     * or the cached token is within {@value #REFRESH_SKEW_SECONDS} seconds of expiry.
     */
    public String getToken() throws OidcAuthException {
        CachedToken current = cache.get();
        long nowSeconds = Instant.now().getEpochSecond();
        if (current != null && current.expiresAtEpochSeconds - REFRESH_SKEW_SECONDS > nowSeconds) {
            return current.token;
        }
        synchronized (refreshLock) {
            // double-check after acquiring the lock
            current = cache.get();
            nowSeconds = Instant.now().getEpochSecond();
            if (current != null && current.expiresAtEpochSeconds - REFRESH_SKEW_SECONDS > nowSeconds) {
                return current.token;
            }
            CachedToken refreshed = fetchToken();
            cache.set(refreshed);
            return refreshed.token;
        }
    }

    /**
     * Forces a refresh on next {@link #getToken()} call by invalidating the cache.
     * Useful after the server reports an authentication failure.
     */
    public void invalidate() {
        cache.set(null);
    }

    private CachedToken fetchToken() throws OidcAuthException {
        try {
            HttpURLConnection con = (HttpURLConnection) new URL(tokenEndpoint).openConnection();
            con.setConnectTimeout(connectTimeoutMillis);
            con.setReadTimeout(readTimeoutMillis);
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            con.setRequestProperty("Accept", "application/json");
            String basic = Base64.getEncoder().encodeToString(
                    (urlEncode(clientId) + ":" + urlEncode(clientSecret)).getBytes(StandardCharsets.UTF_8));
            con.setRequestProperty("Authorization", "Basic " + basic);

            StringBuilder body = new StringBuilder();
            body.append("grant_type=client_credentials");
            if (scope != null && !scope.isEmpty()) {
                body.append("&scope=").append(urlEncode(scope));
            }
            byte[] payload = body.toString().getBytes(StandardCharsets.UTF_8);
            try (OutputStream out = con.getOutputStream()) {
                out.write(payload);
            }
            int status = con.getResponseCode();
            if (status < 200 || status >= 300) {
                String err = readError(con);
                throw new OidcAuthException("token endpoint returned HTTP " + status + ": " + err);
            }
            JsonNode json;
            try (InputStream in = con.getInputStream()) {
                json = MAPPER.readTree(in);
            }
            JsonNode accessToken = json.get("access_token");
            if (accessToken == null || accessToken.isNull()) {
                throw new OidcAuthException("token endpoint response missing access_token");
            }
            String token = accessToken.asText();
            long expiresAt = computeExpiresAt(json, token);
            return new CachedToken(token, expiresAt);
        } catch (OidcAuthException e) {
            throw e;
        } catch (IOException e) {
            throw new OidcAuthException("failed to fetch token from " + tokenEndpoint + ": " + e.getMessage(), e);
        }
    }

    private static long computeExpiresAt(JsonNode json, String jwt) {
        // Prefer expires_in from token endpoint response
        JsonNode expiresIn = json.get("expires_in");
        long now = Instant.now().getEpochSecond();
        if (expiresIn != null && expiresIn.isNumber()) {
            return now + expiresIn.asLong();
        }
        // Fall back to exp claim in the JWT itself
        try {
            Date exp = JWTParser.parse(jwt).getJWTClaimsSet().getExpirationTime();
            if (exp != null) {
                return exp.toInstant().getEpochSecond();
            }
        } catch (Exception ignored) {
            // ignore - we'll assume a very short lifetime
        }
        return now + 60;
    }

    private static String urlEncode(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String readError(HttpURLConnection con) {
        try (InputStream es = con.getErrorStream()) {
            if (es == null) {
                return "(no body)";
            }
            byte[] buf = new byte[2048];
            int read = es.read(buf);
            return read <= 0 ? "(empty)" : new String(buf, 0, read, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "(unreadable: " + e.getMessage() + ")";
        }
    }

    private static final class CachedToken {
        final String token;
        final long expiresAtEpochSeconds;

        CachedToken(String token, long expiresAtEpochSeconds) {
            this.token = token;
            this.expiresAtEpochSeconds = expiresAtEpochSeconds;
        }
    }
}
