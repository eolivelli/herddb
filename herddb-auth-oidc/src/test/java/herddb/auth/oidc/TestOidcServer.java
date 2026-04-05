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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Embedded OIDC stub: serves /.well-known/openid-configuration, /jwks, and /token.
 * Signs tokens with an RSA key generated at startup.
 */
public final class TestOidcServer implements AutoCloseable {

    private final HttpServer http;
    private final RSAKey rsaJwk;
    private final JWKSet jwkSet;
    private final String baseUrl;
    private final Map<String, String> clients = new HashMap<>();
    final AtomicInteger tokenRequests = new AtomicInteger();
    final AtomicInteger jwksRequests = new AtomicInteger();

    public TestOidcServer() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        java.security.KeyPair kp = kpg.generateKeyPair();
        this.rsaJwk = new RSAKey.Builder((RSAPublicKey) kp.getPublic())
                .privateKey((RSAPrivateKey) kp.getPrivate())
                .keyID(UUID.randomUUID().toString())
                .algorithm(JWSAlgorithm.RS256)
                .build();
        this.jwkSet = new JWKSet(Collections.singletonList(rsaJwk.toPublicJWK()));

        this.http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.baseUrl = "http://127.0.0.1:" + http.getAddress().getPort();
        http.createContext("/.well-known/openid-configuration", new DiscoveryHandler());
        http.createContext("/jwks", new JwksHandler());
        http.createContext("/token", new TokenHandler());
        http.start();
    }

    public String getIssuerUrl() {
        return baseUrl;
    }

    public String getJwksUri() {
        return baseUrl + "/jwks";
    }

    public String getTokenEndpoint() {
        return baseUrl + "/token";
    }

    public void registerClient(String clientId, String clientSecret) {
        clients.put(clientId, clientSecret);
    }

    @Override
    public void close() {
        http.stop(0);
    }

    public String issueToken(String subject, Map<String, Object> extraClaims, long lifetimeSeconds) throws JOSEException {
        return issueToken(subject, extraClaims, lifetimeSeconds, baseUrl);
    }

    public String issueToken(String subject, Map<String, Object> extraClaims, long lifetimeSeconds, String issuer) throws JOSEException {
        long now = System.currentTimeMillis();
        JWTClaimsSet.Builder b = new JWTClaimsSet.Builder()
                .subject(subject)
                .issuer(issuer)
                .issueTime(new Date(now))
                .expirationTime(new Date(now + lifetimeSeconds * 1000L))
                .jwtID(UUID.randomUUID().toString());
        if (extraClaims != null) {
            for (Map.Entry<String, Object> e : extraClaims.entrySet()) {
                b.claim(e.getKey(), e.getValue());
            }
        }
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaJwk.getKeyID()).build(),
                b.build());
        jwt.sign(new RSASSASigner(rsaJwk.toRSAPrivateKey()));
        return jwt.serialize();
    }

    private class DiscoveryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            String body = "{\"issuer\":\"" + baseUrl + "\","
                    + "\"jwks_uri\":\"" + getJwksUri() + "\","
                    + "\"token_endpoint\":\"" + getTokenEndpoint() + "\"}";
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        }
    }

    private class JwksHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            jwksRequests.incrementAndGet();
            byte[] out = jwkSet.toString().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        }
    }

    private class TokenHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            tokenRequests.incrementAndGet();
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                ex.close();
                return;
            }
            String authz = ex.getRequestHeaders().getFirst("Authorization");
            if (authz == null || !authz.startsWith("Basic ")) {
                sendError(ex, 401, "{\"error\":\"invalid_client\"}");
                return;
            }
            byte[] dec = java.util.Base64.getDecoder().decode(authz.substring("Basic ".length()));
            String creds = new String(dec, StandardCharsets.UTF_8);
            int colon = creds.indexOf(':');
            if (colon < 0) {
                sendError(ex, 401, "{\"error\":\"invalid_client\"}");
                return;
            }
            String id = URLDecoder.decode(creds.substring(0, colon), "UTF-8");
            String secret = URLDecoder.decode(creds.substring(colon + 1), "UTF-8");
            String expectedSecret = clients.get(id);
            if (expectedSecret == null || !expectedSecret.equals(secret)) {
                sendError(ex, 401, "{\"error\":\"invalid_client\"}");
                return;
            }
            // read and ignore body (grant_type=client_credentials)
            try (InputStream in = ex.getRequestBody()) {
                byte[] buf = new byte[1024];
                while (in.read(buf) > 0) { }
            }
            try {
                String token = issueToken(id, Collections.<String, Object>singletonMap("preferred_username", id), 300);
                String body = "{\"access_token\":\"" + token + "\",\"token_type\":\"Bearer\",\"expires_in\":300}";
                byte[] out = body.getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, out.length);
                ex.getResponseBody().write(out);
            } catch (Exception e) {
                sendError(ex, 500, "{\"error\":\"server_error\"}");
                return;
            }
            ex.close();
        }

        private void sendError(HttpExchange ex, int status, String body) throws IOException {
            byte[] out = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(status, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        }
    }
}
