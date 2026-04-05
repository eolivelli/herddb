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

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jose.util.ResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Validates JWT bearer tokens against an OIDC provider's JWKS.
 * <p>
 * Verification checks:
 * <ul>
 *     <li>Token is a signed JWT (RS256/RS384/RS512/ES256 etc)</li>
 *     <li>Signature matches a key in the provider's JWKS (cached, auto-refreshing)</li>
 *     <li>{@code iss} matches the configured issuer</li>
 *     <li>{@code exp} is in the future (with configurable clock skew)</li>
 *     <li>{@code aud} contains the expected audience, if configured</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public final class OidcTokenValidator {

    private final String issuer;
    private final String expectedAudience;
    private final PrincipalExtractor principalExtractor;
    private final ConfigurableJWTProcessor<SecurityContext> processor;

    public OidcTokenValidator(OidcConfiguration config,
                              String expectedAudience,
                              PrincipalExtractor principalExtractor) {
        config.requireJwksUri();
        this.issuer = Objects.requireNonNull(config.getIssuerUrl(), "issuerUrl");
        this.expectedAudience = expectedAudience;
        this.principalExtractor = principalExtractor == null
                ? new PrincipalExtractor() : principalExtractor;

        URL jwksUrl;
        try {
            jwksUrl = new URL(config.getJwksUri());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid JWKS URL: " + config.getJwksUri(), e);
        }
        ResourceRetriever retriever = new DefaultResourceRetriever(
                config.getConnectTimeoutMillis(),
                config.getReadTimeoutMillis(),
                50 * 1024);
        JWKSource<SecurityContext> jwkSource = new RemoteJWKSet<>(jwksUrl, retriever);

        this.processor = new DefaultJWTProcessor<>();
        // accept common asymmetric algorithms
        Set<JWSAlgorithm> algorithms = new LinkedHashSet<>();
        algorithms.add(JWSAlgorithm.RS256);
        algorithms.add(JWSAlgorithm.RS384);
        algorithms.add(JWSAlgorithm.RS512);
        algorithms.add(JWSAlgorithm.PS256);
        algorithms.add(JWSAlgorithm.PS384);
        algorithms.add(JWSAlgorithm.PS512);
        algorithms.add(JWSAlgorithm.ES256);
        algorithms.add(JWSAlgorithm.ES384);
        algorithms.add(JWSAlgorithm.ES512);
        this.processor.setJWSKeySelector(new JWSVerificationKeySelector<>(algorithms, jwkSource));

        Set<String> requiredClaims = new HashSet<>();
        requiredClaims.add("exp");
        requiredClaims.add("iss");
        JWTClaimsSet.Builder exactMatch = new JWTClaimsSet.Builder().issuer(issuer);
        Set<String> acceptedAudience = expectedAudience == null
                ? null : Collections.singleton(expectedAudience);
        this.processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<SecurityContext>(
                acceptedAudience,
                exactMatch.build(),
                requiredClaims,
                Collections.<String>emptySet()));
    }

    /**
     * Validates the given JWT compact serialization and returns the resolved principal.
     *
     * @param token the JWT (compact form)
     * @return the extracted principal (never null)
     * @throws OidcAuthException on any validation failure
     */
    public String validate(String token) throws OidcAuthException {
        if (token == null || token.isEmpty()) {
            throw new OidcAuthException("empty token");
        }
        try {
            JWTClaimsSet claims = processor.process(token, null);
            String principal = principalExtractor.extract(claims);
            if (principal == null || principal.isEmpty()) {
                throw new OidcAuthException("no username claim (looked for '"
                        + principalExtractor.getPrimaryClaim() + "', preferred_username, sub)");
            }
            return principal;
        } catch (ParseException e) {
            throw new OidcAuthException("malformed JWT: " + e.getMessage(), e);
        } catch (OidcAuthException e) {
            throw e;
        } catch (Exception e) {
            throw new OidcAuthException("JWT validation failed: " + e.getMessage(), e);
        }
    }

    public String getIssuer() {
        return issuer;
    }

    public String getExpectedAudience() {
        return expectedAudience;
    }
}
