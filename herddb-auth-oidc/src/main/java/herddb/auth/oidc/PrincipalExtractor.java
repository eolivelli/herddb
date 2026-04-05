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

import com.nimbusds.jwt.JWTClaimsSet;

/**
 * Extracts a HerdDB principal (username) from validated JWT claims.
 * By default tries the configured claim, then falls back to {@code preferred_username}
 * and then {@code sub}.
 *
 * @author enrico.olivelli
 */
public final class PrincipalExtractor {

    public static final String DEFAULT_USERNAME_CLAIM = "preferred_username";

    private final String primaryClaim;

    public PrincipalExtractor(String primaryClaim) {
        this.primaryClaim = primaryClaim == null || primaryClaim.isEmpty()
                ? DEFAULT_USERNAME_CLAIM : primaryClaim;
    }

    public PrincipalExtractor() {
        this(DEFAULT_USERNAME_CLAIM);
    }

    public String extract(JWTClaimsSet claims) {
        String value = stringClaim(claims, primaryClaim);
        if (value != null) {
            return value;
        }
        if (!DEFAULT_USERNAME_CLAIM.equals(primaryClaim)) {
            value = stringClaim(claims, DEFAULT_USERNAME_CLAIM);
            if (value != null) {
                return value;
            }
        }
        return claims.getSubject();
    }

    private static String stringClaim(JWTClaimsSet claims, String name) {
        try {
            Object v = claims.getClaim(name);
            if (v instanceof String) {
                String s = (String) v;
                return s.isEmpty() ? null : s;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public String getPrimaryClaim() {
        return primaryClaim;
    }
}
