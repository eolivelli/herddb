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

import javax.security.auth.callback.Callback;

/**
 * SASL callback used by {@link OAuthBearerSaslServer}. The callback is filled in with the
 * token received from the client; the callback handler must validate the token and set the
 * resolved principal, or set {@link #setValidationError(String)} to reject the request.
 */
public final class OAuthBearerCallback implements Callback {

    private String token;
    private String authenticatedPrincipal;
    private String validationError;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getAuthenticatedPrincipal() {
        return authenticatedPrincipal;
    }

    public void setAuthenticatedPrincipal(String authenticatedPrincipal) {
        this.authenticatedPrincipal = authenticatedPrincipal;
    }

    public String getValidationError() {
        return validationError;
    }

    public void setValidationError(String validationError) {
        this.validationError = validationError;
    }
}
