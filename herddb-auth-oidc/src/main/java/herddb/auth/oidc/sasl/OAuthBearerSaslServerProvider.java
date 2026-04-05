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

import java.security.Provider;
import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

/**
 * JVM SASL provider for the OAUTHBEARER server mechanism.
 * Call {@link #install()} once at startup.
 */
public final class OAuthBearerSaslServerProvider extends Provider {

    private static final long serialVersionUID = 1L;
    private static volatile boolean installed = false;

    public OAuthBearerSaslServerProvider() {
        super("SASL/OAUTHBEARER Server Provider", 1.0, "SASL/OAUTHBEARER Server Provider for HerdDB");
        put("SaslServerFactory." + OAuthBearerSaslServer.MECHANISM, Factory.class.getName());
    }

    public static synchronized void install() {
        if (!installed) {
            java.security.Security.addProvider(new OAuthBearerSaslServerProvider());
            installed = true;
        }
    }

    public static final class Factory implements SaslServerFactory {
        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
                                           Map<String, ?> props, CallbackHandler cbh) {
            if (!OAuthBearerSaslServer.MECHANISM.equals(mechanism)) {
                return null;
            }
            return new OAuthBearerSaslServer(cbh);
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            return new String[]{OAuthBearerSaslServer.MECHANISM};
        }
    }
}
