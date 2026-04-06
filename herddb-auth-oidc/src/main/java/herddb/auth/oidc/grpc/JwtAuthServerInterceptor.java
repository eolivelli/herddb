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
package herddb.auth.oidc.grpc;

import herddb.auth.oidc.sasl.TokenAuthenticator;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * gRPC server interceptor that validates a bearer token from the {@code authorization}
 * metadata header, closes the call with {@link Status#UNAUTHENTICATED} on failure,
 * and otherwise propagates the resolved principal via {@link #PRINCIPAL_CONTEXT_KEY}.
 */
public final class JwtAuthServerInterceptor implements ServerInterceptor {

    private static final Logger LOGGER = Logger.getLogger(JwtAuthServerInterceptor.class.getName());

    public static final Metadata.Key<String> AUTHORIZATION_HEADER =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    public static final Context.Key<String> PRINCIPAL_CONTEXT_KEY = Context.key("herddb.principal");

    private static final String BEARER_PREFIX = "Bearer ";

    private final TokenAuthenticator authenticator;

    public JwtAuthServerInterceptor(TokenAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {
        String authz = headers.get(AUTHORIZATION_HEADER);
        if (authz == null || authz.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("missing authorization header"), new Metadata());
            return new ServerCall.Listener<ReqT>() { };
        }
        if (!authz.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
            call.close(Status.UNAUTHENTICATED.withDescription("expected Bearer scheme"), new Metadata());
            return new ServerCall.Listener<ReqT>() { };
        }
        String token = authz.substring(BEARER_PREFIX.length()).trim();
        String principal;
        try {
            principal = authenticator.authenticate(token);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "rejected token: {0}", e.getMessage());
            call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), new Metadata());
            return new ServerCall.Listener<ReqT>() { };
        }
        Context ctx = Context.current().withValue(PRINCIPAL_CONTEXT_KEY, principal);
        return Contexts.interceptCall(ctx, call, headers, next);
    }
}
