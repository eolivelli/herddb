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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.auth.oidc.sasl.TokenAuthenticator;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class JwtAuthInterceptorTest {

    private static final MethodDescriptor<String, String> METHOD = MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test/Method")
            .setRequestMarshaller(new StringMarshaller())
            .setResponseMarshaller(new StringMarshaller())
            .build();

    static final class StringMarshaller implements MethodDescriptor.Marshaller<String> {
        @Override
        public java.io.InputStream stream(String value) {
            return new java.io.ByteArrayInputStream(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        @Override
        public String parse(java.io.InputStream stream) {
            try {
                byte[] buf = new byte[1024];
                int n = stream.read(buf);
                return n <= 0 ? "" : new String(buf, 0, n, java.nio.charset.StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void clientInterceptorAttachesBearerHeader() {
        AtomicReference<String> capturedAuthz = new AtomicReference<>();
        io.grpc.Channel channel = new io.grpc.Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<ReqT, RespT>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        capturedAuthz.set(headers.get(JwtAuthServerInterceptor.AUTHORIZATION_HEADER));
                    }
                    @Override public void request(int numMessages) { }
                    @Override public void cancel(String message, Throwable cause) { }
                    @Override public void halfClose() { }
                    @Override public void sendMessage(ReqT message) { }
                };
            }
            @Override
            public String authority() {
                return "test";
            }
        };
        JwtAuthClientInterceptor interceptor = new JwtAuthClientInterceptor(() -> "mytoken");
        ClientCall<String, String> call = interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channel);
        call.start(new ClientCall.Listener<String>() { }, new Metadata());
        assertEquals("Bearer mytoken", capturedAuthz.get());
    }

    @Test
    public void clientInterceptorSkipsEmptyToken() {
        AtomicReference<String> capturedAuthz = new AtomicReference<>("not-set");
        io.grpc.Channel channel = new io.grpc.Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
                return new ClientCall<ReqT, RespT>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        capturedAuthz.set(headers.get(JwtAuthServerInterceptor.AUTHORIZATION_HEADER));
                    }
                    @Override public void request(int numMessages) { }
                    @Override public void cancel(String message, Throwable cause) { }
                    @Override public void halfClose() { }
                    @Override public void sendMessage(ReqT message) { }
                };
            }
            @Override
            public String authority() {
                return "test";
            }
        };
        ClientCall<String, String> call = new JwtAuthClientInterceptor(() -> "")
                .interceptCall(METHOD, CallOptions.DEFAULT, channel);
        call.start(new ClientCall.Listener<String>() { }, new Metadata());
        assertTrue(capturedAuthz.get() == null || "not-set".equals(capturedAuthz.get()));
    }

    @Test
    public void serverInterceptorRejectsMissingHeader() {
        RecordingServerCall call = new RecordingServerCall();
        JwtAuthServerInterceptor interceptor = new JwtAuthServerInterceptor(token -> "user");
        interceptor.interceptCall(call, new Metadata(), failingHandler());
        assertNotNull(call.closedStatus);
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
    }

    @Test
    public void serverInterceptorRejectsWrongScheme() {
        Metadata md = new Metadata();
        md.put(JwtAuthServerInterceptor.AUTHORIZATION_HEADER, "Basic foo");
        RecordingServerCall call = new RecordingServerCall();
        JwtAuthServerInterceptor interceptor = new JwtAuthServerInterceptor(token -> "user");
        interceptor.interceptCall(call, md, failingHandler());
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(call.closedStatus.getDescription().toLowerCase().contains("bearer"));
    }

    @Test
    public void serverInterceptorRejectsBadToken() {
        Metadata md = new Metadata();
        md.put(JwtAuthServerInterceptor.AUTHORIZATION_HEADER, "Bearer bad-token");
        RecordingServerCall call = new RecordingServerCall();
        TokenAuthenticator auth = token -> {
            throw new IOException("invalid");
        };
        JwtAuthServerInterceptor interceptor = new JwtAuthServerInterceptor(auth);
        interceptor.interceptCall(call, md, failingHandler());
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
    }

    @Test
    public void serverInterceptorPassesValidToken() {
        Metadata md = new Metadata();
        md.put(JwtAuthServerInterceptor.AUTHORIZATION_HEADER, "Bearer good-token");
        RecordingServerCall call = new RecordingServerCall();
        JwtAuthServerInterceptor interceptor = new JwtAuthServerInterceptor(token -> "alice");
        AtomicReference<String> principalSeen = new AtomicReference<>();
        ServerCallHandler<String, String> handler = (c, h) -> {
            principalSeen.set(JwtAuthServerInterceptor.PRINCIPAL_CONTEXT_KEY.get());
            return new ServerCall.Listener<String>() { };
        };
        interceptor.interceptCall(call, md, handler);
        assertTrue("call should not have been closed with error", call.closedStatus == null);
        assertEquals("alice", principalSeen.get());
    }

    private static ServerCallHandler<String, String> failingHandler() {
        return (c, h) -> {
            fail("handler should not be invoked");
            throw new StatusRuntimeException(Status.INTERNAL);
        };
    }

    private static final class RecordingServerCall extends ServerCall<String, String> {
        Status closedStatus;

        @Override public void request(int numMessages) { }
        @Override public void sendHeaders(Metadata headers) { }
        @Override public void sendMessage(String message) { }
        @Override public boolean isReady() {
            return false;
        }
        @Override public boolean isCancelled() {
            return false;
        }
        @Override public MethodDescriptor<String, String> getMethodDescriptor() {
            return METHOD;
        }
        @Override public void close(Status status, Metadata trailers) {
            this.closedStatus = status;
        }
    }
}
