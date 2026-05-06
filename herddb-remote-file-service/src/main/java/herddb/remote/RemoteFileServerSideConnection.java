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

package herddb.remote;

import herddb.auth.oidc.OidcAuthException;
import herddb.auth.oidc.OidcTokenValidator;
import herddb.auth.oidc.sasl.OAuthBearerCallback;
import herddb.auth.oidc.sasl.OAuthBearerSaslServer;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.ServerSideConnection;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.remote.admin.RemoteFileServerAdminImpl;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * Per-connection {@link ChannelEventListener} for the file-server Netty
 * transport (issue #425). Each accepted socket gets one of these. It tracks
 * whether the peer has authenticated via OIDC (when OIDC is enabled on the
 * server) and routes inbound PDUs to either the data-plane dispatcher
 * ({@link RemoteFileServiceImpl}) or the admin dispatcher
 * ({@link RemoteFileServerAdminImpl}).
 *
 * <p>When OIDC is disabled the connection is created in an already-authenticated
 * state and SASL handshake PDUs are rejected with an error. When OIDC is
 * enabled the connection is created unauthenticated and only SASL handshake
 * PDUs (TYPE_SASL_TOKEN_MESSAGE_REQUEST / TYPE_SASL_TOKEN_MESSAGE_TOKEN) are
 * accepted until the SASL exchange completes.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServerSideConnection implements ServerSideConnection, ChannelEventListener {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServerSideConnection.class.getName());
    private static final AtomicLong ID_GENERATOR = new AtomicLong();

    private final long id = ID_GENERATOR.incrementAndGet();
    private final Channel channel;
    private final RemoteFileServiceImpl dataDispatcher;
    private final RemoteFileServerAdminImpl adminDispatcher;
    private final OidcTokenValidator oidcValidator;

    private volatile boolean authenticated;
    private volatile SaslServer saslServer;
    private volatile String username = "";

    public RemoteFileServerSideConnection(Channel channel,
                                          RemoteFileServiceImpl dataDispatcher,
                                          RemoteFileServerAdminImpl adminDispatcher,
                                          OidcTokenValidator oidcValidator) {
        this.channel = channel;
        this.channel.setMessagesReceiver(this);
        this.dataDispatcher = dataDispatcher;
        this.adminDispatcher = adminDispatcher;
        this.oidcValidator = oidcValidator;
        // OIDC disabled or in-VM channel → skip auth.
        if (oidcValidator == null || channel.isLocalChannel()) {
            this.authenticated = true;
            this.username = "anonymous";
        }
    }

    @Override
    public long getConnectionId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public void requestReceived(Pdu pdu, Channel channel) {
        boolean releaseSync = true;
        try {
            switch (pdu.type) {
                case Pdu.TYPE_SASL_TOKEN_MESSAGE_REQUEST:
                    handleSaslTokenRequest(pdu, channel);
                    return;
                case Pdu.TYPE_SASL_TOKEN_MESSAGE_TOKEN:
                    handleSaslTokenContinue(pdu, channel);
                    return;
                default:
                    if (!authenticated) {
                        sendAuthRequiredError(pdu.messageId, channel);
                        return;
                    }
                    if (dataDispatcher.handle(pdu, channel)) {
                        releaseSync = false;
                        return;
                    }
                    if (adminDispatcher.handle(pdu, channel)) {
                        releaseSync = false;
                        return;
                    }
                    LOGGER.log(Level.WARNING,
                            "unsupported PDU type {0} from {1}",
                            new Object[]{pdu.type, channel.getRemoteAddress()});
                    channel.sendReplyMessage(pdu.messageId,
                            PduCodec.ErrorResponse.write(pdu.messageId,
                                    "unsupported message type " + pdu.type));
            }
        } finally {
            if (releaseSync) {
                pdu.close();
            }
        }
    }

    @Override
    public void channelClosed(Channel channel) {
        if (!channel.isLocalChannel()) {
            LOGGER.log(Level.FINE, "channelClosed {0}", this);
        }
        disposeSaslServer();
    }

    // ----------------------------------------------------------------------
    // SASL handshake (OAUTHBEARER)
    // ----------------------------------------------------------------------

    private void handleSaslTokenRequest(Pdu pdu, Channel channel) {
        long messageId = pdu.messageId;
        if (oidcValidator == null) {
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "OIDC authentication is disabled on this server");
            channel.sendReplyMessage(messageId, err);
            return;
        }
        try {
            String mech = PduCodec.SaslTokenMessageRequest.readMech(pdu);
            byte[] token = PduCodec.SaslTokenMessageRequest.readToken(pdu);
            if (token == null) {
                token = new byte[0];
            }
            if (!OAuthBearerSaslServer.MECHANISM.equals(mech)) {
                ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                        "Unsupported SASL mechanism '" + mech + "'; only OAUTHBEARER is accepted");
                channel.sendReplyMessage(messageId, err);
                return;
            }
            if (saslServer == null) {
                saslServer = new OAuthBearerSaslServer(new OidcCallbackHandler(oidcValidator));
            }
            byte[] response = saslServer.evaluateResponse(token);
            if (saslServer.isComplete()) {
                username = saslServer.getAuthorizationID();
                authenticated = true;
                LOGGER.log(Level.INFO, "client {0} authenticated as {1}",
                        new Object[]{channel.getRemoteAddress(), username});
                saslServer = null;
            }
            channel.sendReplyMessage(messageId,
                    PduCodec.SaslTokenServerResponse.write(messageId, response));
        } catch (SaslException saslError) {
            // Discard the broken SaslServer so a subsequent
            // TYPE_SASL_TOKEN_MESSAGE_REQUEST on the same connection
            // (e.g. the client retries with a fresher token) starts from a
            // clean state instead of reusing a now-disposed handshake.
            disposeSaslServer();
            LOGGER.log(Level.WARNING, "SASL authentication failed: " + saslError.getMessage());
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "Authentication failed (SASL error)");
            channel.sendReplyMessage(messageId, err);
        } catch (RuntimeException badRequest) {
            // Broad catch: PDU parse can fail on truncated / malformed SASL frames;
            // we must always reply to the client so it can release its callback.
            disposeSaslServer();
            LOGGER.log(Level.WARNING, "SASL initial-token parse failed", badRequest);
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "Authentication failed (bad request: " + badRequest.getMessage() + ")");
            channel.sendReplyMessage(messageId, err);
        }
    }

    private void handleSaslTokenContinue(Pdu pdu, Channel channel) {
        long messageId = pdu.messageId;
        if (saslServer == null) {
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "Authentication failed (SASL protocol error: no in-flight handshake)");
            channel.sendReplyMessage(messageId, err);
            return;
        }
        try {
            byte[] token = PduCodec.SaslTokenMessageToken.readToken(pdu);
            byte[] response = saslServer.evaluateResponse(token == null ? new byte[0] : token);
            if (saslServer.isComplete()) {
                username = saslServer.getAuthorizationID();
                authenticated = true;
                LOGGER.log(Level.INFO, "client {0} authenticated as {1}",
                        new Object[]{channel.getRemoteAddress(), username});
                saslServer = null;
            }
            channel.sendReplyMessage(messageId,
                    PduCodec.SaslTokenServerResponse.write(messageId, response));
        } catch (SaslException saslError) {
            disposeSaslServer();
            LOGGER.log(Level.WARNING, "SASL continuation failed: " + saslError.getMessage());
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "Authentication failed (SASL error)");
            channel.sendReplyMessage(messageId, err);
        } catch (RuntimeException badRequest) {
            // Broad catch: PDU parse can fail on truncated / malformed SASL frames.
            disposeSaslServer();
            LOGGER.log(Level.WARNING, "SASL continuation parse failed", badRequest);
            ByteBuf err = PduCodec.ErrorResponse.write(messageId,
                    "Authentication failed (bad request: " + badRequest.getMessage() + ")");
            channel.sendReplyMessage(messageId, err);
        }
    }

    /**
     * Discards the in-flight {@link SaslServer}. Called after a failed
     * handshake step or on connection close, so a subsequent re-attempt by
     * the same client (or by a retry-style client) starts from a clean
     * state instead of mutating a half-broken state machine.
     */
    private void disposeSaslServer() {
        SaslServer s = this.saslServer;
        if (s != null) {
            try {
                s.dispose();
            } catch (SaslException ignored) {
                // dispose() failures during teardown carry no useful information
            }
            this.saslServer = null;
        }
    }

    private static void sendAuthRequiredError(long messageId, Channel channel) {
        channel.sendReplyMessage(messageId,
                PduCodec.ErrorResponse.write(messageId,
                        "authentication required: OIDC SASL handshake not completed"));
    }

    @Override
    public String toString() {
        return "RemoteFileServerSideConnection{id=" + id + ", remote=" + channel.getRemoteAddress()
                + ", authenticated=" + authenticated + ", username=" + username + "}";
    }

    /**
     * Bridges the SASL OAUTHBEARER {@link OAuthBearerCallback} to the
     * server-side {@link OidcTokenValidator}. The validator returns the
     * principal on success or throws on failure; the callback is then
     * populated accordingly per RFC 7628.
     */
    private static final class OidcCallbackHandler implements CallbackHandler {

        private final OidcTokenValidator validator;

        OidcCallbackHandler(OidcTokenValidator validator) {
            this.validator = validator;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof OAuthBearerCallback) {
                    OAuthBearerCallback bearer = (OAuthBearerCallback) cb;
                    String token = bearer.getToken();
                    try {
                        String principal = validator.validate(token);
                        bearer.setAuthenticatedPrincipal(principal);
                    } catch (OidcAuthException validationError) {
                        bearer.setValidationError(validationError.getMessage());
                    }
                } else {
                    throw new UnsupportedCallbackException(cb,
                            "Unsupported callback for OAUTHBEARER: " + cb.getClass().getName());
                }
            }
        }
    }
}
