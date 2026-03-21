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

package herddb.mysql.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Netty channel handler implementing the server side of the MySQL protocol state machine.
 */
public class MySQLConnectionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(MySQLConnectionHandler.class.getName());
    private static final AtomicLong CONNECTION_ID_GENERATOR = new AtomicLong(1);

    private static final int COM_QUIT = 0x01;
    private static final int COM_INIT_DB = 0x02;
    private static final int COM_QUERY = 0x03;
    private static final int COM_PING = 0x0E;

    private enum State {
        HANDSHAKE_SENT,
        AUTHENTICATING,
        COMMAND_PHASE
    }

    private final MySQLCommandHandler commandHandler;
    private final SecureRandom random = new SecureRandom();

    private State state;
    private long connectionId;
    private byte[] authChallenge;

    public MySQLConnectionHandler(MySQLCommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        connectionId = CONNECTION_ID_GENERATOR.getAndIncrement();
        authChallenge = new byte[20];
        random.nextBytes(authChallenge);

        // Challenge bytes must be in the range 0x01-0x7F.
        // MySQL Connector/J reads the seed as an ASCII String, which corrupts bytes > 0x7F.
        // Also avoid 0x00 which acts as null terminator in wire protocol.
        for (int i = 0; i < authChallenge.length; i++) {
            authChallenge[i] = (byte) ((authChallenge[i] & 0x7F) | 0x01);
        }

        HandshakeV10Packet handshake = new HandshakeV10Packet((int) connectionId, authChallenge);
        ByteBuf packet = handshake.encode(ctx.alloc());
        ctx.writeAndFlush(packet);
        state = State.AUTHENTICATING;

        LOG.log(Level.FINE, "Connection {0} active, handshake sent", connectionId);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MySQLRawPacket rawPacket = (MySQLRawPacket) msg;
        ByteBuf payload = rawPacket.getPayload();
        try {
            switch (state) {
                case AUTHENTICATING:
                    handleHandshakeResponse(ctx, rawPacket);
                    break;
                case COMMAND_PHASE:
                    handleCommand(ctx, rawPacket);
                    break;
                default:
                    LOG.log(Level.WARNING, "Unexpected packet in state {0}", state);
                    ctx.close();
                    break;
            }
        } finally {
            payload.release();
        }
    }

    private void handleHandshakeResponse(ChannelHandlerContext ctx, MySQLRawPacket rawPacket) {
        ByteBuf payload = rawPacket.getPayload();
        int seqId = rawPacket.getSequenceId();

        // capability flags (4 bytes LE)
        int clientCapabilities = payload.readIntLE();
        // max packet size (4 bytes LE)
        payload.readIntLE();
        // character set (1 byte)
        payload.readByte();
        // reserved (23 bytes)
        payload.skipBytes(23);

        // username (null-terminated)
        String username = MySQLBufUtils.readNullTerminatedString(payload, StandardCharsets.UTF_8);

        // auth response
        byte[] authResponse;
        if ((clientCapabilities & CapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
            long len = MySQLBufUtils.readLengthEncodedInt(payload);
            authResponse = new byte[(int) len];
            payload.readBytes(authResponse);
        } else if ((clientCapabilities & CapabilityFlags.CLIENT_SECURE_CONNECTION) != 0) {
            int len = payload.readUnsignedByte();
            authResponse = new byte[len];
            payload.readBytes(authResponse);
        } else {
            // null-terminated
            authResponse = readNullTerminatedBytes(payload);
        }

        // database (if CLIENT_CONNECT_WITH_DB)
        String database = null;
        if ((clientCapabilities & CapabilityFlags.CLIENT_CONNECT_WITH_DB) != 0 && payload.isReadable()) {
            database = MySQLBufUtils.readNullTerminatedString(payload, StandardCharsets.UTF_8);
        }

        // auth plugin name (if CLIENT_PLUGIN_AUTH)
        String authPluginName = null;
        if ((clientCapabilities & CapabilityFlags.CLIENT_PLUGIN_AUTH) != 0 && payload.isReadable()) {
            authPluginName = MySQLBufUtils.readNullTerminatedString(payload, StandardCharsets.UTF_8);
        }

        LOG.log(Level.FINE, "Auth attempt: user={0}, plugin={1}", new Object[]{username, authPluginName});
        boolean authenticated = commandHandler.authenticate(username, authResponse, authChallenge, database);
        if (authenticated) {
            state = State.COMMAND_PHASE;
            OkPacket ok = new OkPacket(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT, 0);
            ctx.writeAndFlush(ok.encode(ctx.alloc(), seqId + 1));
            LOG.log(Level.FINE, "Connection {0} authenticated as {1}", new Object[]{connectionId, username});
        } else {
            ErrPacket err = new ErrPacket(1045, "28000", "Access denied for user '" + username + "'");
            ctx.writeAndFlush(err.encode(ctx.alloc(), seqId + 1));
            ctx.close();
        }
    }

    private void handleCommand(ChannelHandlerContext ctx, MySQLRawPacket rawPacket) {
        ByteBuf payload = rawPacket.getPayload();
        int seqId = rawPacket.getSequenceId();
        int commandType = payload.readUnsignedByte();

        switch (commandType) {
            case COM_QUIT:
                ctx.close();
                break;
            case COM_PING:
                OkPacket pingOk = new OkPacket(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT, 0);
                ctx.writeAndFlush(pingOk.encode(ctx.alloc(), seqId + 1));
                break;
            case COM_INIT_DB: {
                String database = payload.readCharSequence(payload.readableBytes(), StandardCharsets.UTF_8).toString();
                commandHandler.useDatabase(connectionId, database);
                OkPacket ok = new OkPacket(0, 0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT, 0);
                ctx.writeAndFlush(ok.encode(ctx.alloc(), seqId + 1));
                break;
            }
            case COM_QUERY: {
                String sql = payload.readCharSequence(payload.readableBytes(), StandardCharsets.UTF_8).toString();
                LOG.log(Level.FINE, "Connection {0} query: {1}", new Object[]{connectionId, sql});
                commandHandler.executeQuery(connectionId, sql).whenComplete((result, error) -> {
                    if (error != null) {
                        LOG.log(Level.WARNING, "Query error on connection " + connectionId, error);
                        ErrPacket err = new ErrPacket(1064, "42000", error.getMessage());
                        ctx.writeAndFlush(err.encode(ctx.alloc(), seqId + 1));
                    } else {
                        writeQueryResult(ctx, seqId, result);
                    }
                });
                break;
            }
            default:
                ErrPacket err = new ErrPacket(1047, "08S01", "Unknown command: " + commandType);
                ctx.writeAndFlush(err.encode(ctx.alloc(), seqId + 1));
                break;
        }
    }

    private void writeQueryResult(ChannelHandlerContext ctx, int clientSeqId, QueryResult result) {
        ByteBufAllocator alloc = ctx.alloc();
        int nextSeqId = clientSeqId + 1;

        if (result instanceof OkResult) {
            OkResult ok = (OkResult) result;
            OkPacket packet = new OkPacket(ok.getAffectedRows(), ok.getLastInsertId(),
                    ok.getServerStatus(), 0);
            ctx.writeAndFlush(packet.encode(alloc, nextSeqId));
        } else if (result instanceof ErrorResult) {
            ErrorResult err = (ErrorResult) result;
            ErrPacket packet = new ErrPacket(err.getErrorCode(), err.getSqlState(), err.getMessage());
            ctx.writeAndFlush(packet.encode(alloc, nextSeqId));
        } else if (result instanceof ResultSetResponse) {
            ResultSetResponse rs = (ResultSetResponse) result;
            int seqId = nextSeqId;

            // Column count packet
            ByteBuf colCountPayload = alloc.buffer();
            MySQLBufUtils.writeLengthEncodedInt(colCountPayload, rs.getColumns().size());
            int colCountLen = colCountPayload.readableBytes();
            ByteBuf colCountPacket = alloc.buffer(4 + colCountLen);
            colCountPacket.writeMediumLE(colCountLen);
            colCountPacket.writeByte(seqId++);
            colCountPacket.writeBytes(colCountPayload);
            colCountPayload.release();
            ctx.write(colCountPacket);

            // Column definition packets
            for (ColumnDef col : rs.getColumns()) {
                ColumnDefinition41Packet colDef = new ColumnDefinition41Packet(
                        col.getSchema(), col.getTable(), col.getTable(),
                        col.getName(), col.getName(),
                        CharacterSets.UTF8MB4_GENERAL_CI, col.getColumnLength(),
                        col.getColumnType(), 0, 0);
                ctx.write(colDef.encode(alloc, seqId++));
            }

            // EOF after column definitions
            EofPacket colEof = new EofPacket(0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT);
            ctx.write(colEof.encode(alloc, seqId++));

            // Row packets
            for (Object[] row : rs.getRows()) {
                ctx.write(ResultsetRowPacket.encode(alloc, seqId++, row));
            }

            // EOF after rows
            EofPacket rowEof = new EofPacket(0, ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT);
            ctx.writeAndFlush(rowEof.encode(alloc, seqId));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.log(Level.FINE, "Connection {0} closed", connectionId);
        commandHandler.connectionClosed(connectionId);
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.log(Level.WARNING, "Exception on connection " + connectionId, cause);
        ctx.close();
    }

    private static byte[] readNullTerminatedBytes(ByteBuf buf) {
        int start = buf.readerIndex();
        while (buf.isReadable() && buf.readByte() != 0) {
            // advance
        }
        int length = buf.readerIndex() - start - 1;
        byte[] bytes = new byte[length];
        buf.readerIndex(start);
        buf.readBytes(bytes);
        if (buf.isReadable()) {
            buf.readByte(); // skip null terminator
        }
        return bytes;
    }
}
