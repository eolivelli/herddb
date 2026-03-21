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
import java.nio.charset.StandardCharsets;

/**
 * MySQL HandshakeV10 server greeting packet.
 */
public class HandshakeV10Packet {

    private static final String SERVER_VERSION = "5.7.99-HerdDB";
    private static final String AUTH_PLUGIN_NAME = "mysql_native_password";

    private final int connectionId;
    private final byte[] authChallenge; // full 20-byte challenge

    public HandshakeV10Packet(int connectionId, byte[] authChallenge) {
        if (authChallenge.length != 20) {
            throw new IllegalArgumentException("Auth challenge must be 20 bytes");
        }
        this.connectionId = connectionId;
        this.authChallenge = authChallenge;
    }

    public byte[] getAuthChallenge() {
        return authChallenge;
    }

    public int getConnectionId() {
        return connectionId;
    }

    /**
     * Encode the handshake packet including the 4-byte MySQL packet header.
     */
    public ByteBuf encode(ByteBufAllocator alloc) {
        ByteBuf payload = alloc.buffer();
        try {
            // protocol version
            payload.writeByte(10);

            // server version (null-terminated)
            payload.writeBytes(SERVER_VERSION.getBytes(StandardCharsets.UTF_8));
            payload.writeByte(0);

            // connection id (4 bytes LE)
            payload.writeIntLE(connectionId);

            // auth-plugin-data-part-1 (8 bytes)
            payload.writeBytes(authChallenge, 0, 8);

            // filler
            payload.writeByte(0x00);

            int capabilityFlags = CapabilityFlags.CLIENT_LONG_PASSWORD
                    | CapabilityFlags.CLIENT_FOUND_ROWS
                    | CapabilityFlags.CLIENT_LONG_FLAG
                    | CapabilityFlags.CLIENT_CONNECT_WITH_DB
                    | CapabilityFlags.CLIENT_PROTOCOL_41
                    | CapabilityFlags.CLIENT_TRANSACTIONS
                    | CapabilityFlags.CLIENT_SECURE_CONNECTION
                    | CapabilityFlags.CLIENT_PLUGIN_AUTH;

            // capability flags lower 2 bytes
            payload.writeShortLE(capabilityFlags & 0xFFFF);

            // character set
            payload.writeByte(CharacterSets.UTF8MB4_GENERAL_CI);

            // status flags
            payload.writeShortLE(ServerStatusFlags.SERVER_STATUS_AUTOCOMMIT);

            // capability flags upper 2 bytes
            payload.writeShortLE((capabilityFlags >> 16) & 0xFFFF);

            // auth plugin data length = SCRAMBLE_LENGTH (20)
            // The client reads (auth_plugin_data_len - 8) bytes for part-2 = 12 bytes
            // This matches the real MySQL server behavior
            payload.writeByte(20);

            // reserved 10 bytes
            payload.writeZero(10);

            // auth-plugin-data-part-2 (12 bytes + null terminator)
            payload.writeBytes(authChallenge, 8, 12);
            payload.writeByte(0x00);

            // auth plugin name (null-terminated)
            payload.writeBytes(AUTH_PLUGIN_NAME.getBytes(StandardCharsets.UTF_8));
            payload.writeByte(0);

            // Now wrap with the 4-byte header
            int payloadLength = payload.readableBytes();
            ByteBuf packet = alloc.buffer(4 + payloadLength);
            packet.writeMediumLE(payloadLength);
            packet.writeByte(0); // sequence id = 0 for initial handshake
            packet.writeBytes(payload);
            return packet;
        } finally {
            payload.release();
        }
    }
}
