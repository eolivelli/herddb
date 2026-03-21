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
 * MySQL ERR packet.
 */
public class ErrPacket {

    private final int errorCode;
    private final String sqlState;
    private final String message;

    public ErrPacket(int errorCode, String sqlState, String message) {
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.message = message;
    }

    /**
     * Encode the ERR packet including the 4-byte MySQL packet header.
     */
    public ByteBuf encode(ByteBufAllocator alloc, int sequenceId) {
        ByteBuf payload = alloc.buffer();
        try {
            // header
            payload.writeByte(0xFF);
            // error code (2 bytes LE)
            payload.writeShortLE(errorCode);
            // sql state marker
            payload.writeByte('#');
            // sql state (5 bytes ASCII)
            byte[] stateBytes = sqlState.getBytes(StandardCharsets.US_ASCII);
            payload.writeBytes(stateBytes, 0, Math.min(5, stateBytes.length));
            // pad if less than 5
            for (int i = stateBytes.length; i < 5; i++) {
                payload.writeByte(' ');
            }
            // error message
            payload.writeBytes(message.getBytes(StandardCharsets.UTF_8));

            int payloadLength = payload.readableBytes();
            ByteBuf packet = alloc.buffer(4 + payloadLength);
            packet.writeMediumLE(payloadLength);
            packet.writeByte(sequenceId);
            packet.writeBytes(payload);
            return packet;
        } finally {
            payload.release();
        }
    }
}
