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
 * MySQL text result set row packet.
 */
public class ResultsetRowPacket {

    private ResultsetRowPacket() {
    }

    /**
     * Encode a result set row packet including the 4-byte MySQL packet header.
     * Each value is encoded as a length-encoded string, or 0xFB for NULL.
     */
    public static ByteBuf encode(ByteBufAllocator alloc, int sequenceId, Object[] values) {
        ByteBuf payload = alloc.buffer();
        try {
            for (Object value : values) {
                if (value == null) {
                    payload.writeByte(0xFB);
                } else {
                    MySQLBufUtils.writeLengthEncodedString(payload, value.toString(), StandardCharsets.UTF_8);
                }
            }

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
