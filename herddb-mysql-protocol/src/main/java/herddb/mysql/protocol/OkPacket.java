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

/**
 * MySQL OK packet.
 */
public class OkPacket {

    private final long affectedRows;
    private final long lastInsertId;
    private final int statusFlags;
    private final int warnings;

    public OkPacket(long affectedRows, long lastInsertId, int statusFlags, int warnings) {
        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.statusFlags = statusFlags;
        this.warnings = warnings;
    }

    /**
     * Encode the OK packet including the 4-byte MySQL packet header.
     */
    public ByteBuf encode(ByteBufAllocator alloc, int sequenceId) {
        ByteBuf payload = alloc.buffer();
        try {
            // header
            payload.writeByte(0x00);
            // affected rows
            MySQLBufUtils.writeLengthEncodedInt(payload, affectedRows);
            // last insert id
            MySQLBufUtils.writeLengthEncodedInt(payload, lastInsertId);
            // status flags (2 bytes LE)
            payload.writeShortLE(statusFlags);
            // warnings (2 bytes LE)
            payload.writeShortLE(warnings);

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
