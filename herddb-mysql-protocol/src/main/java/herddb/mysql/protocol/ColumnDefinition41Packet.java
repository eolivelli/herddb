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
 * MySQL Column Definition (Protocol::ColumnDefinition41) packet.
 */
public class ColumnDefinition41Packet {

    private final String schema;
    private final String table;
    private final String orgTable;
    private final String name;
    private final String orgName;
    private final int characterSet;
    private final int columnLength;
    private final MySQLColumnType columnType;
    private final int flags;
    private final int decimals;

    public ColumnDefinition41Packet(String schema, String table, String orgTable,
                                     String name, String orgName,
                                     int characterSet, int columnLength,
                                     MySQLColumnType columnType, int flags, int decimals) {
        this.schema = schema;
        this.table = table;
        this.orgTable = orgTable;
        this.name = name;
        this.orgName = orgName;
        this.characterSet = characterSet;
        this.columnLength = columnLength;
        this.columnType = columnType;
        this.flags = flags;
        this.decimals = decimals;
    }

    /**
     * Encode the column definition packet including the 4-byte MySQL packet header.
     */
    public ByteBuf encode(ByteBufAllocator alloc, int sequenceId) {
        ByteBuf payload = alloc.buffer();
        try {
            // catalog = "def"
            MySQLBufUtils.writeLengthEncodedString(payload, "def", StandardCharsets.UTF_8);
            // schema
            MySQLBufUtils.writeLengthEncodedString(payload, schema, StandardCharsets.UTF_8);
            // table
            MySQLBufUtils.writeLengthEncodedString(payload, table, StandardCharsets.UTF_8);
            // org_table
            MySQLBufUtils.writeLengthEncodedString(payload, orgTable, StandardCharsets.UTF_8);
            // name
            MySQLBufUtils.writeLengthEncodedString(payload, name, StandardCharsets.UTF_8);
            // org_name
            MySQLBufUtils.writeLengthEncodedString(payload, orgName, StandardCharsets.UTF_8);
            // fixed fields length marker (0x0c)
            MySQLBufUtils.writeLengthEncodedInt(payload, 0x0c);
            // character set (2 bytes LE)
            payload.writeShortLE(characterSet);
            // column length (4 bytes LE)
            payload.writeIntLE(columnLength);
            // column type (1 byte)
            payload.writeByte(columnType.getCode());
            // flags (2 bytes LE)
            payload.writeShortLE(flags);
            // decimals (1 byte)
            payload.writeByte(decimals);
            // filler (2 bytes)
            payload.writeZero(2);

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
