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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

/**
 * Decodes MySQL wire protocol packets.
 * <p>
 * Each MySQL packet consists of:
 * <ul>
 *   <li>3 bytes: payload length (little-endian)</li>
 *   <li>1 byte: sequence ID</li>
 *   <li>N bytes: payload</li>
 * </ul>
 */
public class MySQLPacketDecoder extends ByteToMessageDecoder {

    private static final int HEADER_SIZE = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        while (in.readableBytes() >= HEADER_SIZE) {
            in.markReaderIndex();

            int payloadLength = in.readUnsignedMediumLE();
            int sequenceId = in.readUnsignedByte();

            if (in.readableBytes() < payloadLength) {
                in.resetReaderIndex();
                return;
            }

            ByteBuf payload = in.readRetainedSlice(payloadLength);
            out.add(new MySQLRawPacket(sequenceId, payload));
        }
    }
}
