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
import java.nio.charset.Charset;

/**
 * Static helpers for MySQL wire format encoding and decoding.
 */
public final class MySQLBufUtils {

    private MySQLBufUtils() {
    }

    /**
     * Write a length-encoded integer to the buffer.
     */
    public static void writeLengthEncodedInt(ByteBuf buf, long value) {
        if (value < 251) {
            buf.writeByte((int) value);
        } else if (value < 0x10000L) {
            buf.writeByte(0xFC);
            buf.writeShortLE((int) value);
        } else if (value < 0x1000000L) {
            buf.writeByte(0xFD);
            buf.writeMediumLE((int) value);
        } else {
            buf.writeByte(0xFE);
            buf.writeLongLE(value);
        }
    }

    /**
     * Read a length-encoded integer from the buffer.
     */
    public static long readLengthEncodedInt(ByteBuf buf) {
        int firstByte = buf.readUnsignedByte();
        if (firstByte < 251) {
            return firstByte;
        } else if (firstByte == 0xFC) {
            return buf.readUnsignedShortLE();
        } else if (firstByte == 0xFD) {
            return buf.readUnsignedMediumLE();
        } else if (firstByte == 0xFE) {
            return buf.readLongLE();
        } else {
            // 0xFB = NULL, 0xFF = error
            return -1;
        }
    }

    /**
     * Write a length-encoded string to the buffer.
     */
    public static void writeLengthEncodedString(ByteBuf buf, String value, Charset charset) {
        byte[] bytes = value.getBytes(charset);
        writeLengthEncodedInt(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * Read a length-encoded string from the buffer.
     */
    public static String readLengthEncodedString(ByteBuf buf, Charset charset) {
        long length = readLengthEncodedInt(buf);
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[(int) length];
        buf.readBytes(bytes);
        return new String(bytes, charset);
    }

    /**
     * Write a null-terminated string to the buffer.
     */
    public static void writeNullTerminatedString(ByteBuf buf, String value, Charset charset) {
        buf.writeBytes(value.getBytes(charset));
        buf.writeByte(0);
    }

    /**
     * Read a null-terminated string from the buffer.
     */
    public static String readNullTerminatedString(ByteBuf buf, Charset charset) {
        int start = buf.readerIndex();
        while (buf.readByte() != 0) {
            // advance
        }
        int length = buf.readerIndex() - start - 1;
        byte[] bytes = new byte[length];
        buf.readerIndex(start);
        buf.readBytes(bytes);
        buf.readByte(); // skip null terminator
        return new String(bytes, charset);
    }

    /**
     * Write a fixed-length little-endian integer to the buffer.
     */
    public static void writeFixedLengthInt(ByteBuf buf, int value, int length) {
        for (int i = 0; i < length; i++) {
            buf.writeByte((value >> (8 * i)) & 0xFF);
        }
    }

    /**
     * Read a fixed-length little-endian integer from the buffer.
     */
    public static long readFixedLengthInt(ByteBuf buf, int length) {
        long result = 0;
        for (int i = 0; i < length; i++) {
            result |= ((long) buf.readUnsignedByte()) << (8 * i);
        }
        return result;
    }
}
