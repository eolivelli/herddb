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

package herddb.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Extended version of DataInputStream
 *
 * @author enrico.olivelli
 * @author diego.salvi
 * @see ExtendedDataInputStream
 */
public final class ExtendedDataOutputStream extends DataOutputStream {

    public static final ExtendedDataOutputStream NULL = new ExtendedDataOutputStream(NullOutputStream.INSTANCE);

    public ExtendedDataOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Writes an int in a variable-length format. Writes between one and five bytes. Smaller values take fewer bytes.
     *
     * @param i
     * @throws java.io.IOException
     */
    public void writeVInt(int i) throws IOException {
        if ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7F) | 0x80));
            i >>>= 7;

            if ((i & ~0x7F) != 0) {
                writeByte((byte) ((i & 0x7F) | 0x80));
                i >>>= 7;

                if ((i & ~0x7F) != 0) {
                    writeByte((byte) ((i & 0x7F) | 0x80));
                    i >>>= 7;

                    if ((i & ~0x7F) != 0) {
                        writeByte((byte) ((i & 0x7F) | 0x80));
                        i >>>= 7;
                    }
                }
            }
        }

        writeByte((byte) i);
    }

    /**
     * Writes a long in a variable-length format. Writes between one and nine bytes. Smaller values take fewer bytes.
     * Negative numbers are not supported.
     *
     * @param i
     * @throws java.io.IOException
     */
    public void writeVLong(long i) throws IOException {
        if (i < 0) {
            throw new IllegalArgumentException("cannot write negative vLong (got: " + i + ")");
        }
        writeSignedVLong(i);
    }

    // write a potentially negative vLong
    private void writeSignedVLong(long i) throws IOException {
        while ((i & ~0x7FL) != 0L) {
            writeByte((byte) ((i & 0x7FL) | 0x80L));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    public void writeZInt(int i) throws IOException {
        writeVInt((i >> 31) ^ (i << 1));
    }

    public void writeZLong(long l) throws IOException {
        writeSignedVLong((l >> 63) ^ (l << 1));
    }

    public void writeArray(Bytes data) throws IOException {
        if (data == null) {
            writeNullArray();
        } else {
            writeVInt(data.getLength());
            write(data.getBuffer(), data.getOffset(), data.getLength());
        }
    }

    public void writeNullArray() throws IOException {
        writeVInt(-1);
    }

    public void writeNullFloatArray() throws IOException {
        writeVInt(-1);
    }

    public void writeArray(byte[] data) throws IOException {
        if (data == null) {
            writeNullArray();
        } else {
            writeVInt(data.length);
            write(data);
        }
    }
    public void writeFloatArray(float[] data) throws IOException {
        if (data == null) {
            writeNullFloatArray();
        } else {
            writeVInt(data.length);
            for (float f : data) {
                writeFloat(f);
            }
        }
    }



    public void writeArray(byte[] data, int offset, int len) throws IOException {
        writeVInt(len);
        write(data, offset, len);
    }

    /**
     * Writes {@code str} as a vint-prefixed standard UTF-8 byte sequence,
     * matching the wire format produced by
     * {@link ByteBufUtils#writeUtf8String(io.netty.buffer.ByteBuf, String)} so
     * that the two write paths can feed into a single deserializer.
     *
     * @param str string to encode (must not be {@code null})
     */
    public void writeUtf8String(String str) throws IOException {
        Objects.requireNonNull(str, "str");
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        writeVInt(bytes.length);
        write(bytes);
    }

}
