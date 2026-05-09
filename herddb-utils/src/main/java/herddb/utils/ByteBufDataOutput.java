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

import io.netty.buffer.ByteBuf;
import java.io.IOException;

/**
 * Write helper wrapping a Netty {@link ByteBuf} with the same API surface as
 * {@link ExtendedDataOutputStream}. Used as the write target for
 * {@code DataStorageManager.DataWriter} so that serialization code can write
 * directly into a {@code ByteBuf} without going through an intermediate
 * {@code OutputStream} wrapper.
 *
 * <p>The key benefit over {@code ExtendedDataOutputStream}: the
 * {@link #writeArray(Bytes)} overload calls {@link Bytes#writeTo(ByteBuf)},
 * which for off-heap-backed {@code Bytes} instances (e.g. keys stored in an
 * {@link IndexKeySlab} direct-memory slab) performs a direct
 * {@code ByteBuf.writeBytes(ByteBuf, int, int)} — a direct-to-direct memory
 * copy with no heap {@code byte[]} allocation per key (issue #497).
 *
 * <p>This class does <em>not</em> take ownership of the supplied
 * {@code ByteBuf}. The caller is responsible for its lifecycle (allocation,
 * release). A {@code ByteBufDataOutput} instance must not be used after the
 * backing {@code ByteBuf} has been released.
 *
 * <p>All write methods declare {@code throws IOException} for drop-in
 * compatibility with existing {@code DataWriter} lambda bodies that already
 * have {@code catch (IOException)} blocks. In practice no {@code IOException}
 * is thrown; the underlying {@code ByteBuf} operations throw
 * {@link IndexOutOfBoundsException} on capacity exhaustion (which cannot
 * happen for auto-expanding buffers) and {@link IllegalStateException} for
 * released {@code Bytes}.
 *
 * @see ExtendedDataOutputStream
 * @see ByteBufUtils
 */
public final class ByteBufDataOutput {

    private final ByteBuf buf;

    /**
     * Creates a {@code ByteBufDataOutput} backed by {@code buf}. The caller
     * retains ownership of {@code buf} and must release it after this output
     * is no longer used.
     */
    public ByteBufDataOutput(ByteBuf buf) {
        this.buf = buf;
    }

    /** Returns the underlying {@link ByteBuf}. */
    public ByteBuf getBuf() {
        return buf;
    }

    // -------------------------------------------------------------------------
    // Variable-length integer encoding (same wire format as ExtendedDataOutputStream)
    // -------------------------------------------------------------------------

    /**
     * Writes a non-negative {@code long} in variable-length format (1–9 bytes).
     * Identical encoding to {@link ExtendedDataOutputStream#writeVLong(long)}.
     */
    public void writeVLong(long i) throws IOException {
        ByteBufUtils.writeVLong(buf, i);
    }

    /**
     * Writes an {@code int} in variable-length format (1–5 bytes).
     * Identical encoding to {@link ExtendedDataOutputStream#writeVInt(int)}.
     */
    public void writeVInt(int i) throws IOException {
        ByteBufUtils.writeVInt(buf, i);
    }

    /**
     * Writes a {@code long} in zig-zag variable-length format.
     * Identical encoding to {@link ExtendedDataOutputStream#writeZLong(long)}.
     */
    public void writeZLong(long l) throws IOException {
        ByteBufUtils.writeZLong(buf, l);
    }

    /**
     * Writes an {@code int} in zig-zag variable-length format.
     * Identical encoding to {@link ExtendedDataOutputStream#writeZInt(int)}.
     */
    public void writeZInt(int i) throws IOException {
        ByteBufUtils.writeZInt(buf, i);
    }

    // -------------------------------------------------------------------------
    // Fixed-width primitives
    // -------------------------------------------------------------------------

    /**
     * Writes the low 8 bits of {@code b} as a single byte.
     * Identical to {@link java.io.DataOutputStream#writeByte(int)}.
     */
    public void writeByte(int b) throws IOException {
        buf.writeByte(b);
    }

    /**
     * Writes a boolean as a single byte (1 = true, 0 = false).
     * Identical to {@link java.io.DataOutputStream#writeBoolean(boolean)}.
     */
    public void writeBoolean(boolean v) throws IOException {
        buf.writeBoolean(v);
    }

    /**
     * Writes a big-endian 8-byte {@code long}.
     * Identical to {@link java.io.DataOutputStream#writeLong(long)}.
     */
    public void writeLong(long l) throws IOException {
        buf.writeLong(l);
    }

    /**
     * Writes a big-endian 4-byte {@code int}.
     * Identical to {@link java.io.DataOutputStream#writeInt(int)}.
     */
    public void writeInt(int i) throws IOException {
        buf.writeInt(i);
    }

    // -------------------------------------------------------------------------
    // Array writes
    // -------------------------------------------------------------------------

    /**
     * Writes a vint-prefixed {@code null} array marker ({@code -1}).
     * Identical to {@link ExtendedDataOutputStream#writeNullArray()}.
     */
    public void writeNullArray() throws IOException {
        ByteBufUtils.writeVInt(buf, -1);
    }

    /**
     * Writes all bytes in {@code data} directly without any length prefix.
     * Equivalent to {@link java.io.OutputStream#write(byte[])}.
     */
    public void write(byte[] data) throws IOException {
        buf.writeBytes(data);
    }

    /**
     * Writes a vint-prefixed byte array, or a null marker if {@code data} is
     * {@code null}. Identical wire format to
     * {@link ExtendedDataOutputStream#writeArray(byte[])}.
     */
    public void writeArray(byte[] data) throws IOException {
        if (data == null) {
            writeNullArray();
        } else {
            ByteBufUtils.writeVInt(buf, data.length);
            buf.writeBytes(data);
        }
    }

    /**
     * Writes a vint-prefixed window of {@code data}.
     * Identical wire format to
     * {@link ExtendedDataOutputStream#writeArray(byte[], int, int)}.
     */
    public void writeArray(byte[] data, int offset, int len) throws IOException {
        ByteBufUtils.writeVInt(buf, len);
        buf.writeBytes(data, offset, len);
    }

    /**
     * Writes a vint-prefixed {@link Bytes} value, or a null marker if
     * {@code data} is {@code null}.
     *
     * <p><b>Zero-copy for off-heap {@code Bytes}:</b> calls
     * {@link Bytes#writeTo(ByteBuf)} which for direct-memory-backed instances
     * performs {@code dst.writeBytes(offHeapSlice, readerIndex, length)} —
     * no heap {@code byte[]} allocation.
     *
     * <p>Identical wire format to
     * {@link ExtendedDataOutputStream#writeArray(Bytes)}.
     */
    public void writeArray(Bytes data) throws IOException {
        if (data == null) {
            writeNullArray();
        } else {
            ByteBufUtils.writeVInt(buf, data.getLength());
            data.writeTo(buf); // zero-copy for off-heap Bytes (issue #497)
        }
    }

    /**
     * Writes a vint-prefixed {@code float[]} in big-endian order.
     * Identical wire format to
     * {@link ExtendedDataOutputStream#writeFloatArray(float[])}.
     */
    public void writeFloatArray(float[] data) throws IOException {
        if (data == null) {
            ByteBufUtils.writeVInt(buf, -1);
        } else {
            ByteBufUtils.writeFloatArray(buf, data);
        }
    }

    /**
     * Writes {@code str} as a vint-prefixed UTF-8 byte sequence.
     * Identical wire format to
     * {@link ExtendedDataOutputStream#writeUtf8String(String)}.
     */
    public void writeUtf8String(String str) throws IOException {
        ByteBufUtils.writeUtf8String(buf, str);
    }
}
