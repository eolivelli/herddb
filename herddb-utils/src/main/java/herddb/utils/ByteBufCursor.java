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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

/**
 * This utility class enables accessing a ByteBuf while leveraging
 * variable-length encoding features without performing unnecessary copies.
 * Similar to ByteArrayCursor but backed by a ByteBuf instead of byte[].
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ByteBufCursor implements Closeable {

    private static final Recycler<ByteBufCursor> RECYCLER = new Recycler<ByteBufCursor>() {

        @Override
        protected ByteBufCursor newObject(
                Handle<ByteBufCursor> handle
        ) {
            return new ByteBufCursor(handle);
        }

    };

    private final io.netty.util.Recycler.Handle<ByteBufCursor> handle;
    private ByteBuf buf;

    public static ByteBufCursor wrap(ByteBuf buf) {
        ByteBufCursor res = RECYCLER.get();
        res.buf = buf;
        return res;
    }

    public static ByteBufCursor wrap(byte[] array) {
        return wrap(Unpooled.wrappedBuffer(array));
    }

    public static ByteBufCursor wrap(byte[] array, int offset, int length) {
        return wrap(Unpooled.wrappedBuffer(array, offset, length));
    }

    private ByteBufCursor(Handle<ByteBufCursor> handle) {
        this.handle = handle;
    }

    public ByteBufCursor(ByteBuf buf) {
        this.buf = buf;
        this.handle = null;
    }

    public boolean isEof() {
        return !buf.isReadable();
    }

    public int read() {
        if (!buf.isReadable()) {
            return -1;
        }
        return buf.readUnsignedByte();
    }

    public byte readByte() throws IOException {
        if (!buf.isReadable()) {
            throw new EOFException("EOF reached");
        }
        return buf.readByte();
    }

    private void checkReadable(int len) throws IOException {
        if (buf.readableBytes() < len) {
            throw new EOFException("EOF: need " + len + " bytes but only " + buf.readableBytes() + " available");
        }
    }

    /**
     * Reads an int stored in variable-length format. Reads between one and five
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     */
    public int readVInt() throws IOException {
        return ByteBufUtils.readVInt(buf);
    }

    /**
     * Same as {@link #readVInt()} but does not throw EOFException. Since
     * throwing exceptions is very expensive for the JVM this operation is
     * preferred if you could hit and EOF
     */
    public int readVIntNoEOFException() throws IOException {
        int ch = read();
        if (ch < 0) {
            return -1;
        }

        byte b = (byte) (ch);
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = readByte();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * Reads a long stored in variable-length format. Reads between one and nine
     * bytes. Smaller values take fewer bytes. Negative numbers are not
     * supported.
     */
    public long readVLong() throws IOException {
        return ByteBufUtils.readVLong(buf);
    }

    public int readZInt() throws IOException {
        return ByteBufUtils.readZInt(buf);
    }

    public long readZLong() throws IOException {
        return ByteBufUtils.readZLong(buf);
    }

    private static final byte[] EMPTY_ARRAY = new byte[0];
    private static final float[] EMPTY_FLOAT_ARRAY = new float[0];

    public int readArrayLen() throws IOException {
        return readVInt();
    }

    public int getPosition() {
        return buf.readerIndex();
    }

    public int readInt() throws IOException {
        checkReadable(4);
        return buf.readInt();
    }

    public float readFloat() throws IOException {
        checkReadable(4);
        return buf.readFloat();
    }

    public void skipFloat() throws IOException {
        skipInt();
    }

    public long readLong() throws IOException {
        checkReadable(8);
        return buf.readLong();
    }

    public final double readDouble() throws IOException {
        return ByteBufUtils.readDouble(buf);
    }

    public final boolean readBoolean() throws IOException {
        int ch = read();
        if (ch < 0) {
            throw new EOFException();
        }
        return (ch != 0);
    }

    public byte[] readArray() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return EMPTY_ARRAY;
        } else if (len == -1) {
            return null;
        }
        byte[] res = new byte[len];
        readArray(len, res);
        return res;
    }

    public void readArray(int len, byte[] buffer) throws IOException {
        if (len == 0) {
            return;
        }
        checkReadable(len);
        buf.readBytes(buffer, 0, len);
    }

    public float[] readFloatArray() throws IOException {
        return ByteBufUtils.readFloatArray(buf);
    }

    public Bytes readBytesNoCopy() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return Bytes.EMPTY_ARRAY;
        } else if (len == -1) {
            return null;
        }
        checkReadable(len);
        byte[] res = new byte[len];
        buf.readBytes(res);
        return Bytes.from_array(res, 0, len);
    }

    public Bytes readBytes() throws IOException {
        return Bytes.from_nullable_array(readArray());
    }

    public RawString readRawStringNoCopy() throws IOException {
        int len = readVInt();
        if (len == 0) {
            return RawString.EMPTY;
        } else if (len == -1) {
            return null;
        }

        byte[] res = new byte[len];
        checkReadable(len);
        buf.readBytes(res);
        RawString string = RawString.newPooledRawString(res, 0, len);
        return string;
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipArray() throws IOException {
        ByteBufUtils.skipArray(buf);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipFloatArray() throws IOException {
        int len = readVInt();
        if (len <= 0) {
            return;
        }
        skip(len * 4);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipInt() throws IOException {
        skip(4);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipLong() throws IOException {
        skip(8);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipDouble() throws IOException {
        skip(8);
    }

    @SuppressFBWarnings(value = "SR_NOT_CHECKED")
    public void skipBoolean() throws IOException {
        skip(1);
    }

    public void skip(int pos) throws IOException {
        if (pos < 0) {
            throw new IOException("corrupted data");
        }
        checkReadable(pos);
        buf.skipBytes(pos);
    }

    @Override
    public void close() {
        if (buf != null) {
            buf.release();
            buf = null;
        }
        if (handle != null) {
            handle.recycle(this);
        }
    }
}
