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
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Parameterized test to verify ByteBufCursor and ByteArrayCursor have
 * identical behavior across all read operations.
 */
@RunWith(Parameterized.class)
public class ByteBufCursorTest {

    enum CursorType {
        BYTE_ARRAY_CURSOR, BYTE_BUF_CURSOR
    }

    private final CursorType cursorType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{CursorType.BYTE_ARRAY_CURSOR});
        params.add(new Object[]{CursorType.BYTE_BUF_CURSOR});
        return params;
    }

    public ByteBufCursorTest(CursorType cursorType) {
        this.cursorType = cursorType;
    }

    private interface CursorFactory {
        Object wrap(byte[] data) throws IOException;
        void close(Object cursor) throws IOException;
        int readVInt(Object cursor) throws IOException;
        long readVLong(Object cursor) throws IOException;
        int readZInt(Object cursor) throws IOException;
        long readZLong(Object cursor) throws IOException;
        int readInt(Object cursor) throws IOException;
        long readLong(Object cursor) throws IOException;
        float readFloat(Object cursor) throws IOException;
        double readDouble(Object cursor) throws IOException;
        boolean readBoolean(Object cursor) throws IOException;
        byte[] readArray(Object cursor) throws IOException;
        float[] readFloatArray(Object cursor) throws IOException;
        boolean isEof(Object cursor);
        int getPosition(Object cursor);
    }

    private CursorFactory factory;

    @Before
    public void setUp() {
        if (cursorType == CursorType.BYTE_ARRAY_CURSOR) {
            factory = new CursorFactory() {
                @Override
                public Object wrap(byte[] data) {
                    return ByteArrayCursor.wrap(data);
                }

                @Override
                public void close(Object cursor) {
                    ((ByteArrayCursor) cursor).close();
                }

                @Override
                public int readVInt(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readVInt();
                }

                @Override
                public long readVLong(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readVLong();
                }

                @Override
                public int readZInt(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readZInt();
                }

                @Override
                public long readZLong(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readZLong();
                }

                @Override
                public int readInt(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readInt();
                }

                @Override
                public long readLong(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readLong();
                }

                @Override
                public float readFloat(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readFloat();
                }

                @Override
                public double readDouble(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readDouble();
                }

                @Override
                public boolean readBoolean(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readBoolean();
                }

                @Override
                public byte[] readArray(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readArray();
                }

                @Override
                public float[] readFloatArray(Object cursor) throws IOException {
                    return ((ByteArrayCursor) cursor).readFloatArray();
                }

                @Override
                public boolean isEof(Object cursor) {
                    return ((ByteArrayCursor) cursor).isEof();
                }

                @Override
                public int getPosition(Object cursor) {
                    return ((ByteArrayCursor) cursor).getPosition();
                }
            };
        } else {
            factory = new CursorFactory() {
                @Override
                public Object wrap(byte[] data) {
                    return ByteBufCursor.wrap(data);
                }

                @Override
                public void close(Object cursor) {
                    ((ByteBufCursor) cursor).close();
                }

                @Override
                public int readVInt(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readVInt();
                }

                @Override
                public long readVLong(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readVLong();
                }

                @Override
                public int readZInt(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readZInt();
                }

                @Override
                public long readZLong(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readZLong();
                }

                @Override
                public int readInt(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readInt();
                }

                @Override
                public long readLong(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readLong();
                }

                @Override
                public float readFloat(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readFloat();
                }

                @Override
                public double readDouble(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readDouble();
                }

                @Override
                public boolean readBoolean(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readBoolean();
                }

                @Override
                public byte[] readArray(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readArray();
                }

                @Override
                public float[] readFloatArray(Object cursor) throws IOException {
                    return ((ByteBufCursor) cursor).readFloatArray();
                }

                @Override
                public boolean isEof(Object cursor) {
                    return ((ByteBufCursor) cursor).isEof();
                }

                @Override
                public int getPosition(Object cursor) {
                    return ((ByteBufCursor) cursor).getPosition();
                }
            };
        }
    }

    @Test
    public void testRoundTripVInt() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeVInt(0);
        out.writeVInt(1);
        out.writeVInt(127);
        out.writeVInt(128);
        out.writeVInt(16384);
        out.writeVInt(Integer.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readVInt(cursor) == 0;
            assert factory.readVInt(cursor) == 1;
            assert factory.readVInt(cursor) == 127;
            assert factory.readVInt(cursor) == 128;
            assert factory.readVInt(cursor) == 16384;
            assert factory.readVInt(cursor) == Integer.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripVLong() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeVLong(0);
        out.writeVLong(1);
        out.writeVLong(127);
        out.writeVLong(128);
        out.writeVLong(16384);
        out.writeVLong(Long.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readVLong(cursor) == 0;
            assert factory.readVLong(cursor) == 1;
            assert factory.readVLong(cursor) == 127;
            assert factory.readVLong(cursor) == 128;
            assert factory.readVLong(cursor) == 16384;
            assert factory.readVLong(cursor) == Long.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripZInt() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeZInt(0);
        out.writeZInt(-1);
        out.writeZInt(1);
        out.writeZInt(-128);
        out.writeZInt(128);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readZInt(cursor) == 0;
            assert factory.readZInt(cursor) == -1;
            assert factory.readZInt(cursor) == 1;
            assert factory.readZInt(cursor) == -128;
            assert factory.readZInt(cursor) == 128;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripZLong() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeZLong(0);
        out.writeZLong(-1);
        out.writeZLong(1);
        out.writeZLong(-128);
        out.writeZLong(128);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readZLong(cursor) == 0;
            assert factory.readZLong(cursor) == -1;
            assert factory.readZLong(cursor) == 1;
            assert factory.readZLong(cursor) == -128;
            assert factory.readZLong(cursor) == 128;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripInt() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeInt(0);
        out.writeInt(-1);
        out.writeInt(1);
        out.writeInt(Integer.MIN_VALUE);
        out.writeInt(Integer.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readInt(cursor) == 0;
            assert factory.readInt(cursor) == -1;
            assert factory.readInt(cursor) == 1;
            assert factory.readInt(cursor) == Integer.MIN_VALUE;
            assert factory.readInt(cursor) == Integer.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripLong() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeLong(0);
        out.writeLong(-1);
        out.writeLong(1);
        out.writeLong(Long.MIN_VALUE);
        out.writeLong(Long.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readLong(cursor) == 0;
            assert factory.readLong(cursor) == -1;
            assert factory.readLong(cursor) == 1;
            assert factory.readLong(cursor) == Long.MIN_VALUE;
            assert factory.readLong(cursor) == Long.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripFloat() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeFloat(0.0f);
        out.writeFloat(-1.0f);
        out.writeFloat(1.0f);
        out.writeFloat(Float.MIN_VALUE);
        out.writeFloat(Float.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readFloat(cursor) == 0.0f;
            assert factory.readFloat(cursor) == -1.0f;
            assert factory.readFloat(cursor) == 1.0f;
            assert factory.readFloat(cursor) == Float.MIN_VALUE;
            assert factory.readFloat(cursor) == Float.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripDouble() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeDouble(0.0);
        out.writeDouble(-1.0);
        out.writeDouble(1.0);
        out.writeDouble(Double.MIN_VALUE);
        out.writeDouble(Double.MAX_VALUE);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readDouble(cursor) == 0.0;
            assert factory.readDouble(cursor) == -1.0;
            assert factory.readDouble(cursor) == 1.0;
            assert factory.readDouble(cursor) == Double.MIN_VALUE;
            assert factory.readDouble(cursor) == Double.MAX_VALUE;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripBoolean() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeBoolean(true);
        out.writeBoolean(false);
        out.writeBoolean(true);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            assert factory.readBoolean(cursor) == true;
            assert factory.readBoolean(cursor) == false;
            assert factory.readBoolean(cursor) == true;
            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeArray(new byte[]{1, 2, 3, 4, 5});
        out.writeArray(new byte[]{});
        out.writeArray((byte[]) null);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            byte[] arr1 = factory.readArray(cursor);
            assert arr1.length == 5;
            assert arr1[0] == 1 && arr1[4] == 5;

            byte[] arr2 = factory.readArray(cursor);
            assert arr2.length == 0;

            byte[] arr3 = factory.readArray(cursor);
            assert arr3 == null;

            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testRoundTripFloatArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos);
        out.writeFloatArray(new float[]{1.0f, 2.5f, 3.7f});
        out.writeFloatArray(new float[]{});
        out.writeFloatArray((float[]) null);
        byte[] data = baos.toByteArray();

        Object cursor = factory.wrap(data);
        try {
            float[] arr1 = factory.readFloatArray(cursor);
            assert arr1.length == 3;
            assert arr1[0] == 1.0f && arr1[1] == 2.5f && arr1[2] == 3.7f;

            float[] arr2 = factory.readFloatArray(cursor);
            assert arr2.length == 0;

            float[] arr3 = factory.readFloatArray(cursor);
            assert arr3 == null;

            assert factory.isEof(cursor);
        } finally {
            factory.close(cursor);
        }
    }

    @Test
    public void testByteBufCursorReleaseOnClose() throws IOException {
        if (cursorType != CursorType.BYTE_BUF_CURSOR) {
            return; // only test ByteBufCursor
        }

        ByteBuf buf = Unpooled.directBuffer(10);
        buf.writeByte(42);
        int refcntBefore = buf.refCnt();

        ByteBufCursor cursor = ByteBufCursor.wrap(buf);
        cursor.readByte();
        cursor.close();

        // After close, the ByteBuf should be released (refCnt should be 0)
        assert buf.refCnt() == 0 : "Expected refCnt=0 after close, got " + buf.refCnt();
    }

    @Test
    public void testByteBufCursorPooling() throws IOException {
        if (cursorType != CursorType.BYTE_BUF_CURSOR) {
            return; // only test ByteBufCursor
        }

        ByteBuf buf1 = Unpooled.directBuffer(10);
        buf1.writeByte(42);

        ByteBufCursor cursor1 = ByteBufCursor.wrap(buf1);
        int pos1 = System.identityHashCode(cursor1);
        cursor1.close();

        // Allocate a second cursor, should be reused from pool
        ByteBuf buf2 = Unpooled.directBuffer(10);
        buf2.writeByte(43);

        ByteBufCursor cursor2 = ByteBufCursor.wrap(buf2);
        int pos2 = System.identityHashCode(cursor2);

        // Same object identity means cursor was reused from pool
        assert pos1 == pos2 : "Expected cursor to be reused from Recycler pool";

        cursor2.close();
    }
}
