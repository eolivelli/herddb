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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Verifies that {@link ByteBufDataOutput} produces the same wire bytes as
 * {@link ExtendedDataOutputStream} for every write method, and that
 * {@link ByteBufDataOutput#writeArray(Bytes)} does not materialise an
 * off-heap-backed {@link Bytes} to a heap {@code byte[]} (issue #497).
 */
public class ByteBufDataOutputTest {

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Write to ExtendedDataOutputStream and return the accumulated bytes. */
    @FunctionalInterface
    interface EdosWriter {
        void write(ExtendedDataOutputStream out) throws IOException;
    }

    private static byte[] viaEdos(EdosWriter w) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ExtendedDataOutputStream edos = new ExtendedDataOutputStream(bos)) {
            w.write(edos);
            edos.flush();
            return bos.toByteArray();
        }
    }

    /** Write to ByteBufDataOutput and return the accumulated bytes. */
    @FunctionalInterface
    interface BbdoWriter {
        void write(ByteBufDataOutput out) throws IOException;
    }

    private static byte[] viaBbdo(BbdoWriter w) throws IOException {
        ByteBuf buf = Unpooled.buffer(256);
        try {
            w.write(new ByteBufDataOutput(buf));
            byte[] result = new byte[buf.writerIndex()];
            buf.getBytes(0, result);
            return result;
        } finally {
            buf.release();
        }
    }

    private static void assertSameBytes(EdosWriter edos, BbdoWriter bbdo) throws IOException {
        assertArrayEquals(viaEdos(edos), viaBbdo(bbdo));
    }

    // -------------------------------------------------------------------------
    // VLong / VInt
    // -------------------------------------------------------------------------

    @Test
    public void testWriteVLongZero() throws IOException {
        assertSameBytes(out -> out.writeVLong(0L), out -> out.writeVLong(0L));
    }

    @Test
    public void testWriteVLongSmall() throws IOException {
        assertSameBytes(out -> out.writeVLong(127L), out -> out.writeVLong(127L));
    }

    @Test
    public void testWriteVLongLarge() throws IOException {
        assertSameBytes(out -> out.writeVLong(Long.MAX_VALUE), out -> out.writeVLong(Long.MAX_VALUE));
    }

    @Test
    public void testWriteVIntNegativeOne() throws IOException {
        // -1 is the null-array marker; both implementations must encode it identically.
        assertSameBytes(out -> out.writeVInt(-1), out -> out.writeVInt(-1));
    }

    @Test
    public void testWriteVIntMax() throws IOException {
        assertSameBytes(out -> out.writeVInt(Integer.MAX_VALUE), out -> out.writeVInt(Integer.MAX_VALUE));
    }

    // -------------------------------------------------------------------------
    // ZLong / ZInt
    // -------------------------------------------------------------------------

    @Test
    public void testWriteZLongNegative() throws IOException {
        assertSameBytes(out -> out.writeZLong(-42L), out -> out.writeZLong(-42L));
    }

    @Test
    public void testWriteZIntNegative() throws IOException {
        assertSameBytes(out -> out.writeZInt(-1), out -> out.writeZInt(-1));
    }

    // -------------------------------------------------------------------------
    // Fixed-width primitives
    // -------------------------------------------------------------------------

    @Test
    public void testWriteByte() throws IOException {
        assertSameBytes(out -> out.writeByte(0xAB), out -> out.writeByte(0xAB));
    }

    @Test
    public void testWriteBoolean() throws IOException {
        assertSameBytes(out -> out.writeBoolean(true), out -> out.writeBoolean(true));
        assertSameBytes(out -> out.writeBoolean(false), out -> out.writeBoolean(false));
    }

    @Test
    public void testWriteLong() throws IOException {
        assertSameBytes(out -> out.writeLong(0xDEADBEEFCAFEBABEL),
                        out -> out.writeLong(0xDEADBEEFCAFEBABEL));
    }

    @Test
    public void testWriteInt() throws IOException {
        assertSameBytes(out -> out.writeInt(0x12345678), out -> out.writeInt(0x12345678));
    }

    // -------------------------------------------------------------------------
    // Array writes
    // -------------------------------------------------------------------------

    @Test
    public void testWriteNullArray() throws IOException {
        assertSameBytes(ExtendedDataOutputStream::writeNullArray,
                        ByteBufDataOutput::writeNullArray);
    }

    @Test
    public void testWriteByteArrayNull() throws IOException {
        assertSameBytes(out -> out.writeArray((byte[]) null),
                        out -> out.writeArray((byte[]) null));
    }

    @Test
    public void testWriteByteArray() throws IOException {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        assertSameBytes(out -> out.writeArray(data), out -> out.writeArray(data));
    }

    @Test
    public void testWriteByteArrayWithOffset() throws IOException {
        byte[] data = "xxhello worldxx".getBytes(StandardCharsets.UTF_8);
        // write the "hello world" slice
        assertSameBytes(out -> out.writeArray(data, 2, 11),
                        out -> out.writeArray(data, 2, 11));
    }

    // -------------------------------------------------------------------------
    // writeArray(Bytes) — heap-backed
    // -------------------------------------------------------------------------

    @Test
    public void testWriteBytesHeapBacked() throws IOException {
        byte[] raw = "heap-key".getBytes(StandardCharsets.UTF_8);
        Bytes b = Bytes.from_array(raw);
        assertSameBytes(out -> out.writeArray(b), out -> out.writeArray(b));
    }

    @Test
    public void testWriteBytesHeapBackedWithOffset() throws IOException {
        // Bytes wrapping a sub-range of a larger heap array (non-zero offset)
        byte[] raw = "xxsubkeyxx".getBytes(StandardCharsets.UTF_8);
        Bytes b = Bytes.from_array(raw, 2, 6); // "subkey"
        assertSameBytes(out -> out.writeArray(b), out -> out.writeArray(b));
    }

    @Test
    public void testWriteBytesNull() throws IOException {
        assertSameBytes(out -> out.writeArray((Bytes) null),
                        out -> out.writeArray((Bytes) null));
    }

    // -------------------------------------------------------------------------
    // writeArray(Bytes) — off-heap-backed, zero-copy verification (issue #497)
    // -------------------------------------------------------------------------

    @Test
    public void testWriteBytesOffHeapDoesNotMaterialise() throws IOException {
        // Build an off-heap Bytes via IndexKeySlab (shared-slab path used by BLink).
        byte[] raw = "offheap-key".getBytes(StandardCharsets.UTF_8);
        IndexKeySlab slab = new IndexKeySlab(raw.length, HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(raw);
        Bytes b = slab.wrap(off, raw.length);

        // Before calling writeArray: the Bytes must be off-heap.
        assertTrue("expected off-heap before write", b.isOffHeap());

        ByteBuf buf = Unpooled.buffer(256);
        try {
            new ByteBufDataOutput(buf).writeArray(b);
        } finally {
            buf.release();
        }

        // After the write the Bytes must STILL be off-heap: ByteBufDataOutput
        // must not have triggered lazy materialisation via getBuffer().
        assertTrue("writeArray(Bytes) must not materialise off-heap Bytes (issue #497)", b.isOffHeap());
    }

    @Test
    public void testWriteBytesOffHeapSameWireBytes() throws IOException {
        // Verify the off-heap path produces identical bytes to the on-heap path.
        byte[] raw = "offheap-key".getBytes(StandardCharsets.UTF_8);
        IndexKeySlab slab = new IndexKeySlab(raw.length, HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(raw);
        Bytes offHeap = slab.wrap(off, raw.length);
        Bytes onHeap = Bytes.from_array(raw);

        byte[] fromOffHeap = viaBbdo(out -> out.writeArray(offHeap));
        byte[] fromOnHeap  = viaBbdo(out -> out.writeArray(onHeap));
        assertArrayEquals("off-heap and on-heap writeArray must produce identical bytes",
                          fromOnHeap, fromOffHeap);
    }

    // -------------------------------------------------------------------------
    // UTF-8 string
    // -------------------------------------------------------------------------

    @Test
    public void testWriteUtf8String() throws IOException {
        assertSameBytes(out -> out.writeUtf8String("Hello, 世界!"),
                        out -> out.writeUtf8String("Hello, 世界!"));
    }

    // -------------------------------------------------------------------------
    // toByteBuf
    // -------------------------------------------------------------------------

    @Test
    public void testToByteBufHeapBacked() {
        byte[] raw = {1, 2, 3, 4, 5};
        Bytes b = Bytes.from_array(raw);
        ByteBuf view = b.toByteBuf();
        byte[] read = new byte[view.readableBytes()];
        view.readBytes(read);
        assertArrayEquals(raw, read);
    }

    @Test
    public void testToByteBufOffHeap() {
        byte[] raw = {10, 20, 30};
        IndexKeySlab slab = new IndexKeySlab(raw.length, HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(raw);
        Bytes b = slab.wrap(off, raw.length);

        ByteBuf view = b.toByteBuf();
        byte[] read = new byte[view.readableBytes()];
        view.readBytes(read);
        assertArrayEquals(raw, read);

        // The original Bytes must still be off-heap (toByteBuf must not materialise).
        assertTrue("toByteBuf must not materialise off-heap Bytes", b.isOffHeap());
    }
}
