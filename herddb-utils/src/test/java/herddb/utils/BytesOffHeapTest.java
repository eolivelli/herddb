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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the off-heap-backed {@link Bytes} path introduced in step 2 of
 * issue #399: round-trip equality with on-heap {@code Bytes}, idempotent
 * release, lazy materialisation, and zero-copy {@code writeTo} helpers.
 */
public class BytesOffHeapTest {

    private byte[] payload;
    private Bytes onHeap;

    @Before
    public void setUp() {
        Random rnd = new Random(0xc0ffee);
        payload = new byte[1024];
        rnd.nextBytes(payload);
        onHeap = Bytes.from_array(payload);
    }

    @After
    public void tearDown() {
        // Bytes instances created in tests own their slice; release() is
        // idempotent so calling here is always safe.
    }

    private Bytes newOffHeap() {
        ByteBuf slice = HerdDBByteBufAllocators.dataPagesAllocator()
                .directBuffer(payload.length);
        slice.writeBytes(payload);
        return Bytes.fromOffHeap(slice);
    }

    @Test
    public void offHeapInstanceReportsCorrectStateAndLength() {
        Bytes off = newOffHeap();
        try {
            assertTrue("instance must report itself as off-heap before any byte[] read",
                    off.isOffHeap());
            assertEquals(payload.length, off.getLength());
            assertTrue("off-heap-backed Bytes is logically shared into a slab",
                    off.isShared());
        } finally {
            off.release();
        }
    }

    @Test
    public void hashCodeAndEqualsRoundTripWithOnHeap() {
        Bytes off = newOffHeap();
        try {
            assertEquals("hashCode must match the on-heap representation",
                    onHeap.hashCode(), off.hashCode());
            assertEquals("off.equals(on) must be true", off, onHeap);
            assertEquals("on.equals(off) must be true", onHeap, off);
            assertTrue("hashCode must still be off-heap-backed (no materialisation triggered)",
                    off.isOffHeap());
        } finally {
            off.release();
        }
    }

    @Test
    public void compareToRoundTripsWithOnHeapWithoutMaterialising() {
        Bytes off = newOffHeap();
        try {
            assertEquals(0, off.compareTo(onHeap));
            assertEquals(0, onHeap.compareTo(off));
            assertTrue("compareTo must NOT trigger materialisation", off.isOffHeap());
        } finally {
            off.release();
        }
    }

    @Test
    public void compareToOrdersDifferentValuesCorrectly() {
        byte[] smaller = payload.clone();
        smaller[0] = (byte) (payload[0] - 1);
        byte[] larger = payload.clone();
        larger[0] = (byte) (payload[0] + 1);
        Bytes off = newOffHeap();
        try {
            assertTrue("off > smaller", off.compareTo(Bytes.from_array(smaller)) > 0);
            assertTrue("off < larger", off.compareTo(Bytes.from_array(larger)) < 0);
        } finally {
            off.release();
        }
    }

    @Test
    public void getBufferLazilyMaterialisesAndReleasesSlice() {
        Bytes off = newOffHeap();
        assertTrue("precondition", off.isOffHeap());
        byte[] materialised = off.getBuffer();
        assertArrayEquals(payload, materialised);
        assertFalse("getBuffer must release the off-heap slice", off.isOffHeap());
        // Subsequent getBuffer is O(1) and returns the same array.
        assertTrue("second getBuffer must return the cached array",
                materialised == off.getBuffer());
    }

    @Test
    public void releaseIsIdempotentAndASafeNoOpOnHeapBacked() {
        // off-heap: release once via the test, again via tear-down — no exception.
        Bytes off = newOffHeap();
        off.release();
        off.release();
        // on-heap: release is always a no-op.
        onHeap.release();
        onHeap.release();
        assertFalse(onHeap.isOffHeap());
    }

    @Test
    public void writeToByteBufIsZeroCopyForOffHeap() {
        Bytes off = newOffHeap();
        try {
            ByteBuf dst = Unpooled.buffer(payload.length);
            try {
                off.writeTo(dst);
                assertEquals(payload.length, dst.readableBytes());
                byte[] copy = new byte[payload.length];
                dst.getBytes(0, copy);
                assertArrayEquals(payload, copy);
                assertTrue("writeTo(ByteBuf) must NOT trigger materialisation",
                        off.isOffHeap());
            } finally {
                dst.release();
            }
        } finally {
            off.release();
        }
    }

    @Test
    public void writeToOutputStreamIsZeroCopyForOffHeap() throws Exception {
        Bytes off = newOffHeap();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(payload.length);
            off.writeTo(baos);
            assertArrayEquals(payload, baos.toByteArray());
            assertTrue("writeTo(OutputStream) must NOT trigger materialisation",
                    off.isOffHeap());
        } finally {
            off.release();
        }
    }

    @Test
    public void differentBytesAreNotEqualAcrossRepresentations() {
        byte[] other = payload.clone();
        other[other.length - 1] ^= 0x55;
        Bytes off = newOffHeap();
        try {
            assertNotEquals(off, Bytes.from_array(other));
            assertNotEquals(Bytes.from_array(other), off);
        } finally {
            off.release();
        }
    }

    @Test
    public void nonSharedMaterialisesOffHeapIntoPrivateArray() {
        Bytes off = newOffHeap();
        Bytes priv = off.nonShared();
        // off is the parent; nonShared() returned a private copy. The original
        // is now materialised because nonShared() called getBuffer() internally.
        assertFalse(off.isOffHeap());
        assertEquals(off, priv);
        // priv must hold its own backing array (offset 0, length matches).
        assertEquals(0, priv.getOffset());
        assertEquals(payload.length, priv.getLength());
        off.release();
    }

    @Test
    public void emptyOffHeapBytesRoundTrips() {
        ByteBuf empty = HerdDBByteBufAllocators.dataPagesAllocator().directBuffer(0);
        Bytes off = Bytes.fromOffHeap(empty);
        try {
            assertEquals(0, off.getLength());
            assertEquals(Bytes.EMPTY_ARRAY, off);
            assertEquals(Bytes.EMPTY_ARRAY.hashCode(), off.hashCode());
        } finally {
            off.release();
        }
    }
}
