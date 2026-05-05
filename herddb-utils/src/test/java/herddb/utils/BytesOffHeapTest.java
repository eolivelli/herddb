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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
            assertTrue("hashCode must NOT trigger materialisation", off.isOffHeap());
            assertEquals("off.equals(on) must be true", off, onHeap);
            assertTrue("equals(off, on) must NOT trigger materialisation", off.isOffHeap());
            assertEquals("on.equals(off) must be true", onHeap, off);
            assertTrue("equals(on, off) must NOT trigger materialisation", off.isOffHeap());
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
        // priv must hold its own backing array (distinct from the parent's).
        assertEquals(0, priv.getOffset());
        assertEquals(payload.length, priv.getLength());
        assertFalse("priv must not be shared", priv.isShared());
        assertNotSame("nonShared() must return a distinct backing array",
                off.getBuffer(), priv.getBuffer());
        off.release();
    }

    @Test
    public void emptyOffHeapBytesRoundTrips() {
        ByteBuf empty = HerdDBByteBufAllocators.dataPagesAllocator().directBuffer(0);
        assertEquals("readerIndex of a fresh allocator buffer must be 0",
                0, empty.readerIndex());
        Bytes off = Bytes.fromOffHeap(empty);
        try {
            assertEquals(0, off.getLength());
            assertEquals(Bytes.EMPTY_ARRAY, off);
            assertEquals(Bytes.EMPTY_ARRAY.hashCode(), off.hashCode());
        } finally {
            off.release();
        }
    }

    /**
     * The slab-owner pattern from step 3+: allocate one big slab, slice it
     * into N retained slices at non-zero offsets, hand each slice to a
     * {@code Bytes}, and verify each slice round-trips correctly.
     */
    @Test
    public void fromOffHeapWithNonZeroReaderIndexRoundTrips() throws java.io.IOException {
        byte[] junk = new byte[37];
        new Random(0x42).nextBytes(junk);
        ByteBuf slab = HerdDBByteBufAllocators.dataPagesAllocator()
                .directBuffer(junk.length + payload.length + junk.length);
        try {
            slab.writeBytes(junk);
            slab.writeBytes(payload);
            slab.writeBytes(junk);
            ByteBuf slice = slab.retainedSlice(junk.length, payload.length);
            // retainedSlice on a pooled direct buffer may report readerIndex == 0
            // on the slice itself even though it points into the middle of the
            // parent — that's exactly the case the off-heap read paths must
            // handle (use slice.readerIndex(), not zero).
            Bytes off = Bytes.fromOffHeap(slice);
            try {
                assertEquals(payload.length, off.getLength());
                assertEquals(onHeap.hashCode(), off.hashCode());
                assertEquals(onHeap, off);
                assertEquals(0, off.compareTo(onHeap));

                ByteBuf dst = Unpooled.buffer(payload.length);
                try {
                    off.writeTo(dst);
                    byte[] copy = new byte[payload.length];
                    dst.getBytes(0, copy);
                    assertArrayEquals(payload, copy);
                } finally {
                    dst.release();
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                off.writeTo(baos);
                assertArrayEquals(payload, baos.toByteArray());
            } finally {
                off.release();
            }
        } finally {
            slab.release();
        }
    }

    /**
     * Verifies that releasing one sibling slice on a slab does not invalidate
     * the other slices — the slab refcount is shared but each slice maintains
     * its own logical lifecycle.
     */
    @Test
    public void siblingSlicesOnSameSlabAreIndependentlyReleasable() {
        byte[] half1 = new byte[64];
        byte[] half2 = new byte[64];
        new Random(0x7).nextBytes(half1);
        new Random(0x8).nextBytes(half2);
        ByteBuf slab = HerdDBByteBufAllocators.dataPagesAllocator()
                .directBuffer(half1.length + half2.length);
        try {
            slab.writeBytes(half1);
            slab.writeBytes(half2);
            ByteBuf s1 = slab.retainedSlice(0, half1.length);
            ByteBuf s2 = slab.retainedSlice(half1.length, half2.length);
            Bytes b1 = Bytes.fromOffHeap(s1);
            Bytes b2 = Bytes.fromOffHeap(s2);

            assertEquals(Bytes.from_array(half1), b1);
            assertEquals(Bytes.from_array(half2), b2);

            // Release b1; b2 must still read correctly.
            b1.release();
            assertFalse(b1.isOffHeap());
            assertEquals(Bytes.from_array(half2), b2);
            assertEquals(Bytes.from_array(half2).hashCode(), b2.hashCode());
            b2.release();
        } finally {
            slab.release();
        }
    }

    /**
     * After {@link Bytes#release()} without prior materialisation, every
     * accessor must throw {@link IllegalStateException} so the failure is
     * localised and actionable instead of surfacing as an NPE deep inside
     * a downstream codec.
     */
    @Test
    public void accessAfterReleaseThrowsIllegalStateException() {
        Bytes off = newOffHeap();
        off.release();
        assertFalse(off.isOffHeap());
        assertThrows(IllegalStateException.class, off::getBuffer);
        assertThrows(IllegalStateException.class, off::getOffset);
        assertThrows(IllegalStateException.class, off::to_array);
        assertThrows(IllegalStateException.class, () -> off.writeTo(Unpooled.buffer(8)));
        assertThrows(IllegalStateException.class, () -> {
            try {
                off.writeTo(new ByteArrayOutputStream());
            } catch (java.io.IOException e) {
                throw new AssertionError("unexpected IOException", e);
            }
        });
        // hashCode must throw too — a silently-zero hash would corrupt every
        // ConcurrentHashMap that previously held the key.
        assertThrows(IllegalStateException.class, off::hashCode);
        // equals against any other Bytes triggers a byteAt() read which
        // throws; we accept either IllegalStateException or false (null/length
        // mismatch) — but our path must throw.
        Bytes other = newOffHeap();
        try {
            assertThrows(IllegalStateException.class, () -> off.equals(other));
        } finally {
            other.release();
        }
    }

    /**
     * Concurrent stress test for the safe-publication and read-path
     * invariants of an off-heap-backed {@code Bytes}: many reader threads
     * compute {@code hashCode()} / {@code equals(...)} / {@code compareTo(...)}
     * concurrently and must all agree with the on-heap baseline. The reader
     * never touches the off-heap slice's lifecycle and the writer never
     * calls {@code release()} until all readers have joined — this is the
     * quiescence contract documented on {@code release()} in action.
     */
    @Test
    public void concurrentReadersOnOffHeapBytesAreSafe() throws Exception {
        final int readers = 16;
        final int iterationsPerReader = 5_000;
        final Bytes off = newOffHeap();
        try {
            ExecutorService pool = Executors.newFixedThreadPool(readers);
            try {
                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger errors = new AtomicInteger();
                Future<?>[] futures = new Future<?>[readers];
                for (int r = 0; r < readers; r++) {
                    futures[r] = pool.submit(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < iterationsPerReader; i++) {
                                if (off.hashCode() != onHeap.hashCode()) {
                                    errors.incrementAndGet();
                                }
                                if (!off.equals(onHeap)) {
                                    errors.incrementAndGet();
                                }
                                if (off.compareTo(onHeap) != 0) {
                                    errors.incrementAndGet();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            errors.incrementAndGet();
                        }
                    });
                }
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }
                assertEquals("no concurrent reader should have observed an error",
                        0, errors.get());
                // After all readers join, the slice has not been released and
                // the bytes are still off-heap (no materialisation triggered).
                assertTrue("readers must not trigger materialisation",
                        off.isOffHeap());
            } finally {
                pool.shutdownNow();
            }
        } finally {
            off.release();
        }
    }
}
