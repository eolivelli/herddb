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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.util.Random;
import org.junit.Test;

/**
 * Verifies {@link Bytes#materialiseAndDetach()} (issue #411): for the three
 * starting states (on-heap, owned-slice off-heap, shared-slab off-heap) the
 * call produces a Bytes that is fully on-heap, has dropped any slab anchor,
 * is idempotent on re-call, and preserves {@code equals}/{@code hashCode}
 * semantics relative to the original payload.
 *
 * <p>The slab-refcount asserts use {@link IndexKeySlab#slabRefCntForTesting()}
 * to confirm shared-slab detach does not decrement the slab's single owner
 * refcount, and that owned-slice detach decrements its slice's refcount by
 * exactly one.
 */
public class BytesMaterialiseAndDetachTest {

    private static byte[] payload(int len, long seed) {
        Random rnd = new Random(seed);
        byte[] out = new byte[len];
        rnd.nextBytes(out);
        return out;
    }

    @Test
    public void onHeapInstanceIsNoOp() {
        byte[] data = payload(64, 1L);
        Bytes b = Bytes.from_array(data);
        assertFalse("precondition: on-heap", b.isOffHeap());
        Bytes ret = b.materialiseAndDetach();
        assertSame("must return this for chaining", b, ret);
        assertFalse("still on-heap", b.isOffHeap());
        assertFalse("on-heap instance is not shared", b.isShared());
        assertArrayEquals("bytes preserved", data, b.to_array());
    }

    @Test
    public void onHeapDetachIsIdempotent() {
        byte[] data = payload(32, 2L);
        Bytes b = Bytes.from_array(data);
        b.materialiseAndDetach();
        b.materialiseAndDetach();
        assertArrayEquals(data, b.to_array());
    }

    @Test
    public void ownedSliceIsCopiedAndSliceReleased() {
        byte[] data = payload(128, 3L);
        ByteBuf slice = HerdDBByteBufAllocators.dataPagesAllocator()
                .directBuffer(data.length);
        slice.writeBytes(data);
        // Hold one extra refcount so we can observe the detach decrement.
        // After fromOffHeap the Bytes owns one refcount; we retain to take
        // a second refcount, so the slice survives detach for our assertion.
        slice.retain();
        int beforeRefCnt = slice.refCnt(); // expected: 2
        Bytes b = Bytes.fromOffHeap(slice);
        assertTrue("precondition: off-heap", b.isOffHeap());
        assertTrue("owned-slice off-heap is shared", b.isShared());

        Bytes ret = b.materialiseAndDetach();
        assertSame(b, ret);
        assertFalse("after detach, no longer off-heap", b.isOffHeap());
        assertFalse("after detach, no longer shared", b.isShared());
        assertEquals("owned-slice detach must release exactly one refcount",
                beforeRefCnt - 1, slice.refCnt());
        assertArrayEquals("bytes preserved across detach", data, b.to_array());

        // Idempotent: second detach must NOT touch the slice further.
        b.materialiseAndDetach();
        assertEquals("second detach must not change slice refcount",
                beforeRefCnt - 1, slice.refCnt());

        // Drop our extra refcount to release the slice.
        slice.release();
        assertEquals(0, slice.refCnt());
    }

    @Test
    public void sharedSlabDetachLeavesSlabRefcountIntactAndClearsAnchor() throws Exception {
        byte[] data = payload(64, 4L);
        IndexKeySlab slab = new IndexKeySlab(data.length,
                HerdDBByteBufAllocators.indexPagesAllocator());
        int beforeRef = slab.slabRefCntForTesting();
        int off = slab.append(data);
        Bytes b = slab.wrap(off, data.length);
        assertTrue("precondition: off-heap", b.isOffHeap());
        assertTrue("precondition: shared", b.isShared());
        assertSame("precondition: slabOwner anchored", slab, slabOwnerOf(b));
        assertEquals("slab.wrap must NOT change refcount",
                beforeRef, slab.slabRefCntForTesting());

        Bytes ret = b.materialiseAndDetach();
        assertSame(b, ret);
        assertFalse("after detach, no longer off-heap", b.isOffHeap());
        assertFalse("after detach, no longer shared", b.isShared());
        assertEquals("shared-slab detach must NOT decrement slab refcount",
                beforeRef, slab.slabRefCntForTesting());
        // The slab anchor must be cleared so GC can reclaim the slab once
        // every other slice on the slab also becomes unreachable.
        assertSame("slabOwner anchor must be cleared after detach",
                null, slabOwnerOf(b));
        assertArrayEquals("bytes preserved across detach", data, b.to_array());

        // Idempotent: second detach must not touch the slab further.
        b.materialiseAndDetach();
        assertEquals(beforeRef, slab.slabRefCntForTesting());

        // The slab is still alive because the IndexKeySlab itself holds the
        // refcount; it will be released by its Cleaner when GC'd. Sanity:
        // refcount stays > 0 here.
        assertTrue("slab refcount stays positive while IndexKeySlab is reachable",
                slab.slabRefCntForTesting() > 0);
    }

    @Test
    public void detachedBytesEqualOnHeapBaseline() {
        byte[] data = payload(96, 5L);
        Bytes onHeap = Bytes.from_array(data);

        IndexKeySlab slab = new IndexKeySlab(data.length,
                HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(data);
        Bytes detached = slab.wrap(off, data.length).materialiseAndDetach();

        assertEquals("equals must round-trip with on-heap baseline", onHeap, detached);
        assertEquals("hashCode must round-trip with on-heap baseline",
                onHeap.hashCode(), detached.hashCode());
        assertEquals("compareTo == 0 with on-heap baseline",
                0, detached.compareTo(onHeap));
    }

    @Test
    public void zeroLengthDetachWorks() {
        IndexKeySlab slab = new IndexKeySlab(0L,
                HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(new byte[0]);
        Bytes b = slab.wrap(off, 0);
        Bytes ret = b.materialiseAndDetach();
        assertSame(b, ret);
        assertFalse(b.isOffHeap());
        assertEquals(0, b.getLength());
        assertArrayEquals(new byte[0], b.to_array());
    }

    /**
     * After lazy {@link Bytes#getBuffer()} materialised the bytes on a
     * shared-slab instance the slab anchor is still held (the existing
     * materialiseFromOffHeap path keeps it for concurrent off-heap readers).
     * A subsequent {@code materialiseAndDetach()} must drop that residual
     * anchor so the detach is fully effective.
     */
    @Test
    public void detachAfterLazyMaterialisationStillClearsAnchor() throws Exception {
        byte[] data = payload(50, 6L);
        IndexKeySlab slab = new IndexKeySlab(data.length,
                HerdDBByteBufAllocators.indexPagesAllocator());
        int off = slab.append(data);
        Bytes b = slab.wrap(off, data.length);
        // Force lazy materialisation via a heap accessor.
        byte[] copy = b.getBuffer();
        assertArrayEquals(data, copy);

        // Anchor still in place per the existing materialiseFromOffHeap contract.
        assertSame("slabOwner still anchored after lazy materialisation",
                slab, slabOwnerOf(b));

        b.materialiseAndDetach();
        assertSame("anchor cleared by materialiseAndDetach",
                null, slabOwnerOf(b));
        assertArrayEquals(data, b.to_array());
    }

    private static Object slabOwnerOf(Bytes b) throws Exception {
        Field f = Bytes.class.getDeclaredField("slabOwner");
        f.setAccessible(true);
        return f.get(b);
    }
}
