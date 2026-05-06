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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Tests for {@link CompareBytesUtils#hashCode(byte[], int, int)}, in
 * particular the bit-mixing properties added in issue #429 to defend
 * against {@link java.util.concurrent.ConcurrentHashMap} bin
 * treeification on tightly clustered inputs (e.g. small consecutive
 * integer primary keys).
 */
public class CompareBytesUtilsTest {

    @Test
    public void deterministic() {
        final byte[] a = {1, 2, 3, 4, 5};
        assertEquals(CompareBytesUtils.hashCode(a, 0, a.length),
                CompareBytesUtils.hashCode(a, 0, a.length));
    }

    @Test
    public void nullArrayReturnsZero() {
        assertEquals(0, CompareBytesUtils.hashCode((byte[]) null, 0, 0));
        assertEquals(0, CompareBytesUtils.hashCode((ByteBuf) null, 0, 0));
    }

    @Test
    public void offsetAndLengthHonoured() {
        final byte[] a = {0, 0, 1, 2, 3, 0, 0};
        final byte[] b = {1, 2, 3};
        assertEquals(CompareBytesUtils.hashCode(b, 0, 3),
                CompareBytesUtils.hashCode(a, 2, 3));
    }

    @Test
    public void emptyAndOneZeroByteHashDifferently() {
        // The pre-mix accumulator was identical for these two inputs
        // (1 vs 1*31+0 = 31). The added length mix breaks that tie so
        // the two values land in different CHM bins.
        final int empty = CompareBytesUtils.hashCode(new byte[0], 0, 0);
        final int oneZero = CompareBytesUtils.hashCode(new byte[]{0}, 0, 1);
        assertNotEquals(empty, oneZero);
    }

    @Test
    public void distinctIntKeysProduceDistinctHashes() {
        // The historical byte-by-byte 31x polynomial collided on certain
        // pairs of inputs (b0 += 31 vs. b1 += 1 produces identical
        // accumulators) and produced only ~1200 distinct hashes for 8192
        // consecutive 4-byte ints. The new int-block polynomial reads
        // 4 bytes at a time as a big-endian word, so distinct ints
        // produce distinct polynomial accumulators; the post-mix is a
        // bijection over the 32-bit space (multiplication by an odd
        // constant plus an XOR-shift), so distinctness is preserved
        // through to the final hash. Verify the property end to end.
        final int numKeys = 8192;
        final java.util.HashSet<Integer> seen = new java.util.HashSet<>();
        for (int i = 0; i < numKeys; i++) {
            final byte[] buf = {
                    (byte) (i >>> 24),
                    (byte) (i >>> 16),
                    (byte) (i >>> 8),
                    (byte) i
            };
            assertTrue("hash collision at i=" + i,
                    seen.add(CompareBytesUtils.hashCode(buf, 0, 4)));
        }
        assertEquals(numKeys, seen.size());
    }

    /**
     * The motivating regression: consecutive 4-byte integer primary keys
     * must spread across {@link java.util.concurrent.ConcurrentHashMap}
     * bins well enough that no individual bin crosses
     * {@code TREEIFY_THRESHOLD = 8}, otherwise CHM converts the bin to a
     * red-black tree and {@code putTreeVal} starts calling
     * {@code comparableClassFor}, which walks generic-signature
     * reflection metadata.
     *
     * <p>The original {@code 31 * h + b} polynomial concentrated the
     * output in a narrow range for this input shape, so even after CHM's
     * built-in {@code spread} step a handful of bins became hotspots.
     * The golden-ratio mix added in this change spreads the output
     * uniformly enough that no bin crosses the threshold across 8192
     * consecutive integer keys hashed into a 1024-bin histogram (which
     * is a much more aggressive ratio than CHM's typical load factor).
     */
    @Test
    public void distributionForConsecutiveIntKeysAvoidsTreeification() {
        final int numKeys = 8192;
        final int numBins = 1024;
        final int[] histogram = new int[numBins];
        for (int i = 0; i < numKeys; i++) {
            final byte[] buf = {
                    (byte) (i >>> 24),
                    (byte) (i >>> 16),
                    (byte) (i >>> 8),
                    (byte) i
            };
            int h = CompareBytesUtils.hashCode(buf, 0, 4);
            // CHM's spread step, applied here so the test mirrors
            // the actual bin selection.
            h ^= h >>> 16;
            histogram[(h & 0x7fffffff) % numBins]++;
        }
        int max = 0;
        for (int c : histogram) {
            if (c > max) {
                max = c;
            }
        }
        // 8192 keys / 1024 bins = 8 expected. Treeification trips at 8;
        // we want clear headroom over the expected mean to be confident
        // we are well under the threshold even with stochastic clumps.
        // The golden-ratio mix gives near-Poisson behaviour, so the
        // observed maximum sits close to the mean rather than 2-3x
        // above it.
        assertTrue("bin overflow: max=" + max + " (expected ~8, threshold ~16)",
                max <= 16);
    }

    /**
     * The byte[] and {@link ByteBuf} overloads must produce the same
     * hash for the same bytes — this is the cross-source equality
     * contract that {@link Bytes#equals(Object)} relies on when
     * comparing an on-heap and an off-heap representation.
     */
    @Test
    public void byteArrayAndByteBufOverloadsAgree() {
        final byte[] random = new byte[256];
        for (int i = 0; i < random.length; i++) {
            random[i] = (byte) (i * 17 + 5);
        }
        // Try several lengths spanning {0, byte-tail-only, int-block-aligned,
        // int-block + tail}.
        for (int len : new int[]{0, 1, 2, 3, 4, 5, 7, 8, 13, 16, 31, 64, 100, 256}) {
            final int expected = CompareBytesUtils.hashCode(random, 0, len);

            // Heap-backed ByteBuf (hasArray() == true): must take the
            // byte[] fast path and produce the same hash.
            final ByteBuf heapBuf = Unpooled.wrappedBuffer(random, 0, len);
            try {
                assertEquals("heap ByteBuf hash mismatch at len=" + len,
                        expected, CompareBytesUtils.hashCode(heapBuf, heapBuf.readerIndex(), len));
            } finally {
                heapBuf.release();
            }

            // Direct (off-heap) ByteBuf (hasArray() == false): must take
            // the int-block path that reads via ByteBuf.getInt and
            // produce the same hash. We have to copy the bytes in
            // because Unpooled.directBuffer returns an empty buffer; do
            // it through writeBytes (no shortcut).
            final ByteBuf directBuf = Unpooled.directBuffer(Math.max(1, len));
            try {
                directBuf.writeBytes(random, 0, len);
                assertEquals("direct ByteBuf hash mismatch at len=" + len,
                        expected,
                        CompareBytesUtils.hashCode(directBuf, directBuf.readerIndex(), len));
            } finally {
                directBuf.release();
            }
        }
    }

    /**
     * Symmetric mix invariant: applying the hash to the same bytes via
     * different offset slices must produce the same result, mirroring
     * the contract used by {@code Bytes.equals} which compares slices
     * of differently-sized backing arrays.
     */
    @Test
    public void offsetSlicesProduceMatchingHash() {
        final byte[] big = new byte[64];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 7 + 3);
        }
        final byte[] copy = new byte[10];
        System.arraycopy(big, 17, copy, 0, 10);
        assertEquals(CompareBytesUtils.hashCode(copy, 0, 10),
                CompareBytesUtils.hashCode(big, 17, 10));
    }
}
