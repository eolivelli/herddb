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

package herddb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.model.Record;
import herddb.utils.Bytes;
import herddb.utils.HerdDBByteBufAllocators;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

/**
 * Issue #409 — verifies that {@link DataPage#toImmutable()} repacks
 * {@code Record.value} bytes into a dedicated off-heap slab allocated from
 * {@link HerdDBByteBufAllocators#dataPagesAllocator()} when the aggregate
 * value bytes meet the slab threshold, and that the slab is released by the
 * JDK {@link java.lang.ref.Cleaner} once the resulting immutable page (and
 * every shared-slab {@link Bytes} extracted from it) becomes GC-unreachable.
 *
 * <p>These tests are intentionally narrow: they exercise the
 * mutable→immutable transition directly (no full {@code TableManager}), so
 * they don't need cluster infrastructure and remain in the core test
 * workflow.
 */
public class DataPageToImmutableSlabPackTest {

    private static List<Record> sampleRecords(int count, int valueLen) {
        List<Record> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] value = new byte[valueLen];
            for (int j = 0; j < valueLen; j++) {
                value[j] = (byte) ((i * 31 + j) & 0xff);
            }
            out.add(new Record(Bytes.from_long(i), Bytes.from_array(value)));
        }
        return out;
    }

    /**
     * Build a mutable {@link DataPage} populated with the given records, mark
     * it non-writable, and return it ready for {@link DataPage#toImmutable()}.
     */
    private static DataPage makeMutablePage(long pageId, long maxSize, List<Record> records) {
        Map<Bytes, Record> map = new ConcurrentHashMap<>(records.size());
        long estimated = 0L;
        for (Record r : records) {
            map.put(r.key, r);
            estimated += DataPage.estimateEntrySize(r);
        }
        DataPage mutable = new DataPage(null, pageId, maxSize, estimated, map, false);
        // toImmutable() requires writable == false (the mutable page must
        // already be sealed by the caller before conversion); replicate that.
        mutable.writable = false;
        return mutable;
    }

    @Test
    public void toImmutableSlabPacksValuesWhenAboveThreshold() throws Exception {
        // 64 × 512 = 32 KiB total > 4 KiB threshold.
        List<Record> records = sampleRecords(64, 512);
        DataPage mutable = makeMutablePage(7L, 1024L * 1024L, records);
        DataPage immutable = mutable.toImmutable();
        try {
            assertTrue("immutable page must be slab-packed",
                    immutable.hasOffHeapValueSlab());

            // Reflect the slab and verify it really lives in direct memory
            // and was created by the data-pages allocator.
            Field f = DataPage.class.getDeclaredField("valueSlab");
            f.setAccessible(true);
            ByteBuf slab = (ByteBuf) f.get(immutable);
            assertNotNull(slab);
            assertTrue("slab must be direct memory", slab.isDirect());
            assertTrue("slab must be allocated by the data-pages allocator (not DEFAULT)",
                    slab.alloc() == HerdDBByteBufAllocators.dataPagesAllocator());
            assertEquals("slab must hold exactly one initial refcount",
                    1, immutable.slabRefCntForTesting());

            // Every value is now shared-slab Bytes, anchored against the new
            // immutable page.
            for (Record original : records) {
                Record packed = immutable.get(original.key);
                assertNotNull("missing key " + original.key, packed);
                assertTrue("value must be off-heap after toImmutable()",
                        packed.value.isOffHeap());
                assertEquals("value length round-trip",
                        original.value.getLength(), packed.value.getLength());
                assertEquals("value bytes round-trip",
                        original.value, packed.value);
            }
        } finally {
            // Drop the strong reference; subsequent GC will return the slab to the pool.
            immutable = null;
        }
    }

    @Test
    public void toImmutableFallsBackToOnHeapWhenBelowThreshold() {
        // 16 × 16 = 256 bytes < 4 KiB threshold.
        List<Record> records = sampleRecords(16, 16);
        DataPage mutable = makeMutablePage(8L, 1024L * 1024L, records);
        DataPage immutable = mutable.toImmutable();
        assertFalse("tiny pages must keep on-heap values after toImmutable()",
                immutable.hasOffHeapValueSlab());
        assertEquals(0, immutable.slabRefCntForTesting());
        for (Record original : records) {
            Record passed = immutable.get(original.key);
            assertNotNull("missing key " + original.key, passed);
            assertFalse("value must remain on-heap (no slab)",
                    passed.value.isOffHeap());
            assertEquals(original.value, passed.value);
        }
    }

    /**
     * Mixed zero-length and non-zero-length values inside the same mutable
     * page — the slab-pack writer must emit no bytes for empty values and
     * slice the next non-empty value at the right offset, just like the
     * existing {@link DataPage#buildSlabPackedImmutable} read-path factory.
     */
    @Test
    public void toImmutableHandlesMixedZeroAndNonZeroLengthValues() {
        List<Record> records = new ArrayList<>();
        records.add(new Record(Bytes.from_long(0), Bytes.from_array(payloadOf(512, 0xa))));
        records.add(new Record(Bytes.from_long(1), Bytes.EMPTY_ARRAY));
        records.add(new Record(Bytes.from_long(2), Bytes.from_array(payloadOf(512, 0xb))));
        records.add(new Record(Bytes.from_long(3), Bytes.EMPTY_ARRAY));
        records.add(new Record(Bytes.from_long(4), Bytes.from_array(payloadOf(3 * 1024, 0xc))));

        DataPage mutable = makeMutablePage(9L, 1024L * 1024L, records);
        DataPage immutable = mutable.toImmutable();
        assertTrue("aggregate values exceed the 4 KiB threshold",
                immutable.hasOffHeapValueSlab());

        for (Record original : records) {
            Record packed = immutable.get(original.key);
            assertNotNull("missing key " + original.key, packed);
            assertEquals("value length round-trip for key " + original.key,
                    original.value.getLength(), packed.value.getLength());
            assertEquals("value bytes round-trip for key " + original.key,
                    original.value, packed.value);
            if (original.value.getLength() == 0) {
                assertTrue(packed.value.isOffHeap());
                assertEquals(Bytes.EMPTY_ARRAY, packed.value);
            }
        }
        assertEquals(1, immutable.slabRefCntForTesting());
    }

    /**
     * Verifies that the page's tracked {@link DataPage#getUsedMemory()} after
     * toImmutable() stays within ±1 % of the mutable page's accounting.
     * Guards against the eviction budget silently drifting after the change.
     */
    @Test
    public void toImmutableEstimatedSizeStableAcrossPack() {
        List<Record> records = sampleRecords(64, 512); // 32 KiB > threshold
        DataPage mutable = makeMutablePage(10L, 1024L * 1024L, records);
        long mutableUsed = mutable.getUsedMemory();
        DataPage immutable = mutable.toImmutable();
        long immutableUsed = immutable.getUsedMemory();
        assertTrue("immutable page must be slab-packed for this test to be meaningful",
                immutable.hasOffHeapValueSlab());
        // Allow at most 1 % drift in either direction. The shared-slab
        // Bytes wrappers carry a slightly larger CONSTANT_BYTE_SIZE than the
        // on-heap variants; with 64 records and ~512-byte values the delta
        // is below 1 % even in the worst case.
        long drift = Math.abs(immutableUsed - mutableUsed);
        long onePercent = Math.max(1L, mutableUsed / 100L);
        assertTrue("usedMemory drift must stay within 1 % "
                        + "(mutable=" + mutableUsed + ", immutable=" + immutableUsed
                        + ", drift=" + drift + ", 1%=" + onePercent + ")",
                drift <= onePercent);
    }

    /**
     * The Cleaner-anchored slab returns to the pool after the new immutable
     * page (and every shared-slab {@code Bytes} extracted from it) becomes
     * GC-unreachable. Mirrors the pattern used by the existing
     * {@code DataPageOffHeapValueSlabTest#cleanerReleasesSlabWhenPageBecomesUnreachable}.
     */
    @Test
    public void cleanerReleasesSlabAfterToImmutable() throws Exception {
        List<Record> records = sampleRecords(64, 512); // 32 KiB > threshold
        DataPage mutable = makeMutablePage(11L, 1024L * 1024L, records);
        DataPage immutable = mutable.toImmutable();
        assertTrue(immutable.hasOffHeapValueSlab());

        // Snapshot the slab without pinning the page.
        Field f = DataPage.class.getDeclaredField("valueSlab");
        f.setAccessible(true);
        ByteBuf slab = (ByteBuf) f.get(immutable);
        assertNotNull(slab);
        assertEquals(1, slab.refCnt());

        // Drop every strong reference to the page and the records that the
        // shared-slab Bytes anchor against.
        immutable = null;
        records = null;
        mutable = null;

        long deadline = System.currentTimeMillis() + 10_000L;
        while (slab.refCnt() > 0 && System.currentTimeMillis() < deadline) {
            System.gc();
            // Give the Cleaner thread a moment to drain.
            Thread.sleep(50);
        }
        assertEquals("slab must be released by the Cleaner once the new"
                        + " immutable page is unreachable",
                0, slab.refCnt());
    }

    /**
     * The legacy mutable-page records (the ones still backed by on-heap
     * {@code byte[]}) stay readable after {@code toImmutable()} returns the
     * new slab-packed copy. This guards against any accidental side-effect
     * on the source page's contents.
     */
    @Test
    public void mutablePageContentsUnchangedAfterToImmutable() {
        List<Record> records = sampleRecords(64, 512); // 32 KiB > threshold
        DataPage mutable = makeMutablePage(12L, 1024L * 1024L, records);
        // Snapshot the mutable map's records before conversion.
        Map<Bytes, Record> snapshot = new HashMap<>();
        for (Record original : records) {
            snapshot.put(original.key, mutable.get(original.key));
        }

        DataPage immutable = mutable.toImmutable();
        assertTrue(immutable.hasOffHeapValueSlab());

        for (Record original : records) {
            Record stillMutable = mutable.get(original.key);
            assertNotNull(stillMutable);
            assertFalse("mutable-page records must remain on-heap-backed",
                    stillMutable.value.isOffHeap());
            // Identity preserved on the source page (no swap-in-place).
            assertTrue("toImmutable must not mutate the source page records",
                    snapshot.get(original.key) == stillMutable);
            assertEquals(original.value, stillMutable.value);
        }
    }

    private static byte[] payloadOf(int length, int seed) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) ((i * 31 + seed) & 0xff);
        }
        return out;
    }
}
