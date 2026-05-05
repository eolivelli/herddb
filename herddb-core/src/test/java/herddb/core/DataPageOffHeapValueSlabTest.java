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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.model.Record;
import herddb.utils.Bytes;
import herddb.utils.HerdDBByteBufAllocators;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for issue #399 step 3 — verifies that immutable data pages built
 * from disk reads pack their {@code Record.value} bytes into a single off-heap
 * slab allocated from {@link HerdDBByteBufAllocators#dataPagesAllocator()}, and
 * that the slab is released when the page becomes GC-unreachable.
 *
 * <p>These tests are intentionally narrow: they exercise the slab-packing
 * factory directly (no full {@code TableManager}), so they don't need cluster
 * infrastructure and remain in the core test workflow.
 */
public class DataPageOffHeapValueSlabTest {

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

    @Test
    public void slabPackedPageRoutesValuesThroughDataAllocator() throws Exception {
        List<Record> records = sampleRecords(64, 512); // 32 KiB total > 4 KiB threshold
        PooledByteBufAllocatorMetric m = HerdDBByteBufAllocators.dataPagesMetric();
        // The pool's `usedDirectMemory` counter tracks chunk-level allocation
        // (4 MiB at a time) rather than per-buffer requests, so we cannot
        // assert the counter moved with each tiny allocation. Instead verify
        // the page-pool counter is non-negative (sanity) and that the slab
        // itself is direct memory.
        assertTrue("data-pool used-direct-memory counter is non-negative",
                m.usedDirectMemory() >= 0L);
        DataPage page = DataPage.buildSlabPackedImmutable(null, 1L, 1024L * 1024L, records);
        try {
            assertTrue("page must be slab-packed", page.hasOffHeapValueSlab());
            // Reflect the slab and verify it really lives in direct memory
            // and was created by the data-pages allocator.
            java.lang.reflect.Field f = DataPage.class.getDeclaredField("valueSlab");
            f.setAccessible(true);
            io.netty.buffer.ByteBuf slab = (io.netty.buffer.ByteBuf) f.get(page);
            assertNotNull(slab);
            assertTrue("slab must be direct memory", slab.isDirect());
            assertTrue("slab must be allocated by the data-pages allocator (not DEFAULT)",
                    slab.alloc() == HerdDBByteBufAllocators.dataPagesAllocator());
            // Each record's value is now off-heap, anchored against this page.
            for (int i = 0; i < records.size(); i++) {
                Record r = page.get(Bytes.from_long(i));
                assertNotNull(r);
                assertTrue("value must be off-heap", r.value.isOffHeap());
                assertEquals("value length must round-trip",
                        records.get(i).value.getLength(), r.value.getLength());
                assertEquals("value bytes must round-trip",
                        records.get(i).value, r.value);
            }
            assertEquals("slab must hold exactly one initial refcount",
                    1, page.slabRefCntForTesting());
            assertEquals("offHeapBytes must report the slab capacity",
                    slab.capacity(), page.offHeapBytes());
        } finally {
            // Drop strong references to the page; GC + Cleaner will release.
            page = null;
        }
    }

    @Test
    public void smallPagesFallBackToHeapPath() {
        // Total value bytes (16 records × 16 = 256) is below the 4 KiB threshold.
        List<Record> records = sampleRecords(16, 16);
        DataPage page = DataPage.buildSlabPackedImmutable(null, 2L, 1024L * 1024L, records);
        assertFalse("tiny pages must keep on-heap values", page.hasOffHeapValueSlab());
        assertEquals(0, page.slabRefCntForTesting());
        for (int i = 0; i < records.size(); i++) {
            Record r = page.get(Bytes.from_long(i));
            assertNotNull(r);
            assertFalse("value must remain on-heap", r.value.isOffHeap());
            assertEquals(records.get(i).value, r.value);
        }
    }

    @Test
    public void slabPackedValuesMaterialiseOnDemand() {
        List<Record> records = sampleRecords(32, 256); // 8 KiB > 4 KiB threshold
        DataPage page = DataPage.buildSlabPackedImmutable(null, 3L, 1024L * 1024L, records);
        Record r = page.get(Bytes.from_long(7));
        assertNotNull(r);
        assertTrue("precondition: value off-heap", r.value.isOffHeap());
        // Materialise via getBuffer() — for a shared-slab Bytes this copies
        // bytes but must NOT release the slab.
        byte[] materialised = r.value.getBuffer();
        byte[] expected = records.get(7).value.to_array();
        // shared-slab materialisation does not null offHeap (so the slabOwner
        // anchor stays alive), but the on-heap path is now preferred.
        assertEquals(expected.length, materialised.length);
        assertArrayEquals(expected, java.util.Arrays.copyOf(materialised, expected.length));
        assertEquals("slab refcount must NOT be touched by per-record materialisation",
                1, page.slabRefCntForTesting());
    }

    @Test
    public void cleanerReleasesSlabWhenPageBecomesUnreachable() throws Exception {
        List<Record> records = sampleRecords(64, 512); // 32 KiB > threshold
        DataPage page = DataPage.buildSlabPackedImmutable(null, 4L, 1024L * 1024L, records);
        assertTrue(page.hasOffHeapValueSlab());

        // Copy the underlying ByteBuf reference so we can observe the
        // refcount without retaining the page or any anchored Bytes.
        io.netty.buffer.ByteBuf slab;
        java.lang.reflect.Field f = DataPage.class.getDeclaredField("valueSlab");
        f.setAccessible(true);
        slab = (io.netty.buffer.ByteBuf) f.get(page);
        assertNotNull(slab);
        assertEquals(1, slab.refCnt());

        // Drop strong references and force GC + Cleaner activity.
        page = null;
        records = null;

        long deadline = System.currentTimeMillis() + 10_000L;
        while (slab.refCnt() > 0 && System.currentTimeMillis() < deadline) {
            System.gc();
            // Give the Cleaner thread a moment to drain.
            Thread.sleep(50);
        }
        assertEquals("slab must be released by the Cleaner once the page is unreachable",
                0, slab.refCnt());
    }

    @Test
    public void buildOnHeapImmutableMatchesLegacyBehaviour() {
        List<Record> records = sampleRecords(8, 128);
        DataPage page = DataPage.buildOnHeapImmutable(null, 5L, 1024L * 1024L, records);
        assertFalse(page.hasOffHeapValueSlab());
        assertEquals(0, page.slabRefCntForTesting());
        for (int i = 0; i < records.size(); i++) {
            assertEquals(records.get(i).value, page.get(Bytes.from_long(i)).value);
        }
    }
}
