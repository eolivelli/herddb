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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import org.junit.Test;

public class IndexKeySlabTest {

    @Test
    public void appendAndWrapRoundTripsKeysIntoOffHeapSlab() {
        IndexKeySlab owner = new IndexKeySlab(64L,
                HerdDBByteBufAllocators.indexPagesAllocator());
        byte[] key1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        byte[] key2 = new byte[]{9, 10, 11, 12};
        int o1 = owner.append(key1);
        int o2 = owner.append(key2);
        assertEquals(0, o1);
        assertEquals(key1.length, o2);
        Bytes b1 = owner.wrap(o1, key1.length);
        Bytes b2 = owner.wrap(o2, key2.length);
        assertEquals(Bytes.from_array(key1), b1);
        assertEquals(Bytes.from_array(key2), b2);
        assertTrue(b1.isOffHeap());
        assertTrue(b2.isOffHeap());
        assertEquals("slab refcount must remain 1 across multiple wraps",
                1, owner.slabRefCntForTesting());
    }

    @Test
    public void slabAllocationGoesThroughIndexPagesPool() throws Exception {
        IndexKeySlab owner = new IndexKeySlab(128L,
                HerdDBByteBufAllocators.indexPagesAllocator());
        assertEquals(128, owner.slabCapacity());
        Field slabField = IndexKeySlab.class.getDeclaredField("slab");
        slabField.setAccessible(true);
        ByteBuf slab = (ByteBuf) slabField.get(owner);
        assertNotNull(slab);
        assertTrue("slab must be direct memory", slab.isDirect());
        assertTrue("slab must come from the index-pages pool",
                slab.alloc() == HerdDBByteBufAllocators.indexPagesAllocator());
    }

    @Test
    public void cleanerReleasesSlabWhenOwnerIsUnreachable() throws Exception {
        IndexKeySlab owner = new IndexKeySlab(64L,
                HerdDBByteBufAllocators.indexPagesAllocator());
        owner.append(new byte[]{1, 2, 3, 4});
        Bytes b = owner.wrap(0, 4);
        // While we hold either `owner` or `b`, the slab stays alive.
        Field f = IndexKeySlab.class.getDeclaredField("slab");
        f.setAccessible(true);
        ByteBuf slab = (ByteBuf) f.get(owner);
        assertEquals(1, slab.refCnt());
        // Drop both references and force GC.
        owner = null;
        b = null;

        long deadline = System.currentTimeMillis() + 10_000L;
        while (slab.refCnt() > 0 && System.currentTimeMillis() < deadline) {
            System.gc();
            Thread.sleep(50);
        }
        assertEquals("slab must be released by the Cleaner", 0, slab.refCnt());
    }

    @Test
    public void rejectsNegativeOrOversizedTotalKeyBytes() {
        assertThrows(IllegalArgumentException.class, () -> new IndexKeySlab(-1L,
                HerdDBByteBufAllocators.indexPagesAllocator()));
        assertThrows(IllegalArgumentException.class,
                () -> new IndexKeySlab(((long) Integer.MAX_VALUE) + 1L,
                        HerdDBByteBufAllocators.indexPagesAllocator()));
    }

    @Test
    public void rejectsNullAllocator() {
        assertThrows(NullPointerException.class, () -> new IndexKeySlab(64L, null));
    }

    @Test
    public void zeroLengthKeyAppendsAndWrapsCorrectly() {
        IndexKeySlab owner = new IndexKeySlab(8L,
                HerdDBByteBufAllocators.indexPagesAllocator());
        int off = owner.append(new byte[0]);
        assertEquals(0, off);
        Bytes b = owner.wrap(0, 0);
        assertEquals(Bytes.EMPTY_ARRAY, b);
    }
}
