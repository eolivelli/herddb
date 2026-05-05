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

package herddb.index.brin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.index.brin.BRINIndexManager.PageContents;
import herddb.index.brin.BlockRangeIndexMetadata.BlockMetadata;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Verifies issue #399 step 5: BRIN data and metadata pages deserialise
 * with off-heap-backed Bytes for keys (and values for blockdata) when the
 * aggregate page bytes exceed the {@code IndexKeySlab} threshold.
 *
 * <p>Drives {@code PageContents.serialize}/{@code deserialize} directly
 * via the package-private API rather than spinning up a full BRIN index,
 * so the tests stay fast and focused on the slab-pack contract.
 */
public class BRINOffHeapKeysTest {

    @Test
    public void blockDataPageSlabPacksKeysAndValuesWhenOverThreshold() throws Exception {
        // 64 entries × (32-byte key + 32-byte value) = 4096 bytes total = 4 KiB,
        // which is exactly the slab threshold; bump entry count to be safely
        // over.
        final int n = 80;
        List<Map.Entry<Bytes, Bytes>> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            entries.add(new AbstractMap.SimpleImmutableEntry<>(
                    Bytes.from_array(make(32, 0xa00 + i)),
                    Bytes.from_array(make(32, 0xb00 + i))));
        }
        PageContents source = new PageContents();
        source.setType(PageContents.TYPE_BLOCKDATA);
        source.setPageData(entries);
        byte[] serialised = source.serialize();
        PageContents loaded = PageContents.deserialize(serialised);

        assertEquals(PageContents.TYPE_BLOCKDATA, loaded.getType());
        assertEquals(n, loaded.getPageData().size());

        boolean anyOffHeap = false;
        for (int i = 0; i < n; i++) {
            Map.Entry<Bytes, Bytes> orig = entries.get(i);
            Map.Entry<Bytes, Bytes> got = loaded.getPageData().get(i);
            assertEquals("key round-trip @ " + i, orig.getKey(), got.getKey());
            assertEquals("value round-trip @ " + i, orig.getValue(), got.getValue());
            if (got.getKey().isOffHeap() || got.getValue().isOffHeap()) {
                anyOffHeap = true;
            }
        }
        assertTrue("at least one BRIN blockdata key/value must be off-heap-backed",
                anyOffHeap);
    }

    @Test
    public void smallBlockDataPageStaysOnHeap() throws Exception {
        // 4 entries × (4-byte key + 4-byte value) = 32 bytes — well below
        // the 4 KiB threshold; the slab path must NOT fire.
        List<Map.Entry<Bytes, Bytes>> entries = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            entries.add(new AbstractMap.SimpleImmutableEntry<>(
                    Bytes.from_int(i), Bytes.from_int(100 + i)));
        }
        PageContents source = new PageContents();
        source.setType(PageContents.TYPE_BLOCKDATA);
        source.setPageData(entries);
        byte[] serialised = source.serialize();
        PageContents loaded = PageContents.deserialize(serialised);

        assertEquals(4, loaded.getPageData().size());
        for (int i = 0; i < 4; i++) {
            assertEquals(entries.get(i).getKey(), loaded.getPageData().get(i).getKey());
            assertEquals(entries.get(i).getValue(), loaded.getPageData().get(i).getValue());
            assertFalse("small page key must remain on-heap",
                    loaded.getPageData().get(i).getKey().isOffHeap());
            assertFalse("small page value must remain on-heap",
                    loaded.getPageData().get(i).getValue().isOffHeap());
        }
    }

    @Test
    public void metadataPageSlabPacksFirstKeysWhenOverThreshold() throws Exception {
        // 256 blocks × ~24-byte firstKey = ~6 KiB > 4 KiB threshold.
        final int n = 256;
        List<BlockMetadata<Bytes>> blocks = new ArrayList<>();
        // First block is HEAD (firstKey == null).
        blocks.add(new BlockMetadata<>(null, 1L, 100L, 1L, 2L));
        for (int i = 1; i < n - 1; i++) {
            blocks.add(new BlockMetadata<>(
                    Bytes.from_array(make(24, 0xc00 + i)),
                    (long) (i + 1), 100L, (long) (i + 1), (long) (i + 2)));
        }
        // Last block is TAIL (nextBlockId == null).
        blocks.add(new BlockMetadata<>(
                Bytes.from_array(make(24, 0xc00 + n)),
                (long) n, 100L, (long) n, null));

        PageContents source = new PageContents();
        source.setType(PageContents.TYPE_METADATA);
        source.setMetadata(blocks);
        byte[] serialised = source.serialize();
        PageContents loaded = PageContents.deserialize(serialised);

        assertEquals(PageContents.TYPE_METADATA, loaded.getType());
        assertEquals(n, loaded.getMetadata().size());

        boolean anyOffHeap = false;
        for (int i = 0; i < n; i++) {
            BlockMetadata<Bytes> orig = blocks.get(i);
            BlockMetadata<Bytes> got = loaded.getMetadata().get(i);
            if (orig.firstKey == null) {
                assertNull("HEAD block firstKey must be null after round-trip",
                        got.firstKey);
            } else {
                assertEquals("firstKey round-trip @ " + i, orig.firstKey, got.firstKey);
                if (got.firstKey.isOffHeap()) {
                    anyOffHeap = true;
                }
            }
            assertEquals(orig.blockId, got.blockId);
            assertEquals(orig.size, got.size);
            assertEquals(orig.pageId, got.pageId);
            assertEquals(orig.nextBlockId, got.nextBlockId);
        }
        assertTrue("at least one BRIN metadata firstKey must be off-heap-backed",
                anyOffHeap);
    }

    private static byte[] make(int len, int seed) {
        byte[] out = new byte[len];
        for (int j = 0; j < len; j++) {
            out[j] = (byte) ((j * 31 + seed) & 0xff);
        }
        return out;
    }
}
