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

package herddb.remote.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SlabCacheFileStore} (issue #475).
 *
 * @author enrico.olivelli
 */
public class SlabCacheFileStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private SlabCacheFileStore openSlab(int cellSize, int totalCells) throws Exception {
        Path slab = folder.newFolder().toPath().resolve("slab.dat");
        return new SlabCacheFileStore(slab, cellSize, totalCells);
    }

    private static ByteBuf bufOf(byte[] bytes) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
        buf.writeBytes(bytes);
        return buf;
    }

    private static byte[] readAll(SlabCacheFileStore slab, String key) throws Exception {
        ByteBuf buf = slab.read(key).get();
        if (buf == null) {
            return null;
        }
        try {
            byte[] copy = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), copy);
            return copy;
        } finally {
            buf.release();
        }
    }

    @Test
    public void testStoreAndReadFullPayload() throws Exception {
        try (SlabCacheFileStore slab = openSlab(4096, 4)) {
            byte[] payload = "hello world".getBytes();
            SlabCacheFileStore.Slot slot = slab.tryStore("k1", bufOf(payload)).get();
            assertNotNull(slot);
            assertEquals(payload.length, slot.payloadLength());

            assertArrayEquals(payload, readAll(slab, "k1"));
            assertEquals(payload.length, slab.bytesInUse());
            assertEquals(1, slab.liveCells());
            assertEquals(3, slab.freeCells());
            assertEquals(1, slab.admitCount());
            assertEquals(0, slab.admitFullCount());
            assertEquals(0, slab.evictCount());
        }
    }

    @Test
    public void testReadRangeReturnsCorrectSlice() throws Exception {
        try (SlabCacheFileStore slab = openSlab(4096, 1)) {
            byte[] payload = new byte[2048];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i & 0xFF);
            }
            slab.tryStore("k1", bufOf(payload)).get();

            ByteBuf slice = slab.readRange("k1", 1000, 16).get();
            try {
                byte[] sliceBytes = new byte[slice.readableBytes()];
                slice.getBytes(slice.readerIndex(), sliceBytes);
                assertEquals(16, sliceBytes.length);
                assertArrayEquals(Arrays.copyOfRange(payload, 1000, 1016), sliceBytes);
            } finally {
                slice.release();
            }
        }
    }

    @Test
    public void testReadRangeOutOfBoundsReturnsNull() throws Exception {
        try (SlabCacheFileStore slab = openSlab(4096, 1)) {
            byte[] payload = new byte[100];
            slab.tryStore("k1", bufOf(payload)).get();

            ByteBuf empty = slab.readRange("k1", 100, 16).get();
            assertNull(empty);
            ByteBuf empty2 = slab.readRange("k1", 1000, 16).get();
            assertNull(empty2);
        }
    }

    @Test
    public void testReadRangeClampsToPayloadLength() throws Exception {
        try (SlabCacheFileStore slab = openSlab(4096, 1)) {
            byte[] payload = new byte[100];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }
            slab.tryStore("k1", bufOf(payload)).get();

            ByteBuf slice = slab.readRange("k1", 90, 1000).get();
            try {
                assertEquals(10, slice.readableBytes());
                byte[] sliceBytes = new byte[slice.readableBytes()];
                slice.getBytes(slice.readerIndex(), sliceBytes);
                assertArrayEquals(Arrays.copyOfRange(payload, 90, 100), sliceBytes);
            } finally {
                slice.release();
            }
        }
    }

    @Test
    public void testTryStoreRejectsOversizedPayload() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 4)) {
            byte[] payload = new byte[65];
            SlabCacheFileStore.Slot slot = slab.tryStore("k1", bufOf(payload)).get();
            assertNull("oversize payload must return null", slot);
            assertEquals(0, slab.liveCells());
            assertEquals(4, slab.freeCells());
            assertEquals(0, slab.admitCount());
            // Oversize is NOT counted as admit-full — admit-full is for tier-full only.
            assertEquals(0, slab.admitFullCount());
        }
    }

    @Test
    public void testTryStoreReportsTierFullAndIncrementsCounter() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 1)) {
            // First admit succeeds.
            assertNotNull(slab.tryStore("k1", bufOf(new byte[10])).get());
            // Second admit fails because the only cell is in use.
            SlabCacheFileStore.Slot rejected = slab.tryStore("k2", bufOf(new byte[10])).get();
            assertNull(rejected);
            assertEquals(1, slab.admitFullCount());
            assertEquals(1, slab.liveCells());
        }
    }

    @Test
    public void testFreeRecyclesCellAndRemovesFromIndex() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 1)) {
            slab.tryStore("k1", bufOf(new byte[10])).get();
            assertTrue(slab.contains("k1"));
            assertTrue(slab.markEvicted("k1"));
            assertFalse(slab.contains("k1"));
            assertEquals(1, slab.evictCount());
            assertEquals(0, slab.bytesInUse());
            assertEquals(0, slab.liveCells());
            assertEquals(1, slab.freeCells());

            // Cell can now be reused for a new key.
            byte[] payload = "second".getBytes();
            SlabCacheFileStore.Slot slot = slab.tryStore("k2", bufOf(payload)).get();
            assertNotNull(slot);
            assertArrayEquals(payload, readAll(slab, "k2"));
        }
    }

    @Test
    public void testRecycledCellHoldsNewBytes() throws Exception {
        // Direct test of "no torn read after recycle" — we deliberately admit, free,
        // and re-admit a different payload at the same key, then ensure the read
        // returns the new bytes (and definitely not bytes from the previous admit).
        try (SlabCacheFileStore slab = openSlab(4096, 1)) {
            byte[] first = new byte[200];
            Arrays.fill(first, (byte) 0x11);
            slab.tryStore("k", bufOf(first)).get();
            slab.markEvicted("k");

            byte[] second = new byte[200];
            Arrays.fill(second, (byte) 0x22);
            slab.tryStore("k", bufOf(second)).get();
            assertArrayEquals(second, readAll(slab, "k"));
        }
    }

    @Test
    public void testReadOfMissingKeyReturnsNull() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 1)) {
            assertNull(slab.read("missing").get());
            assertNull(slab.readRange("missing", 0, 10).get());
        }
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        SlabCacheFileStore slab = openSlab(64, 2);
        slab.close();
        slab.close(); // must not throw
    }

    @Test
    public void testCloseDeletesSlabFile() throws Exception {
        Path slabPath = folder.newFolder().toPath().resolve("slab.dat");
        SlabCacheFileStore slab = new SlabCacheFileStore(slabPath, 64, 2);
        assertTrue(Files.exists(slabPath));
        slab.close();
        assertFalse("close() must delete the slab file", Files.exists(slabPath));
    }

    @Test
    public void testZeroCellsTier() throws Exception {
        // A zero-capacity tier accepts no admissions and counts every attempt as full.
        try (SlabCacheFileStore slab = openSlab(64, 0)) {
            assertEquals(0, slab.totalCells());
            assertEquals(0, slab.freeCells());
            SlabCacheFileStore.Slot rejected = slab.tryStore("k1", bufOf(new byte[10])).get();
            assertNull(rejected);
            assertEquals(1, slab.admitFullCount());
        }
    }

    @Test
    public void testInvalidArgsThrow() throws Exception {
        Path slabPath = folder.newFolder().toPath().resolve("slab.dat");
        try {
            new SlabCacheFileStore(slabPath, 0, 1).close();
            fail("expected IllegalArgumentException for cellSize=0");
        } catch (IllegalArgumentException expected) {
            // Expected: cellSize must be > 0.
            assertTrue(expected.getMessage().contains("cellSize"));
        }
        try {
            new SlabCacheFileStore(slabPath, 64, -1).close();
            fail("expected IllegalArgumentException for negative totalCells");
        } catch (IllegalArgumentException expected) {
            // Expected: totalCells must be >= 0.
            assertTrue(expected.getMessage().contains("totalCells"));
        }
    }

    @Test
    public void testReadRangeNegativeArgs() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 1)) {
            slab.tryStore("k1", bufOf(new byte[10])).get();
            try {
                slab.readRange("k1", -1, 5).get();
                fail("expected IllegalArgumentException for negative offset");
            } catch (ExecutionException expected) {
                assertTrue(expected.getCause() instanceof IllegalArgumentException);
            }
            try {
                slab.readRange("k1", 0, -5).get();
                fail("expected IllegalArgumentException for negative length");
            } catch (ExecutionException expected) {
                assertTrue(expected.getCause() instanceof IllegalArgumentException);
            }
        }
    }

    @Test
    public void testEachCellGetsDistinctSlot() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 4)) {
            Set<Integer> seenCells = new HashSet<>();
            for (int i = 0; i < 4; i++) {
                byte[] payload = ("cell-" + i).getBytes();
                SlabCacheFileStore.Slot slot = slab.tryStore("k" + i, bufOf(payload)).get();
                assertNotNull(slot);
                assertTrue("cells must be distinct, got duplicate: " + slot.cellIndex(),
                        seenCells.add(slot.cellIndex()));
            }
            // Verify all four payloads round-trip correctly to their key.
            for (int i = 0; i < 4; i++) {
                assertArrayEquals(("cell-" + i).getBytes(), readAll(slab, "k" + i));
            }
        }
    }

    @Test
    public void testStorePayloadOfEverySize() throws Exception {
        // Sweep the tier with payloads of size 1, 2, 3, ..., cellSize (subject to the
        // single-cell capacity by freeing each between admits).
        int cellSize = 257;
        try (SlabCacheFileStore slab = openSlab(cellSize, 1)) {
            for (int size = 1; size <= cellSize; size++) {
                byte[] payload = new byte[size];
                for (int i = 0; i < size; i++) {
                    payload[i] = (byte) ((i + size) & 0xFF);
                }
                SlabCacheFileStore.Slot slot = slab.tryStore("k", bufOf(payload)).get();
                assertNotNull("size " + size + " must succeed", slot);
                assertArrayEquals("size " + size + " round-trip", payload, readAll(slab, "k"));
                slab.markEvicted("k");
            }
        }
    }

    @Test
    public void testBytesInUseTracksLiveBytesAcrossAdmitFreeRecycle() throws Exception {
        try (SlabCacheFileStore slab = openSlab(4096, 4)) {
            slab.tryStore("a", bufOf(new byte[100])).get();
            assertEquals(100, slab.bytesInUse());
            slab.tryStore("b", bufOf(new byte[200])).get();
            assertEquals(300, slab.bytesInUse());
            slab.tryStore("c", bufOf(new byte[50])).get();
            assertEquals(350, slab.bytesInUse());
            slab.markEvicted("b");
            assertEquals(150, slab.bytesInUse());
            slab.tryStore("d", bufOf(new byte[400])).get();
            assertEquals(550, slab.bytesInUse());
        }
    }

    @Test
    public void testLiveKeysSnapshot() throws Exception {
        try (SlabCacheFileStore slab = openSlab(64, 4)) {
            slab.tryStore("a", bufOf(new byte[10])).get();
            slab.tryStore("b", bufOf(new byte[10])).get();
            List<String> keys = new ArrayList<>(slab.liveKeys());
            keys.sort(String::compareTo);
            assertEquals(Arrays.asList("a", "b"), keys);
        }
    }
}
