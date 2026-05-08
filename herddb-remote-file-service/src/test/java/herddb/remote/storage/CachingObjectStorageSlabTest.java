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
import static org.junit.Assert.assertTrue;
import herddb.remote.storage.CachingObjectStorageTest.FakeObjectStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end tests for the slab disk cache layout in {@link CachingObjectStorage}
 * (issue #475). Verifies tier selection across small / large / fallback, eviction
 * across tiers under a tight budget, the {@code isInCache} introspection helper,
 * and the {@link CachingObjectStorage#setMaxCacheBytes} resize path.
 *
 * @author enrico.olivelli
 */
public class CachingObjectStorageSlabTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    private CachingObjectStorage build(FakeObjectStorage inner, long maxBytes,
                                       CachingObjectStorage.SlabConfig slabConfig) throws Exception {
        Path cacheDir = folder.newFolder().toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, maxBytes, slabConfig);
    }

    /** Reasonable defaults tuned for unit testing: small 4 KiB cells, large 64 KiB cells. */
    private static CachingObjectStorage.SlabConfig defaultTestSlabConfig() {
        // smallCell=4K, smallTier=8K (=> 2 cells); largeCell=64K, largeTier=128K (=> 2 cells).
        return new CachingObjectStorage.SlabConfig(true, 4 * 1024, 8 * 1024, 64 * 1024, 128 * 1024);
    }

    @Test
    public void testTierSelectionByPayloadSize() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024, defaultTestSlabConfig());

        byte[] tiny = new byte[1024];
        byte[] mid = new byte[16 * 1024];
        byte[] huge = new byte[256 * 1024]; // > 64K largeCell -> fallback

        caching.write("a/tiny.page", tiny).get();
        caching.write("b/mid.page", mid).get();
        caching.write("c/huge.page", huge).get();

        // All three must be reachable via read().
        assertTrue(caching.isInCache("a/tiny.page"));
        assertTrue(caching.isInCache("b/mid.page"));
        assertTrue(caching.isInCache("c/huge.page"));

        // Slab counters should reflect tier assignment.
        assertNotNull(caching.getSmallSlab());
        assertNotNull(caching.getLargeSlab());
        assertEquals(1, caching.getSmallSlab().liveCells());
        assertEquals(1, caching.getLargeSlab().liveCells());
        assertEquals(1L, caching.getFallbackFileCount());
        assertEquals(huge.length, caching.getFallbackBytes());
    }

    @Test
    public void testRoundTripFromEachTier() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024, defaultTestSlabConfig());

        byte[] tiny = new byte[100];
        Arrays.fill(tiny, (byte) 0x11);
        byte[] mid = new byte[5000];
        Arrays.fill(mid, (byte) 0x22);
        byte[] huge = new byte[80 * 1024];
        Arrays.fill(huge, (byte) 0x33);

        caching.write("tiny", tiny).get();
        caching.write("mid", mid).get();
        caching.write("huge", huge).get();

        ReadResult t = caching.read("tiny").get();
        try {
            assertEquals(ReadResult.Status.FOUND, t.status());
            assertArrayEquals(tiny, t.content());
        } finally {
            t.release();
        }
        ReadResult m = caching.read("mid").get();
        try {
            assertArrayEquals(mid, m.content());
        } finally {
            m.release();
        }
        ReadResult h = caching.read("huge").get();
        try {
            assertArrayEquals(huge, h.content());
        } finally {
            h.release();
        }
    }

    @Test
    public void testReadRangeFromSlabTier() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024, defaultTestSlabConfig());

        byte[] block = new byte[5000];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        caching.writeBlock("big.page", 0, block).get();

        // The block fits in the large tier (5000 < 64K) but not in the small tier (5000 > 4K).
        // It MUST go into the large tier — verify by reading a slice and checking the bytes.
        int innerReadsBefore = inner.readCalls.get();
        ReadResult slice = caching.readRange("big.page", 1000, 64, 4096).get();
        try {
            assertEquals(ReadResult.Status.FOUND, slice.status());
            assertEquals("inner.read must NOT be called for a slab-cached slice",
                    innerReadsBefore, inner.readCalls.get());
            byte[] sliceBytes = slice.content();
            // readRange uses blockSize=4096 here, so blockIndex=0, offsetInBlock=1000;
            // length=64 bytes -> the cached block is at multipart/0 with the 5000 bytes.
            byte[] expected = Arrays.copyOfRange(block, 1000, 1064);
            assertArrayEquals(expected, sliceBytes);
        } finally {
            slice.release();
        }
    }

    @Test
    public void testEvictionAcrossTiersUnderTightBudget() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        // Tight budget: small tier 4 cells of 1 KiB, large tier 4 cells of 4 KiB. Caffeine
        // bytes budget = 8 KiB so the LRU must evict to keep total bytes bounded.
        CachingObjectStorage.SlabConfig cfg =
                new CachingObjectStorage.SlabConfig(true, 1024, 4 * 1024, 4096, 16 * 1024);
        CachingObjectStorage caching = build(inner, 8 * 1024, cfg);

        // Admit 8 entries of 3 KiB each → 24 KiB > 8 KiB budget, so Caffeine must
        // evict at least 5 of them to respect the budget.
        for (int i = 0; i < 8; i++) {
            caching.write("k" + i, new byte[3 * 1024]).get();
        }
        caching.cleanUp();

        long resident = 0;
        for (int i = 0; i < 8; i++) {
            if (caching.isInCache("k" + i)) {
                resident++;
            }
        }
        assertTrue("at most 3 entries should fit under the 8 KiB budget, got " + resident,
                resident <= 3);
        assertTrue("at least one entry should still be resident, got " + resident,
                resident >= 1);
        // Bytes-in-use across slab tiers must respect the budget.
        long slabBytes = caching.getSmallSlab().bytesInUse() + caching.getLargeSlab().bytesInUse()
                + caching.getFallbackBytes();
        assertTrue("slab+fallback bytes must respect Caffeine budget",
                slabBytes <= 8 * 1024 + 3 * 1024); // tolerate one in-flight admit
    }

    @Test
    public void testTierFullFallsThroughToNextTier() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        // Small tier: 1 cell of 1 KiB (room for one small entry only); large tier: 8 cells of
        // 16 KiB (plenty of headroom). Caffeine budget large enough to keep both resident.
        CachingObjectStorage.SlabConfig cfg =
                new CachingObjectStorage.SlabConfig(true, 1024, 1024, 16 * 1024, 128 * 1024);
        CachingObjectStorage caching = build(inner, 256 * 1024, cfg);

        byte[] payload = new byte[800]; // fits small tier
        caching.write("a", payload).get();
        assertEquals(1, caching.getSmallSlab().liveCells());
        assertEquals(0, caching.getLargeSlab().liveCells());

        // Second admit can't fit in the small tier (only one cell), should fall through
        // to the large tier (which has plenty of cells).
        caching.write("b", payload).get();
        assertEquals(1, caching.getSmallSlab().liveCells());
        assertEquals(1, caching.getLargeSlab().liveCells());
        assertEquals("admit-full counter must record the fall-through",
                1, caching.getSmallSlab().admitFullCount());
    }

    @Test
    public void testDeleteInvalidatesAcrossTiers() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024, defaultTestSlabConfig());

        caching.write("small/x", new byte[1024]).get();
        caching.write("large/y", new byte[16 * 1024]).get();
        caching.write("huge/z", new byte[200 * 1024]).get(); // fallback

        assertTrue(caching.isInCache("small/x"));
        assertTrue(caching.isInCache("large/y"));
        assertTrue(caching.isInCache("huge/z"));

        caching.delete("small/x").get();
        caching.delete("large/y").get();
        caching.delete("huge/z").get();

        assertFalse(caching.isInCache("small/x"));
        assertFalse(caching.isInCache("large/y"));
        assertFalse(caching.isInCache("huge/z"));
        assertEquals(0L, caching.getFallbackFileCount());
        assertEquals(0L, caching.getFallbackBytes());
    }

    @Test
    public void testSetMaxCacheBytesAffectsCaffeineBudget() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 8 * 1024, defaultTestSlabConfig());
        assertEquals(8 * 1024L, caching.getMaxCacheBytes());

        caching.setMaxCacheBytes(64 * 1024L);
        assertEquals(64 * 1024L, caching.getMaxCacheBytes());

        // Verify entries that previously would have been evicted now stay resident.
        for (int i = 0; i < 4; i++) {
            caching.write("k" + i, new byte[8 * 1024]).get();
        }
        caching.cleanUp();
        // 4 * 8 KiB = 32 KiB, well within the 64 KiB budget; all four should be present.
        for (int i = 0; i < 4; i++) {
            assertTrue("k" + i + " should still be resident under expanded budget",
                    caching.isInCache("k" + i));
        }
    }

    @Test
    public void testDeleteByPrefixInvalidatesAllTiers() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024, defaultTestSlabConfig());

        caching.write("ts1/small", new byte[500]).get();
        caching.write("ts1/big", new byte[20 * 1024]).get();
        caching.write("ts1/huge", new byte[200 * 1024]).get();
        caching.write("ts2/small", new byte[500]).get();

        int deleted = caching.deleteByPrefix("ts1/").get();
        assertEquals(3, deleted);

        assertFalse(caching.isInCache("ts1/small"));
        assertFalse(caching.isInCache("ts1/big"));
        assertFalse(caching.isInCache("ts1/huge"));
        assertTrue(caching.isInCache("ts2/small"));
        assertEquals(0L, caching.getFallbackFileCount());
    }

    @Test
    public void testInnerBufferReleasedWhenSlabAdmitFailsOnLoadAndCache() throws Exception {
        // Regression test for the refcount-leak path called out by pr-reviewer:
        // when CachingObjectStorage.loadAndCache hands a pooled direct ByteBuf to
        // admitChooseTier(ByteBuf) and the slab admit completes EXCEPTIONALLY (e.g.
        // closed channel, disk-full, I/O error), the outer buf retain held alongside
        // the retainedDuplicate handed to the slab must still be released exactly
        // once. Without the whenComplete-on-error guard, that retain leaked because
        // thenCompose only fires on successful completion.
        AtomicReference<ByteBuf> capturedBuf = new AtomicReference<>();
        FakeObjectStorage inner = new FakeObjectStorage() {
            @Override
            public CompletableFuture<ReadResult> read(String path) {
                readCalls.incrementAndGet();
                byte[] bytes = data.get(path);
                if (bytes == null) {
                    return CompletableFuture.completedFuture(ReadResult.notFound());
                }
                ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
                buf.writeBytes(bytes);
                capturedBuf.set(buf);
                return CompletableFuture.completedFuture(ReadResult.found(buf));
            }
        };
        byte[] payload = "payload bytes for slab admit failure".getBytes();
        inner.data.put("k1", payload);

        try (CachingObjectStorage caching = build(inner, 1024 * 1024, defaultTestSlabConfig())) {
            // Close both slabs so any tryStore call resolves exceptionally.
            caching.getSmallSlab().close();
            caching.getLargeSlab().close();

            // Trigger the slab admit failure via loadAndCache. The future returned
            // by caching.read should fail (admitToDisk's whenComplete propagates
            // the exception to the read future).
            ExecutionException thrown = null;
            try {
                ReadResult result = caching.read("k1").get();
                if (result != null) {
                    result.release();
                }
            } catch (ExecutionException expected) {
                thrown = expected;
            }
            assertNotNull("read must propagate the slab admit failure", thrown);
            assertNotNull("inner.read must have been called", capturedBuf.get());
            assertEquals(
                    "inner pooled ByteBuf must be fully released after slab admit failure",
                    0, capturedBuf.get().refCnt());
        }
    }

    @Test
    public void testSlabDisabledRoutesEverythingToFallback() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024,
                CachingObjectStorage.SlabConfig.disabled());
        // No slab tiers should be present.
        assertEquals(null, caching.getSmallSlab());
        assertEquals(null, caching.getLargeSlab());

        caching.write("small/x", new byte[100]).get();
        caching.write("medium/y", new byte[10 * 1024]).get();
        caching.write("big/z", new byte[200 * 1024]).get();

        // All three go to the fallback.
        assertEquals(3L, caching.getFallbackFileCount());
        assertEquals(100L + 10 * 1024 + 200 * 1024, caching.getFallbackBytes());

        // Reads still work end-to-end.
        ReadResult r = caching.read("medium/y").get();
        try {
            assertEquals(ReadResult.Status.FOUND, r.status());
            assertEquals(10 * 1024, r.content().length);
        } finally {
            r.release();
        }
    }
}
