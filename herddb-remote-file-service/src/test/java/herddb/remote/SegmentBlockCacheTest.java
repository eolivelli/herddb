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

package herddb.remote;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Covers the {@link SegmentBlockCache} behavioural contract inherited from
 * the deleted {@code SharedSegmentPageCache}: hits vs misses, concurrent
 * single-flight loading, byte-weighted eviction, path invalidation, stats
 * accessors, and pass-through mode via {@link SegmentBlockCache#disabled()}.
 */
public class SegmentBlockCacheTest {

    private static final int BLOCK = 1024;

    /** Allocates a pooled direct buffer with the given byte pattern. */
    private static ByteBuf direct(int len, byte value) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
        for (int i = 0; i < len; i++) {
            buf.writeByte(value);
        }
        return buf;
    }

    private static byte[] readAllAndRelease(ByteBuf buf) {
        try {
            byte[] out = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), out);
            return out;
        } finally {
            buf.release();
        }
    }

    @Test
    public void firstLoadInvokesLoaderAndCachesResult() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger loads = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            loads.incrementAndGet();
            return direct(BLOCK, (byte) 0x42);
        };

        ByteBuf first = cache.getBlock("seg", 0L, BLOCK, loader);
        ByteBuf second = cache.getBlock("seg", 0L, BLOCK, loader);

        try {
            assertEquals("loader invoked exactly once", 1, loads.get());
            assertEquals("two retained slices have identical content",
                    first.getByte(0), second.getByte(0));
        } finally {
            first.release();
            second.release();
        }
        assertEquals(1L, cache.hitCount());
        assertEquals(1L, cache.missCount());
        assertEquals(1L, cache.loadSuccessCount());
    }

    @Test
    public void differentOffsetsProduceDifferentEntries() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) off);

        byte[] b0 = readAllAndRelease(cache.getBlock("seg", 0L, BLOCK, loader));
        byte[] b1 = readAllAndRelease(cache.getBlock("seg", BLOCK, BLOCK, loader));

        assertEquals((byte) 0, b0[0]);
        assertEquals((byte) BLOCK, b1[0]);
        assertEquals(2L, cache.estimatedSize());
    }

    @Test
    public void concurrentMissesOnSameKeyInvokeLoaderOnce() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger loaderCalls = new AtomicInteger();
        CountDownLatch inLoader = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);

        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            loaderCalls.incrementAndGet();
            inLoader.countDown();
            try {
                releaseLoader.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
            return direct(BLOCK, (byte) 0xA5);
        };

        ExecutorService pool = Executors.newFixedThreadPool(32);
        try {
            Set<Byte> observedFirstByte = Collections.newSetFromMap(new ConcurrentHashMap<>());
            CountDownLatch started = new CountDownLatch(32);
            for (int i = 0; i < 32; i++) {
                pool.submit(() -> {
                    started.countDown();
                    try {
                        ByteBuf slice = cache.getBlock("seg", 0L, BLOCK, loader);
                        try {
                            observedFirstByte.add(slice.getByte(0));
                        } finally {
                            slice.release();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue("at least one thread should enter the loader",
                    inLoader.await(5, TimeUnit.SECONDS));
            releaseLoader.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals("loader invoked once despite 32 concurrent misses",
                    1, loaderCalls.get());
            assertEquals("all waiters see identical bytes", 1, observedFirstByte.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void byteWeightedEvictionRespectsBudget() throws Exception {
        // Budget = 4 KB, entries = 1 KB each. Loading 20 entries forces eviction.
        SegmentBlockCache cache = new SegmentBlockCache(4L * BLOCK);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 1);

        for (int i = 0; i < 20; i++) {
            cache.getBlock("seg", (long) i * BLOCK, BLOCK, loader).release();
        }
        cache.cleanUp();

        assertTrue("some entries must have been evicted to respect the byte budget",
                cache.evictionCount() > 0);
        assertTrue("weightedSize within budget after drain, got " + cache.weightedSize(),
                cache.weightedSize() <= 4L * BLOCK);
    }

    @Test
    public void invalidatePathDropsOnlyMatchingEntries() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);
        cache.getBlock("segA", 0L, BLOCK, loader).release();
        cache.getBlock("segA", BLOCK, BLOCK, loader).release();
        cache.getBlock("segB", 0L, BLOCK, loader).release();

        cache.invalidatePath("segA");

        assertFalse(cache.containsBlock("segA", 0L, BLOCK));
        assertFalse(cache.containsBlock("segA", BLOCK, BLOCK));
        assertTrue(cache.containsBlock("segB", 0L, BLOCK));
    }

    @Test
    public void invalidatePrefixDropsAllEntriesUnderPrefix() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);
        cache.getBlock("ts1/idx/a", 0L, BLOCK, loader).release();
        cache.getBlock("ts1/idx/b", 0L, BLOCK, loader).release();
        cache.getBlock("ts2/idx/a", 0L, BLOCK, loader).release();

        cache.invalidatePrefix("ts1/");

        assertFalse(cache.containsBlock("ts1/idx/a", 0L, BLOCK));
        assertFalse(cache.containsBlock("ts1/idx/b", 0L, BLOCK));
        assertTrue(cache.containsBlock("ts2/idx/a", 0L, BLOCK));
    }

    @Test
    public void loaderIoExceptionIsSurfacedAndNotCached() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader failingThenOk = (p, off, len) -> {
            int call = calls.incrementAndGet();
            if (call == 1) {
                throw new IOException("transient");
            }
            return direct(BLOCK, (byte) 1);
        };

        IOException thrown = assertThrows(IOException.class,
                () -> cache.getBlock("seg", 0L, BLOCK, failingThenOk));
        assertEquals("transient", thrown.getMessage());

        // Subsequent call must not surface the previous failure: Caffeine does
        // not memoize negative results, so the loader runs a second time and
        // succeeds.
        ByteBuf value = cache.getBlock("seg", 0L, BLOCK, failingThenOk);
        try {
            assertEquals((byte) 1, value.getByte(0));
        } finally {
            value.release();
        }
        assertEquals("loader invoked twice: one failure + one success", 2, calls.get());
    }

    @Test
    public void loaderReturningNullSurfacesIoException() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> null;
        IOException thrown = assertThrows(IOException.class,
                () -> cache.getBlock("seg", 0L, BLOCK, loader));
        assertTrue("message mentions null loader result, was: " + thrown.getMessage(),
                thrown.getMessage().contains("null"));
    }

    @Test
    public void zeroMaxBytesBypassesCache() throws Exception {
        // A fresh disabled cache (rather than the shared disabled() singleton)
        // gives deterministic counter values for this assertion.
        SegmentBlockCache cache = new SegmentBlockCache(0);
        assertFalse("cache disabled", cache.isActive());

        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            calls.incrementAndGet();
            return direct(BLOCK, (byte) 7);
        };

        byte[] a = readAllAndRelease(cache.getBlock("seg", 0L, BLOCK, loader));
        byte[] b = readAllAndRelease(cache.getBlock("seg", 0L, BLOCK, loader));
        assertNotNull(a);
        assertNotNull(b);
        assertArrayEquals(a, b);
        assertEquals("loader invoked on every call in pass-through mode",
                2, calls.get());
        assertEquals(0L, cache.hitCount());
        assertEquals(0L, cache.missCount());
        assertEquals(0L, cache.weightedSize());
        assertEquals("passthrough loads tracked via load_success counter",
                2L, cache.loadSuccessCount());
    }

    @Test
    public void disabledSingletonIsShared() {
        assertTrue("disabled() is a singleton",
                SegmentBlockCache.disabled() == SegmentBlockCache.disabled());
    }

    @Test
    public void containsBlockDoesNotTriggerLoad() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            calls.incrementAndGet();
            return direct(BLOCK, (byte) 0);
        };

        assertFalse(cache.containsBlock("seg", 0L, BLOCK));
        assertEquals("containsBlock must not call the loader", 0, calls.get());

        cache.getBlock("seg", 0L, BLOCK, loader).release();
        assertTrue(cache.containsBlock("seg", 0L, BLOCK));
    }

    @Test
    public void blockKeysWithSameHashButDifferentFieldsAreNotEqual() {
        SegmentBlockCache.BlockKey a = new SegmentBlockCache.BlockKey("segA", 1L);
        SegmentBlockCache.BlockKey b = new SegmentBlockCache.BlockKey("segB", 1L);
        SegmentBlockCache.BlockKey c = new SegmentBlockCache.BlockKey("segA", 2L);
        if (a.equals(b) || a.equals(c)) {
            fail("distinct keys equal each other");
        }
        Set<SegmentBlockCache.BlockKey> set = new HashSet<>();
        set.add(a);
        assertTrue(set.contains(new SegmentBlockCache.BlockKey("segA", 1L)));
    }

    @Test
    public void clearRemovesAllEntries() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);
        cache.getBlock("segA", 0L, BLOCK, loader).release();
        cache.getBlock("segB", 0L, BLOCK, loader).release();
        assertEquals(2L, cache.estimatedSize());
        cache.clear();
        cache.cleanUp();
        assertEquals(0L, cache.estimatedSize());
    }

    // ------------------------------------------------------------------
    // Frontier (pinned) region tests
    // ------------------------------------------------------------------

    @Test
    public void pinnedBlockSurvivesMainCacheEvictionPressure() throws Exception {
        // Main budget = 4 KB, frontier budget = 2 KB, blocks = 1 KB each.
        // After we pin one block, flooding the main cache with 20 new blocks
        // must evict main-cache entries but the pinned block must still be
        // reachable via getBlock().
        SegmentBlockCache cache = new SegmentBlockCache(4L * BLOCK, 2L * BLOCK);
        assertTrue("frontier cache should be active", cache.isFrontierCacheActive());

        AtomicInteger pinLoaderCalls = new AtomicInteger();
        SegmentBlockCache.BlockLoader pinLoader = (p, off, len) -> {
            pinLoaderCalls.incrementAndGet();
            return direct(BLOCK, (byte) 0xAA);
        };

        // Pin the block; loader must be called exactly once.
        cache.pinBlock("seg", 0L, BLOCK, pinLoader).release();
        assertEquals(1, pinLoaderCalls.get());
        assertEquals(1L, cache.frontierLoadSuccessCount());

        // Flood the main cache to trigger eviction of non-pinned blocks.
        SegmentBlockCache.BlockLoader floodLoader = (p, off, len) -> direct(BLOCK, (byte) 0x01);
        for (int i = 1; i <= 20; i++) {
            cache.getBlock("seg", (long) i * BLOCK, BLOCK, floodLoader).release();
        }
        cache.cleanUp();

        // The pinned block must still be served (from the frontier region)
        // without calling the loader again.
        ByteBuf hit = cache.getBlock("seg", 0L, BLOCK, pinLoader);
        try {
            assertEquals("pinned byte pattern", (byte) 0xAA, hit.getByte(0));
        } finally {
            hit.release();
        }
        assertEquals("pin loader must not be re-invoked on frontier hit",
                1, pinLoaderCalls.get());
        assertTrue("at least one frontier hit recorded", cache.frontierHitCount() > 0);
    }

    @Test
    public void frontierEvictionStatsIncrementWhenFrontierBudgetOverflows() throws Exception {
        // Frontier budget = 2 KB, blocks = 1 KB each. After inserting 10 blocks
        // via pinBlock the frontier must have evicted some.
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 2L * BLOCK);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0x55);

        for (int i = 0; i < 10; i++) {
            cache.pinBlock("seg", (long) i * BLOCK, BLOCK, loader).release();
        }
        cache.cleanUp();

        assertTrue("some frontier blocks should have been evicted",
                cache.frontierEvictionCount() > 0);
        assertTrue("frontier weighted size within budget, got " + cache.frontierWeightedSize(),
                cache.frontierWeightedSize() <= 2L * BLOCK);
    }

    @Test
    public void invalidatePathClearsFrontierBlocks() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);

        cache.pinBlock("segA", 0L, BLOCK, loader).release();
        cache.pinBlock("segA", BLOCK, BLOCK, loader).release();
        cache.pinBlock("segB", 0L, BLOCK, loader).release();

        assertTrue("segA/0 should be in frontier before invalidation",
                cache.containsBlock("segA", 0L, BLOCK));

        cache.invalidatePath("segA");

        assertFalse("segA/0 must be gone from frontier after invalidatePath",
                cache.containsBlock("segA", 0L, BLOCK));
        assertFalse("segA/BLOCK must be gone from frontier after invalidatePath",
                cache.containsBlock("segA", BLOCK, BLOCK));
        assertTrue("segB must be unaffected",
                cache.containsBlock("segB", 0L, BLOCK));
    }

    @Test
    public void invalidatePrefixClearsFrontierBlocks() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);

        cache.pinBlock("ts1/idx/a", 0L, BLOCK, loader).release();
        cache.pinBlock("ts1/idx/b", 0L, BLOCK, loader).release();
        cache.pinBlock("ts2/idx/a", 0L, BLOCK, loader).release();

        cache.invalidatePrefix("ts1/");

        assertFalse("ts1/idx/a must be gone from frontier",
                cache.containsBlock("ts1/idx/a", 0L, BLOCK));
        assertFalse("ts1/idx/b must be gone from frontier",
                cache.containsBlock("ts1/idx/b", 0L, BLOCK));
        assertTrue("ts2/idx/a must survive",
                cache.containsBlock("ts2/idx/a", 0L, BLOCK));
    }

    @Test
    public void clearRemovesAllFrontierEntries() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0);
        cache.pinBlock("segA", 0L, BLOCK, loader).release();
        cache.pinBlock("segB", 0L, BLOCK, loader).release();

        assertTrue("segA in frontier before clear", cache.isFrontierCacheActive());
        cache.clear();
        cache.cleanUp();

        assertEquals("frontier must be empty after clear",
                0L, cache.frontierEstimatedSize());
    }

    @Test
    public void pinBlockFallsThroughToMainCacheWhenFrontierDisabled() throws Exception {
        // With frontierMaxBytes = 0 the frontier region is not created.
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 0);
        assertFalse("frontier cache must be inactive", cache.isFrontierCacheActive());

        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            calls.incrementAndGet();
            return direct(BLOCK, (byte) 0x77);
        };

        // pinBlock must still work (falls through to getBlock), loader called once.
        ByteBuf a = cache.pinBlock("seg", 0L, BLOCK, loader);
        ByteBuf b = cache.pinBlock("seg", 0L, BLOCK, loader);
        try {
            assertEquals("loader invoked once — second call is a main-cache hit", 1, calls.get());
            assertEquals((byte) 0x77, a.getByte(0));
            assertEquals((byte) 0x77, b.getByte(0));
        } finally {
            a.release();
            b.release();
        }
    }

    @Test
    public void frontierHitCountedWhenGetBlockServedFromFrontier() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0xBB);

        cache.pinBlock("seg", 0L, BLOCK, loader).release();
        // Now getBlock should serve from frontier, not main cache.
        ByteBuf hit = cache.getBlock("seg", 0L, BLOCK, loader);
        hit.release();

        assertTrue("at least one frontier hit", cache.frontierHitCount() >= 1);
    }

    @Test
    public void frontierLoadFailureCountedOnLoaderException() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader failing = (p, off, len) -> {
            throw new IOException("frontier load failure");
        };
        assertThrows(IOException.class, () -> cache.pinBlock("seg", 0L, BLOCK, failing));
        assertEquals(1L, cache.frontierLoadFailureCount());
        assertEquals(0L, cache.frontierLoadSuccessCount());
    }

    @Test
    public void maxFrontierBytesAccessor() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        assertEquals(200_000L, cache.maxFrontierBytes());
    }

    @Test
    public void pinBlockSuccessfulPathDoesNotThrow() throws Exception {
        // compute() is non-null by construction; the guard now throws
        // IllegalStateException instead of silently calling the loader again.
        // Triggering the ISE would require Caffeine internals access; this test
        // verifies the normal (non-exceptional) path: a successful pinBlock
        // returns the expected buffer without throwing.
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 100_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> direct(BLOCK, (byte) 0xCC);
        ByteBuf buf = cache.pinBlock("seg", 0L, BLOCK, loader);
        try {
            assertEquals((byte) 0xCC, buf.getByte(0));
        } finally {
            buf.release();
        }
        assertEquals(1L, cache.frontierLoadSuccessCount());
        assertEquals(0L, cache.frontierHitCount());
    }

    /**
     * Concurrency stress: many threads interleave {@link SegmentBlockCache#pinBlock}
     * and {@link SegmentBlockCache#getBlock} on the same key. The test verifies
     * that refcount discipline holds (no {@code IllegalReferenceCountException},
     * all slices readable) and that every call returns the correct byte pattern.
     */
    @Test
    public void concurrentPinBlockAndGetBlockOnSameKey() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000, 200_000);
        SegmentBlockCache.BlockLoader pinLoader = (p, off, len) -> direct(BLOCK, (byte) 0xAA);
        SegmentBlockCache.BlockLoader getLoader = (p, off, len) -> direct(BLOCK, (byte) 0xAA);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                new java.util.concurrent.atomic.AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final boolean pinMode = (t % 2 == 0);
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int iter = 0; iter < 50; iter++) {
                        ByteBuf buf;
                        if (pinMode) {
                            buf = cache.pinBlock("seg", 0L, BLOCK, pinLoader);
                        } else {
                            buf = cache.getBlock("seg", 0L, BLOCK, getLoader);
                        }
                        try {
                            assertEquals("wrong byte value", (byte) 0xAA, buf.getByte(0));
                        } finally {
                            buf.release();
                        }
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("concurrent pin/get stress test failed", failure.get());
        }
    }
}
