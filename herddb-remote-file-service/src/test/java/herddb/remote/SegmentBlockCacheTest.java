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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
 * accessors, and pass-through mode when the cache is disabled.
 */
public class SegmentBlockCacheTest {

    private static final int BLOCK = 1024;

    private static byte[] fill(int len, byte value) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = value;
        }
        return b;
    }

    @Test
    public void firstLoadInvokesLoaderAndCachesResult() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger loads = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            loads.incrementAndGet();
            return fill(BLOCK, (byte) 0x42);
        };

        byte[] first = cache.getBlock("seg", 0L, BLOCK, loader);
        byte[] second = cache.getBlock("seg", 0L, BLOCK, loader);

        assertEquals("loader invoked exactly once", 1, loads.get());
        assertSame("cache hit returns same array", first, second);
        assertEquals(1L, cache.hitCount());
        assertEquals(1L, cache.missCount());
        assertEquals(1L, cache.loadSuccessCount());
    }

    @Test
    public void differentOffsetsProduceDifferentEntries() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> fill(BLOCK, (byte) off);

        byte[] b0 = cache.getBlock("seg", 0L, BLOCK, loader);
        byte[] b1 = cache.getBlock("seg", BLOCK, BLOCK, loader);

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
            return fill(BLOCK, (byte) 0xA5);
        };

        ExecutorService pool = Executors.newFixedThreadPool(32);
        try {
            Set<byte[]> observed = Collections.newSetFromMap(new ConcurrentHashMap<>());
            CountDownLatch started = new CountDownLatch(32);
            for (int i = 0; i < 32; i++) {
                pool.submit(() -> {
                    started.countDown();
                    try {
                        observed.add(cache.getBlock("seg", 0L, BLOCK, loader));
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
            assertEquals("all waiters see the identical cached byte[]", 1, observed.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void byteWeightedEvictionRespectsBudget() throws Exception {
        // Budget = 4 KB, entries = 1 KB each. Loading 20 entries forces eviction.
        SegmentBlockCache cache = new SegmentBlockCache(4L * BLOCK);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> fill(BLOCK, (byte) 1);

        for (int i = 0; i < 20; i++) {
            cache.getBlock("seg", (long) i * BLOCK, BLOCK, loader);
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
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> fill(BLOCK, (byte) 0);
        cache.getBlock("segA", 0L, BLOCK, loader);
        cache.getBlock("segA", BLOCK, BLOCK, loader);
        cache.getBlock("segB", 0L, BLOCK, loader);

        cache.invalidatePath("segA");

        assertFalse(cache.containsBlock("segA", 0L, BLOCK));
        assertFalse(cache.containsBlock("segA", BLOCK, BLOCK));
        assertTrue(cache.containsBlock("segB", 0L, BLOCK));
    }

    @Test
    public void invalidatePrefixDropsAllEntriesUnderPrefix() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> fill(BLOCK, (byte) 0);
        cache.getBlock("ts1/idx/a", 0L, BLOCK, loader);
        cache.getBlock("ts1/idx/b", 0L, BLOCK, loader);
        cache.getBlock("ts2/idx/a", 0L, BLOCK, loader);

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
            return fill(BLOCK, (byte) 1);
        };

        IOException thrown = assertThrows(IOException.class,
                () -> cache.getBlock("seg", 0L, BLOCK, failingThenOk));
        assertEquals("transient", thrown.getMessage());

        // Subsequent call must not surface the previous failure: Caffeine does
        // not memoize negative results, so the loader runs a second time and
        // succeeds.
        byte[] value = cache.getBlock("seg", 0L, BLOCK, failingThenOk);
        assertEquals((byte) 1, value[0]);
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
        SegmentBlockCache cache = new SegmentBlockCache(0);
        assertFalse("cache disabled", cache.isActive());

        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            calls.incrementAndGet();
            return fill(BLOCK, (byte) 7);
        };

        byte[] a = cache.getBlock("seg", 0L, BLOCK, loader);
        byte[] b = cache.getBlock("seg", 0L, BLOCK, loader);
        assertNotNull(a);
        assertNotNull(b);
        assertArrayEquals(a, b);
        assertEquals("loader invoked on every call in pass-through mode",
                2, calls.get());
        assertEquals(0L, cache.hitCount());
        assertEquals(0L, cache.missCount());
        assertEquals("passthrough loads tracked via fallback counter",
                2L, cache.loadSuccessCount());
        assertEquals(0L, cache.weightedSize());
    }

    @Test
    public void containsBlockDoesNotTriggerLoad() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        AtomicInteger calls = new AtomicInteger();
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            calls.incrementAndGet();
            return fill(BLOCK, (byte) 0);
        };

        assertFalse(cache.containsBlock("seg", 0L, BLOCK));
        assertEquals("containsBlock must not call the loader", 0, calls.get());

        cache.getBlock("seg", 0L, BLOCK, loader);
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
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> fill(BLOCK, (byte) 0);
        cache.getBlock("segA", 0L, BLOCK, loader);
        cache.getBlock("segB", 0L, BLOCK, loader);
        assertEquals(2L, cache.estimatedSize());
        cache.clear();
        cache.cleanUp();
        assertEquals(0L, cache.estimatedSize());
    }
}
