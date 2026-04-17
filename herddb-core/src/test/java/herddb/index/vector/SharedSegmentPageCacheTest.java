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

package herddb.index.vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.index.vector.SharedSegmentPageCache.PageKey;
import herddb.storage.DataStorageManagerException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Tests for {@link SharedSegmentPageCache} covering:
 *  - the single-flight LoadingCache path (hits, misses, loader invocations),
 *  - concurrent-miss deduplication (new behaviour over the old get/put API),
 *  - failure propagation for every exception class the loader can throw,
 *  - null safety,
 *  - byte-weighted eviction,
 *  - {@link PageKey} equality / hashing / null checks,
 *  - stats accessors when the cache is active and when it is disabled.
 */
public class SharedSegmentPageCacheTest {

    private static final String TS = "ts";
    private static final String IDX = "idx";

    private static byte[] payload(int size, int marker) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (marker + i);
        }
        return b;
    }

    // -----------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------

    @Test
    public void firstLoadInvokesLoaderAndCachesResult() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            calls.incrementAndGet();
            return payload(16, (int) k.pageId());
        });

        byte[] first = cache.load(new PageKey(TS, IDX, 1));
        byte[] second = cache.load(new PageKey(TS, IDX, 1));

        assertArrayEquals(payload(16, 1), first);
        assertSame("hit must serve the cached array, not reload", first, second);
        assertEquals("loader called exactly once", 1, calls.get());
        assertEquals("one miss observed", 1L, cache.missCount());
        assertEquals("one hit observed", 1L, cache.hitCount());
        assertEquals("one successful load", 1L, cache.loadSuccessCount());
        assertEquals(0L, cache.loadFailureCount());
    }

    @Test
    public void distinctKeysCoexist() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            calls.incrementAndGet();
            return payload(8, (int) k.pageId());
        });

        byte[] a = cache.load(new PageKey(TS, IDX, 1));
        byte[] b = cache.load(new PageKey(TS, IDX, 2));
        byte[] c = cache.load(new PageKey(TS, IDX, 1));

        assertNotEquals("distinct keys must produce distinct payloads",
                Byte.valueOf(a[0]), Byte.valueOf(b[0]));
        assertSame(a, c);
        assertEquals("two distinct keys -> two loads", 2, calls.get());
    }

    @Test
    public void keysDifferOnTableSpaceAndIndex() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            calls.incrementAndGet();
            // Encode all three key fields into the payload so we can verify isolation.
            byte tag = (byte) (k.tableSpaceUUID().hashCode() ^ k.indexUUID().hashCode()
                    ^ (int) k.pageId());
            return new byte[]{tag};
        });

        cache.load(new PageKey("ts1", IDX, 1));
        cache.load(new PageKey("ts2", IDX, 1));
        cache.load(new PageKey(TS, "idx2", 1));
        cache.load(new PageKey(TS, IDX, 1));

        assertEquals("each distinct (ts, idx, pageId) triggers a fresh load",
                4, calls.get());
    }

    // -----------------------------------------------------------------
    // Concurrent miss deduplication — the headline LoadingCache benefit
    // -----------------------------------------------------------------

    @Test
    public void concurrentMissesDeduplicateToOneLoaderInvocation() throws Exception {
        final int threads = 32;
        AtomicInteger calls = new AtomicInteger(0);
        CountDownLatch start = new CountDownLatch(1);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            calls.incrementAndGet();
            // Hold the loader briefly so concurrent calls collide on the same key.
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return payload(64, 1);
        });

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            Set<byte[]> observed = ConcurrentHashMap.newKeySet();
            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        observed.add(cache.load(new PageKey(TS, IDX, 42)));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue("all threads finished", done.await(10, TimeUnit.SECONDS));
            assertEquals("all callers see the same cached byte[]", 1, observed.size());
            assertEquals("loader invoked exactly once despite concurrent misses",
                    1, calls.get());
        } finally {
            pool.shutdownNow();
        }
    }

    // -----------------------------------------------------------------
    // Failure propagation
    // -----------------------------------------------------------------

    @Test
    public void dataStorageManagerExceptionPropagatesUnwrapped() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            throw new DataStorageManagerException("boom: " + k.pageId());
        });

        DataStorageManagerException ex = assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 7)));
        assertTrue("message must be preserved",
                ex.getMessage() != null && ex.getMessage().contains("boom: 7"));
        assertEquals("a failed load counts as loadFailure", 1L, cache.loadFailureCount());
        assertEquals("a failed load does not count as success", 0L, cache.loadSuccessCount());
    }

    @Test
    public void runtimeExceptionFromLoaderPropagatesAsIs() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            throw new IllegalStateException("explode");
        });
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
        assertEquals("explode", ex.getMessage());
    }

    @Test
    public void loaderReturningNullSurfacesAsException() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> null);
        DataStorageManagerException ex = assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
        assertTrue("error message names the key",
                ex.getMessage() != null && ex.getMessage().contains("page=1"));
    }

    @Test
    public void failedLoadIsNotMemoizedAsNegative() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw new DataStorageManagerException("transient");
            }
            return payload(4, 9);
        });

        assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
        // Second call must re-invoke the loader rather than returning a cached failure.
        byte[] b = cache.load(new PageKey(TS, IDX, 1));
        assertArrayEquals(payload(4, 9), b);
        assertEquals("loader invoked twice — retry after failure is allowed",
                2, calls.get());
    }

    // -----------------------------------------------------------------
    // Cache-disabled (no-op) mode
    // -----------------------------------------------------------------

    @Test
    public void zeroMaxBytesBypassesCacheEntirely() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(0, k -> {
            calls.incrementAndGet();
            return payload(4, 0);
        });
        assertFalse("cache disabled", cache.isActive());
        for (int i = 0; i < 5; i++) {
            cache.load(new PageKey(TS, IDX, 1));
        }
        assertEquals("every call must hit the loader when caching is off",
                5, calls.get());
        assertEquals("stats report zero when disabled", 0L, cache.hitCount());
        assertEquals(0L, cache.missCount());
        assertEquals(0L, cache.maxBytes());
    }

    @Test
    public void negativeMaxBytesAlsoDisablesCache() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(-1, k -> {
            calls.incrementAndGet();
            return payload(1, 0);
        });
        assertFalse(cache.isActive());
        cache.load(new PageKey(TS, IDX, 1));
        cache.load(new PageKey(TS, IDX, 1));
        assertEquals(2, calls.get());
    }

    @Test
    public void disabledCacheStillPropagatesLoaderFailures() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(0, k -> {
            throw new DataStorageManagerException("nope");
        });
        assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
    }

    @Test
    public void disabledCacheNullLoaderResultSurfacesAsException() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(0, k -> null);
        assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
    }

    // -----------------------------------------------------------------
    // Invalidation
    // -----------------------------------------------------------------

    @Test
    public void invalidateForcesNextLoadToRefetch() throws Exception {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            calls.incrementAndGet();
            return payload(4, calls.get());
        });
        PageKey k = new PageKey(TS, IDX, 1);
        cache.load(k);
        cache.load(k);
        assertEquals("second call hit the cache", 1, calls.get());
        cache.invalidate(k);
        cache.load(k);
        assertEquals("post-invalidate load re-invoked the loader", 2, calls.get());
    }

    @Test
    public void invalidateNullKeyIsSafe() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> payload(1, 0));
        cache.invalidate(null); // must not throw
    }

    @Test
    public void invalidateUnknownKeyIsSafe() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> payload(1, 0));
        cache.invalidate(new PageKey(TS, IDX, 999)); // never loaded
    }

    @Test
    public void clearDropsEveryEntry() throws Exception {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> payload(4, (int) k.pageId()));
        cache.load(new PageKey(TS, IDX, 1));
        cache.load(new PageKey(TS, IDX, 2));
        cache.load(new PageKey(TS, IDX, 3));
        assertTrue(cache.size() > 0);
        cache.clear();
        assertEquals("clear leaves the cache empty", 0, cache.size());
    }

    // -----------------------------------------------------------------
    // Byte-weighted eviction
    // -----------------------------------------------------------------

    @Test
    public void byteWeightedEvictionEnforcesBudget() throws Exception {
        long budget = 4096L;
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(budget, k -> {
            calls.incrementAndGet();
            return new byte[1024]; // each entry weighs 1 KiB
        });

        // Insert far more entries than fit, each 1 KiB; total budget is 4 KiB.
        for (int i = 0; i < 20; i++) {
            cache.load(new PageKey(TS, IDX, i));
        }
        // Caffeine eviction is async — force it to run so the assertions below
        // observe a converged state rather than a snapshot mid-maintenance.
        cache.cleanUp();
        assertEquals("each distinct key triggers a load", 20, calls.get());
        assertTrue("evictions happened once the budget was exhausted",
                cache.evictionCount() > 0);
        assertTrue("weighted size cannot exceed the budget (allowing small overshoot)",
                cache.weightedSize() <= budget);
        assertEquals("budget reported matches construction arg", budget, cache.maxBytes());
    }

    // -----------------------------------------------------------------
    // PageKey value semantics
    // -----------------------------------------------------------------

    @Test
    public void pageKeyEqualsAndHashCode() {
        PageKey a = new PageKey("ts", "idx", 42);
        PageKey b = new PageKey("ts", "idx", 42);
        PageKey c = new PageKey("ts", "idx", 43);
        PageKey d = new PageKey("ts2", "idx", 42);
        PageKey e = new PageKey("ts", "idx2", 42);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(a, e);
        assertNotEquals(a, null);
        assertNotEquals(a, "not-a-pagekey");
    }

    @Test
    public void pageKeyRejectsNullTableSpace() {
        assertThrows(NullPointerException.class, () -> new PageKey(null, "idx", 1));
    }

    @Test
    public void pageKeyRejectsNullIndex() {
        assertThrows(NullPointerException.class, () -> new PageKey("ts", null, 1));
    }

    @Test
    public void pageKeyAccessorsReflectConstructorArguments() {
        PageKey k = new PageKey("ts-x", "idx-y", 17);
        assertEquals("ts-x", k.tableSpaceUUID());
        assertEquals("idx-y", k.indexUUID());
        assertEquals(17L, k.pageId());
        assertTrue("toString exposes pageId", k.toString().contains("page=17"));
    }

    @Test
    public void pageKeysSpreadAcrossHashBuckets() {
        // Sanity check: a handful of different keys shouldn't collide on hashCode.
        // This also guards against using only one field in hashCode.
        Set<Integer> hashes = new HashSet<>();
        for (int i = 0; i < 16; i++) {
            hashes.add(new PageKey("ts", "idx", i).hashCode());
        }
        assertTrue("distinct pageIds should mostly hash to distinct buckets: " + hashes.size(),
                hashes.size() >= 12);
    }

    // -----------------------------------------------------------------
    // Constructor contracts
    // -----------------------------------------------------------------

    @Test
    public void constructorRejectsNullLoader() {
        assertThrows(NullPointerException.class,
                () -> new SharedSegmentPageCache(1 << 20, null));
        assertThrows(NullPointerException.class,
                () -> new SharedSegmentPageCache(0, null));
    }

    @Test
    public void loadRejectsNullKey() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> payload(1, 0));
        assertThrows(NullPointerException.class, () -> cache.load(null));
    }

    // -----------------------------------------------------------------
    // Stats accessors
    // -----------------------------------------------------------------

    @Test
    public void statsAccessorsTrackBasicActivity() throws Exception {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> payload(8, (int) k.pageId()));
        assertEquals(0L, cache.hitCount());
        assertEquals(0L, cache.missCount());
        assertEquals(0L, cache.loadSuccessCount());
        assertEquals(0L, cache.totalLoadTimeNanos());

        cache.load(new PageKey(TS, IDX, 1));      // miss
        cache.load(new PageKey(TS, IDX, 1));      // hit
        cache.load(new PageKey(TS, IDX, 2));      // miss

        assertEquals("two unique keys loaded", 2L, cache.missCount());
        assertEquals(1L, cache.hitCount());
        assertEquals(2L, cache.loadSuccessCount());
        assertTrue("totalLoadTimeNanos should record some time",
                cache.totalLoadTimeNanos() >= 0);
        assertTrue("weightedSize tracks cached bytes", cache.weightedSize() > 0);
        assertTrue(cache.estimatedSize() > 0);
    }

    @Test
    public void statsSnapshotIsNonNullEvenWhenDisabled() {
        SharedSegmentPageCache cache = new SharedSegmentPageCache(0, k -> payload(1, 0));
        assertEquals(0L, cache.stats().hitCount());
    }

    // -----------------------------------------------------------------
    // Defensive: loader seeing the right key
    // -----------------------------------------------------------------

    @Test
    public void loaderSeesExactPageKeyPassedByCaller() throws Exception {
        AtomicReference<PageKey> seen = new AtomicReference<>();
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            seen.set(k);
            return payload(2, 0);
        });
        PageKey requested = new PageKey(TS, IDX, 99);
        cache.load(requested);
        assertEquals(requested, seen.get());
    }

    // -----------------------------------------------------------------
    // Regression: failure then success on the same key
    // -----------------------------------------------------------------

    @Test
    public void interleavedFailuresAndSuccessesTrackedIndependently() {
        AtomicInteger calls = new AtomicInteger(0);
        SharedSegmentPageCache cache = new SharedSegmentPageCache(1 << 20, k -> {
            int n = calls.incrementAndGet();
            if ((n & 1) == 1) {
                throw new DataStorageManagerException("odd call fails");
            }
            return payload(1, 0);
        });

        assertThrows(DataStorageManagerException.class,
                () -> cache.load(new PageKey(TS, IDX, 1)));
        try {
            cache.load(new PageKey(TS, IDX, 1));
        } catch (Exception e) {
            fail("second load should succeed: " + e);
        }
        assertEquals("one failure recorded", 1L, cache.loadFailureCount());
        assertEquals("one success recorded", 1L, cache.loadSuccessCount());
    }
}
