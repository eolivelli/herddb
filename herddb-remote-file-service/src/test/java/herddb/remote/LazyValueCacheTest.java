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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import herddb.remote.LazyValueCache.ValueKey;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class LazyValueCacheTest {

    @Test
    public void hitCountedOnSecondCall() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        AtomicInteger loaderCalls = new AtomicInteger();
        ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

        byte[] first = cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1, 2, 3};
        });
        byte[] second = cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{9, 9, 9};
        });

        assertEquals(1, loaderCalls.get());
        assertArrayEquals(new byte[]{1, 2, 3}, first);
        assertSame("cached reference should be reused", first, second);
    }

    @Test
    public void zeroBudgetDisablesCache() {
        LazyValueCache cache = new LazyValueCache(0L);
        assertFalse(cache.isEnabled());
        AtomicInteger loaderCalls = new AtomicInteger();
        ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

        cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });

        assertEquals("every call must hit the loader when cache is disabled",
                2, loaderCalls.get());
        assertEquals(0L, cache.estimatedSize());
    }

    @Test
    public void weightBoundedEviction() {
        // 256-byte budget should evict older entries when the total exceeds the cap.
        LazyValueCache cache = new LazyValueCache(256L);
        AtomicInteger loaderCalls = new AtomicInteger();
        // Insert 16 entries of 64 bytes each (total 1024 — well over 256).
        for (int i = 0; i < 16; i++) {
            final int idx = i;
            ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
            cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[64];
            });
        }
        assertEquals(16, loaderCalls.get());
        // Now fetch the very first keys again; some must have been evicted
        // and re-loaded. We don't pin down exactly which ones are resident
        // (Caffeine's TinyLFU makes no such guarantee), only that the cache
        // does not blow past its budget.
        loaderCalls.set(0);
        for (int i = 0; i < 16; i++) {
            final int idx = i;
            ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
            cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[64];
            });
        }
        assertTrue("some entries should have been evicted and reloaded; got "
                + loaderCalls.get() + " reloads", loaderCalls.get() > 0);
    }

    @Test
    public void concurrentMissesShareSingleLoaderInvocation() throws Exception {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
        final int threads = 16;
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch allStarted = new CountDownLatch(threads);
        final AtomicInteger loaderCalls = new AtomicInteger();
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            Future<?>[] futures = new Future<?>[threads];
            for (int i = 0; i < threads; i++) {
                futures[i] = exec.submit(() -> {
                    allStarted.countDown();
                    try {
                        start.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    cache.getOrFetch(k, () -> {
                        loaderCalls.incrementAndGet();
                        // slow loader so contending threads actually pile up
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        return new byte[]{42};
                    });
                });
            }
            allStarted.await(5, TimeUnit.SECONDS);
            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }
        } finally {
            exec.shutdownNow();
        }
        assertEquals("Caffeine.get(k, loader) must serialise concurrent misses",
                1, loaderCalls.get());
    }

    @Test
    public void invalidateForPageDropsOnlyThatPage() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        ValueKey k1 = new ValueKey("ts", "uuid", 1L, 0L);
        ValueKey k2 = new ValueKey("ts", "uuid", 1L, 100L);
        ValueKey k3 = new ValueKey("ts", "uuid", 2L, 0L);
        AtomicInteger loaderCalls = new AtomicInteger();
        cache.getOrFetch(k1, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        cache.getOrFetch(k2, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{2};
        });
        cache.getOrFetch(k3, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{3};
        });
        assertEquals(3, loaderCalls.get());

        cache.invalidateForPage("ts", "uuid", 1L);

        // Page 1 entries should reload; page 2 entry should hit
        cache.getOrFetch(k1, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        cache.getOrFetch(k2, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{2};
        });
        cache.getOrFetch(k3, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{3};
        });
        assertEquals("expected 2 reloads after invalidate, got " + loaderCalls.get(),
                5, loaderCalls.get());
    }

    @Test
    public void invalidateForTableDropsOnlyThatTable() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        ValueKey a = new ValueKey("ts", "ta", 1L, 0L);
        ValueKey b = new ValueKey("ts", "tb", 1L, 0L);
        AtomicInteger loaderCalls = new AtomicInteger();
        cache.getOrFetch(a, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        cache.getOrFetch(b, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        assertEquals(2, loaderCalls.get());

        cache.invalidateForTable("ts", "ta");

        cache.getOrFetch(a, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        cache.getOrFetch(b, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1};
        });
        assertEquals(3, loaderCalls.get()); // only "ta" reloaded
    }

    @Test
    public void invalidateForTablespaceDropsEverythingUnder() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        ValueKey a = new ValueKey("ts1", "t", 1L, 0L);
        ValueKey b = new ValueKey("ts1", "t", 2L, 0L);
        ValueKey c = new ValueKey("ts2", "t", 1L, 0L);
        AtomicInteger loaderCalls = new AtomicInteger();
        for (ValueKey k : new ValueKey[]{a, b, c}) {
            cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            });
        }
        assertEquals(3, loaderCalls.get());

        cache.invalidateForTablespace("ts1");

        for (ValueKey k : new ValueKey[]{a, b, c}) {
            cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            });
        }
        // ts1/a and ts1/b reloaded; ts2/c hit the cache.
        assertEquals(5, loaderCalls.get());
    }

    @Test
    public void valueKeyEqualityAndHashing() {
        ValueKey a = new ValueKey("ts", "uuid", 1L, 100L);
        ValueKey b = new ValueKey("ts", "uuid", 1L, 100L);
        ValueKey c = new ValueKey("ts", "uuid", 1L, 200L);
        ValueKey d = new ValueKey("ts", "uuid", 2L, 100L);
        ValueKey e = new ValueKey("ts2", "uuid", 1L, 100L);
        ValueKey f = new ValueKey("ts", "other", 1L, 100L);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(a, e);
        assertNotEquals(a, f);
        assertNotEquals("null", a, null);
        assertNotEquals(a, new Object());
        assertNotNull(a.toString());
    }

    @Test
    public void statsReflectHitsAndMisses() {
        LazyValueCache cache = new LazyValueCache(1024);
        ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
        cache.getOrFetch(k, () -> new byte[]{1}); // miss
        cache.getOrFetch(k, () -> new byte[]{1}); // hit
        cache.getOrFetch(k, () -> new byte[]{1}); // hit
        assertNotNull(cache.stats());
        assertEquals(1, cache.stats().missCount());
        assertEquals(2, cache.stats().hitCount());
    }
}
