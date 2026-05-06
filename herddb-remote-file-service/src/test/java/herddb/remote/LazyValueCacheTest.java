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
import static org.junit.Assert.assertTrue;
import herddb.remote.LazyValueCache.ValueKey;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class LazyValueCacheTest {

    /**
     * Test helper: copies a slice's bytes into a fresh {@code byte[]} and
     * releases the slice's refcount. Mirrors what a real consumer
     * ({@code Bytes.fromOffHeap(slice).getBuffer()}) does on first heap access.
     */
    private static byte[] consumeAndRelease(ByteBuf slice) {
        try {
            byte[] out = new byte[slice.readableBytes()];
            slice.getBytes(slice.readerIndex(), out);
            return out;
        } finally {
            slice.release();
        }
    }

    @Test
    public void hitCountedOnSecondCall() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        AtomicInteger loaderCalls = new AtomicInteger();
        ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

        ByteBuf firstSlice = cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{1, 2, 3};
        });
        ByteBuf secondSlice = cache.getOrFetch(k, () -> {
            loaderCalls.incrementAndGet();
            return new byte[]{9, 9, 9};
        });

        try {
            assertEquals(1, loaderCalls.get());
            byte[] firstBytes = new byte[firstSlice.readableBytes()];
            byte[] secondBytes = new byte[secondSlice.readableBytes()];
            firstSlice.getBytes(firstSlice.readerIndex(), firstBytes);
            secondSlice.getBytes(secondSlice.readerIndex(), secondBytes);
            assertArrayEquals(new byte[]{1, 2, 3}, firstBytes);
            assertArrayEquals("second hit must serve the cached bytes", firstBytes, secondBytes);
        } finally {
            firstSlice.release();
            secondSlice.release();
            cache.close();
        }
    }

    @Test
    public void zeroBudgetDisablesCache() {
        LazyValueCache cache = new LazyValueCache(0L);
        try {
            assertFalse(cache.isEnabled());
            AtomicInteger loaderCalls = new AtomicInteger();
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

            ByteBuf firstSlice = cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            });
            ByteBuf secondSlice = cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            });

            assertEquals("every call must hit the loader when cache is disabled",
                    2, loaderCalls.get());
            assertEquals(0L, cache.estimatedSize());
            firstSlice.release();
            secondSlice.release();
        } finally {
            cache.close();
        }
    }

    @Test
    public void weightBoundedEviction() {
        // 256-byte budget should evict older entries when the total exceeds the cap.
        LazyValueCache cache = new LazyValueCache(256L);
        try {
            AtomicInteger loaderCalls = new AtomicInteger();
            // Insert 16 entries of 64 bytes each (total 1024 — well over 256).
            for (int i = 0; i < 16; i++) {
                final int idx = i;
                ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
                ByteBuf slice = cache.getOrFetch(k, () -> {
                    loaderCalls.incrementAndGet();
                    return new byte[64];
                });
                slice.release();
            }
            assertEquals(16, loaderCalls.get());
            // Caffeine weight-based eviction flows through an async write buffer;
            // drain it before refetching so the test does not race the maintenance
            // thread on CI.
            cache.cleanUp();
            // Now fetch the very first keys again; some must have been evicted
            // and re-loaded. We don't pin down exactly which ones are resident
            // (Caffeine's TinyLFU makes no such guarantee), only that the cache
            // does not blow past its budget.
            loaderCalls.set(0);
            for (int i = 0; i < 16; i++) {
                final int idx = i;
                ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
                ByteBuf slice = cache.getOrFetch(k, () -> {
                    loaderCalls.incrementAndGet();
                    return new byte[64];
                });
                slice.release();
            }
            assertTrue("some entries should have been evicted and reloaded; got "
                    + loaderCalls.get() + " reloads", loaderCalls.get() > 0);
        } finally {
            cache.close();
        }
    }

    @Test
    public void concurrentMissesShareSingleLoaderInvocation() throws Exception {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        try {
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
                        ByteBuf slice = cache.getOrFetch(k, () -> {
                            loaderCalls.incrementAndGet();
                            // slow loader so contending threads actually pile up
                            try {
                                Thread.sleep(50);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            return new byte[]{42};
                        });
                        slice.release();
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
        } finally {
            cache.close();
        }
    }

    @Test
    public void invalidateForPageDropsOnlyThatPage() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        try {
            ValueKey k1 = new ValueKey("ts", "uuid", 1L, 0L);
            ValueKey k2 = new ValueKey("ts", "uuid", 1L, 100L);
            ValueKey k3 = new ValueKey("ts", "uuid", 2L, 0L);
            AtomicInteger loaderCalls = new AtomicInteger();
            cache.getOrFetch(k1, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            cache.getOrFetch(k2, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{2};
            }).release();
            cache.getOrFetch(k3, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{3};
            }).release();
            assertEquals(3, loaderCalls.get());

            cache.invalidateForPage("ts", "uuid", 1L);

            // Page 1 entries should reload; page 2 entry should hit
            cache.getOrFetch(k1, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            cache.getOrFetch(k2, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{2};
            }).release();
            cache.getOrFetch(k3, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{3};
            }).release();
            assertEquals("expected 2 reloads after invalidate, got " + loaderCalls.get(),
                    5, loaderCalls.get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void invalidateForTableDropsOnlyThatTable() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        try {
            ValueKey a = new ValueKey("ts", "ta", 1L, 0L);
            ValueKey b = new ValueKey("ts", "tb", 1L, 0L);
            AtomicInteger loaderCalls = new AtomicInteger();
            cache.getOrFetch(a, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            cache.getOrFetch(b, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            assertEquals(2, loaderCalls.get());

            cache.invalidateForTable("ts", "ta");

            cache.getOrFetch(a, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            cache.getOrFetch(b, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{1};
            }).release();
            assertEquals(3, loaderCalls.get()); // only "ta" reloaded
        } finally {
            cache.close();
        }
    }

    @Test
    public void invalidateForTablespaceDropsEverythingUnder() {
        LazyValueCache cache = new LazyValueCache(1024 * 1024);
        try {
            ValueKey a = new ValueKey("ts1", "t", 1L, 0L);
            ValueKey b = new ValueKey("ts1", "t", 2L, 0L);
            ValueKey c = new ValueKey("ts2", "t", 1L, 0L);
            AtomicInteger loaderCalls = new AtomicInteger();
            for (ValueKey k : new ValueKey[]{a, b, c}) {
                cache.getOrFetch(k, () -> {
                    loaderCalls.incrementAndGet();
                    return new byte[]{1};
                }).release();
            }
            assertEquals(3, loaderCalls.get());

            cache.invalidateForTablespace("ts1");

            for (ValueKey k : new ValueKey[]{a, b, c}) {
                cache.getOrFetch(k, () -> {
                    loaderCalls.incrementAndGet();
                    return new byte[]{1};
                }).release();
            }
            // ts1/a and ts1/b reloaded; ts2/c hit the cache.
            assertEquals(5, loaderCalls.get());
        } finally {
            cache.close();
        }
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
        try {
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
            cache.getOrFetch(k, () -> new byte[]{1}).release(); // miss
            cache.getOrFetch(k, () -> new byte[]{1}).release(); // hit
            cache.getOrFetch(k, () -> new byte[]{1}).release(); // hit
            assertNotNull(cache.stats());
            assertEquals(1, cache.stats().missCount());
            assertEquals(2, cache.stats().hitCount());
        } finally {
            cache.close();
        }
    }

    @Test
    public void zeroLengthValueRoundTrips() {
        // A loader returning an empty byte[] is a real case (zero-length
        // blob columns). The cache must allocate a 0-capacity direct
        // buffer, not throw, and the slice must be readable as an empty
        // byte sequence.
        LazyValueCache cache = new LazyValueCache(1024);
        try {
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
            ByteBuf slice = cache.getOrFetch(k, () -> new byte[0]);
            assertEquals(0, slice.readableBytes());
            byte[] consumed = consumeAndRelease(slice);
            assertEquals(0, consumed.length);
        } finally {
            cache.close();
        }
    }
}
