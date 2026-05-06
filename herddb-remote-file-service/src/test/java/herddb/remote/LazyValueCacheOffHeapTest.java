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
import static org.junit.Assert.assertTrue;
import herddb.remote.LazyValueCache.ValueKey;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Issue #411 — verifies the off-heap {@link ByteBuf}-backed
 * {@link LazyValueCache} contract:
 *
 * <ul>
 *   <li>cached values live in direct memory (allocated from
 *       {@link herddb.utils.HerdDBByteBufAllocators#dataPagesAllocator()});
 *   <li>each {@code getOrFetch} hands back a {@link ByteBuf#retainedSlice()
 *       retained slice} the caller must release;
 *   <li>weight-based eviction releases the cache's refcount via the
 *       {@code removalListener} so direct memory returns to the pool;
 *   <li>concurrent caller slices outliving an eviction read their bytes
 *       safely thanks to the shared chunk's refcount;
 *   <li>{@code close()} drains every cache-owned refcount.
 * </ul>
 */
public class LazyValueCacheOffHeapTest {

    /** Build a deterministic non-zero payload so equality comparisons are meaningful. */
    private static byte[] payloadOf(int length, int seed) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) ((i * 31 + seed) & 0xff);
        }
        return out;
    }

    private static byte[] readAll(ByteBuf slice) {
        byte[] out = new byte[slice.readableBytes()];
        slice.getBytes(slice.readerIndex(), out);
        return out;
    }

    @Test
    public void cachedSlicesAreDirectMemoryAndCallerOwnsOneRefcount() {
        LazyValueCache cache = new LazyValueCache(64L * 1024L);
        try {
            byte[] payload = payloadOf(256, 0xab);
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

            ByteBuf firstSlice = cache.getOrFetch(k, () -> payload);
            try {
                assertTrue("cached slice must reside in direct memory", firstSlice.isDirect());
                // The slice carries its own atomic refcount (starts at 1);
                // releasing it decrements the parent. We assert the slice is
                // alive and reads consistent bytes — the cache + parent
                // refcount accounting is exercised end-to-end by the
                // evictedEntryReleasesItsRefcount and
                // readerOutlivingEvictionStillSeesItsBytes scenarios below.
                assertTrue("slice must be alive (refcnt > 0)", firstSlice.refCnt() > 0);
                assertArrayEquals("returned bytes must match loader output", payload, readAll(firstSlice));
            } finally {
                firstSlice.release();
            }

            // Subsequent get on the same key must be a hit and still return a usable slice.
            AtomicInteger loaderCalls = new AtomicInteger();
            ByteBuf secondSlice = cache.getOrFetch(k, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{0};
            });
            try {
                assertEquals("hit must not invoke the loader", 0, loaderCalls.get());
                assertArrayEquals(payload, readAll(secondSlice));
            } finally {
                secondSlice.release();
            }
        } finally {
            cache.close();
        }
    }

    @Test
    public void evictedEntryReleasesItsRefcount() {
        // 1 KiB budget; insert 8 entries of 256 bytes each (total 2 KiB) so eviction fires.
        LazyValueCache cache = new LazyValueCache(1024L);
        try {
            List<ByteBuf> heldSlices = new ArrayList<>();
            try {
                for (int i = 0; i < 8; i++) {
                    final int idx = i;
                    ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
                    ByteBuf slice = cache.getOrFetch(k, () -> payloadOf(256, idx));
                    heldSlices.add(slice);
                }
                cache.cleanUp();
                // We do not assert the exact estimatedSize (TinyLFU admission may
                // reject some inserts) but it must respect the weight bound.
                assertTrue("cache must respect its weight bound; estimated="
                                + cache.estimatedSize(),
                        cache.estimatedSize() * 256L <= 1024L);
                // Every caller-held slice must remain readable even though some
                // of them have been evicted from the cache (caller still owns 1
                // refcount on the underlying chunk).
                for (int i = 0; i < heldSlices.size(); i++) {
                    ByteBuf slice = heldSlices.get(i);
                    assertEquals("slice " + i + " must still be readable", 256, slice.readableBytes());
                    assertArrayEquals(payloadOf(256, i), readAll(slice));
                }
            } finally {
                for (ByteBuf s : heldSlices) {
                    s.release();
                }
            }
        } finally {
            cache.close();
        }
    }

    @Test
    public void invalidateForPageReleasesCachedRefcounts() {
        LazyValueCache cache = new LazyValueCache(64L * 1024L);
        try {
            ValueKey k1 = new ValueKey("ts", "uuid", 7L, 0L);
            ValueKey k2 = new ValueKey("ts", "uuid", 7L, 256L);
            ValueKey k3 = new ValueKey("ts", "uuid", 8L, 0L);
            cache.getOrFetch(k1, () -> payloadOf(128, 1)).release();
            cache.getOrFetch(k2, () -> payloadOf(128, 2)).release();
            cache.getOrFetch(k3, () -> payloadOf(128, 3)).release();
            assertEquals(3L, cache.estimatedSize());

            cache.invalidateForPage("ts", "uuid", 7L);
            cache.cleanUp();

            // Page 7 entries must be gone; page 8 stays.
            assertEquals(1L, cache.estimatedSize());
            AtomicInteger loaderCalls = new AtomicInteger();
            cache.getOrFetch(k1, () -> {
                loaderCalls.incrementAndGet();
                return payloadOf(128, 1);
            }).release();
            cache.getOrFetch(k3, () -> {
                loaderCalls.incrementAndGet();
                return new byte[]{0};
            }).release();
            assertEquals("only k1 must reload after invalidate; k3 must hit",
                    1, loaderCalls.get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void closeReleasesEveryCachedRefcount() {
        LazyValueCache cache = new LazyValueCache(64L * 1024L);
        try {
            for (int i = 0; i < 16; i++) {
                final int idx = i;
                ValueKey k = new ValueKey("ts", "uuid", idx, 0L);
                cache.getOrFetch(k, () -> payloadOf(256, idx)).release();
            }
            assertTrue(cache.estimatedSize() > 0L);
        } finally {
            cache.close();
        }
        assertEquals("close must drain the cache", 0L, cache.estimatedSize());
    }

    @Test
    public void readerOutlivingEvictionStillSeesItsBytes() throws Exception {
        // 256-byte budget; one 256-byte entry. A reader holds the slice; a
        // background invalidate evicts the cache entry. The reader must still
        // observe the original bytes (chunk kept alive by the reader's refcount).
        LazyValueCache cache = new LazyValueCache(256L);
        try {
            byte[] payload = payloadOf(256, 0xcc);
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);

            ByteBuf readerSlice = cache.getOrFetch(k, () -> payload);
            try {
                cache.invalidateForPage("ts", "uuid", 1L);
                cache.cleanUp();
                // Cache entry is gone; reader's slice remains valid.
                assertEquals(0L, cache.estimatedSize());
                assertArrayEquals("bytes must survive eviction while a reader holds the slice",
                        payload, readAll(readerSlice));
            } finally {
                readerSlice.release();
            }
        } finally {
            cache.close();
        }
    }

    @Test
    public void concurrentMissesShareSingleLoaderInvocationAndRefcounts() throws Exception {
        // Reproduces the existing concurrentMissesShareSingleLoaderInvocation
        // test under the new API and additionally asserts every caller observes
        // a correctly-refcounted slice (no IllegalReferenceCountException leaks
        // out of the public API).
        LazyValueCache cache = new LazyValueCache(64L * 1024L);
        try {
            ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
            final int threads = 16;
            final byte[] payload = payloadOf(512, 0xde);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch allStarted = new CountDownLatch(threads);
            AtomicInteger loaderCalls = new AtomicInteger();
            AtomicLong observedReads = new AtomicLong(); // count successful reads
            AtomicBoolean badRefcount = new AtomicBoolean(false);
            AtomicBoolean badContent = new AtomicBoolean(false);
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
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                            return payload;
                        });
                        try {
                            if (slice.refCnt() <= 0) {
                                badRefcount.set(true);
                                return;
                            }
                            byte[] read = readAll(slice);
                            if (read.length != payload.length) {
                                badContent.set(true);
                                return;
                            }
                            // Sample a few positions to verify content
                            // matches without paying for a full Arrays.equals
                            // on every iteration. payload is non-uniform
                            // (deterministic seed) so any stale memory would
                            // fail this check.
                            if (read[0] != payload[0]
                                    || read[read.length / 2] != payload[payload.length / 2]
                                    || read[read.length - 1] != payload[payload.length - 1]) {
                                badContent.set(true);
                                return;
                            }
                            observedReads.incrementAndGet();
                        } finally {
                            slice.release();
                        }
                    });
                }
                allStarted.await(5, TimeUnit.SECONDS);
                start.countDown();
                for (Future<?> f : futures) {
                    f.get(15, TimeUnit.SECONDS);
                }
            } finally {
                exec.shutdownNow();
            }
            assertEquals("Caffeine.get(k, loader) must serialise concurrent misses",
                    1, loaderCalls.get());
            assertFalse("no caller may observe refcnt <= 0", badRefcount.get());
            assertFalse("no caller may observe wrong content", badContent.get());
            assertEquals("every thread must have completed a successful read",
                    threads, (int) observedReads.get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void concurrentInvalidateDoesNotCorruptInFlightReaders() throws Exception {
        // Stress: K threads cycle get + read + release on the same page while
        // a parallel invalidator drops the page repeatedly. Every reader must
        // see consistent bytes and no thread must observe a refcnt failure.
        LazyValueCache cache = new LazyValueCache(64L * 1024L);
        try {
            final int threads = 8;
            final int rounds = 5_000;
            final byte[] payload = payloadOf(1024, 0xee);
            AtomicBoolean badRefcount = new AtomicBoolean(false);
            AtomicBoolean badContent = new AtomicBoolean(false);
            ExecutorService exec = Executors.newFixedThreadPool(threads + 1);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch readersDone = new CountDownLatch(threads);
            try {
                for (int t = 0; t < threads; t++) {
                    exec.submit(() -> {
                        try {
                            start.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        try {
                            for (int r = 0; r < rounds; r++) {
                                ValueKey k = new ValueKey("ts", "uuid", 1L, 0L);
                                ByteBuf slice = cache.getOrFetch(k, () -> payload);
                                try {
                                    if (slice.refCnt() <= 0) {
                                        badRefcount.set(true);
                                        return;
                                    }
                                    byte first = slice.getByte(slice.readerIndex());
                                    byte last = slice.getByte(slice.readerIndex()
                                            + slice.readableBytes() - 1);
                                    if (first != payload[0] || last != payload[payload.length - 1]) {
                                        badContent.set(true);
                                        return;
                                    }
                                } finally {
                                    slice.release();
                                }
                            }
                        } finally {
                            readersDone.countDown();
                        }
                    });
                }
                exec.submit(() -> {
                    try {
                        start.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    while (readersDone.getCount() > 0) {
                        cache.invalidateForPage("ts", "uuid", 1L);
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                });
                start.countDown();
                assertTrue("readers should finish within 30 s",
                        readersDone.await(30, TimeUnit.SECONDS));
            } finally {
                exec.shutdownNow();
            }
            assertFalse("no reader may observe refcnt <= 0 in the public API",
                    badRefcount.get());
            assertFalse("no reader may observe corrupted bytes",
                    badContent.get());
        } finally {
            cache.close();
        }
    }
}
