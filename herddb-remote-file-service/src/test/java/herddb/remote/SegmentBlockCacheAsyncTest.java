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
import static org.junit.Assert.assertNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ResourceLeakDetector;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link SegmentBlockCache#getBlockAsync}: hit/miss counter
 * parity with the sync path, single-flight deduplication, pass-through mode,
 * and reference-count correctness (PARANOID Netty leak detector).
 */
public class SegmentBlockCacheAsyncTest {

    @BeforeClass
    public static void enableLeakDetector() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    private static final int BLOCK = 256;

    /** Cache under test; allocated per test and cleared in @After. */
    private SegmentBlockCache testCache;

    @After
    public void clearCache() {
        if (testCache != null) {
            // Trigger eviction-listener releases so PARANOID leak detector
            // deterministically observes any leaked ByteBufs within this JUnit run.
            testCache.clear();
            testCache.cleanUp();
            testCache = null;
        }
    }

    /** Allocates a pooled direct buffer filled with the given byte value. */
    private static ByteBuf direct(int len, byte value) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
        for (int i = 0; i < len; i++) {
            buf.writeByte(value);
        }
        return buf;
    }

    private static byte[] toArray(ByteBuffer bb) {
        byte[] out = new byte[bb.remaining()];
        bb.get(out);
        return out;
    }

    // -----------------------------------------------------------------------
    // Pass-through mode (disabled cache)
    // -----------------------------------------------------------------------

    @Test
    public void disabledCacheRoutesToLoaderAsync() throws Exception {
        SegmentBlockCache cache = SegmentBlockCache.disabled();
        AtomicInteger loadCount = new AtomicInteger();
        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            loadCount.incrementAndGet();
            return CompletableFuture.completedFuture(direct(len, (byte) 0xAB));
        };

        ByteBuf result = cache.getBlockAsync("p", 0, BLOCK, loader).get(5, TimeUnit.SECONDS);
        try {
            assertNotNull(result);
            assertEquals(1, loadCount.get());
            // Stats: load_success incremented even in pass-through
            assertEquals(1L, cache.loadSuccessCount());
            assertEquals(0L, cache.hitCount());
            assertEquals(0L, cache.missCount());
        } finally {
            ReferenceCountUtil.safeRelease(result);
        }
    }

    // -----------------------------------------------------------------------
    // Cache miss → async load → cache hit on subsequent async call
    // -----------------------------------------------------------------------

    @Test
    public void missPopulatesThenSecondCallIsHit() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        AtomicInteger loadCount = new AtomicInteger();
        byte[] expected = new byte[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            expected[i] = (byte) (i & 0xFF);
        }
        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            loadCount.incrementAndGet();
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
            buf.writeBytes(expected);
            return CompletableFuture.completedFuture(buf);
        };

        // First call — miss
        ByteBuf first = cache.getBlockAsync("p", 0, BLOCK, loader).get(5, TimeUnit.SECONDS);
        try {
            assertEquals(1, loadCount.get());
            assertEquals(1L, cache.missCount());
            assertEquals(0L, cache.hitCount());
        } finally {
            ReferenceCountUtil.safeRelease(first);
        }
        cache.cleanUp();

        // Second call — should hit from cache, no new load
        ByteBuf second = cache.getBlockAsync("p", 0, BLOCK, loader).get(5, TimeUnit.SECONDS);
        try {
            assertEquals("loader must not be called again on cache hit", 1, loadCount.get());
            assertEquals(1L, cache.hitCount());
            // Verify bytes match expected data
            byte[] got = new byte[BLOCK];
            second.getBytes(0, got);
            assertArrayEquals(expected, got);
        } finally {
            ReferenceCountUtil.safeRelease(second);
        }
    }

    // -----------------------------------------------------------------------
    // Async miss hit parity with sync: sync miss → async hit
    // -----------------------------------------------------------------------

    @Test
    public void syncMissPopulatesThenAsyncCallIsHit() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        AtomicInteger loadCount = new AtomicInteger();
        SegmentBlockCache.BlockLoader syncLoader = (p, off, len) -> {
            loadCount.incrementAndGet();
            return direct(len, (byte) 0x77);
        };

        // Populate via sync getBlock
        ByteBuf fromSync = cache.getBlock("p", 0, BLOCK, syncLoader);
        try {
            assertEquals(1, loadCount.get());
        } finally {
            ReferenceCountUtil.safeRelease(fromSync);
        }

        // Async read of same block — must be a hit
        SegmentBlockCache.AsyncBlockLoader asyncLoader = (p, off, len) -> {
            throw new RuntimeException("should not be called on cache hit");
        };
        ByteBuf fromAsync = cache.getBlockAsync("p", 0, BLOCK, asyncLoader).get(5, TimeUnit.SECONDS);
        try {
            assertEquals("loader must not be invoked on async hit", 1, loadCount.get());
            assertEquals(1L, cache.hitCount());
        } finally {
            ReferenceCountUtil.safeRelease(fromAsync);
        }
    }

    // -----------------------------------------------------------------------
    // Single-flight: N concurrent async misses → exactly one loader call
    // -----------------------------------------------------------------------

    @Test
    public void concurrentMissesShareOneLoaderCall() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        AtomicInteger loadCount = new AtomicInteger();
        CountDownLatch loaderStart = new CountDownLatch(1);

        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            loadCount.incrementAndGet();
            // Simulate a slow async load: the future completes after all
            // concurrent callers have called getBlockAsync.
            CompletableFuture<ByteBuf> delayed = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    loaderStart.await();
                    delayed.complete(direct(len, (byte) 0x55));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    delayed.completeExceptionally(e);
                }
            }, "delayed-loader").start();
            return delayed;
        };

        int numCallers = 16;
        List<CompletableFuture<ByteBuf>> futures = new ArrayList<>();
        // All callers fire before the loader completes
        for (int i = 0; i < numCallers; i++) {
            futures.add(cache.getBlockAsync("p", 0, BLOCK, loader));
        }
        // Let the loader complete
        loaderStart.countDown();

        // Wait for all callers
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(10, TimeUnit.SECONDS);

        assertEquals("exactly one loader call for all concurrent misses", 1, loadCount.get());

        // Release all returned slices
        for (CompletableFuture<ByteBuf> f : futures) {
            ReferenceCountUtil.safeRelease(f.getNow(null));
        }
        cache.cleanUp();
    }

    // -----------------------------------------------------------------------
    // Each concurrent caller gets an independent ByteBuf (own refcount)
    // -----------------------------------------------------------------------

    @Test
    public void eachCallerReceivesIndependentSlice() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        byte[] pattern = new byte[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            pattern[i] = (byte) (i * 2);
        }
        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
            buf.writeBytes(pattern);
            return CompletableFuture.completedFuture(buf);
        };

        ByteBuf a = cache.getBlockAsync("p", 0, BLOCK, loader).get(5, TimeUnit.SECONDS);
        ByteBuf b = cache.getBlockAsync("p", 0, BLOCK, loader).get(5, TimeUnit.SECONDS);
        try {
            // Both slices must contain the expected bytes
            byte[] aBytes = new byte[BLOCK];
            byte[] bBytes = new byte[BLOCK];
            a.getBytes(0, aBytes);
            b.getBytes(0, bBytes);
            assertArrayEquals(pattern, aBytes);
            assertArrayEquals(pattern, bBytes);
            // Releasing one slice must not invalidate the other
            ReferenceCountUtil.safeRelease(a);
            a = null;
            // b is still readable
            byte[] bAfterARelease = new byte[BLOCK];
            b.getBytes(0, bAfterARelease);
            assertArrayEquals(pattern, bAfterARelease);
        } finally {
            ReferenceCountUtil.safeRelease(a);
            ReferenceCountUtil.safeRelease(b);
        }
        cache.cleanUp();
    }

    // -----------------------------------------------------------------------
    // Load failure propagated to all waiting callers
    // -----------------------------------------------------------------------

    @Test
    public void loaderFailurePropagatedToAllWaiters() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        CountDownLatch loaderStart = new CountDownLatch(1);
        IOException cause = new IOException("simulated read failure");

        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            CompletableFuture<ByteBuf> failed = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    loaderStart.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                failed.completeExceptionally(cause);
            }, "failing-loader").start();
            return failed;
        };

        int numCallers = 4;
        List<CompletableFuture<ByteBuf>> futures = new ArrayList<>();
        for (int i = 0; i < numCallers; i++) {
            futures.add(cache.getBlockAsync("p", 0, BLOCK, loader));
        }
        loaderStart.countDown();

        int failures = 0;
        for (CompletableFuture<ByteBuf> f : futures) {
            try {
                f.get(5, TimeUnit.SECONDS);
            } catch (java.util.concurrent.ExecutionException e) {
                failures++;
            }
        }
        assertEquals("all waiting callers should receive the failure", numCallers, failures);
        assertEquals("load_failure counter must be incremented", 1L, cache.loadFailureCount());
    }

    // -----------------------------------------------------------------------
    // Concurrent async callers across multiple threads
    // -----------------------------------------------------------------------

    @Test
    public void multiThreadedAsyncLoadsAreCorrect() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 20L);
        SegmentBlockCache cache = testCache;
        byte[] pattern = new byte[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            pattern[i] = (byte) 0xCC;
        }

        SegmentBlockCache.AsyncBlockLoader loader = (p, off, len) -> {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
            buf.writeBytes(pattern);
            return CompletableFuture.completedFuture(buf);
        };

        int threads = 8;
        int callsPerThread = 10;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<CompletableFuture<Void>> results = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            results.add(CompletableFuture.runAsync(() -> {
                for (int c = 0; c < callsPerThread; c++) {
                    try {
                        ByteBuf buf = cache.getBlockAsync("p", 0, BLOCK, loader)
                                .get(5, TimeUnit.SECONDS);
                        byte[] got = new byte[BLOCK];
                        buf.getBytes(0, got);
                        ReferenceCountUtil.safeRelease(buf);
                        assertArrayEquals(pattern, got);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, exec));
        }
        CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);
    }

    // -----------------------------------------------------------------------
    // Synchronously-throwing loader must not deadlock next callers
    // -----------------------------------------------------------------------

    /**
     * If the loader's {@code loadAsync} method throws synchronously (before
     * returning a future), the in-flight entry must be removed and the
     * returned future must fail — not deadlock waiting callers. A subsequent
     * {@code getBlockAsync} call with a working loader must invoke the loader
     * (i.e. the inFlightAsync slot was cleaned up).
     */
    @Test
    public void syncThrowingLoaderDoesNotDeadlockNextCaller() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        RuntimeException boom = new RuntimeException("synchronous boom");

        // First call: loader throws synchronously
        SegmentBlockCache.AsyncBlockLoader throwingLoader = (p, off, len) -> {
            throw boom;
        };
        CompletableFuture<ByteBuf> failFuture = cache.getBlockAsync("p", 0, BLOCK, throwingLoader);
        try {
            failFuture.get(5, TimeUnit.SECONDS);
            Assert.fail("should have failed with the synchronous exception");
        } catch (java.util.concurrent.ExecutionException e) {
            assertEquals(boom, e.getCause());
        }
        assertEquals("load_failure must be incremented", 1L, cache.loadFailureCount());

        // Second call: must invoke the loader (inFlightAsync slot was cleared)
        AtomicInteger loadCount = new AtomicInteger();
        SegmentBlockCache.AsyncBlockLoader goodLoader = (p, off, len) -> {
            loadCount.incrementAndGet();
            return CompletableFuture.completedFuture(direct(len, (byte) 0xAA));
        };
        ByteBuf result = cache.getBlockAsync("p", 0, BLOCK, goodLoader).get(5, TimeUnit.SECONDS);
        try {
            assertEquals("second loader must be called because inFlightAsync was cleared",
                    1, loadCount.get());
            assertNotNull(result);
        } finally {
            ReferenceCountUtil.safeRelease(result);
        }
    }

    // -----------------------------------------------------------------------
    // Concurrent sync getBlock + async getBlockAsync on the same key
    // -----------------------------------------------------------------------

    /**
     * When a sync {@code getBlock} call races an in-flight async load on the
     * same key (the "{@code compute()}-race" path in {@code getBlockAsync}),
     * both callers must receive identical byte content and no {@link ByteBuf}
     * must be leaked.
     */
    @Test
    public void concurrentSyncAndAsyncMissInsertSeesSameBytes() throws Exception {
        testCache = new SegmentBlockCache(BLOCK * 10L);
        SegmentBlockCache cache = testCache;
        byte[] pattern = new byte[BLOCK];
        for (int i = 0; i < BLOCK; i++) {
            pattern[i] = (byte) (i & 0xFF);
        }

        // Latch that holds the async loader open until the sync call has completed
        CountDownLatch asyncLoaderHeld = new CountDownLatch(1);
        CountDownLatch syncCallDone = new CountDownLatch(1);

        SegmentBlockCache.AsyncBlockLoader slowLoader = (p, off, len) -> {
            CompletableFuture<ByteBuf> fut = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    syncCallDone.await(); // wait for sync path to finish
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
                buf.writeBytes(pattern);
                fut.complete(buf);
                asyncLoaderHeld.countDown();
            }, "slow-async-loader").start();
            return fut;
        };

        SegmentBlockCache.BlockLoader syncLoader = (p, off, len) -> {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
            buf.writeBytes(pattern);
            return buf;
        };

        // Start the async load (in-flight, held by latch)
        CompletableFuture<ByteBuf> asyncFuture = cache.getBlockAsync("p", 0, BLOCK, slowLoader);

        // While async load is in flight, insert via sync path
        ByteBuf syncResult = cache.getBlock("p", 0, BLOCK, syncLoader);
        syncCallDone.countDown(); // release the async loader

        // Wait for async load to complete
        asyncLoaderHeld.await(5, TimeUnit.SECONDS);
        ByteBuf asyncResult = asyncFuture.get(5, TimeUnit.SECONDS);

        try {
            byte[] syncBytes = new byte[BLOCK];
            byte[] asyncBytes = new byte[BLOCK];
            syncResult.getBytes(0, syncBytes);
            asyncResult.getBytes(0, asyncBytes);
            assertArrayEquals("sync and async paths must return identical bytes", pattern, syncBytes);
            assertArrayEquals("sync and async paths must return identical bytes", pattern, asyncBytes);
        } finally {
            ReferenceCountUtil.safeRelease(syncResult);
            ReferenceCountUtil.safeRelease(asyncResult);
        }
        // @After calls cache.clear() + cache.cleanUp() for leak detection
    }
}
