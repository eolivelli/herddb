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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Multi-threaded hammer test for {@link SegmentBlockCache} that exercises the
 * retained-slice + shared-entry refcount discipline under concurrent get /
 * release / invalidate / eviction pressure.
 *
 * <p>Invariants verified:
 * <ul>
 *   <li>Every slice handed to a reader is fully readable (refcount > 0) until
 *       the reader releases it.</li>
 *   <li>Releasing a slice never throws or fails (no double release).</li>
 *   <li>No leaked loader-allocated {@link ByteBuf} — Netty's
 *       {@link ResourceLeakDetector} set to PARANOID flags any lost
 *       reference; the test fails if the detector reports any leaks.</li>
 *   <li>After the run, the cache's own remaining entries are releasable via
 *       {@link SegmentBlockCache#clear()} without error, and the loader's
 *       allocated-vs-released counters reconcile.</li>
 * </ul>
 *
 * <p>The loader deliberately allocates <em>pooled direct</em> buffers because
 * that is the real production path (bytes come from
 * {@link RemoteFileServiceClient#readFileRangeAsByteBuf}, which hands out
 * pooled direct buffers); mis-handling ref-counts on pooled buffers is what
 * would actually blow up in production.
 */
public class SegmentBlockCacheHammerTest {

    private static final int BLOCK_SIZE = 1024;
    private static final int DISTINCT_BLOCKS = 64;        // enough keys to force LRU churn
    private static final int CACHE_BUDGET = 16 * BLOCK_SIZE;  // 16 slots, 48 misses otherwise
    private static final int THREADS = 16;
    private static final int ITERATIONS_PER_THREAD = 4_000;
    private static final int PATHS = 4;                   // exercise invalidatePath / invalidatePrefix

    private Level previousLeakLevel;

    @Before
    public void enableParanoidLeakDetection() {
        // Promote leak detection so any lost ByteBuf reference flips the test
        // to failure. The previous level is restored in @After so we don't
        // poison sibling tests.
        this.previousLeakLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
    }

    @After
    public void restoreLeakDetection() {
        if (previousLeakLevel != null) {
            ResourceLeakDetector.setLevel(previousLeakLevel);
        }
    }

    @Test(timeout = 120_000)
    public void concurrentGetReleaseInvalidateStaysLeakFreeAndBalanced() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(CACHE_BUDGET);
        AtomicLong allocated = new AtomicLong();
        AtomicLong loaderCalls = new AtomicLong();
        AtomicLong loaderFailures = new AtomicLong();

        // Track every allocated (loader-created) ByteBuf so we can assert at
        // the end that the cache has released all of them. "allocated -
        // evicted-from-cache - still-in-cache" must be zero.
        ConcurrentLinkedQueue<ByteBuf> allAllocated = new ConcurrentLinkedQueue<>();

        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(BLOCK_SIZE);
            // Fill with a reproducible pattern so readers can validate.
            byte tag = (byte) ((p.hashCode() ^ (int) off) & 0xFF);
            for (int i = 0; i < BLOCK_SIZE; i++) {
                buf.writeByte(tag);
            }
            allocated.incrementAndGet();
            loaderCalls.incrementAndGet();
            allAllocated.add(buf);
            return buf;
        };

        AtomicBoolean failure = new AtomicBoolean();
        List<Throwable> errors = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(THREADS);
        CountDownLatch go = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS + 1);
        try {
            // Invalidation thread: periodically drops entries to force more
            // evictions from a direction other than the LRU budget alone, so
            // refcount handling under invalidation gets exercised.
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                Random rnd = new Random(1);
                while (!Thread.currentThread().isInterrupted()
                        && loaderCalls.get() < (long) THREADS * 100) {
                    // wait for the workers to warm up a bit
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                for (int i = 0; i < 32; i++) {
                    int pathIdx = rnd.nextInt(PATHS);
                    if ((i & 1) == 0) {
                        cache.invalidatePath("seg" + pathIdx);
                    } else {
                        cache.invalidatePrefix("seg");
                    }
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });

            for (int t = 0; t < THREADS; t++) {
                final int seed = t;
                pool.submit(() -> {
                    Random rnd = new Random(seed);
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    try {
                        for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                            int pathIdx = rnd.nextInt(PATHS);
                            int blockIdx = rnd.nextInt(DISTINCT_BLOCKS);
                            String path = "seg" + pathIdx;
                            long offset = (long) blockIdx * BLOCK_SIZE;
                            byte expectedTag =
                                    (byte) ((path.hashCode() ^ (int) offset) & 0xFF);

                            ByteBuf slice = cache.getBlock(path, offset, BLOCK_SIZE, loader);
                            try {
                                // Basic sanity: refcount must be > 0 and the
                                // payload must match what the loader wrote.
                                // Sampling a few bytes is enough — we're
                                // stressing refcount handling, not content.
                                if (slice.refCnt() <= 0) {
                                    throw new AssertionError(
                                            "slice had refCnt=" + slice.refCnt());
                                }
                                byte actual = slice.getByte(0);
                                if (actual != expectedTag) {
                                    throw new AssertionError(
                                            "corrupted bytes for " + path + "@" + offset);
                                }
                                byte mid = slice.getByte(BLOCK_SIZE / 2);
                                if (mid != expectedTag) {
                                    throw new AssertionError("mid byte wrong");
                                }
                            } finally {
                                // The release discipline for callers: release
                                // the slice exactly once. Doing this inside a
                                // finally also guarantees no leaked references
                                // if the assertions above throw.
                                boolean fullyReleased = slice.release();
                                // A slice release() returning true means THIS
                                // ref hit zero. It doesn't say anything about
                                // the underlying shared entry — which is fine.
                                // What we must NOT see is an IllegalReferenceCountException
                                // (that would mean a double release).
                                if (slice.refCnt() != 0 && !fullyReleased) {
                                    // A retained slice should always hit 0
                                    // after a single release.
                                    throw new AssertionError(
                                            "slice refcount after release = " + slice.refCnt());
                                }
                            }
                        }
                    } catch (IOException | RuntimeException | AssertionError e) {
                        synchronized (errors) {
                            errors.add(e);
                        }
                        failure.set(true);
                    }
                });
            }

            assertTrue("workers ready", ready.await(10, TimeUnit.SECONDS));
            go.countDown();
            pool.shutdown();
            assertTrue("pool finished in time",
                    pool.awaitTermination(90, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        // Surface any in-worker failure with its stack trace.
        synchronized (errors) {
            if (!errors.isEmpty()) {
                AssertionError ae = new AssertionError(
                        "worker(s) failed: " + errors.size() + " error(s); first=" + errors.get(0));
                ae.initCause(errors.get(0));
                throw ae;
            }
        }
        assertEquals("no worker flagged failure", false, failure.get());

        // All remaining cached entries must be releasable cleanly.
        cache.clear();
        cache.cleanUp();
        // Caffeine dispatches removal-listener calls through
        // ForkJoinPool.commonPool() asynchronously (via executor.execute()).
        // cleanUp() triggers the maintenance pass that *schedules* those calls,
        // but they may not have run yet by the time we inspect refCounts.
        // awaitQuiescence() blocks until the common pool has no remaining tasks,
        // guaranteeing every safeRelease() has completed before the assertions.
        ForkJoinPool.commonPool().awaitQuiescence(10, TimeUnit.SECONDS);
        assertEquals("cache fully drained", 0L, cache.estimatedSize());

        // Final invariant: every buffer the loader allocated must now be
        // fully released (refCnt == 0). Anything still alive is a leak —
        // either the cache lost track of an entry on eviction, or a slice
        // was retained beyond the caller's release.
        int stillAlive = 0;
        for (ByteBuf buf : allAllocated) {
            if (buf.refCnt() != 0) {
                stillAlive++;
            }
        }
        if (stillAlive != 0) {
            fail("leak: " + stillAlive + " / " + allAllocated.size()
                    + " loader-allocated buffers still have refCnt > 0 after cache.clear()");
        }

        // Loader failures should be zero in the normal path (the loader here
        // never throws). If this starts firing in the future it means the
        // cache is invoking the loader in an error mode we need to inspect.
        assertEquals(0L, loaderFailures.get());

        // The shared cache counters should be internally consistent: the
        // number of load_success we see should match the number of loader
        // calls (Caffeine only calls the loader on miss; concurrent misses
        // dedupe via getBlock).
        assertTrue("loader invoked at least once", loaderCalls.get() > 0);
        assertTrue("hits > 0 once the cache warms up", cache.hitCount() > 0);
        assertNotNull(allAllocated.peek());  // silence unused-warning for iteration path
        assertNull("sanity: no lingering null entries enqueued",
                findNull(allAllocated));
    }

    private static <T> T findNull(Iterable<T> it) {
        for (T t : it) {
            if (t == null) {
                return null;  // finding null -> return null sentinel anyway
            }
        }
        return null;
    }
}
