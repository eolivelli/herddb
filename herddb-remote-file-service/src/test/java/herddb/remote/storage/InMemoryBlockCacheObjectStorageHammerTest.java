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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Multi-threaded hammer test for {@link InMemoryBlockCacheObjectStorage} that
 * exercises the retained-slice + shared-entry refcount discipline under
 * concurrent readRange / invalidate / eviction pressure.
 *
 * <p>Reproduces the race reported in issue
 * <a href="https://github.com/eolivelli/herddb/issues/241">#241</a>:
 * {@code cache.getIfPresent(key)} on the readRange fast path returned a pooled
 * {@link ByteBuf} whose refcount Caffeine's removal listener could drop to 0
 * before the caller's {@code retainedSlice(...)} ran, producing
 * {@link io.netty.util.IllegalReferenceCountException} on the file server and
 * killing worker threads.
 *
 * <p>Invariants verified:
 * <ul>
 *   <li>Every {@link ReadResult} handed to a reader is fully readable
 *       (refcount &gt; 0) until the reader releases it.</li>
 *   <li>Releasing a result never throws or fails (no double release, no
 *       retain-after-release).</li>
 *   <li>No leaked inner-allocated {@link ByteBuf} — Netty's
 *       {@link ResourceLeakDetector} set to PARANOID flags any lost
 *       reference, and the test tracks every allocation and asserts
 *       {@code refCnt() == 0} after close.</li>
 *   <li>Size-based eviction actually fires during the run — without that the
 *       test would not have exercised the race window.</li>
 * </ul>
 *
 * <p>The fake {@link ObjectStorage} deliberately allocates <em>pooled direct</em>
 * buffers (as {@link InMemoryBlockCacheObjectStorageTest.CountingFake} does and
 * as the real production path does via the S3 /
 * {@code LocalObjectStorage} readers), because mis-handling ref-counts on
 * pooled buffers is what actually blows up in production.
 */
public class InMemoryBlockCacheObjectStorageHammerTest {

    private static final int BLOCK_SIZE = 1024;
    private static final int DISTINCT_BLOCKS = 64;
    private static final int PATHS = 4;
    private static final int THREADS = 16;
    private static final int ITERATIONS_PER_THREAD = 4_000;

    private Level previousLeakLevel;

    @Before
    public void enableParanoidLeakDetection() {
        this.previousLeakLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
    }

    @After
    public void restoreLeakDetection() {
        if (previousLeakLevel != null) {
            ResourceLeakDetector.setLevel(previousLeakLevel);
        }
    }

    /**
     * Heavy eviction pressure + heavy concurrent reads on a key space much
     * larger than the cache budget. Directly targets the fast-path race in
     * {@code readRange}: {@code cache.getIfPresent()} followed by
     * {@code retainedSlice()} while the removal listener concurrently releases
     * the shared entry.
     */
    @Test(timeout = 120_000)
    public void evictionHammerStaysLeakFreeAndBalanced() throws Exception {
        // 4 KB cache, 64 possible blocks => ~94 % eviction rate under uniform
        // random access. Plenty of removal-listener fires per worker iteration.
        final long cacheBytes = 4L * BLOCK_SIZE;
        runHammer(cacheBytes, /* enableDisruptor = */ false);
    }

    /**
     * Same pressure as {@link #evictionHammerStaysLeakFreeAndBalanced}, plus a
     * disruptor thread that rotates through every explicit invalidation path
     * ({@code writeBlock}, {@code write}, {@code deleteLogical},
     * {@code deleteByPrefix}). Those all funnel into Caffeine's removal
     * listener, so this exercises invalidation-triggered releases concurrent
     * with reader slices.
     */
    @Test(timeout = 120_000)
    public void evictionAndInvalidationHammerStaysLeakFreeAndBalanced() throws Exception {
        final long cacheBytes = 8L * BLOCK_SIZE;
        runHammer(cacheBytes, /* enableDisruptor = */ true);
    }

    private static void runHammer(long cacheBytes, boolean enableDisruptor) throws Exception {
        TrackingFake inner = new TrackingFake();
        // Pre-populate the fake with a tagged block for every (path, index) the
        // workers will touch, so every read should succeed.
        for (int p = 0; p < PATHS; p++) {
            for (int b = 0; b < DISTINCT_BLOCKS; b++) {
                inner.putBlock(path(p), b, makeTaggedBlock(path(p), b));
            }
        }

        AtomicBoolean failure = new AtomicBoolean();
        List<Throwable> errors = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(THREADS + (enableDisruptor ? 1 : 0));
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch workersDone = new CountDownLatch(THREADS);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong iterationsDone = new AtomicLong();

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, cacheBytes)) {

            ExecutorService pool = Executors.newFixedThreadPool(THREADS + 1);
            try {
                if (enableDisruptor) {
                    pool.submit(() -> {
                        ready.countDown();
                        try {
                            go.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        Random rnd = new Random(4242);
                        int tick = 0;
                        // Run until all workers are done — otherwise the thread
                        // would spin forever and the pool would time out.
                        while (!stop.get() && !Thread.currentThread().isInterrupted()
                                && workersDone.getCount() > 0) {
                            try {
                                int pathIdx = rnd.nextInt(PATHS);
                                int mode = tick++ & 0x3;
                                switch (mode) {
                                    case 0: {
                                        int b = rnd.nextInt(DISTINCT_BLOCKS);
                                        cache.writeBlock(path(pathIdx), b,
                                                makeTaggedBlock(path(pathIdx), b)).get();
                                        break;
                                    }
                                    case 1: {
                                        // Full-path write invalidates every
                                        // cached block for this path.
                                        cache.write(path(pathIdx),
                                                makeTaggedBlock(path(pathIdx), 0)).get();
                                        break;
                                    }
                                    case 2: {
                                        cache.deleteLogical(path(pathIdx)).get();
                                        // Re-seed so readers can still find
                                        // something afterwards.
                                        for (int b = 0; b < DISTINCT_BLOCKS; b++) {
                                            inner.putBlock(path(pathIdx), b,
                                                    makeTaggedBlock(path(pathIdx), b));
                                        }
                                        break;
                                    }
                                    default: {
                                        cache.deleteByPrefix(path(pathIdx)).get();
                                        for (int b = 0; b < DISTINCT_BLOCKS; b++) {
                                            inner.putBlock(path(pathIdx), b,
                                                    makeTaggedBlock(path(pathIdx), b));
                                        }
                                        break;
                                    }
                                }
                                Thread.sleep(1);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return;
                            } catch (RuntimeException re) {
                                synchronized (errors) {
                                    errors.add(re);
                                }
                                failure.set(true);
                                return;
                            } catch (java.util.concurrent.ExecutionException ee) {
                                synchronized (errors) {
                                    errors.add(ee.getCause() != null ? ee.getCause() : ee);
                                }
                                failure.set(true);
                                return;
                            }
                        }
                    });
                }

                for (int t = 0; t < THREADS; t++) {
                    final int seed = t + 1;
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
                                String path = path(pathIdx);
                                long offset = (long) blockIdx * BLOCK_SIZE;

                                ReadResult rr = cache.readRange(path, offset,
                                        BLOCK_SIZE, BLOCK_SIZE).get();
                                try {
                                    if (rr.status() == ReadResult.Status.NOT_FOUND) {
                                        // Can happen briefly if the disruptor
                                        // has just deleted the path — accept.
                                        continue;
                                    }
                                    ByteBuf slice = rr.byteBuf();
                                    if (slice.refCnt() <= 0) {
                                        throw new AssertionError(
                                                "slice had refCnt=" + slice.refCnt());
                                    }
                                    // Validate payload matches what the fake
                                    // wrote for (path, blockIdx). If the read
                                    // returned stale / truncated data, the tag
                                    // check will fire.
                                    byte expectedTag = tagByte(path, blockIdx);
                                    byte head = slice.getByte(0);
                                    byte tail = slice.getByte(BLOCK_SIZE - 1);
                                    if (head != expectedTag || tail != expectedTag) {
                                        throw new AssertionError(
                                                "corrupted bytes for " + path + "@" + blockIdx
                                                        + " head=" + head + " tail=" + tail
                                                        + " expected=" + expectedTag);
                                    }
                                } finally {
                                    rr.release();
                                }
                                iterationsDone.incrementAndGet();
                            }
                        } catch (RuntimeException | AssertionError e) {
                            synchronized (errors) {
                                errors.add(e);
                            }
                            failure.set(true);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (java.util.concurrent.ExecutionException ee) {
                            synchronized (errors) {
                                errors.add(ee.getCause() != null ? ee.getCause() : ee);
                            }
                            failure.set(true);
                        } finally {
                            workersDone.countDown();
                        }
                    });
                }

                assertTrue("workers ready", ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                pool.shutdown();
                assertTrue("pool finished in time",
                        pool.awaitTermination(90, TimeUnit.SECONDS));
            } finally {
                stop.set(true);
                pool.shutdownNow();
            }

            synchronized (errors) {
                if (!errors.isEmpty()) {
                    AssertionError ae = new AssertionError(
                            "worker(s) failed: " + errors.size() + " error(s); first="
                                    + errors.get(0));
                    ae.initCause(errors.get(0));
                    throw ae;
                }
            }
            assertEquals("no worker flagged failure", false, failure.get());
            assertTrue("workers should have made progress", iterationsDone.get() > 0);
            assertTrue(
                    "size-based eviction must have fired; otherwise this test did not "
                            + "exercise the race window (evictionCount="
                            + cache.stats().evictionCount() + ")",
                    cache.stats().evictionCount() > 0);
        }

        // After close(), every buffer the fake allocated must be fully
        // released (refCnt == 0). Anything alive is a leak — in particular,
        // a slice whose release got silently swallowed because the underlying
        // block had already been freed to the pool.
        int stillAlive = 0;
        for (ByteBuf buf : inner.allocations) {
            if (buf.refCnt() != 0) {
                stillAlive++;
            }
        }
        if (stillAlive != 0) {
            fail("leak: " + stillAlive + " / " + inner.allocations.size()
                    + " fake-allocated buffers still have refCnt > 0 after close()");
        }
    }

    private static String path(int idx) {
        return "seg" + idx;
    }

    /** Deterministic byte that depends on (path, blockIdx). Every byte of the block gets this tag. */
    private static byte tagByte(String path, long blockIdx) {
        int h = path.hashCode();
        h = 31 * h + Long.hashCode(blockIdx);
        return (byte) (h & 0xFF);
    }

    private static byte[] makeTaggedBlock(String path, long blockIdx) {
        byte tag = tagByte(path, blockIdx);
        byte[] b = new byte[BLOCK_SIZE];
        Arrays.fill(b, tag);
        return b;
    }

    /**
     * Fake ObjectStorage that matches production's pooled-direct allocation
     * pattern and tracks every allocation so the hammer test can assert
     * zero leaks after {@code cache.close()}.
     */
    static class TrackingFake implements ObjectStorage {
        final Map<String, byte[]> blocks = new ConcurrentHashMap<>();
        final AtomicInteger readRangeCalls = new AtomicInteger();
        final ConcurrentLinkedQueue<ByteBuf> allocations = new ConcurrentLinkedQueue<>();

        void putBlock(String path, long blockIndex, byte[] bytes) {
            blocks.put(key(path, blockIndex), bytes);
        }

        private static String key(String path, long blockIndex) {
            return path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            // Mimic a logical "clear multipart" by dropping every block of this path.
            String prefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
            List<String> toDrop = new ArrayList<>();
            for (String k : blocks.keySet()) {
                if (k.startsWith(prefix)) {
                    toDrop.add(k);
                }
            }
            toDrop.forEach(blocks::remove);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            return CompletableFuture.completedFuture(ReadResult.notFound());
        }

        @Override
        public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
            blocks.put(key(path, blockIndex), content);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length,
                int blockSize) {
            readRangeCalls.incrementAndGet();
            long blockIndex = offset / blockSize;
            int offsetInBlock = (int) (offset % blockSize);
            byte[] block = blocks.get(key(path, blockIndex));
            if (block == null) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            if (offsetInBlock >= block.length) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            int end = Math.min(offsetInBlock + length, block.length);
            byte[] sliceBytes = Arrays.copyOfRange(block, offsetInBlock, end);
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(sliceBytes.length);
            buf.writeBytes(sliceBytes);
            allocations.add(buf);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<Boolean> deleteLogical(String path) {
            String prefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
            List<String> toDrop = new ArrayList<>();
            for (String k : blocks.keySet()) {
                if (k.startsWith(prefix)) {
                    toDrop.add(k);
                }
            }
            toDrop.forEach(blocks::remove);
            return CompletableFuture.completedFuture(!toDrop.isEmpty());
        }

        @Override
        public CompletableFuture<List<String>> listLogical(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return CompletableFuture.completedFuture(blocks.remove(path) != null);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            List<String> toDrop = new ArrayList<>();
            for (String k : blocks.keySet()) {
                int mp = k.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                String logical = mp >= 0 ? k.substring(0, mp) : k;
                if (logical.startsWith(prefix)) {
                    toDrop.add(k);
                }
            }
            toDrop.forEach(blocks::remove);
            int distinct = (int) toDrop.stream()
                    .map(k -> {
                        int mp = k.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                        return mp >= 0 ? k.substring(0, mp) : k;
                    }).distinct().count();
            return CompletableFuture.completedFuture(distinct);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
