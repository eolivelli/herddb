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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Unit tests for {@link SegmentBlockCache#bulkInsert(String, long, int, ByteBuf)},
 * the bulk warmup-prefetch API introduced in issue #619.
 *
 * <p>The contract under test:
 * <ul>
 *   <li>splits a contiguous range into block-sized cache entries keyed
 *       identically to {@link SegmentBlockCache#getBlock};</li>
 *   <li>handles a partial final block at end-of-file;</li>
 *   <li>preserves existing entries (no eviction of a single-flighted block);</li>
 *   <li>validates arguments and releases the input buffer in every branch;</li>
 *   <li>is a no-op (still releasing the buffer) on a disabled cache;</li>
 *   <li>tracks accurate {@code bulkInsertedBytes}/{@code bulkInsertSkipped}
 *       counters.</li>
 * </ul>
 */
public class SegmentBlockCacheBulkInsertTest {

    private static final String PATH = "segments/seg-0/graph";
    private static final int BLOCK = 1024;

    /**
     * Allocates a pooled direct buffer that increments byte values starting at
     * {@code seed}. Lets tests verify that the right bytes end up at the right
     * block by comparing against {@link #expectedSliceBytes}.
     */
    private static ByteBuf directRamp(int len, int seed) {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
        for (int i = 0; i < len; i++) {
            buf.writeByte((byte) (seed + i));
        }
        return buf;
    }

    private static byte[] expectedSliceBytes(int seed, int sliceStart, int sliceLen) {
        byte[] out = new byte[sliceLen];
        for (int i = 0; i < sliceLen; i++) {
            out[i] = (byte) (seed + sliceStart + i);
        }
        return out;
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

    /**
     * Sanity: a 4-block aligned prefetch results in 4 cached entries each
     * containing the right bytes, and the loader is never invoked when a
     * subsequent getBlock is issued.
     *
     * <p>With per-block-copy ownership (issue #619 review fix) the parent
     * buffer is released as soon as {@code bulkInsert} returns — its refCnt
     * is 0 immediately, not "N slices alive" — and the cache's byte budget
     * therefore bounds the actual off-heap footprint exactly.
     */
    @Test
    public void bulkInsertCachesEveryBlock() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf data = directRamp(BLOCK * 4, 7);
        // Initial refCnt is 1 (owner-only).
        assertEquals(1, data.refCnt());

        long inserted = cache.bulkInsert(PATH, 0L, BLOCK, data);
        assertEquals("parent buffer released eagerly — no slice pinning", 0, data.refCnt());
        assertEquals(BLOCK * 4L, inserted);
        assertEquals(4L, cache.bulkInsertedBlockCount());
        assertEquals(BLOCK * 4L, cache.bulkInsertedByteCount());

        AtomicInteger loaderCalls = new AtomicInteger();
        SegmentBlockCache.BlockLoader failingLoader = (p, off, len) -> {
            loaderCalls.incrementAndGet();
            throw new IllegalStateException("loader must not be called: bulk-cached");
        };
        for (int i = 0; i < 4; i++) {
            long off = (long) i * BLOCK;
            assertTrue("block " + i + " must be cached", cache.containsBlock(PATH, off, BLOCK));
            byte[] got = readAllAndRelease(cache.getBlock(PATH, off, BLOCK, failingLoader));
            assertArrayEquals("block " + i + " bytes", expectedSliceBytes(7, i * BLOCK, BLOCK), got);
        }
        assertEquals("loader never invoked after bulkInsert", 0, loaderCalls.get());

        // No leak: cached entries are independent allocations released by the
        // eviction listener on cache.clear().
        cache.clear();
        cache.cleanUp();
        assertEquals("weighted size 0 after cache.clear()", 0L, cache.weightedSize());
    }

    /**
     * The final block at end-of-file may be smaller than {@code blockSize}.
     * Verify it is cached, indexed correctly, and a subsequent getBlock
     * returns the partial bytes.
     */
    @Test
    public void bulkInsertHandlesPartialLastBlock() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        int total = BLOCK * 2 + BLOCK / 4; // 2 full blocks + 256 bytes
        ByteBuf data = directRamp(total, 0);

        long inserted = cache.bulkInsert(PATH, 0L, BLOCK, data);
        assertEquals("parent released eagerly even with partial last block", 0, data.refCnt());
        assertEquals(total, inserted);
        assertEquals(3L, cache.bulkInsertedBlockCount());

        SegmentBlockCache.BlockLoader failingLoader = (p, off, len) -> {
            throw new IllegalStateException("loader must not be called");
        };

        // Full block 0 and 1: full BLOCK bytes each.
        for (int i = 0; i < 2; i++) {
            byte[] got = readAllAndRelease(cache.getBlock(PATH, (long) i * BLOCK, BLOCK, failingLoader));
            assertArrayEquals(expectedSliceBytes(0, i * BLOCK, BLOCK), got);
        }
        // Partial block 2: 256 bytes only.
        byte[] tail = readAllAndRelease(cache.getBlock(PATH, 2L * BLOCK, BLOCK, failingLoader));
        assertEquals(BLOCK / 4, tail.length);
        assertArrayEquals(expectedSliceBytes(0, 2 * BLOCK, BLOCK / 4), tail);
    }

    /**
     * An unaligned baseOffset must be rejected, and the input buffer must
     * still be released so callers cannot leak by reusing the wrong offset.
     */
    @Test
    public void unalignedBaseOffsetThrowsAndReleasesBuffer() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf data = directRamp(BLOCK, 0);
        assertThrows(IllegalArgumentException.class,
                () -> cache.bulkInsert(PATH, BLOCK / 2, BLOCK, data));
        assertEquals("rejected buffer must be released", 0, data.refCnt());
    }

    /**
     * A negative baseOffset is also rejected. Same release contract.
     */
    @Test
    public void negativeBaseOffsetThrowsAndReleasesBuffer() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf data = directRamp(BLOCK, 0);
        assertThrows(IllegalArgumentException.class,
                () -> cache.bulkInsert(PATH, -1L, BLOCK, data));
        assertEquals(0, data.refCnt());
    }

    /**
     * A zero or negative blockSize is rejected. Same release contract.
     */
    @Test
    public void nonPositiveBlockSizeThrowsAndReleasesBuffer() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf data = directRamp(BLOCK, 0);
        assertThrows(IllegalArgumentException.class,
                () -> cache.bulkInsert(PATH, 0L, 0, data));
        assertEquals(0, data.refCnt());
    }

    /**
     * The disabled cache singleton accepts bulkInsert calls (so callers don't
     * need to special-case) but stores nothing and still releases the buffer.
     */
    @Test
    public void disabledCacheIsNoOpButReleasesBuffer() {
        SegmentBlockCache cache = SegmentBlockCache.disabled();
        ByteBuf data = directRamp(BLOCK * 2, 0);
        long inserted = cache.bulkInsert(PATH, 0L, BLOCK, data);
        assertEquals(0L, inserted);
        assertEquals(0, data.refCnt());
        assertFalse(cache.containsBlock(PATH, 0L, BLOCK));
        assertEquals(0L, cache.bulkInsertedBlockCount());
    }

    /**
     * If a block is already cached when bulkInsert runs, the existing entry
     * must be preserved (single-flight is the source of truth) and the bulk
     * slice for that block must be released without entering the cache.
     */
    @Test
    public void existingEntriesArePreservedAndCountedAsSkipped() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);

        // Pre-populate block index 1 via the normal getBlock loader so we know
        // exactly which bytes are in the cache before the bulk insert.
        byte[] preloaded = readAllAndRelease(
                cache.getBlock(PATH, (long) BLOCK, BLOCK, (p, off, len) -> directRamp(BLOCK, 99)));
        assertEquals(BLOCK, preloaded.length);

        // Now bulk-insert blocks 0..3 with a *different* ramp so we can tell
        // whether block 1 was overwritten or preserved.
        ByteBuf data = directRamp(BLOCK * 4, 0);
        long inserted = cache.bulkInsert(PATH, 0L, BLOCK, data);
        // With per-block copy (issue #619 review fix) the parent is always
        // released eagerly — present or skipped, neither path retains it.
        assertEquals("parent released eagerly regardless of skip count", 0, data.refCnt());

        // Three blocks newly inserted (0, 2, 3); block 1 already there, skipped.
        assertEquals(BLOCK * 3L, inserted);
        assertEquals(3L, cache.bulkInsertedBlockCount());
        assertEquals(1L, cache.bulkInsertSkippedCount());

        SegmentBlockCache.BlockLoader failingLoader = (p, off, len) -> {
            throw new IllegalStateException("loader must not be called");
        };

        // Block 0 and 2 and 3 should hold ramp-0 bytes.
        for (int i : new int[]{0, 2, 3}) {
            byte[] got = readAllAndRelease(cache.getBlock(PATH, (long) i * BLOCK, BLOCK, failingLoader));
            assertArrayEquals("block " + i + " came from bulk insert",
                    expectedSliceBytes(0, i * BLOCK, BLOCK), got);
        }
        // Block 1 must STILL hold the pre-loaded ramp-99 bytes.
        byte[] got1 = readAllAndRelease(cache.getBlock(PATH, (long) BLOCK, BLOCK, failingLoader));
        assertArrayEquals("block 1 preserved from earlier load",
                expectedSliceBytes(99, 0, BLOCK), got1);
    }

    /**
     * After invalidatePath every block inserted by bulkInsert must be gone
     * from the cache. With per-block copies (issue #619 review fix) the
     * parent buffer is already released before {@code bulkInsert} returns
     * — invalidate's job is to free the per-block allocations, observable
     * via {@link SegmentBlockCache#weightedSize()} dropping to 0.
     */
    @Test
    public void invalidatePathDropsAllBulkInsertedBlocks() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf data = directRamp(BLOCK * 4, 0);

        cache.bulkInsert(PATH, 0L, BLOCK, data);
        cache.cleanUp();
        long weightedBefore = cache.weightedSize();
        assertTrue("cache should be non-empty after bulk insert", weightedBefore > 0);
        assertEquals("parent released by bulkInsert — no slice pinning", 0, data.refCnt());
        // The cache's byte budget now exactly reflects the cached bytes (no
        // hidden multipart-chunk amplification).
        assertEquals("cache bytes match expected per-block sum",
                (long) BLOCK * 4L, weightedBefore);

        cache.invalidatePath(PATH);
        cache.cleanUp();
        assertEquals("cache empty after invalidatePath", 0L, cache.weightedSize());
        assertFalse(cache.containsBlock(PATH, 0L, BLOCK));
        assertFalse(cache.containsBlock(PATH, (long) BLOCK, BLOCK));
        assertFalse(cache.containsBlock(PATH, 2L * BLOCK, BLOCK));
        assertFalse(cache.containsBlock(PATH, 3L * BLOCK, BLOCK));
    }

    /**
     * Two bulk inserts that target the same range should second-be-skipped
     * (already-cached). Counters reflect that.
     */
    @Test
    public void repeatedBulkInsertCountsSkips() {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        ByteBuf first = directRamp(BLOCK * 2, 0);
        cache.bulkInsert(PATH, 0L, BLOCK, first);
        assertEquals(2L, cache.bulkInsertedBlockCount());

        ByteBuf second = directRamp(BLOCK * 2, 0);
        long inserted2 = cache.bulkInsert(PATH, 0L, BLOCK, second);
        assertEquals("second insert is fully skipped", 0L, inserted2);
        assertEquals(2L, cache.bulkInsertedBlockCount());
        assertEquals(2L, cache.bulkInsertSkippedCount());
        assertEquals("second buffer released even on full skip", 0, second.refCnt());
    }

    /**
     * Concurrent stress: many threads run {@code bulkInsert} against
     * overlapping ranges while another set of threads issues {@code getBlock}
     * calls and a third thread periodically invokes {@code invalidatePath}.
     * The test asserts (a) no {@code IllegalReferenceCountException} or other
     * RuntimeException escapes, (b) every {@code getBlock} returns either the
     * correct ramp bytes from the bulk insert or freshly-loaded bytes from
     * the loader fallback, (c) after the workload completes and the cache is
     * cleared the byte-weighted size drops to zero.
     */
    @Test
    public void concurrentBulkInsertGetBlockAndInvalidate() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(8 * 1024 * 1024);
        final int blocks = 64; // file is 64 KiB at BLOCK==1024
        final int total = BLOCK * blocks;

        // Loader returns ramp(0) bytes — same as the bulk insert — so we can
        // assert each retrieved block's payload regardless of which path it
        // came from.
        SegmentBlockCache.BlockLoader loader = (p, off, len) -> {
            int blockIdx = (int) (off / BLOCK);
            return directRamp(len, blockIdx * BLOCK);
        };

        ExecutorService pool = Executors.newFixedThreadPool(8);
        final int iterations = 200;
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(8);
        try {
            // 3 bulk-insert threads.
            for (int t = 0; t < 3; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < iterations; i++) {
                            cache.bulkInsert(PATH, 0L, BLOCK, directRamp(total, 0));
                        }
                    } catch (Throwable th) {
                        firstFailure.compareAndSet(null, th);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            // 4 read threads — assert payload correctness.
            for (int t = 0; t < 4; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < iterations * 4; i++) {
                            int b = i % blocks;
                            long off = (long) b * BLOCK;
                            byte[] got = readAllAndRelease(cache.getBlock(PATH, off, BLOCK, loader));
                            assertArrayEquals("block " + b + " bytes",
                                    expectedSliceBytes(0, b * BLOCK, BLOCK), got);
                        }
                    } catch (Throwable th) {
                        firstFailure.compareAndSet(null, th);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            // 1 invalidator thread that periodically drops everything.
            pool.submit(() -> {
                try {
                    for (int i = 0; i < iterations / 4; i++) {
                        cache.invalidatePath(PATH);
                        Thread.sleep(1);
                    }
                } catch (Throwable th) {
                    firstFailure.compareAndSet(null, th);
                } finally {
                    latch.countDown();
                }
            });
            assertTrue("workload finished within 60s", latch.await(60, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (firstFailure.get() != null) {
            throw new AssertionError("concurrent workload failed", firstFailure.get());
        }
        // Drain pending eviction work.
        cache.clear();
        cache.cleanUp();
        assertEquals("weighted size 0 after final clear", 0L, cache.weightedSize());
    }

    /**
     * bulkInsert at a non-zero, properly-aligned baseOffset routes to the
     * correct block index — verifies offset arithmetic.
     */
    @Test
    public void bulkInsertAtNonZeroAlignedOffset() throws Exception {
        SegmentBlockCache cache = new SegmentBlockCache(1_000_000);
        long base = BLOCK * 10L;
        ByteBuf data = directRamp(BLOCK * 2, 5);

        cache.bulkInsert(PATH, base, BLOCK, data);

        SegmentBlockCache.BlockLoader failingLoader = (p, off, len) -> {
            throw new IllegalStateException("loader must not be called");
        };
        byte[] got10 = readAllAndRelease(cache.getBlock(PATH, base, BLOCK, failingLoader));
        byte[] got11 = readAllAndRelease(cache.getBlock(PATH, base + BLOCK, BLOCK, failingLoader));
        assertArrayEquals(expectedSliceBytes(5, 0, BLOCK), got10);
        assertArrayEquals(expectedSliceBytes(5, BLOCK, BLOCK), got11);
        // Lower-index blocks must NOT be present.
        assertFalse(cache.containsBlock(PATH, 0L, BLOCK));
    }
}
