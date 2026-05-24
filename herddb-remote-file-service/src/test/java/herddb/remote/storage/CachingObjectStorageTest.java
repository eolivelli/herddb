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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CachingObjectStorageTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    // --- FakeObjectStorage ---

    static class FakeObjectStorage implements ObjectStorage {
        final Map<String, byte[]> data = new ConcurrentHashMap<>();
        final AtomicInteger readCalls = new AtomicInteger();

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
            data.put(path, Arrays.copyOf(content, content.length));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadResult> read(String path) {
            readCalls.incrementAndGet();
            byte[] bytes = data.get(path);
            if (bytes == null) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            byte[] copy = Arrays.copyOf(bytes, bytes.length);
            io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(copy.length);
            buf.writeBytes(copy);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            return CompletableFuture.completedFuture(data.remove(path) != null);
        }

        @Override
        public CompletableFuture<List<String>> list(String prefix) {
            List<String> result = new ArrayList<>();
            for (String key : data.keySet()) {
                if (key.startsWith(prefix)) {
                    result.add(key);
                }
            }
            return CompletableFuture.completedFuture(result);
        }

        @Override
        public CompletableFuture<Integer> deleteByPrefix(String prefix) {
            List<String> toDelete = new ArrayList<>();
            for (String key : data.keySet()) {
                if (key.startsWith(prefix)) {
                    toDelete.add(key);
                }
            }
            toDelete.forEach(data::remove);
            return CompletableFuture.completedFuture(toDelete.size());
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            // Single-object layout: (offset, length) addresses the one object at path.
            // The contract guarantees the slice stays within a single blockSize-aligned window,
            // but we don't depend on that here — we simply slice the stored bytes.
            // readCalls is incremented on EVERY inner fetch (read() or readRange()) so the
            // cache-hit-vs-miss assertions in the tests work uniformly regardless of which
            // entry-point the cache uses to fault a block in.
            readCalls.incrementAndGet();
            byte[] full = data.get(path);
            if (full == null) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            if (offset >= full.length) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            int start = (int) offset;
            int end = Math.min(start + length, full.length);
            byte[] sliceBytes = Arrays.copyOfRange(full, start, end);
            io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(sliceBytes.length);
            buf.writeBytes(sliceBytes);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public void close() {
        }
    }

    private CachingObjectStorage build(FakeObjectStorage inner, long maxBytes) throws Exception {
        Path cacheDir = folder.newFolder("cache").toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, maxBytes);
    }

    /**
     * Builds a {@link CachingObjectStorage} forced into the legacy per-file mode
     * (slab disabled). Used by tests that exercise the {@link Files#exists}-based
     * cache-file-on-disk semantics — those scenarios only apply to the fallback
     * tier with the slab layout in place (issue #475).
     */
    private CachingObjectStorage buildLegacy(FakeObjectStorage inner, long maxBytes) throws Exception {
        Path cacheDir = folder.newFolder("cache").toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, maxBytes,
                CachingObjectStorage.SlabConfig.disabled());
    }

    @Test
    public void testWriteReadFromCache() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage cache = build(inner, 10 * 1024 * 1024);

        byte[] data = "cached data".getBytes();
        cache.write("a/b.page", data).get();

        int readsBefore = inner.readCalls.get();
        ReadResult result = cache.read("a/b.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
            // inner.read must NOT have been called (served from cache)
            assertEquals(readsBefore, inner.readCalls.get());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadMiss() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage cache = build(inner, 10 * 1024 * 1024);

        ReadResult result = cache.read("nonexistent.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadFromInner() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        // Put directly in inner, bypassing cache
        byte[] data = "from inner".getBytes();
        inner.data.put("ts1/x.page", data);

        ReadResult result = caching.read("ts1/x.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
            assertEquals(1, inner.readCalls.get());
        } finally {
            result.release();
        }

        // Entry should now be resident in the cache (in some tier).
        assertTrue("cache entry should be resident", caching.isInCache("ts1/x.page"));
    }

    @Test
    public void testBootClearsCacheDir() throws Exception {
        // Create a file in the cache dir
        Path cacheDir = folder.newFolder("cache2").toPath();
        Path staleFile = cacheDir.resolve("stale.dat");
        Files.write(staleFile, "old data".getBytes());
        assertTrue(Files.exists(staleFile));

        FakeObjectStorage inner = new FakeObjectStorage();
        new CachingObjectStorage(inner, cacheDir, executor, 10 * 1024 * 1024);

        assertFalse("stale file should be deleted on boot", Files.exists(staleFile));
    }

    @Test
    public void testDeleteInvalidatesCache() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] data = "to delete".getBytes();
        caching.write("del/1.page", data).get();

        caching.delete("del/1.page").get();

        ReadResult result = caching.read("del/1.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, result.status());
        } finally {
            result.release();
        }

        assertFalse("cache entry should no longer be resident", caching.isInCache("del/1.page"));
    }

    @Test
    public void testDeleteByPrefixInvalidates() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        caching.write("pfx/a.page", "a".getBytes()).get();
        caching.write("pfx/b.page", "b".getBytes()).get();
        caching.write("other/c.page", "c".getBytes()).get();

        int deleted = caching.deleteByPrefix("pfx/").get();
        assertEquals(2, deleted);

        // pfx entries gone from cache
        ReadResult ra = caching.read("pfx/a.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, ra.status());
        } finally {
            ra.release();
        }
        ReadResult rb = caching.read("pfx/b.page").get();
        try {
            assertEquals(ReadResult.Status.NOT_FOUND, rb.status());
        } finally {
            rb.release();
        }
        // other entry still accessible via inner
        ReadResult rc = caching.read("other/c.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, rc.status());
        } finally {
            rc.release();
        }

        assertFalse(caching.isInCache("pfx/a.page"));
        assertFalse(caching.isInCache("pfx/b.page"));
    }

    @Test
    public void testEvictionDeletesOldestFileWhenBudgetExceeded() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3").toPath();
        // Budget = 250 bytes; each blob is 100 bytes. Writing three blobs forces eviction
        // of the oldest to respect the disk LRU budget.
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 250);

        byte[] blob = new byte[100];
        // Use writeAndDrain so Caffeine's policy accounts for each put before the next
        // arrives — guarantees the LRU age order a < b < c the assertion relies on
        // (see issue #630).
        writeAndDrain(caching, "blobs/a", blob);
        writeAndDrain(caching, "blobs/b", blob);
        writeAndDrain(caching, "blobs/c", blob);

        assertFalse("oldest entry should have been evicted", caching.isInCache("blobs/a"));
        assertTrue("newer entry should remain", caching.isInCache("blobs/b"));
        assertTrue("newest entry should remain", caching.isInCache("blobs/c"));
    }

    @Test
    public void testSingleByteBudgetEvictsEverything() throws Exception {
        // Retains the spirit of the old testEvictionDeletesLocalFile: a blob larger than the
        // entire budget is admitted then immediately evicted.
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3b").toPath();
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 1);

        // writeAndDrain runs cleanUp + flushExecutor synchronously after the put, so
        // the size-policy decision is observable here (see issue #630).
        writeAndDrain(caching, "evict/big.page", new byte[100]);

        assertFalse("evicted entry should no longer be resident",
                caching.isInCache("evict/big.page"));
    }

    @Test
    public void testReadRangeReadsOnlyRequestedSliceFromDisk() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        // Prime the cache with a block of known content via the inner storage + initial readRange.
        byte[] block = new byte[4096];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        inner.data.put("big.page", block);
        caching.readRange("big.page", 0, 1, 4096).get().release(); // admit block 0 into cache

        int readsBefore = inner.readCalls.get();
        ReadResult result = caching.readRange("big.page", 1000, 16, 4096).get();
        try {
            // inner.read must NOT fire: slice must be served from the disk cache via FileChannel.
            assertEquals(readsBefore, inner.readCalls.get());
            assertEquals(ReadResult.Status.FOUND, result.status());
            byte[] resultBytes = result.content();
            assertEquals(16, resultBytes.length);
            byte[] expected = Arrays.copyOfRange(block, 1000, 1016);
            assertArrayEquals(expected, resultBytes);
        } finally {
            result.release();
        }
    }

    @Test
    public void testConcurrentReadMissesDeduplicateInnerCalls() throws Exception {
        // Slow down inner.read so several callers pile up before the first completes.
        final int n = 16;
        FakeObjectStorage inner = new FakeObjectStorage() {
            @Override
            public CompletableFuture<ReadResult> read(String path) {
                readCalls.incrementAndGet();
                byte[] bytes = data.get(path);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if (bytes == null) {
                        return ReadResult.notFound();
                    }
                    byte[] copy = Arrays.copyOf(bytes, bytes.length);
                    io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(copy.length);
                    buf.writeBytes(copy);
                    return ReadResult.found(buf);
                }, executor);
            }
        };
        inner.data.put("hot.page", "hot".getBytes());
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(caching.read("hot.page"));
        }
        for (CompletableFuture<ReadResult> f : futures) {
            ReadResult res = f.get();
            try {
                assertEquals(ReadResult.Status.FOUND, res.status());
            } finally {
                res.release();
            }
        }
        assertEquals("concurrent misses must collapse to a single inner read",
                1, inner.readCalls.get());
    }

    @Test
    public void testReadSurvivesConcurrentEviction() throws Exception {
        // Legacy-mode test: simulate the per-file fallback path having its file
        // unlinked under the cache LRU's feet, and verify the read falls through
        // to inner.read instead of throwing.
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = buildLegacy(inner, 10 * 1024 * 1024);

        byte[] content = "race".getBytes();
        caching.write("race/1.page", content).get();

        Path file = caching.cacheFilePath("race/1.page");
        Files.deleteIfExists(file);

        // Cache LRU still reports the entry present; the next read must not throw and must
        // fall through to inner.read on NoSuchFileException.
        ReadResult result = caching.read("race/1.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(content, result.content());
        } finally {
            result.release();
        }
    }

    /** Ensures any async task (removal listener, supplyAsync) scheduled on {@code executor} completes. */
    private void flushExecutor() throws Exception {
        // Submit a no-op and wait — guarantees all previously submitted tasks have drained
        // on each worker thread of the executor.
        for (int i = 0; i < 8; i++) {
            CompletableFuture.runAsync(() -> { }, executor).get();
        }
    }

    /**
     * Submits one write and drives Caffeine's per-thread write buffer + maintenance
     * pass to a stable state before returning. Use this in size-budget eviction
     * tests instead of a bare {@code write(...).get()} — see issue #630.
     *
     * <p>The race that motivates this helper: {@code write(...).get()} only waits for
     * the disk-write completion and the synchronous {@code diskLru.asMap().put(...)}
     * to return. Caffeine's size-policy accounting goes through a per-thread
     * write buffer that is drained lazily by maintenance. Under contention (slow
     * CPU, busy ForkJoinPool common pool — which is what CI looks like), the
     * write buffer may still hold a pending record for the just-finished put by
     * the time the next put arrives. When the test eventually calls
     * {@code cleanUp()} just once at the end, the policy may then observe puts
     * out of write-order and pick a different LRU victim — or in the worst case
     * fail to reach the budget threshold at all — leaving the oldest entry
     * resident and the assertion fails. Calling {@code cleanUp()} after each
     * write forces the policy to drain the buffer and account for that put's
     * weight before the next one arrives, so the LRU ordering of A &lt; B &lt; C
     * is preserved deterministically.
     */
    private void writeAndDrain(CachingObjectStorage caching, String path, byte[] blob) throws Exception {
        caching.write(path, blob).get();
        // Drain Caffeine's write buffer + run any pending eviction now, while the
        // newly-admitted entry is the freshest one — so the policy's LRU age order
        // matches real-time put order. Pair with flushExecutor() to also drain any
        // async removal-listener work the policy may have scheduled.
        caching.cleanUp();
        flushExecutor();
    }

    @Test
    public void testConcurrentWritesDeduplicate() throws Exception {
        // Multiple concurrent writes to the same path should deduplicate via inFlightWrites
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] data = "test".getBytes();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            futures.add(caching.write("concurrent/file.page", data));
        }

        for (CompletableFuture<Void> f : futures) {
            f.get(); // All should complete successfully
        }

        // Verify data is cached
        ReadResult result = caching.read("concurrent/file.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testAsyncReadFromCacheHandlesNoSuchFile() throws Exception {
        // Legacy-mode test: simulate the per-file fallback path having its file
        // unlinked between LRU lookup and async open. The read must treat that
        // as a cache miss and fall through to inner.read.
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = buildLegacy(inner, 10 * 1024 * 1024);

        byte[] data = "race".getBytes();
        caching.write("race/1.page", data).get();

        // Manually delete the cache file after it's written but before we read
        Path file = caching.cacheFilePath("race/1.page");
        Files.deleteIfExists(file);

        // Inner should have the data
        inner.data.put("race/1.page", data);

        // Read should recover from the eviction and fall through to inner.read
        ReadResult result = caching.read("race/1.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testAsyncReadSliceFromCacheHandlesNoSuchFile() throws Exception {
        // Similar to above but for readRange — also legacy-mode (per-file path).
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = buildLegacy(inner, 10 * 1024 * 1024);

        byte[] block = new byte[4096];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        // Prime cache via initial readRange.
        inner.data.put("big.page", block);
        caching.readRange("big.page", 0, 1, 4096).get().release();

        // Manually delete the cache file (block 0 lives under blockCacheKey "big.page#0")
        Path file = caching.cacheFilePath("big.page#0");
        Files.deleteIfExists(file);

        // readRange should recover and fall through to inner
        ReadResult result = caching.readRange("big.page", 1000, 16, 4096).get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertEquals(16, result.content().length);
        } finally {
            result.release();
        }
    }

    @Test
    public void testAsyncWriteCacheFileToMultipleBlocks() throws Exception {
        // Verify that async write properly handles multiple blocks
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] block0 = new byte[1024];
        byte[] block1 = new byte[2048];
        for (int i = 0; i < block0.length; i++) {
            block0[i] = (byte) i;
        }
        for (int i = 0; i < block1.length; i++) {
            block1[i] = (byte) (i & 0xFF);
        }

        // Single-object layout: store a concatenated file in inner and admit blocks via readRange.
        byte[] full = new byte[block0.length + block1.length];
        System.arraycopy(block0, 0, full, 0, block0.length);
        System.arraycopy(block1, 0, full, block0.length, block1.length);
        inner.data.put("multi.page", full);
        caching.readRange("multi.page", 0, 1, 1024).get().release(); // admit block 0
        caching.readRange("multi.page", 1024, 1, 1024).get().release(); // admit block 1

        // Verify both blocks are resident in the cache (block keys use the "#N" format).
        assertTrue("block 0 should be cached", caching.isInCache("multi.page#0"));
        assertTrue("block 1 should be cached", caching.isInCache("multi.page#1"));

        // Verify reads work
        ReadResult r0 = caching.readRange("multi.page", 0, 100, 1024).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r0.status());
            assertEquals(100, r0.content().length);
        } finally {
            r0.release();
        }
    }

    @Test
    public void testConcurrentReadsMissesDeduplicateInnerCallsAsync() throws Exception {
        // Verify that concurrent cache misses still deduplicate inner.read calls
        // with the new async path
        final int n = 16;
        FakeObjectStorage inner = new FakeObjectStorage() {
            @Override
            public CompletableFuture<ReadResult> read(String path) {
                readCalls.incrementAndGet();
                byte[] bytes = data.get(path);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    if (bytes == null) {
                        return ReadResult.notFound();
                    }
                    byte[] copy = Arrays.copyOf(bytes, bytes.length);
                    io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(copy.length);
                    buf.writeBytes(copy);
                    return ReadResult.found(buf);
                }, executor);
            }
        };
        inner.data.put("hotdata.page", "hot".getBytes());
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        List<CompletableFuture<ReadResult>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(caching.read("hotdata.page"));
        }
        for (CompletableFuture<ReadResult> f : futures) {
            ReadResult res = f.get();
            try {
                assertEquals(ReadResult.Status.FOUND, res.status());
            } finally {
                res.release();
            }
        }
        assertEquals("concurrent misses must collapse to a single inner read",
                1, inner.readCalls.get());
    }

    @Test
    public void testEvictionDuringAsyncWriteDoesNotCorruptCache() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache_evict").toPath();
        // Budget = 250 bytes; writing three 100-byte blobs forces evictions.
        // This matches the existing testEvictionDeletesOldestFileWhenBudgetExceeded
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 250);

        byte[] blob = new byte[100];
        // writeAndDrain drains Caffeine's write buffer after each put, so the size
        // policy observes the puts in real-time order — the assertion that 'a' (the
        // oldest) is evicted is otherwise flaky under CPU contention (issue #630).
        writeAndDrain(caching, "blobs/a", blob);
        writeAndDrain(caching, "blobs/b", blob);
        writeAndDrain(caching, "blobs/c", blob);

        // Oldest blob should be evicted; newer ones should remain resident.
        assertFalse("oldest entry should have been evicted", caching.isInCache("blobs/a"));
        assertTrue("b should remain", caching.isInCache("blobs/b"));
        assertTrue("c should remain", caching.isInCache("blobs/c"));

        // Verify reads still work (newer entries remain accessible)
        ReadResult rb = caching.read("blobs/b").get();
        try {
            assertEquals(ReadResult.Status.FOUND, rb.status());
        } finally {
            rb.release();
        }
        ReadResult rc = caching.read("blobs/c").get();
        try {
            assertEquals(ReadResult.Status.FOUND, rc.status());
        } finally {
            rc.release();
        }
    }

    @Test
    public void testConcurrentWriteAndReadOfSameFile() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] data = "concurrent access".getBytes();

        // Start multiple concurrent operations
        List<CompletableFuture<?>> futures = new ArrayList<>();

        // Concurrent writes
        for (int i = 0; i < 2; i++) {
            futures.add(caching.write("concurrent/file.page", data));
        }

        // Concurrent reads (some may hit cache miss, some may hit cache)
        for (int i = 0; i < 4; i++) {
            futures.add(caching.read("concurrent/file.page"));
        }

        // Wait for all to complete; release any ReadResults returned by the
        // concurrent reads so the pooled buffers don't leak.
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        for (CompletableFuture<?> f : futures) {
            Object v = f.get();
            if (v instanceof ReadResult) {
                ((ReadResult) v).release();
            }
        }

        // Verify final state is correct
        ReadResult result = caching.read("concurrent/file.page").get();
        try {
            assertEquals(ReadResult.Status.FOUND, result.status());
            assertArrayEquals(data, result.content());
        } finally {
            result.release();
        }
    }

    @Test
    public void testReadRangeWithCacheHitAndMiss() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] block0 = new byte[4096];
        byte[] block1 = new byte[4096];
        for (int i = 0; i < block0.length; i++) {
            block0[i] = (byte) (i & 0xFF);
        }
        for (int i = 0; i < block1.length; i++) {
            block1[i] = (byte) ((i + 100) & 0xFF);
        }

        // Single-object layout: concat the two blocks into one object in inner,
        // then admit block 0 into the cache via an explicit readRange. Block 1
        // remains in inner only.
        byte[] full = new byte[block0.length + block1.length];
        System.arraycopy(block0, 0, full, 0, block0.length);
        System.arraycopy(block1, 0, full, block0.length, block1.length);
        inner.data.put("big.page", full);
        caching.readRange("big.page", 0, 1, 4096).get().release(); // admit block 0

        // Read from cached block 0 (should not call inner)
        int innerReadsBefore = inner.readCalls.get();
        ReadResult r0 = caching.readRange("big.page", 1000, 16, 4096).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r0.status());
            assertEquals(innerReadsBefore, inner.readCalls.get()); // No new inner calls
        } finally {
            r0.release();
        }

        // Read from uncached block 1 (should call inner)
        ReadResult r1 = caching.readRange("big.page", 5000, 16, 4096).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r1.status());
            assertTrue("should have called inner for uncached block", inner.readCalls.get() > innerReadsBefore);
        } finally {
            r1.release();
        }
    }

    @Test
    public void testSetMaxCacheBytesUpdatesPolicy() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        long initialMax = 10 * 1024 * 1024; // 10 MB
        CachingObjectStorage cache = build(inner, initialMax);

        assertEquals("getMaxCacheBytes must match construction value",
                initialMax, cache.getMaxCacheBytes());

        long newMax = 20 * 1024 * 1024; // 20 MB
        long prev = cache.setMaxCacheBytes(newMax);

        assertEquals("setMaxCacheBytes must return the previous maximum",
                initialMax, prev);
        assertEquals("getMaxCacheBytes must reflect the new maximum after set",
                newMax, cache.getMaxCacheBytes());
    }

    @Test
    public void testByteCountersAfterMissAndHit() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        int blockSize = 4096;
        byte[] block = new byte[blockSize];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        // Write block to inner so a readRange will go to inner on first access.
        inner.data.put("counters.page", block);

        // First readRange → cache MISS: missBytes must be incremented by the requested length.
        int readLen = 128;
        long missBefore = caching.getMissBytes();
        long hitBefore = caching.getHitBytes();
        ReadResult r1 = caching.readRange("counters.page", 0, readLen, blockSize).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r1.status());
            assertEquals("missBytes must increase by the requested length on a cache miss",
                    missBefore + readLen, caching.getMissBytes());
            assertEquals("hitBytes must not change on a cache miss",
                    hitBefore, caching.getHitBytes());
        } finally {
            r1.release();
        }

        // Second readRange of same block → cache HIT: hitBytes must be incremented.
        long missBefore2 = caching.getMissBytes();
        long hitBefore2 = caching.getHitBytes();
        ReadResult r2 = caching.readRange("counters.page", 0, readLen, blockSize).get();
        try {
            assertEquals(ReadResult.Status.FOUND, r2.status());
            assertEquals("hitBytes must increase by the requested length on a cache hit",
                    hitBefore2 + readLen, caching.getHitBytes());
            assertEquals("missBytes must not change on a cache hit",
                    missBefore2, caching.getMissBytes());
        } finally {
            r2.release();
        }
    }

    @Test
    public void testAsyncWriteFailureDoesNotCacheData() throws Exception {
        // Create a failing inner storage
        FakeObjectStorage inner = new FakeObjectStorage() {
            @Override
            public CompletableFuture<Void> write(String path, byte[] content) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IOException("write failed"));
                return failed;
            }
        };
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] data = "should fail".getBytes();
        try {
            caching.write("fail/file.page", data).get();
        } catch (Exception e) {
            // Expected
        }

        // Data should not be cached
        assertFalse("entry should not be resident after failed inner write",
                caching.isInCache("fail/file.page"));
    }
}
