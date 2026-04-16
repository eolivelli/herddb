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
        public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
            data.put(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex, content);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            long blockIndex = offset / blockSize;
            int offsetInBlock = (int) (offset % blockSize);
            byte[] block = data.get(path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex);
            if (block == null) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            int end = Math.min(offsetInBlock + length, block.length);
            byte[] sliceBytes = Arrays.copyOfRange(block, offsetInBlock, end);
            io.netty.buffer.ByteBuf buf = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(sliceBytes.length);
            buf.writeBytes(sliceBytes);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<Boolean> deleteLogical(String path) {
            String multipartPrefix = path + ObjectStorage.MULTIPART_SUFFIX + "/";
            boolean removed = data.remove(path) != null;
            List<String> blocks = new ArrayList<>();
            for (String key : data.keySet()) {
                if (key.startsWith(multipartPrefix)) {
                    blocks.add(key);
                }
            }
            blocks.forEach(data::remove);
            return CompletableFuture.completedFuture(removed || !blocks.isEmpty());
        }

        @Override
        public CompletableFuture<List<String>> listLogical(String prefix) {
            java.util.LinkedHashSet<String> logical = new java.util.LinkedHashSet<>();
            for (String key : data.keySet()) {
                if (!key.startsWith(prefix)) {
                    continue;
                }
                int mp = key.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                logical.add(mp >= 0 ? key.substring(0, mp) : key);
            }
            return CompletableFuture.completedFuture(new ArrayList<>(logical));
        }

        @Override
        public void close() {
        }
    }

    private CachingObjectStorage build(FakeObjectStorage inner, long maxBytes) throws Exception {
        Path cacheDir = folder.newFolder("cache").toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, maxBytes);
    }

    @Test
    public void testWriteReadFromCache() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage cache = build(inner, 10 * 1024 * 1024);

        byte[] data = "cached data".getBytes();
        cache.write("a/b.page", data).get();

        int readsBefore = inner.readCalls.get();
        ReadResult result = cache.read("a/b.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
        // inner.read must NOT have been called (served from cache)
        assertEquals(readsBefore, inner.readCalls.get());
    }

    @Test
    public void testReadMiss() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage cache = build(inner, 10 * 1024 * 1024);

        ReadResult result = cache.read("nonexistent.page").get();
        assertEquals(ReadResult.Status.NOT_FOUND, result.status());
    }

    @Test
    public void testReadFromInner() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        // Put directly in inner, bypassing cache
        byte[] data = "from inner".getBytes();
        inner.data.put("ts1/x.page", data);

        ReadResult result = caching.read("ts1/x.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
        assertEquals(1, inner.readCalls.get());

        // Local cache file should have been written
        Path cacheFile = caching.cacheFilePath("ts1/x.page");
        assertTrue("cache file should exist", Files.exists(cacheFile));
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
        assertEquals(ReadResult.Status.NOT_FOUND, result.status());

        Path cacheFile = caching.cacheFilePath("del/1.page");
        assertFalse("local cache file should be gone", Files.exists(cacheFile));
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
        assertEquals(ReadResult.Status.NOT_FOUND, caching.read("pfx/a.page").get().status());
        assertEquals(ReadResult.Status.NOT_FOUND, caching.read("pfx/b.page").get().status());
        // other entry still accessible via inner
        assertEquals(ReadResult.Status.FOUND, caching.read("other/c.page").get().status());

        assertFalse(Files.exists(caching.cacheFilePath("pfx/a.page")));
        assertFalse(Files.exists(caching.cacheFilePath("pfx/b.page")));
    }

    @Test
    public void testEvictionDeletesOldestFileWhenBudgetExceeded() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3").toPath();
        // Budget = 250 bytes; each blob is 100 bytes. Writing three blobs forces eviction
        // of the oldest to respect the disk LRU budget.
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 250);

        byte[] blob = new byte[100];
        caching.write("blobs/a", blob).get();
        caching.write("blobs/b", blob).get();
        caching.write("blobs/c", blob).get();

        // Let Caffeine run maintenance and the async removal listener drain on the executor.
        caching.cleanUp();
        flushExecutor();

        Path fileA = caching.cacheFilePath("blobs/a");
        Path fileB = caching.cacheFilePath("blobs/b");
        Path fileC = caching.cacheFilePath("blobs/c");
        assertFalse("oldest entry should have been evicted and unlinked", Files.exists(fileA));
        assertTrue("newer entry should remain on disk", Files.exists(fileB));
        assertTrue("newest entry should remain on disk", Files.exists(fileC));
    }

    @Test
    public void testSingleByteBudgetEvictsEverything() throws Exception {
        // Retains the spirit of the old testEvictionDeletesLocalFile: a blob larger than the
        // entire budget is admitted then immediately evicted.
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3b").toPath();
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 1);

        caching.write("evict/big.page", new byte[100]).get();
        caching.cleanUp();
        flushExecutor();

        Path cacheFile = caching.cacheFilePath("evict/big.page");
        assertFalse("evicted entry's local file should be deleted", Files.exists(cacheFile));
    }

    @Test
    public void testReadRangeReadsOnlyRequestedSliceFromDisk() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        // Prime the cache with a block of known content.
        byte[] block = new byte[4096];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        caching.writeBlock("big.page", 0, block).get();

        int readsBefore = inner.readCalls.get();
        ReadResult result = caching.readRange("big.page", 1000, 16, 4096).get();
        // inner.read must NOT fire: slice must be served from the disk cache via FileChannel.
        assertEquals(readsBefore, inner.readCalls.get());
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertEquals(16, result.content().length);
        byte[] expected = Arrays.copyOfRange(block, 1000, 1016);
        assertArrayEquals(expected, result.content());
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
            assertEquals(ReadResult.Status.FOUND, f.get().status());
        }
        assertEquals("concurrent misses must collapse to a single inner read",
                1, inner.readCalls.get());
    }

    @Test
    public void testReadSurvivesConcurrentEviction() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] content = "race".getBytes();
        caching.write("race/1.page", content).get();

        // Force eviction and wait for the listener to unlink the file.
        caching.cacheFilePath("race/1.page");
        Path file = caching.cacheFilePath("race/1.page");
        Files.deleteIfExists(file);

        // Cache LRU still reports the entry present; the next read must not throw and must
        // fall through to inner.read on NoSuchFileException.
        ReadResult result = caching.read("race/1.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(content, result.content());
    }

    /** Ensures any async task (removal listener, supplyAsync) scheduled on {@code executor} completes. */
    private void flushExecutor() throws Exception {
        // Submit a no-op and wait — guarantees all previously submitted tasks have drained
        // on each worker thread of the executor.
        for (int i = 0; i < 8; i++) {
            CompletableFuture.runAsync(() -> { }, executor).get();
        }
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
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
    }

    @Test
    public void testAsyncReadFromCacheHandlesNoSuchFile() throws Exception {
        // If a file is evicted after the cache membership check but before async open,
        // the read should treat it as a cache miss and fall through to inner.read
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] data = "race".getBytes();
        caching.write("race/1.page", data).get();

        // Manually delete the cache file after it's written but before we read
        Path file = caching.cacheFilePath("race/1.page");
        Files.deleteIfExists(file);

        // Inner should have the data
        inner.data.put("race/1.page", data);

        // Read should recover from the eviction and fall through to inner.read
        ReadResult result = caching.read("race/1.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
    }

    @Test
    public void testAsyncReadSliceFromCacheHandlesNoSuchFile() throws Exception {
        // Similar to above but for readRange
        FakeObjectStorage inner = new FakeObjectStorage();
        CachingObjectStorage caching = build(inner, 10 * 1024 * 1024);

        byte[] block = new byte[4096];
        for (int i = 0; i < block.length; i++) {
            block[i] = (byte) (i & 0xFF);
        }
        caching.writeBlock("big.page", 0, block).get();

        // Manually delete the cache file
        Path file = caching.cacheFilePath("big.page.multipart/0");
        Files.deleteIfExists(file);

        // Inner should have the block
        inner.data.put("big.page.multipart/0", block);

        // readRange should recover and fall through to inner
        ReadResult result = caching.readRange("big.page", 1000, 16, 4096).get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertEquals(16, result.content().length);
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

        caching.writeBlock("multi.page", 0, block0).get();
        caching.writeBlock("multi.page", 1, block1).get();

        // Verify cache files were written
        Path file0 = caching.cacheFilePath("multi.page.multipart/0");
        Path file1 = caching.cacheFilePath("multi.page.multipart/1");
        assertTrue("block 0 cache file should exist", Files.exists(file0));
        assertTrue("block 1 cache file should exist", Files.exists(file1));

        // Verify reads work
        ReadResult r0 = caching.readRange("multi.page", 0, 100, 1024).get();
        assertEquals(ReadResult.Status.FOUND, r0.status());
        assertEquals(100, r0.content().length);
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
            assertEquals(ReadResult.Status.FOUND, res.status());
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
        caching.write("blobs/a", blob).get();
        caching.write("blobs/b", blob).get();
        caching.write("blobs/c", blob).get();

        caching.cleanUp();
        flushExecutor();

        // Oldest blob should be evicted
        Path fileA = caching.cacheFilePath("blobs/a");
        assertFalse("oldest entry should have been evicted", Files.exists(fileA));

        // Newer ones should remain
        Path fileB = caching.cacheFilePath("blobs/b");
        Path fileC = caching.cacheFilePath("blobs/c");
        assertTrue("b should remain", Files.exists(fileB));
        assertTrue("c should remain", Files.exists(fileC));

        // Verify reads still work (newer entries remain accessible)
        assertEquals(ReadResult.Status.FOUND, caching.read("blobs/b").get().status());
        assertEquals(ReadResult.Status.FOUND, caching.read("blobs/c").get().status());
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

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        // Verify final state is correct
        ReadResult result = caching.read("concurrent/file.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
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

        // Write block 0 to cache
        caching.writeBlock("big.page", 0, block0).get();
        // Block 1 is only in inner
        inner.data.put("big.page.multipart/1", block1);

        // Read from cached block 0 (should not call inner)
        int innerReadsBefore = inner.readCalls.get();
        ReadResult r0 = caching.readRange("big.page", 1000, 16, 4096).get();
        assertEquals(ReadResult.Status.FOUND, r0.status());
        assertEquals(innerReadsBefore, inner.readCalls.get()); // No new inner calls

        // Read from uncached block 1 (should call inner)
        ReadResult r1 = caching.readRange("big.page", 5000, 16, 4096).get();
        assertEquals(ReadResult.Status.FOUND, r1.status());
        assertTrue("should have called inner for uncached block", inner.readCalls.get() > innerReadsBefore);
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
        Path cacheFile = caching.cacheFilePath("fail/file.page");
        assertFalse("cache file should not exist after failed write", Files.exists(cacheFile));
    }
}
