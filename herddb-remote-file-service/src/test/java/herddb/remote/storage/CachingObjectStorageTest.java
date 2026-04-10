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
            return CompletableFuture.completedFuture(
                    bytes != null ? ReadResult.found(Arrays.copyOf(bytes, bytes.length)) : ReadResult.notFound());
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
            byte[] result = Arrays.copyOfRange(block, offsetInBlock, end);
            return CompletableFuture.completedFuture(ReadResult.found(result));
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

    /**
     * Disk files must survive heap eviction so they can serve as an L2 cache,
     * avoiding a MinIO round-trip when the heap entry is re-accessed after eviction.
     */
    @Test
    public void testEvictionRetainsDiskFile() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3").toPath();
        // 1 byte max heap cache — any write will be evicted from L1
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 1);

        byte[] data = new byte[100];
        caching.write("evict/big.page", data).get();

        // Force Caffeine to process pending evictions
        caching.cleanUp();

        // The disk file must NOT be deleted — it is the L2 cache
        Path cacheFile = caching.cacheFilePath("evict/big.page");
        assertTrue("evicted entry's local file must be retained as L2 cache", Files.exists(cacheFile));
    }

    /**
     * After a heap eviction, the next read must be served from the disk L2 cache,
     * not from object storage.
     */
    @Test
    public void testEvictedEntryServedFromDisk() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache4").toPath();
        // 1 byte max heap cache — forces immediate eviction
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 1);

        byte[] data = "disk-cached payload".getBytes();
        caching.write("l2/test.page", data).get();
        caching.cleanUp(); // evict from heap

        // Read should hit disk L2, not object storage
        int readsBefore = inner.readCalls.get();
        ReadResult result = caching.read("l2/test.page").get();
        assertEquals(ReadResult.Status.FOUND, result.status());
        assertArrayEquals(data, result.content());
        assertEquals("read should be served from disk L2 cache, not object storage",
                readsBefore, inner.readCalls.get());
    }
}
