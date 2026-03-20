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
    public void testEvictionDeletesLocalFile() throws Exception {
        FakeObjectStorage inner = new FakeObjectStorage();
        Path cacheDir = folder.newFolder("cache3").toPath();
        // 1 byte max cache — any write will be evicted
        CachingObjectStorage caching = new CachingObjectStorage(inner, cacheDir, executor, 1);

        byte[] data = new byte[100];
        caching.write("evict/big.page", data).get();

        // Force Caffeine to process pending evictions
        caching.cleanUp();

        Path cacheFile = caching.cacheFilePath("evict/big.page");
        assertFalse("evicted entry's local file should be deleted", Files.exists(cacheFile));
    }
}
