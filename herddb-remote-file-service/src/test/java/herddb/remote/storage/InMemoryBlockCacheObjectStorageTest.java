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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InMemoryBlockCacheObjectStorageTest {

    private static final int BLOCK_SIZE = 1024;

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    private static byte[] makeBlock(int seed, int size) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (seed + i);
        }
        return b;
    }

    @Test
    public void readRangeCacheHitOnRepeatedRead() throws Exception {
        CountingFake inner = new CountingFake();
        byte[] block0 = makeBlock(1, BLOCK_SIZE);
        inner.putBlock("path/a", 0, block0);

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {

            ReadResult r1 = cache.readRange("path/a", 0, 256, BLOCK_SIZE).get();
            assertEquals(ReadResult.Status.FOUND, r1.status());
            assertArrayEquals(Arrays.copyOfRange(block0, 0, 256), r1.content());

            ReadResult r2 = cache.readRange("path/a", 256, 256, BLOCK_SIZE).get();
            assertEquals(ReadResult.Status.FOUND, r2.status());
            assertArrayEquals(Arrays.copyOfRange(block0, 256, 512), r2.content());

            // Only the first range caused a readRange on inner (to fetch the whole block).
            assertEquals(1, inner.readRangeCalls.get());

            CacheStats stats = cache.stats();
            assertEquals(1, stats.hitCount());
            assertEquals(1, stats.missCount());
        }
    }

    @Test
    public void concurrentMissesAreDeduplicated() throws Exception {
        CountingFake inner = new CountingFake();
        byte[] block0 = makeBlock(2, BLOCK_SIZE);
        inner.putBlock("path/b", 0, block0);

        CountDownLatch releaseInner = new CountDownLatch(1);
        inner.gate = releaseInner;

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {

            CompletableFuture<ReadResult> f1 = cache.readRange("path/b", 0, 512, BLOCK_SIZE);
            CompletableFuture<ReadResult> f2 = cache.readRange("path/b", 0, 512, BLOCK_SIZE);

            // Give the second call a moment to land in the in-flight map.
            Thread.sleep(50);
            releaseInner.countDown();

            ReadResult r1 = f1.get(5, TimeUnit.SECONDS);
            ReadResult r2 = f2.get(5, TimeUnit.SECONDS);
            assertEquals(ReadResult.Status.FOUND, r1.status());
            assertEquals(ReadResult.Status.FOUND, r2.status());
            assertArrayEquals(Arrays.copyOfRange(block0, 0, 512), r1.content());
            assertArrayEquals(Arrays.copyOfRange(block0, 0, 512), r2.content());

            // Only one inner readRange, thanks to in-flight dedup.
            assertEquals(1, inner.readRangeCalls.get());
        }
    }

    @Test
    public void writeBlockInvalidatesSingleBlock() throws Exception {
        CountingFake inner = new CountingFake();
        byte[] original = makeBlock(3, BLOCK_SIZE);
        inner.putBlock("path/c", 0, original);

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            cache.readRange("path/c", 0, 16, BLOCK_SIZE).get();
            assertEquals(1, inner.readRangeCalls.get());

            byte[] updated = makeBlock(99, BLOCK_SIZE);
            cache.writeBlock("path/c", 0, updated).get();

            ReadResult r = cache.readRange("path/c", 0, 16, BLOCK_SIZE).get();
            assertArrayEquals(Arrays.copyOfRange(updated, 0, 16), r.content());
            assertEquals(2, inner.readRangeCalls.get());
        }
    }

    @Test
    public void writeInvalidatesAllBlocksOfPath() throws Exception {
        CountingFake inner = new CountingFake();
        inner.putBlock("path/d", 0, makeBlock(4, BLOCK_SIZE));
        inner.putBlock("path/d", 1, makeBlock(5, BLOCK_SIZE));

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            cache.readRange("path/d", 0, 16, BLOCK_SIZE).get();
            cache.readRange("path/d", BLOCK_SIZE, 16, BLOCK_SIZE).get();
            assertEquals(2, inner.readRangeCalls.get());

            cache.write("path/d", new byte[]{1, 2, 3}).get();

            cache.readRange("path/d", 0, 16, BLOCK_SIZE).get();
            cache.readRange("path/d", BLOCK_SIZE, 16, BLOCK_SIZE).get();
            assertEquals("both blocks must be re-fetched after write", 4, inner.readRangeCalls.get());
        }
    }

    @Test
    public void deleteLogicalInvalidatesAllBlocksOfPath() throws Exception {
        CountingFake inner = new CountingFake();
        inner.putBlock("path/e", 0, makeBlock(6, BLOCK_SIZE));
        inner.putBlock("path/e", 1, makeBlock(7, BLOCK_SIZE));
        inner.putBlock("path/other", 0, makeBlock(8, BLOCK_SIZE));

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            cache.readRange("path/e", 0, 16, BLOCK_SIZE).get();
            cache.readRange("path/e", BLOCK_SIZE, 16, BLOCK_SIZE).get();
            cache.readRange("path/other", 0, 16, BLOCK_SIZE).get();

            cache.deleteLogical("path/e").get();

            // Unrelated path stays cached.
            int before = inner.readRangeCalls.get();
            cache.readRange("path/other", 0, 16, BLOCK_SIZE).get();
            assertEquals(before, inner.readRangeCalls.get());
        }
    }

    @Test
    public void deleteByPrefixInvalidatesMatchingEntries() throws Exception {
        CountingFake inner = new CountingFake();
        inner.putBlock("pfx/a", 0, makeBlock(10, BLOCK_SIZE));
        inner.putBlock("pfx/b", 0, makeBlock(11, BLOCK_SIZE));
        inner.putBlock("other/c", 0, makeBlock(12, BLOCK_SIZE));

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            cache.readRange("pfx/a", 0, 16, BLOCK_SIZE).get();
            cache.readRange("pfx/b", 0, 16, BLOCK_SIZE).get();
            cache.readRange("other/c", 0, 16, BLOCK_SIZE).get();
            int before = inner.readRangeCalls.get();

            int deleted = cache.deleteByPrefix("pfx/").get();
            assertEquals(2, deleted);

            // Unrelated path served from cache.
            cache.readRange("other/c", 0, 16, BLOCK_SIZE).get();
            assertEquals(before, inner.readRangeCalls.get());
        }
    }

    @Test
    public void evictionRespectsWeightBound() throws Exception {
        CountingFake inner = new CountingFake();
        // Three 1 KB blocks, cache holds 2 KB → the oldest must be evicted.
        inner.putBlock("w/a", 0, makeBlock(20, BLOCK_SIZE));
        inner.putBlock("w/b", 0, makeBlock(21, BLOCK_SIZE));
        inner.putBlock("w/c", 0, makeBlock(22, BLOCK_SIZE));

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 2 * BLOCK_SIZE)) {
            cache.readRange("w/a", 0, 16, BLOCK_SIZE).get();
            cache.readRange("w/b", 0, 16, BLOCK_SIZE).get();
            cache.readRange("w/c", 0, 16, BLOCK_SIZE).get();
            cache.cleanUp();

            assertTrue("cache must not exceed its weight bound",
                    cache.estimatedBytes() <= 2L * BLOCK_SIZE);
        }
    }

    @Test
    public void lastPartialBlockServedCorrectly() throws Exception {
        CountingFake inner = new CountingFake();
        // "Last" block is only 300 bytes wide.
        byte[] partial = makeBlock(30, 300);
        inner.putBlock("path/last", 0, partial);

        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            // Requesting 256 bytes from offset 40: expect 256 bytes of the partial block.
            ReadResult r = cache.readRange("path/last", 40, 256, BLOCK_SIZE).get();
            assertEquals(ReadResult.Status.FOUND, r.status());
            assertArrayEquals(Arrays.copyOfRange(partial, 40, 40 + 256), r.content());

            // Request past the end of the stored bytes: should return the available tail.
            ReadResult r2 = cache.readRange("path/last", 200, 256, BLOCK_SIZE).get();
            assertEquals(ReadResult.Status.FOUND, r2.status());
            assertArrayEquals(Arrays.copyOfRange(partial, 200, 300), r2.content());
        }
    }

    @Test
    public void notFoundIsPropagated() throws Exception {
        CountingFake inner = new CountingFake();
        try (InMemoryBlockCacheObjectStorage cache =
                new InMemoryBlockCacheObjectStorage(inner, 4 * 1024 * 1024)) {
            ReadResult r = cache.readRange("missing", 0, 16, BLOCK_SIZE).get();
            assertEquals(ReadResult.Status.NOT_FOUND, r.status());
        }
    }

    @Test
    public void maxBytesMustBePositive() {
        CountingFake inner = new CountingFake();
        try {
            new InMemoryBlockCacheObjectStorage(inner, 0);
            fail("should reject zero");
        } catch (IllegalArgumentException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    // --- Test double: simple block-addressable storage with counters and optional latching. ---
    static class CountingFake implements ObjectStorage {
        final Map<String, byte[]> blocks = new ConcurrentHashMap<>();
        final AtomicInteger readRangeCalls = new AtomicInteger();
        /** If non-null, doReadRange blocks on this before responding. */
        volatile CountDownLatch gate;

        void putBlock(String path, long blockIndex, byte[] bytes) {
            blocks.put(key(path, blockIndex), bytes);
        }

        private static String key(String path, long blockIndex) {
            return path + ObjectStorage.MULTIPART_SUFFIX + "/" + blockIndex;
        }

        @Override
        public CompletableFuture<Void> write(String path, byte[] content) {
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
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            readRangeCalls.incrementAndGet();
            CountDownLatch g = gate;
            CompletableFuture<ReadResult> out = new CompletableFuture<>();
            Runnable task = () -> {
                long blockIndex = offset / blockSize;
                int offsetInBlock = (int) (offset % blockSize);
                byte[] block = blocks.get(key(path, blockIndex));
                if (block == null) {
                    out.complete(ReadResult.notFound());
                    return;
                }
                int end = Math.min(offsetInBlock + length, block.length);
                if (offsetInBlock >= block.length) {
                    out.complete(ReadResult.notFound());
                    return;
                }
                out.complete(ReadResult.found(Arrays.copyOfRange(block, offsetInBlock, end)));
            };
            if (g != null) {
                new Thread(() -> {
                    try {
                        g.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    task.run();
                }).start();
            } else {
                task.run();
            }
            return out;
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
                // Keys are stored as "<path>.multipart/<idx>"; match the logical prefix.
                int mp = k.indexOf(ObjectStorage.MULTIPART_SUFFIX + "/");
                String logical = mp >= 0 ? k.substring(0, mp) : k;
                if (logical.startsWith(prefix)) {
                    toDrop.add(k);
                }
            }
            toDrop.forEach(blocks::remove);
            // Distinct logical paths removed.
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
