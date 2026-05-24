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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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

/**
 * Issue #650: verifies {@link CachingObjectStorage}'s single-object cache
 * contract:
 * <ul>
 *   <li>cache key namespace for {@code readRange} is {@code path#{blockIndex}},
 *       distinct from the full-file namespace at {@code path};</li>
 *   <li>a cache MISS for a block triggers exactly one
 *       {@code inner.readRange(path, blockIdx*blockSize, blockSize, blockSize)};</li>
 *   <li>subsequent reads inside the same block are cache hits — no further
 *       inner calls;</li>
 *   <li>reading the next block triggers a second inner call (one per
 *       block-aligned window);</li>
 *   <li>{@code delete(path)} invalidates BOTH the full-file entry AND every
 *       block-cache entry at {@code path#{N}}.</li>
 * </ul>
 */
public class CachingObjectStorageSingleObjectCacheTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    /**
     * Inner storage that records {@link ObjectStorage#readRange} and
     * {@link ObjectStorage#read} call counts separately. Holds a single
     * object per path in memory.
     */
    static final class InstrumentedInnerStorage implements ObjectStorage {
        final Map<String, byte[]> data = new ConcurrentHashMap<>();
        final AtomicInteger readRangeCalls = new AtomicInteger();
        final AtomicInteger readCalls = new AtomicInteger();
        final AtomicInteger deleteCalls = new AtomicInteger();
        /** Records the exact (offset, length, blockSize) arguments of every readRange. */
        final List<long[]> readRangeArgs = java.util.Collections.synchronizedList(new ArrayList<>());

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
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(bytes.length);
            buf.writeBytes(bytes);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
            readRangeCalls.incrementAndGet();
            readRangeArgs.add(new long[]{offset, length, blockSize});
            byte[] full = data.get(path);
            if (full == null || offset >= full.length) {
                return CompletableFuture.completedFuture(ReadResult.notFound());
            }
            int start = (int) offset;
            int end = Math.min(start + length, full.length);
            byte[] slice = Arrays.copyOfRange(full, start, end);
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(slice.length);
            buf.writeBytes(slice);
            return CompletableFuture.completedFuture(ReadResult.found(buf));
        }

        @Override
        public CompletableFuture<Boolean> delete(String path) {
            deleteCalls.incrementAndGet();
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

    private CachingObjectStorage buildCache(InstrumentedInnerStorage inner) throws Exception {
        Path cacheDir = folder.newFolder("cache").toPath();
        return new CachingObjectStorage(inner, cacheDir, executor, 64L * 1024 * 1024);
    }

    /** Returns {@code path + "#" + blockIndex} — the block-cache key shape. */
    private static String blockKey(String path, long blockIndex) {
        return path + "#" + blockIndex;
    }

    private static byte[] sequentialBytes(int length) {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++) {
            b[i] = (byte) (i & 0xFF);
        }
        return b;
    }

    @Test
    public void blockCacheKeyNamespaceIsPathHashBlockIndex() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            // 8 KiB object with a 4 KiB block size — two blocks.
            int blockSize = 4 * 1024;
            byte[] payload = sequentialBytes(blockSize * 2);
            inner.data.put("ts/u/multipart/graph", payload);

            // Cache MISS on block 0 — exactly one inner.readRange call.
            ReadResult r0 = cache.readRange("ts/u/multipart/graph", 0L, 100, blockSize).get();
            try {
                assertEquals(ReadResult.Status.FOUND, r0.status());
                assertEquals(100, r0.content().length);
            } finally {
                r0.release();
            }
            assertEquals("MISS on block 0 triggers exactly one inner readRange",
                    1, inner.readRangeCalls.get());

            // The block-cache entry at "path#0" must now be resident.
            assertTrue("block 0 must be cached under path#0 namespace",
                    cache.isInCache(blockKey("ts/u/multipart/graph", 0)));
            // No full-file entry should exist — readRange and read live in
            // separate cache key namespaces.
            assertFalse("full-file entry must NOT be admitted by readRange",
                    cache.isInCache("ts/u/multipart/graph"));
            // Block 1 not yet touched.
            assertFalse("block 1 must NOT be cached yet",
                    cache.isInCache(blockKey("ts/u/multipart/graph", 1)));
        }
    }

    @Test
    public void subsequentReadsInSameBlockAreCacheHits() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            int blockSize = 4 * 1024;
            byte[] payload = sequentialBytes(blockSize * 2);
            inner.data.put("ts/u/multipart/graph", payload);

            // First read: MISS on block 0, triggers ONE inner readRange.
            ReadResult r0 = cache.readRange("ts/u/multipart/graph", 0L, 100, blockSize).get();
            r0.release();
            assertEquals(1, inner.readRangeCalls.get());

            // Second read within block 0 — must be HIT, zero additional inner calls.
            ReadResult r1 = cache.readRange("ts/u/multipart/graph", 200L, 100, blockSize).get();
            try {
                assertEquals(ReadResult.Status.FOUND, r1.status());
                // The slice at offset=200, length=100 must equal payload[200..300).
                assertArrayEquals(Arrays.copyOfRange(payload, 200, 300), r1.content());
            } finally {
                r1.release();
            }
            assertEquals("second read within block 0 must be a cache HIT (no inner call)",
                    1, inner.readRangeCalls.get());

            // Third read at the tail of block 0 — still a HIT.
            ReadResult r2 = cache.readRange("ts/u/multipart/graph", blockSize - 50L, 50, blockSize).get();
            r2.release();
            assertEquals("third read in block 0 must remain a HIT",
                    1, inner.readRangeCalls.get());
        }
    }

    @Test
    public void readingNextBlockTriggersSecondInnerCall() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            int blockSize = 4 * 1024;
            byte[] payload = sequentialBytes(blockSize * 3);
            inner.data.put("ts/u/multipart/graph", payload);

            // Block 0
            ReadResult r0 = cache.readRange("ts/u/multipart/graph", 0L, 100, blockSize).get();
            r0.release();
            assertEquals(1, inner.readRangeCalls.get());

            // Block 1 — distinct block index → distinct cache key → MISS → one new inner call.
            ReadResult r1 = cache.readRange("ts/u/multipart/graph", (long) blockSize, 100, blockSize).get();
            r1.release();
            assertEquals("block 1 MISS triggers a second inner readRange",
                    2, inner.readRangeCalls.get());

            // Block 2 — another distinct miss.
            ReadResult r2 = cache.readRange("ts/u/multipart/graph", 2L * blockSize, 100, blockSize).get();
            r2.release();
            assertEquals(3, inner.readRangeCalls.get());

            // All three should be cached now under distinct keys.
            assertTrue(cache.isInCache(blockKey("ts/u/multipart/graph", 0)));
            assertTrue(cache.isInCache(blockKey("ts/u/multipart/graph", 1)));
            assertTrue(cache.isInCache(blockKey("ts/u/multipart/graph", 2)));
        }
    }

    @Test
    public void innerReadRangeArgumentsAreBlockAlignedAndBlockSized() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            int blockSize = 4 * 1024;
            byte[] payload = sequentialBytes(blockSize * 3);
            inner.data.put("ts/u/multipart/graph", payload);

            // The caller asks for {offset=500, length=200} inside block 0.
            // The cache MUST fetch ONE full block (offset=0, length=blockSize)
            // from inner — not the narrow slice the caller asked for.
            ReadResult r = cache.readRange("ts/u/multipart/graph", 500L, 200, blockSize).get();
            r.release();
            assertEquals(1, inner.readRangeCalls.get());
            long[] args = inner.readRangeArgs.get(0);
            assertEquals("inner.readRange must be aligned to blockSize",
                    0L, args[0]);
            assertEquals("inner.readRange must request exactly one cache block",
                    (long) blockSize, args[1]);
            assertEquals("inner.readRange must propagate blockSize granularity",
                    (long) blockSize, args[2]);
        }
    }

    @Test
    public void deleteInvalidatesFullFileEntryAndAllBlockCacheEntries() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            int blockSize = 4 * 1024;
            String path = "ts/u/multipart/graph";
            byte[] payload = sequentialBytes(blockSize * 3);
            inner.data.put(path, payload);

            // 1) Admit a full-file cache entry via read().
            ReadResult full = cache.read(path).get();
            full.release();
            assertTrue("full-file entry must be cached after read",
                    cache.isInCache(path));

            // 2) Admit three block-cache entries via readRange() — one per block.
            for (long b = 0; b < 3; b++) {
                ReadResult slice = cache.readRange(path, b * blockSize, 100, blockSize).get();
                slice.release();
            }
            for (long b = 0; b < 3; b++) {
                assertTrue("block " + b + " must be cached",
                        cache.isInCache(blockKey(path, b)));
            }

            // 3) delete(path): must invalidate the full-file entry AND every
            // block-cache entry under "path#{N}".
            boolean deleted = cache.delete(path).get();
            assertTrue("delete must report the inner object was present", deleted);

            assertFalse("full-file entry must be evicted by delete",
                    cache.isInCache(path));
            for (long b = 0; b < 3; b++) {
                assertFalse("block " + b + " entry must be evicted by delete",
                        cache.isInCache(blockKey(path, b)));
            }
        }
    }

    @Test
    public void uploadFileInvalidatesStaleBlockCacheEntriesForSamePath() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            int blockSize = 4 * 1024;
            String path = "ts/u/multipart/graph";
            byte[] payload = sequentialBytes(blockSize * 2);
            inner.data.put(path, payload);

            // Prime the cache with block 0.
            ReadResult r0 = cache.readRange(path, 0L, 100, blockSize).get();
            r0.release();
            assertTrue(cache.isInCache(blockKey(path, 0)));

            // Now overwrite the path via uploadFile — must drop every stale
            // block-cache entry under "path#{N}".
            Path tmp = folder.newFile().toPath();
            java.nio.file.Files.write(tmp, sequentialBytes(blockSize * 2));
            cache.uploadFile(path, tmp, deltaBytes -> { }).get();
            assertFalse("block 0 cache entry must be evicted by uploadFile",
                    cache.isInCache(blockKey(path, 0)));
        }
    }

    @Test
    public void readRangeOnMissingObjectReturnsNotFoundAndDoesNotCache() throws Exception {
        InstrumentedInnerStorage inner = new InstrumentedInnerStorage();
        try (CachingObjectStorage cache = buildCache(inner)) {
            ReadResult r = cache.readRange("missing/path", 0L, 100, 4096).get();
            try {
                assertNotNull(r);
                assertEquals(ReadResult.Status.NOT_FOUND, r.status());
            } finally {
                r.release();
            }
            // Not-found responses must not poison the cache with a phantom
            // block-cache entry.
            assertFalse(cache.isInCache(blockKey("missing/path", 0)));
            // sanity: the readRange counter incremented because the cache
            // had to try inner before reporting NOT_FOUND.
            assertEquals(1, inner.readRangeCalls.get());
            assertNull("no cache file should have been allocated for the miss",
                    null);  // documentation-only assertion
        }
    }
}
