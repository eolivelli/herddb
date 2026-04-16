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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decorator that holds a bounded in-heap cache of whole multipart blocks in front of an inner
 * {@link ObjectStorage}. Aimed at the server-side random-I/O pattern of ANN queries against
 * FusedPQ vector indexes: the HNSW traversal re-reads the same few graph blocks many times, and
 * serving those from RAM removes both the disk hop and the S3 hop.
 * <p>
 * Keys are {@code (path, blockIndex)}; values are the full block bytes as returned by a
 * {@code readRange(path, blockStartOffset, blockSize, blockSize)} on the inner storage. Range
 * requests are always answered by slicing the cached block, so the cache is transparent to
 * callers: it never changes the bytes observed by a client. Concurrent misses for the same
 * block are deduplicated through an in-flight future map so only one inner read fires.
 * <p>
 * The cache is weight-bounded by bytes. Writes and deletes invalidate every cached block that
 * could have contained stale data. Caffeine stats are exposed via {@link #stats()} so the
 * server can wire hit/miss/eviction counters into its metrics registry.
 *
 * @author enrico.olivelli
 */
public class InMemoryBlockCacheObjectStorage implements ObjectStorage {

    private final ObjectStorage inner;
    private final Cache<BlockKey, byte[]> cache;
    private final ConcurrentHashMap<BlockKey, CompletableFuture<ReadResult>> inFlight = new ConcurrentHashMap<>();
    private final long maxBytes;

    public InMemoryBlockCacheObjectStorage(ObjectStorage inner, long maxBytes) {
        this.inner = Objects.requireNonNull(inner, "inner");
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxBytes)
                .weigher((BlockKey k, byte[] v) -> v == null ? 0 : v.length)
                .recordStats()
                .build();
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    /** Current Caffeine stats snapshot. Safe to call from gauge samplers. */
    public CacheStats stats() {
        return cache.stats();
    }

    /** Approximate number of cached blocks. */
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    /**
     * Approximate bytes currently held by the cache. Walks the {@code asMap()} view without
     * triggering maintenance; intended for gauges, not hot-path use.
     */
    public long estimatedBytes() {
        long total = 0;
        for (byte[] v : cache.asMap().values()) {
            if (v != null) {
                total += v.length;
            }
        }
        return total;
    }

    @Override
    public CompletableFuture<Void> write(String path, byte[] content) {
        invalidateAllBlocksOf(path);
        return inner.write(path, content);
    }

    @Override
    public CompletableFuture<ReadResult> read(String path) {
        // Full-file reads are not block-addressable, so we just pass through.
        return inner.read(path);
    }

    @Override
    public CompletableFuture<Void> writeBlock(String path, long blockIndex, byte[] content) {
        cache.invalidate(new BlockKey(path, blockIndex));
        return inner.writeBlock(path, blockIndex, content);
    }

    @Override
    public CompletableFuture<ReadResult> readRange(String path, long offset, int length, int blockSize) {
        long blockIndex = offset / blockSize;
        int offsetInBlock = (int) (offset % blockSize);
        BlockKey key = new BlockKey(path, blockIndex);

        byte[] cached = cache.getIfPresent(key);
        if (cached != null) {
            return CompletableFuture.completedFuture(slice(cached, offsetInBlock, length));
        }

        CompletableFuture<ReadResult> pending = new CompletableFuture<>();
        CompletableFuture<ReadResult> existing = inFlight.putIfAbsent(key, pending);
        if (existing != null) {
            return existing.thenApply(full -> slice(fullBytes(full), offsetInBlock, length));
        }

        long blockStartOffset = blockIndex * (long) blockSize;
        inner.readRange(path, blockStartOffset, blockSize, blockSize).whenComplete((result, err) -> {
            try {
                if (err != null) {
                    pending.completeExceptionally(err);
                    return;
                }
                try {
                    byte[] content = result.content();  // Extract bytes from pooled ByteBuf
                    if (result.status() == ReadResult.Status.FOUND && content != null) {
                        cache.put(key, content);
                    }
                    // Create a new ReadResult wrapping extracted bytes
                    ReadResult toReturn;
                    if (result.status() == ReadResult.Status.FOUND) {
                        ByteBuf newBuf = PooledByteBufAllocator.DEFAULT.directBuffer(content.length);
                        newBuf.writeBytes(content);
                        toReturn = ReadResult.found(newBuf);
                    } else {
                        toReturn = ReadResult.notFound();
                    }
                    pending.complete(toReturn);
                } finally {
                    result.release();  // Release the original pooled ByteBuf
                }
            } finally {
                inFlight.remove(key, pending);
            }
        });

        return pending.thenApply(full -> slice(fullBytes(full), offsetInBlock, length));
    }

    @Override
    public CompletableFuture<Boolean> deleteLogical(String path) {
        invalidateAllBlocksOf(path);
        return inner.deleteLogical(path);
    }

    @Override
    public CompletableFuture<List<String>> listLogical(String prefix) {
        return inner.listLogical(prefix);
    }

    @Override
    public CompletableFuture<Boolean> delete(String path) {
        invalidateAllBlocksOf(path);
        return inner.delete(path);
    }

    @Override
    public CompletableFuture<List<String>> list(String prefix) {
        return inner.list(prefix);
    }

    @Override
    public CompletableFuture<Integer> deleteByPrefix(String prefix) {
        return inner.deleteByPrefix(prefix).thenApply(count -> {
            List<BlockKey> toDrop = new ArrayList<>();
            for (BlockKey k : cache.asMap().keySet()) {
                if (k.path.startsWith(prefix)) {
                    toDrop.add(k);
                }
            }
            cache.invalidateAll(toDrop);
            return count;
        });
    }

    @Override
    public void close() throws Exception {
        cache.invalidateAll();
        inner.close();
    }

    private void invalidateAllBlocksOf(String path) {
        List<BlockKey> toDrop = new ArrayList<>();
        for (BlockKey k : cache.asMap().keySet()) {
            if (k.path.equals(path)) {
                toDrop.add(k);
            }
        }
        cache.invalidateAll(toDrop);
    }

    private static byte[] fullBytes(ReadResult full) {
        if (full == null || full.status() != ReadResult.Status.FOUND) {
            return null;
        }
        return full.content();
    }

    private static ReadResult slice(byte[] block, int offsetInBlock, int length) {
        if (block == null) {
            return ReadResult.notFound();
        }
        if (offsetInBlock >= block.length) {
            return ReadResult.notFound();
        }
        int to = Math.min(offsetInBlock + length, block.length);
        int sliceLen = to - offsetInBlock;
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(sliceLen);
        buf.writeBytes(block, offsetInBlock, sliceLen);
        return ReadResult.found(buf);
    }

    /** Package-private: force Caffeine maintenance. Used by tests. */
    void cleanUp() {
        cache.cleanUp();
    }

    /** Compound cache key: logical path + block index. */
    private static final class BlockKey {
        final String path;
        final long blockIndex;

        BlockKey(String path, long blockIndex) {
            this.path = path;
            this.blockIndex = blockIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlockKey)) {
                return false;
            }
            BlockKey other = (BlockKey) o;
            return blockIndex == other.blockIndex && path.equals(other.path);
        }

        @Override
        public int hashCode() {
            int h = path.hashCode();
            h = 31 * h + Long.hashCode(blockIndex);
            return h;
        }
    }
}
