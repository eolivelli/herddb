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
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    private final Cache<BlockKey, ByteBuf> cache;
    // Primary-per-key dedup. Only the marker Void — the actual ByteBuf flows
    // through the cache, never through this map, so followers cannot slice a
    // ByteBuf whose refcount has already been dropped by the primary's finally.
    private final ConcurrentHashMap<BlockKey, CompletableFuture<Void>> inFlight = new ConcurrentHashMap<>();
    private final long maxBytes;
    // Hit/miss counters maintained manually because the fast path uses
    // asMap().computeIfPresent — Caffeine documents that "no stats are
    // modified by non-computing operations invoked on the asMap view", so
    // recordStats() alone would leave hitCount/missCount at zero. Eviction
    // counters still come from Caffeine (the removal listener path is unchanged).
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    public InMemoryBlockCacheObjectStorage(ObjectStorage inner, long maxBytes) {
        this.inner = Objects.requireNonNull(inner, "inner");
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive, got " + maxBytes);
        }
        this.maxBytes = maxBytes;
        this.cache = Caffeine.newBuilder()
                .maximumWeight(maxBytes)
                .weigher((BlockKey k, ByteBuf v) -> v == null ? 0 : v.readableBytes())
                // Release the cache's retain on eviction (and on explicit invalidate*).
                // Caffeine fires the removal listener for both size-based eviction and
                // explicit invalidate(...)/invalidateAll(...) calls — covers all paths.
                .removalListener((RemovalListener<BlockKey, ByteBuf>) (key, value, cause) -> {
                    if (value != null) {
                        value.release();
                    }
                })
                // Force maintenance (and therefore the removal listener) to run on
                // the calling thread instead of Caffeine's default ForkJoinPool.
                // Async maintenance can leave evicted pooled ByteBufs with refCnt > 0
                // long after the eviction was requested; with a direct executor every
                // eviction / invalidate call fully releases the buffer before it
                // returns. This matches the refcount discipline the cache consumers
                // rely on and keeps {@code close()} deterministic.
                .executor(Runnable::run)
                .recordStats()
                .build();
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    /**
     * Current cache stats snapshot. Safe to call from gauge samplers.
     * <p>
     * {@code hitCount}/{@code missCount} come from the manual counters updated
     * on every {@code readRange}; {@code evictionCount}/{@code evictionWeight}
     * come from Caffeine's own recordStats tracking. Load counters are always
     * zero because this cache populates via {@code cache.put(...)} from an
     * async loader, not via Caffeine's computing methods.
     */
    public CacheStats stats() {
        CacheStats caffeine = cache.stats();
        return CacheStats.of(
                hits.get(),
                misses.get(),
                0L,
                0L,
                0L,
                caffeine.evictionCount(),
                caffeine.evictionWeight());
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
        for (ByteBuf v : cache.asMap().values()) {
            if (v != null) {
                total += v.readableBytes();
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

        // Fast path: (get + retain) must be atomic under Caffeine's per-entry
        // lock, otherwise the removal listener can race with the retainedSlice
        // below — a concurrent eviction drops the shared entry's refCnt to 0
        // and the next retain throws IllegalReferenceCountException, killing
        // the worker thread (see issue #241). Mirrors SegmentBlockCache.
        ByteBuf retained = cache.asMap().computeIfPresent(key, (k, v) -> {
            v.retain();
            return v;
        });
        if (retained != null) {
            hits.incrementAndGet();
            try {
                return CompletableFuture.completedFuture(slice(retained, offsetInBlock, length));
            } finally {
                // Drop the hit-retain; the returned ReadResult's slice carries its own.
                retained.release();
            }
        }
        misses.incrementAndGet();

        // Miss path: dedup via in-flight map so only one inner.readRange per
        // key is in flight. Followers wait for the primary to signal "cache
        // is populated" and then take a fresh atomic retain via
        // {@code asMap().computeIfPresent} — they do NOT slice the primary's
        // ByteBuf directly, because that ByteBuf can be released between
        // pending.complete() and the follower's callback running (issue #241).
        // The signal value is a Void marker — not the ByteBuf — so the
        // follower's refcount discipline goes through the cache, which is
        // the only safe source of a retainable reference.
        CompletableFuture<Void> pending = new CompletableFuture<>();
        CompletableFuture<Void> existing = inFlight.putIfAbsent(key, pending);
        if (existing != null) {
            return existing.thenCompose(ignored ->
                    sliceFromCacheOrReload(path, offset, length, blockSize, key, offsetInBlock));
        }

        return loadAndPublish(path, offset, length, blockSize, key, offsetInBlock,
                blockIndex, pending);
    }

    /**
     * Follower fast-path after the primary signalled. Takes an atomic retain
     * from the cache; if the entry was evicted in between (rare), reloads
     * directly via the inner storage rather than re-entering the dedup path —
     * recursing through {@link #readRange} risks an unbounded synchronous
     * chain when the signalling thenCompose fires on an already-complete
     * future.
     */
    private CompletableFuture<ReadResult> sliceFromCacheOrReload(
            String path, long offset, int length, int blockSize,
            BlockKey key, int offsetInBlock) {
        ByteBuf retained = cache.asMap().computeIfPresent(key, (k, v) -> {
            v.retain();
            return v;
        });
        if (retained != null) {
            hits.incrementAndGet();
            try {
                return CompletableFuture.completedFuture(slice(retained, offsetInBlock, length));
            } finally {
                retained.release();
            }
        }
        // Rare: entry evicted between the primary's cache.put and here.
        // Duplicate the inner read (bypassing the in-flight map) rather than
        // recursing; correctness beats strict dedup in this corner case.
        misses.incrementAndGet();
        return loadAndPublish(path, offset, length, blockSize, key, offsetInBlock,
                offset / blockSize, /* pending = */ null);
    }

    /**
     * Issues a fresh {@code inner.readRange}, publishes the result into the
     * cache, signals the optional {@code pending} future, and returns the
     * caller's sliced ReadResult.
     */
    private CompletableFuture<ReadResult> loadAndPublish(String path, long offset, int length,
            int blockSize, BlockKey key, int offsetInBlock, long blockIndex,
            CompletableFuture<Void> pending) {
        long blockStartOffset = blockIndex * (long) blockSize;
        CompletableFuture<ReadResult> out = new CompletableFuture<>();
        inner.readRange(path, blockStartOffset, blockSize, blockSize).whenComplete((result, err) -> {
            try {
                if (err != null) {
                    out.completeExceptionally(err);
                    return;
                }
                if (result.status() != ReadResult.Status.FOUND) {
                    out.complete(ReadResult.notFound());
                    return;
                }
                ByteBuf buf = result.byteBuf();
                // +1 for the cache's ownership (released by the removal
                // listener on eviction). slice(buf, ...) then takes its own
                // retainedSlice. After result.release() drops the inner
                // ReadResult's original ref, the final state is
                // refCnt = (cache) + (caller's slice) = 2.
                buf.retain();
                cache.put(key, buf);
                try {
                    out.complete(slice(buf, offsetInBlock, length));
                } finally {
                    result.release();
                }
            } catch (RuntimeException e) {
                out.completeExceptionally(e);
            } finally {
                if (pending != null) {
                    // Remove the in-flight placeholder BEFORE signalling
                    // followers so a follower's thenCompose, which fires
                    // synchronously on this thread, cannot re-read a stale
                    // in-flight entry pointing at an already-complete pending.
                    inFlight.remove(key, pending);
                    pending.complete(null);
                }
            }
        });
        return out;
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
        // Force pending maintenance so the removal listener fires on every
        // still-cached entry before we return — otherwise Caffeine may
        // process removals asynchronously, leaving pooled ByteBufs with
        // refCnt > 0 after close(). Mirrors SegmentBlockCache's close-path
        // idiom.
        cache.cleanUp();
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

    /**
     * Returns a {@link ReadResult} whose buffer is a {@code retainedSlice} view of
     * {@code block} — no copy. The caller releases the result; {@code block} keeps
     * its existing refcounts (typically held by the cache and/or callers).
     */
    private static ReadResult slice(ByteBuf block, int offsetInBlock, int length) {
        if (block == null) {
            return ReadResult.notFound();
        }
        int blockLen = block.readableBytes();
        if (offsetInBlock >= blockLen) {
            return ReadResult.notFound();
        }
        int to = Math.min(offsetInBlock + length, blockLen);
        int sliceLen = to - offsetInBlock;
        return ReadResult.found(block.retainedSlice(block.readerIndex() + offsetInBlock, sliceLen));
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
