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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared, byte-weighted, multipart-aware block cache for remote vector-index
 * segment files. Replaces the page-keyed {@code SharedSegmentPageCache} that
 * was removed when page-based persistence was dropped in favour of multipart
 * objects.
 *
 * <p>Keys are {@code (path, blockIndex)} pairs where {@code path} is the
 * logical multipart path used by {@link RemoteRandomAccessReader} and
 * {@code blockIndex = offset / blockSize}. Values are pooled direct
 * {@link ByteBuf}s owned by the cache; on a hit {@link #getBlock} returns a
 * fresh {@link ByteBuf#retainedSlice() retained slice} which the caller is
 * responsible for releasing. On eviction the cache releases its own reference
 * via a Caffeine removal listener.
 *
 * <p>Eviction is byte-weighted LRU bounded by {@code maxBytes}. A budget of
 * {@code 0} (or negative) disables caching: use {@link #disabled()} to get a
 * pass-through singleton that always invokes the loader and caches nothing.
 * Callers always hold a non-null {@code SegmentBlockCache} — the disabled
 * instance is interchangeable with a real one and eliminates null checks.
 *
 * @author enrico.olivelli
 */
public final class SegmentBlockCache {

    /**
     * Loads a single block when the cache misses. The returned {@link ByteBuf}
     * must be caller-owned: the cache takes ownership and is responsible for
     * releasing it on eviction. Implementations are free to return a pooled
     * direct buffer (the typical case for network reads).
     */
    @FunctionalInterface
    public interface BlockLoader {
        ByteBuf load(String path, long offset, int length) throws IOException;
    }

    private static final SegmentBlockCache DISABLED = new SegmentBlockCache(0L);

    /**
     * @return a singleton pass-through cache. Calls to {@link #getBlock}
     *     always invoke the loader; no entries are stored. Intended for
     *     configurations where the block cache is explicitly off, or for
     *     tests / tooling that does not care about caching.
     */
    public static SegmentBlockCache disabled() {
        return DISABLED;
    }

    private final long maxBytes;
    private final Cache<BlockKey, ByteBuf> cache;
    private final AtomicLong passthroughLoads = new AtomicLong();
    private final AtomicLong passthroughLoadFailures = new AtomicLong();

    public SegmentBlockCache(long maxBytes) {
        this.maxBytes = maxBytes;
        if (maxBytes <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((BlockKey k, ByteBuf v) -> v == null ? 0 : v.capacity())
                    .removalListener((BlockKey k, ByteBuf v, RemovalCause cause) -> {
                        // Release the cache's reference when the entry leaves
                        // the cache. Callers receive retained slices, so an
                        // eviction while a slice is in flight does not cause
                        // use-after-free.
                        ReferenceCountUtil.safeRelease(v);
                    })
                    .recordStats()
                    .build();
        }
    }

    /** Returns {@code true} when caching is enabled (configured budget > 0). */
    public boolean isActive() {
        return cache != null;
    }

    public long maxBytes() {
        return maxBytes;
    }

    /**
     * Fetches the block at {@code (path, offset, length)}, consulting the
     * cache first and invoking {@code loader} on miss. {@code offset} must be
     * a multiple of {@code length} — this is the natural alignment used by
     * {@link RemoteRandomAccessReader}, where the read window is a fixed-size
     * sliding buffer.
     *
     * <p><b>Ownership</b>: the returned {@link ByteBuf} is a caller-owned
     * retained slice; the caller MUST release it exactly once. The underlying
     * shared entry is released automatically when evicted.
     *
     * @param path       logical multipart path
     * @param offset     block start, must be a multiple of {@code length}
     * @param length     block length in bytes (> 0)
     * @param loader     invoked at most once per key on miss
     */
    public ByteBuf getBlock(String path, long offset, int length, BlockLoader loader)
            throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(loader, "loader");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (cache == null) {
            // Pass-through: loader owns the buffer, no retain/release dance.
            return invokeLoader(loader, path, offset, length);
        }
        long blockIndex = offset / length;
        BlockKey key = new BlockKey(path, blockIndex);
        try {
            ByteBuf shared = cache.get(key, k -> {
                try {
                    ByteBuf loaded = loader.load(path, offset, length);
                    if (loaded == null) {
                        throw new CacheLoadException(new IOException(
                                "loader returned null for " + path + "@" + offset));
                    }
                    return loaded;
                } catch (IOException e) {
                    throw new CacheLoadException(e);
                }
            });
            if (shared == null) {
                throw new IOException("cache load returned null for " + path + "@" + offset);
            }
            // retainedSlice bumps the refcount on the shared entry and returns
            // a duplicate view with its own reader/writer indices. The caller
            // releases the slice; the cache releases the shared entry on
            // eviction.
            return shared.retainedSlice(0, shared.readableBytes());
        } catch (CacheLoadException e) {
            throw (IOException) e.getCause();
        }
    }

    private ByteBuf invokeLoader(BlockLoader loader, String path, long offset, int length)
            throws IOException {
        try {
            ByteBuf buf = loader.load(path, offset, length);
            if (buf == null) {
                passthroughLoadFailures.incrementAndGet();
                throw new IOException("loader returned null for " + path + "@" + offset);
            }
            passthroughLoads.incrementAndGet();
            return buf;
        } catch (IOException e) {
            passthroughLoadFailures.incrementAndGet();
            throw e;
        }
    }

    /**
     * Checks whether the cache currently holds the block at
     * {@code (path, offset, length)} without triggering a load or affecting
     * LRU ordering. Used by {@link RemoteRandomAccessReader} to distinguish
     * per-request cache hits from cache misses.
     */
    public boolean containsBlock(String path, long offset, int length) {
        if (cache == null) {
            return false;
        }
        long blockIndex = offset / length;
        return cache.asMap().containsKey(new BlockKey(path, blockIndex));
    }

    /**
     * Removes every cached block whose key path equals {@code path}. Called
     * when a multipart segment file is deleted or rewritten so that stale
     * bytes cannot be served to a subsequent reader that happens to hit the
     * same logical path.
     */
    public void invalidatePath(String path) {
        if (cache == null || path == null) {
            return;
        }
        Iterator<BlockKey> it = cache.asMap().keySet().iterator();
        while (it.hasNext()) {
            BlockKey k = it.next();
            if (path.equals(k.path)) {
                it.remove();
            }
        }
    }

    /**
     * Removes every cached block whose key path starts with {@code prefix}.
     * Used by bulk deletions (e.g. {@code eraseTablespaceData}) so that
     * multipart segments sharing a logical prefix are invalidated together.
     */
    public void invalidatePrefix(String prefix) {
        if (cache == null || prefix == null) {
            return;
        }
        Iterator<BlockKey> it = cache.asMap().keySet().iterator();
        while (it.hasNext()) {
            BlockKey k = it.next();
            if (k.path.startsWith(prefix)) {
                it.remove();
            }
        }
    }

    public void clear() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    /**
     * Forces Caffeine to drain its async maintenance queue. Tests use this
     * before asserting on eviction counts; not needed in production.
     */
    public void cleanUp() {
        if (cache != null) {
            cache.cleanUp();
        }
    }

    /**
     * Convenience used by tests: copy a cached entry's bytes into a heap
     * array without affecting LRU ordering. Returns {@code null} when the
     * entry is not cached.
     */
    byte[] peekBytes(String path, long offset, int length) {
        if (cache == null) {
            return null;
        }
        long blockIndex = offset / length;
        ByteBuf shared = cache.asMap().get(new BlockKey(path, blockIndex));
        if (shared == null) {
            return null;
        }
        byte[] copy = new byte[shared.readableBytes()];
        shared.getBytes(shared.readerIndex(), copy);
        return copy;
    }

    /**
     * Convenience for the common pattern of wrapping a heap byte[] from a
     * legacy loader into an unpooled {@link ByteBuf} that the cache can own.
     */
    public static ByteBuf wrapForCache(byte[] bytes) {
        return io.netty.buffer.Unpooled.wrappedBuffer(bytes);
    }

    /**
     * Allocates a pooled direct buffer of {@code length} bytes. Exposed for
     * loaders that build their own payload rather than copying from another
     * {@link ByteBuf}.
     */
    public static ByteBuf allocateDirect(int length) {
        return PooledByteBufAllocator.DEFAULT.directBuffer(length);
    }

    // ---------------------------------------------------------------------
    // Stats accessors — all return 0 when the cache is disabled.
    // ---------------------------------------------------------------------

    public long hitCount() {
        return cache == null ? 0 : stats().hitCount();
    }

    public long missCount() {
        return cache == null ? 0 : stats().missCount();
    }

    public long evictionCount() {
        return cache == null ? 0 : stats().evictionCount();
    }

    public long loadSuccessCount() {
        return cache == null ? passthroughLoads.get() : stats().loadSuccessCount();
    }

    public long loadFailureCount() {
        return cache == null ? passthroughLoadFailures.get() : stats().loadFailureCount();
    }

    public long totalLoadTimeNanos() {
        return cache == null ? 0 : stats().totalLoadTime();
    }

    public long estimatedSize() {
        return cache == null ? 0 : cache.estimatedSize();
    }

    public long weightedSize() {
        if (cache == null) {
            return 0;
        }
        return cache.policy().eviction()
                .map(e -> e.weightedSize().orElse(0L))
                .orElse(0L);
    }

    private CacheStats stats() {
        return cache.stats();
    }

    // ---------------------------------------------------------------------
    // Types
    // ---------------------------------------------------------------------

    /** Immutable composite cache key. Hash is precomputed to avoid repeated path hashing. */
    static final class BlockKey {
        final String path;
        final long blockIndex;
        private final int hash;

        BlockKey(String path, long blockIndex) {
            this.path = Objects.requireNonNull(path, "path");
            this.blockIndex = blockIndex;
            this.hash = 31 * path.hashCode() + Long.hashCode(blockIndex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlockKey)) {
                return false;
            }
            BlockKey k = (BlockKey) o;
            return hash == k.hash && blockIndex == k.blockIndex && path.equals(k.path);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "BlockKey{" + path + "@" + blockIndex + '}';
        }
    }

    /** Unchecked wrapper used inside the Caffeine loader to propagate IOException. */
    private static final class CacheLoadException extends RuntimeException {
        CacheLoadException(IOException cause) {
            super(cause);
        }
    }
}
