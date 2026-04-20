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
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/**
 * Shared, byte-weighted, multipart-aware block cache for remote vector-index
 * segment files. Replaces the page-keyed {@code SharedSegmentPageCache} that
 * was removed when page-based persistence was dropped in favour of multipart
 * objects.
 *
 * <p>Keys are {@code (path, blockIndex)} pairs where {@code path} is the
 * logical multipart path used by {@link RemoteRandomAccessReader} and
 * {@code blockIndex = offset / blockSize}. Values are {@code byte[]} payloads
 * owned by the cache. On a miss the provided {@link BlockLoader} is invoked
 * exactly once per key (Caffeine {@code get} de-duplicates concurrent misses);
 * the same {@code byte[]} instance is then handed to all waiters. The array
 * is immutable once returned: callers must not mutate it.
 *
 * <p>Eviction is byte-weighted LRU bounded by {@code maxBytes}. A value of
 * {@code 0} (or negative) turns the cache off — {@link #getBlock} then invokes
 * the loader on every call and caches nothing.
 *
 * <p>Why {@code byte[]} instead of a direct {@code ByteBuf}: the cache is
 * long-lived and shared across readers, so owning Netty-pooled buffers would
 * require an explicit {@code RemovalListener} to release them on eviction and
 * careful ref-counting on every hit. Storing {@code byte[]} keeps lifecycle
 * trivial; readers wrap the array with {@code Unpooled.wrappedBuffer(byte[])}
 * for a zero-copy {@code ByteBuf} view.
 *
 * @author enrico.olivelli
 */
public final class SegmentBlockCache {

    /** Loads a single block when the cache misses. */
    @FunctionalInterface
    public interface BlockLoader {
        byte[] load(String path, long offset, int length) throws IOException;
    }

    private final long maxBytes;
    @Nullable
    private final Cache<BlockKey, byte[]> cache;
    private final AtomicLong passthroughLoads = new AtomicLong();
    private final AtomicLong passthroughLoadFailures = new AtomicLong();

    public SegmentBlockCache(long maxBytes) {
        this.maxBytes = maxBytes;
        if (maxBytes <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((BlockKey k, byte[] v) -> v == null ? 0 : v.length)
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
     * sliding buffer. The value is returned as a caller-read-only {@code byte[]};
     * do not mutate.
     *
     * @param path       logical multipart path
     * @param offset     block start, must be a multiple of {@code length}
     * @param length     block length in bytes (>= 0); when cache is inactive
     *                   the loader receives exactly these arguments
     * @param loader     invoked at most once per key on miss
     */
    public byte[] getBlock(String path, long offset, int length, BlockLoader loader)
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
            return invokeLoader(loader, path, offset, length);
        }
        long blockIndex = offset / length;
        BlockKey key = new BlockKey(path, blockIndex);
        try {
            return cache.get(key, k -> {
                try {
                    byte[] bytes = loader.load(path, offset, length);
                    if (bytes == null) {
                        throw new CacheLoadException(new IOException(
                                "loader returned null for " + path + "@" + offset));
                    }
                    return bytes;
                } catch (IOException e) {
                    throw new CacheLoadException(e);
                }
            });
        } catch (CacheLoadException e) {
            throw (IOException) e.getCause();
        }
    }

    private byte[] invokeLoader(BlockLoader loader, String path, long offset, int length)
            throws IOException {
        try {
            byte[] bytes = loader.load(path, offset, length);
            if (bytes == null) {
                passthroughLoadFailures.incrementAndGet();
                throw new IOException("loader returned null for " + path + "@" + offset);
            }
            passthroughLoads.incrementAndGet();
            return bytes;
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
