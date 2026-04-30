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
    // We track hit/miss/load stats ourselves because the only way to do an
    // atomic retain-under-lock is via asMap().compute(), and Caffeine's
    // recordStats() does not count compute() invocations as gets. Keeping our
    // own counters is also what makes the stats meaningful for the Grafana
    // panels and the per-request cache_hits_per_request histogram.
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong loadSuccess = new AtomicLong();
    private final AtomicLong loadFailure = new AtomicLong();
    private final AtomicLong loadTimeNanos = new AtomicLong();

    public SegmentBlockCache(long maxBytes) {
        this.maxBytes = maxBytes;
        if (maxBytes <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((BlockKey k, ByteBuf v) -> v == null ? 0 : v.capacity())
                    // Use a synchronous (caller-thread) executor so that the
                    // removal listener is invoked inline during the maintenance
                    // pass rather than dispatched asynchronously to
                    // ForkJoinPool.commonPool(). This guarantees that after
                    // cleanUp() or invalidateAll() returns, every
                    // safeRelease() has already executed and no ByteBuf
                    // reference is left dangling. The listener only does an
                    // atomic ref-count decrement, so the cost is negligible.
                    .executor(Runnable::run)
                    .removalListener((BlockKey k, ByteBuf v, RemovalCause cause) -> {
                        // Release the cache's reference when the entry leaves
                        // the cache. Callers receive retained slices, so an
                        // eviction while a slice is in flight does not cause
                        // use-after-free.
                        if (cause != null && cause.wasEvicted()) {
                            evictions.incrementAndGet();
                        }
                        ReferenceCountUtil.safeRelease(v);
                    })
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
     * <p><b>Concurrency</b>: retain happens under the map's per-entry lock
     * via {@link java.util.concurrent.ConcurrentMap#compute compute}, so it
     * can never race with the removal listener's release. This avoids a
     * nasty {@link io.netty.util.IllegalReferenceCountException} that would
     * otherwise fire when an entry is evicted between
     * {@code cache.get()} returning a reference and the caller bumping its
     * refcount.
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

        // Everything happens inside a single asMap().compute() call so that
        // the retain is atomic with the entry-present check — no window for
        // the removal listener to release underneath us. Caffeine serialises
        // concurrent callers on the same key, giving single-flight semantics
        // for the loader. Hit/miss/load stats are recorded manually inside
        // the function because Caffeine's recordStats() does not instrument
        // asMap().compute().
        ByteBuf retained;
        try {
            retained = cache.asMap().compute(key, (k, existing) -> {
                if (existing != null) {
                    // Hit: bump refcount by one so the caller has a handle
                    // independent of the cache's ownership.
                    hits.incrementAndGet();
                    existing.retain();
                    return existing;
                }
                // Miss: run the loader. Loader returns a buf with refCnt=1
                // (its own fresh allocation). We retain once more so the
                // final refCnt=2 means "one ref owned by the cache (released
                // on eviction), one ref owned by the caller".
                misses.incrementAndGet();
                long startNanos = System.nanoTime();
                ByteBuf loaded;
                try {
                    loaded = loader.load(path, offset, length);
                } catch (IOException ioe) {
                    loadFailure.incrementAndGet();
                    loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
                    throw new CacheLoadException(ioe);
                }
                loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
                if (loaded == null) {
                    loadFailure.incrementAndGet();
                    throw new CacheLoadException(new IOException(
                            "loader returned null for " + path + "@" + offset));
                }
                loadSuccess.incrementAndGet();
                loaded.retain();
                return loaded;
            });
        } catch (CacheLoadException e) {
            throw (IOException) e.getCause();
        }
        if (retained == null) {
            // Should not happen: compute returns non-null either via the hit
            // branch or via the loader branch (loader null/throw becomes
            // CacheLoadException). Defensive fallback.
            return invokeLoader(loader, path, offset, length);
        }
        try {
            // retainedSlice bumps the shared refcount again and returns a
            // caller-owned view with independent reader/writer indices.
            return retained.retainedSlice(0, retained.readableBytes());
        } finally {
            // Drop the "caller retain" we added inside compute; the slice
            // we returned carries its own ref, so the shared entry stays
            // alive as long as either the cache holds it or a slice is
            // outstanding.
            retained.release();
        }
    }

    private ByteBuf invokeLoader(BlockLoader loader, String path, long offset, int length)
            throws IOException {
        long startNanos = System.nanoTime();
        try {
            ByteBuf buf = loader.load(path, offset, length);
            if (buf == null) {
                loadFailure.incrementAndGet();
                loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
                throw new IOException("loader returned null for " + path + "@" + offset);
            }
            loadSuccess.incrementAndGet();
            loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            return buf;
        } catch (IOException e) {
            loadFailure.incrementAndGet();
            loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
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
    // Stats accessors. Hits/misses are 0 when the cache is disabled (pass-
    // through mode) because no lookups happen; load_success / load_failure /
    // load_time still track the pass-through loader calls.
    // ---------------------------------------------------------------------

    public long hitCount() {
        return hits.get();
    }

    public long missCount() {
        return misses.get();
    }

    public long evictionCount() {
        return evictions.get();
    }

    public long loadSuccessCount() {
        return loadSuccess.get();
    }

    public long loadFailureCount() {
        return loadFailure.get();
    }

    public long totalLoadTimeNanos() {
        return loadTimeNanos.get();
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
