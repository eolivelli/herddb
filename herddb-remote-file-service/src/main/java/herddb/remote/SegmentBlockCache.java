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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * Async sibling of {@link BlockLoader}: returns a {@link CompletableFuture}
     * that completes with a caller-owned {@link ByteBuf}. The cache takes
     * ownership of the buffer on insert (refCnt → 2: one for the cache, one
     * for the first caller) and releases its reference via the eviction
     * listener, exactly like the sync path.
     */
    @FunctionalInterface
    public interface AsyncBlockLoader {
        CompletableFuture<ByteBuf> loadAsync(String path, long offset, int length);
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
    /**
     * Tracks async loads currently in flight so that concurrent callers for the
     * same {@code (path, blockIndex)} share a single network read (single-flight
     * semantics). Non-null only when caching is enabled ({@code cache != null}).
     * Each {@link InFlightLoad} holds the result futures of every caller waiting
     * on the load; the owning caller detaches the entry and fulfils all waiters
     * when the load completes.
     */
    private final ConcurrentHashMap<BlockKey, InFlightLoad> inFlightAsync;
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
            this.inFlightAsync = null;
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
            this.inFlightAsync = new ConcurrentHashMap<>();
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

    /**
     * Async sibling of {@link #getBlock}: fetches the block at
     * {@code (path, offset, length)} and returns a {@link CompletableFuture}
     * that completes with a <em>caller-owned retained slice</em> of the cached
     * buffer. Ownership rules are identical to those of {@link #getBlock}:
     * the caller MUST release the returned {@link ByteBuf} exactly once; the
     * cache's own reference is released on eviction.
     *
     * <p><b>Single-flight</b>: concurrent callers for the same
     * {@code (path, blockIndex)} share a single in-flight loader call via
     * {@link #inFlightAsync}. When the load completes, the owning caller hands
     * every waiter (itself and all piggybackers) its own independent
     * {@link ByteBuf#retainedSlice retained slice}, so they can release
     * independently. All slices are created by the owner while the freshly
     * loaded buffer is provably still alive, so a waiter can never observe a
     * released buffer (issue #557).
     *
     * <p><b>Stats</b>: a concurrent burst of misses for the same key counts as
     * a single miss — only the in-flight owner increments the miss counter,
     * piggybacking callers do not. This mirrors the effective behaviour of the
     * sync path, where concurrent callers serialise inside {@code compute()}
     * and only the first records a miss.
     *
     * <p><b>Pass-through</b>: when the cache is disabled ({@code cache == null})
     * the loader is invoked directly; load-time and success/failure stats are
     * still tracked.
     *
     * @param path   logical multipart path
     * @param offset block start, must be a multiple of {@code length}
     * @param length block length in bytes (&gt; 0)
     * @param loader invoked at most once per key on miss
     * @return future completing with a caller-owned retained-slice {@link ByteBuf}
     */
    public CompletableFuture<ByteBuf> getBlockAsync(String path, long offset, int length,
                                                    AsyncBlockLoader loader) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(loader, "loader");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }

        if (cache == null) {
            // Pass-through: no retain/release dance, just track load stats.
            long startNanos = System.nanoTime();
            return loader.loadAsync(path, offset, length).whenComplete((buf, err) -> {
                loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
                if (err != null || buf == null) {
                    loadFailure.incrementAndGet();
                } else {
                    loadSuccess.incrementAndGet();
                }
            });
        }

        long blockIndex = offset / length;
        BlockKey key = new BlockKey(path, blockIndex);

        // --- Fast path: check cache with atomic retain-under-lock ---
        // Use computeIfPresent so the retain and the entry-present check are
        // serialised with the removal listener, exactly as in getBlock().
        ByteBuf[] retained = new ByteBuf[1];
        cache.asMap().computeIfPresent(key, (k, existing) -> {
            hits.incrementAndGet();
            existing.retain();
            retained[0] = existing;
            return existing;
        });
        if (retained[0] != null) {
            ByteBuf slice = retained[0].retainedSlice(0, retained[0].readableBytes());
            retained[0].release();
            return CompletableFuture.completedFuture(slice);
        }

        // --- Miss path: single-flight via inFlightAsync ---
        // Every caller for this key registers its own result future in a
        // shared InFlightLoad. The first caller becomes the "owner" and fires
        // the loader; when the load completes the owner detaches the entry and
        // hands every registered waiter its own independent retained slice.
        // Unlike a thenApply()-based piggyback, the owner creates all slices
        // itself while the freshly loaded buffer is provably still alive, so a
        // waiter can never observe a released buffer (issue #557).
        CompletableFuture<ByteBuf> ourFuture = new CompletableFuture<>();
        boolean[] isOwner = {false};
        inFlightAsync.compute(key, (k, current) -> {
            if (current == null || current.sealed) {
                // No load in flight (or the previous one is already being
                // delivered and no longer accepts waiters): start a new one.
                InFlightLoad load = new InFlightLoad();
                load.waiters.add(ourFuture);
                isOwner[0] = true;
                return load;
            }
            // Piggyback on the in-flight load: the owner will complete
            // ourFuture with an independent slice once the load resolves.
            current.waiters.add(ourFuture);
            return current;
        });
        if (!isOwner[0]) {
            // Piggybacker: nothing else to do — the owner already counted the
            // miss and will fulfil ourFuture.
            return ourFuture;
        }

        // We are the in-flight owner.
        misses.incrementAndGet();
        long startNanos = System.nanoTime();
        // Guard against a loader whose synchronous portion throws before
        // returning a future. Without this, the InFlightLoad would linger in
        // inFlightAsync forever and every waiter would deadlock.
        CompletableFuture<ByteBuf> loadFuture;
        try {
            loadFuture = loader.loadAsync(path, offset, length);
        } catch (RuntimeException t) {
            loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            loadFailure.incrementAndGet();
            failAllWaiters(detachWaiters(key), t);
            return ourFuture;
        }
        loadFuture.whenComplete((loaded, err) -> {
            loadTimeNanos.addAndGet(System.nanoTime() - startNanos);
            // Detach the InFlightLoad and snapshot every waiter. The compute()
            // call serialises with concurrent piggyback registrations on the
            // same key, so after this returns no new waiter can attach.
            List<CompletableFuture<ByteBuf>> waiters = detachWaiters(key);

            if (err != null) {
                loadFailure.incrementAndGet();
                failAllWaiters(waiters, err);
                return;
            }
            if (loaded == null) {
                loadFailure.incrementAndGet();
                failAllWaiters(waiters, new IOException(
                        "loader returned null for " + path + "@" + offset));
                return;
            }
            loadSuccess.incrementAndGet();

            // Adopt the freshly loaded buffer into the Caffeine cache.
            // retain() brings refCnt 1 → 2: one reference for the cache
            // (released by the eviction listener) and one "distribution"
            // reference that we consume below by handing out per-waiter
            // slices.
            loaded.retain();

            // compute() serialises with any concurrent sync getBlock() call on
            // the same key: if another thread raced and inserted first, we
            // keep that entry and discard our freshly loaded buffer.
            ByteBuf[] shared = new ByteBuf[1];
            cache.asMap().compute(key, (k, prev) -> {
                if (prev != null) {
                    // Concurrent sync insert won the race. Retain prev once as
                    // our distribution reference.
                    prev.retain();
                    shared[0] = prev;
                    return prev;
                }
                shared[0] = loaded;
                return loaded; // cache takes the extra retain we did above
            });

            ByteBuf sharedBuf = shared[0];
            if (sharedBuf != loaded) {
                // The concurrent sync insert won: our 'loaded' buffer is now
                // redundant. refCnt = 2 (loader's original + our retain) —
                // drop both. 'sharedBuf' carries the single distribution
                // reference retained inside compute() above.
                loaded.release(2);
            }
            // Hand every waiter (owner + piggybackers) its own independent
            // retained slice. Each retainedSlice() bumps sharedBuf's refCnt;
            // the matching release happens when that caller releases its
            // slice. Slicing happens here, on the owner thread, while
            // sharedBuf is provably alive (we hold the distribution ref).
            try {
                for (CompletableFuture<ByteBuf> waiter : waiters) {
                    ByteBuf slice = sharedBuf.retainedSlice(0, sharedBuf.readableBytes());
                    if (!waiter.complete(slice)) {
                        // The caller cancelled its future before delivery —
                        // release the orphaned slice so it is not leaked.
                        slice.release();
                    }
                }
            } finally {
                // Drop the distribution reference; sharedBuf now carries only
                // the cache reference plus one reference per outstanding slice.
                sharedBuf.release();
            }
        });
        return ourFuture;
    }

    /**
     * Atomically removes the in-flight load for {@code key} and returns the
     * list of waiter futures registered on it. Marks the entry {@code sealed}
     * so that a piggyback registration racing this call starts a fresh load
     * instead of attaching to one that is already being delivered. Returns an
     * empty list when no entry is present.
     */
    private List<CompletableFuture<ByteBuf>> detachWaiters(BlockKey key) {
        List<CompletableFuture<ByteBuf>> waiters = new ArrayList<>();
        inFlightAsync.compute(key, (k, current) -> {
            if (current != null) {
                current.sealed = true;
                waiters.addAll(current.waiters);
            }
            return null;
        });
        return waiters;
    }

    /** Completes every waiter future exceptionally with {@code error}. */
    private static void failAllWaiters(List<CompletableFuture<ByteBuf>> waiters,
                                       Throwable error) {
        for (CompletableFuture<ByteBuf> waiter : waiters) {
            waiter.completeExceptionally(error);
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

    /**
     * Bookkeeping for a single in-flight async load. Holds the result futures
     * of every caller waiting on the load: the first entry is the owner that
     * fired the loader, the rest are piggybackers. All fields are mutated only
     * inside an {@code inFlightAsync.compute()} callback, so the
     * {@link ConcurrentHashMap} per-key lock provides the necessary mutual
     * exclusion and visibility — no additional synchronization is required.
     */
    private static final class InFlightLoad {
        /** Result futures of every caller awaiting this load. */
        final List<CompletableFuture<ByteBuf>> waiters = new ArrayList<>();
        /**
         * Set once the owner has detached this entry to deliver results. A
         * sealed entry never accepts new waiters: a racing piggyback starts a
         * fresh load instead.
         */
        boolean sealed;
    }
}
