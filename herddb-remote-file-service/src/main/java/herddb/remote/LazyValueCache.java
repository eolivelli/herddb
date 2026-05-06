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
import herddb.utils.HerdDBByteBufAllocators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Bounded off-heap cache of per-record value bytes, keyed by
 * {@code (tableSpace, uuid, pageId, valueOffset)}. Used by
 * {@link RemoteFileDataStorageManager} when serving lazy (v2) data pages so
 * that value bytes fetched via byte-range reads against the remote file
 * service can be reused across multiple record lookups within the lifetime
 * of a query — or across queries that hit the same record repeatedly —
 * without re-fetching from remote storage every time.
 *
 * <h3>Off-heap storage (issue #411)</h3>
 * Values live in direct memory allocated from
 * {@link HerdDBByteBufAllocators#dataPagesAllocator()} so that a large
 * cache does not pin tens of GiB of {@code byte[]} on the JVM heap. The
 * cache is weight-bounded by the underlying {@link ByteBuf#capacity()};
 * eviction synchronously {@link ByteBuf#release() releases} each evicted
 * buffer back to the pool.
 *
 * <h3>Refcount discipline</h3>
 * Each cache entry owns exactly one refcount on its {@link ByteBuf}. Every
 * {@link #getOrFetch} call returns a {@link ByteBuf#retainedSlice()
 * retained slice} so the caller owns an independent refcount on the
 * underlying chunk. The cache's {@code removalListener} releases the
 * <em>cache's</em> refcount on eviction; outstanding caller slices keep
 * the chunk reachable until they are individually released. There is no
 * use-after-free risk because the chunk survives until the last refcount
 * is gone.
 *
 * <p>A {@code maxBytes} value of {@code 0} or negative disables the cache:
 * every call to {@link #getOrFetch(ValueKey, Supplier)} delegates to the
 * loader and returns a one-shot direct buffer that the caller must
 * release. This is useful for tests and for deployments that want
 * deterministic remote-read counts.
 *
 * <h3>Lifecycle</h3>
 * Call {@link #close()} on shutdown to drain the cache and release every
 * outstanding cache-owned refcount. Outstanding caller slices remain
 * valid until they are individually released.
 */
public final class LazyValueCache implements AutoCloseable {

    private final long maxBytes;
    private final Cache<ValueKey, ByteBuf> cache;
    private final PooledByteBufAllocator allocator;

    public LazyValueCache(long maxBytes) {
        this(maxBytes, HerdDBByteBufAllocators.dataPagesAllocator());
    }

    /**
     * Test-only constructor that lets a test inject a different allocator
     * (typically {@link io.netty.buffer.UnpooledByteBufAllocator#DEFAULT}
     * to keep direct-memory accounting deterministic across tests).
     */
    LazyValueCache(long maxBytes, PooledByteBufAllocator allocator) {
        this.maxBytes = maxBytes;
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        if (maxBytes <= 0L) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((ValueKey k, ByteBuf v) -> v == null ? 0 : v.capacity())
                    .removalListener((ValueKey k, ByteBuf v, RemovalCause cause) -> {
                        if (v != null && v.refCnt() > 0) {
                            // Release the cache's refcount. Outstanding caller
                            // slices (each owning their own refcount on the
                            // underlying chunk) keep the bytes reachable until
                            // they are individually released — no use-after-free.
                            v.release();
                        }
                    })
                    .recordStats()
                    .build();
        }
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public boolean isEnabled() {
        return cache != null;
    }

    /**
     * Returns a {@link ByteBuf#retainedSlice() retained slice} of the cached
     * value for {@code key}, or invokes {@code loader} atomically (at most
     * once per concurrent miss, as guaranteed by Caffeine's
     * {@code Cache.get(K, Function)} contract) to populate it. The returned
     * slice is direct-memory and the caller owns one refcount — call
     * {@link ByteBuf#release()} when done.
     *
     * <p>If the cache is disabled ({@link #isEnabled()} returns
     * {@code false}) this method allocates a fresh one-shot direct buffer,
     * fills it from the loader, and returns it. The caller still owns the
     * one refcount and must release it.
     */
    public ByteBuf getOrFetch(ValueKey key, Supplier<byte[]> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        if (cache == null) {
            byte[] data = loader.get();
            return wrapAsDirect(data);
        }
        ByteBuf cached = cache.get(key, k -> {
            byte[] data = loader.get();
            return wrapAsDirect(data);
        });
        // Caller owns one independent refcount via retainedSlice — the cache
        // keeps its own refcount alive until the entry is evicted.
        //
        // Race: Caffeine's TinyLFU admission test can evict the just-loaded
        // entry before this method returns. The removalListener releases the
        // cache's refcount; if it ran before we retain we observe refcnt=0
        // and retainedSlice throws IllegalReferenceCountException. Fall back
        // to a one-shot direct buffer (not cached) so the caller always gets
        // a usable slice and we do not corrupt the eviction-budget invariant.
        try {
            return cached.retainedSlice(0, cached.capacity());
        } catch (io.netty.util.IllegalReferenceCountException raceLost) {
            byte[] data = loader.get();
            return wrapAsDirect(data);
        }
    }

    /**
     * Allocates a direct {@link ByteBuf} of the appropriate capacity and
     * copies {@code data} into it. The returned buffer's refcount is 1
     * (owned by the caller). On any failure between allocation and copy
     * the buffer is released so we do not leak a pooled chunk.
     */
    private ByteBuf wrapAsDirect(byte[] data) {
        if (data == null) {
            // Match the legacy contract: a loader returning null is a
            // programming error upstream. Surface it eagerly so the cache
            // does not store a null entry.
            throw new NullPointerException("loader returned null");
        }
        ByteBuf buf = allocator.directBuffer(data.length, data.length);
        try {
            if (data.length > 0) {
                buf.writeBytes(data);
            }
            return buf;
        } catch (RuntimeException t) {
            // Narrow catch (per CLAUDE.md): writeBytes can throw IOOBE on
            // a corrupted state and the surrounding allocation paths can
            // throw OutOfMemoryError (which is a Throwable subclass not
            // caught here intentionally — the caller upstream is allowed
            // to propagate it). Either way the partially-initialised
            // direct buffer must be released to avoid leaking a pooled chunk.
            buf.release();
            throw t;
        }
    }

    /**
     * Drops every cached value that belongs to the given page. Called when a
     * page is overwritten or deleted so that stale bytes never leak to a
     * subsequent read. Each evicted buffer is released by the cache's
     * {@code removalListener}; outstanding caller slices remain valid until
     * their own refcounts are released.
     */
    public void invalidateForPage(String tableSpace, String uuid, long pageId) {
        if (cache == null) {
            return;
        }
        cache.asMap().keySet().removeIf(k ->
                k.pageId == pageId
                        && k.uuid.equals(uuid)
                        && k.tableSpace.equals(tableSpace));
    }

    /** Drops every cached value for a whole table or tablespace prefix. */
    public void invalidateForTable(String tableSpace, String uuid) {
        if (cache == null) {
            return;
        }
        cache.asMap().keySet().removeIf(k ->
                k.uuid.equals(uuid) && k.tableSpace.equals(tableSpace));
    }

    /** Drops every cached value under a tablespace. */
    public void invalidateForTablespace(String tableSpace) {
        if (cache == null) {
            return;
        }
        cache.asMap().keySet().removeIf(k -> k.tableSpace.equals(tableSpace));
    }

    /** Approximate number of cached entries. Intended for gauges. */
    public long estimatedSize() {
        return cache == null ? 0L : cache.estimatedSize();
    }

    /** Package-private: force Caffeine maintenance. Used by tests. */
    void cleanUp() {
        if (cache != null) {
            cache.cleanUp();
        }
    }

    /** Caffeine stats snapshot, or {@code null} if the cache is disabled. */
    public CacheStats stats() {
        return cache == null ? null : cache.stats();
    }

    /**
     * Drains every cache entry, releasing each cache-owned refcount on the
     * direct buffers. Outstanding caller slices remain valid until their
     * own refcounts are released. Idempotent.
     *
     * <p>Implements {@link AutoCloseable} so a try-with-resources or an
     * explicit shutdown hook can ensure all pooled direct memory returns
     * to the allocator.
     */
    @Override
    public void close() {
        if (cache == null) {
            return;
        }
        cache.invalidateAll();
        // Force any pending eviction work to run synchronously so the
        // removalListener releases every cache-owned refcount before the
        // caller observes close() has returned.
        cache.cleanUp();
    }

    /**
     * Composite cache key. Equality is field-wise; {@code tableSpace} and
     * {@code uuid} are interned-friendly short strings in practice.
     */
    public static final class ValueKey {
        final String tableSpace;
        final String uuid;
        final long pageId;
        final long valueOffset;

        public ValueKey(String tableSpace, String uuid, long pageId, long valueOffset) {
            this.tableSpace = Objects.requireNonNull(tableSpace, "tableSpace");
            this.uuid = Objects.requireNonNull(uuid, "uuid");
            this.pageId = pageId;
            this.valueOffset = valueOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ValueKey)) {
                return false;
            }
            ValueKey other = (ValueKey) o;
            return pageId == other.pageId
                    && valueOffset == other.valueOffset
                    && tableSpace.equals(other.tableSpace)
                    && uuid.equals(other.uuid);
        }

        @Override
        public int hashCode() {
            int h = tableSpace.hashCode();
            h = 31 * h + uuid.hashCode();
            h = 31 * h + Long.hashCode(pageId);
            h = 31 * h + Long.hashCode(valueOffset);
            return h;
        }

        @Override
        public String toString() {
            return "ValueKey{" + tableSpace + "/" + uuid + "#" + pageId + "@" + valueOffset + "}";
        }
    }
}
