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

package herddb.index.vector;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import herddb.storage.DataStorageManagerException;
import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * Global page cache for vector index segments. All segments of a {@link PersistentVectorStore}
 * share a single Caffeine-backed {@link LoadingCache} bounded by total bytes.
 *
 * <p>The cache owns the fetch-on-miss path: callers invoke {@link #load(PageKey)} and
 * Caffeine either serves the cached page or invokes the supplied {@link PageLoader}
 * exactly once per concurrent miss (single-flight). This replaces the older
 * {@code get}/{@code put} API whose callers had to deduplicate themselves.
 *
 * <p>Key = {@link PageKey} (tableSpaceUUID + indexUUID + pageId). Value = byte[] page
 * payload. The cache uses a byte-weight function so entries are weighed by their
 * {@code byte[]} length and the total cached-byte budget is honoured regardless of
 * individual page sizes.
 *
 * <p>Default budget = 1/4 of JVM heap; the actual value is wired in by
 * {@link PersistentVectorStore} via the {@code herddb.vectorindex.segmentPageCacheMaxBytes}
 * system property.
 */
public class SharedSegmentPageCache {

    /** Default cache budget: 1/4 of the JVM max heap. */
    public static final long DEFAULT_MAX_BYTES = Runtime.getRuntime().maxMemory() / 4;

    /**
     * Loads a page by its {@link PageKey}. Called by Caffeine on miss.
     * Implementations are responsible for turning storage errors into
     * {@link DataStorageManagerException}.
     */
    @FunctionalInterface
    public interface PageLoader {
        byte[] load(PageKey key) throws DataStorageManagerException;
    }

    /**
     * Immutable identifier for a cached page. Three fields are enough to disambiguate
     * across every active index in the JVM: {@code pageId} is unique per DSM, and
     * {@code tableSpaceUUID + indexUUID} narrows the lookup to the right DSM call.
     * Page type is intentionally not part of the key — only one type
     * ({@code TYPE_VECTOR_GRAPHCHUNK}) is cached here today, and adding a field per
     * entry just to carry a constant would waste heap for every cached page.
     */
    public static final class PageKey {
        private final String tableSpaceUUID;
        private final String indexUUID;
        private final long pageId;
        private final int hash;

        public PageKey(String tableSpaceUUID, String indexUUID, long pageId) {
            this.tableSpaceUUID = Objects.requireNonNull(tableSpaceUUID, "tableSpaceUUID");
            this.indexUUID = Objects.requireNonNull(indexUUID, "indexUUID");
            this.pageId = pageId;
            this.hash = Objects.hash(tableSpaceUUID, indexUUID, pageId);
        }

        public String tableSpaceUUID() {
            return tableSpaceUUID;
        }

        public String indexUUID() {
            return indexUUID;
        }

        public long pageId() {
            return pageId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PageKey)) {
                return false;
            }
            PageKey k = (PageKey) o;
            // Fast path: both fields are precomputed in the constructor, so a
            // hash mismatch is a certain non-equal without touching the strings.
            if (hash != k.hash) {
                return false;
            }
            return pageId == k.pageId
                    && tableSpaceUUID.equals(k.tableSpaceUUID)
                    && indexUUID.equals(k.indexUUID);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "PageKey{ts=" + tableSpaceUUID + ", idx=" + indexUUID + ", page=" + pageId + "}";
        }
    }

    private final LoadingCache<PageKey, byte[]> cache;
    private final PageLoader loader;
    private final long maxBytes;

    /**
     * Creates a new shared page cache with the given byte budget and loader.
     *
     * @param maxBytes maximum total bytes to cache. If {@code <= 0}, no caching is done —
     *        every {@link #load(PageKey)} call delegates straight to the loader.
     * @param loader fetches a page on miss. Never {@code null}.
     */
    public SharedSegmentPageCache(long maxBytes, PageLoader loader) {
        this.maxBytes = maxBytes;
        this.loader = Objects.requireNonNull(loader, "loader");
        if (maxBytes <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((PageKey k, byte[] v) -> v == null ? 0 : v.length)
                    .recordStats()
                    .build(loader::load);
        }
    }

    /**
     * Returns the page for {@code key}, loading it through the configured
     * {@link PageLoader} if not already cached. Concurrent misses on the same key
     * are deduplicated — the loader is invoked exactly once.
     *
     * @throws DataStorageManagerException if the loader fails
     */
    public byte[] load(PageKey key) throws DataStorageManagerException {
        Objects.requireNonNull(key, "key");
        if (cache == null) {
            byte[] v = loader.load(key);
            if (v == null) {
                throw new DataStorageManagerException("loader returned null for " + key);
            }
            return v;
        }
        try {
            byte[] v = cache.get(key);
            if (v == null) {
                // Caffeine returns null only if the loader returned null — surface it.
                throw new DataStorageManagerException("loader returned null for " + key);
            }
            return v;
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
            if (cause instanceof DataStorageManagerException) {
                throw (DataStorageManagerException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new DataStorageManagerException(cause);
        }
    }

    /**
     * Invalidates (removes) a page from the cache. Safe to call with an unknown key.
     */
    public void invalidate(PageKey key) {
        if (cache != null && key != null) {
            cache.invalidate(key);
        }
    }

    /**
     * Invalidates all entries in the cache.
     */
    public void clear() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    /**
     * Forces any pending maintenance (eviction, reference-expiration) to run on
     * the caller's thread. Useful in tests that assert on {@link #evictionCount()}
     * immediately after an insertion burst, and for operators that want the
     * exposed gauges to reflect in-flight evictions without waiting for the
     * background maintenance pass.
     */
    public void cleanUp() {
        if (cache != null) {
            cache.cleanUp();
        }
    }

    /**
     * Returns the approximate number of entries currently in the cache.
     */
    public long size() {
        if (cache == null) {
            return 0;
        }
        return cache.estimatedSize();
    }

    /**
     * Checks if the cache is active (non-null).
     */
    public boolean isActive() {
        return cache != null;
    }

    // ---------------------------------------------------------------------
    // Stats accessors — backed by Caffeine's recordStats().
    // Return zero when the cache is disabled so callers don't need branches.
    // ---------------------------------------------------------------------

    public long hitCount() {
        return cache == null ? 0 : cache.stats().hitCount();
    }

    public long missCount() {
        return cache == null ? 0 : cache.stats().missCount();
    }

    public long evictionCount() {
        return cache == null ? 0 : cache.stats().evictionCount();
    }

    public long loadSuccessCount() {
        return cache == null ? 0 : cache.stats().loadSuccessCount();
    }

    public long loadFailureCount() {
        return cache == null ? 0 : cache.stats().loadFailureCount();
    }

    public long totalLoadTimeNanos() {
        return cache == null ? 0 : cache.stats().totalLoadTime();
    }

    public long estimatedSize() {
        return cache == null ? 0 : cache.estimatedSize();
    }

    /** Approximate total bytes currently held in the cache. */
    public long weightedSize() {
        if (cache == null) {
            return 0;
        }
        return cache.policy().eviction()
                .map(e -> e.weightedSize().orElse(0L))
                .orElse(0L);
    }

    /** Configured byte budget. Returns 0 when the cache is disabled. */
    public long maxBytes() {
        return cache == null ? 0 : maxBytes;
    }

    /** Returns a snapshot of the underlying Caffeine stats (or a zero snapshot if disabled). */
    public CacheStats stats() {
        return cache == null ? CacheStats.empty() : cache.stats();
    }
}
