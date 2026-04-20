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
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Bounded in-heap cache of per-record value byte arrays, keyed by
 * {@code (tableSpace, uuid, pageId, valueOffset)}. Used by
 * {@link RemoteFileDataStorageManager} when serving lazy (v2) data pages so
 * that value bytes fetched via byte-range reads against the remote file
 * service can be reused across multiple record lookups within the lifetime
 * of a query — or across queries that hit the same record repeatedly —
 * without re-fetching from remote storage every time.
 *
 * <p>The cache is weight-bounded by total value bytes; the weight of a
 * single entry is its value length. Weight-based eviction is strictly
 * independent of the page-replacement policy used by {@code TableManager}:
 * a lazy {@code DataPage} may still be resident in the page map even after
 * all of its values have been evicted from this cache (a subsequent
 * {@code get(key)} on the page just re-issues a range read).
 *
 * <p>A {@code maxBytes} value of {@code 0} or negative disables the cache:
 * every call to {@link #getOrFetch(ValueKey, Supplier)} delegates to the
 * loader. This is useful for tests and for deployments that want
 * deterministic remote-read counts.
 */
public final class LazyValueCache {

    private final long maxBytes;
    private final Cache<ValueKey, byte[]> cache;

    public LazyValueCache(long maxBytes) {
        this.maxBytes = maxBytes;
        if (maxBytes <= 0L) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((ValueKey k, byte[] v) -> v == null ? 0 : v.length)
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
     * Returns the cached value for {@code key}, or invokes {@code loader}
     * atomically (at most once per concurrent miss, as guaranteed by
     * Caffeine's {@code Cache.get(K, Function)} contract) to populate it.
     */
    public byte[] getOrFetch(ValueKey key, Supplier<byte[]> loader) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        if (cache == null) {
            return loader.get();
        }
        return cache.get(key, k -> loader.get());
    }

    /**
     * Drops every cached value that belongs to the given page. Called when a
     * page is overwritten or deleted so that stale bytes never leak to a
     * subsequent read.
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

    /** Caffeine stats snapshot, or {@code null} if the cache is disabled. */
    public CacheStats stats() {
        return cache == null ? null : cache.stats();
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
