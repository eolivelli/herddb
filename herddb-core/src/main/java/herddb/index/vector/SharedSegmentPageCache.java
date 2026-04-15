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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Global page cache for vector index segments. All segments of a {@link PersistentVectorStore}
 * share a single Caffeine-backed LRU cache bounded by total bytes.
 *
 * <p>Key = long pageId (globally unique per DataStorageManager).
 * Value = byte[] page payload (cached graph page data).
 *
 * <p>The cache uses a byte-weight function: each entry's weight is its byte[] length.
 * When the cache exceeds the byte budget, the least-recently-used entries are evicted.
 *
 * <p>Default budget = 1/4 of JVM heap; configurable via
 * {@code herddb.vectorindex.segmentPageCacheMaxBytes} system property.
 */
public class SharedSegmentPageCache {

    /** Default cache budget: 1/4 of the JVM max heap. */
    public static final long DEFAULT_MAX_BYTES = Runtime.getRuntime().maxMemory() / 4;

    private final Cache<Long, byte[]> cache;

    /**
     * Creates a new shared page cache with the given byte budget.
     *
     * @param maxBytes maximum total bytes to cache. If <= 0, behaves as a no-op cache (always miss).
     */
    public SharedSegmentPageCache(long maxBytes) {
        if (maxBytes <= 0) {
            this.cache = null;
        } else {
            this.cache = Caffeine.newBuilder()
                    .maximumWeight(maxBytes)
                    .weigher((Long pageId, byte[] data) -> {
                        long len = data == null ? 0L : data.length;
                        return len > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) len;
                    })
                    .recordStats()
                    .build();
        }
    }

    /**
     * Retrieves a page from the cache, or null if not present (miss).
     */
    public byte[] get(long pageId) {
        if (cache == null) {
            return null;
        }
        return cache.getIfPresent(pageId);
    }

    /**
     * Stores a page in the cache. May trigger eviction of LRU entries.
     */
    public void put(long pageId, byte[] data) {
        if (cache != null) {
            cache.put(pageId, data);
        }
    }

    /**
     * Invalidates (removes) a page from the cache.
     */
    public void invalidate(long pageId) {
        if (cache != null) {
            cache.invalidate(pageId);
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
     * Returns the number of entries currently in the cache.
     */
    public long size() {
        if (cache == null) {
            return 0;
        }
        return cache.asMap().size();
    }

    /**
     * Checks if the cache is active (non-null).
     */
    public boolean isActive() {
        return cache != null;
    }
}
