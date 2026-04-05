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

import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link RandomAccessReader} that reads a logical byte stream assembled from
 * index pages stored in a {@link DataStorageManager}. This is the non-mmap
 * alternative to {@link SegmentedMappedReader} — the graph data is <em>not</em>
 * kept as a resident temp file on disk, and the memory footprint is explicitly
 * bounded by a small LRU page cache.
 *
 * <p>Rationale: mmapped regions are not counted against the process heap, so
 * holding an mmap reader per loaded vector-store segment can silently explode
 * resident memory at scale. This reader uses on-demand page fetches plus a
 * bounded LRU of decoded page bytes, so the cost is both observable and
 * configurable.
 *
 * <p>Thread-safety: each {@code PageStoreReader} instance has its own mutable
 * position and is <em>not</em> safe for concurrent use. Multiple readers
 * obtained from the same {@link Supplier} share a single cache that is
 * internally synchronised.
 */
final class PageStoreReader implements RandomAccessReader {

    /** Default LRU cache size (number of pages). */
    static final int DEFAULT_LRU_PAGES =
            Math.max(2, Integer.getInteger("herddb.vectorindex.graphReaderLruPages", 8));

    private final PageSource source;
    private long position;

    private PageStoreReader(PageSource source) {
        this.source = source;
    }

    @Override
    public void seek(long pos) {
        this.position = pos;
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public int readInt() {
        int b0 = readByte() & 0xFF;
        int b1 = readByte() & 0xFF;
        int b2 = readByte() & 0xFF;
        int b3 = readByte() & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public long readLong() {
        long hi = readInt() & 0xFFFFFFFFL;
        long lo = readInt() & 0xFFFFFFFFL;
        return (hi << 32) | lo;
    }

    private byte readByte() {
        byte[] one = new byte[1];
        readBytesInto(one, 0, 1);
        return one[0];
    }

    private void readBytesInto(byte[] dst, int dstOff, int toRead) {
        if (position + toRead > source.totalLength()) {
            throw new IndexOutOfBoundsException(
                    "read past end: position=" + position + " toRead=" + toRead
                            + " length=" + source.totalLength());
        }
        int remaining = toRead;
        while (remaining > 0) {
            int pageIndex = source.pageIndexFor(position);
            int offsetInPage = (int) (position - source.pageStart(pageIndex));
            int pageSize = source.pageSize(pageIndex);
            int avail = pageSize - offsetInPage;
            if (avail <= 0) {
                // This can happen only if position exactly equals the start of
                // a would-be next page that doesn't exist: bounded above by
                // the early EOF check, but keep a defensive guard.
                throw new IndexOutOfBoundsException(
                        "no bytes available at position=" + position
                                + " pageIndex=" + pageIndex);
            }
            int n = Math.min(remaining, avail);
            byte[] page = source.loadPage(pageIndex);
            System.arraycopy(page, offsetInPage, dst, dstOff, n);
            position += n;
            dstOff += n;
            remaining -= n;
        }
    }

    @Override
    public void readFully(byte[] buf) {
        readBytesInto(buf, 0, buf.length);
    }

    @Override
    public void readFully(ByteBuffer dest) {
        int remaining = dest.remaining();
        while (remaining > 0) {
            int pageIndex = source.pageIndexFor(position);
            int offsetInPage = (int) (position - source.pageStart(pageIndex));
            int pageSize = source.pageSize(pageIndex);
            int avail = pageSize - offsetInPage;
            int n = Math.min(remaining, avail);
            byte[] page = source.loadPage(pageIndex);
            dest.put(page, offsetInPage, n);
            position += n;
            remaining -= n;
        }
    }

    @Override
    public void readFully(float[] dst) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = readFloat();
        }
    }

    @Override
    public void readFully(long[] dst) {
        for (int i = 0; i < dst.length; i++) {
            dst[i] = readLong();
        }
    }

    @Override
    public void read(int[] dst, int offset, int count) {
        for (int i = 0; i < count; i++) {
            dst[offset + i] = readInt();
        }
    }

    @Override
    public void read(float[] dst, int offset, int count) {
        for (int i = 0; i < count; i++) {
            dst[offset + i] = readFloat();
        }
    }

    @Override
    public long length() {
        return source.totalLength();
    }

    @Override
    public void close() {
        // Readers share the page cache via PageSource; nothing per-instance.
    }

    /**
     * Shared state used by all readers for one logical file. Holds the
     * page-id list, the prefix-sum table of page sizes, and a bounded LRU
     * cache of decoded page bytes.
     */
    static final class PageSource {

        final DataStorageManager dsm;
        final String tableSpace;
        final String indexName;
        final int expectedChunkType;
        final long[] pageIds;
        final int[] pageSizes;
        /** prefixSum[i] = sum of pageSizes[0..i-1]; prefixSum[length] = total length. */
        final long[] prefixSum;
        final long totalLength;
        private final int maxCachedPages;
        private final LinkedHashMap<Integer, byte[]> cache;
        /** Observability: number of page loads that hit the cache. */
        volatile long cacheHits;
        /** Observability: number of page loads that missed and read from the page store. */
        volatile long cacheMisses;

        /**
         * Builds the PageSource by reading each page once to learn its length.
         * This is a single linear pass over {@code pageIds}; after it returns
         * the cache may be warmed with up to {@code maxCachedPages} entries.
         */
        PageSource(DataStorageManager dsm, String tableSpace, String indexName,
                   long[] pageIds, int expectedChunkType, int maxCachedPages)
                throws IOException, DataStorageManagerException {
            this.dsm = dsm;
            this.tableSpace = tableSpace;
            this.indexName = indexName;
            this.pageIds = pageIds;
            this.expectedChunkType = expectedChunkType;
            this.pageSizes = new int[pageIds.length];
            this.prefixSum = new long[pageIds.length + 1];
            this.maxCachedPages = Math.max(1, maxCachedPages);
            this.cache = new LinkedHashMap<Integer, byte[]>(maxCachedPages + 1, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                    return size() > PageSource.this.maxCachedPages;
                }
            };

            // First pass: read every page once to discover its length. We keep
            // the last `maxCachedPages` in the cache as a warm start — no extra
            // reads required if the caller immediately iterates forward.
            for (int i = 0; i < pageIds.length; i++) {
                byte[] payload = readPayload(pageIds[i], expectedChunkType);
                pageSizes[i] = payload.length;
                prefixSum[i + 1] = prefixSum[i] + payload.length;
                synchronized (cache) {
                    cache.put(i, payload);
                }
            }
            this.totalLength = prefixSum[pageIds.length];
        }

        long totalLength() {
            return totalLength;
        }

        int pageSize(int pageIndex) {
            return pageSizes[pageIndex];
        }

        long pageStart(int pageIndex) {
            return prefixSum[pageIndex];
        }

        int pageIndexFor(long position) {
            // Binary search on prefixSum: find largest i such that prefixSum[i] <= position
            int lo = 0;
            int hi = pageIds.length - 1;
            while (lo < hi) {
                int mid = (lo + hi + 1) >>> 1;
                if (prefixSum[mid] <= position) {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            return lo;
        }

        byte[] loadPage(int pageIndex) {
            byte[] page;
            synchronized (cache) {
                page = cache.get(pageIndex);
                if (page != null) {
                    cacheHits++;
                    return page;
                }
            }
            try {
                page = readPayload(pageIds[pageIndex], expectedChunkType);
            } catch (IOException | DataStorageManagerException e) {
                throw new RuntimeException(
                        "failed to read page " + pageIds[pageIndex] + " for index " + indexName, e);
            }
            synchronized (cache) {
                cache.put(pageIndex, page);
                cacheMisses++;
            }
            return page;
        }

        private byte[] readPayload(long pageId, int expectedType)
                throws IOException, DataStorageManagerException {
            return dsm.readIndexPage(tableSpace, indexName, pageId,
                    in -> {
                        int type = in.readVInt();
                        if (type != expectedType) {
                            throw new IOException(
                                    "page " + pageId + ": expected type "
                                            + expectedType + " but got " + type);
                        }
                        int len = in.readVInt();
                        byte[] data = new byte[len];
                        in.readArray(len, data);
                        return data;
                    });
        }

        int cachedPages() {
            synchronized (cache) {
                return cache.size();
            }
        }
    }

    /**
     * A {@link ReaderSupplier} backed by a single shared {@link PageSource}.
     * All returned readers share the LRU page cache but each has its own
     * {@code position}, so concurrent search traffic on one segment does not
     * multiply the cached footprint.
     */
    static final class Supplier implements ReaderSupplier {

        private final PageSource source;

        Supplier(DataStorageManager dsm, String tableSpace, String indexName,
                 long[] pageIds, int expectedChunkType, int maxCachedPages)
                throws IOException, DataStorageManagerException {
            this.source = new PageSource(
                    dsm, tableSpace, indexName, pageIds, expectedChunkType, maxCachedPages);
        }

        @Override
        public RandomAccessReader get() {
            return new PageStoreReader(source);
        }

        @Override
        public void close() {
            // Drop cached pages to free memory promptly.
            synchronized (source.cache) {
                source.cache.clear();
            }
        }

        /** Visible for tests. */
        long getCacheHits() {
            return source.cacheHits;
        }

        /** Visible for tests. */
        long getCacheMisses() {
            return source.cacheMisses;
        }

        /** Visible for tests. */
        int getCachedPages() {
            return source.cachedPages();
        }

        /** Visible for tests. */
        long getTotalLength() {
            return source.totalLength;
        }
    }
}
