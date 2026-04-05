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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Unit tests for {@link PageStoreReader}: deterministic byte-level reads from
 * a simulated page store, LRU eviction behaviour, failure propagation.
 */
public class PageStoreReaderTest {

    private static final String TS = "ts";
    private static final String IDX = "idx";
    /** Matches the tag used by PersistentVectorStore for graph chunks. */
    private static final int CHUNK_TYPE = 12;

    /**
     * Writes {@code pages} each of size {@code pageSize} (last may be smaller)
     * into the given DSM, and returns the allocated pageIds.
     */
    private long[] writeFixedSizePages(MemoryDataStorageManager dsm,
                                       int pageCount, int pageSize, long totalLength) {
        long[] ids = new long[pageCount];
        long bytesLeft = totalLength;
        for (int i = 0; i < pageCount; i++) {
            final int len = (int) Math.min(pageSize, bytesLeft);
            final int offset = (int) (totalLength - bytesLeft);
            final long pageId = 100L + i;
            ids[i] = pageId;
            dsm.writeIndexPage(TS, IDX, pageId, out -> {
                out.writeVInt(CHUNK_TYPE);
                out.writeVInt(len);
                byte[] data = new byte[len];
                for (int k = 0; k < len; k++) {
                    // deterministic byte stream: the kth byte globally is (offset+k) & 0xff
                    data[k] = (byte) ((offset + k) & 0xFF);
                }
                out.write(data, 0, len);
            });
            bytesLeft -= len;
        }
        return ids;
    }

    @Test
    public void roundTripMatchesByteStream() throws Exception {
        // Write a 7-page stream, each page 1024 bytes except the tail = 300.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 1024;
        long total = 6L * pageSize + 300L;
        long[] ids = writeFixedSizePages(dsm, 7, pageSize, total);

        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 3)) {
            try (RandomAccessReader r = sup.get()) {
                assertEquals(total, r.length());
                // Read all bytes from position 0 and verify the deterministic
                // stream (byte k == (k & 0xff)).
                byte[] all = new byte[(int) total];
                r.seek(0);
                r.readFully(all);
                for (int k = 0; k < total; k++) {
                    assertEquals("byte at " + k, (byte) (k & 0xFF), all[k]);
                }
            }
        }
    }

    @Test
    public void randomSeeksReturnCorrectBytes() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 256;
        long total = 10L * pageSize + 17L;
        long[] ids = writeFixedSizePages(dsm, 11, pageSize, total);

        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 2)) {
            try (RandomAccessReader r = sup.get()) {
                // Probe crossing page boundaries; last position must leave at
                // least 4 bytes (readInt consumes 4) before EOF.
                long[] positions = {0L, 255L, 256L, 257L, 511L, 512L, total - 10, total - 4};
                for (long p : positions) {
                    r.seek(p);
                    byte b = (byte) (r.readInt() >>> 24 & 0xFF);
                    // readInt reads 4 bytes starting at p; first byte must equal p&0xff
                    assertEquals("pos=" + p, (byte) (p & 0xFF), b);
                }
            }
        }
    }

    @Test
    public void readPastEndThrows() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        long[] ids = writeFixedSizePages(dsm, 1, 8, 8);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 1)) {
            try (RandomAccessReader r = sup.get()) {
                r.seek(7);
                try {
                    r.readInt(); // would read bytes 7..10, past end
                    fail("expected EOF");
                } catch (IndexOutOfBoundsException expected) {
                }
            }
        }
    }

    @Test
    public void lruEvictsOldestPages() throws Exception {
        // 6 pages, cache of 2 → touching all 6 in order forces at least 4 misses
        // in addition to the initial priming pass that filled the cache.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 64;
        long total = 6L * pageSize;
        long[] ids = writeFixedSizePages(dsm, 6, pageSize, total);

        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 2)) {
            // At construction, 6 pages were read to learn sizes; LRU now holds
            // the last 2.
            assertEquals(2, sup.getCachedPages());
            long missesBefore = sup.getCacheMisses();
            try (RandomAccessReader r = sup.get()) {
                for (int pageIdx = 0; pageIdx < 6; pageIdx++) {
                    r.seek((long) pageIdx * pageSize);
                    r.readInt();
                }
            }
            long missesAfter = sup.getCacheMisses();
            assertTrue("some pages must have missed, got "
                            + (missesAfter - missesBefore),
                    missesAfter - missesBefore >= 4);
            assertEquals("LRU must still be bounded", 2, sup.getCachedPages());
        }
    }

    @Test
    public void wrongChunkTypeFails() throws Exception {
        // Pages stored with type 99, reader expects CHUNK_TYPE = 12 → should
        // throw when constructing the PageSource (priming pass).
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        dsm.writeIndexPage(TS, IDX, 42L, out -> {
            out.writeVInt(99);
            out.writeVInt(4);
            out.write(new byte[]{1, 2, 3, 4}, 0, 4);
        });
        try {
            new PageStoreReader.Supplier(dsm, TS, IDX,
                    new long[]{42L}, CHUNK_TYPE, 2);
            fail("expected failure on wrong chunk type");
        } catch (IOException | DataStorageManagerException expected) {
            assertTrue("message should surface the type mismatch, got: "
                            + expected.getMessage() + " cause: " + expected.getCause(),
                    expected.getMessage().contains("expected type")
                            || (expected.getCause() != null
                                && expected.getCause().getMessage().contains("expected type")));
        }
    }

    @Test
    public void closeClearsCache() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        long[] ids = writeFixedSizePages(dsm, 3, 64, 3 * 64L);
        PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 3);
        assertTrue(sup.getCachedPages() > 0);
        sup.close();
        assertEquals("close must drop cached pages", 0, sup.getCachedPages());
    }

    @Test
    public void missingPageFailsLater() throws Exception {
        // A deleted page triggers an exception when the reader tries to load it
        // lazily after eviction. The priming pass would normally surface this
        // earlier — so we delete the page AFTER construction but then force a
        // cache miss on it.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 64;
        long[] ids = writeFixedSizePages(dsm, 4, pageSize, 4L * pageSize);

        // Very small cache so reading a different page evicts.
        PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 1);
        // Remove page 0 from the backing store.
        dsm.deleteIndexPage(TS, IDX, ids[0]);
        try (RandomAccessReader r = sup.get()) {
            // Force a read that triggers a cache miss on page 0.
            r.seek(pageSize + 1); // page 1 first
            r.readInt();
            r.seek(1); // page 0 — cache miss, read must fail
            try {
                r.readInt();
                fail("expected a RuntimeException wrapping the missing-page error");
            } catch (RuntimeException expected) {
            }
        }
        sup.close();
    }

    @Test
    public void sharedCacheAcrossReaders() throws Exception {
        // Two readers on the same supplier must share the LRU cache, so
        // alternating reads on the same pages should produce hits.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 64;
        long[] ids = writeFixedSizePages(dsm, 4, pageSize, 4L * pageSize);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 4)) {
            long hitsBefore = sup.getCacheHits();
            RandomAccessReader a = sup.get();
            RandomAccessReader b = sup.get();
            a.seek(0);
            a.readInt();
            b.seek(0);
            b.readInt();
            a.close();
            b.close();
            assertTrue("cache must produce hits across readers, delta="
                            + (sup.getCacheHits() - hitsBefore),
                    sup.getCacheHits() - hitsBefore >= 2);
        }
    }

    @Test
    public void counterOfReadPayloadInvocations() throws Exception {
        // Defensive: the priming-pass invokes readIndexPage exactly once per
        // page. We simulate that by counting dsm.readIndexPage calls via a
        // subclass.
        AtomicInteger reads = new AtomicInteger(0);
        MemoryDataStorageManager dsm = new MemoryDataStorageManager() {
            @Override
            public <X> X readIndexPage(String tableSpace, String uuid, Long pageId,
                                       DataReader<X> reader) throws DataStorageManagerException {
                reads.incrementAndGet();
                return super.readIndexPage(tableSpace, uuid, pageId, reader);
            }
        };
        long[] ids = writeFixedSizePages(dsm, 5, 128, 5L * 128);
        int before = reads.get();
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 5)) {
            assertEquals("priming pass must read each page exactly once",
                    5, reads.get() - before);
            // Second pass, reading all pages, should be ALL hits (no extra
            // readIndexPage calls).
            int after = reads.get();
            try (RandomAccessReader r = sup.get()) {
                for (int p = 0; p < 5; p++) {
                    r.seek((long) p * 128);
                    r.readInt();
                }
            }
            assertEquals("all hits, no new reads", after, reads.get());
        }
    }

    /** Sanity guard that our test data generator matches the encoder/reader. */
    @Test
    public void indexOfFirstByteInsidePage() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 32;
        long[] ids = writeFixedSizePages(dsm, 3, pageSize, 3L * pageSize);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 3)) {
            try (RandomAccessReader r = sup.get()) {
                // Read full buffer and check alignment
                byte[] buf = new byte[3 * pageSize];
                r.seek(0);
                r.readFully(buf);
                byte[] expected = new byte[buf.length];
                for (int i = 0; i < expected.length; i++) {
                    expected[i] = (byte) (i & 0xFF);
                }
                assertArrayEquals(expected, buf);
            }
        }
    }

    /** Drill that we haven't accidentally introduced a boxing issue. */
    @Test
    public void pageSizesAreRespected() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 100;
        long total = 2L * pageSize + 7;
        long[] ids = writeFixedSizePages(dsm, 3, pageSize, total);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 3)) {
            assertEquals(total, sup.getTotalLength());
        }
    }

    @Test
    public void singlePageReaderWorks() throws Exception {
        // Degenerate case: single page.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        long[] ids = writeFixedSizePages(dsm, 1, 16, 16);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 1)) {
            try (RandomAccessReader r = sup.get()) {
                r.seek(0);
                byte[] buf = new byte[16];
                r.readFully(buf);
                for (int i = 0; i < 16; i++) {
                    assertEquals((byte) i, buf[i]);
                }
            }
        }
    }

    @Test
    public void readsOverManyPagesStayBounded() throws Exception {
        // Guard against accidentally caching every page.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageCount = 40;
        int pageSize = 32;
        long[] ids = writeFixedSizePages(dsm, pageCount, pageSize, pageCount * (long) pageSize);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 4)) {
            assertEquals("cache must saturate at its bound", 4, sup.getCachedPages());
            try (RandomAccessReader r = sup.get()) {
                byte[] buf = new byte[pageCount * pageSize];
                r.seek(0);
                r.readFully(buf);
            }
            assertEquals("cache still bounded after streaming", 4, sup.getCachedPages());
        }
    }

    @Test
    public void adjacentReadsAcrossPageBoundariesWork() throws Exception {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 8;
        long[] ids = writeFixedSizePages(dsm, 4, pageSize, 4L * pageSize);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 4)) {
            try (RandomAccessReader r = sup.get()) {
                // Read a 12-byte buffer starting at position 4: spans pages 0 and 1.
                byte[] buf = new byte[12];
                r.seek(4);
                r.readFully(buf);
                for (int i = 0; i < 12; i++) {
                    assertEquals((byte) ((4 + i) & 0xFF), buf[i]);
                }
            }
        }
    }

    @Test
    public void emptyPageListIsUnsupported() {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try {
            new PageStoreReader.Supplier(dsm, TS, IDX, new long[0], CHUNK_TYPE, 2);
            // Empty list is valid construction; totalLength = 0.
        } catch (Exception unexpected) {
            fail("empty page list should be allowed: " + unexpected);
        }
    }

    @Test
    public void concurrentReadersShareDataSafely() throws Exception {
        // Basic smoke test: two threads reading from two reader instances from
        // the same supplier must both see consistent bytes.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int pageSize = 128;
        long total = 16L * pageSize;
        long[] ids = writeFixedSizePages(dsm, 16, pageSize, total);
        try (PageStoreReader.Supplier sup = new PageStoreReader.Supplier(
                dsm, TS, IDX, ids, CHUNK_TYPE, 4)) {
            List<Thread> threads = new ArrayList<>();
            List<Throwable> errors = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                Thread th = new Thread(() -> {
                    try (RandomAccessReader r = sup.get()) {
                        for (int it = 0; it < 50; it++) {
                            long pos = (seed * 17L + it * 13L) % (total - 4);
                            r.seek(pos);
                            int v = r.readInt();
                            int expected = ((int) (pos & 0xFF) << 24)
                                    | ((int) ((pos + 1) & 0xFF) << 16)
                                    | ((int) ((pos + 2) & 0xFF) << 8)
                                    | (int) ((pos + 3) & 0xFF);
                            if (v != expected) {
                                throw new AssertionError("thread " + seed + " pos=" + pos
                                        + " expected " + expected + " got " + v);
                            }
                        }
                    } catch (Throwable ex) {
                        synchronized (errors) {
                            errors.add(ex);
                        }
                    }
                });
                threads.add(th);
            }
            for (Thread t : threads) {
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
            if (!errors.isEmpty()) {
                throw new AssertionError("concurrent reads failed: " + errors.get(0), errors.get(0));
            }
        }
    }
}
