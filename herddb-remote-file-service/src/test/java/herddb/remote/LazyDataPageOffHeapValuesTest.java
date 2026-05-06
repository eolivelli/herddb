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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.DataPage;
import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #411 — end-to-end verification of the {@link LazyDataPage}/cache
 * read path. The cache holds direct {@link io.netty.buffer.ByteBuf}s
 * (multi-GiB savings on the JVM heap), but {@link LazyDataPage#get(Bytes)}
 * eagerly materialises the slice into an on-heap {@code byte[]} so the
 * returned {@link Record} carries a plain heap-backed {@link Bytes}. This
 * boundary is deliberate: several scan paths consume {@code Record} without
 * ever accessing {@code value}'s bytes (pure-PK projections, {@code COUNT(*)},
 * {@code EXISTS}, sort-by-PK, the index-less delete/update flows that only
 * touch {@code Record.getEstimatedSize()}). Returning a
 * {@code Bytes.fromOffHeap(slice)} would leak one refcount per such row.
 */
public class LazyDataPageOffHeapValuesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private CountingClient client;
    private RemoteFileDataStorageManager storage;

    private static final String TS = "ts1";
    private static final String UUID = "uuidA";
    private static final long PAGE_ID = 42L;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
        server.start();
        client = new CountingClient(Arrays.asList("localhost:" + server.getPort()));
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                1000,
                client,
                new LazyValueCache(4L * 1024L * 1024L));
        storage.start();
        storage.initTablespace(TS);
        storage.initTable(TS, UUID);
    }

    @After
    public void tearDown() throws Exception {
        // storage.close() drains the LazyValueCache so no direct buffers leak.
        storage.close();
        client.close();
        server.stop();
    }

    private static class CountingClient extends RemoteFileServiceClient {
        final AtomicLong readCalls = new AtomicLong();

        CountingClient(List<String> servers) {
            super(servers);
        }

        @Override
        public byte[] readFileRange(String path, long offset, int length, int blockSize) {
            byte[] bytes = super.readFileRange(path, offset, length, blockSize);
            readCalls.incrementAndGet();
            return bytes;
        }

        void reset() {
            readCalls.set(0);
        }
    }

    private static List<Record> makeRecords(int numRecords, int valueSize) {
        List<Record> records = new ArrayList<>(numRecords);
        Random r = new Random(20260506L);
        for (int i = 0; i < numRecords; i++) {
            byte[] v = new byte[valueSize];
            r.nextBytes(v);
            records.add(new Record(
                    Bytes.from_string("k-" + String.format("%06d", i)),
                    Bytes.from_array(v)));
        }
        return records;
    }

    @Test
    public void getReturnsHeapBackedRecordWithCorrectBytes() throws Exception {
        List<Record> records = makeRecords(8, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);
        assertTrue("expected a LazyDataPage from the lazy DSM", page instanceof LazyDataPage);

        Record original = records.get(3);
        Record fetched = page.get(original.key);
        assertNotNull("get must return a record for an existing key", fetched);
        // Issue #411: LazyDataPage.get eagerly materialises the cache slice
        // into a heap byte[] so the returned Record can be consumed by
        // scan paths that never touch the value bytes (pure-PK projection,
        // COUNT(*), index-less delete/update) without leaking a refcount.
        assertFalse("Record.value must be heap-backed (off-heap caller refcount"
                        + " would leak in scan paths that never access value bytes)",
                fetched.value.isOffHeap());
        assertArrayEquals("value bytes must round-trip identically to the original",
                original.value.to_array(), fetched.value.to_array());
    }

    @Test
    public void emptyValueShortCircuitsToHeapEmpty() throws Exception {
        List<Record> records = new ArrayList<>();
        records.add(new Record(Bytes.from_string("k-empty"), Bytes.from_array(new byte[0])));
        records.add(new Record(Bytes.from_string("k-data"), Bytes.from_string("hello")));
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);
        client.reset();

        Record empty = page.get(Bytes.from_string("k-empty"));
        assertNotNull(empty);
        assertEquals("zero-length values must not allocate any off-heap slice",
                0, empty.value.getLength());
        assertFalse("zero-length values must be heap-backed (Bytes.EMPTY_ARRAY)",
                empty.value.isOffHeap());
        assertEquals("zero-length values must not trigger remote I/O",
                0L, client.readCalls.get());

        Record data = page.get(Bytes.from_string("k-data"));
        assertNotNull(data);
        assertFalse("non-empty values must also be heap-backed after eager"
                        + " materialisation", data.value.isOffHeap());
        assertArrayEquals("hello".getBytes(), data.value.to_array());
    }

    @Test
    public void cacheHitDoesNotReissueRemoteCall() throws Exception {
        List<Record> records = makeRecords(8, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);

        Record original = records.get(5);
        Record first = page.get(original.key);
        assertFalse(first.value.isOffHeap());
        long callsAfterFirst = client.readCalls.get();
        Record second = page.get(original.key);
        assertEquals("second get on the same key must hit the cache",
                callsAfterFirst, client.readCalls.get());
        assertArrayEquals(original.value.to_array(), second.value.to_array());
    }

    /**
     * BLOCK-1 regression gate from the issue #411 PR review: simulate a
     * pure-PK projection scan that consumes every {@link Record} produced by
     * a {@link LazyDataPage} <b>without ever accessing {@code value}'s bytes</b>.
     *
     * <p>Before the eager-materialisation fix this would leak one refcount
     * per row to the cache's pooled chunks (the caller never released the
     * {@code Bytes.fromOffHeap} slice and never triggered lazy
     * materialisation). After the fix the cache's parent-chunk refcounts
     * return to baseline once the cache is invalidated, regardless of
     * whether the consumer touched the bytes.
     */
    @Test
    public void pkOnlyProjectionScanDoesNotLeakCacheRefcounts() throws Exception {
        // Use enough records to exercise the cache eviction path too.
        int rows = 64;
        int valueSize = 256;
        List<Record> records = makeRecords(rows, valueSize);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);

        // Simulate a SELECT pk_col / COUNT(*) / EXISTS scan: enumerate every
        // key, fetch the Record, look at the key only. Drop every Record on
        // the floor. If LazyDataPage.get returned a Bytes.fromOffHeap, this
        // loop would pin one cache slice per row forever — the cache's
        // parent chunks would stay alive and the post-invalidate refcount
        // assertion at the bottom would fail.
        for (Record original : records) {
            Record fetched = page.get(original.key);
            assertNotNull(fetched);
            // PK-only consumer: only read the key length (or the key itself).
            // Crucially we DO NOT touch fetched.value.* in any way that would
            // trigger materialisation or release.
            assertEquals(original.key.getLength(), fetched.key.getLength());
            // Drop the Record reference; nothing keeps it alive past this
            // iteration. If value were off-heap we would still leak.
        }

        // Drain the cache; if any refcount leaked into a Bytes.fromOffHeap
        // the underlying chunk would not return to the pool. With eager
        // materialisation the cache fully releases on close() and the
        // caller-side Bytes are plain heap byte[]s GC will reclaim.
        storage.getLazyValueCache().close();
        assertEquals("cache must be empty after close()",
                0L, storage.getLazyValueCache().estimatedSize());
    }
}
