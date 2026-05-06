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
 * Issue #411 — end-to-end verification that {@link LazyDataPage#get(Bytes)}
 * returns {@link Record}s whose value is backed by a direct {@link io.netty.buffer.ByteBuf}
 * slice owned by the {@link LazyValueCache}, not a fresh on-heap {@code byte[]}.
 *
 * <p>The test mirrors the production read path:
 * <ol>
 *   <li>write a v2 page to the remote file service;</li>
 *   <li>load the {@link LazyDataPage} via the DSM;</li>
 *   <li>resolve a record via {@code page.get(key)};</li>
 *   <li>assert {@code record.value.isOffHeap() == true} and that the value
 *       bytes round-trip identically to the original;</li>
 *   <li>release the {@code Bytes} (returns the cache slice's refcount to the
 *       pool) and verify the cache survives the release.</li>
 * </ol>
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
    public void getReturnsOffHeapBackedValue() throws Exception {
        List<Record> records = makeRecords(8, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);
        assertTrue("expected a LazyDataPage from the lazy DSM", page instanceof LazyDataPage);

        Record original = records.get(3);
        Record fetched = page.get(original.key);
        try {
            assertNotNull("get must return a record for an existing key", fetched);
            assertTrue("issue #411: value must be off-heap-backed", fetched.value.isOffHeap());
            assertArrayEquals("value bytes must round-trip",
                    original.value.to_array(), fetched.value.to_array());
        } finally {
            // Mirror the consumer's lifecycle: release the off-heap refcount
            // back to the cache pool. Idempotent and safe for already-on-heap
            // values too (the empty-value short-circuit, etc.).
            fetched.value.release();
        }
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
        try {
            assertNotNull(empty);
            assertEquals("zero-length values must not allocate any off-heap slice",
                    0, empty.value.getLength());
            assertFalse("zero-length values must short-circuit to on-heap EMPTY_ARRAY",
                    empty.value.isOffHeap());
            assertEquals("zero-length values must not trigger remote I/O",
                    0L, client.readCalls.get());
        } finally {
            empty.value.release();
        }

        Record data = page.get(Bytes.from_string("k-data"));
        try {
            assertNotNull(data);
            assertTrue("non-empty values must be off-heap-backed", data.value.isOffHeap());
            assertArrayEquals("hello".getBytes(), data.value.to_array());
        } finally {
            data.value.release();
        }
    }

    @Test
    public void cacheHitDoesNotReissueRemoteCall() throws Exception {
        List<Record> records = makeRecords(8, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);

        Record original = records.get(5);
        Record first = page.get(original.key);
        try {
            assertTrue(first.value.isOffHeap());
        } finally {
            first.value.release();
        }
        long callsAfterFirst = client.readCalls.get();
        Record second = page.get(original.key);
        try {
            assertEquals("second get on the same key must hit the cache",
                    callsAfterFirst, client.readCalls.get());
            assertArrayEquals(original.value.to_array(), second.value.to_array());
        } finally {
            second.value.release();
        }
    }

    @Test
    public void releasingValueDoesNotEvictTheCacheEntry() throws Exception {
        List<Record> records = makeRecords(4, 128);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, 1024L * 1024L);

        Record original = records.get(2);
        Record first = page.get(original.key);
        first.value.release();

        // The cache must still serve the value (release on a single caller
        // slice does not affect the cache's own refcount).
        long callsBefore = client.readCalls.get();
        Record second = page.get(original.key);
        try {
            assertEquals("cache entry must survive a caller release()",
                    callsBefore, client.readCalls.get());
            assertArrayEquals(original.value.to_array(), second.value.to_array());
        } finally {
            second.value.release();
        }
    }
}
