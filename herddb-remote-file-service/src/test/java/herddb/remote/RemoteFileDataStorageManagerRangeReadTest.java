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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.model.Record;
import herddb.remote.LazyDataPageFormat.FixedHeader;
import herddb.remote.LazyDataPageFormat.RecordMetadata;
import herddb.storage.DataPageDoesNotExistException;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
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
 * Verifies the byte-range helpers on {@link RemoteFileDataStorageManager}:
 * the fixed header, the index section, and individual record values can be
 * fetched with byte-range reads that transfer far fewer bytes than the full
 * page would.
 */
public class RemoteFileDataStorageManagerRangeReadTest {

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
        // Cache disabled so byte counts are fully deterministic per call.
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                1000,
                client,
                new LazyValueCache(0L));
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
        final AtomicLong readBytes = new AtomicLong();
        final AtomicLong readCalls = new AtomicLong();

        CountingClient(List<String> servers) {
            super(servers);
        }

        @Override
        public byte[] readFileRange(String path, long offset, int length, int blockSize) {
            byte[] bytes = super.readFileRange(path, offset, length, blockSize);
            readCalls.incrementAndGet();
            if (bytes != null) {
                readBytes.addAndGet(bytes.length);
            }
            return bytes;
        }

        void reset() {
            readBytes.set(0);
            readCalls.set(0);
        }
    }

    private static List<Record> buildLargePage(int numRecords, int valueSize) {
        List<Record> records = new ArrayList<>(numRecords);
        Random r = new Random(1337L);
        for (int i = 0; i < numRecords; i++) {
            byte[] value = new byte[valueSize];
            r.nextBytes(value);
            records.add(new Record(
                    Bytes.from_string("key-" + String.format("%06d", i)),
                    Bytes.from_array(value)));
        }
        return records;
    }

    @Test
    public void readPageHeaderReturnsCorrectCountsAndSizes() throws Exception {
        List<Record> records = buildLargePage(64, 1024);
        storage.writePage(TS, UUID, PAGE_ID, records);
        client.reset();

        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        assertNotNull(h);
        assertEquals(64, h.numRecords);
        assertTrue("indexSize should cover 64 records", h.indexSize > 0);
        assertEquals(64L * 1024L, h.valueSize);
        assertEquals("header read must transfer only the fixed-header bytes",
                LazyDataPageFormat.FIXED_HEADER_SIZE, client.readBytes.get());
        assertEquals(1L, client.readCalls.get());
    }

    @Test
    public void readPageIndexReturnsOffsetsThatMatchRecords() throws Exception {
        List<Record> records = buildLargePage(32, 512);
        storage.writePage(TS, UUID, PAGE_ID, records);
        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        client.reset();

        List<RecordMetadata> metadata = storage.readPageIndex(TS, UUID, PAGE_ID, h);
        assertEquals(records.size(), metadata.size());
        assertEquals("index read must fetch exactly indexSize bytes",
                (long) h.indexSize, client.readBytes.get());
        assertEquals(1L, client.readCalls.get());

        // Keys and value lengths match the original records, and offsets are
        // monotonically increasing and contiguous.
        long expectedOffset = 0;
        for (int i = 0; i < metadata.size(); i++) {
            RecordMetadata m = metadata.get(i);
            Record orig = records.get(i);
            assertEquals("key mismatch at " + i, orig.key, m.key);
            assertEquals("value length mismatch at " + i,
                    orig.value.getLength(), m.valueLength);
            assertEquals("offset mismatch at " + i, expectedOffset, m.valueOffset);
            expectedOffset += m.valueLength;
        }
    }

    @Test
    public void readPageValueReturnsExactBytesForASingleRecord() throws Exception {
        List<Record> records = buildLargePage(16, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);
        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        List<RecordMetadata> metadata = storage.readPageIndex(TS, UUID, PAGE_ID, h);

        // Pick a record in the middle.
        int pickIdx = 7;
        RecordMetadata m = metadata.get(pickIdx);
        client.reset();

        byte[] value = storage.readPageValue(TS, UUID, PAGE_ID, h, m.valueOffset, m.valueLength);
        assertArrayEquals(records.get(pickIdx).value.to_array(), value);
        assertEquals("value read must fetch exactly valueLength bytes",
                (long) m.valueLength, client.readBytes.get());
        assertEquals(1L, client.readCalls.get());
    }

    @Test
    public void totalBytesForSingleValueAccessIsMuchSmallerThanFullPage() throws Exception {
        // 256 records × 4 KiB = ~1 MiB payload page; fetching one value
        // should transfer on the order of (header + index + single value),
        // which for this page is ≈ 22 + ~4 KiB index + 4 KiB value ≈ 8 KiB.
        int numRecords = 256;
        int valueSize = 4096;
        List<Record> records = buildLargePage(numRecords, valueSize);
        storage.writePage(TS, UUID, PAGE_ID, records);
        long expectedFullPageBytes = (long) numRecords * (long) valueSize;

        client.reset();
        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        storage.readPageIndex(TS, UUID, PAGE_ID, h);
        RecordMetadata pick = storage.readPageIndex(TS, UUID, PAGE_ID, h).get(42);
        byte[] v = storage.readPageValue(TS, UUID, PAGE_ID, h, pick.valueOffset, pick.valueLength);
        assertArrayEquals(records.get(42).value.to_array(), v);

        long transferred = client.readBytes.get();
        assertTrue("expected lazy fetches to transfer ≪ full page ("
                        + transferred + " vs " + expectedFullPageBytes + ")",
                transferred < expectedFullPageBytes / 10);
    }

    @Test
    public void missingPageHeaderThrowsDataPageDoesNotExist() {
        try {
            storage.readPageHeader(TS, UUID, 9999L);
            fail("expected DataPageDoesNotExistException");
        } catch (Exception e) {
            assertTrue("got " + e.getClass() + ": " + e.getMessage(),
                    e instanceof DataPageDoesNotExistException);
        }
    }

    @Test
    public void emptyValueRoundTripsWithoutRemoteCall() throws Exception {
        List<Record> records = new ArrayList<>();
        records.add(new Record(Bytes.from_string("k1"), Bytes.from_array(new byte[0])));
        records.add(new Record(Bytes.from_string("k2"), Bytes.from_string("non-empty")));
        storage.writePage(TS, UUID, PAGE_ID, records);

        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        List<RecordMetadata> metadata = storage.readPageIndex(TS, UUID, PAGE_ID, h);
        client.reset();

        byte[] v0 = storage.readPageValue(TS, UUID, PAGE_ID, h,
                metadata.get(0).valueOffset, metadata.get(0).valueLength);
        assertEquals(0, v0.length);
        // Zero-length values are handled locally; no remote call expected.
        assertEquals(0L, client.readCalls.get());

        byte[] v1 = storage.readPageValue(TS, UUID, PAGE_ID, h,
                metadata.get(1).valueOffset, metadata.get(1).valueLength);
        assertArrayEquals("non-empty".getBytes(), v1);
        assertEquals(1L, client.readCalls.get());
    }

    @Test
    public void valueCacheHitAvoidsRemoteCall() throws Exception {
        // Swap the DSM for one that has a real value cache.
        storage.close();
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata2").toPath(),
                folder.newFolder("tmp2").toPath(),
                1000,
                client,
                new LazyValueCache(1024 * 1024));
        storage.start();
        storage.initTablespace(TS);
        storage.initTable(TS, UUID);

        List<Record> records = buildLargePage(8, 128);
        storage.writePage(TS, UUID, PAGE_ID, records);
        FixedHeader h = storage.readPageHeader(TS, UUID, PAGE_ID);
        List<RecordMetadata> metadata = storage.readPageIndex(TS, UUID, PAGE_ID, h);
        RecordMetadata m = metadata.get(3);
        client.reset();

        byte[] first = storage.readPageValue(TS, UUID, PAGE_ID, h, m.valueOffset, m.valueLength);
        long afterFirst = client.readCalls.get();
        byte[] second = storage.readPageValue(TS, UUID, PAGE_ID, h, m.valueOffset, m.valueLength);
        assertEquals("second call must be served from cache",
                afterFirst, client.readCalls.get());
        assertArrayEquals(first, second);

        // Writing over the page must invalidate the cache entry.
        storage.writePage(TS, UUID, PAGE_ID, records);
        FixedHeader h2 = storage.readPageHeader(TS, UUID, PAGE_ID);
        long beforeThird = client.readCalls.get();
        storage.readPageValue(TS, UUID, PAGE_ID, h2, m.valueOffset, m.valueLength);
        assertTrue("third call after invalidation should hit remote",
                client.readCalls.get() > beforeThird);
    }
}
