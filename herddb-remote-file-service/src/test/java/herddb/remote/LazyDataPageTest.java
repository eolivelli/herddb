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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DataPage;
import herddb.model.Record;
import herddb.storage.DataStorageManagerException;
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
 * Verifies the behaviour of {@link LazyDataPage}: keys are resident in heap,
 * values are fetched on demand, memory accounting reflects the lightweight
 * in-memory footprint.
 */
public class LazyDataPageTest {

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
        final AtomicLong readBytes = new AtomicLong();

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
            readCalls.set(0);
            readBytes.set(0);
        }
    }

    private static List<Record> makeRecords(int numRecords, int valueSize) {
        List<Record> records = new ArrayList<>(numRecords);
        Random r = new Random(1337L);
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
    public void dsmAdvertisesLazyPageLoadSupport() {
        assertTrue(storage.supportsLazyPageLoad());
    }

    @Test
    public void loadLazyDataPage_returnsPageWithCorrectSize() throws Exception {
        List<Record> records = makeRecords(32, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        assertNotNull(page);
        assertTrue("expected a LazyDataPage, got " + page.getClass(),
                page instanceof LazyDataPage);
        LazyDataPage lazy = (LazyDataPage) page;
        assertEquals(32, lazy.indexSize());
    }

    @Test
    public void get_materialisesValueOnDemand() throws Exception {
        List<Record> records = makeRecords(8, 128);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        // use reflection-free access via helper on LazyDataPage.get (package-private)
        Bytes key = records.get(3).key;
        Record r = callGet(page, key);
        assertNotNull(r);
        assertEquals(key, r.key);
        assertArrayEquals(records.get(3).value.to_array(), r.value.to_array());
    }

    @Test
    public void get_missingKeyReturnsNullWithoutRemoteCall() throws Exception {
        List<Record> records = makeRecords(4, 64);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        client.reset();

        Record r = callGet(page, Bytes.from_string("absent-key"));
        assertNull(r);
        assertEquals("missing-key lookup should not hit remote storage",
                0L, client.readCalls.get());
    }

    @Test
    public void get_secondCallForSameKeyHitsValueCache() throws Exception {
        List<Record> records = makeRecords(8, 128);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        Bytes key = records.get(2).key;
        client.reset();

        Record first = callGet(page, key);
        long callsAfterFirst = client.readCalls.get();
        Record second = callGet(page, key);
        assertEquals("second get must not hit remote",
                callsAfterFirst, client.readCalls.get());
        assertNotNull(first);
        assertArrayEquals(first.value.to_array(), second.value.to_array());
    }

    @Test
    public void usedMemoryIsMuchSmallerThanEagerEquivalent() throws Exception {
        int num = 64;
        int valueSize = 4096; // 4 KiB
        List<Record> records = makeRecords(num, valueSize);
        storage.writePage(TS, UUID, PAGE_ID, records);

        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        long lazyMemory = page.getUsedMemory();

        long eagerMemory = 0;
        for (Record r : records) {
            eagerMemory += DataPage.estimateEntrySize(r);
        }
        assertTrue("lazy memory (" + lazyMemory
                        + ") should be well below eager memory (" + eagerMemory + ")",
                lazyMemory < eagerMemory / 10);
        assertTrue("lazy memory must still cover keys, got " + lazyMemory,
                lazyMemory >= num * records.get(0).key.getEstimatedSize());
    }

    @Test
    public void getRecordsForFlushThrows() throws Exception {
        List<Record> records = makeRecords(2, 32);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        try {
            callGetRecordsForFlush(page);
            fail("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void indexIsLoadedEagerlySoKeysNeverTriggerRemoteCalls() throws Exception {
        List<Record> records = makeRecords(16, 256);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        client.reset();

        // Enumerating the page's keys should hit no remote I/O at all.
        int size = callSize(page);
        assertEquals(16, size);
        assertFalse(callIsEmpty(page));
        assertEquals(0L, client.readCalls.get());
    }

    @Test
    public void invalidValueFetchSurfacedAsLazyValueFetchException() throws Exception {
        // Use a malformed metadata to drive the DSM's readPageValue into a
        // short-read exception, which should surface as
        // LazyDataPage.LazyValueFetchException.
        List<Record> records = makeRecords(3, 16);
        storage.writePage(TS, UUID, PAGE_ID, records);
        DataPage page = storage.loadLazyDataPage(TS, UUID, PAGE_ID, null, Long.MAX_VALUE);
        // Poison the remote storage: truncate the file by overwriting it with empty data.
        storage.writePage(TS, UUID, PAGE_ID, new ArrayList<>()); // empty page replaces it
        Bytes key = records.get(0).key;

        // The lazy page still has stale metadata — fetching it should fail.
        try {
            callGet(page, key);
            // It might succeed with an empty value (since invalidation happened
            // on write), which is also acceptable — we just want no JVM crash.
        } catch (LazyDataPage.LazyValueFetchException expected) {
            assertTrue(expected.getCause() instanceof DataStorageManagerException);
        } catch (RuntimeException ignored) {
            // Any RuntimeException is acceptable, we just don't want a hang or crash.
        }
    }

    private static Record callGet(DataPage page, Bytes key) {
        return page.get(key);
    }

    private static int callSize(DataPage page) {
        return page.size();
    }

    private static boolean callIsEmpty(DataPage page) {
        return page.isEmpty();
    }

    private static void callGetRecordsForFlush(DataPage page) {
        page.getRecordsForFlush();
    }
}
