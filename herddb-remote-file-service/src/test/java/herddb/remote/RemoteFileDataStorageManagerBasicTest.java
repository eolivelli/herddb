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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.PageSet;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.model.Record;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Basic DataStorageManager operations tests for RemoteFileDataStorageManager.
 *
 * @author enrico.olivelli
 */
public class RemoteFileDataStorageManagerBasicTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private RemoteFileDataStorageManager storage;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
        server.start();
        client = new RemoteFileServiceClient(Arrays.asList("localhost:" + server.getPort()));
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                1000,
                client);
        storage.start();
    }

    @After
    public void tearDown() throws Exception {
        storage.close();
        client.close();
        server.stop();
    }

    private static PageSet.DataPageMetaData makeMeta() throws Exception {
        VisibleByteArrayOutputStream baos = new VisibleByteArrayOutputStream(16);
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(baos)) {
            out.writeVLong(100); // size
            out.writeVLong(50);  // avgRecordSize
            out.writeVLong(0);   // dirt
        }
        try (ExtendedDataInputStream in = new ExtendedDataInputStream(
                new SimpleByteArrayInputStream(baos.toByteArray()))) {
            return PageSet.DataPageMetaData.deserialize(in);
        }
    }

    @Test
    public void testInitTablespaceAndTable() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
    }

    @Test
    public void testWriteAndReadPage() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        List<Record> page = new ArrayList<>();
        page.add(new Record(Bytes.from_string("k1"), Bytes.from_string("v1")));
        page.add(new Record(Bytes.from_string("k2"), Bytes.from_string("v2")));
        storage.writePage("ts1", "uuid1", 1L, page);

        List<Record> read = storage.readPage("ts1", "uuid1", 1L);
        assertEquals(2, read.size());
    }

    @Test
    public void testWrittenPageUsesV2Format() throws Exception {
        // Lock in the on-disk format: pages written by the remote DSM must
        // start with the v2 magic and be decodable as a v2 page.
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        List<Record> page = new ArrayList<>();
        page.add(new Record(Bytes.from_string("alpha"), Bytes.from_string("A".repeat(64))));
        page.add(new Record(Bytes.from_string("bravo"), Bytes.from_string("B".repeat(128))));
        page.add(new Record(Bytes.from_string("charlie"), Bytes.from_string("C".repeat(16))));
        storage.writePage("ts1", "uuid1", 42L, page);

        byte[] raw = client.readFile("ts1/uuid1/data/42.page");
        assertNotNull(raw);
        // Magic at bytes [0..4): "HDP2"
        int magic = ((raw[0] & 0xff) << 24)
                | ((raw[1] & 0xff) << 16)
                | ((raw[2] & 0xff) << 8)
                | (raw[3] & 0xff);
        assertEquals(LazyDataPageFormat.MAGIC, magic);
        assertEquals(LazyDataPageFormat.VERSION_V2, raw[4]);

        // Round-trip via the DSM.
        List<Record> read = storage.readPage("ts1", "uuid1", 42L);
        assertEquals(3, read.size());
        assertEquals(page.get(0).key, read.get(0).key);
        assertEquals(page.get(0).value, read.get(0).value);
        assertEquals(page.get(2).key, read.get(2).key);
        assertEquals(page.get(2).value, read.get(2).value);
    }

    @Test
    public void testWriteAndReadIndexPage() throws Exception {
        storage.initTablespace("ts1");
        storage.initIndex("ts1", "idx1");

        byte[] indexData = {1, 2, 3, 4, 5};
        storage.writeIndexPage("ts1", "idx1", 1L, out -> out.write(indexData));

        byte[] read = storage.readIndexPage("ts1", "idx1", 1L, in -> {
            byte[] buf = new byte[5];
            in.readArray(5, buf);
            return buf;
        });
        assertTrue(Arrays.equals(indexData, read));
    }

    @Test
    public void testDeleteIndexPageRemovesFromRemoteStorage() throws Exception {
        // PersistentVectorStore's Phase B rollback calls deleteIndexPage to
        // reclaim provisionally written pages after a failure. For remote
        // storage this must translate to a real client.deleteFile(...) — the
        // no-op base-class default would leak S3 objects indefinitely.
        storage.initTablespace("ts1");
        storage.initIndex("ts1", "idx1");

        byte[] indexData = {1, 2, 3, 4, 5};
        storage.writeIndexPage("ts1", "idx1", 1L, out -> out.write(indexData));

        // Sanity: round-trip works before delete.
        byte[] read = storage.readIndexPage("ts1", "idx1", 1L, in -> {
            byte[] buf = new byte[5];
            in.readArray(5, buf);
            return buf;
        });
        assertTrue(Arrays.equals(indexData, read));

        // Act.
        storage.deleteIndexPage("ts1", "idx1", 1L);

        // After delete, reading the same page must fail.
        try {
            storage.readIndexPage("ts1", "idx1", 1L, in -> {
                byte[] buf = new byte[5];
                in.readArray(5, buf);
                return buf;
            });
            org.junit.Assert.fail("readIndexPage must fail after deleteIndexPage");
        } catch (herddb.storage.DataStorageManagerException expected) {
            // ok
        }
    }

    @Test
    public void testDeleteIndexPageIsIdempotent() throws Exception {
        // The DataStorageManager.deleteIndexPage contract is idempotent —
        // deleting a pageId that was never written (or already deleted)
        // must not throw. This is important because the Phase B rollback
        // path iterates through possibly-missing pageIds.
        storage.initTablespace("ts1");
        storage.initIndex("ts1", "idx1");

        // Never-written pageId: must not throw.
        storage.deleteIndexPage("ts1", "idx1", 999L);

        // Write then delete twice: second delete must not throw.
        storage.writeIndexPage("ts1", "idx1", 42L, out -> out.write(new byte[]{7}));
        storage.deleteIndexPage("ts1", "idx1", 42L);
        storage.deleteIndexPage("ts1", "idx1", 42L);
    }

    @Test
    public void testTableCheckpoint() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        List<Record> page = new ArrayList<>();
        page.add(new Record(Bytes.from_string("k1"), Bytes.from_string("v1")));
        storage.writePage("ts1", "uuid1", 1L, page);

        LogSequenceNumber lsn = new LogSequenceNumber(1, 1);
        Map<Long, PageSet.DataPageMetaData> activePages = new HashMap<>();
        activePages.put(1L, makeMeta());
        TableStatus status = new TableStatus("uuid1", lsn,
                Bytes.longToByteArray(2L), 2L, activePages);
        assertNotNull(storage.tableCheckpoint("ts1", "uuid1", status, false));
    }

    @Test
    public void testIndexCheckpoint() throws Exception {
        storage.initTablespace("ts1");
        storage.initIndex("ts1", "idx1");

        byte[] indexData = {1, 2, 3};
        storage.writeIndexPage("ts1", "idx1", 1L, out -> out.write(indexData));

        LogSequenceNumber lsn = new LogSequenceNumber(1, 1);
        Set<Long> activePages = new HashSet<>();
        activePages.add(1L);
        IndexStatus indexStatus = new IndexStatus("idx1", lsn, 2L, activePages, indexData);
        assertNotNull(storage.indexCheckpoint("ts1", "idx1", indexStatus, false));
    }

    @Test
    public void testFullTableScan() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        for (int p = 1; p <= 2; p++) {
            List<Record> page = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                page.add(new Record(Bytes.from_string("k" + p + "_" + i), Bytes.from_string("v" + i)));
            }
            storage.writePage("ts1", "uuid1", p, page);
        }

        LogSequenceNumber lsn = new LogSequenceNumber(1, 1);
        Map<Long, PageSet.DataPageMetaData> activePages = new HashMap<>();
        activePages.put(1L, makeMeta());
        activePages.put(2L, makeMeta());
        TableStatus status = new TableStatus("uuid1", lsn,
                Bytes.longToByteArray(3L), 3L, activePages);
        storage.tableCheckpoint("ts1", "uuid1", status, false);

        AtomicInteger totalRecords = new AtomicInteger(0);
        storage.fullTableScan("ts1", "uuid1", new FullTableScanConsumer() {
            @Override
            public void acceptPage(long pageId, List<Record> records) {
                totalRecords.addAndGet(records.size());
            }
        });
        assertEquals(6, totalRecords.get());
    }

    @Test
    public void testDropTable() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        storage.writePage("ts1", "uuid1", 1L, Collections.singletonList(
                new Record(Bytes.from_string("k"), Bytes.from_string("v"))));
        storage.dropTable("ts1", "uuid1");
    }

    @Test
    public void testTruncateAndDropIndex() throws Exception {
        storage.initTablespace("ts1");
        storage.initIndex("ts1", "idx1");
        byte[] data = {1, 2};
        storage.writeIndexPage("ts1", "idx1", 1L, out -> out.write(data));
        storage.truncateIndex("ts1", "idx1");
        storage.dropIndex("ts1", "idx1");
    }

    @Test
    public void testCleanupAfterTableBoot() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        for (int p = 1; p <= 2; p++) {
            storage.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }
        Set<Long> activePagesAtBoot = new HashSet<>();
        activePagesAtBoot.add(1L);
        storage.cleanupAfterTableBoot("ts1", "uuid1", activePagesAtBoot);

        // Page 1 still readable
        List<Record> records = storage.readPage("ts1", "uuid1", 1L);
        assertEquals(1, records.size());
    }

    /**
     * Verifies that stale pages from a previous checkpoint are scheduled for deletion on the
     * second checkpoint using the in-memory diff (no remote listFiles needed after the first
     * checkpoint).
     *
     * Scenario:
     *   1. Write pages 1, 2, 3.
     *   2. First checkpoint: active = {1, 2, 3}. Triggers listFiles fallback; no stale pages.
     *   3. Write page 4.
     *   4. Second checkpoint: active = {2, 3, 4}. Page 1 is stale; diff path used.
     *      A RemoteDeletePageAction for page 1 must be returned.
     *   5. Execute PostCheckpointActions → page 1 is deleted remotely.
     *   6. Third checkpoint: active = {3, 4}. Pages 1 and 2 become stale.
     *      Page 1 is already gone (idempotent delete); page 2 deletion is returned.
     */
    @Test
    public void testTableCheckpointInMemoryDiffDeletesStalePages() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        // Write pages 1, 2, 3
        for (int p = 1; p <= 3; p++) {
            storage.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }

        LogSequenceNumber lsn1 = new LogSequenceNumber(1, 1);
        Map<Long, PageSet.DataPageMetaData> activePages1 = new HashMap<>();
        activePages1.put(1L, makeMeta());
        activePages1.put(2L, makeMeta());
        activePages1.put(3L, makeMeta());
        TableStatus status1 = new TableStatus("uuid1", lsn1, Bytes.longToByteArray(4L), 4L, activePages1);

        // First checkpoint — listFiles fallback; pages 1, 2, 3 are all active → no stale pages
        List<PostCheckpointAction> actions1 = storage.tableCheckpoint("ts1", "uuid1", status1, false);
        for (PostCheckpointAction a : actions1) {
            a.run();
        }

        // All three pages still readable
        assertNotNull(storage.readPage("ts1", "uuid1", 1L));
        assertNotNull(storage.readPage("ts1", "uuid1", 2L));
        assertNotNull(storage.readPage("ts1", "uuid1", 3L));

        // Write page 4
        storage.writePage("ts1", "uuid1", 4L, Collections.singletonList(
                new Record(Bytes.from_string("k4"), Bytes.from_string("v4"))));

        LogSequenceNumber lsn2 = new LogSequenceNumber(1, 2);
        Map<Long, PageSet.DataPageMetaData> activePages2 = new HashMap<>();
        activePages2.put(2L, makeMeta());
        activePages2.put(3L, makeMeta());
        activePages2.put(4L, makeMeta());
        TableStatus status2 = new TableStatus("uuid1", lsn2, Bytes.longToByteArray(5L), 5L, activePages2);

        // Second checkpoint — diff path: page 1 is stale and must be scheduled for deletion
        List<PostCheckpointAction> actions2 = storage.tableCheckpoint("ts1", "uuid1", status2, false);
        long staleCount2 = actions2.stream()
                .filter(a -> a.description.contains("page 1"))
                .count();
        assertTrue("Expected a delete action for stale page 1; got actions: " + actions2, staleCount2 >= 1);

        // Execute actions → page 1 deleted remotely
        for (PostCheckpointAction a : actions2) {
            a.run();
        }
        // Pages 2, 3, 4 still accessible
        assertNotNull(storage.readPage("ts1", "uuid1", 2L));
        assertNotNull(storage.readPage("ts1", "uuid1", 3L));
        assertNotNull(storage.readPage("ts1", "uuid1", 4L));

        // Third checkpoint: active = {3, 4} → pages 1 and 2 should be scheduled for deletion
        // (page 1 is already gone — delete is idempotent)
        LogSequenceNumber lsn3 = new LogSequenceNumber(1, 3);
        Map<Long, PageSet.DataPageMetaData> activePages3 = new HashMap<>();
        activePages3.put(3L, makeMeta());
        activePages3.put(4L, makeMeta());
        TableStatus status3 = new TableStatus("uuid1", lsn3, Bytes.longToByteArray(6L), 6L, activePages3);

        List<PostCheckpointAction> actions3 = storage.tableCheckpoint("ts1", "uuid1", status3, false);
        long staleCount3page2 = actions3.stream()
                .filter(a -> a.description.contains("page 2"))
                .count();
        assertTrue("Expected a delete action for stale page 2 in third checkpoint", staleCount3page2 >= 1);
        for (PostCheckpointAction a : actions3) {
            a.run();
        }
        // Pages 3, 4 still accessible
        assertNotNull(storage.readPage("ts1", "uuid1", 3L));
        assertNotNull(storage.readPage("ts1", "uuid1", 4L));
    }

    /**
     * Verifies that when the file server is unavailable after the first checkpoint,
     * subsequent checkpoints still succeed using the in-memory diff path (no listFiles call).
     */
    @Test
    public void testTableCheckpointSucceedsWithoutServerAfterFirstCheckpoint() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");

        storage.writePage("ts1", "uuid1", 1L, Collections.singletonList(
                new Record(Bytes.from_string("k1"), Bytes.from_string("v1"))));
        storage.writePage("ts1", "uuid1", 2L, Collections.singletonList(
                new Record(Bytes.from_string("k2"), Bytes.from_string("v2"))));

        LogSequenceNumber lsn1 = new LogSequenceNumber(1, 1);
        Map<Long, PageSet.DataPageMetaData> activePages1 = new HashMap<>();
        activePages1.put(1L, makeMeta());
        activePages1.put(2L, makeMeta());
        TableStatus status1 = new TableStatus("uuid1", lsn1, Bytes.longToByteArray(3L), 3L, activePages1);

        // First checkpoint — populates in-memory state via listFiles fallback
        List<PostCheckpointAction> actions1 = storage.tableCheckpoint("ts1", "uuid1", status1, false);
        for (PostCheckpointAction a : actions1) {
            a.run();
        }

        // Stop the file server — subsequent listFiles calls would fail
        server.stop();

        // Second checkpoint — must use the diff path, not listFiles; page 1 becomes stale
        LogSequenceNumber lsn2 = new LogSequenceNumber(1, 2);
        Map<Long, PageSet.DataPageMetaData> activePages2 = new HashMap<>();
        activePages2.put(2L, makeMeta());
        TableStatus status2 = new TableStatus("uuid1", lsn2, Bytes.longToByteArray(3L), 3L, activePages2);

        // Must NOT throw even though the file server is gone
        List<PostCheckpointAction> actions2 = storage.tableCheckpoint("ts1", "uuid1", status2, false);
        long staleCount = actions2.stream()
                .filter(a -> a.description.contains("page 1"))
                .count();
        assertTrue("Expected a delete action for stale page 1", staleCount >= 1);
        // Note: tearDown calls server.stop() (idempotent) and client.close() which are safe here.
    }
}
