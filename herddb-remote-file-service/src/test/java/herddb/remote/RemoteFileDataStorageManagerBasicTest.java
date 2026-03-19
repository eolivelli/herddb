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
}
