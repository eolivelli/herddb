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
import static org.junit.Assert.assertTrue;
import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests {@code RemoteFileDataStorageManager.cleanupAfterTableBoot} with a
 * configurable batch size (issue #398) — verifies the batch loop, the metrics,
 * and that active pages are preserved.
 */
public class RemoteFileDataStorageManagerCleanupBatchTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;
    private TestStatsProvider statsProvider;
    private RemoteFileDataStorageManager storage;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        server.start();
        client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()));
        statsProvider = new TestStatsProvider();
        storage = new RemoteFileDataStorageManager(
                folder.newFolder("metadata").toPath(),
                folder.newFolder("tmp").toPath(),
                1000,
                client,
                new LazyValueCache(0L),
                herddb.server.ServerConfiguration.PROPERTY_REMOTE_FILE_BLOCK_PARALLELISM_DEFAULT,
                herddb.server.ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT,
                herddb.server.ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT,
                /* cleanupBatchSize */ 7,
                statsProvider.getStatsLogger("test"));
        storage.start();
    }

    @After
    public void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void cleanupBatchesAndPreservesActivePages() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        // Write 50 pages.
        int totalPages = 50;
        for (int p = 1; p <= totalPages; p++) {
            storage.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }
        // Mark a non-contiguous subset as active so the cleanup actually has work
        // to do (45 pages should be deleted).
        Set<Long> active = new HashSet<>();
        active.add(2L);
        active.add(11L);
        active.add(23L);
        active.add(37L);
        active.add(50L);
        storage.cleanupAfterTableBoot("ts1", "uuid1", active);

        // Active pages must still be readable.
        for (Long pageId : active) {
            List<Record> records = storage.readPage("ts1", "uuid1", pageId);
            assertEquals("active page " + pageId + " should still hold its single record",
                    1, records.size());
        }

        // 45 stale pages with batch size 7 → ceil(45/7) = 7 batches.
        // (6 full batches of 7 + 1 partial batch of 3.)
        int expectedDeletions = totalPages - active.size(); // 45
        int expectedBatches = (expectedDeletions + 7 - 1) / 7; // 7
        Counter deletions = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("deletions");
        Counter batches = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("batches");
        OpStatsLogger latency = statsProvider.getStatsLogger("test").scope("cleanup").getOpStatsLogger("batch_latency");
        assertEquals("deletion counter should match number of stale pages",
                expectedDeletions, deletions.get().intValue());
        assertEquals("batch counter should match ceil(staleCount/batchSize)",
                expectedBatches, batches.get().intValue());
        long successCount = ((TestStatsProvider.TestOpStatsLogger) latency).getSuccessCount();
        assertEquals("every batch must register a successful event",
                expectedBatches, (int) successCount);
    }

    @Test
    public void cleanupNoStalePagesIsNoOp() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        for (int p = 1; p <= 3; p++) {
            storage.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }
        Set<Long> all = new HashSet<>(Arrays.asList(1L, 2L, 3L));
        storage.cleanupAfterTableBoot("ts1", "uuid1", all);

        Counter deletions = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("deletions");
        Counter batches = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("batches");
        assertEquals("no deletions when every page is active", 0, deletions.get().intValue());
        assertEquals("no batches when nothing to delete", 0, batches.get().intValue());

        // Pages still readable.
        for (long p = 1; p <= 3; p++) {
            assertTrue(storage.readPage("ts1", "uuid1", p).size() == 1);
        }
    }

    @Test
    public void cleanupAllStalePages() throws Exception {
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        for (int p = 1; p <= 14; p++) {
            storage.writePage("ts1", "uuid1", p, Collections.singletonList(
                    new Record(Bytes.from_string("k" + p), Bytes.from_string("v" + p))));
        }
        // Active set is empty → all 14 pages are stale.
        // 14 / 7 = 2 batches exactly.
        storage.cleanupAfterTableBoot("ts1", "uuid1", Collections.emptySet());
        Counter deletions = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("deletions");
        Counter batches = statsProvider.getStatsLogger("test").scope("cleanup").getCounter("batches");
        assertEquals(14, deletions.get().intValue());
        assertEquals(2, batches.get().intValue());
    }
}
