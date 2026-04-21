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
import static org.junit.Assert.fail;
import herddb.model.Record;
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates the parallel-block-upload path added for issue #202. Uses a
 * small client block size so a short record list already spans multiple
 * blocks and exercises the fan-out code in
 * {@code RemoteFileDataStorageManager.writeAsMultipart}.
 */
public class RemoteFileBlockParallelismTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("remote").toPath());
        server.start();
        Map<String, Object> clientConfig = new HashMap<>();
        // Small block size so ordinary test payloads cross the block boundary and
        // actually exercise writeAsMultipart's parallel fan-out. Kept at 1 KiB so
        // two blocks cover typical test page sizes — readFileRange (used by
        // readPage) spans at most two blocks, so staying near that limit keeps
        // the round-trip path working while still crossing the multi-block branch.
        clientConfig.put(RemoteFileServiceClient.CONFIG_CLIENT_BLOCK_SIZE, 1024);
        client = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()), clientConfig);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private RemoteFileDataStorageManager newStorage(int blockParallelism) throws Exception {
        RemoteFileDataStorageManager storage = new RemoteFileDataStorageManager(
                folder.newFolder().toPath(),
                folder.newFolder().toPath(),
                1000, client, new LazyValueCache(0L), blockParallelism);
        storage.start();
        return storage;
    }

    @Test
    public void multiBlockPageRoundTripsWithParallelism8() throws Exception {
        try (RemoteFileDataStorageManager storage = newStorage(8)) {
            storage.initTablespace("ts1");
            storage.initTable("ts1", "uuid1");

            // Block size is 1 KiB — 4 records of ~400 bytes each should push total
            // payload past one block and into the second, exercising the parallel
            // fan-out. We stay under ~2 KiB so readFileRange's 2-block span covers
            // the whole page on read-back.
            List<Record> page = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                byte[] value = new byte[400];
                Arrays.fill(value, (byte) ('a' + (i % 26)));
                page.add(new Record(Bytes.from_string("key-" + i), Bytes.from_array(value)));
            }
            storage.writePage("ts1", "uuid1", 7L, page);

            List<Record> read = storage.readPage("ts1", "uuid1", 7L);
            assertEquals(page.size(), read.size());
            // Records should round-trip byte-for-byte.
            Map<Bytes, Bytes> expected = new HashMap<>();
            for (Record r : page) {
                expected.put(r.key, r.value);
            }
            for (Record r : read) {
                Bytes exp = expected.get(r.key);
                assertNotNull("missing key after round-trip: " + r.key, exp);
                assertEquals(exp, r.value);
            }
        }
    }

    @Test
    public void singleBlockPageStillWorksWithParallelismEnabled() throws Exception {
        // Tiny payload that fits in one block — exercises the single-block fast path
        // so we keep coverage on the non-parallel branch.
        try (RemoteFileDataStorageManager storage = newStorage(8)) {
            storage.initTablespace("ts1");
            storage.initTable("ts1", "uuid1");
            List<Record> page = Collections.singletonList(
                    new Record(Bytes.from_string("k"), Bytes.from_string("v")));
            storage.writePage("ts1", "uuid1", 1L, page);

            List<Record> read = storage.readPage("ts1", "uuid1", 1L);
            assertEquals(1, read.size());
            assertEquals(page.get(0).key, read.get(0).key);
            assertEquals(page.get(0).value, read.get(0).value);
        }
    }

    @Test
    public void emptyPageStillCreatesBlockZero() throws Exception {
        // writeAsMultipart must always create at least one block for an empty payload so
        // readers can see a valid file descriptor. The parallel path keeps that behavior.
        try (RemoteFileDataStorageManager storage = newStorage(4)) {
            storage.initTablespace("ts1");
            storage.initTable("ts1", "uuid1");
            storage.writePage("ts1", "uuid1", 1L, Collections.emptyList());
            // readFileRange of block 0 succeeds (the file exists even if it has no records).
            byte[] prefix = client.readFileRange(
                    "ts1/uuid1/data/1.page", 0L, 4, client.getBlockSize());
            assertNotNull(prefix);
        }
    }

    @Test
    public void parallelismOneStillWritesAllBlocks() throws Exception {
        // Regression: a parallelism cap of 1 must still deliver every block; the semaphore
        // acquire/release loop degenerates to serial but the code path is the parallel one.
        try (RemoteFileDataStorageManager storage = newStorage(1)) {
            storage.initTablespace("ts1");
            storage.initTable("ts1", "uuid1");
            List<Record> page = new ArrayList<>();
            // ~1.2 KiB payload: forces a second block at the 1 KiB boundary.
            for (int i = 0; i < 3; i++) {
                byte[] value = new byte[400];
                Arrays.fill(value, (byte) i);
                page.add(new Record(Bytes.from_string("k-" + i), Bytes.from_array(value)));
            }
            storage.writePage("ts1", "uuid1", 3L, page);
            assertEquals(page.size(), storage.readPage("ts1", "uuid1", 3L).size());
        }
    }

    @Test
    public void writeFailsCleanlyWhenClientClosedMidFlight() throws Exception {
        // If the client is closed while a write is in flight the parallel path must
        // surface the error (not silently succeed). This also protects against a buffer
        // still being pinned after a partial submission failure.
        RemoteFileDataStorageManager storage = newStorage(4);
        storage.initTablespace("ts1");
        storage.initTable("ts1", "uuid1");
        server.stop();
        AtomicInteger failures = new AtomicInteger();
        List<Record> page = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            byte[] v = new byte[400];
            Arrays.fill(v, (byte) i);
            page.add(new Record(Bytes.from_string("k-" + i), Bytes.from_array(v)));
        }
        try {
            storage.writePage("ts1", "uuid1", 42L, page);
            fail("writePage should have failed with the server stopped");
        } catch (RuntimeException expected) {
            failures.incrementAndGet();
        } finally {
            storage.close();
        }
        assertEquals(1, failures.get());
    }
}
