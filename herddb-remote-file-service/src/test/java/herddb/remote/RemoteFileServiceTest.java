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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end tests for the file service. Issue #425 ported these from raw
 * gRPC stubs onto the {@link RemoteFileServiceClient} public API; the
 * underlying wire is now the native Netty PDU protocol shared with HerdDB
 * core.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private RemoteFileServiceClient client;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();
        client = new RemoteFileServiceClient(List.of("localhost:" + server.getPort()));
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

    @Test
    public void testWriteReadRoundTrip() {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        client.writeFile("ts1/uuid1/data/1.page", data);
        byte[] readBack = client.readFile("ts1/uuid1/data/1.page");
        assertNotNull(readBack);
        assertEquals("hello world", new String(readBack, StandardCharsets.UTF_8));
    }

    @Test
    public void testReadMissingFile() {
        byte[] readBack = client.readFile("notexist/foo.page");
        assertNull(readBack);
    }

    @Test
    public void testDeleteFile() {
        byte[] data = "content".getBytes(StandardCharsets.UTF_8);
        client.writeFile("ts1/uuid1/data/2.page", data);
        assertTrue(client.deleteFile("ts1/uuid1/data/2.page"));
        // Second delete returns false (file gone)
        assertFalse(client.deleteFile("ts1/uuid1/data/2.page"));
        // Read confirms gone
        assertNull(client.readFile("ts1/uuid1/data/2.page"));
    }

    @Test
    public void testListFiles() {
        client.writeFile("ts1/uuid1/data/10.page", "a".getBytes());
        client.writeFile("ts1/uuid1/data/11.page", "b".getBytes());
        client.writeFile("ts2/uuid2/data/1.page", "c".getBytes());

        List<String> paths = client.listFiles("ts1/uuid1/data/");
        assertEquals(2, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.startsWith("ts1/uuid1/data/")));
    }

    @Test
    public void testDeleteByPrefix() {
        client.writeFile("ts1/uuid3/data/1.page", "x".getBytes());
        client.writeFile("ts1/uuid3/data/2.page", "y".getBytes());
        client.writeFile("ts1/uuid4/data/1.page", "z".getBytes());

        int deletedCount = client.deleteByPrefix("ts1/uuid3/");
        assertEquals(2, deletedCount);

        // ts1/uuid4/data/1.page still exists
        List<String> remaining = client.listFiles("ts1/");
        assertEquals(1, remaining.size());
        assertTrue(remaining.get(0).startsWith("ts1/uuid4/"));
    }

    @Test
    public void testAsyncClientApis() throws Exception {
        byte[] data = "async test data".getBytes(StandardCharsets.UTF_8);

        // Async write
        CompletableFuture<Long> writeFuture = client.writeFileAsync("async/test/1.page", data);
        long writtenSize = writeFuture.get(5, TimeUnit.SECONDS);
        assertEquals(data.length, writtenSize);

        // Async read
        CompletableFuture<byte[]> readFuture = client.readFileAsync("async/test/1.page");
        byte[] content = readFuture.get(5, TimeUnit.SECONDS);
        assertNotNull(content);
        assertEquals("async test data", new String(content, StandardCharsets.UTF_8));

        // Async read missing
        CompletableFuture<byte[]> missingFuture = client.readFileAsync("async/missing.page");
        assertNull(missingFuture.get(5, TimeUnit.SECONDS));

        // Async list
        CompletableFuture<List<String>> listFuture = client.listFilesAsync("async/test/");
        List<String> listed = listFuture.get(5, TimeUnit.SECONDS);
        assertEquals(1, listed.size());
        assertEquals("async/test/1.page", listed.get(0));

        // Async delete
        CompletableFuture<Boolean> deleteFuture = client.deleteFileAsync("async/test/1.page");
        assertTrue(deleteFuture.get(5, TimeUnit.SECONDS));

        // Async deleteByPrefix
        client.writeFileAsync("pfx/a.page", "a".getBytes()).get(5, TimeUnit.SECONDS);
        client.writeFileAsync("pfx/b.page", "b".getBytes()).get(5, TimeUnit.SECONDS);
        CompletableFuture<Integer> delPfxFuture = client.deleteByPrefixAsync("pfx/");
        assertEquals(2, (int) delPfxFuture.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testWriteFileBlockAndReadFileRange() {
        byte[] block0 = new byte[100];
        byte[] block1 = new byte[60];
        for (int i = 0; i < 100; i++) {
            block0[i] = (byte) i;
        }
        for (int i = 0; i < 60; i++) {
            block1[i] = (byte) (i + 100);
        }

        client.writeFileBlock("ts1/uuid1/multipart/graph", 0, block0);
        client.writeFileBlock("ts1/uuid1/multipart/graph", 1, block1);

        // Read a range within block 0 (offset=10, length=20, blockSize=100)
        byte[] got0 = client.readFileRange("ts1/uuid1/multipart/graph", 10, 20, 100);
        assertNotNull(got0);
        assertEquals(20, got0.length);
        for (int i = 0; i < 20; i++) {
            assertEquals((byte) (10 + i), got0[i]);
        }

        // Read start of block 1 (offset=100, length=5, blockSize=100)
        byte[] got1 = client.readFileRange("ts1/uuid1/multipart/graph", 100, 5, 100);
        assertNotNull(got1);
        assertEquals(5, got1.length);
        for (int i = 0; i < 5; i++) {
            assertEquals((byte) (100 + i), got1[i]);
        }

        // Missing block
        byte[] missing = client.readFileRange("ts1/uuid1/multipart/graph", 500, 10, 100);
        assertNull(missing);
    }

    @Test
    public void testWriteMultipartFileAndRoundTrip() throws Exception {
        int blockSize = 64;
        byte[] data = new byte[blockSize * 2 + 30];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }

        long written = client.writeMultipartFile("ts1/uuid1/largefile",
                new ByteArrayInputStream(data), blockSize);
        assertEquals(data.length, written);

        byte[] b0 = client.readFileRange("ts1/uuid1/largefile", 0, blockSize, blockSize);
        assertNotNull(b0);
        assertEquals(blockSize, b0.length);
        for (int i = 0; i < blockSize; i++) {
            assertEquals((byte) (i & 0xFF), b0[i]);
        }

        byte[] b1 = client.readFileRange("ts1/uuid1/largefile", blockSize, blockSize, blockSize);
        assertNotNull(b1);
        assertEquals(blockSize, b1.length);
        for (int i = 0; i < blockSize; i++) {
            assertEquals((byte) ((blockSize + i) & 0xFF), b1[i]);
        }

        byte[] b2 = client.readFileRange("ts1/uuid1/largefile", blockSize * 2, 30, blockSize);
        assertNotNull(b2);
        assertEquals(30, b2.length);
        for (int i = 0; i < 30; i++) {
            assertEquals((byte) ((blockSize * 2 + i) & 0xFF), b2[i]);
        }
    }

    @Test
    public void testMultipartListAndDelete() throws Exception {
        int blockSize = 32;
        byte[] data = new byte[blockSize * 3];
        client.writeMultipartFile("ts1/uuid2/graphfile",
                new ByteArrayInputStream(data), blockSize);
        client.writeFileAsync("ts1/uuid2/plain.page", "x".getBytes()).get(5, TimeUnit.SECONDS);

        // listFiles should return logical paths (deduped)
        List<String> listed = client.listFilesAsync("ts1/uuid2/").get(5, TimeUnit.SECONDS);
        assertEquals(2, listed.size());
        assertTrue(listed.contains("ts1/uuid2/graphfile"));
        assertTrue(listed.contains("ts1/uuid2/plain.page"));

        // deleteFile on logical multipart path removes all blocks
        client.deleteFileAsync("ts1/uuid2/graphfile").get(5, TimeUnit.SECONDS);
        List<String> afterDelete = client.listFilesAsync("ts1/uuid2/").get(5, TimeUnit.SECONDS);
        assertEquals(1, afterDelete.size());
        assertEquals("ts1/uuid2/plain.page", afterDelete.get(0));
    }
}
