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
import com.google.protobuf.ByteString;
import herddb.remote.proto.DeleteByPrefixRequest;
import herddb.remote.proto.DeleteByPrefixResponse;
import herddb.remote.proto.DeleteFileRequest;
import herddb.remote.proto.DeleteFileResponse;
import herddb.remote.proto.ListFilesEntry;
import herddb.remote.proto.ListFilesRequest;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileRequest;
import herddb.remote.proto.WriteFileResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for RemoteFileService via raw gRPC stubs and async client APIs.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private ManagedChannel channel;
    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub stub;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("data").toPath());
        server.start();

        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        stub = RemoteFileServiceGrpc.newBlockingStub(channel);
    }

    @After
    public void tearDown() throws Exception {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        server.stop();
    }

    @Test
    public void testWriteReadRoundTrip() {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);

        WriteFileResponse writeResp = stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/1.page")
                .setContent(ByteString.copyFrom(data))
                .build());
        assertEquals(data.length, writeResp.getWrittenSize());

        ReadFileResponse readResp = stub.readFile(ReadFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/1.page")
                .build());
        assertTrue(readResp.getFound());
        assertEquals("hello world", readResp.getContent().toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testReadMissingFile() {
        ReadFileResponse readResp = stub.readFile(ReadFileRequest.newBuilder()
                .setPath("notexist/foo.page")
                .build());
        assertFalse(readResp.getFound());
        assertTrue(readResp.getContent().isEmpty());
    }

    @Test
    public void testDeleteFile() {
        byte[] data = "content".getBytes(StandardCharsets.UTF_8);
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/2.page")
                .setContent(ByteString.copyFrom(data))
                .build());

        DeleteFileResponse deleteResp = stub.deleteFile(DeleteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/2.page")
                .build());
        assertTrue(deleteResp.getDeleted());

        // Second delete returns false
        DeleteFileResponse deleteResp2 = stub.deleteFile(DeleteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/2.page")
                .build());
        assertFalse(deleteResp2.getDeleted());

        // Read confirms gone
        ReadFileResponse readResp = stub.readFile(ReadFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/2.page")
                .build());
        assertFalse(readResp.getFound());
    }

    @Test
    public void testListFiles() {
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/10.page")
                .setContent(ByteString.copyFromUtf8("a"))
                .build());
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid1/data/11.page")
                .setContent(ByteString.copyFromUtf8("b"))
                .build());
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts2/uuid2/data/1.page")
                .setContent(ByteString.copyFromUtf8("c"))
                .build());

        // Server-streaming: blocking stub returns an Iterator
        Iterator<ListFilesEntry> it = stub.listFiles(ListFilesRequest.newBuilder()
                .setPrefix("ts1/uuid1/data/")
                .build());
        List<String> paths = new ArrayList<>();
        it.forEachRemaining(entry -> paths.add(entry.getPath()));
        assertEquals(2, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.startsWith("ts1/uuid1/data/")));
    }

    @Test
    public void testDeleteByPrefix() {
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid3/data/1.page")
                .setContent(ByteString.copyFromUtf8("x"))
                .build());
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid3/data/2.page")
                .setContent(ByteString.copyFromUtf8("y"))
                .build());
        stub.writeFile(WriteFileRequest.newBuilder()
                .setPath("ts1/uuid4/data/1.page")
                .setContent(ByteString.copyFromUtf8("z"))
                .build());

        DeleteByPrefixResponse resp = stub.deleteByPrefix(DeleteByPrefixRequest.newBuilder()
                .setPrefix("ts1/uuid3/")
                .build());
        assertEquals(2, resp.getDeletedCount());

        // ts1/uuid4/data/1.page still exists
        Iterator<ListFilesEntry> it = stub.listFiles(ListFilesRequest.newBuilder()
                .setPrefix("ts1/")
                .build());
        List<String> remaining = new ArrayList<>();
        it.forEachRemaining(entry -> remaining.add(entry.getPath()));
        assertEquals(1, remaining.size());
        assertTrue(remaining.get(0).startsWith("ts1/uuid4/"));
    }

    @Test
    public void testAsyncClientApis() throws Exception {
        List<String> servers = List.of("localhost:" + server.getPort());
        try (RemoteFileServiceClient client = new RemoteFileServiceClient(servers)) {
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
    }
}
