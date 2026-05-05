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
import static org.junit.Assert.assertTrue;
import herddb.remote.proto.DeleteFileOutcome;
import herddb.remote.proto.DeleteFilesRequest;
import herddb.remote.proto.DeleteFilesResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Server-side tests for the {@code DeleteFiles} batch RPC (issue #398).
 * Hits {@link RemoteFileServiceImpl} directly via a gRPC channel so the per-path
 * outcome semantics (success / not-found / error) are nailed down independently
 * of the client-side fan-out logic.
 */
public class RemoteFileServiceImplDeleteFilesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RemoteFileServer server;
    private ManagedChannel channel;
    private RemoteFileServiceClient writeClient;

    @Before
    public void setUp() throws Exception {
        server = new RemoteFileServer(0, folder.newFolder("rfs").toPath());
        server.start();
        channel = ManagedChannelBuilder
                .forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();
        writeClient = new RemoteFileServiceClient(
                Arrays.asList("localhost:" + server.getPort()));
    }

    @After
    public void tearDown() throws Exception {
        if (writeClient != null) {
            writeClient.close();
        }
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void emptyRequestReturnsEmptyResponse() throws Exception {
        DeleteFilesResponse response = sendDeleteFiles(Collections.emptyList());
        assertEquals(0, response.getOutcomesCount());
    }

    @Test
    public void perPathOutcomesPreserveInputOrder() throws Exception {
        // Three pre-existing paths, three missing paths, interleaved. The server
        // must return one outcome per input path with deleted=true/false, in the
        // SAME ORDER as the request.
        writeClient.writeFile("server/exists-1", new byte[]{1});
        writeClient.writeFile("server/exists-2", new byte[]{2});
        writeClient.writeFile("server/exists-3", new byte[]{3});
        List<String> input = Arrays.asList(
                "server/exists-1",
                "server/missing-a",
                "server/exists-2",
                "server/missing-b",
                "server/exists-3",
                "server/missing-c"
        );
        DeleteFilesResponse response = sendDeleteFiles(input);
        assertEquals(input.size(), response.getOutcomesCount());
        for (int i = 0; i < input.size(); i++) {
            DeleteFileOutcome outcome = response.getOutcomes(i);
            assertEquals("outcome path must match input order at index " + i,
                    input.get(i), outcome.getPath());
            assertTrue("no per-path error expected, got " + outcome.getError(),
                    outcome.getError().isEmpty());
            if (input.get(i).startsWith("server/exists-")) {
                assertTrue("existing path should be reported deleted=true: " + input.get(i),
                        outcome.getDeleted());
            } else {
                assertFalse("missing path should be reported deleted=false: " + input.get(i),
                        outcome.getDeleted());
            }
        }
        assertTrue("server/ should be empty after batch delete",
                writeClient.listFiles("server/").isEmpty());
    }

    private DeleteFilesResponse sendDeleteFiles(List<String> paths) throws Exception {
        RemoteFileServiceGrpc.RemoteFileServiceStub stub = RemoteFileServiceGrpc.newStub(channel)
                .withDeadlineAfter(30, TimeUnit.SECONDS);
        CompletableFuture<DeleteFilesResponse> future = new CompletableFuture<>();
        stub.deleteFiles(
                DeleteFilesRequest.newBuilder().addAllPaths(paths).build(),
                new StreamObserver<DeleteFilesResponse>() {
                    private DeleteFilesResponse response;

                    @Override
                    public void onNext(DeleteFilesResponse value) {
                        this.response = value;
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(response);
                    }
                });
        return future.get(30, TimeUnit.SECONDS);
    }
}
