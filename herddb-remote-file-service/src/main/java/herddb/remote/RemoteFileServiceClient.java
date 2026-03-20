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
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for RemoteFileService. Manages one ManagedChannel per server and uses
 * ConsistentHashRouter for path distribution.
 * <p>
 * Provides both synchronous and asynchronous (CompletableFuture) APIs.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClient implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceClient.class.getName());

    private final ConsistentHashRouter router;
    private final Map<String, ManagedChannel> channels;
    private final Map<String, RemoteFileServiceGrpc.RemoteFileServiceBlockingStub> blockingStubs;
    private final Map<String, RemoteFileServiceGrpc.RemoteFileServiceStub> asyncStubs;

    public RemoteFileServiceClient(List<String> servers) {
        this.router = new ConsistentHashRouter(servers);
        this.channels = new HashMap<>();
        this.blockingStubs = new HashMap<>();
        this.asyncStubs = new HashMap<>();

        for (String server : servers) {
            String[] parts = server.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            channels.put(server, channel);
            blockingStubs.put(server, RemoteFileServiceGrpc.newBlockingStub(channel));
            asyncStubs.put(server, RemoteFileServiceGrpc.newStub(channel));
        }
    }

    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub blockingStubForPath(String path) {
        String server = router.getServer(path);
        return blockingStubs.get(server);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForPath(String path) {
        String server = router.getServer(path);
        return asyncStubs.get(server);
    }

    // --- Async APIs returning CompletableFuture ---

    public CompletableFuture<Long> writeFileAsync(String path, byte[] content) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        asyncStubForPath(path).writeFile(
                WriteFileRequest.newBuilder()
                        .setPath(path)
                        .setContent(ByteString.copyFrom(content))
                        .build(),
                new StreamObserver<WriteFileResponse>() {
                    private long writtenSize;

                    @Override
                    public void onNext(WriteFileResponse response) {
                        writtenSize = response.getWrittenSize();
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(writtenSize);
                    }
                });
        return future;
    }

    public CompletableFuture<byte[]> readFileAsync(String path) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        asyncStubForPath(path).readFile(
                ReadFileRequest.newBuilder().setPath(path).build(),
                new StreamObserver<ReadFileResponse>() {
                    private byte[] result;

                    @Override
                    public void onNext(ReadFileResponse response) {
                        if (response.getFound()) {
                            result = response.getContent().toByteArray();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(result);
                    }
                });
        return future;
    }

    public CompletableFuture<Boolean> deleteFileAsync(String path) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        asyncStubForPath(path).deleteFile(
                DeleteFileRequest.newBuilder().setPath(path).build(),
                new StreamObserver<DeleteFileResponse>() {
                    private boolean deleted;

                    @Override
                    public void onNext(DeleteFileResponse response) {
                        deleted = response.getDeleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(deleted);
                    }
                });
        return future;
    }

    public CompletableFuture<List<String>> listFilesAsync(String prefix) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (RemoteFileServiceGrpc.RemoteFileServiceStub stub : asyncStubs.values()) {
            CompletableFuture<List<String>> serverFuture = new CompletableFuture<>();
            List<String> paths = Collections.synchronizedList(new ArrayList<>());
            stub.listFiles(
                    ListFilesRequest.newBuilder().setPrefix(prefix).build(),
                    new StreamObserver<ListFilesEntry>() {
                        @Override
                        public void onNext(ListFilesEntry entry) {
                            paths.add(entry.getPath());
                        }

                        @Override
                        public void onError(Throwable t) {
                            serverFuture.completeExceptionally(t);
                        }

                        @Override
                        public void onCompleted() {
                            serverFuture.complete(paths);
                        }
                    });
            futures.add(serverFuture);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<String> result = new ArrayList<>();
                    for (CompletableFuture<List<String>> f : futures) {
                        result.addAll(f.join());
                    }
                    return result;
                });
    }

    public CompletableFuture<Integer> deleteByPrefixAsync(String prefix) {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (RemoteFileServiceGrpc.RemoteFileServiceStub stub : asyncStubs.values()) {
            CompletableFuture<Integer> serverFuture = new CompletableFuture<>();
            stub.deleteByPrefix(
                    DeleteByPrefixRequest.newBuilder().setPrefix(prefix).build(),
                    new StreamObserver<DeleteByPrefixResponse>() {
                        private int count;

                        @Override
                        public void onNext(DeleteByPrefixResponse response) {
                            count = response.getDeletedCount();
                        }

                        @Override
                        public void onError(Throwable t) {
                            serverFuture.completeExceptionally(t);
                        }

                        @Override
                        public void onCompleted() {
                            serverFuture.complete(count);
                        }
                    });
            futures.add(serverFuture);
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    int total = 0;
                    for (CompletableFuture<Integer> f : futures) {
                        total += f.join();
                    }
                    return total;
                });
    }

    // --- Synchronous APIs (wrappers around async) ---

    public void writeFile(String path, byte[] content) {
        getUnchecked(writeFileAsync(path, content));
    }

    public byte[] readFile(String path) {
        return getUnchecked(readFileAsync(path));
    }

    public boolean deleteFile(String path) {
        return getUnchecked(deleteFileAsync(path));
    }

    public List<String> listFiles(String prefix) {
        return getUnchecked(listFilesAsync(prefix));
    }

    public int deleteByPrefix(String prefix) {
        return getUnchecked(deleteByPrefixAsync(prefix));
    }

    /**
     * Returns the server address that would store the given path.
     */
    public String getServerForPath(String path) {
        return router.getServer(path);
    }

    @Override
    public void close() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            try {
                entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted while shutting down channel to " + entry.getKey(), e);
            }
        }
    }

    private static <T> T getUnchecked(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
