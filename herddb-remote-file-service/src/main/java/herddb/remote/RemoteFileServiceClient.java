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
import com.google.protobuf.UnsafeByteOperations;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

    /** Configuration key for per-call deadline in seconds. */
    public static final String CONFIG_CLIENT_TIMEOUT = "remote.file.client.timeout";
    /** Configuration key for max retries on idempotent operations. */
    public static final String CONFIG_CLIENT_RETRIES = "remote.file.client.retries";

    private static final long DEFAULT_CLIENT_TIMEOUT_SECONDS = 1800; // 30 minutes
    private static final int DEFAULT_CLIENT_RETRIES = 10;

    private final ConsistentHashRouter router;
    private final Map<String, ManagedChannel> channels;
    private final int maxRetries;
    private final long clientTimeoutSeconds;
    private final ScheduledExecutorService retryScheduler;

    public RemoteFileServiceClient(List<String> servers) {
        this(servers, Collections.emptyMap());
    }

    public RemoteFileServiceClient(List<String> servers, Map<String, Object> configuration) {
        this.clientTimeoutSeconds = longConfig(configuration, CONFIG_CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT_SECONDS);
        this.maxRetries = intConfig(configuration, CONFIG_CLIENT_RETRIES, DEFAULT_CLIENT_RETRIES);
        this.router = new ConsistentHashRouter(servers);
        this.channels = new HashMap<>();
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "remote-file-retry");
            t.setDaemon(true);
            return t;
        });

        for (String server : servers) {
            String[] parts = server.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .keepAliveTime(300, TimeUnit.SECONDS)
                    .keepAliveTimeout(20, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(false)
                    .build();
            channels.put(server, channel);
        }

        LOGGER.log(Level.INFO, "RemoteFileServiceClient: servers={0}, timeout={1}s, retries={2}",
                new Object[]{servers, clientTimeoutSeconds, maxRetries});
    }

    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub blockingStubForPath(String path) {
        String server = router.getServer(path);
        return RemoteFileServiceGrpc.newBlockingStub(channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForPath(String path) {
        String server = router.getServer(path);
        return RemoteFileServiceGrpc.newStub(channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForServer(String server) {
        return RemoteFileServiceGrpc.newStub(channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    // --- Async APIs returning CompletableFuture ---

    public CompletableFuture<Long> writeFileAsync(String path, byte[] content) {
        return writeFileAsync(path, UnsafeByteOperations.unsafeWrap(content));
    }

    public CompletableFuture<Long> writeFileAsync(String path, byte[] buf, int offset, int len) {
        return writeFileAsync(path, UnsafeByteOperations.unsafeWrap(buf, offset, len));
    }

    private CompletableFuture<Long> writeFileAsync(String path, ByteString content) {
        // Writes are not idempotent — no retry
        CompletableFuture<Long> future = new CompletableFuture<>();
        asyncStubForPath(path).writeFile(
                WriteFileRequest.newBuilder()
                        .setPath(path)
                        .setContent(content)
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
        return retryAsync(() -> doReadFileAsync(path), "readFile", path, 0);
    }

    private CompletableFuture<byte[]> doReadFileAsync(String path) {
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
        return retryAsync(() -> doDeleteFileAsync(path), "deleteFile", path, 0);
    }

    private CompletableFuture<Boolean> doDeleteFileAsync(String path) {
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
        return retryAsync(() -> doListFilesAsync(prefix), "listFiles", prefix, 0);
    }

    private CompletableFuture<List<String>> doListFilesAsync(String prefix) {
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (String server : channels.keySet()) {
            RemoteFileServiceGrpc.RemoteFileServiceStub stub = asyncStubForServer(server);
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
        return retryAsync(() -> doDeleteByPrefixAsync(prefix), "deleteByPrefix", prefix, 0);
    }

    private CompletableFuture<Integer> doDeleteByPrefixAsync(String prefix) {
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (String server : channels.keySet()) {
            RemoteFileServiceGrpc.RemoteFileServiceStub stub = asyncStubForServer(server);
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

    public void writeFile(String path, byte[] buf, int offset, int len) {
        getUnchecked(writeFileAsync(path, buf, offset, len));
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
        retryScheduler.shutdownNow();
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            try {
                entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted while shutting down channel to " + entry.getKey(), e);
            }
        }
    }

    // --- Retry helper ---

    @FunctionalInterface
    private interface AsyncAction<T> {
        CompletableFuture<T> execute();
    }

    private <T> CompletableFuture<T> retryAsync(AsyncAction<T> action, String opName, String path, int attempt) {
        CompletableFuture<T> result = new CompletableFuture<>();
        action.execute().whenComplete((value, error) -> {
            if (error == null) {
                result.complete(value);
                return;
            }
            int nextAttempt = attempt + 1;
            if (nextAttempt > maxRetries) {
                LOGGER.log(Level.WARNING, "remote file {0} failed after {1} retries for path {2}",
                        new Object[]{opName, maxRetries, path});
                result.completeExceptionally(error);
                return;
            }
            // Exponential backoff: 1s, 2s, 4s, ...
            long delayMs = 1000L * (1L << (nextAttempt - 1));
            LOGGER.log(Level.INFO, "remote file {0} retry {1}/{2} for path {3} after {4}ms (error: {5})",
                    new Object[]{opName, nextAttempt, maxRetries, path, delayMs, error.getMessage()});
            retryScheduler.schedule(() -> {
                retryAsync(action, opName, path, nextAttempt).whenComplete((r, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else {
                        result.complete(r);
                    }
                });
            }, delayMs, TimeUnit.MILLISECONDS);
        });
        return result;
    }

    // --- Internal helpers ---

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

    private static long longConfig(Map<String, Object> config, String key, long defaultValue) {
        Object v = config.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        return Long.parseLong(v.toString());
    }

    private static int intConfig(Map<String, Object> config, String key, int defaultValue) {
        Object v = config.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        return Integer.parseInt(v.toString());
    }
}
