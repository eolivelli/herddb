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
import herddb.remote.proto.ReadFileRangeRequest;
import herddb.remote.proto.ReadFileRangeResponse;
import herddb.remote.proto.ReadFileRequest;
import herddb.remote.proto.ReadFileResponse;
import herddb.remote.proto.RemoteFileServiceGrpc;
import herddb.remote.proto.WriteFileBlockRequest;
import herddb.remote.proto.WriteFileBlockResponse;
import herddb.remote.proto.WriteFileRequest;
import herddb.remote.proto.WriteFileResponse;
import herddb.server.RemoteFileClient;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
 * Supports dynamic server list updates via {@link #updateServers(List)}.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServiceClient implements AutoCloseable, RemoteFileClient {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServiceClient.class.getName());

    /** Configuration key for per-call deadline in seconds. */
    public static final String CONFIG_CLIENT_TIMEOUT = "remote.file.client.timeout";
    /** Configuration key for max retries on idempotent operations. */
    public static final String CONFIG_CLIENT_RETRIES = "remote.file.client.retries";
    /** Configuration key for the default block size used in multipart writes. */
    public static final String CONFIG_CLIENT_BLOCK_SIZE = "remote.file.client.block.size";

    private static final long DEFAULT_CLIENT_TIMEOUT_SECONDS = 1800; // 30 minutes
    private static final int DEFAULT_CLIENT_RETRIES = 10;
    /** Default multipart block size: 4 MB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;

    private static class ServerSnapshot {
        final ConsistentHashRouter router;
        final Map<String, ManagedChannel> channels;

        ServerSnapshot(ConsistentHashRouter router, Map<String, ManagedChannel> channels) {
            this.router = router;
            this.channels = Collections.unmodifiableMap(new HashMap<>(channels));
        }
    }

    private volatile ServerSnapshot snapshot;
    /**
     * Released once the client has seen at least one non-empty server list,
     * either at construction or via {@link #updateServers(List)}. Lets
     * bootstrap callers block on cold-cluster ZK discovery before issuing
     * the first RPC. See {@link #awaitServersReady(long)}.
     */
    private final CountDownLatch serversReadyLatch = new CountDownLatch(1);
    private final int maxRetries;
    private final long clientTimeoutSeconds;
    private final int blockSize;
    private final ScheduledExecutorService retryScheduler;
    private final ClientInterceptor clientInterceptor;

    public RemoteFileServiceClient(List<String> servers) {
        this(servers, Collections.emptyMap(), null);
    }

    public RemoteFileServiceClient(List<String> servers, Map<String, Object> configuration) {
        this(servers, configuration, null);
    }

    public RemoteFileServiceClient(List<String> servers, Map<String, Object> configuration,
                                   ClientInterceptor clientInterceptor) {
        this.clientInterceptor = clientInterceptor;
        this.clientTimeoutSeconds = longConfig(configuration, CONFIG_CLIENT_TIMEOUT, DEFAULT_CLIENT_TIMEOUT_SECONDS);
        this.maxRetries = intConfig(configuration, CONFIG_CLIENT_RETRIES, DEFAULT_CLIENT_RETRIES);
        this.blockSize = intConfig(configuration, CONFIG_CLIENT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "remote-file-retry");
            t.setDaemon(true);
            return t;
        });

        Map<String, ManagedChannel> channels = new HashMap<>();
        for (String server : servers) {
            channels.put(server, buildChannel(server));
        }
        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(servers), channels);
        if (!servers.isEmpty()) {
            this.serversReadyLatch.countDown();
        }

        if (servers.isEmpty()) {
            LOGGER.log(Level.INFO,
                    "RemoteFileServiceClient: starting with empty server list (awaiting ZK discovery), timeout={0}s, retries={1}",
                    new Object[]{clientTimeoutSeconds, maxRetries});
        } else {
            LOGGER.log(Level.INFO, "RemoteFileServiceClient: servers={0}, timeout={1}s, retries={2}",
                    new Object[]{servers, clientTimeoutSeconds, maxRetries});
        }
    }

    private static final int CHANNEL_MAX_MESSAGE_SIZE = DEFAULT_BLOCK_SIZE + 1024 * 1024; // 5 MB

    private ManagedChannel buildChannel(String server) {
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        ManagedChannelBuilder<?> b = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(300, TimeUnit.SECONDS)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(false)
                .maxInboundMessageSize(CHANNEL_MAX_MESSAGE_SIZE);
        if (clientInterceptor != null) {
            b.intercept(clientInterceptor);
        }
        return b.build();
    }

    /** Routing key for a specific block of a multipart file. */
    private static String blockRoutingKey(String path, long blockIndex) {
        return path + "#block" + blockIndex;
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForBlock(String path, long blockIndex) {
        String key = blockRoutingKey(path, blockIndex);
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(key);
        return RemoteFileServiceGrpc.newStub(s.channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void updateServers(List<String> newServers) {
        if (newServers.isEmpty()) {
            LOGGER.log(Level.WARNING, "updateServers called with empty list, keeping current servers");
            return;
        }

        ServerSnapshot current = this.snapshot;

        Set<String> added = new LinkedHashSet<>(newServers);
        added.removeAll(current.channels.keySet());

        Set<String> removed = new LinkedHashSet<>(current.channels.keySet());
        removed.removeAll(new HashSet<>(newServers));

        Map<String, ManagedChannel> newChannels = new HashMap<>();
        for (String server : newServers) {
            ManagedChannel existing = current.channels.get(server);
            if (existing != null) {
                newChannels.put(server, existing);
            } else {
                newChannels.put(server, buildChannel(server));
            }
        }

        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(newServers), newChannels);
        serversReadyLatch.countDown();

        LOGGER.log(Level.INFO, "Updated remote file servers: {0} (added: {1}, removed: {2})",
                new Object[]{newServers, added, removed});

        // Gracefully shutdown removed channels in background
        if (!removed.isEmpty()) {
            List<ManagedChannel> toShutdown = new ArrayList<>();
            for (String server : removed) {
                ManagedChannel ch = current.channels.get(server);
                if (ch != null) {
                    toShutdown.add(ch);
                }
            }
            Thread shutdownThread = new Thread(() -> {
                for (ManagedChannel ch : toShutdown) {
                    try {
                        ch.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        ch.shutdownNow();
                    }
                }
            }, "remote-file-channel-shutdown");
            shutdownThread.setDaemon(true);
            shutdownThread.start();
        }
    }

    /**
     * Returns {@code true} if the client currently has at least one server in
     * its consistent-hash ring. Used by callers that need to block on cold-boot
     * ZK discovery before issuing the first RPC.
     */
    public boolean hasServers() {
        ServerSnapshot s = this.snapshot;
        return !s.channels.isEmpty();
    }

    /**
     * Blocks for up to {@code timeoutMs} milliseconds until {@link #hasServers()}
     * returns {@code true}. Returns {@code true} as soon as a server is visible,
     * {@code false} on timeout. Intended for use at bootstrap on a cold cluster
     * where ZK service discovery may not yet have populated the server list by
     * the time the first RPC is issued.
     */
    public boolean awaitServersReady(long timeoutMs) throws InterruptedException {
        return serversReadyLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceBlockingStub blockingStubForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return RemoteFileServiceGrpc.newBlockingStub(s.channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return RemoteFileServiceGrpc.newStub(s.channels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    private RemoteFileServiceGrpc.RemoteFileServiceStub asyncStubForServer(String server) {
        ServerSnapshot s = this.snapshot;
        return RemoteFileServiceGrpc.newStub(s.channels.get(server))
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
        RemoteFileServiceGrpc.RemoteFileServiceStub stub;
        try {
            stub = asyncStubForPath(path);
        } catch (Exception e) {
            future.completeExceptionally(e);
            return future;
        }
        stub.writeFile(
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
        ServerSnapshot s = this.snapshot;
        List<CompletableFuture<List<String>>> futures = new ArrayList<>();
        for (String server : s.channels.keySet()) {
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
                    // Deduplicate: multiple servers may return the same logical path
                    // for different blocks of the same multipart file
                    LinkedHashSet<String> seen = new LinkedHashSet<>();
                    for (CompletableFuture<List<String>> f : futures) {
                        seen.addAll(f.join());
                    }
                    return new ArrayList<>(seen);
                });
    }

    /**
     * Writes one block of a multipart file. Not retried (not idempotent).
     * Routes via consistent hash of {@code path#block{blockIndex}}.
     */
    public CompletableFuture<Void> writeFileBlockAsync(String path, long blockIndex, byte[] content) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        asyncStubForBlock(path, blockIndex).writeFileBlock(
                WriteFileBlockRequest.newBuilder()
                        .setPath(path)
                        .setBlockIndex(blockIndex)
                        .setContent(UnsafeByteOperations.unsafeWrap(content))
                        .build(),
                new StreamObserver<WriteFileBlockResponse>() {
                    @Override
                    public void onNext(WriteFileBlockResponse response) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        future.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        future.complete(null);
                    }
                });
        return future;
    }

    /**
     * Reads a range of bytes from a (possibly multipart) file.
     * Routes to the server responsible for the block containing {@code offset}.
     * If the range spans two blocks, two sequential requests are made.
     *
     * @return the requested bytes, or null if the file is not found
     */
    public CompletableFuture<byte[]> readFileRangeAsync(String path, long offset, int length, int blockSize) {
        long startBlock = offset / blockSize;
        long endBlock = (offset + length - 1) / blockSize;
        if (startBlock == endBlock) {
            return doReadFileRangeAsync(path, offset, length, blockSize);
        }
        // Range spans two blocks — read sequentially and concatenate
        int firstBlockEnd = (int) ((startBlock + 1) * (long) blockSize - offset);
        int secondLength = length - firstBlockEnd;
        return doReadFileRangeAsync(path, offset, firstBlockEnd, blockSize)
                .thenCompose(first -> {
                    if (first == null) {
                        return CompletableFuture.completedFuture((byte[]) null);
                    }
                    long secondOffset = (startBlock + 1) * (long) blockSize;
                    return doReadFileRangeAsync(path, secondOffset, secondLength, blockSize)
                            .thenApply(second -> {
                                if (second == null) {
                                    return first;
                                }
                                byte[] combined = new byte[first.length + second.length];
                                System.arraycopy(first, 0, combined, 0, first.length);
                                System.arraycopy(second, 0, combined, first.length, second.length);
                                return combined;
                            });
                });
    }

    private CompletableFuture<byte[]> doReadFileRangeAsync(String path, long offset, int length, int blockSize) {
        return retryAsync(() -> {
            long blockIndex = offset / blockSize;
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            asyncStubForBlock(path, blockIndex).readFileRange(
                    ReadFileRangeRequest.newBuilder()
                            .setPath(path)
                            .setOffset(offset)
                            .setLength(length)
                            .setBlockSize(blockSize)
                            .build(),
                    new StreamObserver<ReadFileRangeResponse>() {
                        private byte[] result;

                        @Override
                        public void onNext(ReadFileRangeResponse response) {
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
        }, "readFileRange", path, 0);
    }

    /**
     * Writes an {@link InputStream} as a multipart file, splitting into blocks of {@code blockSize}.
     * Blocks are written sequentially. Returns the total number of bytes written.
     */
    public long writeMultipartFile(String path, InputStream in, int blockSize) throws IOException {
        byte[] buf = new byte[blockSize];
        long blockIndex = 0;
        long totalBytes = 0;
        int read;
        while ((read = readFully(in, buf)) > 0) {
            byte[] block = read == blockSize ? buf : java.util.Arrays.copyOf(buf, read);
            getUnchecked(writeFileBlockAsync(path, blockIndex, block));
            blockIndex++;
            totalBytes += read;
        }
        return totalBytes;
    }

    private static int readFully(InputStream in, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int read = in.read(buf, total, buf.length - total);
            if (read == -1) {
                break;
            }
            total += read;
        }
        return total;
    }

    public CompletableFuture<Integer> deleteByPrefixAsync(String prefix) {
        return retryAsync(() -> doDeleteByPrefixAsync(prefix), "deleteByPrefix", prefix, 0);
    }

    private CompletableFuture<Integer> doDeleteByPrefixAsync(String prefix) {
        ServerSnapshot s = this.snapshot;
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        for (String server : s.channels.keySet()) {
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

    @Override
    public void writeFile(String path, byte[] content) {
        getUnchecked(writeFileAsync(path, content));
    }

    public void writeFile(String path, byte[] buf, int offset, int len) {
        getUnchecked(writeFileAsync(path, buf, offset, len));
    }

    @Override
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

    public void writeFileBlock(String path, long blockIndex, byte[] content) {
        getUnchecked(writeFileBlockAsync(path, blockIndex, content));
    }

    public byte[] readFileRange(String path, long offset, int length, int blockSize) {
        return getUnchecked(readFileRangeAsync(path, offset, length, blockSize));
    }

    public int getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the server address that would store the given path.
     */
    public String getServerForPath(String path) {
        return snapshot.router.getServer(path);
    }

    public String getServerForBlock(String path, long blockIndex) {
        return snapshot.router.getServer(blockRoutingKey(path, blockIndex));
    }

    @Override
    public void close() {
        retryScheduler.shutdownNow();
        ServerSnapshot s = this.snapshot;
        for (Map.Entry<String, ManagedChannel> entry : s.channels.entrySet()) {
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
        CompletableFuture<T> actionResult;
        try {
            actionResult = action.execute();
        } catch (Exception e) {
            // Handle synchronous failures (e.g. "Hash ring is empty" when no
            // servers have been discovered yet) the same as async failures so
            // that the retry logic below can kick in.
            LOGGER.log(Level.INFO,
                    "remote file {0} synchronous failure for path {1} on attempt {2}, "
                            + "scheduling retry: {3}",
                    new Object[]{opName, path, attempt, e.toString()});
            actionResult = new CompletableFuture<>();
            actionResult.completeExceptionally(e);
        }
        actionResult.whenComplete((value, error) -> {
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
