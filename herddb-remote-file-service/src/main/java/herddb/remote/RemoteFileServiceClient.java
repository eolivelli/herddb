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
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

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
    /**
     * Configuration key for the maximum number of bytes across all in-flight
     * {@code readFile}/{@code readFileRange} gRPC calls whose payloads are
     * currently being staged into a pooled direct {@link ByteBuf}. Each call
     * reserves a byte budget equal to its requested payload length before
     * allocation; the reservation is released on the terminal gRPC callback.
     * The ceiling caps peak direct-memory footprint from in-flight reads,
     * preventing IS checkpoint Phase C and query bursts that miss the
     * {@code SegmentBlockCache} from growing Netty's {@code PoolArena}
     * beyond {@code -XX:MaxDirectMemorySize} (see issue #246).
     *
     * <p>Bytes, not request count: {@code readFileRange} is also used by
     * {@link RemoteRandomAccessReader} with 16 KiB windows, so a per-request
     * cap would grossly under-count direct-memory pressure from small reads
     * and over-throttle large ones. The underlying Netty {@code PoolChunk}
     * size (typically 4 MiB) is irrelevant here: a 16 KiB read contributes
     * 16 KiB to the reservation, the pool is free to share a chunk across
     * sibling reads, and the reservation tracks only what the caller asked
     * for.
     */
    public static final String CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES =
            "remote.file.client.max.inflight.read.bytes";

    private static final long DEFAULT_CLIENT_TIMEOUT_SECONDS = 1800; // 30 minutes
    private static final int DEFAULT_CLIENT_RETRIES = 10;
    /** Default multipart block size: 4 MB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;
    /**
     * Default cap on in-flight read bytes: 256 MiB. Chosen so a 3.5 GiB
     * {@code SegmentBlockCache} plus this cap plus routine gRPC transport
     * comfortably fit a 6 GiB {@code -XX:MaxDirectMemorySize} budget, while
     * still admitting tens of thousands of 16 KiB concurrent block reads
     * ({@code 256 MiB / 16 KiB = 16,384}) or a full fan-out of 4 MiB
     * Phase-C chunks ({@code 256 MiB / 4 MiB = 64}) without blocking.
     */
    public static final long DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES = 256L * 1024 * 1024;

    /**
     * Emit a {@link Level#WARNING} log line if acquiring the reservation
     * blocks for longer than this threshold. Non-configurable: the threshold
     * is only meant to surface saturation; the gauge
     * ({@code remote_file_client_inflight_read_bytes_available}) is the
     * primary observability signal.
     */
    private static final long PERMIT_ACQUIRE_WARN_THRESHOLD_MS = 500;

    private static class ServerSnapshot {
        final ConsistentHashRouter router;
        /**
         * Read-plane channels, one per server. Keyed by server id. Separate from
         * {@link #writeChannels} so read and write RPCs sit on independent TCP
         * connections and cannot share HTTP/2 stream budgets (issue #100).
         */
        final Map<String, ManagedChannel> readChannels;
        /** Write-plane channels. Same keys as {@link #readChannels}. */
        final Map<String, ManagedChannel> writeChannels;

        ServerSnapshot(ConsistentHashRouter router,
                       Map<String, ManagedChannel> readChannels,
                       Map<String, ManagedChannel> writeChannels) {
            this.router = router;
            this.readChannels = Collections.unmodifiableMap(new HashMap<>(readChannels));
            this.writeChannels = Collections.unmodifiableMap(new HashMap<>(writeChannels));
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
    private final long maxInflightReadBytes;
    /**
     * Gates direct-buffer byte-volume across in-flight RPCs performed by
     * {@link #doReadFileAsByteBufAsync} and
     * {@link #doReadFileRangeAsByteBufAsync}. Each call reserves bytes equal
     * to its requested payload length and releases the same count when the
     * gRPC stream terminates (onError or onCompleted). Unfair semaphore:
     * the workload is bursty and latency-insensitive (caller threads are
     * indistinguishable), so unfair throughput beats FIFO starvation
     * avoidance here.
     *
     * <p>The semaphore is sized as {@code min(maxInflightReadBytes,
     * Integer.MAX_VALUE)} because {@link Semaphore} uses {@code int} permits
     * internally; practical caps (≲ 2 GiB) fit comfortably.
     */
    private final Semaphore inflightReadBytes;
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
        long configuredMaxInflightBytes = longConfig(configuration, CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES,
                DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES);
        if (configuredMaxInflightBytes <= 0) {
            throw new IllegalArgumentException(CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES
                    + " must be > 0, got " + configuredMaxInflightBytes);
        }
        if (configuredMaxInflightBytes < this.blockSize) {
            throw new IllegalArgumentException(CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES
                    + " (" + configuredMaxInflightBytes + ") must be >= "
                    + CONFIG_CLIENT_BLOCK_SIZE + " (" + this.blockSize
                    + ") so a single full-block read is always admissible");
        }
        this.maxInflightReadBytes = configuredMaxInflightBytes;
        // Semaphore uses int permits; clamp but keep the public view in bytes.
        int permits = maxInflightReadBytes > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) maxInflightReadBytes;
        this.inflightReadBytes = new Semaphore(permits);
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "remote-file-retry");
            t.setDaemon(true);
            return t;
        });

        Map<String, ManagedChannel> readChannels = new HashMap<>();
        Map<String, ManagedChannel> writeChannels = new HashMap<>();
        for (String server : servers) {
            readChannels.put(server, buildChannel(server));
            writeChannels.put(server, buildChannel(server));
        }
        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(servers), readChannels, writeChannels);
        if (!servers.isEmpty()) {
            this.serversReadyLatch.countDown();
        }

        if (servers.isEmpty()) {
            LOGGER.log(Level.INFO,
                    "RemoteFileServiceClient: starting with empty server list (awaiting ZK discovery), "
                            + "timeout={0}s, retries={1}, maxInflightReadBytes={2}",
                    new Object[]{clientTimeoutSeconds, maxRetries, maxInflightReadBytes});
        } else {
            LOGGER.log(Level.INFO,
                    "RemoteFileServiceClient: servers={0}, timeout={1}s, retries={2}, maxInflightReadBytes={3}",
                    new Object[]{servers, clientTimeoutSeconds, maxRetries, maxInflightReadBytes});
        }
    }

    /**
     * @return the configured maximum number of bytes that may be in flight
     *     across {@code readFile}/{@code readFileRange} calls at once.
     */
    public long maxInflightReadBytes() {
        return maxInflightReadBytes;
    }

    /**
     * @return the number of bytes currently available for reservation.
     *     Equal to {@link #maxInflightReadBytes()} when idle, approaches
     *     zero under saturation. Primary observability signal for the
     *     in-flight read budget; exposed as a Prometheus gauge by the
     *     Indexing Service.
     */
    public long availableInflightReadBytes() {
        return inflightReadBytes.availablePermits();
    }

    /**
     * Reserves {@code bytes} against the in-flight read budget, blocking
     * the caller until the reservation is satisfied. Emits a
     * {@link Level#WARNING} log line if the wait exceeds
     * {@link #PERMIT_ACQUIRE_WARN_THRESHOLD_MS}, a cheap saturation hint
     * for operators while the gauge carries the precise signal.
     *
     * <p>Uses {@code acquireUninterruptibly} so in-flight reads are not
     * cancelled by thread interrupts from callers that opportunistically
     * interrupt worker threads. Callers needing true cancellation should
     * wrap the {@code CompletableFuture} they receive.
     *
     * <p>Callers MUST pair every successful acquire with exactly one
     * {@link #releaseInflightReadBytes(int)} carrying the same byte count
     * on the terminal gRPC callback path.
     *
     * @param bytes the payload size being staged; clamped to the
     *     {@code Integer.MAX_VALUE} semaphore permit space. Must be > 0.
     */
    private void acquireInflightReadBytes(int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0, got " + bytes);
        }
        if (inflightReadBytes.tryAcquire(bytes)) {
            return;
        }
        long startNanos = System.nanoTime();
        inflightReadBytes.acquireUninterruptibly(bytes);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (elapsedMs >= PERMIT_ACQUIRE_WARN_THRESHOLD_MS) {
            LOGGER.log(Level.WARNING,
                    "remote file client inflight-read reservation blocked for {0}ms "
                            + "(requested={1} bytes, available={2}/{3}); consider raising "
                            + CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES
                            + " or reducing concurrent IS load",
                    new Object[]{elapsedMs, bytes, inflightReadBytes.availablePermits(),
                            maxInflightReadBytes});
        }
    }

    /** Releases a byte reservation previously taken via {@link #acquireInflightReadBytes(int)}. */
    private void releaseInflightReadBytes(int bytes) {
        inflightReadBytes.release(bytes);
    }

    /**
     * Registers {@code inflight_read_bytes_available} and
     * {@code inflight_read_bytes_max} gauges under the given scope.
     * Matches the gauges exported by the Indexing Service so the same
     * Grafana panels work for both servers.
     */
    @Override
    public void registerMetrics(StatsLogger statsLogger) {
        if (statsLogger == null) {
            return;
        }
        statsLogger.registerGauge("inflight_read_bytes_available", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return availableInflightReadBytes();
            }
        });
        statsLogger.registerGauge("inflight_read_bytes_max", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return maxInflightReadBytes();
            }
        });
    }

    private static final int CHANNEL_MAX_MESSAGE_SIZE = DEFAULT_BLOCK_SIZE + 1024 * 1024; // 5 MB

    private ManagedChannel buildChannel(String server) {
        String[] parts = server.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        ManagedChannelBuilder<?> b = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .disableRetry()  // eliminate UncommittedRetriableStreamsRegistry lock contention (issue #122)
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

    /** Read-plane stub for a specific block (issue #100). */
    private RemoteFileServiceGrpc.RemoteFileServiceStub readStubForBlock(String path, long blockIndex) {
        String key = blockRoutingKey(path, blockIndex);
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(key);
        return RemoteFileServiceGrpc.newStub(s.readChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS)
                .withMaxOutboundMessageSize(CHANNEL_MAX_MESSAGE_SIZE);
    }

    /** Write-plane stub for a specific block (issue #100). */
    private RemoteFileServiceGrpc.RemoteFileServiceStub writeStubForBlock(String path, long blockIndex) {
        String key = blockRoutingKey(path, blockIndex);
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(key);
        return RemoteFileServiceGrpc.newStub(s.writeChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS)
                .withMaxOutboundMessageSize(CHANNEL_MAX_MESSAGE_SIZE);
    }

    /**
     * Runs {@code action} under {@link Context#ROOT} so any gRPC stub built and
     * invoked inside it does not inherit a deadline from the caller's current
     * {@link Context}. gRPC resolves the effective deadline as the minimum of
     * the stub deadline set via {@code .withDeadlineAfter(...)} and the
     * propagated context deadline — without detaching, an incoming 600 s search
     * RPC on the Indexing Service would clamp every downstream readFileRange
     * call to whatever remained on that context, causing
     * {@code DEADLINE_EXCEEDED: context timed out} on large segment loads
     * (issue #242). Detaching gives each client call the full
     * {@code clientTimeoutSeconds} budget configured on the stub.
     */
    private static <T> T runDetached(Supplier<T> action) {
        Context previous = Context.ROOT.attach();
        try {
            return action.get();
        } finally {
            Context.ROOT.detach(previous);
        }
    }

    @Override
    public synchronized void updateServers(List<String> newServers) {
        if (newServers.isEmpty()) {
            LOGGER.log(Level.WARNING, "updateServers called with empty list, keeping current servers");
            return;
        }

        ServerSnapshot current = this.snapshot;

        Set<String> added = new LinkedHashSet<>(newServers);
        added.removeAll(current.readChannels.keySet());

        Set<String> removed = new LinkedHashSet<>(current.readChannels.keySet());
        removed.removeAll(new HashSet<>(newServers));

        Map<String, ManagedChannel> newReadChannels = new HashMap<>();
        Map<String, ManagedChannel> newWriteChannels = new HashMap<>();
        for (String server : newServers) {
            ManagedChannel existingRead = current.readChannels.get(server);
            ManagedChannel existingWrite = current.writeChannels.get(server);
            newReadChannels.put(server, existingRead != null ? existingRead : buildChannel(server));
            newWriteChannels.put(server, existingWrite != null ? existingWrite : buildChannel(server));
        }

        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(newServers),
                newReadChannels, newWriteChannels);
        serversReadyLatch.countDown();

        LOGGER.log(Level.INFO, "Updated remote file servers: {0} (added: {1}, removed: {2})",
                new Object[]{newServers, added, removed});

        // Gracefully shutdown removed channel pairs in background
        if (!removed.isEmpty()) {
            List<ManagedChannel> toShutdown = new ArrayList<>();
            for (String server : removed) {
                ManagedChannel readCh = current.readChannels.get(server);
                if (readCh != null) {
                    toShutdown.add(readCh);
                }
                ManagedChannel writeCh = current.writeChannels.get(server);
                if (writeCh != null) {
                    toShutdown.add(writeCh);
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
        return !s.readChannels.isEmpty();
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

    /** Read-plane async stub for a path. */
    private RemoteFileServiceGrpc.RemoteFileServiceStub readStubForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return RemoteFileServiceGrpc.newStub(s.readChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    /** Write-plane async stub for a path. */
    private RemoteFileServiceGrpc.RemoteFileServiceStub writeStubForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return RemoteFileServiceGrpc.newStub(s.writeChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    /** Read-plane async stub targeting a specific server (used by listFiles fan-out). */
    private RemoteFileServiceGrpc.RemoteFileServiceStub readStubForServer(String server) {
        ServerSnapshot s = this.snapshot;
        return RemoteFileServiceGrpc.newStub(s.readChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    /** Write-plane async stub targeting a specific server (used by deleteByPrefix fan-out). */
    private RemoteFileServiceGrpc.RemoteFileServiceStub writeStubForServer(String server) {
        ServerSnapshot s = this.snapshot;
        return RemoteFileServiceGrpc.newStub(s.writeChannels.get(server))
                .withDeadlineAfter(clientTimeoutSeconds, TimeUnit.SECONDS);
    }

    // --- Async APIs returning CompletableFuture ---

    public CompletableFuture<Long> writeFileAsync(String path, byte[] content) {
        return writeFileAsync(path, UnsafeByteOperations.unsafeWrap(content));
    }

    public CompletableFuture<Long> writeFileAsync(String path, byte[] buf, int offset, int len) {
        return writeFileAsync(path, UnsafeByteOperations.unsafeWrap(buf, offset, len));
    }

    /**
     * Writes a file from a pooled ByteBuf. Zero-copy to gRPC ByteString.
     * The ByteBuf is NOT released by this method; caller must release after completion.
     */
    public CompletableFuture<Long> writeFileAsync(String path, ByteBuf content) {
        ByteString bs = content.hasArray()
                ? UnsafeByteOperations.unsafeWrap(content.array(),
                        content.arrayOffset() + content.readerIndex(), content.readableBytes())
                : UnsafeByteOperations.unsafeWrap(content.nioBuffer());
        return writeFileAsync(path, bs);
    }

    private CompletableFuture<Long> writeFileAsync(String path, ByteString content) {
        // Writes are not idempotent — no retry
        return runDetached(() -> {
            CompletableFuture<Long> future = new CompletableFuture<>();
            RemoteFileServiceGrpc.RemoteFileServiceStub stub;
            try {
                stub = writeStubForPath(path);
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
        });
    }

    public CompletableFuture<byte[]> readFileAsync(String path) {
        return retryAsync(() -> doReadFileAsync(path), "readFile", path, 0);
    }

    private CompletableFuture<byte[]> doReadFileAsync(String path) {
        return runDetached(() -> {
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            readStubForPath(path).readFile(
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
        });
    }

    /**
     * Reads a file into a pooled ByteBuf. Returns null if file not found.
     * Caller must release the ByteBuf after use.
     */
    public CompletableFuture<ByteBuf> readFileAsByteBufAsync(String path) {
        return retryAsync(() -> doReadFileAsByteBufAsync(path), "readFile", path, 0);
    }

    private CompletableFuture<ByteBuf> doReadFileAsByteBufAsync(String path) {
        return runDetached(() -> {
            CompletableFuture<ByteBuf> future = new CompletableFuture<>();
            // readFile returns the whole file in a single onNext payload,
            // whose size is unknown until the server responds. Reserve
            // blockSize bytes as a conservative proxy: readFile is a legacy
            // small-object API (metadata, watermarks — not segment data),
            // so files are typically well under blockSize. Files larger
            // than blockSize will transiently exceed the in-flight budget
            // by the difference; acceptable because segment data flows
            // through readFileRange, not readFile.
            int reservation = blockSize;
            acquireInflightReadBytes(reservation);
            AtomicBoolean reservationReleased = new AtomicBoolean(false);
            Runnable releaseOnce = () -> {
                if (reservationReleased.compareAndSet(false, true)) {
                    releaseInflightReadBytes(reservation);
                }
            };
            try {
                readStubForPath(path).readFile(
                        ReadFileRequest.newBuilder().setPath(path).build(),
                        new StreamObserver<ReadFileResponse>() {
                            private ByteBuf result;

                            @Override
                            public void onNext(ReadFileResponse response) {
                                if (response.getFound()) {
                                    ByteString content = response.getContent();
                                    int size = content.size();
                                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(size);
                                    // Single copy from the protobuf LiteralByteString into the
                                    // pooled direct buffer; matches doReadFileRangeAsByteBufAsync.
                                    content.copyTo(buf.nioBuffer(0, size));
                                    buf.writerIndex(size);
                                    result = buf;
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                try {
                                    if (result != null) {
                                        result.release();
                                        result = null;
                                    }
                                    future.completeExceptionally(t);
                                } finally {
                                    releaseOnce.run();
                                }
                            }

                            @Override
                            public void onCompleted() {
                                try {
                                    future.complete(result);
                                } finally {
                                    releaseOnce.run();
                                }
                            }
                        });
            } catch (RuntimeException e) {
                // Synchronous failure (no observer callback will fire) —
                // release the permit here to keep the semaphore in balance.
                releaseOnce.run();
                throw e;
            }
            return future;
        });
    }

    public CompletableFuture<Boolean> deleteFileAsync(String path) {
        return retryAsync(() -> doDeleteFileAsync(path), "deleteFile", path, 0);
    }

    private CompletableFuture<Boolean> doDeleteFileAsync(String path) {
        return runDetached(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            writeStubForPath(path).deleteFile(
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
        });
    }

    public CompletableFuture<List<String>> listFilesAsync(String prefix) {
        return retryAsync(() -> doListFilesAsync(prefix), "listFiles", prefix, 0);
    }

    private CompletableFuture<List<String>> doListFilesAsync(String prefix) {
        return runDetached(() -> {
            ServerSnapshot s = this.snapshot;
            List<CompletableFuture<List<String>>> futures = new ArrayList<>();
            for (String server : s.readChannels.keySet()) {
                RemoteFileServiceGrpc.RemoteFileServiceStub stub = readStubForServer(server);
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
        });
    }

    /**
     * Writes one block of a multipart file. Not retried (not idempotent).
     * Routes via consistent hash of {@code path#block{blockIndex}}.
     */
    public CompletableFuture<Void> writeFileBlockAsync(String path, long blockIndex, byte[] content) {
        return runDetached(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            writeStubForBlock(path, blockIndex).writeFileBlock(
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
        });
    }

    /**
     * Writes one block of a multipart file from a ByteBuf. Not retried (not idempotent).
     * Routes via consistent hash of {@code path#block{blockIndex}}.
     * The ByteBuf is NOT released by this method; caller must release after completion.
     */
    public CompletableFuture<Void> writeFileBlockAsync(String path, long blockIndex, ByteBuf content) {
        ByteString bs = content.hasArray()
                ? UnsafeByteOperations.unsafeWrap(content.array(),
                        content.arrayOffset() + content.readerIndex(), content.readableBytes())
                : UnsafeByteOperations.unsafeWrap(content.nioBuffer());
        return runDetached(() -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            writeStubForBlock(path, blockIndex).writeFileBlock(
                    WriteFileBlockRequest.newBuilder()
                            .setPath(path)
                            .setBlockIndex(blockIndex)
                            .setContent(bs)
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
        });
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
        return retryAsync(() -> runDetached(() -> {
            long blockIndex = offset / blockSize;
            CompletableFuture<byte[]> future = new CompletableFuture<>();
            readStubForBlock(path, blockIndex).readFileRange(
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
                            // Broad catch at the gRPC callback boundary: if onNext
                            // throws (e.g. OutOfMemoryError from toByteArray), gRPC
                            // replaces the cause with an opaque "CANCELLED: Failed
                            // to read message." — which makes root-cause analysis
                            // impossible. Capture the cause directly.
                            try {
                                if (response.getFound()) {
                                    result = response.getContent().toByteArray();
                                }
                            } catch (Throwable t) {
                                future.completeExceptionally(t);
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            future.completeExceptionally(t);
                        }

                        @Override
                        public void onCompleted() {
                            if (!future.isDone()) {
                                future.complete(result);
                            }
                        }
                    });
            return future;
        }), "readFileRange", path, 0);
    }

    /**
     * ByteBuf variant of {@link #readFileRangeAsync(String, long, int, int)} —
     * the response bytes land directly in a pooled direct {@link ByteBuf} via
     * {@code ByteString.copyTo(ByteBuffer)}, skipping the throw-away
     * {@code byte[]} that {@code response.getContent().toByteArray()} would
     * allocate. The protobuf parser still materialises the response payload
     * once inside its {@code LiteralByteString} (gRPC marshaller-mandated copy);
     * eliminating that requires bypassing protobuf entirely.
     * <p>
     * Cross-block ranges are returned as a {@link CompositeByteBuf} view of the
     * two slices — no extra arraycopy. Caller MUST {@code release()} the result
     * when done. Returns {@code null} if the file is not found.
     */
    public CompletableFuture<ByteBuf> readFileRangeAsByteBufAsync(String path, long offset,
                                                                  int length, int blockSize) {
        long startBlock = offset / blockSize;
        long endBlock = (offset + length - 1) / blockSize;
        if (startBlock == endBlock) {
            return doReadFileRangeAsByteBufAsync(path, offset, length, blockSize);
        }
        int firstBlockEnd = (int) ((startBlock + 1) * (long) blockSize - offset);
        int secondLength = length - firstBlockEnd;
        return doReadFileRangeAsByteBufAsync(path, offset, firstBlockEnd, blockSize)
                .thenCompose(first -> {
                    if (first == null) {
                        return CompletableFuture.completedFuture((ByteBuf) null);
                    }
                    long secondOffset = (startBlock + 1) * (long) blockSize;
                    return doReadFileRangeAsByteBufAsync(path, secondOffset, secondLength, blockSize)
                            .thenApply(second -> {
                                if (second == null) {
                                    return first;
                                }
                                // CompositeByteBuf gives a contiguous view over the two
                                // pooled slices without copying their bytes. addComponent
                                // takes ownership of the component's refcount.
                                CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT
                                        .compositeDirectBuffer(2);
                                composite.addComponent(true, first);
                                composite.addComponent(true, second);
                                return composite;
                            })
                            .exceptionally(t -> {
                                ReferenceCountUtil.safeRelease(first);
                                if (t instanceof RuntimeException) {
                                    throw (RuntimeException) t;
                                }
                                throw new RuntimeException(t);
                            });
                });
    }

    private CompletableFuture<ByteBuf> doReadFileRangeAsByteBufAsync(String path, long offset,
                                                                    int length, int blockSize) {
        return retryAsync(() -> runDetached(() -> {
            long blockIndex = offset / blockSize;
            CompletableFuture<ByteBuf> future = new CompletableFuture<>();
            // Reserve bytes equal to the requested payload length before
            // firing the RPC — bounds the peak direct-memory high-water
            // across concurrent reads. Reservation is released on the
            // terminal observer callback (onError or onCompleted).
            // Using bytes (not request count) means 16 KiB block reads
            // from RemoteRandomAccessReader and 4 MiB Phase C chunks both
            // contribute to the same budget in proportion to the actual
            // direct-memory pressure they impose (issue #246).
            int reservation = length;
            acquireInflightReadBytes(reservation);
            AtomicBoolean reservationReleased = new AtomicBoolean(false);
            Runnable releaseOnce = () -> {
                if (reservationReleased.compareAndSet(false, true)) {
                    releaseInflightReadBytes(reservation);
                }
            };
            try {
                readStubForBlock(path, blockIndex).readFileRange(
                        ReadFileRangeRequest.newBuilder()
                                .setPath(path)
                                .setOffset(offset)
                                .setLength(length)
                                .setBlockSize(blockSize)
                                .build(),
                        new StreamObserver<ReadFileRangeResponse>() {
                            private ByteBuf result;

                            @Override
                            public void onNext(ReadFileRangeResponse response) {
                                // Broad catch at the gRPC callback boundary: direct ByteBuf
                                // allocation and the protobuf copy can throw OOM /
                                // OutOfDirectMemoryError. gRPC would otherwise rewrap the
                                // failure as an opaque "CANCELLED: Failed to read message.",
                                // hiding the real cause from the retry log and metrics.
                                ByteBuf local = null;
                                try {
                                    if (response.getFound()) {
                                        ByteString content = response.getContent();
                                        int size = content.size();
                                        local = PooledByteBufAllocator.DEFAULT.directBuffer(size);
                                        // copyTo writes into the ByteBuffer view of the pooled buf
                                        // and the protobuf parser already holds the bytes in heap;
                                        // this is the single copy from heap into direct memory.
                                        content.copyTo(local.nioBuffer(0, size));
                                        local.writerIndex(size);
                                        result = local;
                                        local = null;
                                    }
                                } catch (Throwable t) {
                                    if (local != null) {
                                        ReferenceCountUtil.safeRelease(local);
                                    }
                                    future.completeExceptionally(t);
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                try {
                                    if (result != null) {
                                        ReferenceCountUtil.safeRelease(result);
                                        result = null;
                                    }
                                    future.completeExceptionally(t);
                                } finally {
                                    releaseOnce.run();
                                }
                            }

                            @Override
                            public void onCompleted() {
                                try {
                                    if (!future.isDone()) {
                                        future.complete(result);
                                    } else if (result != null) {
                                        // Future already failed in onNext — release the buffer we
                                        // would have returned so it does not leak.
                                        ReferenceCountUtil.safeRelease(result);
                                        result = null;
                                    }
                                } finally {
                                    releaseOnce.run();
                                }
                            }
                        });
            } catch (RuntimeException e) {
                // Stub-side synchronous failure — no observer callback will
                // ever fire, so release the permit here to keep the
                // semaphore balanced across retries.
                releaseOnce.run();
                throw e;
            }
            return future;
        }), "readFileRange", path, 0);
    }

    /**
     * Writes an {@link InputStream} as a multipart file, splitting into blocks of {@code blockSize}.
     * Uses a pooled heap ByteBuf internally to avoid per-block allocations.
     * Blocks are written sequentially. Returns the total number of bytes written.
     */
    public long writeMultipartFile(String path, InputStream in, int blockSize) throws IOException {
        long blockIndex = 0;
        long totalBytes = 0;
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(blockSize, blockSize);
        try {
            byte[] backingArray = buf.array();
            int startOffset = buf.arrayOffset();
            int read;
            while ((read = readFully(in, backingArray, startOffset, blockSize)) > 0) {
                buf.setIndex(0, read); // set readerIndex=0, writerIndex=read
                totalBytes += read;
                getUnchecked(writeFileBlockAsync(path, blockIndex, buf)); // synchronous via getUnchecked
                buf.clear(); // reset for next iteration; safe because writeFileBlockAsync is blocking
                blockIndex++;
            }
        } finally {
            buf.release();
        }
        return totalBytes;
    }

    private static int readFully(InputStream in, byte[] buf, int offset, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int read = in.read(buf, offset + total, len - total);
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
        return runDetached(() -> {
            ServerSnapshot s = this.snapshot;
            List<CompletableFuture<Integer>> futures = new ArrayList<>();
            for (String server : s.writeChannels.keySet()) {
                RemoteFileServiceGrpc.RemoteFileServiceStub stub = writeStubForServer(server);
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

    public void writeFile(String path, ByteBuf content) {
        getUnchecked(writeFileAsync(path, content));
    }

    public void writeFileBlock(String path, long blockIndex, ByteBuf content) {
        getUnchecked(writeFileBlockAsync(path, blockIndex, content));
    }

    /**
     * Reads a file into a pooled ByteBuf. Returns null if file not found.
     * Caller must release the ByteBuf after use.
     */
    public ByteBuf readFileAsByteBuf(String path) {
        return getUnchecked(readFileAsByteBufAsync(path));
    }

    public byte[] readFileRange(String path, long offset, int length, int blockSize) {
        return getUnchecked(readFileRangeAsync(path, offset, length, blockSize));
    }

    /**
     * Synchronous variant of {@link #readFileRangeAsByteBufAsync}.
     * Caller MUST {@code release()} the returned buffer when done.
     */
    public ByteBuf readFileRangeAsByteBuf(String path, long offset, int length, int blockSize) {
        return getUnchecked(readFileRangeAsByteBufAsync(path, offset, length, blockSize));
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
        shutdownChannels(s.readChannels, "read");
        shutdownChannels(s.writeChannels, "write");
    }

    private static void shutdownChannels(Map<String, ManagedChannel> channels, String planeName) {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            try {
                entry.getValue().shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING,
                        "Interrupted while shutting down " + planeName + " channel to " + entry.getKey(), e);
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
            // Log the full Throwable so the cause chain is visible: gRPC can
            // swallow the original failure as a generic "CANCELLED: Failed to
            // read message." when a StreamObserver throws, and dropping the
            // cause here would hide the real root cause (e.g. OutOfDirectMemoryError).
            LogRecord record = new LogRecord(Level.INFO,
                    "remote file " + opName + " retry " + nextAttempt + "/" + maxRetries
                            + " for path " + path + " after " + delayMs + "ms (error: "
                            + error + ")");
            record.setThrown(error);
            record.setLoggerName(LOGGER.getName());
            LOGGER.log(record);
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
