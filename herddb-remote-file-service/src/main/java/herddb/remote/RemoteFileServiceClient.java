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

import herddb.auth.oidc.sasl.OAuthBearerSaslClient;
import herddb.network.Channel;
import herddb.network.ChannelEventListener;
import herddb.network.netty.NettyConnector;
import herddb.network.netty.NetworkUtils;
import herddb.proto.Pdu;
import herddb.proto.PduCodec;
import herddb.server.RemoteFileClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Native-Netty client for the file service (issue #425).
 *
 * <p>Manages one read-plane and one write-plane {@link Channel} per file
 * server endpoint, distributes requests across servers via
 * {@link ConsistentHashRouter}, and exposes both blocking and async
 * (CompletableFuture) APIs that match the previous gRPC client one-for-one
 * so existing callers do not need to change.
 *
 * <p>Lane separation (issue #100) is preserved: read-plane and write-plane
 * Channels are independent TCP connections so a slow checkpoint write
 * cannot starve hot-path reads.
 *
 * <p>Authentication is OIDC OAUTHBEARER (issue #425). When the caller
 * supplies a {@code Supplier<String>} bearer-token source, the client
 * performs a SASL handshake on every freshly-opened {@link Channel}
 * before issuing any data-plane RPCs. With no token supplier the client
 * connects in plaintext and assumes the server has OIDC disabled.
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
     * {@code readFile}/{@code readFileRange} calls whose payloads are
     * currently being staged into a pooled direct {@link ByteBuf}.
     */
    public static final String CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES =
            "remote.file.client.max.inflight.read.bytes";
    /**
     * Configuration key for the maximum number of bytes across all in-flight
     * {@code writeFile}/{@code writeFileBlock}/{@code writeMultipartFile}
     * calls whose payloads are currently being staged into the write-plane
     * Netty channel. Symmetric to
     * {@link #CONFIG_CLIENT_MAX_INFLIGHT_READ_BYTES} but for the write side
     * (issue #468). Acquired before each block-async call is launched and
     * released when the corresponding future completes; bounds peak
     * write-plane network pressure so a multipart compaction write cannot
     * fan out hundreds of in-flight blocks at once and starve concurrent
     * reads on the shared event-loop pool / file server.
     */
    public static final String CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES =
            "remote.file.client.max.inflight.write.bytes";

    private static final long DEFAULT_CLIENT_TIMEOUT_SECONDS = 1800; // 30 minutes
    private static final int DEFAULT_CLIENT_RETRIES = 10;
    /** Default multipart block size: 4 MB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;
    /** Default cap on in-flight read bytes: 256 MiB (issue #246). */
    public static final long DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES = 256L * 1024 * 1024;
    /** Default cap on in-flight write bytes: 256 MiB (issue #468). */
    public static final long DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES = 256L * 1024 * 1024;
    /**
     * Emit a WARNING log line if acquiring the in-flight reservation
     * blocks for longer than this threshold.
     */
    private static final long PERMIT_ACQUIRE_WARN_THRESHOLD_MS = 500;

    /** Connection establishment timeout (sockets only): 10 s. */
    private static final int CONNECT_TIMEOUT_MS = 10_000;

    private static class ServerSnapshot {
        final ConsistentHashRouter router;
        /**
         * Read-plane channels, one per server. Keyed by server id. Separate
         * from {@link #writeChannels} so read and write RPCs sit on
         * independent TCP connections and cannot share resources (issue #100).
         */
        final Map<String, ServerChannel> readChannels;
        /** Write-plane channels. Same keys as {@link #readChannels}. */
        final Map<String, ServerChannel> writeChannels;

        ServerSnapshot(ConsistentHashRouter router,
                       Map<String, ServerChannel> readChannels,
                       Map<String, ServerChannel> writeChannels) {
            this.router = router;
            this.readChannels = Collections.unmodifiableMap(new LinkedHashMap<>(readChannels));
            this.writeChannels = Collections.unmodifiableMap(new LinkedHashMap<>(writeChannels));
        }
    }

    private volatile ServerSnapshot snapshot;
    private final CountDownLatch serversReadyLatch = new CountDownLatch(1);
    private final int maxRetries;
    private final long clientTimeoutSeconds;
    private final int blockSize;
    private final long maxInflightReadBytes;
    private final Semaphore inflightReadBytes;
    private final long maxInflightWriteBytes;
    private final Semaphore inflightWriteBytes;
    private final ScheduledExecutorService retryScheduler;
    private final Supplier<String> oidcTokenSupplier;

    // Netty plumbing reused across all ServerChannels.
    private final MultithreadEventLoopGroup eventLoopGroup;
    private final ExecutorService callbackExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public RemoteFileServiceClient(List<String> servers) {
        this(servers, Collections.emptyMap(), null);
    }

    public RemoteFileServiceClient(List<String> servers, Map<String, Object> configuration) {
        this(servers, configuration, null);
    }

    /**
     * @param servers           initial list of {@code host:port} server addresses; may be empty
     *                          for cold-start ZK discovery (see {@link #updateServers(List)}).
     * @param configuration     CONFIG_CLIENT_* tuning knobs; never {@code null}.
     * @param oidcTokenSupplier when non-null, every freshly-opened channel
     *                          performs an OAUTHBEARER SASL handshake using
     *                          the token returned by the supplier; the
     *                          supplier is invoked again on every new
     *                          channel so it can return rotated tokens.
     *                          When {@code null}, channels connect in
     *                          plaintext and the server must have OIDC
     *                          disabled.
     */
    public RemoteFileServiceClient(List<String> servers, Map<String, Object> configuration,
                                   Supplier<String> oidcTokenSupplier) {
        this.oidcTokenSupplier = oidcTokenSupplier;
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
        int permits = maxInflightReadBytes > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) maxInflightReadBytes;
        this.inflightReadBytes = new Semaphore(permits);
        long configuredMaxInflightWriteBytes = longConfig(configuration,
                CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES);
        if (configuredMaxInflightWriteBytes <= 0) {
            throw new IllegalArgumentException(CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES
                    + " must be > 0, got " + configuredMaxInflightWriteBytes);
        }
        if (configuredMaxInflightWriteBytes < this.blockSize) {
            throw new IllegalArgumentException(CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES
                    + " (" + configuredMaxInflightWriteBytes + ") must be >= "
                    + CONFIG_CLIENT_BLOCK_SIZE + " (" + this.blockSize
                    + ") so a single full-block write is always admissible");
        }
        this.maxInflightWriteBytes = configuredMaxInflightWriteBytes;
        int writePermits = maxInflightWriteBytes > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) maxInflightWriteBytes;
        this.inflightWriteBytes = new Semaphore(writePermits);
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "remote-file-retry");
            t.setDaemon(true);
            return t;
        });
        this.eventLoopGroup = buildEventLoopGroup();
        this.callbackExecutor = buildCallbackExecutor();

        Map<String, ServerChannel> readChannels = new LinkedHashMap<>();
        Map<String, ServerChannel> writeChannels = new LinkedHashMap<>();
        for (String server : servers) {
            readChannels.put(server, new ServerChannel(server, "read"));
            writeChannels.put(server, new ServerChannel(server, "write"));
        }
        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(servers), readChannels, writeChannels);
        if (!servers.isEmpty()) {
            this.serversReadyLatch.countDown();
        }
        if (servers.isEmpty()) {
            LOGGER.log(Level.INFO,
                    "RemoteFileServiceClient: starting with empty server list (awaiting ZK discovery), "
                            + "timeout={0}s, retries={1}, maxInflightReadBytes={2}, "
                            + "maxInflightWriteBytes={3}",
                    new Object[]{clientTimeoutSeconds, maxRetries, maxInflightReadBytes,
                            maxInflightWriteBytes});
        } else {
            LOGGER.log(Level.INFO,
                    "RemoteFileServiceClient: servers={0}, timeout={1}s, retries={2}, "
                            + "maxInflightReadBytes={3}, maxInflightWriteBytes={4}",
                    new Object[]{servers, clientTimeoutSeconds, maxRetries, maxInflightReadBytes,
                            maxInflightWriteBytes});
        }
    }

    private static MultithreadEventLoopGroup buildEventLoopGroup() {
        ThreadFactory threadFactory = r -> new FastThreadLocalThread(r,
                "remote-file-client-io-" + System.identityHashCode(r));
        if (NetworkUtils.isEnableEpoolNative()) {
            return new EpollEventLoopGroup(0, threadFactory);
        }
        return new NioEventLoopGroup(0, threadFactory);
    }

    private static ExecutorService buildCallbackExecutor() {
        AtomicInteger ctr = new AtomicInteger();
        ThreadFactory threadFactory = r -> {
            Thread t = new FastThreadLocalThread(r, "remote-file-client-cb-" + ctr.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return Executors.newCachedThreadPool(threadFactory);
    }

    public long maxInflightReadBytes() {
        return maxInflightReadBytes;
    }

    public long availableInflightReadBytes() {
        return inflightReadBytes.availablePermits();
    }

    public long maxInflightWriteBytes() {
        return maxInflightWriteBytes;
    }

    public long availableInflightWriteBytes() {
        return inflightWriteBytes.availablePermits();
    }

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

    private void releaseInflightReadBytes(int bytes) {
        inflightReadBytes.release(bytes);
    }

    private void acquireInflightWriteBytes(int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0, got " + bytes);
        }
        if (inflightWriteBytes.tryAcquire(bytes)) {
            return;
        }
        long startNanos = System.nanoTime();
        inflightWriteBytes.acquireUninterruptibly(bytes);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (elapsedMs >= PERMIT_ACQUIRE_WARN_THRESHOLD_MS) {
            LOGGER.log(Level.WARNING,
                    "remote file client inflight-write reservation blocked for {0}ms "
                            + "(requested={1} bytes, available={2}/{3}); consider raising "
                            + CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES
                            + " or reducing concurrent compaction/page-write load",
                    new Object[]{elapsedMs, bytes, inflightWriteBytes.availablePermits(),
                            maxInflightWriteBytes});
        }
    }

    private void releaseInflightWriteBytes(int bytes) {
        inflightWriteBytes.release(bytes);
    }

    /**
     * Acquires {@code bytes} from the in-flight write-bytes budget and
     * returns an idempotent {@link Runnable} that releases the same number
     * of permits exactly once. Callers wire the runnable into the
     * {@code whenComplete} hook of the future returned by
     * {@code sendRequest}, and into every synchronous failure branch (e.g.
     * {@code writeChannelForBlock} throwing) so the reservation is always
     * returned. Empty payloads ({@code bytes == 0}) skip both the acquire
     * and the release — {@link #writeAsMultipart} legitimately uses
     * {@code Unpooled.EMPTY_BUFFER} to materialise an empty file marker
     * and would otherwise be rejected by {@link #acquireInflightWriteBytes}.
     */
    private Runnable reserveInflightWriteBytes(int bytes) {
        if (bytes <= 0) {
            return () -> { };
        }
        acquireInflightWriteBytes(bytes);
        AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                releaseInflightWriteBytes(bytes);
            }
        };
    }

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
        statsLogger.registerGauge("inflight_write_bytes_available", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return availableInflightWriteBytes();
            }
        });
        statsLogger.registerGauge("inflight_write_bytes_max", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return maxInflightWriteBytes();
            }
        });
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

        Map<String, ServerChannel> newRead = new LinkedHashMap<>();
        Map<String, ServerChannel> newWrite = new LinkedHashMap<>();
        for (String server : newServers) {
            ServerChannel existingRead = current.readChannels.get(server);
            ServerChannel existingWrite = current.writeChannels.get(server);
            newRead.put(server, existingRead != null ? existingRead : new ServerChannel(server, "read"));
            newWrite.put(server, existingWrite != null ? existingWrite : new ServerChannel(server, "write"));
        }

        this.snapshot = new ServerSnapshot(new ConsistentHashRouter(newServers), newRead, newWrite);
        serversReadyLatch.countDown();
        LOGGER.log(Level.INFO, "Updated remote file servers: {0} (added: {1}, removed: {2})",
                new Object[]{newServers, added, removed});

        if (!removed.isEmpty()) {
            // Detached close so we do not block updateServers() on socket teardown.
            Thread t = new Thread(() -> {
                for (String server : removed) {
                    ServerChannel rc = current.readChannels.get(server);
                    if (rc != null) {
                        rc.closeQuiet();
                    }
                    ServerChannel wc = current.writeChannels.get(server);
                    if (wc != null) {
                        wc.closeQuiet();
                    }
                }
            }, "remote-file-channel-shutdown");
            t.setDaemon(true);
            t.start();
        }
    }

    public boolean hasServers() {
        ServerSnapshot s = this.snapshot;
        return !s.readChannels.isEmpty();
    }

    public boolean awaitServersReady(long timeoutMs) throws InterruptedException {
        return serversReadyLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Read-plane connection for a path. */
    private ServerChannel readChannelForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return s.readChannels.get(server);
    }

    /** Write-plane connection for a path. */
    private ServerChannel writeChannelForPath(String path) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(path);
        return s.writeChannels.get(server);
    }

    /** Read-plane connection for a multipart block. */
    private ServerChannel readChannelForBlock(String path, long blockIndex) {
        return readChannelForKey(blockRoutingKey(path, blockIndex));
    }

    /** Write-plane connection for a multipart block. */
    private ServerChannel writeChannelForBlock(String path, long blockIndex) {
        return writeChannelForKey(blockRoutingKey(path, blockIndex));
    }

    private ServerChannel readChannelForKey(String key) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(key);
        return s.readChannels.get(server);
    }

    private ServerChannel writeChannelForKey(String key) {
        ServerSnapshot s = this.snapshot;
        String server = s.router.getServer(key);
        return s.writeChannels.get(server);
    }

    private static String blockRoutingKey(String path, long blockIndex) {
        return path + "#block" + blockIndex;
    }

    public String getServerForPath(String path) {
        return snapshot.router.getServer(path);
    }

    public String getServerForBlock(String path, long blockIndex) {
        return snapshot.router.getServer(blockRoutingKey(path, blockIndex));
    }

    public int getBlockSize() {
        return blockSize;
    }

    // ---------------------------------------------------------------------
    // Async APIs returning CompletableFuture
    // ---------------------------------------------------------------------

    public CompletableFuture<Long> writeFileAsync(String path, byte[] content) {
        return writeFileAsync(path, content, 0, content.length);
    }

    public CompletableFuture<Long> writeFileAsync(String path, byte[] buf, int offset, int length) {
        // Writes are not idempotent — no retry.
        Runnable releaseOnce = reserveInflightWriteBytes(length);
        ServerChannel ch;
        try {
            ch = writeChannelForPath(path);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<Long> result = sendRequest(ch, requestId ->
                        PduCodec.WriteFileRequest.write(requestId, path, buf, offset, length),
                pdu -> PduCodec.WriteFileResponse.readWrittenSize(pdu));
        return result.whenComplete((written, err) -> releaseOnce.run());
    }

    /**
     * Writes a file from a pooled ByteBuf. Caller still owns {@code content}
     * and must release after this future completes.
     */
    public CompletableFuture<Long> writeFileAsync(String path, ByteBuf content) {
        Runnable releaseOnce = reserveInflightWriteBytes(content.readableBytes());
        ServerChannel ch;
        try {
            ch = writeChannelForPath(path);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<Long> result = sendRequest(ch, requestId ->
                        PduCodec.WriteFileRequest.write(requestId, path, content),
                pdu -> PduCodec.WriteFileResponse.readWrittenSize(pdu));
        return result.whenComplete((written, err) -> releaseOnce.run());
    }

    public CompletableFuture<byte[]> readFileAsync(String path) {
        return retryAsync(() -> doReadFileAsync(path), "readFile", path, 0);
    }

    private CompletableFuture<byte[]> doReadFileAsync(String path) {
        ServerChannel ch;
        try {
            ch = readChannelForPath(path);
        } catch (RuntimeException e) {
            return failed(e);
        }
        return sendRequest(ch, requestId -> PduCodec.ReadFileRequest.write(requestId, path),
                pdu -> {
                    if (!PduCodec.ReadFileResponse.readFound(pdu)) {
                        return null;
                    }
                    int len = PduCodec.ReadFileResponse.readContentLength(pdu);
                    byte[] out = new byte[len];
                    if (len > 0) {
                        ByteBuf slice = PduCodec.ReadFileResponse.readContent(pdu);
                        try {
                            slice.readBytes(out);
                        } finally {
                            slice.release();
                        }
                    }
                    return out;
                });
    }

    /**
     * Reads a file into a pooled ByteBuf. Returns null if the file is not
     * found. Caller must release the ByteBuf after use.
     */
    public CompletableFuture<ByteBuf> readFileAsByteBufAsync(String path) {
        return retryAsync(() -> doReadFileAsByteBufAsync(path), "readFile", path, 0);
    }

    private CompletableFuture<ByteBuf> doReadFileAsByteBufAsync(String path) {
        // readFile returns the whole file in a single response, whose size
        // is unknown until the server replies. Reserve blockSize bytes as a
        // conservative proxy: readFile is a legacy small-object API.
        int reservation = blockSize;
        acquireInflightReadBytes(reservation);
        AtomicBoolean reservationReleased = new AtomicBoolean(false);
        Runnable releaseOnce = () -> {
            if (reservationReleased.compareAndSet(false, true)) {
                releaseInflightReadBytes(reservation);
            }
        };
        ServerChannel ch;
        try {
            ch = readChannelForPath(path);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<ByteBuf> result = sendRequest(ch,
                requestId -> PduCodec.ReadFileRequest.write(requestId, path),
                pdu -> {
                    if (!PduCodec.ReadFileResponse.readFound(pdu)) {
                        return (ByteBuf) null;
                    }
                    int len = PduCodec.ReadFileResponse.readContentLength(pdu);
                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
                    if (len > 0) {
                        ByteBuf slice = PduCodec.ReadFileResponse.readContent(pdu);
                        try {
                            buf.writeBytes(slice);
                        } finally {
                            slice.release();
                        }
                    }
                    return buf;
                });
        return result.whenComplete((buf, err) -> releaseOnce.run());
    }

    public CompletableFuture<Boolean> deleteFileAsync(String path) {
        return retryAsync(() -> doDeleteFileAsync(path), "deleteFile", path, 0);
    }

    private CompletableFuture<Boolean> doDeleteFileAsync(String path) {
        ServerChannel ch;
        try {
            ch = writeChannelForPath(path);
        } catch (RuntimeException e) {
            return failed(e);
        }
        return sendRequest(ch, requestId -> PduCodec.DeleteFileRequest.write(requestId, path),
                pdu -> PduCodec.DeleteFileResponse.readDeleted(pdu));
    }

    public CompletableFuture<List<String>> listFilesAsync(String prefix) {
        return retryAsync(() -> doListFilesAsync(prefix), "listFiles", prefix, 0);
    }

    private CompletableFuture<List<String>> doListFilesAsync(String prefix) {
        ServerSnapshot s = this.snapshot;
        if (s.readChannels.isEmpty()) {
            return failed(new IllegalStateException("no servers configured"));
        }
        List<CompletableFuture<List<String>>> perServer = new ArrayList<>(s.readChannels.size());
        for (ServerChannel ch : s.readChannels.values()) {
            perServer.add(sendRequest(ch, requestId -> PduCodec.ListFilesRequest.write(requestId, prefix),
                    pdu -> PduCodec.ListFilesResponse.readPaths(pdu)));
        }
        return CompletableFuture.allOf(perServer.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    // Multiple servers may return the same logical path for
                    // different blocks of the same multipart file; dedupe.
                    LinkedHashSet<String> seen = new LinkedHashSet<>();
                    for (CompletableFuture<List<String>> f : perServer) {
                        seen.addAll(f.join());
                    }
                    return new ArrayList<>(seen);
                });
    }

    /**
     * Writes one block of a multipart file. Not retried (not idempotent).
     *
     * <p>Acquires the in-flight write-bytes reservation (issue #468) before
     * launching the RPC and releases it when the returned future completes,
     * regardless of outcome. The acquisition is synchronous: callers fanning
     * many blocks out at once will block here when the cap is hit, providing
     * natural backpressure that protects concurrent reads on the shared
     * event-loop pool.
     */
    public CompletableFuture<Void> writeFileBlockAsync(String path, long blockIndex, byte[] content) {
        Runnable releaseOnce = reserveInflightWriteBytes(content.length);
        ServerChannel ch;
        try {
            ch = writeChannelForBlock(path, blockIndex);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<Void> result = sendRequest(ch,
                requestId -> PduCodec.WriteFileBlockRequest.write(requestId, path, blockIndex, content),
                pdu -> {
                    PduCodec.WriteFileBlockResponse.readWrittenSize(pdu); // ignored
                    return (Void) null;
                });
        return result.whenComplete((v, err) -> releaseOnce.run());
    }

    /**
     * Writes one block of a multipart file from a ByteBuf. Not retried.
     * Caller still owns {@code content} and must release after completion.
     * Acquires/releases the in-flight write-bytes reservation (issue #468)
     * symmetrically to the {@code byte[]} overload.
     */
    public CompletableFuture<Void> writeFileBlockAsync(String path, long blockIndex, ByteBuf content) {
        Runnable releaseOnce = reserveInflightWriteBytes(content.readableBytes());
        ServerChannel ch;
        try {
            ch = writeChannelForBlock(path, blockIndex);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<Void> result = sendRequest(ch,
                requestId -> PduCodec.WriteFileBlockRequest.write(requestId, path, blockIndex, content),
                pdu -> {
                    PduCodec.WriteFileBlockResponse.readWrittenSize(pdu);
                    return (Void) null;
                });
        return result.whenComplete((v, err) -> releaseOnce.run());
    }

    public CompletableFuture<byte[]> readFileRangeAsync(String path, long offset, int length, int blockSizeArg) {
        // Per the storage-side contract a single readRange call must not span two
        // blocks. Split cross-block requests into sequential per-block reads and
        // concatenate the results — the same shape the legacy gRPC client used.
        long startBlock = offset / blockSizeArg;
        long endBlock = (offset + length - 1) / blockSizeArg;
        if (startBlock == endBlock) {
            return retryAsync(() -> doReadFileRangeAsync(path, offset, length, blockSizeArg),
                    "readFileRange", path, 0);
        }
        int firstBlockEnd = (int) ((startBlock + 1) * (long) blockSizeArg - offset);
        int secondLength = length - firstBlockEnd;
        long secondOffset = (startBlock + 1) * (long) blockSizeArg;
        CompletableFuture<byte[]> firstFuture = retryAsync(
                () -> doReadFileRangeAsync(path, offset, firstBlockEnd, blockSizeArg),
                "readFileRange", path, 0);
        return firstFuture.thenCompose(first -> {
            if (first == null) {
                return CompletableFuture.completedFuture((byte[]) null);
            }
            CompletableFuture<byte[]> secondFuture = retryAsync(
                    () -> doReadFileRangeAsync(path, secondOffset, secondLength, blockSizeArg),
                    "readFileRange", path, 0);
            return secondFuture.thenApply(second -> {
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

    private CompletableFuture<byte[]> doReadFileRangeAsync(String path, long offset, int length, int blockSizeArg) {
        ServerChannel ch;
        try {
            ch = readChannelForBlock(path, offset / blockSizeArg);
        } catch (RuntimeException e) {
            return failed(e);
        }
        return sendRequest(ch,
                requestId -> PduCodec.ReadFileRangeRequest.write(requestId, path, offset, length, blockSizeArg),
                pdu -> {
                    if (!PduCodec.ReadFileRangeResponse.readFound(pdu)) {
                        return (byte[]) null;
                    }
                    int len = PduCodec.ReadFileRangeResponse.readContentLength(pdu);
                    byte[] out = new byte[len];
                    if (len > 0) {
                        ByteBuf slice = PduCodec.ReadFileRangeResponse.readContent(pdu);
                        try {
                            slice.readBytes(out);
                        } finally {
                            slice.release();
                        }
                    }
                    return out;
                });
    }

    public CompletableFuture<ByteBuf> readFileRangeAsByteBufAsync(String path, long offset,
                                                                  int length, int blockSizeArg) {
        long startBlock = offset / blockSizeArg;
        long endBlock = (offset + length - 1) / blockSizeArg;
        if (startBlock == endBlock) {
            return retryAsync(() -> doReadFileRangeAsByteBufAsync(path, offset, length, blockSizeArg),
                    "readFileRange", path, 0);
        }
        int firstBlockEnd = (int) ((startBlock + 1) * (long) blockSizeArg - offset);
        int secondLength = length - firstBlockEnd;
        long secondOffset = (startBlock + 1) * (long) blockSizeArg;
        CompletableFuture<ByteBuf> firstFuture = retryAsync(
                () -> doReadFileRangeAsByteBufAsync(path, offset, firstBlockEnd, blockSizeArg),
                "readFileRange", path, 0);
        return firstFuture.thenCompose(first -> {
            if (first == null) {
                return CompletableFuture.completedFuture((ByteBuf) null);
            }
            CompletableFuture<ByteBuf> secondFuture = retryAsync(
                    () -> doReadFileRangeAsByteBufAsync(path, secondOffset, secondLength, blockSizeArg),
                    "readFileRange", path, 0);
            // Use .handle() so one branch covers BOTH the secondFuture failure
            // and any throw from the composite-assembly lambda. Mixing
            // thenApply().exceptionally() risks double-releasing `first` when
            // a lambda partially built the composite (transferring `first`
            // into it) and then threw — the .exceptionally would then release
            // `first` a second time after composite.release() already did.
            return secondFuture.handle((second, error) -> {
                if (error != null) {
                    // The second read failed before we touched the composite —
                    // `first` is still solely owned by us, so release it.
                    ReferenceCountUtil.safeRelease(first);
                    if (error instanceof RuntimeException) {
                        throw (RuntimeException) error;
                    }
                    throw new RuntimeException(error);
                }
                if (second == null) {
                    return first;
                }
                // Refcount-safe composite assembly: `composite` is non-null
                // only between creation and the point where ownership
                // transfers to the caller. `firstTransferred`/`secondTransferred`
                // track which components have been moved into the composite.
                // On partial failure, releasing the composite releases the
                // already-transferred components; not-yet-transferred ones
                // are released independently. Setting `composite = null`
                // before return signals that ownership has moved out.
                CompositeByteBuf composite = null;
                boolean firstTransferred = false;
                boolean secondTransferred = false;
                try {
                    composite = PooledByteBufAllocator.DEFAULT.compositeDirectBuffer(2);
                    composite.addComponent(true, first);
                    firstTransferred = true;
                    composite.addComponent(true, second);
                    secondTransferred = true;
                    ByteBuf result = composite;
                    composite = null;
                    return result;
                } finally {
                    if (composite != null) {
                        ReferenceCountUtil.safeRelease(composite);
                        if (!firstTransferred) {
                            ReferenceCountUtil.safeRelease(first);
                        }
                        if (!secondTransferred) {
                            ReferenceCountUtil.safeRelease(second);
                        }
                    }
                }
            });
        });
    }

    private CompletableFuture<ByteBuf> doReadFileRangeAsByteBufAsync(String path, long offset,
                                                                     int length, int blockSizeArg) {
        // Reserve precisely the requested length (issue #246).
        int reservation = length;
        acquireInflightReadBytes(reservation);
        AtomicBoolean reservationReleased = new AtomicBoolean(false);
        Runnable releaseOnce = () -> {
            if (reservationReleased.compareAndSet(false, true)) {
                releaseInflightReadBytes(reservation);
            }
        };
        ServerChannel ch;
        try {
            ch = readChannelForBlock(path, offset / blockSizeArg);
        } catch (RuntimeException e) {
            releaseOnce.run();
            return failed(e);
        }
        CompletableFuture<ByteBuf> result = sendRequest(ch,
                requestId -> PduCodec.ReadFileRangeRequest.write(requestId, path, offset, length, blockSizeArg),
                pdu -> {
                    if (!PduCodec.ReadFileRangeResponse.readFound(pdu)) {
                        return (ByteBuf) null;
                    }
                    int len = PduCodec.ReadFileRangeResponse.readContentLength(pdu);
                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(len);
                    if (len > 0) {
                        ByteBuf slice = PduCodec.ReadFileRangeResponse.readContent(pdu);
                        try {
                            buf.writeBytes(slice);
                        } finally {
                            slice.release();
                        }
                    }
                    return buf;
                });
        return result.whenComplete((buf, err) -> releaseOnce.run());
    }

    /**
     * Streaming multipart write helper. Reads from {@code in} in
     * {@code blockSizeArg}-sized blocks and dispatches each as a parallel
     * {@link #writeFileBlockAsync(String, long, byte[])} call. Returns the
     * total bytes written.
     */
    public long writeMultipartFile(String path, InputStream in, int blockSizeArg) throws IOException {
        long total = 0;
        long blockIndex = 0;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        byte[] buf = new byte[blockSizeArg];
        while (true) {
            int read = readFully(in, buf, 0, blockSizeArg);
            if (read <= 0) {
                break;
            }
            byte[] block = new byte[read];
            System.arraycopy(buf, 0, block, 0, read);
            futures.add(writeFileBlockAsync(path, blockIndex, block));
            blockIndex++;
            total += read;
            if (read < blockSizeArg) {
                break;
            }
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing multipart " + path, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("multipart write failed for " + path, cause);
        }
        return total;
    }

    private static int readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int read = in.read(buf, off + total, len - total);
            if (read < 0) {
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
        if (s.writeChannels.isEmpty()) {
            return failed(new IllegalStateException("no servers configured"));
        }
        List<CompletableFuture<Integer>> perServer = new ArrayList<>(s.writeChannels.size());
        for (ServerChannel ch : s.writeChannels.values()) {
            perServer.add(sendRequest(ch,
                    requestId -> PduCodec.DeleteByPrefixRequest.write(requestId, prefix),
                    pdu -> PduCodec.DeleteByPrefixResponse.readDeletedCount(pdu)));
        }
        return CompletableFuture.allOf(perServer.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    int total = 0;
                    for (CompletableFuture<Integer> f : perServer) {
                        total += f.join();
                    }
                    return total;
                });
    }

    public CompletableFuture<Integer> deleteFilesAsync(List<String> paths) {
        if (paths == null || paths.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        return retryAsync(() -> doDeleteFilesAsync(paths), "deleteFiles", paths.get(0), 0);
    }

    private CompletableFuture<Integer> doDeleteFilesAsync(List<String> paths) {
        if (paths.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        ServerSnapshot s = this.snapshot;
        if (s.writeChannels.isEmpty()) {
            return failed(new IllegalStateException("no servers configured"));
        }
        // Group by routing key so each batch goes to exactly one server.
        Map<String, List<String>> grouped = new HashMap<>();
        for (String p : paths) {
            String server = s.router.getServer(p);
            grouped.computeIfAbsent(server, k -> new ArrayList<>()).add(p);
        }
        List<CompletableFuture<Integer>> perServer = new ArrayList<>(grouped.size());
        for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
            ServerChannel ch = s.writeChannels.get(entry.getKey());
            if (ch == null) {
                continue;
            }
            final List<String> batch = entry.getValue();
            perServer.add(sendRequest(ch,
                    requestId -> PduCodec.DeleteFilesRequest.write(requestId, batch),
                    pdu -> {
                        int deleted = 0;
                        for (PduCodec.DeleteFilesResponse.Outcome o
                                : PduCodec.DeleteFilesResponse.readOutcomes(pdu)) {
                            if (o.deleted) {
                                deleted++;
                            }
                        }
                        return deleted;
                    }));
        }
        // Partial-success semantics: if at least one sub-batch succeeded,
        // surface the sum of successful sub-batches even when others failed.
        // Only when EVERY sub-batch fails do we propagate the failure (so
        // the retry layer kicks in). Mirrors the issue #398 contract: the
        // deleteFiles call is best-effort and the count is what was
        // confirmed deleted, so callers drive retry by counting deletions.
        return CompletableFuture.allOf(perServer.toArray(new CompletableFuture[0]))
                .handle((v, error) -> {
                    int total = 0;
                    int succeeded = 0;
                    Throwable lastError = null;
                    for (CompletableFuture<Integer> f : perServer) {
                        if (f.isCompletedExceptionally()) {
                            lastError = extractFailure(f);
                            continue;
                        }
                        try {
                            total += f.getNow(0);
                            succeeded++;
                        } catch (RuntimeException e) {
                            // Defensive: getNow on a non-completed future is impossible
                            // here (allOf has fired), but we keep tallying.
                            lastError = e;
                        }
                    }
                    if (succeeded == 0 && lastError != null) {
                        throw new CompletionException(lastError);
                    }
                    return total;
                });
    }

    private static Throwable extractFailure(CompletableFuture<?> f) {
        try {
            f.getNow(null);
            return null;
        } catch (CompletionException ce) {
            return ce.getCause() != null ? ce.getCause() : ce;
        } catch (RuntimeException e) {
            return e;
        }
    }

    public int deleteFiles(List<String> paths) {
        return getUnchecked(deleteFilesAsync(paths));
    }

    // ---------------------------------------------------------------------
    // Sync wrappers
    // ---------------------------------------------------------------------

    @Override
    public void writeFile(String path, byte[] content) {
        getUnchecked(writeFileAsync(path, content));
    }

    public void writeFile(String path, byte[] buf, int offset, int length) {
        getUnchecked(writeFileAsync(path, buf, offset, length));
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

    public ByteBuf readFileAsByteBuf(String path) {
        return getUnchecked(readFileAsByteBufAsync(path));
    }

    public byte[] readFileRange(String path, long offset, int length, int blockSizeArg) {
        return getUnchecked(readFileRangeAsync(path, offset, length, blockSizeArg));
    }

    public ByteBuf readFileRangeAsByteBuf(String path, long offset, int length, int blockSizeArg) {
        return getUnchecked(readFileRangeAsByteBufAsync(path, offset, length, blockSizeArg));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ServerSnapshot s = this.snapshot;
        for (ServerChannel ch : s.readChannels.values()) {
            ch.closeQuiet();
        }
        for (ServerChannel ch : s.writeChannels.values()) {
            ch.closeQuiet();
        }
        retryScheduler.shutdownNow();
        eventLoopGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        callbackExecutor.shutdown();
        try {
            callbackExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------------------------------------------------------------
    // Per-server channel: connects on demand, performs SASL handshake,
    // reconnects on failure, serializes a single in-flight Channel reference.
    // ---------------------------------------------------------------------

    private final class ServerChannel {
        private final String server;
        private final String plane; // "read" or "write" (logging only)
        private final Object lock = new Object();
        private volatile Channel current;
        private volatile boolean closed;

        ServerChannel(String server, String plane) {
            this.server = server;
            this.plane = plane;
        }

        /**
         * Returns the currently-open Channel, lazily connecting and
         * authenticating if needed. Reconnects when the previous Channel
         * has gone invalid or was closed by the peer.
         *
         * <p>The fast-path read of {@link #current} is volatile-only
         * (no lock acquired) so an authenticated channel is reused without
         * contention. The slow path serializes one connect attempt at a
         * time; concurrent callers wait for the in-flight attempt rather
         * than launching their own reconnect storm. The connect+SASL
         * handshake itself runs under the per-server monitor for code
         * simplicity — switching to a {@code CompletableFuture<Channel>}
         * pattern that releases the monitor while the handshake is in
         * flight is the natural next step if connect-time contention
         * shows up in profiles.
         */
        Channel get() throws IOException {
            Channel c = this.current;
            if (c != null && c.isValid() && !c.isClosed()) {
                return c;
            }
            synchronized (lock) {
                if (closed) {
                    throw new IOException("ServerChannel " + server + "/" + plane + " is closed");
                }
                c = this.current;
                if (c != null && c.isValid() && !c.isClosed()) {
                    return c;
                }
                if (c != null) {
                    safeClose(c);
                    this.current = null;
                }
                c = openAndAuthenticate();
                this.current = c;
                return c;
            }
        }

        private Channel openAndAuthenticate() throws IOException {
            String[] parts = server.split(":", 2);
            if (parts.length != 2) {
                throw new IOException("Bad server address (expected host:port): " + server);
            }
            String host = parts[0];
            int port;
            try {
                port = Integer.parseInt(parts[1]);
            } catch (NumberFormatException e) {
                throw new IOException("Bad server port: " + server, e);
            }
            ChannelEventListener listener = new ChannelEventListener() {
                // Default no-op; pdu callbacks are correlated by messageId
                // through Channel's own request/response infrastructure, and
                // unsolicited inbound PDUs are not expected from the server.
            };
            int socketTimeoutSeconds = 0; // disabled — request-level timeouts are enforced via sendRequestWithAsyncReply
            Channel ch = NettyConnector.connectUsingNetwork(host, port, false,
                    CONNECT_TIMEOUT_MS, socketTimeoutSeconds,
                    listener, callbackExecutor, eventLoopGroup);
            try {
                if (oidcTokenSupplier != null) {
                    performSaslHandshake(ch);
                }
                return ch;
            } catch (RuntimeException | IOException badAuth) {
                ch.close();
                throw badAuth;
            }
        }

        private void performSaslHandshake(Channel ch) throws IOException {
            OAuthBearerSaslClient saslClient = new OAuthBearerSaslClient(oidcTokenSupplier);
            byte[] firstToken;
            try {
                firstToken = saslClient.evaluateChallenge(new byte[0]);
            } catch (javax.security.sasl.SaslException e) {
                throw new IOException("SASL initial-token build failed: " + e.getMessage(), e);
            }
            long timeoutMs = TimeUnit.SECONDS.toMillis(clientTimeoutSeconds);
            long requestId = ch.generateRequestId();
            try {
                Pdu reply = ch.sendMessageWithPduReply(requestId,
                        PduCodec.SaslTokenMessageRequest.write(requestId,
                                OAuthBearerSaslClient.MECHANISM, firstToken),
                        timeoutMs);
                try {
                    if (reply.type == Pdu.TYPE_ERROR) {
                        throw new IOException("SASL handshake rejected: "
                                + PduCodec.ErrorResponse.readError(reply));
                    }
                    if (reply.type != Pdu.TYPE_SASL_TOKEN_SERVER_RESPONSE) {
                        throw new IOException("Unexpected reply type during SASL handshake: " + reply.type);
                    }
                    if (!saslClient.isComplete()) {
                        throw new IOException("OAUTHBEARER SASL did not complete in one round");
                    }
                } finally {
                    reply.close();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during SASL handshake", e);
            } catch (java.util.concurrent.TimeoutException e) {
                throw new IOException("SASL handshake timed out", e);
            }
        }

        void closeQuiet() {
            synchronized (lock) {
                closed = true;
                Channel c = current;
                if (c != null) {
                    safeClose(c);
                    current = null;
                }
            }
        }

        private void safeClose(Channel c) {
            try {
                c.close();
            } catch (RuntimeException ignored) {
                // Broad catch: socket close on a half-broken channel can throw
                // in netty; the caller's intent is "best-effort drain".
            }
        }

        @Override
        public String toString() {
            return server + "/" + plane;
        }
    }

    // ---------------------------------------------------------------------
    // PDU send/reply plumbing
    // ---------------------------------------------------------------------

    @FunctionalInterface
    private interface RequestPduFactory {
        ByteBuf build(long requestId);
    }

    @FunctionalInterface
    private interface ResponsePduParser<T> {
        T parse(Pdu pdu);
    }

    /**
     * Sends a request PDU on the given {@link ServerChannel} and resolves the
     * future when the matching response arrives. {@code TYPE_ERROR} replies
     * are converted to a failed future carrying the server-side message.
     */
    private <T> CompletableFuture<T> sendRequest(ServerChannel sc,
                                                 RequestPduFactory requestFactory,
                                                 ResponsePduParser<T> parser) {
        Objects.requireNonNull(sc, "ServerChannel");
        CompletableFuture<T> future = new CompletableFuture<>();
        Channel ch;
        try {
            ch = sc.get();
        } catch (IOException openError) {
            future.completeExceptionally(openError);
            return future;
        }
        long requestId = ch.generateRequestId();
        ByteBuf request;
        try {
            request = requestFactory.build(requestId);
        } catch (RuntimeException buildError) {
            future.completeExceptionally(buildError);
            return future;
        }
        long timeoutMs = TimeUnit.SECONDS.toMillis(clientTimeoutSeconds);
        ch.sendRequestWithAsyncReply(requestId, request, timeoutMs, (Pdu reply, Throwable err) -> {
            if (err != null) {
                future.completeExceptionally(err);
                return;
            }
            try {
                if (reply.type == Pdu.TYPE_ERROR) {
                    future.completeExceptionally(new IOException(
                            PduCodec.ErrorResponse.readError(reply)));
                    return;
                }
                T value = parser.parse(reply);
                future.complete(value);
            } catch (RuntimeException parseError) {
                future.completeExceptionally(parseError);
            } finally {
                // Pdu.close() releases the underlying pooled ByteBuf and
                // recycles the Pdu wrapper. Pdu is NOT itself a Netty
                // ReferenceCounted, so ReferenceCountUtil.safeRelease(reply)
                // would silently no-op and leak the buffer.
                reply.close();
            }
        });
        return future;
    }

    private static <T> CompletableFuture<T> failed(Throwable t) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(t);
        return f;
    }

    // ---------------------------------------------------------------------
    // Retry helper
    // ---------------------------------------------------------------------

    @FunctionalInterface
    private interface AsyncAction<T> {
        CompletableFuture<T> execute();
    }

    private <T> CompletableFuture<T> retryAsync(AsyncAction<T> action, String opName, String path, int attempt) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<T> actionResult;
        try {
            actionResult = action.execute();
        } catch (RuntimeException e) {
            // Synchronous failures (e.g. "Hash ring is empty") flow into the
            // same retry path as async failures.
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
            long delayMs = 1000L * (1L << (nextAttempt - 1));
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

    // ---------------------------------------------------------------------
    // Internal helpers
    // ---------------------------------------------------------------------

    private static <T> T getUnchecked(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new CompletionException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
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
