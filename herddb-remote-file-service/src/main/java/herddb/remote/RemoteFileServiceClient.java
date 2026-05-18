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
import java.util.concurrent.TimeoutException;
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
    /**
     * Configuration key for the interval (in milliseconds) between periodic
     * {@link herddb.network.Channel#channelIdle()} calls that drive
     * {@link herddb.network.netty.AbstractChannel#processPendingReplyMessagesDeadline()}.
     * Without this heartbeat, per-request deadlines registered by
     * {@link herddb.network.Channel#sendRequestWithAsyncReply} are never
     * checked and a silently-dead TCP channel hangs forever (issue #584).
     */
    public static final String CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS =
            "remote.file.client.idle.check.interval.ms";

    private static final long DEFAULT_CLIENT_TIMEOUT_SECONDS = 1800; // 30 minutes
    private static final int DEFAULT_CLIENT_RETRIES = 10;
    /** Default multipart block size: 4 MB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;
    /** Default cap on in-flight read bytes: 256 MiB (issue #246). */
    public static final long DEFAULT_CLIENT_MAX_INFLIGHT_READ_BYTES = 256L * 1024 * 1024;
    /**
     * Default cap on in-flight write bytes: 256 MiB (issue #468).
     *
     * <p>This value intentionally keeps back-pressure active. Payloads that
     * exceed the cap (e.g. a large serialised open-transactions blob written
     * during a checkpoint — issue #523) are handled without deadlocking by
     * {@link #acquireInflightWriteBytes}: the acquisition is capped to
     * {@code writePermits} and performed in {@link #blockSize}-sized chunks
     * so that the full semaphore capacity is never requested in a single
     * {@code acquireUninterruptibly} call.
     */
    public static final long DEFAULT_CLIENT_MAX_INFLIGHT_WRITE_BYTES = 256L * 1024 * 1024;
    /**
     * Default idle-check interval: 30 seconds.
     * <p>The idle check calls {@link herddb.network.Channel#channelIdle()} on
     * every open channel, which triggers deadline scanning in
     * {@link herddb.network.netty.AbstractChannel#processPendingReplyMessagesDeadline()}.
     * Setting this shorter speeds up detection of dead channels at the cost of
     * a tiny periodic wakeup on the retry scheduler thread.
     */
    private static final long DEFAULT_IDLE_CHECK_INTERVAL_MS = 30_000L;

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
    /** Semaphore capacity for writes: {@code min(maxInflightWriteBytes, Integer.MAX_VALUE)}. */
    private final int writePermits;
    private final Semaphore inflightWriteBytes;
    private final ScheduledExecutorService retryScheduler;
    private final Supplier<String> oidcTokenSupplier;

    /**
     * Interval in milliseconds between periodic {@link #processChannelDeadlines()} calls.
     * Configurable via {@link #CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS}.
     */
    private final long idleCheckIntervalMs;

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
        this.writePermits = maxInflightWriteBytes > Integer.MAX_VALUE
                ? Integer.MAX_VALUE
                : (int) maxInflightWriteBytes;
        this.inflightWriteBytes = new Semaphore(this.writePermits);
        this.idleCheckIntervalMs = longConfig(configuration, CONFIG_CLIENT_IDLE_CHECK_INTERVAL_MS,
                DEFAULT_IDLE_CHECK_INTERVAL_MS);
        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            FastThreadLocalThread t = new FastThreadLocalThread(r, "remote-file-retry");
            t.setDaemon(true);
            return t;
        });
        // Schedule periodic channelIdle() calls so that per-request deadlines registered by
        // sendRequestWithAsyncReply are actually checked. Without this heartbeat,
        // AbstractChannel.processPendingReplyMessagesDeadline() is never invoked and a
        // silently-dead TCP channel (no FIN/RST from the peer) hangs every in-flight
        // CompletableFuture forever (issue #584).
        this.retryScheduler.scheduleAtFixedRate(
                this::processChannelDeadlines,
                idleCheckIntervalMs, idleCheckIntervalMs, TimeUnit.MILLISECONDS);
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

    /**
     * Acquires permits from the in-flight write-bytes semaphore and returns
     * the number of permits actually taken (which may be less than
     * {@code bytes} when the payload exceeds the configured cap — see below).
     *
     * <h3>Deadlock prevention (issue #523)</h3>
     * <p>{@link java.util.concurrent.Semaphore#acquireUninterruptibly(int)}
     * blocks forever when the requested count exceeds the semaphore's total
     * capacity. Large single-shot writes (e.g. the serialised
     * open-transactions blob written during a checkpoint) can exceed the
     * 256 MiB default. To avoid the permanent block:
     * <ol>
     *   <li>The requested permits are capped to {@link #writePermits} (the
     *       semaphore's initial capacity): {@code toAcquire = min(bytes, writePermits)}.
     *   <li>On the slow path the acquisition is done in chunks of
     *       {@link #blockSize} so that smaller concurrent writes can
     *       interleave between chunks.
     *   <li>A WARNING is logged when the payload exceeds the cap (so
     *       operators can tune {@link #CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES})
     *       and a separate WARNING is logged before blocking begins
     *       (previously the warning only fired after unblocking).
     * </ol>
     *
     * @return the number of permits actually acquired; always
     *         {@code min(bytes, writePermits)} and always &gt; 0.
     */
    private int acquireInflightWriteBytes(int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0, got " + bytes);
        }
        // Cap to semaphore capacity — requesting more than writePermits would
        // deadlock on acquireUninterruptibly.
        int toAcquire = Math.min(bytes, writePermits);
        if (bytes > writePermits) {
            // Payload exceeds the cap. We will hold up to writePermits permits at
            // a time; smaller concurrent writes can interleave between chunks.
            logPreservingInterrupt(Level.WARNING,
                    "remote file client write payload ({0} bytes) exceeds inflight-write cap "
                            + "({1} bytes); will hold up to {1} permits at a time during "
                            + "this write — consider raising "
                            + CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                    new Object[]{bytes, writePermits});
        }
        // Fast path: all permits available right now.
        if (inflightWriteBytes.tryAcquire(toAcquire)) {
            return toAcquire;
        }
        // Slow path: acquire in chunks of blockSize so smaller concurrent
        // writes can interleave. Warn before blocking starts.
        logPreservingInterrupt(Level.WARNING,
                "remote file client inflight-write reservation blocked "
                        + "(requested={0} bytes, available={1}/{2}); waiting — consider raising "
                        + CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES
                        + " or reducing concurrent compaction/page-write load",
                new Object[]{toAcquire, inflightWriteBytes.availablePermits(),
                        maxInflightWriteBytes});
        long startNanos = System.nanoTime();
        int acquired = 0;
        while (acquired < toAcquire) {
            int chunk = Math.min(toAcquire - acquired, blockSize);
            inflightWriteBytes.acquireUninterruptibly(chunk);
            acquired += chunk;
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (elapsedMs >= PERMIT_ACQUIRE_WARN_THRESHOLD_MS) {
            logPreservingInterrupt(Level.WARNING,
                    "remote file client inflight-write reservation unblocked after {0}ms "
                            + "(requested={1} bytes, available={2}/{3}); consider raising "
                            + CONFIG_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                    new Object[]{elapsedMs, toAcquire, inflightWriteBytes.availablePermits(),
                            maxInflightWriteBytes});
        }
        return toAcquire;
    }

    private void releaseInflightWriteBytes(int bytes) {
        inflightWriteBytes.release(bytes);
    }

    /**
     * Emits a log record at the given level while preserving the calling
     * thread's interrupt status.
     *
     * <p>Some I/O streams that back the log handler (e.g. a pipe that Maven
     * Surefire uses to capture test output) silently consume the interrupt
     * flag. Callers that need interrupt-sensitive semantics around
     * {@link java.util.concurrent.Semaphore#acquireUninterruptibly} must
     * use this wrapper so that the flag survives the log call.
     */
    private static void logPreservingInterrupt(Level level, String msg, Object[] params) {
        boolean interrupted = Thread.interrupted();
        try {
            LOGGER.log(level, msg, params);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Acquires permits from the in-flight write-bytes budget via
     * {@link #acquireInflightWriteBytes} and returns an idempotent
     * {@link Runnable} that releases the same number of permits exactly once.
     *
     * <p>Callers wire the runnable into the {@code whenComplete} hook of the
     * future returned by {@code sendRequest}, and into every synchronous
     * failure branch (e.g. {@code writeChannelForBlock} throwing) so the
     * reservation is always returned.
     *
     * <p>Empty payloads ({@code bytes == 0}) skip both the acquire and the
     * release — {@link #writeAsMultipart} legitimately uses
     * {@code Unpooled.EMPTY_BUFFER} to materialise an empty file marker
     * and would otherwise be rejected by {@link #acquireInflightWriteBytes}.
     *
     * <p>When {@code bytes > writePermits} the returned runnable releases
     * only {@code writePermits} permits (the amount actually acquired),
     * never {@code bytes}, so the semaphore is never over-inflated.
     */
    private Runnable reserveInflightWriteBytes(int bytes) {
        if (bytes <= 0) {
            return () -> { };
        }
        // acquired may be < bytes when the payload exceeds the cap.
        int acquired = acquireInflightWriteBytes(bytes);
        AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                releaseInflightWriteBytes(acquired);
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
            FastThreadLocalThread t = new FastThreadLocalThread(() -> {
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
     * Parses a {@code ReadFile} response PDU into a new direct {@code ByteBuf}
     * allocated from {@code allocator}. Returns {@code null} if the file was
     * not found ({@code found == 0}). The caller owns the returned buffer and
     * must release it when done.
     *
     * <p>No-leak guarantee (issue #582): if any exception is thrown after
     * {@code buf = allocator.directBuffer(len)} has been executed, {@code buf}
     * is released in the {@code finally} block before the exception propagates.
     * This covers:
     * <ul>
     *   <li>{@code len > 0} path: {@code readContent(pdu)} calls
     *       {@code retainedSlice(offset, len)} which throws
     *       {@link IndexOutOfBoundsException} when the response is truncated.
     *   <li>{@code len == 0} path: the allocation succeeds and we return
     *       immediately; no exception can be thrown after allocation, so no
     *       special handling is needed — but the {@code finally} guard is still
     *       in place for completeness.
     *   <li>{@code found == false} path: {@code buf} is never allocated;
     *       no release is needed.
     * </ul>
     *
     * <p>Package-private for direct unit testing (see
     * {@code RemoteFileServiceClientReadFileRangeByteBufLeakTest}).
     */
    static ByteBuf parseReadFileResponse(Pdu pdu, PooledByteBufAllocator allocator) {
        if (!PduCodec.ReadFileResponse.readFound(pdu)) {
            return null;
        }
        int len = PduCodec.ReadFileResponse.readContentLength(pdu);
        ByteBuf buf = allocator.directBuffer(len);
        boolean success = false;
        try {
            if (len > 0) {
                ByteBuf slice = PduCodec.ReadFileResponse.readContent(pdu);
                try {
                    buf.writeBytes(slice);
                } finally {
                    slice.release();
                }
            }
            success = true;
            return buf;
        } finally {
            if (!success) {
                buf.release();
            }
        }
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
                pdu -> parseReadFileResponse(pdu, PooledByteBufAllocator.DEFAULT));
        return result.whenComplete((buf, err) -> releaseOnce.run());
    }

    public CompletableFuture<Boolean> deleteFileAsync(String path) {
        // Issue #551 forensics: log every delete request issued by the
        // client. File deletions are the root cause of zombie-segment
        // failure modes (files gone while metadata still references them);
        // making them visible at INFO is required so the IS / optimizer
        // logs alone are enough to investigate future incidents. The user
        // explicitly chose INFO over FINE for this reason despite the
        // higher log volume on the retention-reaper hot path.
        LOGGER.log(Level.INFO, "file-server client: deleteFile path={0}", path);
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
        return writeFileBlockAsyncPreacquired(path, blockIndex, content, releaseOnce);
    }

    /**
     * Internal helper used by {@link #writeMultipartFile}: writes one block
     * using an already-acquired inflight-write reservation.
     *
     * <p>The caller acquires the reservation via {@link #reserveInflightWriteBytes}
     * and tracks the returned {@code releaseOnce} runnable in its own list so that,
     * when {@code writeMultipartFile} catches an exception, it can immediately
     * release permits for every block that was dispatched but not yet complete
     * (issue #575). The runnable is idempotent (backed by {@link
     * java.util.concurrent.atomic.AtomicBoolean}): calling it again in the
     * {@code whenComplete} hook of the returned future is a safe no-op.
     */
    private CompletableFuture<Void> writeFileBlockAsyncPreacquired(
            String path, long blockIndex, byte[] content, Runnable releaseOnce) {
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

    /**
     * Parses a {@code ReadFileRange} response PDU into a new direct
     * {@code ByteBuf} allocated from {@code allocator}. Returns {@code null}
     * if the requested range was not found ({@code found == 0}). The caller
     * owns the returned buffer and must release it when done.
     *
     * <p>No-leak guarantee (issue #582): mirrors the contract documented on
     * {@link #parseReadFileResponse(Pdu, PooledByteBufAllocator)}. Any
     * exception thrown after {@code buf} is allocated — in particular
     * {@link IndexOutOfBoundsException} from
     * {@code PduCodec.ReadFileRangeResponse.readContent(pdu)} when the server
     * sends a truncated response — causes the {@code finally} block to call
     * {@code buf.release()}, preventing the buffer from leaking into the
     * pooled allocator's unreachable set.
     *
     * <p>Package-private for direct unit testing (see
     * {@code RemoteFileServiceClientReadFileRangeByteBufLeakTest}).
     */
    static ByteBuf parseReadFileRangeResponse(Pdu pdu, PooledByteBufAllocator allocator) {
        if (!PduCodec.ReadFileRangeResponse.readFound(pdu)) {
            return null;
        }
        int len = PduCodec.ReadFileRangeResponse.readContentLength(pdu);
        ByteBuf buf = allocator.directBuffer(len);
        boolean success = false;
        try {
            if (len > 0) {
                ByteBuf slice = PduCodec.ReadFileRangeResponse.readContent(pdu);
                try {
                    buf.writeBytes(slice);
                } finally {
                    slice.release();
                }
            }
            success = true;
            return buf;
        } finally {
            if (!success) {
                buf.release();
            }
        }
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
                pdu -> parseReadFileRangeResponse(pdu, PooledByteBufAllocator.DEFAULT));
        return result.whenComplete((buf, err) -> releaseOnce.run());
    }

    /**
     * Streaming multipart write helper. Reads from {@code in} in
     * {@code blockSizeArg}-sized blocks and dispatches each as a parallel
     * {@link #writeFileBlockAsync(String, long, byte[])} call. Returns the
     * total bytes written.
     *
     * <p>Issue #575: each block's inflight-write permit is acquired in this
     * loop and tracked in {@code blockReleasers}. On any exception the catch
     * blocks immediately call every tracked releaser so that permits are
     * returned right away, instead of waiting for the individual block futures
     * to time out (which can take up to {@code clientTimeoutSeconds} — up to
     * 30 min by default). Each releaser is idempotent, so a later call from
     * the {@code whenComplete} hook of a block future is a safe no-op.
     */
    public long writeMultipartFile(String path, InputStream in, int blockSizeArg) throws IOException {
        long total = 0;
        long blockIndex = 0;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // Track one releaser per dispatched block so we can return all permits
        // immediately on failure (issue #575).
        List<Runnable> blockReleasers = new ArrayList<>();
        byte[] buf = new byte[blockSizeArg];
        while (true) {
            int read = readFully(in, buf, 0, blockSizeArg);
            if (read <= 0) {
                break;
            }
            byte[] block = new byte[read];
            System.arraycopy(buf, 0, block, 0, read);
            // Acquire the reservation here so the caller can release it
            // immediately on failure (see catch blocks below).
            Runnable releaseOnce = reserveInflightWriteBytes(block.length);
            blockReleasers.add(releaseOnce);
            futures.add(writeFileBlockAsyncPreacquired(path, blockIndex, block, releaseOnce));
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
            // Release permits for every block that was dispatched but has not
            // yet completed its network call (issue #575). Releasers already
            // fired via whenComplete are idempotent no-ops.
            releaseBlockPermits(blockReleasers);
            throw new IOException("Interrupted while writing multipart " + path, e);
        } catch (ExecutionException e) {
            // Release permits for every still-pending block immediately so the
            // inflight-write semaphore is not held for the full network timeout
            // (issue #575). Releasers that already fired via whenComplete are
            // idempotent no-ops.
            releaseBlockPermits(blockReleasers);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("multipart write failed for " + path, cause);
        }
        return total;
    }

    /**
     * Releases the inflight-write permits for all dispatched blocks after a
     * {@link #writeMultipartFile} failure (issue #575). Each runnable is
     * idempotent: if a block's {@code whenComplete} already fired and released
     * its permits, calling the runnable again is a safe no-op.
     */
    private static void releaseBlockPermits(List<Runnable> blockReleasers) {
        for (Runnable r : blockReleasers) {
            r.run();
        }
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
        // Issue #551 forensics: log every prefix-delete request. Prefix
        // deletes are the bulk-removal entry point (DROP INDEX, segment
        // wholesale wipe) so a stray call here can take out an entire
        // segment or index in one shot.
        LOGGER.log(Level.INFO, "file-server client: deleteByPrefix prefix={0}", prefix);
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
        // Issue #551 forensics: log every batch-delete request. Batch deletes
        // are the dominant deletion path in steady-state operation (compaction
        // input cleanup, retention reaper). Log the count plus the first/last
        // path so a stuck or runaway reaper is obvious from grep'ing the IS
        // and optimizer logs (full-list logging at INFO would be too noisy in
        // healthy operation).
        LOGGER.log(Level.INFO,
                "file-server client: deleteFiles count={0} first={1} last={2}",
                new Object[]{paths.size(), paths.get(0), paths.get(paths.size() - 1)});
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
            // Use NettyConnector.connect (not connectUsingNetwork) so that JVM-local
            // servers registered via LocalServerRegistry are preferred over TCP when
            // socketTimeout <= 0. In production the remote file server runs in a
            // separate process, so LocalServerRegistry is always empty and the call
            // falls through to TCP. In-process tests (e.g., RemoteFileServiceClient*Test)
            // benefit from zero-latency JVM channels without any production impact.
            Channel ch = NettyConnector.connect(host, port, false,
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

    /**
     * Calls {@link Channel#channelIdle()} on every currently-open channel so that
     * {@link herddb.network.netty.AbstractChannel#processPendingReplyMessagesDeadline()}
     * runs and can expire any requests that have exceeded their per-request deadline.
     *
     * <p>This method is invoked periodically by the retry scheduler (every
     * {@link #idleCheckIntervalMs} milliseconds). Before this fix (issue #584),
     * {@code channelIdle()} was never called from anywhere, making the deadline
     * mechanism dead code and allowing a silently-dead TCP channel to block
     * CompletableFutures forever.
     *
     * <p>The {@code current} channel field is read with a volatile load (no lock).
     * If the channel is swapped or nulled concurrently, the worst case is a
     * no-op call on a closed channel, which is safe.
     *
     * <p><b>Exception handling:</b> the body is wrapped in a broad {@code RuntimeException}
     * catch. This is required because {@code scheduleAtFixedRate} permanently cancels a
     * recurring task when its {@code Runnable} throws any unchecked exception. A single
     * rogue {@code channelIdle()} call (e.g. due to an internal Netty bug or a race
     * during channel close) must not silently kill the heartbeat for the lifetime of this
     * client, as that would re-introduce the indefinite-hang of issue #584.
     */
    private void processChannelDeadlines() {
        try {
            ServerSnapshot s = snapshot;
            if (s == null) {
                return;
            }
            for (ServerChannel sc : s.readChannels.values()) {
                Channel ch = sc.current; // volatile read
                if (ch != null) {
                    ch.channelIdle();
                }
            }
            for (ServerChannel sc : s.writeChannels.values()) {
                Channel ch = sc.current; // volatile read
                if (ch != null) {
                    ch.channelIdle();
                }
            }
        } catch (RuntimeException e) {
            // Catching RuntimeException broadly so that a single failing channelIdle()
            // invocation does not permanently cancel the scheduleAtFixedRate heartbeat.
            // Any such failure means one idle-check tick is skipped; the scanner resumes
            // on the next interval. A broad catch is required because Channel.channelIdle()
            // internally closes channels and fires callbacks whose exception behaviour is
            // implementation-defined (see AbstractChannel.processPendingReplyMessagesDeadline).
            LOGGER.log(Level.WARNING,
                    "processChannelDeadlines: unexpected error during idle-check tick "
                            + "(skipping this tick, heartbeat continues on next interval)", e);
        }
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

    /**
     * Synchronously unwraps the result of a {@link CompletableFuture}.
     *
     * <p>A safety-net deadline is applied to prevent an indefinite block when the underlying
     * async machinery fails to complete the future (e.g. if the idle-check mechanism were
     * somehow bypassed or disabled). The primary per-request timeout is enforced by the periodic
     * {@link #processChannelDeadlines()} task through
     * {@link herddb.network.netty.AbstractChannel#processPendingReplyMessagesDeadline()}
     * (issue #584).
     *
     * <p>Safety-net formula: {@code (maxRetries + 3) × clientTimeoutSeconds}.
     * The multiplier {@code (maxRetries + 3)} comfortably exceeds the worst-case full retry
     * chain cost of {@code (maxRetries + 1) × (clientTimeoutSeconds + idleCheckIntervalMs/1000s)}
     * plus exponential back-off {@code (2^maxRetries − 1) s}, provided
     * {@code idleCheckIntervalMs} is tuned to a reasonable fraction of {@code clientTimeoutSeconds}
     * (the default 30 s interval is well below the default 1800 s timeout). The safety net is
     * intentionally generous so it never fires during normal retry chains; it is a last-resort
     * guard only.
     */
    private <T> T getUnchecked(CompletableFuture<T> future) {
        // Compute once outside try/catch so both branches use the same value.
        long safetyNetSeconds = (maxRetries + 3L) * clientTimeoutSeconds;
        try {
            return future.get(safetyNetSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // future.cancel(true) marks the outer CompletableFuture as cancelled but does NOT
            // interrupt the in-flight Netty request or any pending retryScheduler retry tasks;
            // cleanup of those is delegated to the processChannelDeadlines() heartbeat which will
            // eventually close the dead channel and complete orphaned futures exceptionally.
            // No ByteBuf leak: request ByteBufs are consumed by Channel.writeAndFlush; response
            // ByteBufs are released in the sendRequest() callback's finally block regardless of
            // the outer future's cancellation state.
            future.cancel(true);
            throw new CompletionException(
                    new IOException("remote file operation did not complete within the safety-net "
                            + "timeout of " + safetyNetSeconds + "s — per-request deadline "
                            + "mechanism may be impaired", e));
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
