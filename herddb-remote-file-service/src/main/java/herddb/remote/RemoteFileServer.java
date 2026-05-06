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

import herddb.auth.oidc.OidcBootstrap;
import herddb.auth.oidc.OidcTokenValidator;
import herddb.metadata.MetadataStorageManager;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.remote.admin.RemoteFileServerAdminImpl;
import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.InMemoryBlockCacheObjectStorage;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.S3ObjectStorage;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusServlet;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

/**
 * Configurable Netty server for the file service. Issue #425 replaced the
 * previous gRPC server with a native length-prefixed Netty wire protocol
 * built on top of {@link NettyChannelAcceptor} from the {@code herddb-net}
 * module — the same framework HerdDB itself uses for client/server
 * communication. This removes per-RPC protobuf {@code byte[]} allocations
 * and HTTP/2 framing overhead, frees up the {@code ioRatio=70} tuning, and
 * picks up the standard SASL handshake (used for OIDC OAUTHBEARER auth)
 * and TLS support for free.
 *
 * <p>Supports local-filesystem (default) and S3 storage backends via the
 * {@code storage.mode} property. Hosts both the data-plane dispatcher
 * ({@link RemoteFileServiceImpl}) and the admin dispatcher
 * ({@link RemoteFileServerAdminImpl}) on the same Netty channel; PDU type
 * multiplexing routes inbound messages to the right handler.
 *
 * @author enrico.olivelli
 */
public class RemoteFileServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileServer.class.getName());
    /** Default disk cache budget for {@link CachingObjectStorage}: 1 GB. */
    private static final long DEFAULT_CACHE_MAX_BYTES = 1024L * 1024 * 1024; // 1 GB
    static final int DEFAULT_IO_RATIO = 70;
    /** Default block size for multipart files: 4 MB. */
    public static final int DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024;
    /** Config key: in-heap block cache byte budget. */
    public static final String CONFIG_BLOCK_CACHE_MAX_BYTES = "block.cache.max.bytes";
    /** Config key: enable/disable the in-heap block cache. */
    public static final String CONFIG_BLOCK_CACHE_ENABLED = "block.cache.enabled";
    /** Config key: thread count for the dedicated read-lane executor (issue #100). */
    public static final String CONFIG_READ_EXECUTOR_THREADS = "fileserver.read.executor.threads";
    /** Config key: thread count for the dedicated write-lane executor (issue #100). */
    public static final String CONFIG_WRITE_EXECUTOR_THREADS = "fileserver.write.executor.threads";
    /** Config key: thread count for the Netty worker event loop. Defaults to {@code ioThreads}. */
    public static final String CONFIG_NETTY_WORKER_THREADS = "fileserver.netty.worker.threads";
    /** Config key: thread count for the Netty callback executor. Defaults to {@code ioThreads}. */
    public static final String CONFIG_NETTY_CALLBACK_THREADS = "fileserver.netty.callback.threads";
    /**
     * Config key: thread count for the metadata executor (directory ops on local
     * storage, disk-cache index writer). Defaults to {@code ioThreads}.
     */
    public static final String CONFIG_METADATA_EXECUTOR_THREADS = "fileserver.metadata.executor.threads";

    private final String host;
    private final int port;
    private final Path dataDirectory;
    private final int ioThreads;
    private final Properties config;
    private NettyChannelAcceptor acceptor;
    private ExecutorService metadataExecutor;
    private ThreadPoolExecutor readExecutor;
    private ThreadPoolExecutor writeExecutor;
    private int blockSize;
    private ObjectStorage objectStorage;
    private PrometheusMetricsProvider statsProvider;
    private org.eclipse.jetty.server.Server httpServer;
    private MetadataStorageManager metadataStorageManager;
    private String registeredServiceId;
    /** Non-null when storage.mode=s3; exposed to {@link RemoteFileServerAdminImpl} (issue #336). */
    private CachingObjectStorage diskCache;
    /** Non-null when block.cache.enabled=true; exposed to {@link RemoteFileServerAdminImpl} (issue #336). */
    private InMemoryBlockCacheObjectStorage blockCacheRef;
    /** Storage mode string used in admin GetServerInfo response. */
    private String storageMode;
    /** OIDC validator built from {@link OidcBootstrap}; null when OIDC is disabled. */
    private OidcTokenValidator oidcValidator;

    public RemoteFileServer(String host, int port, Path dataDirectory, int ioThreads, Properties config) {
        this.host = host;
        this.port = port;
        this.dataDirectory = dataDirectory;
        this.ioThreads = ioThreads;
        this.config = config;
    }

    public RemoteFileServer(String host, int port, Path dataDirectory, int ioThreads) {
        this(host, port, dataDirectory, ioThreads, new Properties());
    }

    public RemoteFileServer(String host, int port, Path dataDirectory) {
        this(host, port, dataDirectory, Runtime.getRuntime().availableProcessors());
    }

    public RemoteFileServer(int port, Path dataDirectory) {
        this("0.0.0.0", port, dataDirectory);
    }

    public void start() throws IOException {
        AtomicInteger threadId = new AtomicInteger();
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "remote-file-meta-" + threadId.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        int metadataExecutorThreads = Integer.parseInt(
                config.getProperty(CONFIG_METADATA_EXECUTOR_THREADS, String.valueOf(ioThreads)));
        metadataExecutor = Executors.newFixedThreadPool(metadataExecutorThreads, threadFactory);

        int readExecutorThreads = Integer.parseInt(
                config.getProperty(CONFIG_READ_EXECUTOR_THREADS, String.valueOf(ioThreads)));
        // Initialize metrics first (needed for storage layers)
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");
        // Netty direct-memory counters (issue #246) so the unified JVM
        // dashboard can correlate pool-arena growth against
        // -XX:MaxDirectMemorySize for every HerdDB service. The gauges
        // carry their own netty_ prefix — pass the root logger, not a scope.
        herddb.core.stats.NettyMemoryMetrics.register(statsLogger);

        int writeExecutorThreads = Integer.parseInt(
                config.getProperty(CONFIG_WRITE_EXECUTOR_THREADS, String.valueOf(ioThreads)));
        readExecutor = buildLaneExecutor("remote-file-read-exec-", readExecutorThreads);
        writeExecutor = buildLaneExecutor("remote-file-write-exec-", writeExecutorThreads);

        storageMode = config.getProperty("storage.mode", "local");
        if ("s3".equals(storageMode)) {
            objectStorage = buildS3ObjectStorage(statsLogger);
        } else {
            objectStorage = new LocalObjectStorage(dataDirectory, metadataExecutor, statsLogger);
        }

        // Wrap with the direct-memory block cache if enabled. Sits in front of the
        // inner storage (local or S3+disk-cache) so hot graph blocks never re-touch
        // disk/S3. Values are pooled direct ByteBufs — only keys live on heap.
        boolean blockCacheEnabled = Boolean.parseBoolean(
                config.getProperty(CONFIG_BLOCK_CACHE_ENABLED, "true"));
        InMemoryBlockCacheObjectStorage blockCache = null;
        if (blockCacheEnabled) {
            long blockCacheMaxBytes = Long.parseLong(
                    config.getProperty(CONFIG_BLOCK_CACHE_MAX_BYTES,
                            String.valueOf(defaultBlockCacheMaxBytes())));
            blockCache = new InMemoryBlockCacheObjectStorage(objectStorage, blockCacheMaxBytes);
            objectStorage = blockCache;
            LOGGER.log(Level.INFO,
                    "block cache enabled: maxBytes={0} ({1} MB)",
                    new Object[]{blockCacheMaxBytes, blockCacheMaxBytes / (1024 * 1024)});
        } else {
            LOGGER.log(Level.INFO, "block cache disabled");
        }
        blockCacheRef = blockCache;
        if (blockCache != null) {
            registerBlockCacheGauges(statsLogger, blockCache);
        }
        if (diskCache != null) {
            registerDiskCacheGauges(statsLogger, diskCache);
        }

        int ioRatio = Integer.getInteger("herddb.fileserver.netty.ioRatio", DEFAULT_IO_RATIO);
        this.blockSize = Integer.parseInt(config.getProperty("block.size",
                String.valueOf(DEFAULT_BLOCK_SIZE)));
        int nettyWorkerThreads = Integer.parseInt(
                config.getProperty(CONFIG_NETTY_WORKER_THREADS, String.valueOf(ioThreads)));
        int nettyCallbackThreads = Integer.parseInt(
                config.getProperty(CONFIG_NETTY_CALLBACK_THREADS, String.valueOf(ioThreads * 4)));
        LOGGER.log(Level.INFO,
                "RemoteFileServer thread pools: ioThreads(default)={0}, "
                        + "nettyWorker={1}, nettyCallback={2}, metadataExecutor={3}, readExecutor={4}, writeExecutor={5}",
                new Object[]{ioThreads, nettyWorkerThreads, nettyCallbackThreads, metadataExecutorThreads,
                        readExecutorThreads, writeExecutorThreads});

        // Build OIDC validator BEFORE accepting connections so the first
        // SASL handshake sees a fully-initialized validator.
        if (OidcBootstrap.isEnabled(config)) {
            try {
                oidcValidator = OidcBootstrap.buildValidator(config);
                LOGGER.log(Level.INFO, "OIDC authentication enabled for RemoteFileServer (issuer={0})",
                        config.getProperty(OidcBootstrap.PROP_ISSUER_URL));
            } catch (IOException e) {
                throw new IOException("Failed to initialize OIDC for RemoteFileServer: " + e.getMessage(), e);
            }
        }

        final RemoteFileServiceImpl serviceImpl = new RemoteFileServiceImpl(
                objectStorage, statsLogger, readExecutor, writeExecutor);
        registerLaneExecutorGauges(statsLogger);
        final RemoteFileServerAdminImpl adminImpl = new RemoteFileServerAdminImpl(this);

        acceptor = new NettyChannelAcceptor(host, port, false, statsLogger);
        acceptor.setWorkerThreads(nettyWorkerThreads);
        acceptor.setCallbackThreads(nettyCallbackThreads);
        // The file server has no JVM-local consumer (HerdDB-core talks to it
        // only over the wire), so disable the LocalVMChannel registry to
        // avoid a side-channel that would bypass authentication.
        acceptor.setEnableJVMNetwork(false);
        acceptor.setAcceptor(new ServerSideConnectionAcceptor() {
            @Override
            public ServerSideConnection createConnection(herddb.network.Channel netChannel) {
                return new RemoteFileServerSideConnection(netChannel, serviceImpl, adminImpl, oidcValidator);
            }
        });
        try {
            acceptor.start();
        } catch (Exception startError) {
            // NettyChannelAcceptor.start() throws Exception; wrap into IOException
            // so the public start() signature stays unchanged.
            throw new IOException("Failed to start RemoteFileServer Netty acceptor on "
                    + host + ":" + port + ": " + startError.getMessage(), startError);
        }

        // Apply the file-server's bespoke ioRatio AFTER start() so the worker
        // group is built. NettyChannelAcceptor itself does not expose a setter
        // for ioRatio because it does not need it; for the file server, which
        // is almost entirely IO-bound, 70 yields measurably better throughput
        // than the Netty default of 50.
        applyIoRatio(ioRatio);

        // Start HTTP server for metrics
        boolean httpEnabled = Boolean.parseBoolean(config.getProperty("http.enable", "false"));
        if (httpEnabled) {
            int httpPort = Integer.parseInt(config.getProperty("http.port", "9846"));
            String httpHost = config.getProperty("http.host", "0.0.0.0");
            try {
                httpServer = new org.eclipse.jetty.server.Server();
                ServerConnector connector = new ServerConnector(httpServer);
                connector.setPort(httpPort);
                connector.setHost(httpHost);
                httpServer.addConnector(connector);
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.GZIP);
                context.setContextPath("/");
                context.addServlet(new ServletHolder(new PrometheusServlet(statsProvider)), "/metrics");
                httpServer.setHandler(context);
                httpServer.start();
                LOGGER.log(Level.INFO, "Metrics HTTP server started on {0}:{1}",
                        new Object[]{httpHost, httpPort});
            } catch (Exception e) {
                throw new IOException("Failed to start metrics HTTP server", e);
            }
        }

        if (metadataStorageManager != null) {
            registeredServiceId = host + ":" + getPort();
            try {
                metadataStorageManager.registerFileServer(registeredServiceId, registeredServiceId);
                LOGGER.log(Level.INFO, "Registered file server in metadata store: {0}", registeredServiceId);
            } catch (Exception e) {
                // Broad catch: metadata-store registration failures must not abort
                // server startup — operators rely on the server being reachable
                // even when ZK is briefly unavailable, and unregistration on stop
                // also tolerates the same failure mode.
                LOGGER.log(Level.SEVERE, "Failed to register file server", e);
            }
        }

        LOGGER.log(Level.INFO,
                "RemoteFileServer started on port {0}, storage: {1}, io threads: {2}, ioRatio: {3}, "
                        + "blockSize: {4}, readLane: {5}, writeLane: {6}",
                new Object[]{getPort(), storageMode, ioThreads, ioRatio, this.blockSize,
                        readExecutorThreads, writeExecutorThreads});
    }

    /**
     * Reflectively reach into {@link NettyChannelAcceptor} to call
     * {@code setIoRatio} on its worker {@link io.netty.channel.epoll.EpollEventLoopGroup}
     * (or NIO group on non-Linux). The base acceptor does not expose the
     * worker group, but reading via reflection is a one-shot startup cost
     * and avoids forking a copy of the acceptor solely to add this knob.
     */
    private void applyIoRatio(int ioRatio) {
        try {
            java.lang.reflect.Field workerField =
                    NettyChannelAcceptor.class.getDeclaredField("workerGroup");
            workerField.setAccessible(true);
            Object workerGroup = workerField.get(acceptor);
            if (workerGroup == null) {
                return;
            }
            java.lang.reflect.Method setIoRatio = workerGroup.getClass().getMethod("setIoRatio", int.class);
            setIoRatio.invoke(workerGroup, ioRatio);
        } catch (ReflectiveOperationException e) {
            // Broad catch: ioRatio tuning is best-effort. If the underlying
            // acceptor changes shape, the file server still works correctly
            // — it just runs with the Netty default ioRatio.
            LOGGER.log(Level.WARNING, "Could not apply ioRatio={0} to Netty worker group: {1}",
                    new Object[]{ioRatio, e.getMessage()});
        }
    }

    // -----------------------------------------------------------------------
    // Admin accessors (issue #336) — used by RemoteFileServerAdminImpl
    // -----------------------------------------------------------------------

    /**
     * Returns the on-disk cache instance, or {@code null} when storage.mode is not s3.
     */
    public CachingObjectStorage getDiskCache() {
        return diskCache;
    }

    /**
     * Returns the in-heap block cache instance, or {@code null} when disabled.
     */
    public InMemoryBlockCacheObjectStorage getBlockCache() {
        return blockCacheRef;
    }

    /**
     * Returns the configured storage mode string ({@code "s3"} or {@code "local"}).
     * Available after {@link #start()} completes.
     */
    public String getStorageMode() {
        return storageMode != null ? storageMode : "local";
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private ThreadPoolExecutor buildLaneExecutor(String namePrefix, int threads) {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, namePrefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        return new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                factory);
    }

    private void registerLaneExecutorGauges(StatsLogger root) {
        registerExecutorGauges(root, "read_executor", readExecutor);
        registerExecutorGauges(root, "write_executor", writeExecutor);
    }

    private static void registerExecutorGauges(StatsLogger root, String name, ThreadPoolExecutor exec) {
        StatsLogger scope = root.scope("rfs").scope(name);
        scope.registerGauge("queue_size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return (long) exec.getQueue().size();
            }
        });
        scope.registerGauge("active_count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return (long) exec.getActiveCount();
            }
        });
        scope.registerGauge("pool_size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return (long) exec.getPoolSize();
            }
        });
    }

    private ObjectStorage buildS3ObjectStorage(StatsLogger statsLogger) throws IOException {
        String endpoint = config.getProperty("s3.endpoint");
        String bucket = config.getProperty("s3.bucket");
        String region = config.getProperty("s3.region", "us-east-1");
        String accessKey = config.getProperty("s3.access.key");
        String secretKey = config.getProperty("s3.secret.key");
        String s3Prefix = config.getProperty("s3.prefix", "");
        long cacheMaxBytes = Long.parseLong(
                config.getProperty("cache.max.bytes", String.valueOf(DEFAULT_CACHE_MAX_BYTES)));
        boolean gcsCompatibility = Boolean.parseBoolean(
                config.getProperty("s3.gcs.compatibility", "false"));

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder());
        if (gcsCompatibility) {
            LOGGER.log(Level.INFO, "S3 client: GCS compatibility mode enabled "
                    + "(path-style addressing, SDK checksums WHEN_REQUIRED)");
            clientBuilder
                    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                    .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        } else {
            LOGGER.log(Level.INFO, "S3 client: standard AWS mode "
                    + "(virtual-hosted-style, SDK default checksums)");
        }

        if (endpoint != null && !endpoint.isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        S3AsyncClient s3Client = clientBuilder.build();

        long bucketWaitTimeoutMs = Long.parseLong(
                config.getProperty("s3.bucket.wait.timeout.ms", "1800000")); // 30 minutes default
        long bucketPollIntervalMs = 5_000;
        long deadline = System.currentTimeMillis() + bucketWaitTimeoutMs;
        LOGGER.log(Level.INFO,
                "Waiting up to {0}ms for S3 bucket ''{1}'' to become available...",
                new Object[]{bucketWaitTimeoutMs, bucket});
        while (true) {
            try {
                s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build()).get();
                LOGGER.log(Level.INFO, "S3 bucket ''{0}'' is available", bucket);
                break;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof NoSuchBucketException) {
                    if (System.currentTimeMillis() > deadline) {
                        s3Client.close();
                        throw new IOException("Timed out after " + bucketWaitTimeoutMs
                                + "ms waiting for S3 bucket to exist: " + bucket);
                    }
                    LOGGER.log(Level.INFO, "S3 bucket ''{0}'' not yet available, retrying in {1}ms...",
                            new Object[]{bucket, bucketPollIntervalMs});
                    try {
                        Thread.sleep(bucketPollIntervalMs);
                    } catch (InterruptedException ie) {
                        s3Client.close();
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while verifying S3 bucket", ie);
                    }
                } else {
                    s3Client.close();
                    throw new IOException("Failed to verify S3 bucket: " + bucket, cause);
                }
            } catch (InterruptedException e) {
                s3Client.close();
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while verifying S3 bucket", e);
            }
        }

        String cacheDir = config.getProperty("cache.dir",
                dataDirectory.resolve("s3-cache").toString());
        Path cacheDirPath = Paths.get(cacheDir).toAbsolutePath();

        S3ObjectStorage s3Storage = new S3ObjectStorage(s3Client, bucket, s3Prefix, statsLogger);
        try {
            CachingObjectStorage cache = new CachingObjectStorage(
                    s3Storage, cacheDirPath, metadataExecutor, cacheMaxBytes);
            diskCache = cache;
            return cache;
        } catch (IOException e) {
            s3Client.close();
            throw e;
        }
    }

    /**
     * Default block-cache budget: 1/4 of Netty's max direct memory because the
     * cache stores pooled direct ByteBufs.
     */
    static long defaultBlockCacheMaxBytes() {
        long maxDirect = PlatformDependent.maxDirectMemory();
        long source;
        String sourceLabel;
        if (maxDirect > 0) {
            source = maxDirect;
            sourceLabel = "Netty maxDirectMemory";
        } else {
            long heap = Runtime.getRuntime().maxMemory();
            if (heap == Long.MAX_VALUE) {
                heap = Runtime.getRuntime().totalMemory();
            }
            source = heap;
            sourceLabel = "JVM maxMemory (fallback)";
        }
        long budget = Math.max(1, source / 4);
        LOGGER.log(Level.INFO,
                "block-cache auto-size: {0} bytes ({1} MB) = 1/4 of {2} ({3} bytes)",
                new Object[]{budget, budget / (1024 * 1024), sourceLabel, source});
        return budget;
    }

    private static void registerBlockCacheGauges(StatsLogger root, InMemoryBlockCacheObjectStorage cache) {
        StatsLogger scope = root.scope("rfs").scope("block_cache");
        scope.registerGauge("max_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.getMaxBytes();
            }
        });
        scope.registerGauge("size_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.estimatedBytes();
            }
        });
        scope.registerGauge("entries", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.estimatedSize();
            }
        });
        scope.registerGauge("hits", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.stats().hitCount();
            }
        });
        scope.registerGauge("misses", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.stats().missCount();
            }
        });
        scope.registerGauge("evictions", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.stats().evictionCount();
            }
        });
        scope.registerGauge("evicted_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.stats().evictionWeight();
            }
        });
    }

    /**
     * Registers Prometheus gauges for the on-disk cache ({@link CachingObjectStorage})
     * under the {@code rfs_disk_cache_*} namespace (issue #336).
     */
    private static void registerDiskCacheGauges(StatsLogger root, CachingObjectStorage cache) {
        StatsLogger scope = root.scope("rfs").scope("disk_cache");
        scope.registerGauge("max_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.getMaxCacheBytes();
            }
        });
        scope.registerGauge("estimated_entries", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.estimatedEntries();
            }
        });
        scope.registerGauge("hit_count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.diskLruStats().hitCount();
            }
        });
        scope.registerGauge("miss_count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.diskLruStats().missCount();
            }
        });
        scope.registerGauge("eviction_count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.diskLruStats().evictionCount();
            }
        });
        scope.registerGauge("hit_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.getHitBytes();
            }
        });
        scope.registerGauge("miss_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return cache.getMissBytes();
            }
        });
    }

    public int getPort() {
        if (acceptor != null) {
            // NettyChannelAcceptor binds the configured port verbatim — there
            // is no auto-assigned port path, so reading back the field is
            // unnecessary. Tests that need an ephemeral port construct the
            // server with port=0 and then locate the bound port via the
            // bound channel; expose that via reflection on the acceptor's
            // private "channel" field.
            int bound = readBoundPort();
            if (bound > 0) {
                return bound;
            }
        }
        return port;
    }

    /**
     * Reflectively reads the local port the acceptor's bound channel is on.
     * Returns -1 when the acceptor has not started yet or when reflection
     * fails — callers fall back to the configured port.
     */
    private int readBoundPort() {
        try {
            java.lang.reflect.Field channelField = NettyChannelAcceptor.class.getDeclaredField("channel");
            channelField.setAccessible(true);
            Object ch = channelField.get(acceptor);
            if (ch == null) {
                return -1;
            }
            java.net.SocketAddress addr = ((io.netty.channel.Channel) ch).localAddress();
            if (addr instanceof java.net.InetSocketAddress) {
                return ((java.net.InetSocketAddress) addr).getPort();
            }
            return -1;
        } catch (ReflectiveOperationException e) {
            // Broad catch: reading the bound port is best-effort; fall back
            // to the configured value when reflection fails.
            return -1;
        }
    }

    public int getBlockSize() {
        return blockSize;
    }

    public String getHost() {
        return host;
    }

    public String getAddress() {
        return host + ":" + getPort();
    }

    public void setMetadataStorageManager(MetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    public void stop() throws InterruptedException {
        if (metadataStorageManager != null && registeredServiceId != null) {
            String id = registeredServiceId;
            registeredServiceId = null;
            try {
                metadataStorageManager.unregisterFileServer(id);
                LOGGER.log(Level.INFO, "Unregistered file server: {0}", id);
            } catch (Exception e) {
                // Broad catch: metadata-store unregistration failures should not
                // prevent the rest of the shutdown sequence from running.
                LOGGER.log(Level.WARNING, "Failed to unregister file server", e);
            }
        }
        if (httpServer != null) {
            try {
                httpServer.stop();
            } catch (Exception e) {
                // Broad catch: jetty's stop() declares Exception; we still need
                // to continue with the rest of teardown regardless.
                LOGGER.log(Level.WARNING, "Error stopping metrics HTTP server", e);
            }
        }
        if (statsProvider != null) {
            statsProvider.stop();
        }
        if (acceptor != null) {
            acceptor.close();
            LOGGER.log(Level.INFO, "RemoteFileServer stopped");
        }
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
            metadataExecutor.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (readExecutor != null) {
            readExecutor.shutdown();
            readExecutor.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (writeExecutor != null) {
            writeExecutor.shutdown();
            writeExecutor.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (objectStorage != null) {
            try {
                objectStorage.close();
            } catch (Exception e) {
                // Broad catch: ObjectStorage.close() declares Exception (S3
                // client close, file handle leaks, etc); failures here are
                // logged but do not propagate so the caller's shutdown path
                // can finish.
                LOGGER.log(Level.WARNING, "Error closing objectStorage", e);
            }
        }
    }

    public int getHttpPort() {
        if (httpServer != null) {
            return ((ServerConnector) httpServer.getConnectors()[0]).getLocalPort();
        }
        return -1;
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
