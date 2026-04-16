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
import herddb.auth.oidc.grpc.JwtAuthServerInterceptor;
import herddb.metadata.MetadataStorageManager;
import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.InMemoryBlockCacheObjectStorage;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.S3ObjectStorage;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
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
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

/**
 * Configurable gRPC server for RemoteFileService.
 * Supports local filesystem (default) and S3 storage backends via {@code storage.mode} property.
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

    private final String host;
    private final int port;
    private final Path dataDirectory;
    private final int ioThreads;
    private final Properties config;
    private Server server;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workerGroup;
    private ExecutorService metadataExecutor;
    private ThreadPoolExecutor readExecutor;
    private ThreadPoolExecutor writeExecutor;
    private int blockSize;
    private ObjectStorage objectStorage;
    private PrometheusMetricsProvider statsProvider;
    private org.eclipse.jetty.server.Server httpServer;
    private MetadataStorageManager metadataStorageManager;
    private String registeredServiceId;

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
        metadataExecutor = Executors.newFixedThreadPool(ioThreads, threadFactory);

        int readExecutorThreads = Integer.parseInt(
                config.getProperty(CONFIG_READ_EXECUTOR_THREADS, String.valueOf(ioThreads)));
        // Initialize metrics first (needed for storage layers)
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");

        int writeExecutorThreads = Integer.parseInt(
                config.getProperty(CONFIG_WRITE_EXECUTOR_THREADS, String.valueOf(ioThreads)));
        readExecutor = buildLaneExecutor("remote-file-read-exec-", readExecutorThreads);
        writeExecutor = buildLaneExecutor("remote-file-write-exec-", writeExecutorThreads);

        String storageMode = config.getProperty("storage.mode", "local");
        if ("s3".equals(storageMode)) {
            objectStorage = buildS3ObjectStorage(statsLogger);
        } else {
            objectStorage = new LocalObjectStorage(dataDirectory, metadataExecutor, statsLogger);
        }

        // Wrap with in-heap block cache if enabled. Sits in front of the inner storage
        // (local or S3+disk-cache) so hot graph blocks never re-touch disk/S3.
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
                    "In-heap block cache enabled: maxBytes={0} ({1} MB)",
                    new Object[]{blockCacheMaxBytes, blockCacheMaxBytes / (1024 * 1024)});
        } else {
            LOGGER.log(Level.INFO, "In-heap block cache disabled");
        }
        if (blockCache != null) {
            registerBlockCacheGauges(statsLogger, blockCache);
        }

        int ioRatio = Integer.getInteger("herddb.fileserver.netty.ioRatio", DEFAULT_IO_RATIO);
        this.blockSize = Integer.parseInt(config.getProperty("block.size",
                String.valueOf(DEFAULT_BLOCK_SIZE)));
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(ioThreads);
        workerGroup.setIoRatio(ioRatio);

        RemoteFileServiceImpl serviceImpl = new RemoteFileServiceImpl(
                objectStorage, statsLogger, readExecutor, writeExecutor);
        registerLaneExecutorGauges(statsLogger);
        NettyServerBuilder grpcBuilder = NettyServerBuilder.forPort(port)
                .bossEventLoopGroup(bossGroup)
                .workerEventLoopGroup(workerGroup)
                .channelType(NioServerSocketChannel.class)
                .addService(serviceImpl)
                .maxInboundMessageSize(blockSize + 1024 * 1024);
        if (OidcBootstrap.isEnabled(config)) {
            try {
                OidcTokenValidator validator = OidcBootstrap.buildValidator(config);
                grpcBuilder.intercept(new JwtAuthServerInterceptor(validator::validate));
                LOGGER.log(Level.INFO, "OIDC authentication enabled for RemoteFileServer (issuer={0})",
                        config.getProperty(OidcBootstrap.PROP_ISSUER_URL));
            } catch (IOException e) {
                throw new IOException("Failed to initialize OIDC for RemoteFileServer: " + e.getMessage(), e);
            }
        }
        server = grpcBuilder.build().start();

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
            registeredServiceId = host + ":" + server.getPort();
            try {
                metadataStorageManager.registerFileServer(registeredServiceId, registeredServiceId);
                LOGGER.log(Level.INFO, "Registered file server in metadata store: {0}", registeredServiceId);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to register file server", e);
            }
        }

        LOGGER.log(Level.INFO,
                "RemoteFileServer started on port {0}, storage: {1}, io threads: {2}, ioRatio: {3}, "
                        + "blockSize: {4}, readLane: {5}, writeLane: {6}",
                new Object[]{port, storageMode, ioThreads, ioRatio, this.blockSize,
                        readExecutorThreads, writeExecutorThreads});
    }

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
        // cache.max.bytes is the local DISK cache budget for CachingObjectStorage.
        // It is NOT an in-heap cache: byte[] values are never retained in the JVM heap;
        // the OS page cache serves hot data under kernel-managed memory pressure.
        long cacheMaxBytes = Long.parseLong(
                config.getProperty("cache.max.bytes", String.valueOf(DEFAULT_CACHE_MAX_BYTES)));

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                // Use AWS CRT async HTTP client for better stability and non-blocking shutdown
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder());

        if (endpoint != null && !endpoint.isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        S3AsyncClient s3Client = clientBuilder.build();

        // Wait for bucket to become available, with configurable timeout
        long bucketWaitTimeoutMs = Long.parseLong(
                config.getProperty("s3.bucket.wait.timeout.ms", "1800000")); // 30 minutes default
        long bucketPollIntervalMs = 5_000; // 5 seconds between retries
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
                    // Non-bucket errors (permissions, network, etc.) fail immediately
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
            return new CachingObjectStorage(s3Storage, cacheDirPath, metadataExecutor, cacheMaxBytes);
        } catch (IOException e) {
            s3Client.close();
            throw e;
        }
    }

    /** Default in-heap block-cache budget: 1/4 of the JVM max heap. */
    static long defaultBlockCacheMaxBytes() {
        long max = Runtime.getRuntime().maxMemory();
        if (max == Long.MAX_VALUE) {
            // -Xmx unset: fall back to total memory so we don't allocate everything in sight.
            max = Runtime.getRuntime().totalMemory();
        }
        return Math.max(1, max / 4);
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

    public int getPort() {
        return server != null ? server.getPort() : port;
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
                LOGGER.log(Level.WARNING, "Failed to unregister file server", e);
            }
        }
        if (httpServer != null) {
            try {
                httpServer.stop();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error stopping metrics HTTP server", e);
            }
        }
        if (statsProvider != null) {
            statsProvider.stop();
        }
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, "RemoteFileServer stopped");
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
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
