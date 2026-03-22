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

import herddb.remote.storage.CachingObjectStorage;
import herddb.remote.storage.LocalObjectStorage;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.S3ObjectStorage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusServlet;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
    private static final long DEFAULT_CACHE_MAX_BYTES = 1024L * 1024 * 1024; // 1 GB

    private final String host;
    private final int port;
    private final Path dataDirectory;
    private final int ioThreads;
    private final Properties config;
    private Server server;
    private ExecutorService metadataExecutor;
    private ObjectStorage objectStorage;
    private PrometheusMetricsProvider statsProvider;
    private org.eclipse.jetty.server.Server httpServer;

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

        String storageMode = config.getProperty("storage.mode", "local");
        if ("s3".equals(storageMode)) {
            objectStorage = buildS3ObjectStorage();
        } else {
            objectStorage = new LocalObjectStorage(dataDirectory, metadataExecutor);
        }

        // Initialize metrics
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");

        RemoteFileServiceImpl serviceImpl = new RemoteFileServiceImpl(objectStorage, statsLogger);
        server = ServerBuilder.forPort(port)
                .addService(serviceImpl)
                .build()
                .start();

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

        LOGGER.log(Level.INFO, "RemoteFileServer started on port {0}, storage: {1}, io threads: {2}",
                new Object[]{port, storageMode, ioThreads});
    }

    private ObjectStorage buildS3ObjectStorage() throws IOException {
        String endpoint = config.getProperty("s3.endpoint");
        String bucket = config.getProperty("s3.bucket");
        String region = config.getProperty("s3.region", "us-east-1");
        String accessKey = config.getProperty("s3.access.key");
        String secretKey = config.getProperty("s3.secret.key");
        String s3Prefix = config.getProperty("s3.prefix", "");
        long cacheMaxBytes = Long.parseLong(
                config.getProperty("cache.max.bytes", String.valueOf(DEFAULT_CACHE_MAX_BYTES)));

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());

        if (endpoint != null && !endpoint.isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        S3AsyncClient s3Client = clientBuilder.build();

        // Fail fast: verify bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build()).get();
        } catch (ExecutionException e) {
            s3Client.close();
            Throwable cause = e.getCause();
            if (cause instanceof NoSuchBucketException) {
                throw new IOException("S3 bucket does not exist: " + bucket);
            }
            throw new IOException("Failed to verify S3 bucket: " + bucket, cause);
        } catch (InterruptedException e) {
            s3Client.close();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while verifying S3 bucket", e);
        }

        String cacheDir = config.getProperty("cache.dir",
                dataDirectory.resolve("s3-cache").toString());
        Path cacheDirPath = Paths.get(cacheDir).toAbsolutePath();

        S3ObjectStorage s3Storage = new S3ObjectStorage(s3Client, bucket, s3Prefix);
        try {
            return new CachingObjectStorage(s3Storage, cacheDirPath, metadataExecutor, cacheMaxBytes);
        } catch (IOException e) {
            s3Client.close();
            throw e;
        }
    }

    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    public String getHost() {
        return host;
    }

    public String getAddress() {
        return host + ":" + getPort();
    }

    public void stop() throws InterruptedException {
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
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
            metadataExecutor.awaitTermination(30, TimeUnit.SECONDS);
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
