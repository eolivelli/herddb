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

package herddb.indexing;

import herddb.auth.oidc.OidcBootstrap;
import herddb.auth.oidc.OidcTokenValidator;
import herddb.auth.oidc.grpc.JwtAuthServerInterceptor;
import herddb.core.MemoryManager;
import herddb.core.stats.NettyMemoryMetrics;
import herddb.file.FileDataStorageManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.storage.ObjectStorage;
import herddb.remote.storage.S3ObjectStorage;
import herddb.server.RemoteFileClient;
import herddb.server.RemoteFileServiceFactory;
import herddb.server.RemoteFileStorageManager;
import herddb.server.SharedCheckpointMetadata;
import herddb.storage.DataStorageManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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

/**
 * gRPC server for the IndexingService.
 * Exposes vector index Search and GetIndexStatus RPCs, plus a Prometheus metrics endpoint.
 *
 * @author enrico.olivelli
 */
public class IndexingServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(IndexingServer.class.getName());

    private final String host;
    private final int port;
    private final IndexingServerConfiguration config;
    private final IndexingServiceEngine engine;

    private Server server;
    private PrometheusMetricsProvider statsProvider;
    private org.eclipse.jetty.server.Server httpServer;
    private MetadataStorageManager metadataStorageManager;
    private String registeredServiceId;
    // Stored so the /status HTTP servlet can read aggregate search counters
    // (issue #541). Set in start() before the HTTP server is mounted.
    private IndexingServiceImpl serviceImpl;

    // When storage.type=remote, these are set by buildDataStorageManager
    // and consumed by the tablespace-resolved hook.
    private RemoteFileClient remoteFileServiceClient;
    private SharedCheckpointMetadata sharedCheckpointMetadataManager;
    private RemoteFileStorageManager remoteDataStorageManager;
    private Path remoteMetaDir;

    public IndexingServer(String host, int port, IndexingServiceEngine engine, IndexingServerConfiguration config) {
        this.host = host;
        this.port = port;
        this.engine = engine;
        this.config = config;
    }

    public IndexingServer(int port, IndexingServiceEngine engine) {
        this("0.0.0.0", port, engine, new IndexingServerConfiguration());
    }

    /**
     * Builds a MemoryManager based on configuration.
     * If {@code indexing.memory.vector.limit} is 0 (auto), uses 33% of JVM max heap
     * (consistent with the vector back-pressure budget set in {@link #start()}).
     */
    MemoryManager buildMemoryManager() {
        long maxVectorMemory = config.getLong(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT_DEFAULT);
        long maxLogicalPageSize = config.getLong(IndexingServerConfiguration.PROPERTY_MEMORY_PAGE_SIZE,
                IndexingServerConfiguration.PROPERTY_MEMORY_PAGE_SIZE_DEFAULT);

        long maxDataUsedMemory;
        if (maxVectorMemory <= 0) {
            // Auto: use 33% of JVM max heap (same fraction as vector back-pressure budget)
            maxDataUsedMemory = Runtime.getRuntime().maxMemory() / 3;
        } else {
            maxDataUsedMemory = maxVectorMemory;
        }

        // Ensure maxDataUsedMemory is at least maxLogicalPageSize
        if (maxDataUsedMemory < maxLogicalPageSize) {
            maxDataUsedMemory = maxLogicalPageSize;
        }

        LOGGER.log(Level.INFO, "Building MemoryManager: maxDataUsedMemory={0} MB, maxLogicalPageSize={1}",
                new Object[]{maxDataUsedMemory / (1024 * 1024), maxLogicalPageSize});

        // maxPKUsedMemory must be >= maxLogicalPageSize, so pass maxLogicalPageSize for it
        // maxIndexUsedMemory=0 means use data memory for index pages
        return new MemoryManager(maxDataUsedMemory, 0, maxLogicalPageSize, maxLogicalPageSize);
    }

    /**
     * Builds a DataStorageManager based on configuration.
     * Supported types: "file" (default) and "memory".
     */
    DataStorageManager buildDataStorageManager(Path dataDir) {
        String storageType = config.getString(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE,
                IndexingServerConfiguration.PROPERTY_STORAGE_TYPE_DEFAULT);

        LOGGER.log(Level.INFO, "Building DataStorageManager: type={0}, dataDir={1}",
                new Object[]{storageType, dataDir});

        switch (storageType) {
            case "memory":
                return new MemoryDataStorageManager();
            case "remote": {
                String remoteServersConfig = config.getString(
                        IndexingServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS,
                        IndexingServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS_DEFAULT);
                List<String> servers;
                boolean useZKDiscovery = false;
                if (!remoteServersConfig.isEmpty()) {
                    servers = Arrays.asList(remoteServersConfig.split(","));
                    LOGGER.log(Level.INFO, "Remote file services for indexing (static): {0}", servers);
                } else if (metadataStorageManager != null) {
                    try {
                        servers = metadataStorageManager.listFileServers();
                        useZKDiscovery = true;
                        LOGGER.log(Level.INFO, "Remote file services for indexing (ZK discovery): {0}", servers);
                    } catch (Exception e) {
                        servers = Collections.emptyList();
                        LOGGER.log(Level.WARNING, "Failed to discover remote file servers via ZK", e);
                    }
                } else {
                    servers = Collections.emptyList();
                    LOGGER.log(Level.WARNING, "Remote storage configured but no file servers "
                            + "and no metadata storage manager for ZK discovery");
                }
                Map<String, Object> clientConfig = new HashMap<>();
                clientConfig.put(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                        config.getLong(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT_DEFAULT));
                clientConfig.put(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                        config.getInt(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES_DEFAULT));
                clientConfig.put(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES,
                        config.getLong(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES,
                                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_READ_BYTES_DEFAULT));
                clientConfig.put(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                        config.getLong(IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_WRITE_BYTES,
                                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_MAX_INFLIGHT_WRITE_BYTES_DEFAULT));
                try {
                    RemoteFileServiceFactory factory = RemoteFileServiceFactory.load();
                    RemoteFileClient client = factory.createClient(servers, clientConfig);
                    if (useZKDiscovery) {
                        metadataStorageManager.addServiceDiscoveryListener(
                                new ServiceDiscoveryListener() {
                                    @Override
                                    public void onFileServersChanged(List<String> currentAddresses) {
                                        LOGGER.log(Level.INFO,
                                                "Remote file servers for indexing changed via ZK: {0}",
                                                currentAddresses);
                                        client.updateServers(currentAddresses);
                                    }
                                });
                        // Re-query after listener registration to close the race window
                        // between the initial listFileServers() and addServiceDiscoveryListener().
                        try {
                            List<String> currentServers = metadataStorageManager.listFileServers();
                            if (!currentServers.isEmpty()) {
                                client.updateServers(currentServers);
                            }
                        } catch (Exception re) {
                            LOGGER.log(Level.WARNING,
                                    "Failed to re-query file servers after listener registration (indexing)", re);
                        }
                    }
                    // Give the RemoteFileDataStorageManager its own subdir
                    // under the indexing data dir. The wrapped
                    // FileDataStorageManager wipes its tmp dir on close(),
                    // so it must NOT overlap with sibling state that we
                    // want to survive restarts (e.g. WatermarkStore's
                    // watermark.dat, which sits directly in {dataDir}).
                    // The wrapped FileDataStorageManager wipes its tmpDir on
                    // both start() and close(). Keeping the metadata dir
                    // separate from the tmp dir means IndexStatus checkpoint
                    // markers written to disk survive restarts. Both live
                    // under {dataDir}/... so they stay inside the
                    // indexing-service data directory (never on S3).
                    Path metaDir = dataDir.resolve("remote-metadata");
                    Path remoteTmpDir = dataDir.resolve("remote-tmp");
                    java.nio.file.Files.createDirectories(metaDir);
                    java.nio.file.Files.createDirectories(remoteTmpDir);
                    DataStorageManager dsm = factory.createDataStorageManager(
                            metaDir, remoteTmpDir, Integer.MAX_VALUE, client);

                    // Create a SharedCheckpointMetadata manager and wire it into
                    // the RemoteFileStorageManager so that every
                    // indexCheckpoint publishes its IndexStatus to S3, and a
                    // restart on a wiped disk can hydrate {metaDir} from S3.
                    SharedCheckpointMetadata shared = factory.createSharedCheckpointMetadata(client);
                    ((RemoteFileStorageManager) dsm).setSharedCheckpointMetadataManager(shared);

                    // Store for later use by the tablespace-resolved hook.
                    this.remoteFileServiceClient = client;
                    this.sharedCheckpointMetadataManager = shared;
                    this.remoteDataStorageManager = (RemoteFileStorageManager) dsm;
                    this.remoteMetaDir = metaDir;

                    // Wire direct S3 for fast segment-map download during cold-start
                    // recovery (issue #381). When enabled, PersistentVectorStore
                    // bypasses the gRPC file-server and downloads segment map blocks
                    // directly from S3, turning a 6+ hour serial gRPC walk into a
                    // parallel S3 download. Must be wired before the DSM is handed
                    // to the engine so supportsDirectMultipartDownload() returns true
                    // on the very first loadMultiSegmentFormat() call.
                    //
                    // Shadow replicas discard this DSM (replaced by ReadReplicaDataStorageManager
                    // below), so direct S3 is only wired for primaries.
                    boolean s3DirectEnabled = config.getBoolean(
                            IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED,
                            IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED_DEFAULT);
                    if (s3DirectEnabled && !config.isShadow()) {
                        if (dsm instanceof RemoteFileDataStorageManager) {
                            try {
                                ObjectStorage directStorage = buildDirectS3ObjectStorage();
                                ((RemoteFileDataStorageManager) dsm).setDirectObjectStorage(directStorage);
                                LOGGER.log(Level.INFO,
                                        "Direct S3 download enabled for segment map files (issue #381)");
                            } catch (IOException ioe) {
                                throw new RuntimeException(
                                        "Failed to build direct S3 client for indexing service", ioe);
                            }
                        } else {
                            LOGGER.log(Level.WARNING,
                                    "indexing.s3.direct.enabled=true but the data storage manager "
                                            + "({0}) is not a RemoteFileDataStorageManager — "
                                            + "direct S3 download will NOT be active",
                                    dsm.getClass().getSimpleName());
                        }
                    }

                    // Shadows operate strictly from remote storage. Swap the
                    // writable RemoteFileDataStorageManager we just built for
                    // a pure read-only DSM backed by the same client and
                    // metadata manager — keeps the file-server discovery and
                    // hydrate-from-S3 plumbing unchanged while forbidding any
                    // accidental writes. The writable DSM was fully
                    // constructed and wired above so that the metadata dir
                    // gets created and the SharedCheckpointMetadata instance
                    // is ready; we discard it after building the replica DSM.
                    if (config.isShadow()) {
                        DataStorageManager replica = factory.createReadReplicaDataStorageManager(
                                client, shared, remoteTmpDir, Integer.MAX_VALUE);
                        return replica;
                    }

                    return dsm;
                } catch (java.io.IOException e) {
                    throw new RuntimeException(
                            "Cannot create RemoteFileDataStorageManager metadata directory", e);
                }
            }
            case "file":
            default:
                return new FileDataStorageManager(dataDir);
        }
    }

    public void start() throws IOException {
        // Fail fast on bad role/shadow combinations before we touch any heavy
        // I/O or storage subsystem.
        config.validateRoleAndShadow();

        // Build and set MemoryManager and DataStorageManager on the engine
        MemoryManager memoryManager = buildMemoryManager();
        engine.setMemoryManager(memoryManager);

        // Set the effective vector memory limit for back-pressure enforcement
        long maxVectorMemory = config.getLong(IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT,
                IndexingServerConfiguration.PROPERTY_MEMORY_VECTOR_LIMIT_DEFAULT);
        long effectiveVectorMemoryLimit = maxVectorMemory <= 0
                ? Runtime.getRuntime().maxMemory() / 3
                : maxVectorMemory;
        engine.setMaxVectorMemoryBytes(effectiveVectorMemoryLimit);

        DataStorageManager dataStorageManager = buildDataStorageManager(engine.getDataDirectory());
        engine.setDataStorageManager(dataStorageManager);

        // Remote-storage bootstrap: once the tablespace UUID is known, hydrate
        // the local metadata cache from S3 and install an S3WatermarkStore so
        // the indexing service can restart on a wiped disk.
        if (sharedCheckpointMetadataManager != null) {
            engine.setAfterTableSpaceResolved(this::bootstrapRemoteStateForTablespace);
        }

        // Initialize metrics
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");
        // Register Netty direct-memory gauges so the JVM dashboard can show
        // pool-arena footprint for this service (issue #503).
        NettyMemoryMetrics.register(statsLogger);

        engine.setStatsLogger(statsLogger);
        this.serviceImpl = new IndexingServiceImpl(engine, statsLogger);
        ServerBuilder<?> grpcBuilder = ServerBuilder.forPort(port).addService(serviceImpl);
        java.util.Properties oidcProps = config.asProperties();
        if (OidcBootstrap.isEnabled(oidcProps)) {
            try {
                OidcTokenValidator validator = OidcBootstrap.buildValidator(oidcProps);
                grpcBuilder.intercept(new JwtAuthServerInterceptor(validator::validate));
                LOGGER.log(Level.INFO, "OIDC authentication enabled for IndexingServer (issuer={0})",
                        oidcProps.getProperty(OidcBootstrap.PROP_ISSUER_URL));
            } catch (IOException e) {
                throw new IOException("Failed to initialize OIDC for IndexingServer: " + e.getMessage(), e);
            }
        }
        server = grpcBuilder.build().start();

        // Start HTTP server for metrics
        boolean httpEnabled = config.getBoolean(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE,
                IndexingServerConfiguration.PROPERTY_HTTP_ENABLE_DEFAULT);
        if (httpEnabled) {
            int httpPort = config.getInt(IndexingServerConfiguration.PROPERTY_HTTP_PORT,
                    IndexingServerConfiguration.PROPERTY_HTTP_PORT_DEFAULT);
            String httpHost = config.getString(IndexingServerConfiguration.PROPERTY_HTTP_HOST,
                    IndexingServerConfiguration.PROPERTY_HTTP_HOST_DEFAULT);
            try {
                httpServer = new org.eclipse.jetty.server.Server();
                ServerConnector connector = new ServerConnector(httpServer);
                connector.setPort(httpPort);
                connector.setHost(httpHost);
                httpServer.addConnector(connector);
                ServletContextHandler context = new ServletContextHandler(ServletContextHandler.GZIP);
                context.setContextPath("/");
                context.addServlet(new ServletHolder(new PrometheusServlet(statsProvider)), "/metrics");
                context.addServlet(new ServletHolder(new StatusServlet()), "/status");
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
                metadataStorageManager.registerIndexingServiceInstance(buildInstanceDescriptor());
                LOGGER.log(Level.INFO, "Registered indexing service in metadata store: {0}", registeredServiceId);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to register indexing service", e);
            }
        } else {
            registeredServiceId = host + ":" + server.getPort();
        }
        engine.setInstanceIdLabel(registeredServiceId);

        LOGGER.log(Level.INFO, "IndexingServer started on port {0}", getPort());
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

    public int getHttpPort() {
        if (httpServer != null) {
            return ((ServerConnector) httpServer.getConnectors()[0]).getLocalPort();
        }
        return -1;
    }

    public void setMetadataStorageManager(MetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    /**
     * Builds an {@link ObjectStorage} that connects directly to S3/GCS for
     * segment map file downloads, bypassing the gRPC file-server (issue #381).
     *
     * <p>Access and secret keys are read from the {@code S3_ACCESS_KEY} and
     * {@code S3_SECRET_KEY} environment variables — never from the properties
     * file — so they are not visible in ConfigMaps or log output.
     *
     * @throws IOException if the required environment variables are absent or
     *                     the S3 client cannot be constructed
     */
    private ObjectStorage buildDirectS3ObjectStorage() throws IOException {
        String bucket = config.getString(IndexingServerConfiguration.PROPERTY_S3_BUCKET,
                IndexingServerConfiguration.PROPERTY_S3_BUCKET_DEFAULT);
        String region = config.getString(IndexingServerConfiguration.PROPERTY_S3_REGION,
                IndexingServerConfiguration.PROPERTY_S3_REGION_DEFAULT);
        String endpoint = config.getString(IndexingServerConfiguration.PROPERTY_S3_ENDPOINT,
                IndexingServerConfiguration.PROPERTY_S3_ENDPOINT_DEFAULT);
        String s3Prefix = config.getString(IndexingServerConfiguration.PROPERTY_S3_PREFIX,
                IndexingServerConfiguration.PROPERTY_S3_PREFIX_DEFAULT);
        boolean gcsCompatibility = config.getBoolean(
                IndexingServerConfiguration.PROPERTY_S3_GCS_COMPATIBILITY,
                IndexingServerConfiguration.PROPERTY_S3_GCS_COMPATIBILITY_DEFAULT);
        // Keys are injected exclusively via environment variables so that they
        // never appear in ConfigMaps, properties files, or log output.
        String accessKey = System.getenv("S3_ACCESS_KEY");
        String secretKey = System.getenv("S3_SECRET_KEY");
        if (accessKey == null || accessKey.isEmpty()) {
            throw new IOException(
                    "S3_ACCESS_KEY environment variable must be set when "
                            + IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED + "=true");
        }
        if (secretKey == null || secretKey.isEmpty()) {
            throw new IOException(
                    "S3_SECRET_KEY environment variable must be set when "
                            + IndexingServerConfiguration.PROPERTY_S3_DIRECT_ENABLED + "=true");
        }

        S3AsyncClientBuilder clientBuilder = S3AsyncClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder());

        if (gcsCompatibility) {
            LOGGER.log(Level.INFO,
                    "Direct S3 client (GCS compatibility): path-style addressing, "
                            + "SDK checksums WHEN_REQUIRED");
            clientBuilder
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                    .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);
        } else {
            LOGGER.log(Level.INFO,
                    "Direct S3 client (AWS mode): virtual-hosted-style, "
                            + "SDK default checksums");
        }
        if (!endpoint.isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }

        S3AsyncClient s3Client = clientBuilder.build();
        LOGGER.log(Level.INFO,
                "Direct S3 client built for segment map downloads: bucket={0}, region={1}, prefix={2}",
                new Object[]{bucket, region, s3Prefix});
        // No StatsLogger here: this object is used only for cold-start reads and
        // the metrics provider is not yet initialised at the point buildDirectS3ObjectStorage
        // is called (it is set up later in start()).
        return new S3ObjectStorage(s3Client, bucket, s3Prefix);
    }

    /**
     * Called from {@link IndexingServiceEngine} once the tablespace UUID has been
     * resolved. Hydrates the local {@code remote-metadata} directory from S3 and
     * installs an {@link S3WatermarkStore} on the engine so that a subsequent
     * restart with a wiped local disk can resume from the correct watermark.
     */
    private void bootstrapRemoteStateForTablespace(String tableSpaceUUID) {
        try {
            // Hydrate local metadata dir from S3 if it is empty.
            boolean localEmpty = isMetadataDirEmpty(remoteMetaDir, tableSpaceUUID);
            if (localEmpty) {
                int count = sharedCheckpointMetadataManager.hydrateLocalMetadataDir(
                        remoteMetaDir, tableSpaceUUID);
                LOGGER.log(Level.INFO,
                        "hydrated {0} checkpoint file(s) for tableSpace {1} from S3",
                        new Object[]{count, tableSpaceUUID});
            } else {
                LOGGER.log(Level.INFO,
                        "local metadata already present for tableSpace {0}, skipping S3 hydrate",
                        tableSpaceUUID);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to hydrate local metadata from S3 for " + tableSpaceUUID, e);
        }

        // Install S3WatermarkStore for this tablespace UUID + instanceId.
        int instanceId = config.getInt(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                IndexingServerConfiguration.PROPERTY_INSTANCE_ID_DEFAULT);
        final RemoteFileClient client = remoteFileServiceClient;

        // Before issuing the first readFile for the watermark, block until the
        // remote file client has at least one server in its consistent-hash
        // ring. On a cold k3s boot the indexing service can race the
        // file-server's ZK registration; without this wait the very first
        // S3WatermarkStore.load() would throw "Hash ring is empty" and leave
        // the pod in a zombie state. See #42.
        long bootstrapWaitMs = config.getLong(
                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS,
                IndexingServerConfiguration.PROPERTY_REMOTE_FILE_BOOTSTRAP_WAIT_MS_DEFAULT);
        LOGGER.log(Level.INFO,
                "Waiting up to {0}ms for remote file servers to be discovered via ZK for tableSpace {1}...",
                new Object[]{bootstrapWaitMs, tableSpaceUUID});
        try {
            boolean ready = client.awaitServersReady(bootstrapWaitMs);
            if (!ready) {
                throw new RuntimeException(
                        "Timed out after " + bootstrapWaitMs + "ms waiting for remote file"
                                + " servers to be discovered via ZK for tableSpace "
                                + tableSpaceUUID);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting for remote file servers to be discovered", ie);
        }

        S3WatermarkStore.RemoteFileIO io = new S3WatermarkStore.RemoteFileIO() {
            @Override
            public void writeFile(String path, byte[] content) {
                client.writeFile(path, content);
            }

            @Override
            public byte[] readFile(String path) {
                return client.readFile(path);
            }
        };
        S3WatermarkStore s3WatermarkStore = new S3WatermarkStore(io, tableSpaceUUID, instanceId);
        engine.setWatermarkStore(s3WatermarkStore);
        LOGGER.log(Level.INFO,
                "Installed S3WatermarkStore at path {0}", s3WatermarkStore.getPath());
    }

    /**
     * Returns {@code true} if the per-tablespace metadata subtree is absent or
     * empty (no checkpoint markers) — in which case this is a fresh/ephemeral
     * boot and we should hydrate from S3.
     */
    private static boolean isMetadataDirEmpty(Path remoteMetaDir, String tableSpaceUUID) {
        try {
            Path tsDir = remoteMetaDir.resolve(tableSpaceUUID + ".tablespace");
            if (!java.nio.file.Files.exists(tsDir)) {
                return true;
            }
            try (java.util.stream.Stream<Path> walk = java.nio.file.Files.walk(tsDir)) {
                return walk.filter(java.nio.file.Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".checkpoint"))
                        .findAny()
                        .isEmpty();
            }
        } catch (java.io.IOException e) {
            return true;
        }
    }

    private IndexingServiceInstanceDescriptor buildInstanceDescriptor() {
        final String role = config.getString(IndexingServerConfiguration.PROPERTY_ROLE,
                IndexingServerConfiguration.PROPERTY_ROLE_DEFAULT);
        final int instanceId = config.getInt(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                IndexingServerConfiguration.PROPERTY_INSTANCE_ID_DEFAULT);
        if (IndexingServerConfiguration.ROLE_SHADOW.equals(role)) {
            final int shadowOf = config.getInt(IndexingServerConfiguration.PROPERTY_SHADOW_OF,
                    IndexingServerConfiguration.PROPERTY_SHADOW_OF_UNSET);
            return IndexingServiceInstanceDescriptor.shadow(registeredServiceId, registeredServiceId, shadowOf);
        }
        return IndexingServiceInstanceDescriptor.primary(registeredServiceId, registeredServiceId, instanceId);
    }

    public void stop() throws InterruptedException {
        if (metadataStorageManager != null && registeredServiceId != null) {
            String id = registeredServiceId;
            registeredServiceId = null;
            try {
                metadataStorageManager.unregisterIndexingServiceInstance(id);
                LOGGER.log(Level.INFO, "Unregistered indexing service: {0}", id);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to unregister indexing service", e);
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
            LOGGER.log(Level.INFO, "IndexingServer stopped");
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    // -------------------------------------------------------------------------
    // /status — JSON snapshot of instance info, engine stats, shadow status,
    // and per-search metric totals (issue #541).
    // Mirrors the union of GetInstanceInfo + GetEngineStats + GetShadowStatus
    // gRPC RPCs so operators can curl a single pod without gRPC tooling.
    // -------------------------------------------------------------------------

    /**
     * Jetty servlet that serves a JSON status snapshot on {@code GET /status}.
     * Reads directly from the outer {@link IndexingServer}'s {@code engine},
     * {@code config}, and {@code serviceImpl} fields — no synchronization
     * needed beyond what each accessor already guarantees.
     */
    private final class StatusServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws IOException {
            String json = buildStatusJson();
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            resp.setContentType("application/json; charset=utf-8");
            resp.setContentLength(body.length);
            resp.setStatus(HttpServletResponse.SC_OK);
            try (OutputStream os = resp.getOutputStream()) {
                os.write(body);
            }
        }

        private String buildStatusJson() {
            StringBuilder sb = new StringBuilder(2048);
            sb.append("{\n");

            // --- GetInstanceInfo ---
            IndexingServerConfiguration cfg = engine.getConfig();
            String storageType = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE,
                            IndexingServerConfiguration.PROPERTY_STORAGE_TYPE_DEFAULT)
                    : "";
            String dataDir = engine.getDataDirectory() != null
                    ? engine.getDataDirectory().toAbsolutePath().toString()
                    : "";
            String tablespaceName = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                            IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME_DEFAULT)
                    : "";
            int grpcPort = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_GRPC_PORT,
                            IndexingServerConfiguration.PROPERTY_GRPC_PORT_DEFAULT)
                    : 0;
            String grpcHost = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_GRPC_HOST,
                            IndexingServerConfiguration.PROPERTY_GRPC_HOST_DEFAULT)
                    : "";
            int instanceOrdinal = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                            IndexingServerConfiguration.PROPERTY_INSTANCE_ID_DEFAULT)
                    : 0;
            int numInstances = cfg != null
                    ? cfg.getInt(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                            IndexingServerConfiguration.PROPERTY_NUM_INSTANCES_DEFAULT)
                    : 1;
            String role = cfg != null
                    ? cfg.getString(IndexingServerConfiguration.PROPERTY_ROLE,
                            IndexingServerConfiguration.PROPERTY_ROLE_DEFAULT)
                    : IndexingServerConfiguration.ROLE_PRIMARY;
            appendJsonStr(sb, "instance_id", engine.getInstanceIdLabel(), true);
            appendJsonStr(sb, "grpc_host", grpcHost, true);
            appendJsonNum(sb, "grpc_port", grpcPort, true);
            appendJsonStr(sb, "storage_type", storageType, true);
            appendJsonStr(sb, "data_dir", dataDir, true);
            appendJsonStr(sb, "tablespace_name", tablespaceName, true);
            appendJsonStr(sb, "tablespace_uuid", engine.getTableSpaceUUID(), true);
            appendJsonNum(sb, "instance_ordinal", instanceOrdinal, true);
            appendJsonNum(sb, "num_instances", numInstances, true);
            appendJsonStr(sb, "role", role, true);
            appendJsonNum(sb, "shadow_of",
                    engine.isConfiguredAsShadow() ? engine.getShadowOfOrMinusOne() : -1L, true);
            appendJsonNum(sb, "jvm_max_heap_bytes", Runtime.getRuntime().maxMemory(), true);

            // --- GetEngineStats ---
            long startMs = engine.getStartTimeMillis();
            long uptime = startMs > 0 ? System.currentTimeMillis() - startMs : 0L;
            LogSequenceNumber lsn = engine.getLastProcessedLsn();
            java.lang.management.MemoryUsage heap =
                    java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long heapUsed = heap.getUsed();
            long heapMax = heap.getMax();
            long heapPct = heapMax > 0 ? Math.min(100L, (100L * heapUsed) / heapMax) : -1L;
            appendJsonNum(sb, "uptime_millis", uptime, true);
            appendJsonBool(sb, "tailer_running", engine.isTailerRunning(), true);
            appendJsonNum(sb, "tailer_watermark_ledger", lsn != null ? lsn.ledgerId : -1L, true);
            appendJsonNum(sb, "tailer_watermark_offset", lsn != null ? lsn.offset : -1L, true);
            appendJsonNum(sb, "tailer_entries_processed", engine.getTailerEntriesProcessed(), true);
            appendJsonNum(sb, "tailer_entries_accepted", engine.getTailerEntriesAccepted(), true);
            appendJsonNum(sb, "tailer_entries_skipped", engine.getTailerEntriesSkipped(), true);
            appendJsonNum(sb, "tailer_entries_shard_filtered",
                    engine.getTailerEntriesShardFiltered(), true);
            appendJsonNum(sb, "tailer_inserts", engine.getTailerInserts(), true);
            appendJsonNum(sb, "tailer_updates", engine.getTailerUpdates(), true);
            appendJsonNum(sb, "tailer_deletes", engine.getTailerDeletes(), true);
            appendJsonNum(sb, "tailer_ddl", engine.getTailerDdl(), true);
            appendJsonNum(sb, "tailer_batches", engine.getTailerBatchesProcessed(), true);
            appendJsonNum(sb, "apply_queue_size", engine.getApplyQueueSize(), true);
            appendJsonNum(sb, "apply_queue_capacity", engine.getApplyQueueCapacity(), true);
            appendJsonNum(sb, "apply_parallelism", engine.getApplyParallelism(), true);
            appendJsonNum(sb, "loaded_index_count", engine.getLoadedIndexCount(), true);
            appendJsonNum(sb, "total_estimated_memory_bytes",
                    engine.getTotalEstimatedMemoryBytes(), true);
            appendJsonNum(sb, "jvm_heap_used_bytes", heapUsed, true);
            appendJsonNum(sb, "jvm_heap_max_bytes", heapMax, true);
            appendJsonNum(sb, "jvm_heap_used_pct", heapPct, true);

            // --- GetShadowStatus ---
            LogSequenceNumber shadowLoaded = engine.getShadowLoadedLsn();
            LogSequenceNumber primaryAdvertised = engine.getPrimaryAdvertisedLsn();
            appendJsonBool(sb, "is_shadow", engine.isConfiguredAsShadow(), true);
            appendJsonBool(sb, "shadow_ready",
                    engine.isConfiguredAsShadow() ? engine.isShadowReady() : true, true);
            appendJsonNum(sb, "shadow_loaded_ledger_id",
                    shadowLoaded != null ? shadowLoaded.ledgerId : -1L, true);
            appendJsonNum(sb, "shadow_loaded_offset",
                    shadowLoaded != null ? shadowLoaded.offset : -1L, true);
            appendJsonNum(sb, "primary_advertised_ledger_id",
                    primaryAdvertised != null ? primaryAdvertised.ledgerId : -1L, true);
            appendJsonNum(sb, "primary_advertised_offset",
                    primaryAdvertised != null ? primaryAdvertised.offset : -1L, true);
            appendJsonNum(sb, "shadow_last_reload_timestamp_ms",
                    engine.getShadowLastReloadTimestampMs(), true);
            appendJsonNum(sb, "shadow_reload_count", engine.getShadowReloadCount(), true);
            appendJsonNum(sb, "applied_index_status_generation",
                    engine.getMinAppliedIndexStatusGeneration(), true);
            appendJsonNum(sb, "shadow_loaded_entry_timestamp_ms",
                    engine.getShadowLoadedEntryTimestamp(), true);

            // --- Search metrics (issue #541) ---
            // serviceImpl is set in start() before the HTTP server is mounted;
            // guard against null defensively in case this is ever called early.
            IndexingServiceImpl si = serviceImpl;
            appendJsonNum(sb, "search_requests_total",
                    si != null ? si.getSearchRequestsTotal() : 0L, true);
            appendJsonNum(sb, "search_errors_total",
                    si != null ? si.getSearchErrorsTotal() : 0L, true);
            appendJsonNum(sb, "search_readfilerange_calls_total",
                    si != null ? si.getSearchReadFileRangeCallsTotal() : 0L, true);
            appendJsonNum(sb, "search_readfilerange_bytes_total",
                    si != null ? si.getSearchReadFileRangeBytesTotal() : 0L, true);
            appendJsonNum(sb, "search_readfilerange_wait_nanos_total",
                    si != null ? si.getSearchReadFileRangeWaitNanosTotal() : 0L, true);
            appendJsonNum(sb, "search_segments_queried_total",
                    si != null ? si.getSearchSegmentsQueriedTotal() : 0L, true);
            appendJsonNum(sb, "search_cache_hits_total",
                    si != null ? si.getSearchCacheHitsTotal() : 0L, true);
            appendJsonNum(sb, "search_cache_misses_total",
                    si != null ? si.getSearchCacheMissesTotal() : 0L, false);

            sb.append("}\n");
            return sb.toString();
        }
    }

    // -------------------------------------------------------------------------
    // JSON helpers shared by StatusServlet (issue #541)
    // -------------------------------------------------------------------------

    private static void appendJsonStr(StringBuilder sb, String key, String value,
                                      boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ");
        if (value == null || value.isEmpty()) {
            sb.append("null");
        } else {
            sb.append('"');
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"') {
                    sb.append("\\\"");
                } else if (c == '\\') {
                    sb.append("\\\\");
                } else {
                    sb.append(c);
                }
            }
            sb.append('"');
        }
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendJsonNum(StringBuilder sb, String key, long value,
                                      boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ").append(value);
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendJsonBool(StringBuilder sb, String key, boolean value,
                                       boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ").append(value);
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }
}
