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
import herddb.file.FileDataStorageManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.server.RemoteFileClient;
import herddb.server.RemoteFileServiceFactory;
import herddb.server.RemoteFileStorageManager;
import herddb.server.SharedCheckpointMetadata;
import herddb.storage.DataStorageManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusServlet;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

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
                try {
                    RemoteFileServiceFactory factory = RemoteFileServiceFactory.load();
                    RemoteFileClient client = factory.createClient(servers, clientConfig);
                    if (useZKDiscovery) {
                        metadataStorageManager.addServiceDiscoveryListener(
                                new ServiceDiscoveryListener() {
                                    @Override
                                    public void onIndexingServicesChanged(List<String> currentAddresses) {
                                        // not relevant here
                                    }

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

        engine.setStatsLogger(statsLogger);
        IndexingServiceImpl serviceImpl = new IndexingServiceImpl(engine, statsLogger);
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
                metadataStorageManager.registerIndexingService(registeredServiceId, registeredServiceId);
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

    public void stop() throws InterruptedException {
        if (metadataStorageManager != null && registeredServiceId != null) {
            String id = registeredServiceId;
            registeredServiceId = null;
            try {
                metadataStorageManager.unregisterIndexingService(id);
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
}
