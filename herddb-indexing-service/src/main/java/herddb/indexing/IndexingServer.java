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

import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.ServiceDiscoveryListener;
import herddb.server.DynamicServiceClient;
import herddb.storage.DataStorageManager;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.lang.reflect.Constructor;
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
                    Class<?> clientClass = Class.forName("herddb.remote.RemoteFileServiceClient");
                    Object client = clientClass.getConstructor(List.class, Map.class)
                            .newInstance(servers, clientConfig);
                    if (useZKDiscovery && client instanceof DynamicServiceClient) {
                        DynamicServiceClient dynamicClient = (DynamicServiceClient) client;
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
                                        dynamicClient.updateServers(currentAddresses);
                                    }
                                });
                    }
                    Class<?> storageClass = Class.forName("herddb.remote.RemoteFileDataStorageManager");
                    Constructor<?> ctor = storageClass.getConstructor(
                            Path.class, Path.class, int.class, clientClass);
                    return (DataStorageManager) ctor.newInstance(
                            dataDir, dataDir, Integer.MAX_VALUE, client);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException("Cannot create RemoteFileDataStorageManager. "
                            + "Ensure herddb-remote-file-service is on the classpath.", e);
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

        // Initialize metrics
        statsProvider = new PrometheusMetricsProvider();
        PropertiesConfiguration statsConfig = new PropertiesConfiguration();
        statsConfig.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        statsProvider.start(statsConfig);
        StatsLogger statsLogger = statsProvider.getStatsLogger("");

        engine.setStatsLogger(statsLogger);
        IndexingServiceImpl serviceImpl = new IndexingServiceImpl(engine, statsLogger);
        server = ServerBuilder.forPort(port)
                .addService(serviceImpl)
                .build()
                .start();

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
        }

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
