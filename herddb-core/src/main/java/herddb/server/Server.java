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

package herddb.server;

import herddb.auth.oidc.OidcConfiguration;
import herddb.auth.oidc.OidcTokenValidator;
import herddb.auth.oidc.PrincipalExtractor;
import herddb.auth.oidc.sasl.TokenAuthenticator;
import herddb.client.ClientConfiguration;
import herddb.cluster.BookKeeperDataStorageManager;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.EmbeddedBookie;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.core.DBManager;
import herddb.core.stats.ConnectionsInfo;
import herddb.core.stats.ConnectionsInfoProvider;
import herddb.file.FileBasedUserManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.log.CommitLogManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryLocalNodeIdManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.TableSpace;
import herddb.network.Channel;
import herddb.network.ServerHostData;
import herddb.network.ServerSideConnection;
import herddb.network.ServerSideConnectionAcceptor;
import herddb.network.netty.NettyChannelAcceptor;
import herddb.network.netty.NetworkUtils;
import herddb.security.SimpleSingleUserManager;
import herddb.security.UserManager;
import herddb.storage.DataStorageManager;
import herddb.utils.Version;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;

/**
 * HerdDB Server
 *
 * @author enrico.olivelli
 */
public class Server implements AutoCloseable, ServerSideConnectionAcceptor<ServerSideConnection>, ConnectionsInfoProvider {

    private static final Logger LOGGER = Logger.getLogger(Server.class.getName());
    private final DBManager manager;
    private final NettyChannelAcceptor networkServer;
    private final ServerConfiguration configuration;
    private final StatsProvider statsProvider;
    private final StatsLogger statsLogger;
    private final Path baseDirectory;
    private final Path dataDirectory;
    private final Path tmpDirectory;
    private final ServerHostData serverHostData;
    private final Map<Long, ServerSideConnectionPeer> connections = new ConcurrentHashMap<>();
    private final String mode;
    private final MetadataStorageManager metadataStorageManager;
    private final CommitLogManager commitLogManager;
    private String jdbcUrl;
    private UserManager userManager;
    private EmbeddedBookie embeddedBookie;
    private volatile TokenAuthenticator tokenAuthenticator;

    public UserManager getUserManager() {
        return userManager;
    }

    /**
     * Returns the token authenticator used by the OAUTHBEARER SASL mechanism,
     * or {@code null} if OIDC authentication is not configured.
     */
    public TokenAuthenticator getTokenAuthenticator() {
        return tokenAuthenticator;
    }

    public void setTokenAuthenticator(TokenAuthenticator tokenAuthenticator) {
        this.tokenAuthenticator = tokenAuthenticator;
    }

    private static TokenAuthenticator buildOidcTokenAuthenticator(ServerConfiguration configuration) throws IOException {
        String issuer = configuration.getString(ServerConfiguration.PROPERTY_OIDC_ISSUER_URL, "");
        if (issuer.isEmpty()) {
            throw new IOException(ServerConfiguration.PROPERTY_OIDC_ENABLED
                    + " is true but " + ServerConfiguration.PROPERTY_OIDC_ISSUER_URL + " is not set");
        }
        String audience = configuration.getString(ServerConfiguration.PROPERTY_OIDC_AUDIENCE, "");
        String usernameClaim = configuration.getString(ServerConfiguration.PROPERTY_OIDC_USERNAME_CLAIM, "");
        String jwksUri = configuration.getString(ServerConfiguration.PROPERTY_OIDC_JWKS_URI, "");
        OidcConfiguration oidcCfg = new OidcConfiguration(issuer);
        if (!jwksUri.isEmpty()) {
            oidcCfg.setJwksUri(jwksUri);
        } else {
            oidcCfg.discover();
        }
        final OidcTokenValidator validator = new OidcTokenValidator(
                oidcCfg, audience.isEmpty() ? null : audience,
                new PrincipalExtractor(usernameClaim));
        LOGGER.log(Level.INFO, "OIDC authentication enabled (issuer={0}, audience={1}, usernameClaim={2})",
                new Object[]{issuer, audience, usernameClaim});
        return validator::validate;
    }

    public void setUserManager(UserManager userManager) {
        this.userManager = userManager;
    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    public DBManager getManager() {
        return manager;
    }

    public NettyChannelAcceptor getNetworkServer() {
        return networkServer;
    }

    public Server(ServerConfiguration configuration) {
        this(configuration, null);
    }

    public Server(ServerConfiguration configuration, StatsProvider statsProvider) {
        this.statsProvider = statsProvider;
        this.statsLogger = statsProvider == null ? new NullStatsLogger() : statsProvider.getStatsLogger("");
        this.configuration = configuration;

        String nodeId = configuration.getString(ServerConfiguration.PROPERTY_NODEID, "");

        this.mode = configuration.getString(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_STANDALONE);
        this.baseDirectory = Paths.get(configuration.getString(ServerConfiguration.PROPERTY_BASEDIR, ServerConfiguration.PROPERTY_BASEDIR_DEFAULT)).toAbsolutePath();
        if (!mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
            try {
                Files.createDirectories(this.baseDirectory);
            } catch (IOException ignore) {
                LOGGER.log(Level.SEVERE, "Cannot create baseDirectory " + this.baseDirectory, ignore);
            }
        }
        this.dataDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_DATADIR, ServerConfiguration.PROPERTY_DATADIR_DEFAULT));
        this.tmpDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_TMPDIR, ServerConfiguration.PROPERTY_TMPDIR_DEFAULT));
        String usersfile = configuration.getString(ServerConfiguration.PROPERTY_USERS_FILE, ServerConfiguration.PROPERTY_USERS_FILE_DEFAULT);
        if (usersfile.isEmpty()) {
            this.userManager = new SimpleSingleUserManager(configuration);
        } else {
            try {
                Path userDirectoryFile = baseDirectory.resolve(usersfile).toAbsolutePath();
                LOGGER.log(Level.INFO, "Reading users from file " + userDirectoryFile);
                this.userManager = new FileBasedUserManager(userDirectoryFile);
            } catch (IOException error) {
                throw new RuntimeException(error);
            }
        }
        if (configuration.getBoolean(ServerConfiguration.PROPERTY_OIDC_ENABLED,
                ServerConfiguration.PROPERTY_OIDC_ENABLED_DEFAULT)) {
            try {
                this.tokenAuthenticator = buildOidcTokenAuthenticator(configuration);
            } catch (IOException err) {
                throw new RuntimeException("Failed to initialize OIDC token validator: " + err.getMessage(), err);
            }
        }
        this.metadataStorageManager = buildMetadataStorageManager();
        String host = configuration.getString(ServerConfiguration.PROPERTY_HOST, ServerConfiguration.PROPERTY_HOST_DEFAULT);
        int port = configuration.getInt(ServerConfiguration.PROPERTY_PORT, ServerConfiguration.PROPERTY_PORT_DEFAULT);
        if (!mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
            LOGGER.log(Level.INFO, "Configured network parameters: " + ServerConfiguration.PROPERTY_HOST + "={0}, "
                + ServerConfiguration.PROPERTY_PORT + "={1}", new Object[]{host, port});
        }
        if (host.trim().isEmpty()) {
            String _host = "0.0.0.0";
            LOGGER.log(Level.INFO, "As configuration parameter "
                    + ServerConfiguration.PROPERTY_HOST + " is {0}, I have choosen to use {1}."
                    + " Set to a non-empty value in order to use a fixed hostname", new Object[]{host, _host});
            host = _host;
        }
        if (port <= 0) {
            try {
                int _port = NetworkUtils.assignFirstFreePort();
                LOGGER.log(Level.INFO, "As configuration parameter "
                        + ServerConfiguration.PROPERTY_PORT + " is {0},I have choosen to listen on port {1}."
                        + " Set to a positive number in order to use a fixed port", new Object[]{Integer.toString(port), Integer.toString(_port)});
                port = _port;
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Cannot find a free port", err);
                throw new RuntimeException(err);
            }
        }
        String advertised_host = configuration.getString(ServerConfiguration.PROPERTY_ADVERTISED_HOST, host);
        if (advertised_host.trim().isEmpty() || advertised_host.equals("0.0.0.0")) {
            try {
                String _host = NetworkUtils.getLocalNetworkAddress();
                LOGGER.log(Level.INFO, "As configuration parameter "
                        + ServerConfiguration.PROPERTY_ADVERTISED_HOST + " is {0}, I have choosen to use {1}."
                        + " Set to a non-empty value in order to use a fixed hostname", new Object[]{advertised_host, _host});
                advertised_host = _host;
            } catch (IOException err) {
                LOGGER.log(Level.SEVERE, "Cannot get local host name", err);
                throw new RuntimeException(err);
            }
        }
        int advertised_port = configuration.getInt(ServerConfiguration.PROPERTY_ADVERTISED_PORT, port);

        HashMap<String, String> realData = new HashMap<>();
        realData.put(ServerConfiguration.PROPERTY_HOST, host);
        realData.put(ServerConfiguration.PROPERTY_PORT, port + "");
        if (!mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
            LOGGER.info("Public endpoint: " + ServerConfiguration.PROPERTY_ADVERTISED_HOST + "=" + advertised_host
                    + ", Public endpoint: " + ServerConfiguration.PROPERTY_ADVERTISED_PORT + "=" + advertised_port);
        }
        this.serverHostData = new ServerHostData(
                advertised_host,
                advertised_port,
                "",
                configuration.getBoolean(ServerConfiguration.PROPERTY_SSL, false),
                realData);

        if (nodeId.isEmpty()) {
            LocalNodeIdManager localNodeIdManager = buildLocalNodeIdManager();
            try {
                nodeId = localNodeIdManager.readLocalNodeId();
                if (nodeId == null) {
                    // we need to eagerly start the metadataStorageManager, for instance to open the connection to ZK
                    metadataStorageManager.start();
                    nodeId = metadataStorageManager.generateNewNodeId(configuration);
                    if (!mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
                        LOGGER.info("Generated new node id " + nodeId);
                    }
                    localNodeIdManager.persistLocalNodeId(nodeId);
                    // let downstream code see this new id (Embedded Bookie for instance)
                    configuration.set(ServerConfiguration.PROPERTY_NODEID, nodeId);
                }
            } catch (IOException | MetadataStorageManagerException error) {
                LOGGER.log(Level.SEVERE, "Fatal error while generating the local node ID", error);
                throw new RuntimeException(new Exception("Fatal error while generating the local node ID: " + error, error));
            }
        }

        // Ensure the metadata storage manager is started before building the
        // data storage manager — it may need a ZK connection for file server
        // discovery.  start() is idempotent, so this is safe even when we
        // already started it above to generate a new node ID.
        try {
            metadataStorageManager.start();
        } catch (MetadataStorageManagerException e) {
            throw new RuntimeException("Failed to start metadata storage manager", e);
        }

        this.commitLogManager = buildCommitLogManager();
        this.manager = new DBManager(nodeId,
                metadataStorageManager,
                buildDataStorageManager(nodeId),
                commitLogManager,
                tmpDirectory, serverHostData, configuration, statsLogger
        );

        this.manager.setClearAtBoot(configuration.getBoolean(ServerConfiguration.PROPERTY_CLEAR_AT_BOOT, ServerConfiguration.PROPERTY_CLEAR_AT_BOOT_DEFAULT));

        this.manager.setHaltOnTableSpaceBootError(configuration.getBoolean(ServerConfiguration.PROPERTY_HALT_ON_TABLESPACE_BOOT_ERROR, ServerConfiguration.PROPERTY_HALT_ON_TABLESPACE_BOOT_ERROR_DEAULT));
        this.manager.setConnectionsInfoProvider(this);
        this.manager.setServerToServerUsername(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_USERNAME, ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT));
        this.manager.setServerToServerPassword(configuration.getString(ServerConfiguration.PROPERTY_SERVER_TO_SERVER_PASSWORD, ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT));
        this.manager.setCheckpointPeriod(configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD_DEFAULT));
        this.manager.setCheckpointMemoryLimit(configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_MEMORY_LIMIT, ServerConfiguration.PROPERTY_CHECKPOINT_MEMORY_LIMIT_DEFAULT));
        this.manager.setAbandonedTransactionsTimeout(configuration.getLong(ServerConfiguration.PROPERTY_ABANDONED_TRANSACTIONS_TIMEOUT, ServerConfiguration.PROPERTY_ABANDONED_TRANSACTIONS_TIMEOUT_DEFAULT));

        boolean enforeLeadership = configuration.getBoolean(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP_DEFAULT);
        if (mode.equals(ServerConfiguration.PROPERTY_MODE_SHARED_STORAGE)) {
            // shared-storage replicas allow reads from followers but reject writes
            this.manager.setErrorIfNotLeader(false);
            this.manager.setReadOnlyReplica(true);
        } else {
            this.manager.setErrorIfNotLeader(enforeLeadership);
        }

        this.networkServer = buildChannelAcceptor();
        this.networkServer.setAcceptor(this);

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                jdbcUrl = "jdbc:herddb:server:" + serverHostData.getHost() + ":" + serverHostData.getPort();
                LOGGER.info("JDBC URL is not available. This server will not be accessible outside the JVM");
                break;
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                jdbcUrl = "jdbc:herddb:server:" + serverHostData.getHost() + ":" + serverHostData.getPort();
                LOGGER.log(Level.INFO, "Use this JDBC URL to connect to this server: {0}", new Object[]{jdbcUrl});
                break;
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
            case ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER:
                this.embeddedBookie = new EmbeddedBookie(baseDirectory, configuration, (ZookeeperMetadataStorageManager) this.metadataStorageManager, statsProvider);
                jdbcUrl = "jdbc:herddb:zookeeper:" + configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT) + configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT);
                LOGGER.log(Level.INFO, "Use this JDBC URL to connect to this HerdDB cluster: {0}", new Object[]{jdbcUrl});
                break;
            default:
                throw new IllegalStateException("invalid " + ServerConfiguration.PROPERTY_MODE + "=" + mode);
        }
        LOGGER.log(Level.INFO, "HerdDB version {0}", new Object[]{Version.getVERSION()});
        LOGGER.log(Level.INFO, "Local " + ServerConfiguration.PROPERTY_NODEID + " is {0}", new Object[]{nodeId});
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    private NettyChannelAcceptor buildChannelAcceptor() {
        String realHost = serverHostData.getAdditionalData().get(ServerConfiguration.PROPERTY_HOST);
        int realPort = Integer.parseInt(serverHostData.getAdditionalData().get(ServerConfiguration.PROPERTY_PORT));

        NettyChannelAcceptor acceptor = new NettyChannelAcceptor(realHost, realPort, serverHostData.isSsl(),
                statsLogger.scope("network"));
        // with 'local' mode we are disabling network by default
        // but in case you want to run benchmarks with 'local' mode using
        // the client on a separate process/machine you should be able
        // to enable network
        boolean isLocal = ServerConfiguration.PROPERTY_MODE_LOCAL.equals(mode);
        boolean nextworkEnabled = configuration.getBoolean(ServerConfiguration.PROPERTY_NETWORK_ENABLED,
                !isLocal && ServerConfiguration.PROPERTY_NETWORK_ENABLED_DEFAULT);
        if (!nextworkEnabled) {
            acceptor.setEnableRealNetwork(false);
            LOGGER.log(Level.FINE, "Local in-JVM acceptor on {0}:{1} ssl:{2}",
                    new Object[]{realHost, realPort, serverHostData.isSsl()});
        } else {
            LOGGER.log(Level.INFO, "Binding network acceptor to {0}:{1} ssl:{2}",
                    new Object[]{realHost, realPort, serverHostData.isSsl()});
        }

        int callbackThreads = configuration.getInt(
                ServerConfiguration.PROPERTY_NETWORK_CALLBACK_THREADS,
                ServerConfiguration.PROPERTY_NETWORK_CALLBACK_THREADS_DEFAULT);
        int workerThreads = configuration.getInt(
                ServerConfiguration.PROPERTY_NETWORK_WORKER_THREADS,
                ServerConfiguration.PROPERTY_NETWORK_WORKER_THREADS_DEFAULT);
        acceptor.setCallbackThreads(callbackThreads);
        acceptor.setWorkerThreads(workerThreads);

        return acceptor;
    }

    private MetadataStorageManager buildMetadataStorageManager() {
        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryMetadataStorageManager();
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                Path metadataDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_METADATADIR, ServerConfiguration.PROPERTY_METADATADIR_DEFAULT));
                return new FileMetadataStorageManager(metadataDirectory);
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
            case ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER:
            case ServerConfiguration.PROPERTY_MODE_SHARED_STORAGE:
                return new ZookeeperMetadataStorageManager(configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT),
                        configuration.getString(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, ServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT));
            default:
                throw new RuntimeException("invalid " + ServerConfiguration.PROPERTY_MODE + "=" + mode);
        }
    }

    private DataStorageManager buildDataStorageManager(String nodeId) {
        // shared-storage mode forces remote storage
        if (mode.equals(ServerConfiguration.PROPERTY_MODE_SHARED_STORAGE)) {
            return buildSharedStorageDataManager(nodeId);
        }
        String defaultStorageMode = mode.equals(ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER)
                ? ServerConfiguration.PROPERTY_STORAGE_MODE_BOOKKEEPER
                : ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL;
        String storageMode = configuration.getString(
                ServerConfiguration.PROPERTY_STORAGE_MODE, defaultStorageMode);
        LOGGER.log(Level.INFO, "Storage mode is {0}, server mode is {1}, for nodeId {2}",
                new Object[]{storageMode, mode, nodeId});
        switch (storageMode) {
            case ServerConfiguration.PROPERTY_STORAGE_MODE_LOCAL: {
                if (mode.equals(ServerConfiguration.PROPERTY_MODE_LOCAL)) {
                    return new MemoryDataStorageManager();
                }
                int diskswapThreshold = configuration.getInt(ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
                boolean requirefsync = configuration.getBoolean(ServerConfiguration.PROPERTY_REQUIRE_FSYNC, ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT);
                boolean pageodirect = configuration.getBoolean(ServerConfiguration.PROPERTY_PAGE_USE_ODIRECT, ServerConfiguration.PROPERTY_PAGE_USE_ODIRECT_DEFAULT);
                boolean indexodirect = configuration.getBoolean(ServerConfiguration.PROPERTY_INDEX_USE_ODIRECT, ServerConfiguration.PROPERTY_INDEX_USE_ODIRECT_DEFAULT);
                boolean hashChecksEnabled = configuration.getBoolean(ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED, ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT);
                boolean hashWritesEnabled = configuration.getBoolean(ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED, ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT);
                return new FileDataStorageManager(dataDirectory, tmpDirectory, diskswapThreshold, requirefsync, pageodirect, indexodirect, hashChecksEnabled, hashWritesEnabled, statsLogger);
            }
            case ServerConfiguration.PROPERTY_STORAGE_MODE_BOOKKEEPER: {
                if (!mode.equals(ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER)) {
                    throw new RuntimeException(ServerConfiguration.PROPERTY_STORAGE_MODE + "="
                            + ServerConfiguration.PROPERTY_STORAGE_MODE_BOOKKEEPER
                            + " is only supported with " + ServerConfiguration.PROPERTY_MODE + "="
                            + ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER);
                }
                int diskswapThreshold = configuration.getInt(ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
                return new BookKeeperDataStorageManager(nodeId, tmpDirectory, diskswapThreshold, (ZookeeperMetadataStorageManager) metadataStorageManager,
                        (BookkeeperCommitLogManager) this.commitLogManager, this.statsLogger);
            }
            case ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE: {
                int diskswapThreshold = configuration.getInt(ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
                String remoteServersConfig = configuration.getString(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS_DEFAULT);
                List<String> servers;
                boolean useZKDiscovery = false;
                if (!remoteServersConfig.isEmpty()) {
                    servers = Arrays.asList(remoteServersConfig.split(","));
                    LOGGER.log(Level.INFO, "Remote file services (static): {0}", servers);
                } else {
                    // Try ZK-based discovery
                    try {
                        servers = metadataStorageManager.listFileServers();
                        useZKDiscovery = true;
                        LOGGER.log(Level.INFO, "Remote file services (ZK discovery): {0}", servers);
                    } catch (Exception e) {
                        servers = Collections.emptyList();
                        LOGGER.log(Level.WARNING, "Failed to discover remote file servers via ZK", e);
                    }
                }
                Map<String, Object> clientConfig = new HashMap<>();
                clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                        configuration.getLong(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                                ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT_DEFAULT));
                clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                        configuration.getInt(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                                ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES_DEFAULT));
                RemoteFileServiceFactory factory = RemoteFileServiceFactory.load();
                RemoteFileClient client = factory.createClient(servers, clientConfig);
                // Set up ZK-based discovery listener if applicable
                if (useZKDiscovery) {
                    metadataStorageManager.addServiceDiscoveryListener(
                            new herddb.metadata.ServiceDiscoveryListener() {
                                @Override
                                public void onIndexingServicesChanged(List<String> currentAddresses) {
                                    // handled by ServerMain for IndexingServiceClient
                                }

                                @Override
                                public void onFileServersChanged(List<String> currentAddresses) {
                                    LOGGER.log(Level.INFO, "Remote file servers changed via ZK: {0}",
                                            currentAddresses);
                                    client.updateServers(currentAddresses);
                                }
                            });
                    // Re-query after listener registration to close the race window
                    // between the initial listFileServers() (which sets a one-shot ZK
                    // watcher) and addServiceDiscoveryListener().  If the file server
                    // registered in that gap the watcher notification was lost.
                    try {
                        List<String> currentServers = metadataStorageManager.listFileServers();
                        if (!currentServers.isEmpty()) {
                            client.updateServers(currentServers);
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Failed to re-query file servers after listener registration", e);
                    }
                }
                DataStorageManager dsm = factory.createDataStorageManager(
                        dataDirectory, tmpDirectory, diskswapThreshold, client);

                // Optionally enable checkpoint metadata publication to S3 for read replicas
                boolean publishToRemote = configuration.getBoolean(
                        ServerConfiguration.PROPERTY_CHECKPOINT_PUBLISH_TO_REMOTE,
                        ServerConfiguration.PROPERTY_CHECKPOINT_PUBLISH_TO_REMOTE_DEFAULT);
                if (publishToRemote) {
                    LOGGER.log(Level.INFO, "Checkpoint metadata publication to remote storage enabled");
                    SharedCheckpointMetadata metaMgr = factory.createSharedCheckpointMetadata(client);
                    RemoteFileStorageManager remoteDsm = (RemoteFileStorageManager) dsm;
                    remoteDsm.setSharedCheckpointMetadataManager(metaMgr);

                    // Enable deferred page deletion so replicas can safely consume old pages.
                    long minRetentionMillis = configuration.getLong(
                            ServerConfiguration.PROPERTY_CHECKPOINT_REPLICA_RETENTION_MIN_MILLIS,
                            ServerConfiguration.PROPERTY_CHECKPOINT_REPLICA_RETENTION_MIN_MILLIS_DEFAULT);
                    long maxRetentionMillis = configuration.getLong(
                            ServerConfiguration.PROPERTY_CHECKPOINT_REPLICA_RETENTION_MAX_MILLIS,
                            ServerConfiguration.PROPERTY_CHECKPOINT_REPLICA_RETENTION_MAX_MILLIS_DEFAULT);
                    final MetadataStorageManager msm = metadataStorageManager;
                    java.util.function.Function<String, herddb.log.LogSequenceNumber> supplier = tableSpaceUuid -> {
                        try {
                            return msm.getMinReplicaCheckpointLsn(tableSpaceUuid);
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING,
                                    "Failed to read min replica LSN from metadata for " + tableSpaceUuid, e);
                            return null;
                        }
                    };
                    remoteDsm.setRetentionPolicy(supplier, minRetentionMillis, maxRetentionMillis);
                    LOGGER.log(Level.INFO,
                            "Replica-aware page retention enabled: min={0}ms, max={1}ms",
                            new Object[]{minRetentionMillis, maxRetentionMillis});
                }

                return dsm;
            }
            default:
                throw new RuntimeException("invalid " + ServerConfiguration.PROPERTY_STORAGE_MODE + "=" + storageMode);
        }
    }

    private DataStorageManager buildSharedStorageDataManager(String nodeId) {
        int diskswapThreshold = configuration.getInt(ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS, ServerConfiguration.PROPERTY_DISK_SWAP_MAX_RECORDS_DEFAULT);
        String remoteServersConfig = configuration.getString(ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS, ServerConfiguration.PROPERTY_REMOTE_FILE_SERVERS_DEFAULT);
        List<String> servers;
        boolean useZKDiscovery = false;
        if (!remoteServersConfig.isEmpty()) {
            servers = Arrays.asList(remoteServersConfig.split(","));
            LOGGER.log(Level.INFO, "Shared-storage remote file services (static): {0}", servers);
        } else {
            try {
                servers = metadataStorageManager.listFileServers();
                useZKDiscovery = true;
                LOGGER.log(Level.INFO, "Shared-storage remote file services (ZK discovery): {0}", servers);
            } catch (Exception e) {
                servers = Collections.emptyList();
                LOGGER.log(Level.WARNING, "Failed to discover remote file servers via ZK for shared-storage", e);
            }
        }
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                configuration.getLong(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT,
                        ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_TIMEOUT_DEFAULT));
        clientConfig.put(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                configuration.getInt(ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES,
                        ServerConfiguration.PROPERTY_REMOTE_FILE_CLIENT_RETRIES_DEFAULT));
        RemoteFileServiceFactory factory = RemoteFileServiceFactory.load();
        RemoteFileClient client = factory.createClient(servers, clientConfig);
        // Set up ZK-based discovery listener if applicable
        if (useZKDiscovery) {
            metadataStorageManager.addServiceDiscoveryListener(
                    new herddb.metadata.ServiceDiscoveryListener() {
                        @Override
                        public void onIndexingServicesChanged(List<String> currentAddresses) {
                        }
                        @Override
                        public void onFileServersChanged(List<String> currentAddresses) {
                            LOGGER.log(Level.INFO, "Remote file servers changed via ZK (shared-storage): {0}",
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
            } catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "Failed to re-query file servers after listener registration (shared-storage)", e);
            }
        }
        // Create SharedCheckpointMetadata + PromotableRemoteFileDataStorageManager
        // (wraps a ReadReplicaDataStorageManager) for failover support.
        SharedCheckpointMetadata metaMgr = factory.createSharedCheckpointMetadata(client);
        return factory.createPromotableDataStorageManager(
                client, metaMgr, dataDirectory, tmpDirectory, diskswapThreshold);
    }

    protected CommitLogManager buildCommitLogManager() {

        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryCommitLogManager(false);
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
                Path logDirectory = this.baseDirectory.resolve(configuration.getString(ServerConfiguration.PROPERTY_LOGDIR, ServerConfiguration.PROPERTY_LOGDIR_DEFAULT));
                return new FileCommitLogManager(logDirectory,
                        configuration.getLong(ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE, ServerConfiguration.PROPERTY_MAX_LOG_FILE_SIZE_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH, ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES, ServerConfiguration.PROPERTY_MAX_UNSYNCHED_BATCH_BYTES_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_MAX_SYNC_TIME, ServerConfiguration.PROPERTY_MAX_SYNC_TIME_DEFAULT),
                        configuration.getBoolean(ServerConfiguration.PROPERTY_REQUIRE_FSYNC, ServerConfiguration.PROPERTY_REQUIRE_FSYNC_DEFAULT),
                        configuration.getBoolean(ServerConfiguration.PROPERTY_TXLOG_USE_ODIRECT, ServerConfiguration.PROPERTY_TXLOG_USE_ODIRECT_DEFAULT),
                        configuration.getInt(ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD, ServerConfiguration.PROPERTY_DEFERRED_SYNC_PERIOD_DEFAULT),
                        statsLogger.scope("txlog")
                );
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
            case ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER:
            case ServerConfiguration.PROPERTY_MODE_SHARED_STORAGE:
                BookkeeperCommitLogManager bkmanager = new BookkeeperCommitLogManager((ZookeeperMetadataStorageManager) this.metadataStorageManager, configuration, statsLogger);
                bkmanager.setAckQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_ACKQUORUMSIZE_DEFAULT));
                bkmanager.setEnsemble(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE, ServerConfiguration.PROPERTY_BOOKKEEPER_ENSEMBLE_DEFAULT));
                bkmanager.setWriteQuorumSize(configuration.getInt(ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_WRITEQUORUMSIZE_DEFAULT));
                long ledgersRetentionPeriod = configuration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD, ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD_DEFAULT);
                bkmanager.setLedgersRetentionPeriod(ledgersRetentionPeriod);
                long maxLedgerSizeBytes = configuration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE, ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_MAX_SIZE_DEFAULT);
                bkmanager.setMaxLedgerSizeBytes(maxLedgerSizeBytes);
                long maxIdleTime = configuration.getLong(ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME, ServerConfiguration.PROPERTY_BOOKKEEPER_MAX_IDLE_TIME_DEFAULT);
                bkmanager.setMaxIdleTime(maxIdleTime);
                long checkPointperiod = configuration.getLong(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD, ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD_DEFAULT);

                if (checkPointperiod > 0 && ledgersRetentionPeriod > 0) {
                    long limit = ledgersRetentionPeriod / 2;
                    if (checkPointperiod > limit) {
                        throw new RuntimeException(ServerConfiguration.PROPERTY_CHECKPOINT_PERIOD + "=" + checkPointperiod
                                + " must be less then " + ServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_RETENTION_PERIOD + "/2=" + limit);
                    }
                }

                return bkmanager;
            default:
                throw new RuntimeException();
        }
    }

    private LocalNodeIdManager buildLocalNodeIdManager() {
        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_LOCAL:
                return new MemoryLocalNodeIdManager(dataDirectory);
            case ServerConfiguration.PROPERTY_MODE_STANDALONE:
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
            case ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER:
            case ServerConfiguration.PROPERTY_MODE_SHARED_STORAGE:
                return new LocalNodeIdManager(dataDirectory);
            default:
                throw new RuntimeException();
        }
    }

    public void start() throws Exception {
        boolean startBookie = configuration.getBoolean(ServerConfiguration.PROPERTY_BOOKKEEPER_START,
                ServerConfiguration.PROPERTY_BOOKKEEPER_START_DEFAULT);
        if (startBookie && embeddedBookie != null) {
            this.embeddedBookie.start();
        }
        this.manager.start();
        this.networkServer.start();
    }

    public void waitForStandaloneBoot() throws Exception {
        waitForTableSpaceBoot(TableSpace.DEFAULT, true);
    }

    public void waitForTableSpaceBoot(String tableSpace, boolean leader) throws Exception {
        waitForTableSpaceBoot(tableSpace, 180000, leader);
    }

    public void waitForTableSpaceBoot(String tableSpace, int timeout, boolean leader) throws Exception {
        if (!this.manager.waitForTablespace(tableSpace, timeout, leader)) {
            throw new Exception("TableSpace " + tableSpace + " not started within " + timeout + " ms");
        }
    }

    public void waitForBootOfLocalTablespaces(int timeout) throws Exception {
        this.manager.waitForBootOfLocalTablespaces(timeout);
    }

    @Override
    public void close() throws Exception {
        try {
            networkServer.close();
        } catch (Throwable error) {
            LOGGER.log(Level.SEVERE, "error while stopping Network Manager" + error, error);
        }
        try {
            manager.close();
        } catch (Throwable error) {
            LOGGER.log(Level.SEVERE, "error while stopping embedded DBManager " + error, error);
        }

        if (embeddedBookie != null) {
            try {
                embeddedBookie.close();
            } catch (Throwable error) {
                LOGGER.log(Level.SEVERE, "error while stopping embedded bookie " + error, error);
            }
        }
    }

    protected ServerSideConnectionPeer buildPeer(Channel channel) {
        return new ServerSideConnectionPeer(channel, this);
    }

    @Override
    public ServerSideConnection createConnection(Channel channel) {
        ServerSideConnectionPeer peer = buildPeer(channel);
        connections.put(peer.getConnectionId(), peer);
        return peer;
    }

    Map<Long, ServerSideConnectionPeer> getConnections() {
        return connections;
    }

    void connectionClosed(ServerSideConnectionPeer connection) {
        connections.remove(connection.getConnectionId());
    }

    public String getNodeId() {
        return manager.getNodeId();
    }

    public ServerHostData getServerHostData() {
        return serverHostData;
    }

    public int getConnectionCount() {
        return connections.size();
    }

    @Override
    public ConnectionsInfo getActualConnections() {
        return new ConnectionsInfo(connections
                .values()
                .stream()
                .map(c -> {
                    return c.toConnectionInfo();
                })
                .collect(Collectors.toList()));
    }

    public EmbeddedBookie getEmbeddedBookie() {
        return embeddedBookie;
    }

}
