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

package herddb.client;

import herddb.client.impl.UnreachableServerException;
import herddb.model.TableSpace;
import herddb.network.ServerHostData;
import herddb.server.StaticClientSideMetadataProvider;
import herddb.utils.DataAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link ClientSideMetadataProvider} that discovers cluster topology by
 * querying the {@code sysnodes} and {@code systablespaces} virtual tables on
 * the HerdDB server itself. No ZooKeeper connection is required.
 *
 * <p>On the first metadata request the provider opens a short-lived internal
 * {@link HDBClient} (using {@link StaticClientSideMetadataProvider} pointing
 * at the configured bootstrap endpoint) to run the two discovery queries. The
 * results are cached; subsequent requests are served from the cache until
 * {@link #requestMetadataRefresh(Exception)} forces a re-query.
 *
 * <p>When the bootstrap server is unreachable during a refresh the provider
 * tries every node address that was discovered in the previous successful
 * refresh, enabling transparent failover across a cluster.
 *
 * <p>All authentication settings (DIGEST-MD5 credentials, OAUTHBEARER / OIDC
 * parameters) are forwarded from the outer {@link ClientConfiguration} to the
 * internal bootstrap client so that the discovery queries authenticate with
 * the same identity as regular application traffic.
 *
 * @author enrico.olivelli
 */
public final class ServerBasedClientSideMetadataProvider implements ClientSideMetadataProvider {

    private static final Logger LOG = Logger.getLogger(ServerBasedClientSideMetadataProvider.class.getName());

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final boolean bootstrapSsl;
    private final ClientConfiguration configuration;

    /* ---- cached topology (populated lazily, cleared on refresh) ---- */
    private final Map<String, String> tableSpaceLeaders = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> tableSpaceReplicas = new ConcurrentHashMap<>();
    private final Map<String, ServerHostData> nodeHostData = new ConcurrentHashMap<>();

    /**
     * All nodes discovered in the last successful refresh. Used as fallback
     * servers when the bootstrap endpoint is unreachable during a subsequent
     * refresh.
     *
     * <p>Package-private for testing only — do not access from outside this
     * package.
     */
    volatile List<ServerHostData> knownNodes = Collections.emptyList();

    /**
     * Guards concurrent metadata refreshes so that only one thread queries the
     * server at a time. Re-entrant to allow the same thread to call
     * {@link #getTableSpaceLeader} from within {@link #getTableSpaceReplicas}.
     */
    private final ReentrantLock refreshLock = new ReentrantLock();

    public ServerBasedClientSideMetadataProvider(
            String bootstrapHost,
            int bootstrapPort,
            boolean bootstrapSsl,
            ClientConfiguration configuration) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.bootstrapSsl = bootstrapSsl;
        this.configuration = configuration;
    }

    @Override
    public String getTableSpaceLeader(String tableSpace) throws ClientSideMetadataProviderException {
        tableSpace = tableSpace.toLowerCase();
        String cached = tableSpaceLeaders.get(tableSpace);
        if (cached != null) {
            return cached;
        }
        refreshMetadata();
        return tableSpaceLeaders.get(tableSpace);
    }

    @Override
    public ServerHostData getServerHostData(String nodeId) throws ClientSideMetadataProviderException {
        ServerHostData cached = nodeHostData.get(nodeId);
        if (cached != null) {
            return cached;
        }
        refreshMetadata();
        return nodeHostData.get(nodeId);
    }

    @Override
    public Set<String> getTableSpaceReplicas(String tableSpace) throws ClientSideMetadataProviderException {
        tableSpace = tableSpace.toLowerCase();
        Set<String> cached = tableSpaceReplicas.get(tableSpace);
        if (cached != null) {
            return cached;
        }
        // getTableSpaceLeader also populates the replica cache
        getTableSpaceLeader(tableSpace);
        cached = tableSpaceReplicas.get(tableSpace);
        return cached != null ? cached : Collections.emptySet();
    }

    @Override
    public void requestMetadataRefresh(Exception error) throws ClientSideMetadataProviderException {
        if (error instanceof UnreachableServerException) {
            // Targeted invalidation: only remove data for the failed node.
            String failedNode = ((UnreachableServerException) error).getNodeId();
            nodeHostData.remove(failedNode);
            tableSpaceLeaders.entrySet().removeIf(e -> failedNode.equalsIgnoreCase(e.getValue()));
            tableSpaceReplicas.clear();
        } else {
            // Full invalidation for any other error (leader changed, etc.).
            tableSpaceLeaders.clear();
            tableSpaceReplicas.clear();
            nodeHostData.clear();
        }
    }

    // -------------------------------------------------------------------------
    // Internal refresh logic
    // -------------------------------------------------------------------------

    private void refreshMetadata() throws ClientSideMetadataProviderException {
        refreshLock.lock();
        try {
            // Build the ordered list of servers to try: bootstrap first, then
            // every previously-discovered node (excluding the bootstrap to
            // avoid a duplicate on the first attempt).
            List<ServerHostData> candidates = new ArrayList<>();
            candidates.add(new ServerHostData(bootstrapHost, bootstrapPort, "", bootstrapSsl, new HashMap<>()));
            for (ServerHostData known : knownNodes) {
                if (!known.getHost().equals(bootstrapHost) || known.getPort() != bootstrapPort) {
                    candidates.add(known);
                }
            }

            ClientSideMetadataProviderException lastError = null;
            for (ServerHostData candidate : candidates) {
                try {
                    loadMetadataFromServer(candidate);
                    return; // success — stop trying
                } catch (ClientSideMetadataProviderException e) {
                    lastError = e;
                    LOG.log(Level.WARNING,
                            "Could not load metadata from {0}:{1,number,#}: {2}",
                            new Object[]{candidate.getHost(), candidate.getPort(), e.getMessage()});
                }
            }
            if (lastError != null) {
                throw lastError;
            }
            throw new ClientSideMetadataProviderException(
                    new Exception("No server available for metadata discovery"));
        } finally {
            refreshLock.unlock();
        }
    }

    /**
     * Opens a short-lived internal {@link HDBClient} to {@code server}, runs
     * {@code SELECT} queries against {@code sysnodes} and {@code systablespaces},
     * and updates the caches.
     *
     * <p>After creating the internal client the provider is immediately replaced
     * with a {@link StaticClientSideMetadataProvider} pointing at the known
     * endpoint. Without this override the {@code STANDALONE}-mode
     * {@link HDBClient} constructor would create another
     * {@link ServerBasedClientSideMetadataProvider}, and the first query on the
     * connection would trigger recursive metadata discovery, causing a
     * {@link StackOverflowError}.
     */
    private void loadMetadataFromServer(ServerHostData server)
            throws ClientSideMetadataProviderException {
        ClientConfiguration bootstrapConfig = buildBootstrapConfig(server);

        try (HDBClient bootstrapClient = new HDBClient(bootstrapConfig)) {
            // Replace the provider to prevent recursive discovery. The bootstrap
            // client connects to a fixed, known endpoint — no topology lookup needed.
            bootstrapClient.setClientSideMetadataProvider(
                    new StaticClientSideMetadataProvider(
                            server.getHost(), server.getPort(), server.isSsl()));

            try (HDBConnection conn = bootstrapClient.openConnection()) {

                // ---- 1. discover all nodes from sysnodes ----
                List<ServerHostData> discoveredNodes = new ArrayList<>();
                try (ScanResultSet scan = conn.executeScan(
                        TableSpace.DEFAULT,
                        "SELECT nodeid, address, ssl FROM sysnodes",
                        false, Collections.emptyList(),
                        0 /* tx */, 0 /* maxRows */, 1000 /* fetchSize */, false)) {
                    while (scan.hasNext()) {
                        DataAccessor row = scan.next();
                        String nodeId = row.get("nodeid").toString();
                        String address = row.get("address").toString();
                        Object sslVal = row.get("ssl");
                        boolean ssl = sslVal instanceof Number && ((Number) sslVal).intValue() != 0;

                        // "address" is stored as "host:port"
                        int colon = address.lastIndexOf(':');
                        String host = colon > 0 ? address.substring(0, colon) : address;
                        int port = colon > 0
                                ? Integer.parseInt(address.substring(colon + 1))
                                : ClientConfiguration.PROPERTY_SERVER_PORT_DEFAULT;

                        // Qualify short hostnames using the bootstrap server's domain suffix.
                        // In Kubernetes, InetAddress.getLocalHost().getHostName() returns the
                        // pod's short name (e.g. "server-0"), but DNS requires the full
                        // StatefulSet FQDN ("server-0.headless-svc.namespace.svc.cluster.local").
                        // If the bootstrap host is an FQDN (has dots) and the discovered host
                        // is a short name, append the same domain suffix to qualify it.
                        host = qualifyHostname(host, server.getHost());

                        ServerHostData hostData = new ServerHostData(host, port, "", ssl, new HashMap<>());
                        nodeHostData.put(nodeId, hostData);
                        discoveredNodes.add(hostData);
                    }
                }
                knownNodes = Collections.unmodifiableList(discoveredNodes);

                // ---- 2. discover tablespace leaders and replicas ----
                try (ScanResultSet scan = conn.executeScan(
                        TableSpace.DEFAULT,
                        "SELECT tablespace_name, leader, replica FROM systablespaces",
                        false, Collections.emptyList(),
                        0 /* tx */, 0 /* maxRows */, 10000 /* fetchSize */, false)) {
                    while (scan.hasNext()) {
                        DataAccessor row = scan.next();
                        String tsName = row.get("tablespace_name").toString().toLowerCase();
                        Object leaderObj = row.get("leader");
                        if (leaderObj == null) {
                            // tablespace exists but has no leader yet — skip
                            continue;
                        }
                        String leader = leaderObj.toString();
                        tableSpaceLeaders.put(tsName, leader);

                        Object replicaObj = row.get("replica");
                        if (replicaObj != null) {
                            String replicaStr = replicaObj.toString();
                            if (!replicaStr.isEmpty()) {
                                Set<String> replicaSet = new HashSet<>(Arrays.asList(replicaStr.split(",")));
                                tableSpaceReplicas.put(tsName, Collections.unmodifiableSet(replicaSet));
                            }
                        }
                    }
                }

            } catch (HDBException e) {
                throw new ClientSideMetadataProviderException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClientSideMetadataProviderException(e);
            }
        }
    }

    /**
     * Builds a minimal {@link ClientConfiguration} for the short-lived
     * internal bootstrap client. All authentication settings — DIGEST-MD5
     * credentials and every OIDC property — are copied from the outer
     * configuration so that discovery queries authenticate with the same
     * identity as regular application traffic.
     */
    private ClientConfiguration buildBootstrapConfig(ServerHostData server) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.set(ClientConfiguration.PROPERTY_MODE, ClientConfiguration.PROPERTY_MODE_STANDALONE);
        cfg.set(ClientConfiguration.PROPERTY_SERVER_ADDRESS, server.getHost());
        cfg.set(ClientConfiguration.PROPERTY_SERVER_PORT, server.getPort());
        cfg.set(ClientConfiguration.PROPERTY_SERVER_SSL, server.isSsl());

        // Forward credentials
        cfg.set(ClientConfiguration.PROPERTY_CLIENT_USERNAME,
                configuration.getString(ClientConfiguration.PROPERTY_CLIENT_USERNAME,
                        ClientConfiguration.PROPERTY_CLIENT_USERNAME_DEFAULT));
        cfg.set(ClientConfiguration.PROPERTY_CLIENT_PASSWORD,
                configuration.getString(ClientConfiguration.PROPERTY_CLIENT_PASSWORD,
                        ClientConfiguration.PROPERTY_CLIENT_PASSWORD_DEFAULT));

        // Forward auth mechanism and every OIDC property so that the internal
        // client authenticates with the same identity as the outer client.
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_AUTH_MECH);
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_OIDC_ISSUER_URL);
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_OIDC_CLIENT_ID);
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_OIDC_CLIENT_SECRET);
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_OIDC_SCOPE);
        copyIfPresent(cfg, ClientConfiguration.PROPERTY_OIDC_TOKEN);

        // Forward network timeout so the bootstrap client respects the same
        // timeout policy as the outer client.
        int networkTimeout = configuration.getInt(
                ClientConfiguration.PROPERTY_NETWORK_TIMEOUT,
                ClientConfiguration.PROPERTY_NETWORK_TIMEOUT_DEFAULT);
        if (networkTimeout > 0) {
            cfg.set(ClientConfiguration.PROPERTY_NETWORK_TIMEOUT, networkTimeout);
        }

        // Short operation timeout — discovery should not stall for minutes.
        cfg.set(ClientConfiguration.PROPERTY_TIMEOUT, 30_000);
        // One connection per server is enough for two small queries.
        cfg.set(ClientConfiguration.PROPERTY_MAX_CONNECTIONS_PER_SERVER, 1);
        // Limit retries in the discovery path to fail fast and try the next
        // known node.
        cfg.set(ClientConfiguration.PROPERTY_MAX_OPERATION_RETRY_COUNT, 3);

        return cfg;
    }

    private void copyIfPresent(ClientConfiguration target, String key) {
        String value = configuration.getString(key, "");
        if (!value.isEmpty()) {
            target.set(key, value);
        }
    }

    /**
     * Qualifies a short hostname by appending the domain suffix from the
     * bootstrap host, when appropriate.
     *
     * <p>In Kubernetes StatefulSet deployments, a server's
     * {@code InetAddress.getLocalHost().getHostName()} returns only the pod's
     * short name (e.g. {@code server-0}), but DNS resolution across pods
     * requires the full headless-service FQDN
     * ({@code server-0.headless-svc.namespace.svc.cluster.local}).  The server
     * stores its short name in {@code sysnodes.address}; without qualification,
     * the client cannot resolve that name from another pod.
     *
     * <p>If {@code discoveredHost} contains no dots (i.e. it is a short name)
     * and {@code knownFqdn} is an actual FQDN (has at least one dot), this
     * method appends the domain suffix of {@code knownFqdn} to
     * {@code discoveredHost}.  The resulting qualified name can be resolved
     * from any pod in the same namespace.
     *
     * <p>If {@code discoveredHost} already has dots, or {@code knownFqdn} has
     * no dots, the original {@code discoveredHost} is returned unchanged.
     *
     * <p>Package-private for testing.
     *
     * @param discoveredHost hostname from {@code sysnodes.address} (may be short)
     * @param knownFqdn      host the bootstrap client actually connected to
     * @return a qualified hostname suitable for direct TCP connection
     */
    static String qualifyHostname(String discoveredHost, String knownFqdn) {
        // Already an FQDN — nothing to do.
        if (discoveredHost.contains(".")) {
            return discoveredHost;
        }
        // No domain to borrow from.
        int dotIdx = knownFqdn.indexOf('.');
        if (dotIdx < 0) {
            return discoveredHost;
        }
        // Append the domain portion of the known FQDN (including the leading dot).
        return discoveredHost + knownFqdn.substring(dotIdx);
    }
}
