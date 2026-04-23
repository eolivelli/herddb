/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.log.LogNotAvailableException;
import herddb.log.LogSequenceNumber;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.DDLException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceAlreadyExistsException;
import herddb.model.TableSpaceDoesNotExistException;
import herddb.model.TableSpaceReplicaState;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Metadata storage manager over Zookeeper
 *
 * @author enrico.olivelli
 */
public class ZookeeperMetadataStorageManager extends MetadataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperMetadataStorageManager.class.getName());

    private ZooKeeper zooKeeper;
    private final String zkAddress;
    private final int zkSessionTimeout;
    private final String basePath;
    private final String ledgersPath;
    private final String tableSpacesPath;
    private final String tableSpacesReplicasPath;
    private final String nodesPath;
    private final String indexingServicesPath;
    private final String indexingServicesInstancesPath;
    private final String indexingServicesStatePath;
    private final String fileServersPath;
    private final String checkpointsPath;
    private volatile boolean started;

    // Checkpoint LSN watchers for shared-storage read replicas
    private final ConcurrentHashMap<String, Consumer<LogSequenceNumber>> checkpointWatchers = new ConcurrentHashMap<>();

    // Watchers for indexing-service per-primary checkpoint-state znodes, keyed by instanceId.
    private final ConcurrentHashMap<Integer, Consumer<IndexingServiceCheckpointState>>
            indexingServiceCheckpointWatchers = new ConcurrentHashMap<>();

    // For re-registration after session expiry
    private volatile String registeredIndexingServiceId;
    private volatile String registeredIndexingServiceAddress;
    private volatile IndexingServiceInstanceDescriptor registeredIndexingServiceDescriptor;
    private volatile String registeredFileServerId;
    private volatile String registeredFileServerAddress;

    private final Watcher mainWatcher = (WatchedEvent event) -> {
        switch (event.getState()) {
            case SyncConnected:
            case SaslAuthenticated:
                notifyMetadataChanged("zkevent " + event.getState() + " " + event.getType() + " " + event.getPath());
                break;
            default:
                // ignore
                break;

        }
    };

    private final Watcher indexingServicesWatcher = (WatchedEvent event) -> {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                // listIndexingServiceInstances re-arms the watch and emits both the
                // legacy address-only callback and the new descriptor callback.
                listIndexingServiceInstances();
            } catch (MetadataStorageManagerException e) {
                LOGGER.log(Level.WARNING, "Error refreshing indexing services list", e);
            }
        }
    };

    private final Watcher fileServersWatcher = (WatchedEvent event) -> {
        if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                List<String> addresses = listFileServers();
                notifyFileServersChanged(addresses);
            } catch (MetadataStorageManagerException e) {
                LOGGER.log(Level.WARNING, "Error refreshing file servers list", e);
            }
        }
    };

    public String getBasePath() {
        return basePath;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public int getZkSessionTimeout() {
        return zkSessionTimeout;
    }

    private synchronized void restartZooKeeper() throws IOException, InterruptedException {
        ZooKeeper old = zooKeeper;
        if (old != null) {
            old.close();
        }
        CountDownLatch firstConnectionLatch = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(zkAddress, zkSessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                switch (event.getState()) {
                    case SyncConnected:
                    case SaslAuthenticated:
                        firstConnectionLatch.countDown();
                        notifyMetadataChanged("zkevent " + event.getState() + " " + event.getType() + " " + event.getPath());
                        break;
                    default:
                        // ignore
                        break;
                }
            }
        });
        if (!firstConnectionLatch.await(zkSessionTimeout, TimeUnit.SECONDS)) {
            zk.close();
            throw new IOException("Could not connect to zookeeper at " + zkAddress + " within " + zkSessionTimeout + " ms");
        }
        this.zooKeeper = zk;
        LOGGER.info("Connected to ZK " + zk);

        // Re-register services after session expiry. Prefer the descriptor-based
        // registration when available (role/instanceId/shadowOf preserved).
        if (registeredIndexingServiceDescriptor != null) {
            try {
                registerIndexingServiceInstance(registeredIndexingServiceDescriptor);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to re-register indexing service instance after session expiry", e);
            }
        } else if (registeredIndexingServiceId != null) {
            try {
                registerIndexingService(registeredIndexingServiceId, registeredIndexingServiceAddress);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to re-register indexing service after session expiry", e);
            }
        }
        if (registeredFileServerId != null) {
            try {
                registerFileServer(registeredFileServerId, registeredFileServerAddress);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to re-register file server after session expiry", e);
            }
        }
    }

    private void handleSessionExpiredError(Throwable error) {
        if (!(error instanceof KeeperException.SessionExpiredException)) {
            return;
        }
        try {
            restartZooKeeper();
        } catch (IOException | InterruptedException err) {
            LOGGER.log(Level.SEVERE, "Error handling session expired", err);
        }
    }

    public ZookeeperMetadataStorageManager(String zkAddress, int zkSessionTimeout, String basePath) {
        this.zkAddress = zkAddress;
        this.zkSessionTimeout = zkSessionTimeout;
        this.basePath = basePath;
        this.ledgersPath = basePath + "/ledgers"; // ledgers/TABLESPACEUUID
        this.tableSpacesPath = basePath + "/tableSpaces";  // tableSpaces/TABLESPACENAME
        this.tableSpacesReplicasPath = basePath + "/replicas";  // replicas/TABLESPACEUUID
        this.nodesPath = basePath + "/nodes";
        this.indexingServicesPath = basePath + "/indexingServices";
        this.indexingServicesInstancesPath = indexingServicesPath + "/instances";
        this.indexingServicesStatePath = indexingServicesPath + "/state";
        this.fileServersPath = basePath + "/fileServers";
        this.checkpointsPath = basePath + "/checkpoints"; // checkpoints/TABLESPACEUUID
    }

    @Override
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
    public void start() throws MetadataStorageManagerException {
        start(true);
    }

    public synchronized void start(boolean formatIfNeeded) throws MetadataStorageManagerException {
        if (started) {
            return;
        }
        LOGGER.log(Level.SEVERE, "start, zkAddress " + zkAddress + ", zkSessionTimeout:" + zkSessionTimeout + ", basePath:" + basePath);
        try {
            restartZooKeeper();
            if (formatIfNeeded) {
                ensureRoot();
            }
            started = true;
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new MetadataStorageManagerException(err);
        }
    }

    private void ensureRoot() throws KeeperException, InterruptedException, IOException {
        try {
            zooKeeper.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(tableSpacesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(tableSpacesReplicasPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(ledgersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(nodesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(indexingServicesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(indexingServicesInstancesPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(indexingServicesStatePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }
        try {
            zooKeeper.create(fileServersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ok) {
        }

    }

    public synchronized ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public synchronized ZooKeeper ensureZooKeeper() throws KeeperException, InterruptedException, IOException {
        if (!started) {
            throw new IOException("MetadataStorageManager not yet started");
        }
        if (zooKeeper == null) {
            restartZooKeeper();
        }
        return this.zooKeeper;
    }

    /**
     * Let (embedded) brokers read actual list of ledgers used. in order to perform extrernal clean ups
     *
     * @param zooKeeper
     * @param ledgersPath
     * @param tableSpaceUUID
     * @return
     * @throws LogNotAvailableException
     */
    public static LedgersInfo readActualLedgersListFromZookeeper(ZooKeeper zooKeeper, String ledgersPath, String tableSpaceUUID) throws LogNotAvailableException {
        while (zooKeeper.getState() != ZooKeeper.States.CLOSED) {
            try {
                Stat stat = new Stat();
                byte[] actualLedgers = zooKeeper.getData(ledgersPath + "/" + tableSpaceUUID, false, stat);
                return LedgersInfo.deserialize(actualLedgers, stat.getVersion());
            } catch (KeeperException.NoNodeException firstboot) {
                LOGGER.log(Level.INFO, "node " + ledgersPath + "/" + tableSpaceUUID + " not found");
                return LedgersInfo.deserialize(null, -1); // -1 is a special ZK version
            } catch (KeeperException.ConnectionLossException error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath + "/" + tableSpaceUUID, error);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException err) {
                    // maybe stopping
                    throw new LogNotAvailableException(err);
                }
            } catch (Exception error) {
                LOGGER.log(Level.SEVERE, "error while loading actual ledgers list at " + ledgersPath + "/" + tableSpaceUUID, error);
                throw new LogNotAvailableException(error);
            }
        }
        throw new LogNotAvailableException(new Exception("zk client closed"));
    }

    public LedgersInfo getActualLedgersList(String tableSpaceUUID) throws LogNotAvailableException {
        try {
            return readActualLedgersListFromZookeeper(ensureZooKeeper(), ledgersPath, tableSpaceUUID);
        } catch (IOException | InterruptedException | KeeperException err) {
            throw new LogNotAvailableException(err);
        }
    }

    public void saveActualLedgersList(String tableSpaceUUID, LedgersInfo info) throws LogNotAvailableException {
        byte[] actualLedgers = info.serialize();
        try {
            while (true) {
                try {
                    try {
                        Stat newStat = ensureZooKeeper().setData(ledgersPath + "/" + tableSpaceUUID, actualLedgers, info.getZkVersion());
                        info.setZkVersion(newStat.getVersion());
                        LOGGER.log(Level.SEVERE, "save new ledgers list " + info + " to " + ledgersPath + "/" + tableSpaceUUID);
                        return;
                    } catch (KeeperException.NoNodeException firstboot) {
                        ensureZooKeeper().create(ledgersPath + "/" + tableSpaceUUID, actualLedgers, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException.BadVersionException fenced) {
                        throw new LogNotAvailableException(new Exception("ledgers actual list was fenced, expecting version " + info.getZkVersion() + " " + fenced, fenced).fillInStackTrace());
                    }
                } catch (KeeperException.ConnectionLossException anyError) {
                    LOGGER.log(Level.SEVERE, "temporary error", anyError);
                    Thread.sleep(10000);
                } catch (Exception anyError) {
                    handleSessionExpiredError(anyError);
                    throw new LogNotAvailableException(anyError);
                }
            }
        } catch (InterruptedException stop) {
            LOGGER.log(Level.SEVERE, "fatal error", stop);
            throw new LogNotAvailableException(stop);

        }
    }

    private static class TableSpaceList {

        private final int version;
        private final List<String> tableSpaces;

        public TableSpaceList(int version, List<String> tableSpaces) {
            this.version = version;
            this.tableSpaces = tableSpaces;
        }

    }

    private TableSpaceList listTablesSpaces() throws KeeperException, InterruptedException, IOException {
        Stat stat = new Stat();
        List<String> children = ensureZooKeeper().getChildren(tableSpacesPath, mainWatcher, stat);
        return new TableSpaceList(stat.getVersion(), children);
    }

    private void createTableSpaceNode(TableSpace tableSpace) throws KeeperException, InterruptedException, IOException, TableSpaceAlreadyExistsException {
        try {
            ensureZooKeeper().create(tableSpacesPath + "/" + tableSpace.name.toLowerCase(), tableSpace.serialize(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException err) {
            throw new TableSpaceAlreadyExistsException(tableSpace.uuid);
        }
    }

    private boolean updateTableSpaceNode(TableSpace tableSpace, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException, TableSpaceDoesNotExistException {
        try {
            ensureZooKeeper().setData(tableSpacesPath + "/" + tableSpace.name.toLowerCase(), tableSpace.serialize(), metadataStorageVersion);
            notifyMetadataChanged("updateTableSpaceNode " + tableSpace + " metadataStorageVersion " + metadataStorageVersion);
            return true;
        } catch (KeeperException.BadVersionException changed) {
            return false;
        } catch (KeeperException.NoNodeException changed) {
            throw new TableSpaceDoesNotExistException(tableSpace.uuid);
        }
    }

    private boolean deleteTableSpaceNode(String tableSpaceName, int metadataStorageVersion) throws KeeperException, InterruptedException, IOException, TableSpaceDoesNotExistException {
        try {
            ensureZooKeeper().delete(tableSpacesPath + "/" + tableSpaceName.toLowerCase(), metadataStorageVersion);
            notifyMetadataChanged("deleteTableSpaceNode " + tableSpaceName + " metadataStorageVersion " + metadataStorageVersion);
            return true;
        } catch (KeeperException.BadVersionException changed) {
            return false;
        } catch (KeeperException.NoNodeException changed) {
            throw new TableSpaceDoesNotExistException(tableSpaceName);
        }
    }

    @Override
    public boolean ensureDefaultTableSpace(String localNodeId,
                                        String initialReplicaList,
                                        long maxLeaderInactivityTime,
                                        int expectedReplicaCount) throws MetadataStorageManagerException {
        try {
            TableSpaceList list = listTablesSpaces();
            if (!list.tableSpaces.contains(TableSpace.DEFAULT)) {
                TableSpace tableSpace = TableSpace.builder()
                        .leader(localNodeId)
                        .replica(initialReplicaList)
                        .expectedReplicaCount(1)
                        .maxLeaderInactivityTime(maxLeaderInactivityTime)
                        .expectedReplicaCount(expectedReplicaCount)
                        .name(TableSpace.DEFAULT)
                        .build();
                createTableSpaceNode(tableSpace);
                return true;
            } else {
                return false;
            }
        } catch (TableSpaceAlreadyExistsException err) {
            // not a problem
            return false;
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void close() throws MetadataStorageManagerException {
        try {
            synchronized (this) {
                if (zooKeeper == null) {
                    return;
                }
            }
            ZooKeeper zk = ensureZooKeeper();
            if (zk != null) {
                try {
                    zk.close();
                } catch (InterruptedException err) {
                }
            }
        } catch (IOException | InterruptedException | KeeperException err) {

        }
    }

    @Override
    public Collection<String> listTableSpaces() throws MetadataStorageManagerException {
        try {
            return listTablesSpaces().tableSpaces;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public TableSpace describeTableSpace(String name) throws MetadataStorageManagerException {
        name = name.toLowerCase();
        try {
            Stat stat = new Stat();
            byte[] result = ensureZooKeeper().getData(tableSpacesPath + "/" + name.toLowerCase(), mainWatcher, stat);
            return TableSpace.deserialize(result, stat.getVersion(), stat.getCtime());
        } catch (KeeperException.NoNodeException ex) {
            return null;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException {

        try {
            createTableSpaceNode(tableSpace);
            notifyMetadataChanged("registerTableSpace " + tableSpace);
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous == null || previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = updateTableSpaceNode(tableSpace, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged("updateTableSpace " + tableSpace);
            }
            return result;
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException {
        if (previous == null || previous.metadataStorageVersion == null) {
            throw new MetadataStorageManagerException("metadataStorageVersion not read from ZK");
        }
        try {
            boolean result = deleteTableSpaceNode(name, (Integer) previous.metadataStorageVersion);
            if (result) {
                notifyMetadataChanged("dropTableSpace " + name);
            }
        } catch (KeeperException | InterruptedException | IOException ex) {
            handleSessionExpiredError(ex);
            throw new MetadataStorageManagerException(ex);
        }
    }

    @Override
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        try {
            List<String> children = ensureZooKeeper().getChildren(nodesPath, mainWatcher, null);
            List<NodeMetadata> result = new ArrayList<>();
            for (String child : children) {
                NodeMetadata nodeMetadata = getNode(child);
                result.add(nodeMetadata);
            }
            return result;
        } catch (IOException | InterruptedException | KeeperException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }

    }

    @Override
    public void clear() throws MetadataStorageManagerException {
        try {
            List<String> children = ensureZooKeeper().getChildren(nodesPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(nodesPath + "/" + child, -1);
            }
            children = ensureZooKeeper().getChildren(tableSpacesPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(tableSpacesPath + "/" + child, -1);
            }
            children = ensureZooKeeper().getChildren(ledgersPath, false);
            for (String child : children) {
                ensureZooKeeper().delete(ledgersPath + "/" + child, -1);
            }
            {
                // Delete children of indexing-services/instances (ephemeral, but
                // also handles leftovers from prior sessions) and state (persistent).
                try {
                    List<String> instanceChildren =
                            ensureZooKeeper().getChildren(indexingServicesInstancesPath, false);
                    for (String child : instanceChildren) {
                        ensureZooKeeper().delete(indexingServicesInstancesPath + "/" + child, -1);
                    }
                } catch (KeeperException.NoNodeException ok) {
                    // path absent on legacy clusters — nothing to clean
                }
                try {
                    List<String> stateChildren =
                            ensureZooKeeper().getChildren(indexingServicesStatePath, false);
                    for (String child : stateChildren) {
                        ensureZooKeeper().delete(indexingServicesStatePath + "/" + child, -1);
                    }
                } catch (KeeperException.NoNodeException ok) {
                    // path absent on legacy clusters — nothing to clean
                }
                // Finally, also delete any stray direct children under indexingServicesPath
                // (legacy flat registrations or the "instances" / "state" subnodes
                // themselves on a full wipe). We preserve the subnodes on a normal
                // clear because ensureRoot() re-creates them.
                List<String> indexingChildren = ensureZooKeeper().getChildren(indexingServicesPath, false);
                for (String child : indexingChildren) {
                    if ("instances".equals(child) || "state".equals(child)) {
                        continue;
                    }
                    ensureZooKeeper().delete(indexingServicesPath + "/" + child, -1);
                }
            }
            {
                List<String> fileServerChildren = ensureZooKeeper().getChildren(fileServersPath, false);
                for (String child : fileServerChildren) {
                    ensureZooKeeper().delete(fileServersPath + "/" + child, -1);
                }
            }
        } catch (InterruptedException | KeeperException | IOException error) {
            LOGGER.log(Level.SEVERE, "Cannot clear metadata", error);
            throw new MetadataStorageManagerException(error);
        }

    }

    private NodeMetadata getNode(String nodeId) throws KeeperException, IOException, InterruptedException {
        Stat stat = new Stat();
        byte[] node = ensureZooKeeper().getData(nodesPath + "/" + nodeId, mainWatcher, stat);
        NodeMetadata nodeMetadata = NodeMetadata.deserialize(node, stat.getVersion());
        return nodeMetadata;
    }

    @Override
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
        try {
            String path = nodesPath + "/" + nodeMetadata.nodeId;
            LOGGER.severe("registerNode at " + path + " -> " + nodeMetadata);
            byte[] data = nodeMetadata.serialize();
            try {
                ensureZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ok) {
                LOGGER.severe("registerNode at " + path + " " + ok);
                ensureZooKeeper().setData(path, data, -1);
            }
            notifyMetadataChanged("registerNode " + nodeMetadata);
        } catch (IOException | InterruptedException | KeeperException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }

    }

    @Override
    public void registerIndexingService(String serviceId, String address) throws MetadataStorageManagerException {
        // Legacy shim: callers that only know the address register as a primary
        // with instanceId=0. New code should use registerIndexingServiceInstance.
        registerIndexingServiceInstance(
                IndexingServiceInstanceDescriptor.primary(serviceId, address, 0));
    }

    @Override
    public void unregisterIndexingService(String serviceId) throws MetadataStorageManagerException {
        unregisterIndexingServiceInstance(serviceId);
    }

    @Override
    public List<String> listIndexingServices() throws MetadataStorageManagerException {
        List<IndexingServiceInstanceDescriptor> instances = listIndexingServiceInstances();
        List<String> addresses = new ArrayList<>(instances.size());
        for (IndexingServiceInstanceDescriptor d : instances) {
            addresses.add(d.getAddress());
        }
        return addresses;
    }

    @Override
    public void registerIndexingServiceInstance(IndexingServiceInstanceDescriptor descriptor)
            throws MetadataStorageManagerException {
        try {
            String path = indexingServicesInstancesPath + "/" + descriptor.getServiceId();
            byte[] data = descriptor.serialize();
            try {
                ensureZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException ok) {
                ensureZooKeeper().setData(path, data, -1);
            }
            this.registeredIndexingServiceDescriptor = descriptor;
            this.registeredIndexingServiceId = descriptor.getServiceId();
            this.registeredIndexingServiceAddress = descriptor.getAddress();
            LOGGER.log(Level.INFO, "Registered indexing service instance: {0}", descriptor);
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void unregisterIndexingServiceInstance(String serviceId) throws MetadataStorageManagerException {
        try {
            ensureZooKeeper().delete(indexingServicesInstancesPath + "/" + serviceId, -1);
            this.registeredIndexingServiceDescriptor = null;
            this.registeredIndexingServiceId = null;
            this.registeredIndexingServiceAddress = null;
            LOGGER.log(Level.INFO, "Unregistered indexing service instance: {0}", serviceId);
        } catch (KeeperException.NoNodeException ok) {
            // already gone
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public List<IndexingServiceInstanceDescriptor> listIndexingServiceInstances()
            throws MetadataStorageManagerException {
        try {
            List<String> children;
            try {
                children = ensureZooKeeper().getChildren(indexingServicesInstancesPath, indexingServicesWatcher);
            } catch (KeeperException.NoNodeException absent) {
                // Path has not yet been created (fresh cluster before start()
                // formatted the tree). Nothing to return; no watch to install.
                return Collections.emptyList();
            }
            List<IndexingServiceInstanceDescriptor> result = new ArrayList<>(children.size());
            List<String> addresses = new ArrayList<>(children.size());
            for (String child : children) {
                try {
                    byte[] data = ensureZooKeeper().getData(
                            indexingServicesInstancesPath + "/" + child, false, null);
                    IndexingServiceInstanceDescriptor descriptor =
                            IndexingServiceInstanceDescriptor.deserialize(data);
                    result.add(descriptor);
                    addresses.add(descriptor.getAddress());
                } catch (KeeperException.NoNodeException gone) {
                    // node disappeared between getChildren and getData — skip.
                } catch (IOException parseErr) {
                    LOGGER.log(Level.WARNING,
                            "Failed to parse indexing-service instance descriptor for " + child, parseErr);
                }
            }
            // Fire both callbacks so legacy listeners (addresses only) and new
            // listeners (full descriptors) are both informed.
            notifyIndexingServicesChanged(addresses);
            notifyIndexingServiceInstancesChanged(result);
            return result;
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void publishIndexingServiceCheckpointState(IndexingServiceCheckpointState state)
            throws MetadataStorageManagerException {
        try {
            String path = indexingServicesStatePath + "/" + state.getInstanceId();
            byte[] data = state.serialize();
            try {
                ensureZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ok) {
                ensureZooKeeper().setData(path, data, -1);
            }
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public IndexingServiceCheckpointState getIndexingServiceCheckpointState(int instanceId)
            throws MetadataStorageManagerException {
        try {
            String path = indexingServicesStatePath + "/" + instanceId;
            byte[] data;
            try {
                data = ensureZooKeeper().getData(path, false, null);
            } catch (KeeperException.NoNodeException absent) {
                return null;
            }
            return IndexingServiceCheckpointState.deserialize(data);
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void watchIndexingServiceCheckpointState(
            int instanceId, Consumer<IndexingServiceCheckpointState> callback)
            throws MetadataStorageManagerException {
        indexingServiceCheckpointWatchers.put(instanceId, callback);
        installIndexingServiceStateWatch(instanceId);
    }

    private void installIndexingServiceStateWatch(int instanceId) throws MetadataStorageManagerException {
        try {
            String path = indexingServicesStatePath + "/" + instanceId;
            Watcher w = event -> {
                // One-shot watch — re-arm and re-deliver the current state on
                // NodeCreated, NodeDataChanged. On NodeDeleted we still re-arm
                // via getData(..., true, null) below which will install an exists
                // watch.
                try {
                    installIndexingServiceStateWatch(instanceId);
                    IndexingServiceCheckpointState latest = getIndexingServiceCheckpointState(instanceId);
                    if (latest != null) {
                        Consumer<IndexingServiceCheckpointState> cb =
                                indexingServiceCheckpointWatchers.get(instanceId);
                        if (cb != null) {
                            cb.accept(latest);
                        }
                    }
                } catch (MetadataStorageManagerException e) {
                    LOGGER.log(Level.WARNING,
                            "Error handling indexing-service state watch for instance " + instanceId, e);
                }
            };
            try {
                ensureZooKeeper().getData(path, w, null);
            } catch (KeeperException.NoNodeException absent) {
                // No state published yet — install an exists watch that will
                // fire when the primary first publishes.
                ensureZooKeeper().exists(path, w);
            }
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void registerFileServer(String serviceId, String address) throws MetadataStorageManagerException {
        try {
            String path = fileServersPath + "/" + serviceId;
            byte[] data = address.getBytes(StandardCharsets.UTF_8);
            try {
                ensureZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException ok) {
                ensureZooKeeper().setData(path, data, -1);
            }
            this.registeredFileServerId = serviceId;
            this.registeredFileServerAddress = address;
            LOGGER.log(Level.INFO, "Registered file server: {0} -> {1}", new Object[]{serviceId, address});
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void unregisterFileServer(String serviceId) throws MetadataStorageManagerException {
        try {
            ensureZooKeeper().delete(fileServersPath + "/" + serviceId, -1);
            this.registeredFileServerId = null;
            this.registeredFileServerAddress = null;
            LOGGER.log(Level.INFO, "Unregistered file server: {0}", serviceId);
        } catch (KeeperException.NoNodeException ok) {
            // already gone
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public List<String> listFileServers() throws MetadataStorageManagerException {
        try {
            List<String> children = ensureZooKeeper().getChildren(fileServersPath, fileServersWatcher);
            List<String> addresses = new ArrayList<>();
            for (String child : children) {
                byte[] data = ensureZooKeeper().getData(fileServersPath + "/" + child, false, null);
                addresses.add(new String(data, StandardCharsets.UTF_8));
            }
            return addresses;
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) throws MetadataStorageManagerException {
        try {
            List<String> children;
            try {
                children = ensureZooKeeper().getChildren(tableSpacesReplicasPath + "/" + tableSpaceUuid, false);
            } catch (KeeperException.NoNodeException err) {
                return Collections.emptyList();
            }
            List<TableSpaceReplicaState> result = new ArrayList<>();
            for (String child : children) {
                String path = tableSpacesReplicasPath + "/" + tableSpaceUuid + "/" + child;
                try {
                    byte[] data = ensureZooKeeper().getData(path, false, null);
                    TableSpaceReplicaState nodeMetadata = TableSpaceReplicaState.deserialize(data);
                    result.add(nodeMetadata);
                } catch (IOException deserializeError) {
                    LOGGER.log(Level.SEVERE, "error reading " + path, deserializeError);
                }
            }
            return result;
        } catch (KeeperException | InterruptedException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void updateTableSpaceReplicaState(TableSpaceReplicaState state) throws MetadataStorageManagerException {
        try {
            String tableSpacePath = tableSpacesReplicasPath + "/" + state.uuid;
            byte[] data = state.serialize();
            try {
                ensureZooKeeper().setData(tableSpacePath + "/" + state.nodeId, data, -1);
            } catch (KeeperException.NoNodeException notExists) {
                try {
                    ensureZooKeeper().create(tableSpacePath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException existsRoot) {
                }
                ensureZooKeeper().create(tableSpacePath + "/" + state.nodeId, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    // -------------------------------------------------------------------------
    // Checkpoint LSN notification for shared-storage read replicas
    // -------------------------------------------------------------------------

    private static byte[] serializeLsn(LogSequenceNumber lsn) {
        // Simple format: "ledgerId:offset" as UTF-8
        return (lsn.ledgerId + ":" + lsn.offset).getBytes(StandardCharsets.UTF_8);
    }

    private static LogSequenceNumber deserializeLsn(byte[] data) {
        if (data == null || data.length == 0) {
            return LogSequenceNumber.START_OF_TIME;
        }
        String s = new String(data, StandardCharsets.UTF_8);
        String[] parts = s.split(":");
        if (parts.length != 2) {
            return LogSequenceNumber.START_OF_TIME;
        }
        try {
            return new LogSequenceNumber(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        } catch (NumberFormatException e) {
            return LogSequenceNumber.START_OF_TIME;
        }
    }

    @Override
    public void publishCheckpointLsn(String tableSpaceUUID, LogSequenceNumber lsn) throws MetadataStorageManagerException {
        try {
            String path = checkpointsPath + "/" + tableSpaceUUID;
            byte[] data = serializeLsn(lsn);
            try {
                ensureZooKeeper().setData(path, data, -1);
            } catch (KeeperException.NoNodeException notExists) {
                try {
                    ensureZooKeeper().create(checkpointsPath, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException existsRoot) {
                    // ignore
                }
                try {
                    ensureZooKeeper().create(path, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException exists) {
                    // race: another node created it, just set
                    ensureZooKeeper().setData(path, data, -1);
                }
            }
            LOGGER.log(Level.FINE, "Published checkpoint LSN {0} for tablespace {1}",
                    new Object[]{lsn, tableSpaceUUID});
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public LogSequenceNumber getCheckpointLsn(String tableSpaceUUID) throws MetadataStorageManagerException {
        try {
            String path = checkpointsPath + "/" + tableSpaceUUID;
            Stat stat = new Stat();
            byte[] data = ensureZooKeeper().getData(path, false, stat);
            return deserializeLsn(data);
        } catch (KeeperException.NoNodeException notExists) {
            return LogSequenceNumber.START_OF_TIME;
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void watchCheckpointLsn(String tableSpaceUUID, Consumer<LogSequenceNumber> callback) throws MetadataStorageManagerException {
        checkpointWatchers.put(tableSpaceUUID, callback);
        try {
            String path = checkpointsPath + "/" + tableSpaceUUID;
            // Ensure parent and node exist
            try {
                ensureZooKeeper().create(checkpointsPath, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                ensureZooKeeper().create(path, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            // Set the watch
            installCheckpointWatch(tableSpaceUUID, path);
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void publishReplicaCheckpointLsn(String tableSpaceUUID, String nodeId, LogSequenceNumber lsn) throws MetadataStorageManagerException {
        try {
            // Ensure parent persistent znode exists (the leader's checkpoint znode)
            String parentPath = checkpointsPath + "/" + tableSpaceUUID;
            try {
                ensureZooKeeper().create(checkpointsPath, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            try {
                ensureZooKeeper().create(parentPath, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // ignore
            }
            // Now create/update the ephemeral replica node
            String replicaPath = parentPath + "/" + nodeId;
            byte[] data = serializeLsn(lsn);
            try {
                ensureZooKeeper().setData(replicaPath, data, -1);
            } catch (KeeperException.NoNodeException notExists) {
                try {
                    ensureZooKeeper().create(replicaPath, data,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (KeeperException.NodeExistsException exists) {
                    // race: session came back and reclaimed, just set
                    ensureZooKeeper().setData(replicaPath, data, -1);
                }
            }
            LOGGER.log(Level.FINE, "Published replica checkpoint LSN {0} for {1}/{2}",
                    new Object[]{lsn, tableSpaceUUID, nodeId});
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public void unregisterReplicaCheckpointLsn(String tableSpaceUUID, String nodeId) throws MetadataStorageManagerException {
        try {
            String replicaPath = checkpointsPath + "/" + tableSpaceUUID + "/" + nodeId;
            try {
                ensureZooKeeper().delete(replicaPath, -1);
            } catch (KeeperException.NoNodeException notExists) {
                // already gone
            }
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    @Override
    public LogSequenceNumber getMinReplicaCheckpointLsn(String tableSpaceUUID) throws MetadataStorageManagerException {
        try {
            String parentPath = checkpointsPath + "/" + tableSpaceUUID;
            List<String> children;
            try {
                children = ensureZooKeeper().getChildren(parentPath, false);
            } catch (KeeperException.NoNodeException notExists) {
                return null;
            }
            if (children.isEmpty()) {
                return null;
            }
            LogSequenceNumber min = null;
            for (String child : children) {
                try {
                    byte[] data = ensureZooKeeper().getData(parentPath + "/" + child, false, new Stat());
                    LogSequenceNumber lsn = deserializeLsn(data);
                    if (min == null || min.after(lsn)) {
                        min = lsn;
                    }
                } catch (KeeperException.NoNodeException disappeared) {
                    // replica disconnected mid-query, skip
                }
            }
            return min;
        } catch (InterruptedException | KeeperException | IOException err) {
            handleSessionExpiredError(err);
            throw new MetadataStorageManagerException(err);
        }
    }

    private void installCheckpointWatch(String tableSpaceUUID, String path) throws KeeperException, InterruptedException, IOException {
        Watcher watcher = (WatchedEvent event) -> {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                Consumer<LogSequenceNumber> cb = checkpointWatchers.get(tableSpaceUUID);
                if (cb != null) {
                    try {
                        Stat stat = new Stat();
                        byte[] data = ensureZooKeeper().getData(path, false, stat);
                        LogSequenceNumber lsn = deserializeLsn(data);
                        // Re-install the watch before calling the callback
                        installCheckpointWatch(tableSpaceUUID, path);
                        cb.accept(lsn);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "Error processing checkpoint watch for " + tableSpaceUUID, e);
                        // Try to re-install the watch
                        try {
                            installCheckpointWatch(tableSpaceUUID, path);
                        } catch (Exception reInstallError) {
                            LOGGER.log(Level.SEVERE, "Failed to re-install checkpoint watch for " + tableSpaceUUID, reInstallError);
                        }
                    }
                }
            }
        };
        ensureZooKeeper().getData(path, watcher, null);
    }

}
