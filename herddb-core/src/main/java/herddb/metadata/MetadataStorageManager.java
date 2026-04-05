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

package herddb.metadata;

import herddb.log.LogSequenceNumber;
import herddb.model.DDLException;
import herddb.model.InvalidTableException;
import herddb.model.NodeMetadata;
import herddb.model.TableSpace;
import herddb.model.TableSpaceReplicaState;
import herddb.server.ServerConfiguration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Store of all metadata of the system: definition of tables, tablesets, available nodes
 *
 * @author enrico.olivelli
 */
public abstract class MetadataStorageManager implements AutoCloseable {

    private MetadataChangeListener listener;
    private final CopyOnWriteArrayList<ServiceDiscoveryListener> serviceDiscoveryListeners = new CopyOnWriteArrayList<>();

    public abstract void start() throws MetadataStorageManagerException;

    public abstract boolean ensureDefaultTableSpace(String localNodeId, String initialReplicaList, long maxLeaderInactivityTime, int replicationFactor) throws MetadataStorageManagerException;

    public abstract void close() throws MetadataStorageManagerException;

    /**
     * Enumerates all the available TableSpaces in the system
     *
     * @return
     */
    public abstract Collection<String> listTableSpaces() throws MetadataStorageManagerException;

    /**
     * Describe a single TableSpace
     *
     * @param name
     * @return
     */
    public abstract TableSpace describeTableSpace(String name) throws MetadataStorageManagerException;

    /**
     * Registers a new table space on the metadata storage
     *
     * @param tableSpace
     */
    public abstract void registerTableSpace(TableSpace tableSpace) throws DDLException, MetadataStorageManagerException;

    /**
     * Drop a tablespace
     *
     * @param name
     * @param previous
     * @throws DDLException
     * @throws MetadataStorageManagerException
     */
    public abstract void dropTableSpace(String name, TableSpace previous) throws DDLException, MetadataStorageManagerException;

    /**
     * Updates table space metadata on the metadata storage
     *
     * @param tableSpace
     */
    public abstract boolean updateTableSpace(TableSpace tableSpace, TableSpace previous) throws DDLException, MetadataStorageManagerException;

    protected void validateTableSpace(TableSpace tableSpace) throws DDLException {
        // TODO: implement sensible validations
        if (tableSpace.name == null || tableSpace.name.trim().isEmpty()) {
            throw new InvalidTableException("null tablespace name");
        }
    }

    /**
     * Registers a node in the system
     *
     * @param nodeMetadata
     * @throws MetadataStorageManagerException
     */
    public void registerNode(NodeMetadata nodeMetadata) throws MetadataStorageManagerException {
    }

    /**
     * Notifies on metadata storage manage the state of the node against a given tablespace
     *
     * @param state
     * @throws MetadataStorageManagerException
     */
    public abstract void updateTableSpaceReplicaState(TableSpaceReplicaState state) throws MetadataStorageManagerException;

    public abstract List<TableSpaceReplicaState> getTableSpaceReplicaState(String tableSpaceUuid) throws MetadataStorageManagerException;

    /**
     * Enumerates all known nodes
     *
     * @return
     * @throws herddb.metadata.MetadataStorageManagerException
     */
    public List<NodeMetadata> listNodes() throws MetadataStorageManagerException {
        return Collections.emptyList();
    }

    public final void setMetadataChangeListener(MetadataChangeListener listener) {
        this.listener = listener;
    }

    public final MetadataChangeListener getListener() {
        return listener;
    }


    protected final void notifyMetadataChanged(String description) {
        if (listener != null) {
            listener.metadataChanged(description);
        }
    }

    public void registerIndexingService(String serviceId, String address) throws MetadataStorageManagerException {
    }

    public void unregisterIndexingService(String serviceId) throws MetadataStorageManagerException {
    }

    public List<String> listIndexingServices() throws MetadataStorageManagerException {
        return Collections.emptyList();
    }

    public void registerFileServer(String serviceId, String address) throws MetadataStorageManagerException {
    }

    public void unregisterFileServer(String serviceId) throws MetadataStorageManagerException {
    }

    public List<String> listFileServers() throws MetadataStorageManagerException {
        return Collections.emptyList();
    }

    public final void addServiceDiscoveryListener(ServiceDiscoveryListener listener) {
        serviceDiscoveryListeners.add(listener);
    }

    public final void removeServiceDiscoveryListener(ServiceDiscoveryListener listener) {
        serviceDiscoveryListeners.remove(listener);
    }

    protected final void notifyIndexingServicesChanged(List<String> addresses) {
        for (ServiceDiscoveryListener l : serviceDiscoveryListeners) {
            try {
                l.onIndexingServicesChanged(addresses);
            } catch (Exception e) {
                // log but don't propagate
            }
        }
    }

    protected final void notifyFileServersChanged(List<String> addresses) {
        for (ServiceDiscoveryListener l : serviceDiscoveryListeners) {
            try {
                l.onFileServersChanged(addresses);
            } catch (Exception e) {
                // log but don't propagate
            }
        }
    }

    public void clear() throws MetadataStorageManagerException {

    }

    // -------------------------------------------------------------------------
    // Checkpoint LSN notification for shared-storage read replicas
    // -------------------------------------------------------------------------

    /**
     * Publishes the latest checkpoint LSN for a tablespace so that read replicas
     * can be notified of new checkpoints. Default implementation is a no-op.
     */
    public void publishCheckpointLsn(String tableSpaceUUID, LogSequenceNumber lsn) throws MetadataStorageManagerException {
        // no-op for non-ZK implementations
    }

    /**
     * Reads the latest published checkpoint LSN for a tablespace.
     *
     * @return the latest LSN, or {@link LogSequenceNumber#START_OF_TIME} if none published
     */
    public LogSequenceNumber getCheckpointLsn(String tableSpaceUUID) throws MetadataStorageManagerException {
        return LogSequenceNumber.START_OF_TIME;
    }

    /**
     * Sets a watch on checkpoint LSN changes for the given tablespace.
     * The callback is invoked each time the leader publishes a new checkpoint LSN.
     * The watch is persistent and re-registers automatically.
     */
    public void watchCheckpointLsn(String tableSpaceUUID, Consumer<LogSequenceNumber> callback) throws MetadataStorageManagerException {
        // no-op for non-ZK implementations
    }

    /**
     * A replica publishes its current applied checkpoint LSN. The leader reads this to
     * determine when it is safe to delete stale pages from remote storage.
     * <p>
     * Implementations should use an ephemeral mechanism (e.g., ephemeral znode in ZK) so
     * that the entry is automatically removed when the replica disconnects/crashes.
     */
    public void publishReplicaCheckpointLsn(String tableSpaceUUID, String nodeId, LogSequenceNumber lsn) throws MetadataStorageManagerException {
        // no-op for non-ZK implementations
    }

    /**
     * Removes the replica's published checkpoint LSN (called on clean shutdown).
     */
    public void unregisterReplicaCheckpointLsn(String tableSpaceUUID, String nodeId) throws MetadataStorageManagerException {
        // no-op for non-ZK implementations
    }

    /**
     * Returns the minimum checkpoint LSN across all currently-registered replicas for
     * the given tablespace. Returns {@code null} if no replicas are registered.
     * <p>
     * The leader uses this to decide whether it is safe to delete stale remote pages.
     */
    public LogSequenceNumber getMinReplicaCheckpointLsn(String tableSpaceUUID) throws MetadataStorageManagerException {
        return null;
    }

    public String generateNewNodeId(ServerConfiguration config) throws MetadataStorageManagerException {
        List<NodeMetadata> actualNodes = listNodes();
        NodeIdGenerator generator = new NodeIdGenerator();
        for (int i = 0; i < 10000; i++) {
            String _nodeId = generator.nextId();
            if (!actualNodes.stream().filter(node -> _nodeId.equals(node.nodeId)).findFirst().isPresent()) {
                return _nodeId;
            }
        }
        throw new MetadataStorageManagerException("cannot find a new node id");
    }

}
