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

import herddb.server.RemoteFileClient;
import herddb.server.RemoteFileServiceFactory;
import herddb.server.SharedCheckpointMetadata;
import herddb.storage.DataStorageManager;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Default {@link RemoteFileServiceFactory} implementation. Lives in
 * {@code herddb-remote-file-service} so the core modules never take a
 * compile-time dependency on this artifact; it is loaded reflectively via
 * {@link RemoteFileServiceFactory#load()}.
 */
public class RemoteFileServiceFactoryImpl implements RemoteFileServiceFactory {

    public RemoteFileServiceFactoryImpl() {
    }

    @Override
    public RemoteFileClient createClient(List<String> servers, Map<String, Object> config) {
        return new RemoteFileServiceClient(servers, config);
    }

    @Override
    public SharedCheckpointMetadata createSharedCheckpointMetadata(RemoteFileClient client) {
        return new SharedCheckpointMetadataManager((RemoteFileServiceClient) client);
    }

    @Override
    public DataStorageManager createDataStorageManager(
            Path dataDirectory, Path tmpDirectory, int swapThreshold, RemoteFileClient client) {
        return new RemoteFileDataStorageManager(
                dataDirectory, tmpDirectory, swapThreshold, (RemoteFileServiceClient) client);
    }

    @Override
    public DataStorageManager createPromotableDataStorageManager(
            RemoteFileClient client,
            SharedCheckpointMetadata metadata,
            Path dataDirectory,
            Path tmpDirectory,
            int swapThreshold) {
        RemoteFileServiceClient concreteClient = (RemoteFileServiceClient) client;
        SharedCheckpointMetadataManager concreteMetadata = (SharedCheckpointMetadataManager) metadata;
        ReadReplicaDataStorageManager readReplica = new ReadReplicaDataStorageManager(
                concreteClient, concreteMetadata, tmpDirectory, swapThreshold);
        return new PromotableRemoteFileDataStorageManager(
                readReplica, concreteClient, concreteMetadata,
                dataDirectory, tmpDirectory, swapThreshold);
    }
}
