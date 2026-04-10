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

import herddb.storage.DataStorageManager;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Factory for constructing {@code herddb-remote-file-service} components
 * without a compile-time dependency on that module. Loaded reflectively
 * once at startup via
 * {@link #load()} and used through typed interfaces afterward.
 */
public interface RemoteFileServiceFactory {

    /**
     * FQN of the implementation shipped in {@code herddb-remote-file-service}.
     */
    String IMPL_CLASS_NAME = "herddb.remote.RemoteFileServiceFactoryImpl";

    /**
     * Loads the implementation reflectively from the current context class
     * loader. Throws {@link RuntimeException} with a helpful message if the
     * implementation is not on the classpath.
     */
    static RemoteFileServiceFactory load() {
        try {
            Class<?> impl = Class.forName(IMPL_CLASS_NAME);
            return (RemoteFileServiceFactory) impl.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                    "Cannot load " + IMPL_CLASS_NAME
                            + ". Ensure herddb-remote-file-service is on the classpath.", e);
        }
    }

    RemoteFileClient createClient(List<String> servers, Map<String, Object> config);

    SharedCheckpointMetadata createSharedCheckpointMetadata(RemoteFileClient client);

    /**
     * Creates a writable data storage manager backed by the remote file
     * service. The returned value is both a {@link DataStorageManager} and a
     * {@link RemoteFileStorageManager}.
     */
    DataStorageManager createDataStorageManager(
            Path dataDirectory, Path tmpDirectory, int swapThreshold, RemoteFileClient client);

    /**
     * Creates a promotable (initially read-only) data storage manager for
     * shared-storage replicas.
     */
    DataStorageManager createPromotableDataStorageManager(
            RemoteFileClient client,
            SharedCheckpointMetadata metadata,
            Path dataDirectory,
            Path tmpDirectory,
            int swapThreshold);
}
