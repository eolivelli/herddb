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

import herddb.log.LogSequenceNumber;
import java.util.function.Function;

/**
 * Subset of {@code RemoteFileDataStorageManager}'s configuration API exposed
 * to modules that cannot take a compile-time dependency on
 * {@code herddb-remote-file-service}. Implementations also extend
 * {@code DataStorageManager}.
 */
public interface RemoteFileStorageManager {

    /**
     * Enables publication of checkpoint metadata to remote storage for
     * shared-storage read replicas.
     */
    void setSharedCheckpointMetadataManager(SharedCheckpointMetadata manager);

    /**
     * Enables deferred page deletion so read replicas can safely consume
     * pages from old checkpoints.
     */
    void setRetentionPolicy(
            Function<String, LogSequenceNumber> minReplicaLsnSupplier,
            long minRetentionMillis,
            long maxRetentionMillis);
}
