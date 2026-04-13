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

package herddb.indexing.admin;

import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import java.util.List;

/**
 * One-shot helper that connects to ZooKeeper, reads the list of registered
 * indexing service instances, and disconnects. Used by the
 * {@code indexing-admin list-instances} CLI command.
 */
public final class ZkInstanceDiscovery {

    private static final int DEFAULT_SESSION_TIMEOUT_MS = 10_000;

    private ZkInstanceDiscovery() {
    }

    public static List<String> listInstances(String zkAddress, String basePath) throws MetadataStorageManagerException {
        return listInstances(zkAddress, basePath, DEFAULT_SESSION_TIMEOUT_MS);
    }

    public static List<String> listInstances(String zkAddress, String basePath,
                                               int sessionTimeoutMs) throws MetadataStorageManagerException {
        ZookeeperMetadataStorageManager mgr = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeoutMs, basePath);
        try {
            mgr.start();
            return mgr.listIndexingServices();
        } finally {
            try {
                mgr.close();
            } catch (MetadataStorageManagerException ignore) {
                // best effort
            }
        }
    }
}
