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

import java.util.List;

/**
 * Listener for dynamic service discovery changes.
 */
public interface ServiceDiscoveryListener {
    /**
     * Legacy address-only callback. Kept for backwards compatibility; new
     * listeners should override
     * {@link #onIndexingServiceInstancesChanged(List)} instead, which also
     * conveys role/instanceId/shadowOf metadata needed for shadow-aware
     * query routing. Default implementation is a no-op so that listeners can
     * implement only the richer callback.
     */
    default void onIndexingServicesChanged(List<String> currentAddresses) {
    }

    /**
     * Full indexing-service discovery callback. Fires whenever the set of
     * registered indexing-service instances (primaries + shadows) changes.
     */
    default void onIndexingServiceInstancesChanged(List<IndexingServiceInstanceDescriptor> currentInstances) {
    }

    void onFileServersChanged(List<String> currentAddresses);
}
