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
package herddb.indexing.optimizer;

import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.metadata.IndexingServiceInstanceDescriptor;
import herddb.metadata.MetadataStorageManagerException;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.IntSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * Production {@link IndexingServiceInstanceDirectory} backed by the optimizer's
 * long-lived {@link ZookeeperMetadataStorageManager}. Reads the
 * {@code /<basePath>/indexingServices/instances} ephemeral registrations and
 * filters to primary instances (shadows do not own segments). When ZK reports
 * no live instances, falls back to {@code [0..fallbackNumInstances)} so
 * single-IS deployments with cold-boot ordering still work. When both sources
 * are empty, the directory returns an empty array and the
 * {@link LeastLoadedOwnerSelector} will throw {@code IllegalStateException},
 * propagating up to {@link IndexOptimizerEngine}'s tick error handler so the
 * next tick retries instead of silently writing {@code ownerInstanceId=0}.
 */
public final class ZkIndexingServiceInstanceDirectory implements IndexingServiceInstanceDirectory {

    private static final Logger LOGGER =
            Logger.getLogger(ZkIndexingServiceInstanceDirectory.class.getName());

    private final ZookeeperMetadataStorageManager zkMeta;
    private final IntSupplier fallbackNumInstances;

    public ZkIndexingServiceInstanceDirectory(ZookeeperMetadataStorageManager zkMeta,
                                              IntSupplier fallbackNumInstances) {
        this.zkMeta = Objects.requireNonNull(zkMeta, "zkMeta");
        this.fallbackNumInstances = Objects.requireNonNull(fallbackNumInstances, "fallbackNumInstances");
    }

    @Override
    public int[] liveInstanceOrdinals() {
        List<IndexingServiceInstanceDescriptor> descriptors;
        try {
            descriptors = zkMeta.listIndexingServiceInstances();
        } catch (MetadataStorageManagerException ze) {
            LOGGER.log(Level.WARNING,
                    "could not list indexing-service instances; falling back to configured count: {0}",
                    ze.getMessage());
            descriptors = List.of();
        }
        TreeSet<Integer> ordinals = new TreeSet<>();
        for (IndexingServiceInstanceDescriptor d : descriptors) {
            if (IndexingServiceInstanceDescriptor.ROLE_PRIMARY.equals(d.getRole())) {
                ordinals.add(d.getInstanceId());
            }
        }
        if (ordinals.isEmpty()) {
            int n = fallbackNumInstances.getAsInt();
            if (n > 0) {
                LOGGER.log(Level.FINE,
                        "no live IS primaries discoverable from ZK; using configured fallback {0}",
                        n);
                IntStream.range(0, n).forEach(ordinals::add);
            }
        }
        if (ordinals.isEmpty()) {
            return new int[0];
        }
        return ordinals.stream().mapToInt(Integer::intValue).toArray();
    }
}
