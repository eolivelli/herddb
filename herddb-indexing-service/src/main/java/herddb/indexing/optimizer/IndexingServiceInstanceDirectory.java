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

import java.util.Collections;
import java.util.List;

/**
 * Reports the live indexing-service primary instance ordinals the optimizer
 * may target when assigning ownership of merged segments. Implementations
 * source the list from ZK ephemerals (production:
 * {@link ZkIndexingServiceInstanceDirectory}) or static config / test stubs.
 *
 * <p>Returned array from {@link #liveInstanceOrdinals()} is sorted ascending,
 * contains distinct primary ordinals only (shadows are excluded — a shadow
 * does not own segments), and may be empty when no live instances are
 * discoverable.
 *
 * <p>Issue #555 adds {@link #serviceIdsForEffectiveInstance(int)} so the
 * optimizer can enumerate every pod (primary + shadow replicas) whose
 * {@code effectiveInstanceId} matches the new owner of a staged merge
 * output. Those pods are the parties the consumer waits on before committing
 * the atomic swap.
 */
public interface IndexingServiceInstanceDirectory {

    int[] liveInstanceOrdinals();

    /**
     * Returns the sorted list of {@code serviceId}s of every IS pod whose
     * {@code effectiveInstanceId} equals {@code effectiveInstanceId}.
     * Includes the primary {@code serviceId} (if registered) plus every
     * shadow replica of that primary. Returns an empty list when none are
     * discoverable (cold start, network blip, scale-down race).
     *
     * <p>Default implementation: empty list. Implementations that participate
     * in the issue #555 ack protocol must override this method; legacy
     * test stubs that don't can rely on the default so their compile units
     * stay green.
     */
    default List<String> serviceIdsForEffectiveInstance(int effectiveInstanceId) {
        return Collections.emptyList();
    }
}
