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

import java.util.Objects;

/**
 * Picks the live instance ordinal with the lowest current byte footprint,
 * breaking ties in favour of the lowest ordinal. The byte snapshot is sourced
 * from an {@link InstanceLoadProbe} so the selector can be unit-tested with a
 * stub (step 1) and later wired to the real registry-based probe (step 2).
 *
 * <p>Step 1 introduces the selector but does not make it the production
 * default — that flip happens in step 2 together with the discovery wiring.
 */
public final class LeastLoadedOwnerSelector implements OwnerSelector {

    private final InstanceLoadProbe probe;

    public LeastLoadedOwnerSelector(InstanceLoadProbe probe) {
        this.probe = Objects.requireNonNull(probe, "probe");
    }

    @Override
    public int selectOwner(String tablespaceUuid, String indexUuid) {
        long[] bytes = probe.currentBytesPerInstance(tablespaceUuid, indexUuid);
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException(
                    "InstanceLoadProbe returned no live instances for tablespace="
                            + tablespaceUuid + " index=" + indexUuid);
        }
        int best = 0;
        long bestBytes = bytes[0];
        for (int i = 1; i < bytes.length; i++) {
            if (bytes[i] < bestBytes) {
                bestBytes = bytes[i];
                best = i;
            }
        }
        return best;
    }
}
