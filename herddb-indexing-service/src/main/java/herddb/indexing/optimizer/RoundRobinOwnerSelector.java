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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

/**
 * Cycles through live instance ordinals in order. Cursor is in-memory only —
 * step 7 may persist it under ZK so leader handovers don't reset to 0, but for
 * step 1 it lives within the JVM. Good for deterministic tests and for
 * deployments where strict bytes-fairness is not required.
 */
public final class RoundRobinOwnerSelector implements OwnerSelector {

    private final IntSupplier numInstancesSupplier;
    private final AtomicInteger cursor = new AtomicInteger();

    public RoundRobinOwnerSelector(IntSupplier numInstancesSupplier) {
        this.numInstancesSupplier = Objects.requireNonNull(numInstancesSupplier, "numInstancesSupplier");
    }

    @Override
    public int selectOwner(String tablespaceUuid, String indexUuid) {
        int n = Math.max(1, numInstancesSupplier.getAsInt());
        return Math.floorMod(cursor.getAndIncrement(), n);
    }
}
