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

/**
 * Plug-point used by {@link LeastLoadedOwnerSelector} to read the current byte
 * footprint each indexing-service instance carries. Decoupled from ZK / the
 * segment registry so the selector can be unit-tested with a stub probe; the
 * production wiring (reading ACTIVE segments from the registry and grouping by
 * {@code ownerInstanceId}) lands in step 2.
 */
@FunctionalInterface
public interface InstanceLoadProbe {

    /**
     * @return live byte counts per instance ordinal. Array length is the
     * number of live instances; entry {@code i} is the bytes owned by ordinal
     * {@code i}. Must return at least length 1 — callers treat an empty array
     * as "no live instances" and refuse to assign.
     */
    long[] currentBytesPerInstance(String tablespaceUuid, String indexUuid);
}
