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
 * Decides which indexing-service instance becomes the owner of a newly-merged
 * segment. Implementations range from a hard-coded "always 0" (back-compat) to
 * load-aware policies that read the segment registry every tick.
 *
 * <p>The selector is consulted by the optimizer at task-creation time (once
 * the task-queue model is wired in step 7) so the target owner is recorded in
 * the task znode and survives crash/restart. The result is used by
 * {@link SegmentMerger#merge(java.util.List, int)} when stamping the output
 * segment.
 */
public interface OwnerSelector {

    /**
     * Picks the owner instance ordinal for the next merged segment.
     *
     * @param tablespaceUuid the tablespace UUID
     * @param indexUuid the index UUID
     * @return owner instance ordinal in {@code [0, numInstances)}
     */
    int selectOwner(String tablespaceUuid, String indexUuid);

    /**
     * Hook invoked once at the start of every leader tick before any
     * {@link #selectOwner} call. Implementations that cache per-tick state
     * (e.g. a per-instance byte snapshot) refresh it here. Default no-op.
     */
    default void tickStart() {
    }
}
