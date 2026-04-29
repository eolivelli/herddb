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
package herddb.index.vector;

import java.util.List;

/**
 * Pluggable hook invoked by {@link PersistentVectorStore} immediately after a
 * successful checkpoint. Used to publish freshly-emitted segments to an
 * external registry (currently the segmented-v2 ZooKeeper segment registry
 * exercised by the index-optimizer service).
 *
 * <p>Default behaviour is no-op (publisher unset); legacy indexes do not use it.
 */
@FunctionalInterface
public interface SegmentPublisher {

    /**
     * Publish a batch of newly-sealed segments. Called once per successful checkpoint
     * after the IndexStatus has been durably persisted, with the multipart files
     * already uploaded to remote storage.
     *
     * <p>The publisher must be best-effort: a failure here MUST NOT corrupt the local
     * checkpoint state. Implementations are expected to log and continue (the segment
     * remains visible locally; on the next checkpoint cycle a retry can re-register
     * any missing entries). For the same reason, this hook does not throw checked
     * exceptions; runtime exceptions are caught and logged by the caller.
     *
     * @param segments freshly-emitted segments. Empty list means a checkpoint cycle
     *                 where Phase B produced no new segments (all live shards were
     *                 too small or empty); the publisher may use this signal as a
     *                 keep-alive.
     */
    void publishNewSegments(List<NewSegmentInfo> segments);
}
