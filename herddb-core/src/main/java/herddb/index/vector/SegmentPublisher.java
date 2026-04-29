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
 * Pluggable hook invoked by {@link PersistentVectorStore} during checkpoint to
 * register freshly-emitted segments in an external registry (currently the
 * segmented-v2 ZooKeeper segment registry exercised by the index-optimizer
 * service).
 *
 * <p>Default behaviour is no-op (publisher unset); legacy indexes do not use it.
 *
 * <p><b>Two-phase publish (review item A1+A3):</b> {@link #stageNewSegments} is
 * called BEFORE IndexStatus is persisted (state PROVISIONAL); on success
 * {@link #commitStagedSegments} is called AFTER IndexStatus is durably
 * persisted (PROVISIONAL → ACTIVE). On failure the local checkpoint still
 * succeeds — orphan PROVISIONAL znodes are cleaned up by
 * {@link #reconcileWithIndexStatus} on the next start, and missing-but-known
 * IndexStatus segments are re-registered.
 */
public interface SegmentPublisher {

    /**
     * Stage a batch of about-to-be-emitted segments. Invoked AFTER multipart files
     * are uploaded but BEFORE IndexStatus is durably written. Implementations should
     * register a PROVISIONAL znode per segment.
     *
     * <p>Failures MUST be reported as a {@code RuntimeException} so the caller can
     * decide whether to abort the checkpoint or fall back to legacy behaviour. The
     * default implementation falls back to {@link #publishNewSegments} for
     * backward-compat with the old single-phase contract.
     */
    default void stageNewSegments(List<NewSegmentInfo> segments) {
        // single-phase fallback: do nothing here, let publishNewSegments do all the work
    }

    /**
     * Commit previously-staged segments. Called once per successful checkpoint after
     * IndexStatus is durable. Implementations transition each PROVISIONAL znode to
     * ACTIVE. Idempotent on retry. Best-effort: failures are caught and logged by
     * the caller; reconciliation on next start mops up.
     */
    default void commitStagedSegments(List<NewSegmentInfo> segments) {
        publishNewSegments(segments);
    }

    /**
     * Legacy single-phase entry point. Default for implementations that pre-date the
     * staged contract; equivalent to commit-after-IndexStatus with no PROVISIONAL
     * step. New implementations should override the staged methods instead.
     *
     * @deprecated prefer {@link #stageNewSegments} + {@link #commitStagedSegments}
     */
    @Deprecated
    default void publishNewSegments(List<NewSegmentInfo> segments) {
    }

    /**
     * Reconcile the registry with IndexStatus on startup (review item A1+A3):
     * <ul>
     *   <li>Promote any PROVISIONAL znode whose UUID matches a segment in IndexStatus
     *       (a previous checkpoint crashed between stage and commit).</li>
     *   <li>Drop PROVISIONAL znodes whose UUID is NOT in IndexStatus (a previous
     *       checkpoint crashed between stage and IndexStatus persist; orphan files in
     *       remote storage are tracked by separate retention machinery).</li>
     *   <li>Register-as-ACTIVE any IndexStatus segment whose znode is missing entirely
     *       (a previous publisher invocation failed transiently).</li>
     * </ul>
     *
     * <p>Default implementation no-ops for backward compat with publishers that
     * don't need reconciliation.
     */
    default void reconcileWithIndexStatus(List<NewSegmentInfo> existingSegments) {
    }
}
