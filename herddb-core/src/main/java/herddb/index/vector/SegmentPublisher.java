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
     * Revalidate that every supplied input segment is still in {@code ACTIVE}
     * state in the registry. Used by the IS-local compaction fallback
     * (companion to the external index-optimizer): after staging a merged
     * output, we must verify no concurrent optimizer has already deprecated
     * any of the inputs. If revalidation fails the local compactor aborts
     * the swap, deletes the staged PROVISIONAL znode via {@link #unstage},
     * and queues the merged output's multipart files for cleanup.
     *
     * <p>The default {@code SegmentRegistryPublisher} implementation only
     * checks {@code state == ACTIVE} (not znode version) — segment UUIDs are
     * unique per physical segment so a state-only check is sufficient to
     * detect the only race that matters: another actor moved the segment out
     * of ACTIVE between candidate selection and our merge attempt.
     *
     * <p>Default returns {@code true} (no-op): publishers that don't track
     * input lifecycle (legacy single-phase) treat every input as still ACTIVE.
     *
     * @return {@code true} iff every input is still in {@code ACTIVE} state in
     *         the registry
     */
    default boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
        return true;
    }

    /**
     * CAS-deprecate the given input segments after a successful local merge.
     * For each input, transition its registry znode from {@code ACTIVE} to
     * {@code DEPRECATED}, recording {@code replacementUuid} in {@code replacedBy}
     * and {@code retentionUntilEpochMillis} in the metadata. Best-effort:
     * implementations log and continue on per-input version-mismatch (the
     * optimizer may have raced us on a single input; the merged output remains
     * valid for the others).
     *
     * <p>Default no-op: legacy publishers don't drive the registry.
     */
    default void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                 long retentionUntilEpochMillis) {
    }

    /**
     * Best-effort cleanup of previously-staged PROVISIONAL znodes. Called by
     * the IS-local compactor when revalidation fails (a concurrent optimizer
     * raced us between stage and revalidate) and we must abort the swap.
     * Failures are logged; a leaked PROVISIONAL znode is reaped by the next
     * {@link #reconcileWithIndexStatus} pass on restart, or by an out-of-band
     * cleanup tick.
     *
     * <p>Default no-op: legacy publishers don't track PROVISIONAL state.
     */
    default void unstage(List<NewSegmentInfo> staged) {
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

    /**
     * Drop every registry entry for the index this publisher is bound to.
     * Invoked when the IS receives a DROP_INDEX or TRUNCATE_TABLE log entry,
     * AFTER the local store has been closed and BEFORE
     * {@code DataStorageManager.dropIndex} wipes the on-storage data: without
     * this hook the segmented-v2 registry would be left with orphan ACTIVE
     * znodes pointing at now-deleted multipart files. Other IS instances
     * watching the registry would observe phantom segments and (in the worst
     * case) attempt ownership-transfers for files that no longer exist.
     *
     * <p>Best-effort: implementations log per-segment failures and continue.
     * A znode that cannot be deleted (concurrent CAS bump from a transfer
     * recovery, or transient ZK error) is left for the next reconcile pass on
     * restart, OR for the optimizer's reaper if it ever picks up an
     * abandoned-index sweep.
     *
     * <p>Default no-op: legacy publishers that don't track a registry have
     * nothing to drop.
     */
    default void dropAllSegmentsForIndex() {
    }
}
