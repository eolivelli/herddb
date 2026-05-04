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
package herddb.indexing.segment;

import herddb.index.vector.NewSegmentInfo;
import herddb.index.vector.SegmentPublisher;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Adapter that wires {@link SegmentPublisher} (called by
 * {@link herddb.index.vector.PersistentVectorStore} after each successful
 * checkpoint) to a {@link SegmentRegistryClient}, registering every freshly-emitted
 * segment as ACTIVE owned by the local IS instance.
 *
 * <p>The UUID is taken from {@link NewSegmentInfo#getSegmentUuid()} — it must already
 * have been stamped by {@link herddb.index.vector.PersistentVectorStore} during Phase B
 * BEFORE IndexStatus is persisted, so the same UUID survives a restart and we cannot
 * double-register the same physical segment file (review item A2). The publisher never
 * mints UUIDs of its own.
 *
 * <p>The publisher is idempotent for the {@code SegmentAlreadyExists} case: a stale call
 * (e.g. retried after a successful but lost ZK reply) silently no-ops. Other registry
 * failures surface as a runtime exception so the caller (PersistentVectorStore) can decide
 * to swallow them — the publisher contract documents that the local checkpoint must
 * succeed regardless.
 */
public final class SegmentRegistryPublisher implements SegmentPublisher {

    private static final Logger LOGGER = Logger.getLogger(SegmentRegistryPublisher.class.getName());

    private final SegmentRegistryClient registry;
    private final String tablespaceUuid;
    private final String tableName;
    private final String indexUuid;
    private final String indexName;
    private final int instanceId;
    private final LongSupplier clock;

    public SegmentRegistryPublisher(SegmentRegistryClient registry,
                                    String tablespaceUuid, String tableName,
                                    String indexUuid, String indexName,
                                    int instanceId) {
        this(registry, tablespaceUuid, tableName, indexUuid, indexName, instanceId,
                System::currentTimeMillis);
    }

    /** Test-friendly constructor accepting an injected clock. */
    public SegmentRegistryPublisher(SegmentRegistryClient registry,
                                    String tablespaceUuid, String tableName,
                                    String indexUuid, String indexName,
                                    int instanceId, LongSupplier clock) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.indexName = Objects.requireNonNull(indexName, "indexName");
        this.instanceId = instanceId;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Stage segments as PROVISIONAL znodes (review item A1+A3 phase 1). Called
     * BEFORE IndexStatus is persisted; the next call is either
     * {@link #commitStagedSegments} or — on a crash — the next start's
     * {@link #reconcileWithIndexStatus} cleanup.
     */
    @Override
    public void stageNewSegments(List<NewSegmentInfo> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }
        long now = clock.getAsLong();
        for (NewSegmentInfo info : segments) {
            String segmentUuid = requireUuid(info);
            SegmentMetadata metadata = buildMetadata(info, segmentUuid, SegmentState.PROVISIONAL, now);
            try {
                registry.createSegment(metadata);
                LOGGER.log(Level.FINE,
                        "staged PROVISIONAL segment {0} for index {1} (segmentId={2})",
                        new Object[]{segmentUuid, indexName, info.getSegmentId()});
            } catch (SegmentRegistryException.SegmentAlreadyExists alreadyExists) {
                // Idempotent retry: the previous stage attempt left a znode behind.
                LOGGER.log(Level.INFO,
                        "segment {0} already staged — keeping existing entry",
                        new Object[]{segmentUuid});
            } catch (SegmentRegistryException e) {
                throw new RuntimeException("failed to stage segment " + segmentUuid
                        + " for index " + indexName, e);
            }
        }
    }

    /**
     * Commit previously-staged segments by transitioning PROVISIONAL → ACTIVE
     * (review item A1+A3 phase 2). Idempotent on retry; gracefully handles the
     * case where {@link #stageNewSegments} was bypassed (single-phase legacy
     * caller) by creating the ACTIVE znode in one shot.
     */
    @Override
    public void commitStagedSegments(List<NewSegmentInfo> segments) {
        if (segments == null || segments.isEmpty()) {
            return;
        }
        long now = clock.getAsLong();
        for (NewSegmentInfo info : segments) {
            String segmentUuid = requireUuid(info);
            // CAS path: read current PROVISIONAL, flip to ACTIVE.
            try {
                java.util.Optional<VersionedSegmentMetadata> current =
                        registry.getSegment(tablespaceUuid, indexUuid, segmentUuid);
                if (current.isPresent()) {
                    SegmentMetadata m = current.get().metadata();
                    if (m.getState() == SegmentState.ACTIVE) {
                        // Already committed (idempotent retry).
                        continue;
                    }
                    if (m.getState() != SegmentState.PROVISIONAL) {
                        LOGGER.log(Level.WARNING,
                                "cannot commit segment {0}: unexpected state {1} (expected PROVISIONAL)",
                                new Object[]{segmentUuid, m.getState()});
                        continue;
                    }
                    SegmentMetadata next = m.toBuilder().state(SegmentState.ACTIVE).build();
                    registry.casUpdateSegment(current.get(), next);
                    LOGGER.log(Level.FINE,
                            "committed segment {0} for index {1} (PROVISIONAL → ACTIVE)",
                            new Object[]{segmentUuid, indexName});
                    continue;
                }
                // No PROVISIONAL znode — single-phase legacy caller, or staged
                // znode was reaped by a concurrent reconcile. Create ACTIVE outright.
                SegmentMetadata metadata = buildMetadata(info, segmentUuid, SegmentState.ACTIVE, now);
                try {
                    registry.createSegment(metadata);
                } catch (SegmentRegistryException.SegmentAlreadyExists raceLost) {
                    // Concurrent stage finished between our get and our create — fine,
                    // the ACTIVE state will be reached on the next pass.
                    LOGGER.log(Level.INFO, "segment {0} concurrently staged; retrying commit",
                            new Object[]{segmentUuid});
                }
            } catch (SegmentRegistryException.VersionMismatch retry) {
                // Concurrent update on the znode (e.g. ownership transfer started). Skip;
                // the next reconcile will pick up consistent state.
                LOGGER.log(Level.INFO, "segment {0} version mismatch during commit; will reconcile",
                        new Object[]{segmentUuid});
            } catch (SegmentRegistryException e) {
                throw new RuntimeException("failed to commit segment " + segmentUuid
                        + " for index " + indexName, e);
            }
        }
    }

    /**
     * Reconcile the registry with IndexStatus at IS start (review-item A1+A3 phase 3,
     * extended for review-item R5 from the second pr-reviewer pass).
     *
     * <p>The reconcile considers every (znode-state × in-IndexStatus) combination:
     * <ul>
     *   <li>znode missing + in IndexStatus → register as ACTIVE.</li>
     *   <li>znode PROVISIONAL + in IndexStatus → promote to ACTIVE.</li>
     *   <li>znode PROVISIONAL + NOT in IndexStatus → drop the orphan (Pass 2).</li>
     *   <li>znode ACTIVE + in IndexStatus → leave alone (no-op).</li>
     *   <li>znode TRANSFERRING + in IndexStatus → leave alone; the in-flight
     *       transfer (or its abort by a future tick) will resolve the state.</li>
     *   <li>znode DEPRECATED + in IndexStatus → leave alone; the IS still references
     *       this segment but the optimizer has scheduled it for retention. The IS
     *       must continue to load the segment until it gets a transfer-away or
     *       observes the znode disappear. Logged at WARNING so operators notice
     *       the gap (review-item R5).</li>
     *   <li>znode DEPRECATED + NOT in IndexStatus → leave alone; the optimizer
     *       owns the lifecycle.</li>
     *   <li>znode DELETED + anything → leave alone; the optimizer is mid-reap.</li>
     *   <li>znode ACTIVE + NOT in IndexStatus → leave alone; this is a segment
     *       transferred TO us that we have not yet learned about — it will surface
     *       on the next checkpoint.</li>
     * </ul>
     */
    @Override
    public void reconcileWithIndexStatus(List<NewSegmentInfo> existingSegments) {
        long now = clock.getAsLong();
        java.util.Set<String> indexStatusUuids = new java.util.HashSet<>();
        if (existingSegments != null) {
            for (NewSegmentInfo info : existingSegments) {
                if (info.getSegmentUuid() != null) {
                    indexStatusUuids.add(info.getSegmentUuid());
                }
            }
        }

        // Pass 1: promote / register every IndexStatus segment.
        if (existingSegments != null) {
            for (NewSegmentInfo info : existingSegments) {
                String segmentUuid = info.getSegmentUuid();
                if (segmentUuid == null) {
                    continue;
                }
                try {
                    java.util.Optional<VersionedSegmentMetadata> current =
                            registry.getSegment(tablespaceUuid, indexUuid, segmentUuid);
                    if (!current.isPresent()) {
                        // znode missing — re-register as ACTIVE.
                        SegmentMetadata metadata = buildMetadata(info, segmentUuid, SegmentState.ACTIVE, now);
                        try {
                            registry.createSegment(metadata);
                            LOGGER.log(Level.INFO,
                                    "reconcile: re-registered ACTIVE segment {0} for index {1}",
                                    new Object[]{segmentUuid, indexName});
                        } catch (SegmentRegistryException.SegmentAlreadyExists raceLost) {
                            // benign — someone else just created it
                        }
                        continue;
                    }
                    SegmentMetadata m = current.get().metadata();
                    if (m.getState() == SegmentState.PROVISIONAL) {
                        SegmentMetadata next = m.toBuilder().state(SegmentState.ACTIVE).build();
                        try {
                            registry.casUpdateSegment(current.get(), next);
                            LOGGER.log(Level.INFO,
                                    "reconcile: promoted PROVISIONAL → ACTIVE for segment {0}",
                                    new Object[]{segmentUuid});
                        } catch (SegmentRegistryException.VersionMismatch retry) {
                            // someone won the CAS — that's fine, the znode is no longer PROVISIONAL.
                        }
                    } else if (m.getState() == SegmentState.DEPRECATED) {
                        // Review-item R5: the optimizer has already scheduled this
                        // segment for retention, but we still reference it in IndexStatus.
                        // Surface a WARNING so the operator knows there's a gap in the
                        // ownership pipeline — typically the IS-side
                        // SegmentAssignmentWatcher hasn't processed the transfer yet.
                        // We do NOT roll the segment back to ACTIVE: the optimizer's
                        // decision is authoritative, and the IS should learn about the
                        // deprecation through the watcher when it lands.
                        LOGGER.log(Level.WARNING,
                                "reconcile: segment {0} is DEPRECATED in registry but still"
                                        + " referenced by IndexStatus. The IS will keep loading"
                                        + " it until the assignment-change handler processes the"
                                        + " transfer; verify that the SegmentAssignmentWatcher"
                                        + " is wired and consuming events.",
                                new Object[]{segmentUuid});
                    }
                    // ACTIVE / TRANSFERRING / DELETED: leave alone — see Javadoc table.
                } catch (SegmentRegistryException e) {
                    LOGGER.log(Level.WARNING,
                            "reconcile failed to inspect/promote segment {0}: {1}",
                            new Object[]{segmentUuid, e.getMessage()});
                }
            }
        }

        // Pass 2: drop orphan PROVISIONAL znodes for this index.
        try {
            for (VersionedSegmentMetadata v : registry.listSegments(tablespaceUuid, indexUuid)) {
                SegmentMetadata m = v.metadata();
                if (m.getState() != SegmentState.PROVISIONAL) {
                    continue;
                }
                if (indexStatusUuids.contains(m.getSegmentUuid())) {
                    continue;
                }
                try {
                    registry.casDeleteSegment(v);
                    LOGGER.log(Level.INFO,
                            "reconcile: dropped orphan PROVISIONAL segment {0} (not in IndexStatus)",
                            new Object[]{m.getSegmentUuid()});
                } catch (SegmentRegistryException.VersionMismatch retry) {
                    // someone else just touched it; ignore.
                }
            }
        } catch (SegmentRegistryException e) {
            LOGGER.log(Level.WARNING,
                    "reconcile: failed to list segments for orphan-PROVISIONAL sweep: {0}",
                    e.getMessage());
        }
    }

    /**
     * Revalidate that every input segment is still {@code ACTIVE} in the
     * registry. Used by the IS-local compaction fallback (companion to the
     * external optimizer) to catch the race where another compactor (a
     * different IS instance, or the optimizer itself) deprecated an input
     * between candidate selection and our merge attempt.
     *
     * <p>Returns {@code true} only if every input is present AND in
     * {@code ACTIVE} state. A missing znode, a DEPRECATED state, or a registry
     * error all return {@code false}; the caller (PersistentVectorStore) treats
     * those uniformly as "abort the local merge, roll back staging".
     */
    @Override
    public boolean revalidateInputsActive(List<NewSegmentInfo> inputs) {
        if (inputs == null || inputs.isEmpty()) {
            return true;
        }
        for (NewSegmentInfo info : inputs) {
            String segmentUuid = info.getSegmentUuid();
            if (segmentUuid == null) {
                // Legacy segments (pre-segmented-v2) have no UUID and therefore
                // no registry entry; the local compactor must not include them
                // in the input set when the publisher is wired up. Treat as
                // drift to be safe.
                LOGGER.log(Level.WARNING,
                        "revalidate: input segmentId={0} for index {1} has no UUID;"
                                + " treating as drift",
                        new Object[]{info.getSegmentId(), indexName});
                return false;
            }
            try {
                java.util.Optional<VersionedSegmentMetadata> latest =
                        registry.getSegment(tablespaceUuid, indexUuid, segmentUuid);
                if (!latest.isPresent()) {
                    LOGGER.log(Level.INFO,
                            "revalidate: input {0} disappeared from registry; aborting local merge",
                            new Object[]{segmentUuid});
                    return false;
                }
                SegmentState state = latest.get().metadata().getState();
                if (state != SegmentState.ACTIVE) {
                    LOGGER.log(Level.INFO,
                            "revalidate: input {0} state={1} (expected ACTIVE);"
                                    + " aborting local merge",
                            new Object[]{segmentUuid, state});
                    return false;
                }
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING,
                        "revalidate: registry error for input {0}: {1};"
                                + " aborting local merge",
                        new Object[]{segmentUuid, e.getMessage()});
                return false;
            }
        }
        return true;
    }

    /**
     * CAS-deprecate every input segment after a successful local merge. Each
     * znode transitions from {@code ACTIVE} to {@code DEPRECATED} carrying
     * {@code replacementUuid} in {@code replacedBy} and the supplied retention
     * timestamp. Best-effort: a per-input version mismatch (rare — the
     * optimizer raced us between revalidate and deprecate on this specific
     * input) is logged and skipped; our merged output remains valid for the
     * remaining inputs, and the next optimizer tick will fold the orphan
     * ACTIVE input into a follow-up merge.
     */
    @Override
    public void deprecateInputs(List<NewSegmentInfo> inputs, String replacementUuid,
                                long retentionUntilEpochMillis) {
        if (inputs == null || inputs.isEmpty()) {
            return;
        }
        for (NewSegmentInfo info : inputs) {
            String segmentUuid = info.getSegmentUuid();
            if (segmentUuid == null) {
                continue;
            }
            try {
                java.util.Optional<VersionedSegmentMetadata> current =
                        registry.getSegment(tablespaceUuid, indexUuid, segmentUuid);
                if (!current.isPresent()) {
                    LOGGER.log(Level.INFO,
                            "deprecate: input {0} disappeared between revalidate and deprecate;"
                                    + " skipping (idempotent)",
                            new Object[]{segmentUuid});
                    continue;
                }
                SegmentMetadata m = current.get().metadata();
                if (m.getState() != SegmentState.ACTIVE) {
                    // Already deprecated by another actor — fine, idempotent.
                    LOGGER.log(Level.INFO,
                            "deprecate: input {0} already in state {1}; skipping",
                            new Object[]{segmentUuid, m.getState()});
                    continue;
                }
                SegmentMetadata next = m.toBuilder()
                        .state(SegmentState.DEPRECATED)
                        .replacedBy(java.util.Collections.singletonList(replacementUuid))
                        .retentionUntilEpochMillis(retentionUntilEpochMillis)
                        .build();
                registry.casUpdateSegment(current.get(), next);
                LOGGER.log(Level.INFO,
                        "deprecated input segment {0} (replaced by {1})",
                        new Object[]{segmentUuid, replacementUuid});
            } catch (SegmentRegistryException.VersionMismatch retry) {
                // Optimizer raced us on THIS specific input. Our merged output
                // is still valid for the others. The next optimizer tick will
                // fold the orphan ACTIVE input into a follow-up merge.
                LOGGER.log(Level.INFO,
                        "deprecate: input {0} CAS bumped (raced); leaving for next optimizer tick",
                        new Object[]{segmentUuid});
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING,
                        "deprecate: registry error for input {0}: {1}",
                        new Object[]{segmentUuid, e.getMessage()});
            }
        }
    }

    /**
     * Best-effort delete of previously-staged PROVISIONAL znodes. Called by the
     * IS-local compactor when it must abort the swap (e.g. revalidation failed).
     * We only delete the znode if it is still in {@code PROVISIONAL} state and
     * still owned by us — a CAS bump or state change means another actor has
     * taken over and we must not interfere.
     */
    @Override
    public void unstage(List<NewSegmentInfo> staged) {
        if (staged == null || staged.isEmpty()) {
            return;
        }
        for (NewSegmentInfo info : staged) {
            String segmentUuid = info.getSegmentUuid();
            if (segmentUuid == null) {
                continue;
            }
            try {
                java.util.Optional<VersionedSegmentMetadata> current =
                        registry.getSegment(tablespaceUuid, indexUuid, segmentUuid);
                if (!current.isPresent()) {
                    continue;
                }
                if (current.get().metadata().getState() != SegmentState.PROVISIONAL) {
                    LOGGER.log(Level.INFO,
                            "unstage: segment {0} is no longer PROVISIONAL (state={1});"
                                    + " leaving alone",
                            new Object[]{segmentUuid, current.get().metadata().getState()});
                    continue;
                }
                try {
                    registry.casDeleteSegment(current.get());
                    LOGGER.log(Level.INFO, "unstaged PROVISIONAL segment {0}", segmentUuid);
                } catch (SegmentRegistryException.VersionMismatch retry) {
                    // someone else just touched it
                }
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING,
                        "unstage: registry error for segment {0}: {1}",
                        new Object[]{segmentUuid, e.getMessage()});
            }
        }
    }

    /**
     * Best-effort sweep of every registry entry for {@link #indexUuid}. Used
     * by the IS engine on DROP_INDEX / TRUNCATE_TABLE: after the local store
     * is closed and before the DataStorageManager.dropIndex call wipes the
     * multipart files, we walk {@code listSegments} and CAS-delete each
     * znode regardless of state — including TRANSFERRING and DEPRECATED
     * entries. Skipping that step would leave orphan ACTIVE/TRANSFERRING
     * znodes in the registry pointing at now-deleted files, which other IS
     * instances would observe as phantom segments.
     *
     * <p>Per-znode CAS failures (a concurrent transfer or optimizer bumping
     * the version under us) are logged and skipped — the next sweep, the
     * optimizer's reaper, or a future reconcile pass will catch any
     * stragglers. The sweep does NOT touch the underlying multipart files;
     * those are the {@link herddb.storage.DataStorageManager#dropIndex}
     * caller's responsibility.
     */
    @Override
    public void dropAllSegmentsForIndex() {
        try {
            List<VersionedSegmentMetadata> all = registry.listSegments(tablespaceUuid, indexUuid);
            int deleted = 0;
            int skipped = 0;
            for (VersionedSegmentMetadata v : all) {
                try {
                    registry.casDeleteSegment(v);
                    deleted++;
                } catch (SegmentRegistryException.VersionMismatch raceLost) {
                    // Another actor (transfer recovery, optimizer reaper) bumped
                    // the znode between list and delete. Leave it; the next sweep
                    // handles it.
                    LOGGER.log(Level.INFO,
                            "dropAllSegmentsForIndex: znode {0} CAS-bumped, skipping (will be"
                                    + " reaped by next sweep or optimizer)",
                            new Object[]{v.metadata().getSegmentUuid()});
                    skipped++;
                } catch (SegmentRegistryException e) {
                    LOGGER.log(Level.WARNING,
                            "dropAllSegmentsForIndex: registry error deleting {0}: {1}",
                            new Object[]{v.metadata().getSegmentUuid(), e.getMessage()});
                    skipped++;
                }
            }
            LOGGER.log(Level.INFO,
                    "dropAllSegmentsForIndex: index {0} swept ({1} znodes deleted, {2} skipped)",
                    new Object[]{indexName, deleted, skipped});
        } catch (SegmentRegistryException e) {
            // listSegments failed — nothing more we can do here. The IS
            // engine still proceeds with the local store close + dropIndex;
            // a future reconcile (or operator) may catch the residual state.
            LOGGER.log(Level.WARNING,
                    "dropAllSegmentsForIndex: failed to list segments for index {0}: {1}",
                    new Object[]{indexName, e.getMessage()});
        }
    }

    private String requireUuid(NewSegmentInfo info) {
        String segmentUuid = info.getSegmentUuid();
        if (segmentUuid == null || segmentUuid.isEmpty()) {
            throw new IllegalStateException(
                    "NewSegmentInfo for index " + indexName + " segmentId=" + info.getSegmentId()
                            + " arrived without a stamped UUID — PersistentVectorStore must stamp"
                            + " UUIDs before invoking the publisher (review item A2)");
        }
        return segmentUuid;
    }

    private SegmentMetadata buildMetadata(NewSegmentInfo info, String segmentUuid,
                                          SegmentState state, long now) {
        return SegmentMetadata.builder()
                .segmentUuid(segmentUuid)
                .tablespaceUuid(tablespaceUuid)
                .tableName(tableName)
                .indexUuid(indexUuid)
                .indexName(indexName)
                .state(state)
                .ownerInstanceId(instanceId)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .segmentId(info.getSegmentId())
                .graphPath(info.getGraphFilePath())
                .mapPath(info.getMapFilePath())
                .baseLsn(info.getBaseLsn())
                .sizeBytes(info.getEstimatedSizeBytes())
                .vectorCount(info.getVectorCount())
                .generation(info.getGeneration())
                .createdAtEpochMillis(now)
                .build();
    }
}
