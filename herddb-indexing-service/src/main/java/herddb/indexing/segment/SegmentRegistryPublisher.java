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
     * Reconcile the registry with IndexStatus at IS start (review item A1+A3 phase 3):
     * <ul>
     *   <li>For each existing segment in IndexStatus: if its znode is missing, register
     *       as ACTIVE; if PROVISIONAL, promote to ACTIVE.</li>
     *   <li>Walk PROVISIONAL znodes registered for this index whose UUID is NOT in
     *       IndexStatus — they are orphans from a prior crash between stage and
     *       IndexStatus persist. Drop them.</li>
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
                    }
                    // ACTIVE / TRANSFERRING / DEPRECATED: leave alone.
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
