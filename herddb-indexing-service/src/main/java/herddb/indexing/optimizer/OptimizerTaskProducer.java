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

import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Leader-only producer that enqueues merge tasks into the ZK-backed task
 * queue (step 5). Per tick:
 * <ol>
 *   <li>{@link OptimizerLeaderLock#tryAcquire()} — skip when not leader.</li>
 *   <li>{@link LeaderEpoch#bumpEpoch()} — abort tick on
 *       {@link OptimizerTaskRegistryException.VersionMismatch} (stale leader).</li>
 *   <li>{@link OptimizerOrphanScanner#scan()} — reset / poison orphaned
 *       CLAIMED tasks and GC terminal ones.</li>
 *   <li>Build the exclusion set of segment UUIDs referenced by any non-terminal
 *       task (PENDING / CLAIMED / AWAITING_ACK — POISON does NOT block).</li>
 *   <li>For each index, pick merge candidates via
 *       {@link MergePolicy#pickMergeCandidates(List, java.util.function.Predicate)},
 *       choose an owner via {@link OwnerSelector#selectOwner}, snapshot the
 *       expected-ack service-id list via
 *       {@link IndexingServiceInstanceDirectory#serviceIdsForEffectiveInstance(int)}
 *       (issue #555), and create the task znode. Each new task's inputs are
 *       added to the running exclusion set so concurrent indexes in the
 *       same tick do not pick overlapping segments (defence-in-depth —
 *       within one tick the producer is single-threaded so the second pick
 *       wouldn't see overlap anyway).</li>
 * </ol>
 *
 * <p>Step 5 lands the class without wiring it. Step 7 invokes
 * {@code produceTasks()} from the scheduler tick.
 */
public final class OptimizerTaskProducer {

    private static final Logger LOGGER = Logger.getLogger(OptimizerTaskProducer.class.getName());

    private final OptimizerTaskRegistry taskRegistry;
    private final SegmentRegistryClient segmentRegistry;
    private final String tablespaceUuid;
    private final MergePolicy mergePolicy;
    private final OwnerSelector ownerSelector;
    private final OptimizerLeaderLock leaderLock;
    private final LeaderEpoch leaderEpoch;
    private final OptimizerOrphanScanner orphanScanner;
    private final LongSupplier clock;
    /**
     * Issue #555. Used at task-creation time to snapshot the list of
     * {@code serviceId}s the consumer will wait on before committing the
     * atomic swap. Snapshotting at producer time (instead of consumer time)
     * means a shadow being scaled up during the wait does NOT extend the
     * expected-acks set indefinitely.
     */
    private final IndexingServiceInstanceDirectory instanceDirectory;

    /**
     * Directory-less constructor — package-private on purpose (issue #555):
     * a producer built without an {@link IndexingServiceInstanceDirectory}
     * emits tasks with {@code requiresAcks=false}, which lets the consumer
     * commit the atomic swap WITHOUT waiting for any IS pod to acknowledge.
     * That is acceptable for unit/integration tests in this package but
     * MUST NOT be used by production wiring — {@link IndexOptimizerMain}
     * uses the public 10-arg constructor with a live directory.
     */
    OptimizerTaskProducer(OptimizerTaskRegistry taskRegistry,
                          SegmentRegistryClient segmentRegistry,
                          String tablespaceUuid,
                          MergePolicy mergePolicy,
                          OwnerSelector ownerSelector,
                          OptimizerLeaderLock leaderLock,
                          LeaderEpoch leaderEpoch,
                          OptimizerOrphanScanner orphanScanner,
                          LongSupplier clock) {
        this(taskRegistry, segmentRegistry, tablespaceUuid, mergePolicy, ownerSelector,
                leaderLock, leaderEpoch, orphanScanner, clock,
                /* instanceDirectory */ null);
    }

    public OptimizerTaskProducer(OptimizerTaskRegistry taskRegistry,
                                 SegmentRegistryClient segmentRegistry,
                                 String tablespaceUuid,
                                 MergePolicy mergePolicy,
                                 OwnerSelector ownerSelector,
                                 OptimizerLeaderLock leaderLock,
                                 LeaderEpoch leaderEpoch,
                                 OptimizerOrphanScanner orphanScanner,
                                 LongSupplier clock,
                                 IndexingServiceInstanceDirectory instanceDirectory) {
        this.taskRegistry = Objects.requireNonNull(taskRegistry, "taskRegistry");
        this.segmentRegistry = Objects.requireNonNull(segmentRegistry, "segmentRegistry");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.mergePolicy = Objects.requireNonNull(mergePolicy, "mergePolicy");
        this.ownerSelector = Objects.requireNonNull(ownerSelector, "ownerSelector");
        this.leaderLock = leaderLock;
        this.leaderEpoch = Objects.requireNonNull(leaderEpoch, "leaderEpoch");
        this.orphanScanner = Objects.requireNonNull(orphanScanner, "orphanScanner");
        this.clock = Objects.requireNonNull(clock, "clock");
        // Null only for in-package unit/integration tests (see the
        // package-private directory-less constructor above). When null,
        // produced tasks carry requiresAcks=false and the consumer commits
        // the swap without waiting for acks. Production (IndexOptimizerMain)
        // always passes a live ZkIndexingServiceInstanceDirectory here.
        this.instanceDirectory = instanceDirectory;
    }

    public ProduceResult produceTasks()
            throws OptimizerTaskRegistryException, SegmentRegistryException {
        if (leaderLock != null && !leaderLock.tryAcquire()) {
            return new ProduceResult(false, 0, 0, null, 0, 0, 0, 0, 0);
        }
        long epoch;
        try {
            epoch = leaderEpoch.bumpEpoch();
        } catch (OptimizerTaskRegistryException.VersionMismatch staleLeader) {
            LOGGER.log(Level.WARNING,
                    "leader-epoch bump rejected — another leader is producing; aborting tick");
            return new ProduceResult(false, 0, 0, null, 0, 0, 0, 0, 0);
        }

        OptimizerOrphanScanner.ScanResult orphans = orphanScanner.scan();

        Set<String> excludedInputs = new HashSet<>();
        for (VersionedOptimizerTask vt : taskRegistry.listTasks(tablespaceUuid)) {
            if (vt.task().getState().blocksInputs()) {
                excludedInputs.addAll(vt.task().getInputSegmentUuids());
            }
        }

        ownerSelector.tickStart();
        long tasksCreated = 0;
        long tasksSkippedNoAckTargets = 0;
        long indexesScanned = 0;
        for (String indexUuid : segmentRegistry.listIndexes(tablespaceUuid)) {
            indexesScanned++;
            List<VersionedSegmentMetadata> all = segmentRegistry.listSegments(tablespaceUuid, indexUuid);
            List<VersionedSegmentMetadata> active = new ArrayList<>();
            for (VersionedSegmentMetadata v : all) {
                if (v.metadata().getState() == SegmentState.ACTIVE) {
                    active.add(v);
                }
            }
            Set<String> currentExclusion = excludedInputs;
            List<VersionedSegmentMetadata> candidates =
                    mergePolicy.pickMergeCandidates(active, currentExclusion::contains);
            if (candidates.size() < 2) {
                continue;
            }
            int owner;
            try {
                owner = ownerSelector.selectOwner(tablespaceUuid, indexUuid);
            } catch (RuntimeException noLiveInstances) {
                LOGGER.log(Level.WARNING,
                        "owner selector refused to assign for index {0}: {1}; skipping task creation",
                        new Object[]{indexUuid, noLiveInstances.getMessage()});
                continue;
            }
            OptimizerTask task = buildTask(indexUuid, candidates, owner, epoch);
            if (task == null) {
                // Issue #555: a directory-backed producer could not resolve
                // the ack-target service IDs for the chosen owner. Emitting
                // a task here would let the consumer commit the atomic swap
                // with zero acks — the exact data-loss window we are closing.
                // Skip this index; the next tick retries once the IS pods
                // are visible in ZK again.
                tasksSkippedNoAckTargets++;
                continue;
            }
            try {
                taskRegistry.createTask(task);
                tasksCreated++;
                excludedInputs.addAll(task.getInputSegmentUuids());
            } catch (OptimizerTaskRegistryException.TaskAlreadyExists collision) {
                LOGGER.log(Level.WARNING,
                        "task UUID collision (statistically improbable) — skipping: {0}",
                        task.getTaskId());
            }
        }
        return new ProduceResult(true, indexesScanned, tasksCreated, epoch,
                orphans.orphansReset, orphans.orphansPoisoned,
                orphans.orphansDeletedAfterDeprecate,
                orphans.awaitingAckAborted, orphans.terminalGcCount,
                tasksSkippedNoAckTargets);
    }

    /**
     * Builds a merge task for the chosen candidates / owner, or returns
     * {@code null} when a directory-backed producer cannot resolve the
     * ack-target service IDs for {@code owner} (issue #555 fail-closed:
     * the caller skips this index instead of emitting a task whose atomic
     * swap would commit without waiting for any IS pod to acknowledge).
     */
    private OptimizerTask buildTask(String indexUuid,
                                    List<VersionedSegmentMetadata> candidates,
                                    int owner, long epoch) {
        Map<String, Integer> versions = new LinkedHashMap<>();
        List<String> uuids = new ArrayList<>(candidates.size());
        for (VersionedSegmentMetadata v : candidates) {
            uuids.add(v.metadata().getSegmentUuid());
            versions.put(v.metadata().getSegmentUuid(), v.zkVersion());
        }
        // Issue #555: snapshot every IS pod (primary + shadows) whose
        // effectiveInstanceId == owner. The consumer waits for every one of
        // these to acknowledge the staged output before firing the atomic
        // swap. Snapshot at producer time so a shadow scaled up DURING the
        // wait does not extend the expected-acks set forever; a shadow
        // scaled DOWN during the wait is handled by the swap-ack-timeout
        // abort path.
        //
        // requiresAcks is true iff a live IndexingServiceInstanceDirectory
        // is wired (production). When the directory cannot produce any
        // ack target (ZK blip, no IS pod registered for `owner`) we MUST
        // NOT emit the task — returning null makes the caller skip the
        // index. A directory-less producer (test / legacy) sets
        // requiresAcks=false and the consumer commits without acks.
        boolean requiresAcks = instanceDirectory != null;
        List<String> expectedAcks = requiresAcks
                ? instanceDirectory.serviceIdsForEffectiveInstance(owner)
                : java.util.Collections.<String>emptyList();
        if (requiresAcks && expectedAcks.isEmpty()) {
            LOGGER.log(Level.WARNING,
                    "issue #555: no IS pod registered for effectiveInstanceId={0} at task-creation"
                            + " time (ZK blip or scale-down race); skipping task creation for"
                            + " index {1} — the next tick retries when the IS pods are visible.",
                    new Object[]{owner, indexUuid});
            return null;
        }
        return OptimizerTask.builder()
                .taskId(UUID.randomUUID().toString())
                .tablespaceUuid(tablespaceUuid)
                .indexUuid(indexUuid)
                .inputSegmentUuids(uuids)
                .inputSegmentExpectedVersions(versions)
                .targetOwnerInstanceId(owner)
                .state(OptimizerTaskState.PENDING)
                .createdAtEpochMillis(clock.getAsLong())
                .leaderEpoch(epoch)
                .expectedAckServiceIds(expectedAcks)
                .requiresAcks(requiresAcks)
                .build();
    }

    /** Outcome of a single produce tick, exposed for observability + tests. */
    public static final class ProduceResult {
        public final boolean ranAsLeader;
        public final long indexesScanned;
        public final long tasksCreated;
        public final Long leaderEpoch;
        public final long orphansReset;
        public final long orphansPoisoned;
        public final long orphansDeletedAfterDeprecate;
        /** Issue #555: AWAITING_ACK tasks aborted by the orphan scanner. */
        public final long awaitingAckAborted;
        public final long terminalGcCount;
        /**
         * Issue #555: indexes for which a task was NOT created because the
         * directory could not resolve the ack-target service IDs (fail-closed).
         */
        public final long tasksSkippedNoAckTargets;

        ProduceResult(boolean ranAsLeader, long indexesScanned, long tasksCreated,
                      Long leaderEpoch, long orphansReset, long orphansPoisoned,
                      long orphansDeletedAfterDeprecate, long awaitingAckAborted,
                      long terminalGcCount) {
            this(ranAsLeader, indexesScanned, tasksCreated, leaderEpoch, orphansReset,
                    orphansPoisoned, orphansDeletedAfterDeprecate, awaitingAckAborted,
                    terminalGcCount, /* tasksSkippedNoAckTargets */ 0L);
        }

        ProduceResult(boolean ranAsLeader, long indexesScanned, long tasksCreated,
                      Long leaderEpoch, long orphansReset, long orphansPoisoned,
                      long orphansDeletedAfterDeprecate, long awaitingAckAborted,
                      long terminalGcCount, long tasksSkippedNoAckTargets) {
            this.ranAsLeader = ranAsLeader;
            this.indexesScanned = indexesScanned;
            this.tasksCreated = tasksCreated;
            this.leaderEpoch = leaderEpoch;
            this.orphansReset = orphansReset;
            this.orphansPoisoned = orphansPoisoned;
            this.orphansDeletedAfterDeprecate = orphansDeletedAfterDeprecate;
            this.awaitingAckAborted = awaitingAckAborted;
            this.terminalGcCount = terminalGcCount;
            this.tasksSkippedNoAckTargets = tasksSkippedNoAckTargets;
        }
    }
}
