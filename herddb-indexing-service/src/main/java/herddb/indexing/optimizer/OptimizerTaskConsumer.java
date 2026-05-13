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

import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker-side consumer that claims and executes one merge task per call.
 * Every optimizer pod runs the consumer (the leader also drains tasks in
 * addition to producing). Step 6 lands the class without wiring it; step 7
 * invokes {@code consumeOneTask()} from the scheduler tick.
 *
 * <p>Single-task flow:
 * <ol>
 *   <li>Pick the oldest PENDING task from the registry.</li>
 *   <li>Create the EPHEMERAL lease FIRST. {@code NodeExists} ⇒ another worker is
 *       mid-claim; skip.</li>
 *   <li>CAS the task state PENDING → CLAIMED. {@code VersionMismatch} ⇒ another
 *       worker beat us to the CAS; delete our lease and skip.</li>
 *   <li>Re-read every input segment at its expected znode version. If any
 *       drifted (deprecated by an earlier merge, ownership transferred, znode
 *       gone), CAS the task to DONE with {@code outputSegmentUuid=null} and a
 *       "drifted" {@code lastError} — the work was already (or is being) done
 *       elsewhere; do not duplicate it.</li>
 *   <li>Call {@link SegmentMerger#merge}. {@code null} return ⇒ merger declined
 *       this batch (e.g. too few live entries); CAS the task to DONE with no
 *       output. An exception ⇒ CAS the task to FAILED (or POISON when
 *       attempts ≥ max).</li>
 *   <li>Re-run {@link SegmentRevalidator#revalidateInputsStillActive}. Any drift
 *       in the narrow window between merge completion and publish ⇒
 *       {@code merger.abandon(output)} + FAILED.</li>
 *   <li>Publish the output via {@link SegmentRegistryClient#createSegment} and
 *       CAS-deprecate every input. A per-input CAS loss is logged and skipped;
 *       the engine's reaper will fold the orphan input into the next tick.</li>
 *   <li>CAS the task to DONE with the new {@code outputSegmentUuid} and delete
 *       the lease.</li>
 * </ol>
 *
 * <p>The source of truth for "this task is claimed" is the task's {@code state}
 * field, not the ephemeral lease. The lease is a liveness signal: its
 * disappearance tells the orphan scanner the worker is gone and the task can
 * be reset. Creating the lease BEFORE the state CAS means a CAS-loser can
 * simply delete its own lease and retry — no permanent stuck-CLAIMED window.
 */
public final class OptimizerTaskConsumer {

    private static final Logger LOGGER = Logger.getLogger(OptimizerTaskConsumer.class.getName());

    private final OptimizerTaskRegistry taskRegistry;
    private final SegmentRegistryClient segmentRegistry;
    private final SegmentRevalidator revalidator;
    private final SegmentMerger merger;
    private final String tablespaceUuid;
    private final String workerId;
    private final long retentionMillis;
    private final int maxAttempts;
    private final LongSupplier clock;

    // Observability counters wired into /metrics by OptimizerHttpServer.
    private final AtomicLong tasksClaimedTotal = new AtomicLong();
    private final AtomicLong tasksCompletedSuccessfullyTotal = new AtomicLong();
    private final AtomicLong tasksCompletedNoopTotal = new AtomicLong();
    private final AtomicLong tasksFailedTotal = new AtomicLong();
    private final AtomicLong tasksPoisonedTotal = new AtomicLong();

    public OptimizerTaskConsumer(OptimizerTaskRegistry taskRegistry,
                                 SegmentRegistryClient segmentRegistry,
                                 SegmentMerger merger,
                                 String tablespaceUuid,
                                 String workerId,
                                 long retentionMillis,
                                 int maxAttempts,
                                 LongSupplier clock) {
        this.taskRegistry = Objects.requireNonNull(taskRegistry, "taskRegistry");
        this.segmentRegistry = Objects.requireNonNull(segmentRegistry, "segmentRegistry");
        this.revalidator = new SegmentRevalidator(segmentRegistry, tablespaceUuid);
        this.merger = Objects.requireNonNull(merger, "merger");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.retentionMillis = retentionMillis;
        this.maxAttempts = maxAttempts;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @return {@code true} if a task was claimed and processed this call (the
     *     caller should attempt another); {@code false} when no PENDING tasks
     *     remained or every candidate lost the claim race.
     */
    public boolean consumeOneTask() throws OptimizerTaskRegistryException, SegmentRegistryException {
        VersionedOptimizerTask oldest = pickOldestPending();
        if (oldest == null) {
            return false;
        }
        String taskId = oldest.task().getTaskId();
        // Step 1: lease-first
        boolean leaseAcquired;
        try {
            leaseAcquired = taskRegistry.createLease(tablespaceUuid, taskId, workerId);
        } catch (OptimizerTaskRegistryException.TaskNotFound disappeared) {
            return false;
        }
        if (!leaseAcquired) {
            return false;
        }
        // Step 2: CAS state PENDING → CLAIMED
        OptimizerTask claimedView = oldest.task().toBuilder()
                .state(OptimizerTaskState.CLAIMED)
                .claim(new OptimizerTask.WorkerClaim(workerId, clock.getAsLong()))
                .build();
        VersionedOptimizerTask claimed;
        try {
            claimed = taskRegistry.casUpdateTask(oldest, claimedView);
        } catch (OptimizerTaskRegistryException.VersionMismatch lost) {
            taskRegistry.deleteLease(tablespaceUuid, taskId);
            return false;
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            return false;
        }
        tasksClaimedTotal.incrementAndGet();
        return executeClaimedTask(claimed);
    }

    public long getTasksClaimedTotal() {
        return tasksClaimedTotal.get();
    }

    public long getTasksCompletedSuccessfullyTotal() {
        return tasksCompletedSuccessfullyTotal.get();
    }

    public long getTasksCompletedNoopTotal() {
        return tasksCompletedNoopTotal.get();
    }

    public long getTasksFailedTotal() {
        return tasksFailedTotal.get();
    }

    public long getTasksPoisonedTotal() {
        return tasksPoisonedTotal.get();
    }

    private VersionedOptimizerTask pickOldestPending() throws OptimizerTaskRegistryException {
        List<VersionedOptimizerTask> all = taskRegistry.listTasks(tablespaceUuid);
        VersionedOptimizerTask oldest = null;
        long oldestAt = Long.MAX_VALUE;
        for (VersionedOptimizerTask vt : all) {
            if (vt.task().getState() != OptimizerTaskState.PENDING) {
                continue;
            }
            long createdAt = vt.task().getCreatedAtEpochMillis();
            if (createdAt < oldestAt) {
                oldestAt = createdAt;
                oldest = vt;
            }
        }
        return oldest;
    }

    private boolean executeClaimedTask(VersionedOptimizerTask claimed)
            throws OptimizerTaskRegistryException, SegmentRegistryException {
        OptimizerTask task = claimed.task();
        // Step 3: re-read inputs at expected versions
        List<VersionedSegmentMetadata> inputs = readInputsAtExpectedVersions(task);
        if (inputs == null) {
            return finishWithNoopDone(claimed, "inputs drifted before merge");
        }
        // Step 4: merge
        List<SegmentMetadata> inputMetadata = new ArrayList<>(inputs.size());
        for (VersionedSegmentMetadata v : inputs) {
            inputMetadata.add(v.metadata());
        }
        SegmentMetadata output;
        try {
            output = merger.merge(inputMetadata, task.getTargetOwnerInstanceId());
        } catch (Exception mergerThrew) {
            // Broad catch (SegmentMerger.merge declares "throws Exception" — it is a
            // plugin boundary that may throw anything from a remote-file gRPC error
            // to a jvector graph rebuild failure). Surface as task FAILED / POISON
            // so the optimizer keeps making progress on other tasks.
            LOGGER.log(Level.WARNING,
                    "merger threw while processing task {0}: {1}",
                    new Object[]{task.getTaskId(), mergerThrew.getMessage()});
            if (LOGGER.isLoggable(Level.FINE)) {
                // Log each input's feature set to help diagnose heterogeneous-feature
                // failures (issue #543: "Each source must have the same features").
                StringBuilder sb = new StringBuilder("  Input segment feature sets:");
                for (VersionedSegmentMetadata v : inputs) {
                    sb.append("\n    ").append(v.metadata().getSegmentUuid())
                            .append(" -> ").append(v.metadata().getJvectorFeatureIds());
                }
                LOGGER.log(Level.FINE, sb.toString());
            }
            return finishWithFailureOrPoison(claimed,
                    "merger threw: " + mergerThrew.getMessage());
        }
        if (output == null) {
            // Merger declined — record as DONE (no-op) so the producer doesn't loop on it.
            return finishWithNoopDone(claimed, "merger declined");
        }
        // Step 5: revalidate immediately before publish
        if (!revalidator.revalidateInputsStillActive(task.getIndexUuid(), inputs)) {
            abandonBestEffort(output);
            return finishWithFailureOrPoison(claimed, "inputs drifted between merge and publish");
        }
        // Step 6: publish output + deprecate inputs
        try {
            segmentRegistry.createSegment(output);
        } catch (SegmentRegistryException.SegmentAlreadyExists ok) {
            LOGGER.log(Level.INFO, "merged segment {0} already registered (idempotent)",
                    output.getSegmentUuid());
        }
        long retentionUntil = retentionMillis == 0L
                ? SegmentMetadata.NO_RETENTION
                : clock.getAsLong() + retentionMillis;
        for (VersionedSegmentMetadata v : inputs) {
            SegmentMetadata deprecated = v.metadata().toBuilder()
                    .state(SegmentState.DEPRECATED)
                    .replacedBy(Collections.singletonList(output.getSegmentUuid()))
                    .retentionUntilEpochMillis(retentionUntil)
                    .build();
            try {
                segmentRegistry.casUpdateSegment(v, deprecated);
            } catch (SegmentRegistryException.VersionMismatch raced) {
                LOGGER.log(Level.INFO,
                        "input segment {0} CAS bumped between revalidate and deprecate; "
                                + "engine reaper will fold it on the next tick",
                        v.metadata().getSegmentUuid());
            }
        }
        // Step 7: mark DONE
        tasksCompletedSuccessfullyTotal.incrementAndGet();
        OptimizerTask done = task.toBuilder()
                .state(OptimizerTaskState.DONE)
                .outputSegmentUuid(output.getSegmentUuid())
                .build();
        try {
            taskRegistry.casUpdateTask(claimed, done);
        } catch (OptimizerTaskRegistryException.VersionMismatch raced) {
            LOGGER.log(Level.WARNING,
                    "could not mark task {0} as DONE (CAS lost); the orphan scanner will reconcile",
                    task.getTaskId());
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            // Task znode disappeared between our CAS-claim and now — either
            // the orphan scanner raced us at the orphan-reset boundary and
            // deleted the task (because it observed an input already
            // DEPRECATED by us mid-loop above) or an operator removed it.
            // The merge output is published and inputs are deprecated; the
            // work landed. Surface as a no-op + INFO log so the scheduler
            // tick is not aborted.
            LOGGER.log(Level.INFO,
                    "task {0} znode was deleted while we were processing it;"
                            + " merge output {1} is already published — treating as success",
                    new Object[]{task.getTaskId(), output.getSegmentUuid()});
        }
        try {
            taskRegistry.deleteLease(tablespaceUuid, task.getTaskId());
        } catch (OptimizerTaskRegistryException leaseRemoval) {
            // Best-effort: lease may already be gone with the task znode.
            LOGGER.log(Level.FINE,
                    "lease removal for task {0} failed (already gone?): {1}",
                    new Object[]{task.getTaskId(), leaseRemoval.getMessage()});
        }
        return true;
    }

    private List<VersionedSegmentMetadata> readInputsAtExpectedVersions(OptimizerTask task)
            throws SegmentRegistryException {
        List<VersionedSegmentMetadata> inputs = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : task.getInputSegmentExpectedVersions().entrySet()) {
            Optional<VersionedSegmentMetadata> v = segmentRegistry.getSegment(
                    tablespaceUuid, task.getIndexUuid(), entry.getKey());
            if (!v.isPresent()) {
                return null;
            }
            VersionedSegmentMetadata vsm = v.get();
            if (vsm.zkVersion() != entry.getValue()
                    || vsm.metadata().getState() != SegmentState.ACTIVE) {
                return null;
            }
            inputs.add(vsm);
        }
        return inputs;
    }

    private boolean finishWithNoopDone(VersionedOptimizerTask claimed, String reason)
            throws OptimizerTaskRegistryException {
        tasksCompletedNoopTotal.incrementAndGet();
        OptimizerTask done = claimed.task().toBuilder()
                .state(OptimizerTaskState.DONE)
                .outputSegmentUuid(null)
                .lastError(new OptimizerTask.LastError(workerId, reason, clock.getAsLong()))
                .build();
        try {
            taskRegistry.casUpdateTask(claimed, done);
        } catch (OptimizerTaskRegistryException.VersionMismatch
                | OptimizerTaskRegistryException.TaskNotFound recovered) {
            // VersionMismatch: orphan scanner reconciles on the next tick.
            // TaskNotFound: scanner already deleted the task — the no-op
            // outcome is consistent with "task is gone".
        }
        try {
            taskRegistry.deleteLease(tablespaceUuid, claimed.task().getTaskId());
        } catch (OptimizerTaskRegistryException leaseRemoval) {
            LOGGER.log(Level.FINE,
                    "lease removal for task {0} failed (already gone?): {1}",
                    new Object[]{claimed.task().getTaskId(), leaseRemoval.getMessage()});
        }
        return true;
    }

    private boolean finishWithFailureOrPoison(VersionedOptimizerTask claimed, String reason)
            throws OptimizerTaskRegistryException {
        OptimizerTask t = claimed.task();
        int nextAttempts = t.getAttempts() + 1;
        OptimizerTaskState nextState = nextAttempts >= maxAttempts
                ? OptimizerTaskState.POISON
                : OptimizerTaskState.FAILED;
        if (nextState == OptimizerTaskState.POISON) {
            tasksPoisonedTotal.incrementAndGet();
        } else {
            tasksFailedTotal.incrementAndGet();
        }
        OptimizerTask failed = t.toBuilder()
                .state(nextState)
                .attempts(nextAttempts)
                .lastError(new OptimizerTask.LastError(workerId, reason, clock.getAsLong()))
                .build();
        try {
            taskRegistry.casUpdateTask(claimed, failed);
        } catch (OptimizerTaskRegistryException.VersionMismatch
                | OptimizerTaskRegistryException.TaskNotFound recovered) {
            // Same recovery semantics as finishWithNoopDone: orphan scanner
            // reconciles, or the task was already deleted.
        }
        try {
            taskRegistry.deleteLease(tablespaceUuid, t.getTaskId());
        } catch (OptimizerTaskRegistryException leaseRemoval) {
            LOGGER.log(Level.FINE,
                    "lease removal for task {0} failed (already gone?): {1}",
                    new Object[]{t.getTaskId(), leaseRemoval.getMessage()});
        }
        return true;
    }

    private void abandonBestEffort(SegmentMetadata output) {
        try {
            merger.abandon(output);
        } catch (RuntimeException abandonFailed) {
            LOGGER.log(Level.WARNING,
                    "merger.abandon failed for {0}: {1}",
                    new Object[]{output.getSegmentUuid(), abandonFailed.getMessage()});
        }
    }
}
