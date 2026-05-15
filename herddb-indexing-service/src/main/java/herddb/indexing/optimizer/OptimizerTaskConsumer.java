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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker-side consumer that drives one merge task per call through the
 * issue #555 atomic-swap protocol. Every optimizer pod runs the consumer
 * (the leader also drains tasks in addition to producing).
 *
 * <p>The consumer is the second half of the merge pipeline. The producer
 * enqueues a {@link OptimizerTaskState#PENDING} task; the consumer drives it
 * through the following state machine:
 *
 * <pre>
 *   PENDING ──claim──► CLAIMED ──merge + stage PROVISIONAL──► AWAITING_ACK
 *   AWAITING_ACK ──acks complete + multi-op success──► DONE
 *   AWAITING_ACK ──timeout / CAS exhaust──► FAILED (then POISON after maxAttempts)
 *   CLAIMED ──merger failure / revalidate failure──► FAILED / POISON
 * </pre>
 *
 * <p>A single call to {@link #consumeOneTask()} may handle either kind of
 * transition: if it claims a PENDING task it runs the merge and lands the
 * task in AWAITING_ACK; if it picks up an AWAITING_ACK task whose acks
 * have arrived it commits the swap and lands the task in DONE. Both paths
 * are CAS-ordered, so multiple optimizer pods racing on the same task is
 * safe — the loser sees {@code VersionMismatch} and yields.
 *
 * <p>Issue #555 fix summary:
 * <ul>
 *   <li>The output is staged in ZK as {@link SegmentState#PROVISIONAL} after
 *       the multipart files are uploaded.</li>
 *   <li>Each interested IS pod (primary owner + every shadow replica of that
 *       primary) loads the segment locally and writes an ephemeral
 *       acknowledgement node at
 *       {@code {basePath}/index-segments-acks/{outputSegmentUuid}/{serviceId}}.</li>
 *   <li>When every expected ack is present, the consumer fires
 *       {@link SegmentRegistryClient#atomicSwap} which transitions the output
 *       to {@link SegmentState#ACTIVE} AND every input to
 *       {@link SegmentState#DEPRECATED} inside a single {@code ZooKeeper.multi}
 *       transaction. No watcher snapshot from then on observes a partial
 *       "output ACTIVE without inputs DEPRECATED" or "inputs DEPRECATED
 *       without output ACTIVE" intermediate state.</li>
 *   <li>On ack timeout, repeated CAS loss, or merger failure the abort path
 *       deletes the staged output's multipart files + znode + acks tree,
 *       leaves inputs ACTIVE, and transitions the task FAILED → POISON.</li>
 * </ul>
 */
public final class OptimizerTaskConsumer {

    private static final Logger LOGGER = Logger.getLogger(OptimizerTaskConsumer.class.getName());

    /**
     * Maximum number of retries when {@link SegmentRegistryClient#atomicSwap}
     * fails with {@code VersionMismatch} (i.e. an input's znode version
     * drifted between our last revalidate and the multi-op). Bounded so a
     * persistently drifting input doesn't loop forever; on exhaustion we
     * abort the swap and mark the task FAILED.
     */
    private static final int ATOMIC_SWAP_MAX_RETRIES = 3;

    private final OptimizerTaskRegistry taskRegistry;
    private final SegmentRegistryClient segmentRegistry;
    private final SegmentRevalidator revalidator;
    /**
     * Active merger. Declared {@code volatile} (not {@code final}) so that
     * {@link IndexOptimizerMain#maybeUpgradeMerger()} can propagate a
     * NoopMerger → RemoteSegmentMerger upgrade to this consumer after startup
     * (issue #542). The upgrade write happens inside the {@code synchronized}
     * {@code maybeUpgradeMerger()} method; the {@code volatile} keyword ensures
     * visibility for the unsynchronized {@link #consumeOneTask()} reader.
     */
    private volatile SegmentMerger merger;
    private final String tablespaceUuid;
    private final String workerId;
    private final long retentionMillis;
    private final int maxAttempts;
    private final long swapAckTimeoutMillis;
    private final LongSupplier clock;

    /**
     * Test hook (package-private) — invoked AFTER {@code driveOneAwaitingAck}
     * has observed every expected ack and re-read the inputs, but BEFORE the
     * {@link SegmentRegistryClient#atomicSwap} multi-op. Used by
     * {@code OptimizerSwapAtomicityTest} to delete an ack ephemeral in the
     * window between the ack check and the commit, proving the swap still
     * commits (the multi-op operates only on segment znodes, never on the
     * acks subtree). Production code never sets this field.
     */
    volatile Runnable postAckCheckPreSwapHookForTests;

    // Observability counters wired into /metrics by OptimizerHttpServer.
    private final AtomicLong tasksClaimedTotal = new AtomicLong();
    private final AtomicLong tasksCompletedSuccessfullyTotal = new AtomicLong();
    private final AtomicLong tasksCompletedNoopTotal = new AtomicLong();
    private final AtomicLong tasksFailedTotal = new AtomicLong();
    private final AtomicLong tasksPoisonedTotal = new AtomicLong();
    /** Issue #555: tasks the consumer staged as PROVISIONAL and now waiting for acks. */
    private final AtomicLong tasksStagedProvisionalTotal = new AtomicLong();
    /** Issue #555: tasks for which the atomic-swap multi-op succeeded. */
    private final AtomicLong tasksSwapCommittedTotal = new AtomicLong();
    /** Issue #555: tasks whose ack wait timed out (abort path fired). */
    private final AtomicLong tasksAckTimeoutTotal = new AtomicLong();
    /** Issue #555: tasks whose atomic-swap multi-op exhausted retries (abort path fired). */
    private final AtomicLong tasksSwapCasExhaustedTotal = new AtomicLong();

    /**
     * Legacy 8-arg constructor for tests that don't yet pass a swap-ack
     * timeout. Falls back to {@code 60_000 ms} which mirrors
     * {@link OptimizerConfiguration#PROPERTY_SWAP_ACK_TIMEOUT_MS_DEFAULT}.
     */
    public OptimizerTaskConsumer(OptimizerTaskRegistry taskRegistry,
                                 SegmentRegistryClient segmentRegistry,
                                 SegmentMerger merger,
                                 String tablespaceUuid,
                                 String workerId,
                                 long retentionMillis,
                                 int maxAttempts,
                                 LongSupplier clock) {
        this(taskRegistry, segmentRegistry, merger, tablespaceUuid, workerId,
                retentionMillis, maxAttempts, /* swapAckTimeoutMillis */ 60_000L, clock);
    }

    public OptimizerTaskConsumer(OptimizerTaskRegistry taskRegistry,
                                 SegmentRegistryClient segmentRegistry,
                                 SegmentMerger merger,
                                 String tablespaceUuid,
                                 String workerId,
                                 long retentionMillis,
                                 int maxAttempts,
                                 long swapAckTimeoutMillis,
                                 LongSupplier clock) {
        this.taskRegistry = Objects.requireNonNull(taskRegistry, "taskRegistry");
        this.segmentRegistry = Objects.requireNonNull(segmentRegistry, "segmentRegistry");
        this.revalidator = new SegmentRevalidator(segmentRegistry, tablespaceUuid);
        this.merger = Objects.requireNonNull(merger, "merger");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.retentionMillis = retentionMillis;
        this.maxAttempts = maxAttempts;
        this.swapAckTimeoutMillis = swapAckTimeoutMillis;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * @return {@code true} if a task was claimed and progressed this call
     *     (the caller should attempt another); {@code false} when no
     *     PENDING / AWAITING_ACK tasks remained, or every candidate lost the
     *     claim race, or all AWAITING_ACK tasks are still within their
     *     swap-ack timeout window with no acks yet.
     */
    public boolean consumeOneTask() throws OptimizerTaskRegistryException, SegmentRegistryException {
        // Issue #555: first try to drive any AWAITING_ACK task forward (commit
        // multi-op if acks complete, or abort on timeout). Polling ordering
        // matters: a stale AWAITING_ACK task with all acks ready should be
        // committed before we start new work, otherwise its inputs stay
        // blocked from re-selection by the producer.
        boolean progressedAwaiting = drainOneAwaitingAck();
        if (progressedAwaiting) {
            return true;
        }
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

    /**
     * Returns the currently active merger. {@code volatile} read; safe to call
     * from any thread without synchronization. Exposed for observability and
     * testing (e.g. verifying that a NoopMerger → RemoteSegmentMerger upgrade
     * propagated to the consumer — issue #542).
     */
    public SegmentMerger getMerger() {
        return merger;
    }

    /**
     * Package-private: only {@link IndexOptimizerMain#maybeUpgradeMerger()} is
     * expected to call this from production code.
     */
    void setMerger(SegmentMerger newMerger) {
        this.merger = Objects.requireNonNull(newMerger, "newMerger");
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

    public long getTasksStagedProvisionalTotal() {
        return tasksStagedProvisionalTotal.get();
    }

    public long getTasksSwapCommittedTotal() {
        return tasksSwapCommittedTotal.get();
    }

    public long getTasksAckTimeoutTotal() {
        return tasksAckTimeoutTotal.get();
    }

    public long getTasksSwapCasExhaustedTotal() {
        return tasksSwapCasExhaustedTotal.get();
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

    /**
     * Issue #555: scans for any task in {@link OptimizerTaskState#AWAITING_ACK}
     * and drives it forward by either:
     * <ul>
     *   <li>committing the atomic swap when every expected ack is present, or</li>
     *   <li>aborting the swap when {@code now - provisionalCreatedAt > swapAckTimeoutMillis}.</li>
     * </ul>
     * Returns true when a task was driven forward (the caller may
     * immediately try another). Returns false when there are no
     * AWAITING_ACK tasks, or every such task is still inside its ack window
     * and not yet ready to commit.
     *
     * <p>The function is idempotent and CAS-ordered: multiple optimizer pods
     * may race on the same task; the multi-op (or the abort-via-FAILED
     * CAS) is contention-free or yields a {@code VersionMismatch} the
     * loser silently absorbs.
     */
    private boolean drainOneAwaitingAck()
            throws OptimizerTaskRegistryException, SegmentRegistryException {
        List<VersionedOptimizerTask> all = taskRegistry.listTasks(tablespaceUuid);
        for (VersionedOptimizerTask vt : all) {
            if (vt.task().getState() != OptimizerTaskState.AWAITING_ACK) {
                continue;
            }
            if (driveOneAwaitingAck(vt)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to drive a single AWAITING_ACK task forward. Returns true on
     * any state-changing transition (committed, aborted, or yielded to CAS
     * loser); returns false when the task is still waiting and nothing was
     * mutated.
     */
    private boolean driveOneAwaitingAck(VersionedOptimizerTask vt)
            throws OptimizerTaskRegistryException, SegmentRegistryException {
        OptimizerTask t = vt.task();
        String outputUuid = t.getOutputSegmentUuid();
        if (outputUuid == null) {
            // Defensive: a task in AWAITING_ACK without an output UUID is
            // malformed (we set it together with the state transition).
            // Mark FAILED so it doesn't get re-picked indefinitely.
            LOGGER.log(Level.WARNING,
                    "AWAITING_ACK task {0} has no outputSegmentUuid; marking FAILED",
                    t.getTaskId());
            transitionToFailedOrPoison(vt, "AWAITING_ACK without outputSegmentUuid");
            return true;
        }

        // 1. Read the staged output to learn its current state + zkVersion.
        Optional<VersionedSegmentMetadata> stagedOpt = segmentRegistry.getSegment(
                tablespaceUuid, t.getIndexUuid(), outputUuid);
        if (!stagedOpt.isPresent()) {
            // The output znode was deleted out from under us (abort by
            // another pod, or operator cleanup). The task's swap can no
            // longer be completed.
            LOGGER.log(Level.WARNING,
                    "AWAITING_ACK task {0}'s output {1} is gone from ZK; marking FAILED",
                    new Object[]{t.getTaskId(), outputUuid});
            transitionToFailedOrPoison(vt, "PROVISIONAL output disappeared");
            return true;
        }
        VersionedSegmentMetadata staged = stagedOpt.get();
        if (staged.metadata().getState() != SegmentState.PROVISIONAL) {
            // Another pod already committed the swap; we just need to
            // mark the task DONE (best-effort — orphan scanner reconciles
            // if we lose this CAS).
            LOGGER.log(Level.INFO,
                    "AWAITING_ACK task {0}'s output {1} is already {2}; marking task DONE",
                    new Object[]{t.getTaskId(), outputUuid, staged.metadata().getState()});
            transitionToDone(vt, outputUuid);
            return true;
        }

        // 2. Issue #555 fail-closed: a production task (requiresAcks=true)
        //    with an empty expected-acks list is a fatal misconfiguration —
        //    the producer is supposed to skip task creation entirely when it
        //    cannot resolve ack targets. If such a task ever reaches the
        //    consumer, ABORT the swap rather than commit it with zero acks
        //    (committing would re-open the issue #555 data-loss window).
        List<String> expected = t.getExpectedAckServiceIds();
        if (t.isRequiresAcks() && expected.isEmpty()) {
            LOGGER.log(Level.SEVERE,
                    "issue #555: AWAITING_ACK task {0} requiresAcks but has an empty"
                            + " expectedAckServiceIds list — aborting swap of {1} (a"
                            + " production producer must never emit such a task)",
                    new Object[]{t.getTaskId(), outputUuid});
            abortStagedSwap(vt, staged, "requiresAcks task with empty expectedAckServiceIds");
            return true;
        }

        // 3. Are all expected acks present? Check the acks BEFORE the timeout
        //    so that acks landing exactly at the timeout boundary still
        //    commit the swap (a healthy merge must not be spuriously aborted
        //    by a GC pause / loaded CI runner — review follow-up #4).
        Set<String> presentAcks = new HashSet<>(
                segmentRegistry.listSwapAcks(outputUuid, /* watcher */ null));
        boolean allAcksPresent = expected.isEmpty() || presentAcks.containsAll(expected);

        if (!allAcksPresent) {
            // Not all acks in yet — only now do we consider the timeout.
            long nowWaiting = clock.getAsLong();
            long stagedAtWaiting = t.getProvisionalOutputCreatedAtEpochMillis();
            if (stagedAtWaiting > 0L && nowWaiting - stagedAtWaiting > swapAckTimeoutMillis) {
                LOGGER.log(Level.WARNING,
                        "issue #555: AWAITING_ACK task {0} timed out after {1} ms with"
                                + " acks {2}/{3} present; aborting swap of {4}",
                        new Object[]{t.getTaskId(), nowWaiting - stagedAtWaiting,
                                presentAcks.size(), expected.size(), outputUuid});
                tasksAckTimeoutTotal.incrementAndGet();
                abortStagedSwap(vt, staged, "ack timeout ("
                        + (nowWaiting - stagedAtWaiting) + " ms, "
                        + presentAcks.size() + "/" + expected.size() + " acks)");
                return true;
            }
            // Still inside the ack window with acks incomplete — keep waiting.
            return false;
        }
        if (expected.isEmpty()) {
            // Directory-less producer (test / legacy path, requiresAcks=false).
            // Commit immediately: no expected parties.
            LOGGER.log(Level.FINE,
                    "AWAITING_ACK task {0} has empty expectedAckServiceIds (requiresAcks=false);"
                            + " committing swap of {1} immediately",
                    new Object[]{t.getTaskId(), outputUuid});
        }

        // 4. Re-read inputs at their expected znode versions. If any drifted
        //    (e.g. another optimizer aborted-and-recreated since), we cannot
        //    safely commit and must abort.
        List<VersionedSegmentMetadata> inputs = readInputsAtExpectedVersions(t);
        if (inputs == null) {
            LOGGER.log(Level.WARNING,
                    "AWAITING_ACK task {0}: inputs drifted between produce and swap; aborting",
                    t.getTaskId());
            abortStagedSwap(vt, staged, "inputs drifted before swap commit");
            return true;
        }

        // Test hook: inject a mutation (e.g. delete an ack ephemeral) in the
        // window between the ack check and the multi-op. Production never
        // sets this — the atomic swap below operates ONLY on segment znodes,
        // so a vanishing ack here cannot stop a swap the consumer already
        // decided to commit.
        Runnable preSwapHook = postAckCheckPreSwapHookForTests;
        if (preSwapHook != null) {
            preSwapHook.run();
        }

        // 5. Fire the atomic swap with bounded retries against
        //    BadVersionException — a CAS loss means an input got bumped
        //    between our re-read and the multi-op.
        long retentionUntil = retentionMillis == 0L
                ? SegmentMetadata.NO_RETENTION
                : clock.getAsLong() + retentionMillis;
        boolean committed = false;
        SegmentRegistryException lastCasFailure = null;
        for (int attempt = 0; attempt < ATOMIC_SWAP_MAX_RETRIES; attempt++) {
            try {
                segmentRegistry.atomicSwap(staged, inputs, retentionUntil);
                committed = true;
                break;
            } catch (SegmentRegistryException.VersionMismatch versionDrift) {
                lastCasFailure = versionDrift;
                LOGGER.log(Level.INFO,
                        "atomicSwap CAS lost on attempt {0}/{1} for task {2} output {3}: {4}; "
                                + "re-reading and retrying",
                        new Object[]{attempt + 1, ATOMIC_SWAP_MAX_RETRIES,
                                t.getTaskId(), outputUuid, versionDrift.getMessage()});
                // Re-read everything for the next iteration.
                Optional<VersionedSegmentMetadata> rereadOutput = segmentRegistry.getSegment(
                        tablespaceUuid, t.getIndexUuid(), outputUuid);
                if (!rereadOutput.isPresent()
                        || rereadOutput.get().metadata().getState() != SegmentState.PROVISIONAL) {
                    // Output deleted or already committed — bail out of the loop.
                    LOGGER.log(Level.INFO,
                            "atomicSwap retry observed output {0} is no longer PROVISIONAL; yielding",
                            outputUuid);
                    return true;
                }
                staged = rereadOutput.get();
                inputs = readInputsAtExpectedVersions(t);
                if (inputs == null) {
                    LOGGER.log(Level.WARNING,
                            "AWAITING_ACK task {0}: inputs drifted mid-retry; aborting",
                            t.getTaskId());
                    abortStagedSwap(vt, staged, "inputs drifted mid-retry");
                    return true;
                }
            }
        }
        if (!committed) {
            tasksSwapCasExhaustedTotal.incrementAndGet();
            LOGGER.log(Level.WARNING,
                    "atomicSwap CAS exhausted after {0} attempts for task {1} output {2}: {3}; aborting",
                    new Object[]{ATOMIC_SWAP_MAX_RETRIES, t.getTaskId(), outputUuid,
                            lastCasFailure == null ? "n/a" : lastCasFailure.getMessage()});
            abortStagedSwap(vt, staged, "atomicSwap CAS exhausted after "
                    + ATOMIC_SWAP_MAX_RETRIES + " attempts");
            return true;
        }

        // 6. Multi-op succeeded. Clean up the acks tree (best-effort; the
        //    multi-op committed first, so any later ack write is silently
        //    a no-op once the parent disappears).
        try {
            segmentRegistry.deleteSwapAcksTree(outputUuid);
        } catch (SegmentRegistryException acksCleanup) {
            // Best-effort: the swap is committed regardless. Log and move on.
            LOGGER.log(Level.WARNING,
                    "issue #555: post-swap acks-tree cleanup failed for {0}: {1}",
                    new Object[]{outputUuid, acksCleanup.getMessage()});
        }

        tasksSwapCommittedTotal.incrementAndGet();
        tasksCompletedSuccessfullyTotal.incrementAndGet();
        transitionToDone(vt, outputUuid);
        return true;
    }

    /**
     * Drives a freshly CLAIMED task through {@code merge → upload → publish
     * PROVISIONAL → transition to AWAITING_ACK}. The lease is released on
     * transition so any optimizer pod can pick up the ack wait.
     */
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
        // Step 5: revalidate immediately before stage
        if (!revalidator.revalidateInputsStillActive(task.getIndexUuid(), inputs)) {
            abandonBestEffort(output);
            return finishWithFailureOrPoison(claimed, "inputs drifted between merge and stage");
        }

        // Step 6 (issue #555): publish the output as PROVISIONAL (NOT
        // ACTIVE) and pre-create the per-segment acks parent. Inputs stay
        // ACTIVE — we only flip them to DEPRECATED via the atomicSwap
        // multi-op AFTER every interested IS pod has acknowledged.
        long stagedAt = clock.getAsLong();
        SegmentMetadata provisional = output.toBuilder()
                .state(SegmentState.PROVISIONAL)
                .provisionalCreatedAtEpochMillis(stagedAt)
                .build();
        try {
            segmentRegistry.createSegment(provisional);
        } catch (SegmentRegistryException.SegmentAlreadyExists alreadyThere) {
            // Should not happen with random UUIDs, but if it ever did the
            // PROVISIONAL znode is already published; carry on.
            LOGGER.log(Level.INFO,
                    "staged segment {0} already registered (idempotent)",
                    provisional.getSegmentUuid());
        }
        segmentRegistry.createSwapAckParent(provisional.getSegmentUuid());
        tasksStagedProvisionalTotal.incrementAndGet();

        // Step 7: CAS task CLAIMED → AWAITING_ACK, releasing the lease so
        // any optimizer pod can resume the ack wait on the next tick.
        OptimizerTask awaiting = task.toBuilder()
                .state(OptimizerTaskState.AWAITING_ACK)
                .outputSegmentUuid(provisional.getSegmentUuid())
                .provisionalOutputCreatedAtEpochMillis(stagedAt)
                .claim(null)
                .build();
        try {
            taskRegistry.casUpdateTask(claimed, awaiting);
        } catch (OptimizerTaskRegistryException.VersionMismatch raced) {
            // Lost the CAS to a parallel writer; the staged output is in
            // a known good state, so this is not fatal. Subsequent ticks
            // will pick the task up via the AWAITING_ACK path. Just clean
            // up our own lease so we don't hold up other workers.
            LOGGER.log(Level.WARNING,
                    "lost CAS to AWAITING_ACK on task {0}; the orphan scanner / next tick reconciles",
                    task.getTaskId());
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            // Task znode was deleted while we were merging (orphan scanner
            // raced us). The PROVISIONAL output is on disk; the orphan
            // scanner's abort path will clean it up.
            LOGGER.log(Level.INFO,
                    "task {0} znode disappeared while staging PROVISIONAL output {1};"
                            + " orphan scanner will reconcile",
                    new Object[]{task.getTaskId(), provisional.getSegmentUuid()});
        }
        try {
            taskRegistry.deleteLease(tablespaceUuid, task.getTaskId());
        } catch (OptimizerTaskRegistryException leaseRemoval) {
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
                .claim(null)
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
                .claim(null)
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

    /**
     * Issue #555 abort path: the staged PROVISIONAL output is no longer
     * desirable (ack timeout, CAS exhausted, drifted inputs). Tears down
     * its multipart files + znode + acks tree, leaves inputs untouched,
     * and transitions the task to FAILED (then POISON on maxAttempts).
     * Idempotent: every cleanup step is wrapped to tolerate "already gone".
     */
    private void abortStagedSwap(VersionedOptimizerTask vt,
                                 VersionedSegmentMetadata staged,
                                 String reason)
            throws OptimizerTaskRegistryException, SegmentRegistryException {
        SegmentMetadata stagedMeta = staged.metadata();
        String outputUuid = stagedMeta.getSegmentUuid();
        try {
            merger.abandon(stagedMeta);
        } catch (RuntimeException abandonFailed) {
            LOGGER.log(Level.WARNING,
                    "merger.abandon failed for aborted swap {0}: {1}",
                    new Object[]{outputUuid, abandonFailed.getMessage()});
        }
        try {
            segmentRegistry.casDeleteSegment(staged);
        } catch (SegmentRegistryException.SegmentNotFound gone) {
            // Already deleted by another aborter — fine
        } catch (SegmentRegistryException.VersionMismatch raced) {
            // Another writer changed the znode (e.g. committed it via
            // atomicSwap) between our read and delete. Re-read; if it's
            // now ACTIVE, treat the task as DONE.
            Optional<VersionedSegmentMetadata> reread = segmentRegistry.getSegment(
                    tablespaceUuid, vt.task().getIndexUuid(), outputUuid);
            if (reread.isPresent()
                    && reread.get().metadata().getState() == SegmentState.ACTIVE) {
                LOGGER.log(Level.INFO,
                        "abort raced with atomicSwap commit for {0}; marking task DONE",
                        outputUuid);
                tasksSwapCommittedTotal.incrementAndGet();
                tasksCompletedSuccessfullyTotal.incrementAndGet();
                transitionToDone(vt, outputUuid);
                return;
            }
            // Otherwise log and proceed with the abort. The orphan scanner
            // sweep at the configured GC delay will retry the znode delete.
            LOGGER.log(Level.WARNING,
                    "abort CAS lost for {0} (znode is now in state {1}); "
                            + "orphan scanner will clean up on next sweep",
                    new Object[]{outputUuid,
                            reread.map(r -> r.metadata().getState().name()).orElse("GONE")});
        }
        try {
            segmentRegistry.deleteSwapAcksTree(outputUuid);
        } catch (SegmentRegistryException acksCleanup) {
            LOGGER.log(Level.WARNING,
                    "abort: acks-tree cleanup failed for {0}: {1}",
                    new Object[]{outputUuid, acksCleanup.getMessage()});
        }
        transitionToFailedOrPoison(vt, reason);
    }

    /**
     * CAS-transitions a task to {@link OptimizerTaskState#DONE} with the
     * given output uuid (idempotent: VersionMismatch / TaskNotFound are
     * absorbed because they imply another worker already did the work).
     */
    private void transitionToDone(VersionedOptimizerTask vt, String outputUuid)
            throws OptimizerTaskRegistryException {
        OptimizerTask done = vt.task().toBuilder()
                .state(OptimizerTaskState.DONE)
                .outputSegmentUuid(outputUuid)
                .claim(null)
                .build();
        try {
            taskRegistry.casUpdateTask(vt, done);
        } catch (OptimizerTaskRegistryException.VersionMismatch
                | OptimizerTaskRegistryException.TaskNotFound raced) {
            // Orphan scanner reconciles on the next tick.
        }
    }

    private void transitionToFailedOrPoison(VersionedOptimizerTask vt, String reason)
            throws OptimizerTaskRegistryException {
        OptimizerTask t = vt.task();
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
                .claim(null)
                .build();
        try {
            taskRegistry.casUpdateTask(vt, failed);
        } catch (OptimizerTaskRegistryException.VersionMismatch
                | OptimizerTaskRegistryException.TaskNotFound raced) {
            // Either someone else committed the swap (DONE) or the task is gone.
        }
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
