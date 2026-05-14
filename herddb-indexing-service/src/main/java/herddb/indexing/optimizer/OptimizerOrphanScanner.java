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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Drives the producer's housekeeping pass over the task queue (step 5):
 * <ol>
 *   <li>Resets {@code CLAIMED} tasks whose ephemeral lease has expired
 *       (worker died / session lost) AND whose age exceeds
 *       {@code orphanResetMs}. POISON-eligible tasks (attempts &gt;= max) are
 *       transitioned to {@link OptimizerTaskState#POISON} instead.</li>
 *   <li>If an orphaned task's input set already shows any DEPRECATED segment,
 *       the work was already finished by another worker (the stateless engine
 *       will re-pick whatever remains ACTIVE next tick) — delete the task
 *       znode outright via {@code casDeleteTask}.</li>
 *   <li>Issue #555: aborts {@code AWAITING_ACK} tasks whose
 *       {@code provisionalOutputCreatedAtEpochMillis} is older than
 *       {@code provisionalGcMs}. The abort path deletes the staged output's
 *       multipart files (best-effort), its znode, and its acks subtree,
 *       leaving the inputs ACTIVE so a future tick can retry.</li>
 *   <li>GCs terminal {@code DONE}/{@code FAILED}/{@code POISON} tasks that
 *       have lived past their retention window (POISON uses a separate longer
 *       window so operators have time to investigate).</li>
 * </ol>
 *
 * <p>Idempotent: running this twice produces the same outcome. Wraps every
 * CAS in a best-effort try block — a {@code VersionMismatch} just means we
 * lost the race; the next tick handles it.
 */
public final class OptimizerOrphanScanner {

    private static final Logger LOGGER = Logger.getLogger(OptimizerOrphanScanner.class.getName());

    private final OptimizerTaskRegistry taskRegistry;
    private final SegmentRegistryClient segmentRegistry;
    private final String tablespaceUuid;
    private final int maxAttempts;
    private final long orphanResetMs;
    private final long terminalRetentionMs;
    private final long poisonRetentionMs;
    /**
     * Issue #555. Maximum age of an {@code AWAITING_ACK} task before the
     * scanner aborts its staged swap (deletes the output's multipart files,
     * znode, and acks tree; leaves inputs ACTIVE; transitions the task to
     * FAILED / POISON). Should be ≥ {@code swapAckTimeoutMs} so the
     * consumer's own polling tick gets a chance to commit successful
     * acknowledgements first.
     */
    private final long provisionalGcMs;
    private final LongSupplier clock;

    /** Legacy 8-arg constructor used by existing tests that don't care about issue #555. */
    public OptimizerOrphanScanner(OptimizerTaskRegistry taskRegistry,
                                  SegmentRegistryClient segmentRegistry,
                                  String tablespaceUuid,
                                  int maxAttempts,
                                  long orphanResetMs,
                                  long terminalRetentionMs,
                                  long poisonRetentionMs,
                                  LongSupplier clock) {
        this(taskRegistry, segmentRegistry, tablespaceUuid, maxAttempts,
                orphanResetMs, terminalRetentionMs, poisonRetentionMs,
                /* provisionalGcMs */ Long.MAX_VALUE, clock);
    }

    public OptimizerOrphanScanner(OptimizerTaskRegistry taskRegistry,
                                  SegmentRegistryClient segmentRegistry,
                                  String tablespaceUuid,
                                  int maxAttempts,
                                  long orphanResetMs,
                                  long terminalRetentionMs,
                                  long poisonRetentionMs,
                                  long provisionalGcMs,
                                  LongSupplier clock) {
        this.taskRegistry = Objects.requireNonNull(taskRegistry, "taskRegistry");
        this.segmentRegistry = Objects.requireNonNull(segmentRegistry, "segmentRegistry");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.maxAttempts = maxAttempts;
        this.orphanResetMs = orphanResetMs;
        this.terminalRetentionMs = terminalRetentionMs;
        this.poisonRetentionMs = poisonRetentionMs;
        this.provisionalGcMs = provisionalGcMs;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public ScanResult scan() throws OptimizerTaskRegistryException {
        long now = clock.getAsLong();
        long orphansReset = 0;
        long orphansPoisoned = 0;
        long orphansDeletedAfterDeprecate = 0;
        long awaitingAckAborted = 0;
        long terminalGcCount = 0;

        List<VersionedOptimizerTask> all = taskRegistry.listTasks(tablespaceUuid);
        for (VersionedOptimizerTask vt : all) {
            OptimizerTask t = vt.task();
            OptimizerTaskState state = t.getState();
            if (state == OptimizerTaskState.CLAIMED) {
                Outcome outcome = handleClaimed(vt, now);
                switch (outcome) {
                    case RESET: orphansReset++; break;
                    case POISONED: orphansPoisoned++; break;
                    case DELETED_AFTER_DEPRECATE: orphansDeletedAfterDeprecate++; break;
                    default: break;
                }
            } else if (state == OptimizerTaskState.AWAITING_ACK) {
                if (handleAwaitingAck(vt, now)) {
                    awaitingAckAborted++;
                }
            } else if (state.isTerminal()) {
                if (gcIfExpired(vt, now, state)) {
                    terminalGcCount++;
                }
            }
        }
        return new ScanResult(orphansReset, orphansPoisoned,
                orphansDeletedAfterDeprecate, awaitingAckAborted, terminalGcCount);
    }

    private Outcome handleClaimed(VersionedOptimizerTask vt, long now) throws OptimizerTaskRegistryException {
        OptimizerTask t = vt.task();
        if (taskRegistry.leaseExists(tablespaceUuid, t.getTaskId())) {
            return Outcome.SKIPPED;
        }
        long claimedAt = t.getClaim() != null
                ? t.getClaim().getClaimedAtEpochMillis()
                : t.getCreatedAtEpochMillis();
        if (now - claimedAt < orphanResetMs) {
            return Outcome.SKIPPED;
        }
        if (anyInputAlreadyDeprecated(t)) {
            try {
                taskRegistry.casDeleteTask(vt);
                return Outcome.DELETED_AFTER_DEPRECATE;
            } catch (OptimizerTaskRegistryException.VersionMismatch lost) {
                return Outcome.SKIPPED;
            } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
                return Outcome.DELETED_AFTER_DEPRECATE;
            }
        }
        int nextAttempts = t.getAttempts() + 1;
        OptimizerTaskState nextState = nextAttempts >= maxAttempts
                ? OptimizerTaskState.POISON
                : OptimizerTaskState.PENDING;
        OptimizerTask updated = t.toBuilder()
                .state(nextState)
                .attempts(nextAttempts)
                .claim(null)
                .build();
        try {
            taskRegistry.casUpdateTask(vt, updated);
            return nextState == OptimizerTaskState.POISON ? Outcome.POISONED : Outcome.RESET;
        } catch (OptimizerTaskRegistryException.VersionMismatch lost) {
            return Outcome.SKIPPED;
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            return Outcome.SKIPPED;
        }
    }

    /**
     * Issue #555: abort handler for AWAITING_ACK tasks that have exceeded
     * {@link #provisionalGcMs}. Deletes the staged output's znode + acks
     * subtree (multipart files are left to the next operator pass — the
     * scanner doesn't have a DSM handle), leaves the inputs ACTIVE, and
     * transitions the task to FAILED / POISON per maxAttempts. Returns
     * true when the abort actually fired (state change committed); false
     * otherwise (still within timeout, no output UUID, or CAS loss).
     */
    private boolean handleAwaitingAck(VersionedOptimizerTask vt, long now)
            throws OptimizerTaskRegistryException {
        OptimizerTask t = vt.task();
        long stagedAt = t.getProvisionalOutputCreatedAtEpochMillis();
        if (stagedAt <= 0L || now - stagedAt < provisionalGcMs) {
            return false;
        }
        String outputUuid = t.getOutputSegmentUuid();
        if (outputUuid != null) {
            try {
                segmentRegistry.getSegment(tablespaceUuid, t.getIndexUuid(), outputUuid)
                        .ifPresent(staged -> {
                            try {
                                segmentRegistry.casDeleteSegment(staged);
                            } catch (SegmentRegistryException e) {
                                LOGGER.log(Level.WARNING,
                                        "scanner: failed to delete stale PROVISIONAL znode {0}: {1}",
                                        new Object[]{outputUuid, e.getMessage()});
                            }
                        });
            } catch (SegmentRegistryException lookupFailed) {
                LOGGER.log(Level.WARNING,
                        "scanner: could not read PROVISIONAL znode {0}: {1}",
                        new Object[]{outputUuid, lookupFailed.getMessage()});
            }
            try {
                segmentRegistry.deleteSwapAcksTree(outputUuid);
            } catch (SegmentRegistryException ackCleanup) {
                LOGGER.log(Level.WARNING,
                        "scanner: failed to delete acks tree for {0}: {1}",
                        new Object[]{outputUuid, ackCleanup.getMessage()});
            }
        }
        int nextAttempts = t.getAttempts() + 1;
        OptimizerTaskState nextState = nextAttempts >= maxAttempts
                ? OptimizerTaskState.POISON
                : OptimizerTaskState.FAILED;
        OptimizerTask aborted = t.toBuilder()
                .state(nextState)
                .attempts(nextAttempts)
                .lastError(new OptimizerTask.LastError("orphan-scanner",
                        "AWAITING_ACK task aborted by scanner (age "
                                + (now - stagedAt) + " ms > provisionalGcMs " + provisionalGcMs + ")",
                        now))
                .build();
        try {
            taskRegistry.casUpdateTask(vt, aborted);
            return true;
        } catch (OptimizerTaskRegistryException.VersionMismatch raced) {
            // The consumer (or another scanner) progressed the task; we lose harmlessly.
            return false;
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            return false;
        }
    }

    private boolean gcIfExpired(VersionedOptimizerTask vt, long now, OptimizerTaskState state)
            throws OptimizerTaskRegistryException {
        long retention = state == OptimizerTaskState.POISON
                ? poisonRetentionMs
                : terminalRetentionMs;
        if (now - vt.task().getCreatedAtEpochMillis() < retention) {
            return false;
        }
        try {
            taskRegistry.casDeleteTask(vt);
            return true;
        } catch (OptimizerTaskRegistryException.VersionMismatch lost) {
            return false;
        } catch (OptimizerTaskRegistryException.TaskNotFound gone) {
            return false;
        }
    }

    private boolean anyInputAlreadyDeprecated(OptimizerTask t) {
        for (String segUuid : t.getInputSegmentUuids()) {
            try {
                Optional<VersionedSegmentMetadata> v = segmentRegistry.getSegment(
                        tablespaceUuid, t.getIndexUuid(), segUuid);
                if (v.isPresent() && v.get().metadata().getState() == SegmentState.DEPRECATED) {
                    return true;
                }
            } catch (SegmentRegistryException registryFailed) {
                LOGGER.log(Level.FINE,
                        "could not read input segment {0} for orphan scan: {1}",
                        new Object[]{segUuid, registryFailed.getMessage()});
            }
        }
        return false;
    }

    private enum Outcome { RESET, POISONED, DELETED_AFTER_DEPRECATE, SKIPPED }

    /** Per-scan counters exposed for metrics + observability. */
    public static final class ScanResult {
        public final long orphansReset;
        public final long orphansPoisoned;
        public final long orphansDeletedAfterDeprecate;
        /** Issue #555: AWAITING_ACK tasks aborted by the scanner (age > provisionalGcMs). */
        public final long awaitingAckAborted;
        public final long terminalGcCount;

        public ScanResult(long orphansReset, long orphansPoisoned,
                   long orphansDeletedAfterDeprecate, long awaitingAckAborted,
                   long terminalGcCount) {
            this.orphansReset = orphansReset;
            this.orphansPoisoned = orphansPoisoned;
            this.orphansDeletedAfterDeprecate = orphansDeletedAfterDeprecate;
            this.awaitingAckAborted = awaitingAckAborted;
            this.terminalGcCount = terminalGcCount;
        }
    }
}
