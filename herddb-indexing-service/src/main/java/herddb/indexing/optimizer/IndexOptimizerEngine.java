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

import herddb.index.vector.RemoteSegmentGraphMerger;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.TombstoneOverlayManager;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single-tick driver for the index-optimizer service.
 *
 * <p>Each call to {@link #runOnce()}:
 * <ol>
 *   <li>Lists all indexes for the configured tablespace.</li>
 *   <li>For each index, lists the segments and partitions them by state.</li>
 *   <li>Applies the merge policy
 *       (count-based or byte-based, see {@link MergePolicy})
 *       to pick a candidate set of ACTIVE segments to merge.</li>
 *   <li>If a merge fires, calls the {@link SegmentMerger}, then publishes the
 *       output via {@link SegmentRegistryClient#createSegment}, and CAS-marks
 *       the inputs as DEPRECATED with {@code retentionUntilEpochMillis}.</li>
 *   <li>Reaps DEPRECATED segments whose retention has elapsed: CAS them to
 *       DELETED then casDelete the znodes. (Multipart-file deletion from S3
 *       is left to the integrator's {@link SegmentMerger} or a future cleaner;
 *       the registry-side lifecycle is what this engine drives.)</li>
 * </ol>
 *
 * <p>The engine is deliberately stateless across runs: the next {@link #runOnce()}
 * re-reads everything from ZK. That keeps recovery trivial — if the optimizer
 * dies between merge and registry update, the next instance picks up where the
 * old one left off (potential duplicates are addressed by ZK CAS — only the
 * first registration of a given UUID wins).
 *
 * <p>Singleton enforcement is the caller's responsibility (Helm
 * {@code replicas: 1} per the user-approved plan).
 */
public final class IndexOptimizerEngine {

    private static final Logger LOGGER = Logger.getLogger(IndexOptimizerEngine.class.getName());

    private final SegmentRegistryClient registry;
    private final SegmentMerger merger;
    private final String tablespaceUuid;
    private final MergePolicy mergePolicy;
    private final long retentionMillis;
    private final IntSupplier ownerSelector;
    private final LongSupplier clock;
    /**
     * Optional: when non-null, the reaper invokes
     * {@link DataStorageManager#deleteMultipartIndexFile} for the graph + map +
     * current tombstone-overlay files of every reaped segment (review item A8).
     * Pass {@code null} to skip physical-file deletion — useful when the test fakes
     * the storage layer or doesn't care about file residue.
     */
    private final DataStorageManager dataStorageManager;
    /**
     * Optional: when non-null, every {@link #runOnce()} invocation tries to acquire
     * this lock first and skips the tick if it can't. Enforces the singleton
     * invariant across rolling upgrades / k8s evictions where Helm's {@code replicas: 1}
     * alone briefly allows two pods (review item C2). Pass {@code null} for tests
     * that don't want lock contention.
     */
    private final OptimizerLeaderLock leaderLock;
    private final AtomicLong ticksSkippedNotLeader = new AtomicLong();

    /**
     * Test hook (package-private) — invoked AFTER {@link #revalidateInputsStillActive}
     * returns true but BEFORE {@link #deprecateInputSegment} runs on each candidate.
     * Used by {@code OptimizerTransferRaceTest.driftBetweenRevalidateAndDeprecate} to
     * inject ownership-change drift into the narrow remaining race window. Production
     * code never sets this field.
     */
    volatile Runnable postRevalidatePreDeprecateHookForTests;

    /**
     * When {@code true} (the default), the reaper at retention time does NOT call
     * {@link DataStorageManager#deleteMultipartIndexFile}. Production deployments
     * of segmented-v2 require a IS-side {@code SegmentAssignmentWatcher} that
     * notifies owners to close their handles before files vanish; until that
     * wiring exists deleting files would corrupt the IS that still references the
     * segment in its IndexStatus. Operators MUST opt out explicitly by passing
     * {@code safeModeFileDeletion = false} to the long-form constructor
     * {@link #IndexOptimizerEngine(SegmentRegistryClient, SegmentMerger, String, MergePolicy, long, java.util.function.IntSupplier, java.util.function.LongSupplier, herddb.storage.DataStorageManager, OptimizerLeaderLock, boolean)}
     * once the listener wiring is verified end-to-end (review-item B1 from the
     * second pr-reviewer pass).
     */
    private final boolean safeModeFileDeletion;

    private final AtomicLong runs = new AtomicLong();
    private final AtomicLong segmentsMerged = new AtomicLong();
    private final AtomicLong segmentsDeprecated = new AtomicLong();
    private final AtomicLong segmentsDeleted = new AtomicLong();

    /**
     * Compaction-side observability (Grafana panel: "Compaction"). The
     * counters bump only on the leader path so a non-leader pod's metrics
     * line up with the actual work it did.
     */
    private final AtomicLong mergeFailuresTotal = new AtomicLong();
    private final AtomicLong mergeAbortsRevalidateFailedTotal = new AtomicLong();
    private final AtomicLong mergeDeclinedTotal = new AtomicLong();
    /** Last successful merge wall-clock duration in milliseconds; -1 means "no merge yet". */
    private final AtomicLong lastMergeDurationMs = new AtomicLong(-1L);
    /** Wall-clock millis when the last tick body STARTED running; -1 means "never run". */
    private final AtomicLong lastRunAtMillis = new AtomicLong(-1L);
    /** Number of indexes observed in the most recent tick (cardinality of {@code listIndexes}). */
    private final AtomicLong observedIndexes = new AtomicLong();
    /** Snapshot of segment counts from the most recent tick, by state. */
    private final AtomicLong observedActiveSegments = new AtomicLong();
    private final AtomicLong observedDeprecatedSegments = new AtomicLong();
    private final AtomicLong observedTransferringSegments = new AtomicLong();
    private final AtomicLong observedProvisionalSegments = new AtomicLong();
    /**
     * Optimizer-driven ownership relocations. The optimizer doesn't currently
     * relocate segments itself (the protocol primitives in
     * {@code OwnershipTransfer} are dormant in production), so these counters
     * stay at 0 until the relocate path is wired. They are wired into the
     * Grafana dashboard now so the panels are ready when the wiring lands.
     */
    private final AtomicLong relocationsInitiatedTotal = new AtomicLong();
    private final AtomicLong relocationsCompletedTotal = new AtomicLong();
    private final AtomicLong relocationsAbortedTotal = new AtomicLong();

    // -------------------------------------------------------------------------
    // Extended observability (issue #503)
    // -------------------------------------------------------------------------

    private static final MemoryMXBean MEMORY_MX = ManagementFactory.getMemoryMXBean();

    /**
     * Per-phase cumulative timing counters (milliseconds). Updated after each
     * successful merge cycle; exposed via {@code GET /metrics}.
     */
    private final AtomicLong phaseDownloadMsTotal    = new AtomicLong();
    private final AtomicLong phaseCompactionMsTotal  = new AtomicLong();
    private final AtomicLong phasePqTrainingMsTotal  = new AtomicLong();
    private final AtomicLong phaseUploadMsTotal      = new AtomicLong();
    private final AtomicLong phaseZkRegisterMsTotal  = new AtomicLong();
    private final AtomicLong phaseDeprecateMsTotal   = new AtomicLong();

    /**
     * Live-state holder for the current merge. Read by the HTTP server thread
     * for {@code GET /status}; written by the optimizer thread (single-threaded
     * scheduler). Created at construction so the HTTP server always gets a
     * non-null reference.
     */
    private final MergeProgress mergeProgress = new MergeProgress();

    /**
     * Bounded history of the last {@value #MERGE_HISTORY_MAX} completed merges
     * (including declined and failed ones). Guarded by {@code synchronized}
     * on {@code mergeHistory} for thread-safe access from the HTTP server.
     */
    private static final int MERGE_HISTORY_MAX = 20;
    private final Deque<MergeRecord> mergeHistory = new ArrayDeque<>(MERGE_HISTORY_MAX + 1);

    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector) {
        this(registry, merger, tablespaceUuid, mergePolicy, retentionMillis,
                ownerSelector, System::currentTimeMillis, /* dataStorageManager */ null,
                /* leaderLock */ null);
    }

    /** Test-friendly constructor accepting an injected clock and no DSM / no lock. */
    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector,
                                LongSupplier clock) {
        this(registry, merger, tablespaceUuid, mergePolicy, retentionMillis,
                ownerSelector, clock, /* dataStorageManager */ null, /* leaderLock */ null);
    }

    /**
     * Variant that accepts a {@link DataStorageManager} but no leader lock — used by
     * the reaper-file-deletion tests.
     */
    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector,
                                LongSupplier clock,
                                DataStorageManager dataStorageManager) {
        this(registry, merger, tablespaceUuid, mergePolicy, retentionMillis,
                ownerSelector, clock, dataStorageManager, /* leaderLock */ null);
    }

    /**
     * Full constructor wiring a {@link DataStorageManager} (for A8 file deletion)
     * and an {@link OptimizerLeaderLock} (for C2 singleton enforcement). Defaults
     * to {@code safeModeFileDeletion = true} so the reaper does NOT physically
     * delete files until the operator opts in via the next constructor.
     */
    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector,
                                LongSupplier clock,
                                DataStorageManager dataStorageManager,
                                OptimizerLeaderLock leaderLock) {
        this(registry, merger, tablespaceUuid, mergePolicy, retentionMillis,
                ownerSelector, clock, dataStorageManager, leaderLock,
                /* safeModeFileDeletion */ true);
    }

    /**
     * Power-user constructor that lets the deployer opt out of safe-mode and let
     * the reaper physically delete graph/map/tombstone files when transitioning
     * DEPRECATED → DELETED (review-item B1). Set
     * {@code safeModeFileDeletion = false} ONLY when the IS-side
     * {@code SegmentAssignmentWatcher} wiring is in place — otherwise the IS
     * still references the segment in IndexStatus and will fail to load on the
     * next restart.
     */
    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector,
                                LongSupplier clock,
                                DataStorageManager dataStorageManager,
                                OptimizerLeaderLock leaderLock,
                                boolean safeModeFileDeletion) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.merger = Objects.requireNonNull(merger, "merger");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.mergePolicy = Objects.requireNonNull(mergePolicy, "mergePolicy");
        this.retentionMillis = retentionMillis;
        this.ownerSelector = Objects.requireNonNull(ownerSelector, "ownerSelector");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.dataStorageManager = dataStorageManager;
        this.leaderLock = leaderLock;
        this.safeModeFileDeletion = safeModeFileDeletion;
        if (!safeModeFileDeletion && dataStorageManager == null) {
            throw new IllegalArgumentException(
                    "safeModeFileDeletion=false requires a non-null DataStorageManager");
        }
        if (!safeModeFileDeletion) {
            LOGGER.log(Level.SEVERE,
                    "OPTIMIZER FILE-DELETION ENABLED (safeMode=false): the optimizer will"
                            + " physically delete multipart graph/map/tombstone files at"
                            + " retention. Verify that every IS instance hosting this"
                            + " tablespace has a SegmentAssignmentWatcher consuming"
                            + " ownership-change events; otherwise the IS will fail to"
                            + " load on next restart with file-not-found.");
        }
    }

    public long getRuns() {
        return runs.get();
    }

    public long getSegmentsMerged() {
        return segmentsMerged.get();
    }

    public long getSegmentsDeprecated() {
        return segmentsDeprecated.get();
    }

    public long getSegmentsDeleted() {
        return segmentsDeleted.get();
    }

    public long getMergeFailuresTotal() {
        return mergeFailuresTotal.get();
    }

    public long getMergeAbortsRevalidateFailedTotal() {
        return mergeAbortsRevalidateFailedTotal.get();
    }

    public long getMergeDeclinedTotal() {
        return mergeDeclinedTotal.get();
    }

    /** Last successful merge duration in milliseconds, or {@code -1} if no merge has run. */
    public long getLastMergeDurationMs() {
        return lastMergeDurationMs.get();
    }

    /** Wall-clock millis at which the last tick body started, or {@code -1} if never run. */
    public long getLastRunAtMillis() {
        return lastRunAtMillis.get();
    }

    /** Number of indexes observed in the most recent tick (cardinality of {@code listIndexes}). */
    public long getObservedIndexes() {
        return observedIndexes.get();
    }

    public long getObservedActiveSegments() {
        return observedActiveSegments.get();
    }

    public long getObservedDeprecatedSegments() {
        return observedDeprecatedSegments.get();
    }

    public long getObservedTransferringSegments() {
        return observedTransferringSegments.get();
    }

    public long getObservedProvisionalSegments() {
        return observedProvisionalSegments.get();
    }

    public long getRelocationsInitiatedTotal() {
        return relocationsInitiatedTotal.get();
    }

    public long getRelocationsCompletedTotal() {
        return relocationsCompletedTotal.get();
    }

    public long getRelocationsAbortedTotal() {
        return relocationsAbortedTotal.get();
    }

    public long getTicksSkippedNotLeader() {
        return ticksSkippedNotLeader.get();
    }

    // --- Extended observability getters (issue #503) ---

    public long getPhaseDownloadMsTotal() {
        return phaseDownloadMsTotal.get();
    }

    public long getPhaseCompactionMsTotal() {
        return phaseCompactionMsTotal.get();
    }

    public long getPhasePqTrainingMsTotal() {
        return phasePqTrainingMsTotal.get();
    }

    public long getPhaseUploadMsTotal() {
        return phaseUploadMsTotal.get();
    }

    public long getPhaseZkRegisterMsTotal() {
        return phaseZkRegisterMsTotal.get();
    }

    public long getPhaseDeprecateMsTotal() {
        return phaseDeprecateMsTotal.get();
    }

    /** Returns the live-merge state (never null). Safe to read from any thread. */
    public MergeProgress getMergeProgress() {
        return mergeProgress;
    }

    /**
     * Returns a snapshot of the merge history (most-recent-last order).
     * Thread-safe; returns a copy so the caller is not affected by subsequent
     * writes.
     */
    public List<MergeRecord> getMergeHistory() {
        synchronized (mergeHistory) {
            return new ArrayList<>(mergeHistory);
        }
    }

    /**
     * Runs one optimizer tick. Throws {@link SegmentRegistryException} only on
     * top-level registry failures (e.g. ZK session expiry while listing indexes);
     * per-index failures are caught internally and logged, so a single bad index
     * does not stop the tick (review item H1: narrow exception types).
     */
    public void runOnce() throws SegmentRegistryException {
        // Singleton enforcement (review item C2). When a lock is configured, only
        // the leader runs work; everyone else short-circuits. The runs counter still
        // ticks so observability sees liveness regardless of leadership.
        runs.incrementAndGet();
        long now = clock.getAsLong();
        lastRunAtMillis.set(now);
        if (leaderLock != null && !leaderLock.tryAcquire()) {
            ticksSkippedNotLeader.incrementAndGet();
            return;
        }
        List<String> indexes = registry.listIndexes(tablespaceUuid);
        observedIndexes.set(indexes.size());
        // Reset per-tick observed-segment gauges; they are aggregated across
        // all indexes inside runForIndex and represent the most recent
        // top-of-tick snapshot.
        long active = 0;
        long deprecated = 0;
        long transferring = 0;
        long provisional = 0;
        for (String indexUuid : indexes) {
            try {
                long[] counts = runForIndex(indexUuid, now);
                active += counts[0];
                deprecated += counts[1];
                transferring += counts[2];
                provisional += counts[3];
            } catch (RuntimeException | SegmentRegistryException e) {
                LOGGER.log(Level.WARNING, "optimizer tick failed for index " + indexUuid, e);
                // Continue with the next index — a single index failure must not stop the
                // whole tick. We catch RuntimeException + SegmentRegistryException because
                // the merger is a plugin boundary that may throw arbitrary runtime
                // exceptions; SegmentRegistryException covers our own typed failures.
            }
        }
        observedActiveSegments.set(active);
        observedDeprecatedSegments.set(deprecated);
        observedTransferringSegments.set(transferring);
        observedProvisionalSegments.set(provisional);
    }

    /**
     * @return per-state segment counts observed in this tick — index 0 ACTIVE,
     *         1 DEPRECATED, 2 TRANSFERRING, 3 PROVISIONAL. The caller
     *         aggregates across indexes and exposes the totals via the
     *         {@code observedXxxSegments} gauges so a Grafana panel can
     *         display the registry's state distribution at the cadence of
     *         the optimizer interval.
     */
    private long[] runForIndex(String indexUuid, long now) throws SegmentRegistryException {
        List<VersionedSegmentMetadata> all = registry.listSegments(tablespaceUuid, indexUuid);
        List<VersionedSegmentMetadata> active = new ArrayList<>();
        List<VersionedSegmentMetadata> deprecated = new ArrayList<>();
        long transferringCount = 0;
        long provisionalCount = 0;
        for (VersionedSegmentMetadata v : all) {
            switch (v.metadata().getState()) {
                case ACTIVE:
                    active.add(v);
                    break;
                case DEPRECATED:
                    deprecated.add(v);
                    break;
                case TRANSFERRING:
                    transferringCount++;
                    break;
                case PROVISIONAL:
                    provisionalCount++;
                    break;
                default:
                    // DELETED: ignored for merge picking and for observability
                    // (the znode is mid-reap and will disappear soon).
                    break;
            }
        }
        long[] observed = new long[]{active.size(), deprecated.size(),
                transferringCount, provisionalCount};

        // 1. Reap DEPRECATED segments whose retention has elapsed.
        for (VersionedSegmentMetadata v : deprecated) {
            long retentionUntil = v.metadata().getRetentionUntilEpochMillis();
            if (retentionUntil != SegmentMetadata.NO_RETENTION && now >= retentionUntil) {
                deleteRetainedSegment(v);
            }
        }

        // 2. Pick merge candidates from the ACTIVE pool.
        List<VersionedSegmentMetadata> candidates = mergePolicy.pickMergeCandidates(active);
        if (candidates.size() < 2) {
            return observed;
        }

        // 3. Run the merger.
        List<SegmentMetadata> inputMetadata = new ArrayList<>(candidates.size());
        long totalInputVectors = 0;
        for (VersionedSegmentMetadata v : candidates) {
            inputMetadata.add(v.metadata());
            totalInputVectors += v.metadata().getVectorCount();
        }

        // Assign a merge ID and update the live-state holder.
        String mergeId = UUID.randomUUID().toString();
        long peakHeapUsed = MEMORY_MX.getHeapMemoryUsage().getUsed();
        long peakDirectUsed = PlatformDependent.usedDirectMemory();
        // Source the start timestamp from the injected clock before calling
        // enterMerge so the /status snapshot reflects the same clock as the
        // duration accounting below, and so tests with fake clocks see the
        // correct mergeStartMs value in the MergeProgress snapshot.
        long mergeStartMs = clock.getAsLong();
        mergeProgress.enterMerge(mergeId, candidates.size(), totalInputVectors, mergeStartMs);
        if (merger instanceof RemoteSegmentMerger) {
            ((RemoteSegmentMerger) merger).setMergeProgress(mergeProgress);
        }

        SegmentMetadata output;
        try {
            output = merger.merge(inputMetadata, ownerSelector.getAsInt());
        } catch (Exception e) {
            mergeFailuresTotal.incrementAndGet();
            long mergeEndMs2 = clock.getAsLong();
            LOGGER.log(Level.WARNING,
                    "merger failed for index " + indexUuid + " (" + candidates.size()
                            + " candidates); leaving inputs untouched", e);
            recordMergeHistory(mergeId, indexUuid, mergeStartMs, mergeEndMs2, "failed",
                    candidates.size(), totalInputVectors, 0L, peakHeapUsed, peakDirectUsed);
            mergeProgress.enterIdle();
            return observed;
        } finally {
            if (merger instanceof RemoteSegmentMerger) {
                // Clear the progress holder in the finally block so the HTTP thread
                // stops receiving stale callbacks after the merge call returns.
                // Wrapped in its own try/catch so a failure here cannot mask the
                // original merge exception thrown above.
                try {
                    ((RemoteSegmentMerger) merger).setMergeProgress(null);
                } catch (RuntimeException clearFailed) {
                    LOGGER.log(Level.WARNING,
                            "setMergeProgress(null) threw unexpectedly; "
                                    + "original exception (if any) is preserved", clearFailed);
                }
            }
        }

        if (output == null) {
            // Merger declined this batch (e.g., not enough live entries); skip publishing.
            mergeDeclinedTotal.incrementAndGet();
            long mergeEndMs2 = clock.getAsLong();
            recordMergeHistory(mergeId, indexUuid, mergeStartMs, mergeEndMs2, "declined",
                    candidates.size(), totalInputVectors, 0L, peakHeapUsed, peakDirectUsed);
            mergeProgress.enterIdle();
            return observed;
        }

        // 4. Pre-publish validation (review item A9). Re-read each input's znode and
        //    verify it is still ACTIVE at the same version we observed. If any input
        //    drifted (e.g. ownership transfer started, or another optimizer raced and
        //    deprecated it) we ABORT this run — do NOT publish the output. Otherwise
        //    a partial deprecate would leave an orphan ACTIVE output covering some
        //    inputs that get re-merged on the next tick into a SECOND output, with
        //    duplicate PKs across both ACTIVE outputs.
        if (!revalidateInputsStillActive(indexUuid, candidates)) {
            mergeAbortsRevalidateFailedTotal.incrementAndGet();
            LOGGER.log(Level.INFO,
                    "optimizer aborted merge for index {0}: at least one input drifted under us",
                    indexUuid);
            // Notify the merger so it can clean up the multipart artefacts it just
            // uploaded (review-item R4 from the second pr-reviewer pass). The default
            // implementation is a no-op; production mergers override to delete the
            // graph/map files of the abandoned output.
            try {
                merger.abandon(output);
            } catch (RuntimeException abandonFailed) {
                LOGGER.log(Level.WARNING,
                        "merger.abandon failed for {0}: {1}",
                        new Object[]{output.getSegmentUuid(), abandonFailed.getMessage()});
            }
            long mergeEndMs2 = clock.getAsLong();
            recordMergeHistory(mergeId, indexUuid, mergeStartMs, mergeEndMs2, "aborted",
                    candidates.size(), totalInputVectors, 0L, peakHeapUsed, peakDirectUsed);
            mergeProgress.enterIdle();
            return observed;
        }

        // 5. Publish the output and deprecate the inputs. We do this without a true
        //    multi-op transaction — instead each step is a CAS. If we crash between
        //    steps the next tick observes a partial state and recovers:
        //    - if create succeeded but inputs not yet deprecated: re-run will see the
        //      new segment; the inputs are still ACTIVE so they get re-merged with
        //      the (now larger) output. We accept that re-merge cost.
        //    - if create failed: nothing happens; inputs remain candidates.
        //
        // Wrapped in a try/catch so any SegmentRegistryException or RuntimeException
        // in the publish/deprecate phase (e.g. ZK session loss between merge completion
        // and createSegment) still resets the live progress state and records a "failed"
        // history entry before propagating.
        long zkRegisterMs = 0L;
        long deprecateMs = 0L;
        try {
            mergeProgress.updatePhase("registering-zk");
            long zkRegisterStartMs = clock.getAsLong();
            try {
                registry.createSegment(output);
                segmentsMerged.incrementAndGet();
            } catch (SegmentRegistryException.SegmentAlreadyExists ok) {
                // Should not happen with random UUIDs, but if it ever did the merged file
                // is already published; fall through to deprecate inputs.
                LOGGER.log(Level.INFO, "merged segment {0} already registered (idempotent)",
                        output.getSegmentUuid());
            }
            zkRegisterMs = Math.max(0L, clock.getAsLong() - zkRegisterStartMs);
            phaseZkRegisterMsTotal.addAndGet(zkRegisterMs);

            // Test hook for review-item R2 (second pr-reviewer pass): inject drift in the
            // narrow window between createSegment-of-output and deprecate-of-inputs.
            Runnable hook = postRevalidatePreDeprecateHookForTests;
            if (hook != null) {
                hook.run();
            }

            mergeProgress.updatePhase("deprecating-inputs");
            long deprecateStartMs = clock.getAsLong();
            long retentionUntil = (retentionMillis == 0L)
                    ? SegmentMetadata.NO_RETENTION
                    : now + retentionMillis;
            for (VersionedSegmentMetadata v : candidates) {
                try {
                    deprecateInputSegment(v, output.getSegmentUuid(), retentionUntil);
                    segmentsDeprecated.incrementAndGet();
                } catch (SegmentRegistryException.VersionMismatch retry) {
                    // Someone else (a transfer? another optimizer?) bumped the znode under us
                    // BETWEEN the revalidation and the deprecate CAS. This is rare given how
                    // close together the two reads are, but if it happens we've already
                    // published the output — leave the input ACTIVE; the next tick will
                    // notice the orphan output covering it and fold it in. Acceptable cost.
                    LOGGER.log(Level.INFO, "input segment {0} CAS bumped; will retry next tick",
                            v.metadata().getSegmentUuid());
                }
            }
            deprecateMs = Math.max(0L, clock.getAsLong() - deprecateStartMs);
            phaseDeprecateMsTotal.addAndGet(deprecateMs);

            // Record successful merge duration. We capture it AFTER deprecate so
            // the duration reflects the full publish-and-deprecate cycle visible
            // to operators, not just the merger.merge() span.
            long mergeEndMs = clock.getAsLong();
            lastMergeDurationMs.set(Math.max(0L, mergeEndMs - mergeStartMs));

            // Accumulate per-phase totals from the merger's timing breakdown.
            RemoteSegmentGraphMerger.MergePhaseTimings timings = null;
            if (merger instanceof RemoteSegmentMerger) {
                timings = ((RemoteSegmentMerger) merger).getLastMergeTimings();
            }
            if (timings != null) {
                phaseDownloadMsTotal.addAndGet(timings.downloadMs);
                phaseCompactionMsTotal.addAndGet(timings.compactionMs);
                phasePqTrainingMsTotal.addAndGet(timings.pqTrainingMs);
                phaseUploadMsTotal.addAndGet(timings.uploadMs);
            }

            recordMergeHistory(mergeId, indexUuid, mergeStartMs, mergeEndMs, "success",
                    candidates.size(), totalInputVectors, output.getVectorCount(),
                    peakHeapUsed, peakDirectUsed, timings, zkRegisterMs, deprecateMs);
            mergeProgress.enterIdle();
        } catch (RuntimeException postMergeFailed) {
            mergeFailuresTotal.incrementAndGet();
            long failedAt = clock.getAsLong();
            LOGGER.log(Level.WARNING,
                    "post-merge publish/deprecate failed for index " + indexUuid
                            + " (output=" + output.getSegmentUuid()
                            + "); progress reset, recording failure", postMergeFailed);
            recordMergeHistory(mergeId, indexUuid, mergeStartMs, failedAt, "failed",
                    candidates.size(), totalInputVectors, 0L, peakHeapUsed, peakDirectUsed);
            mergeProgress.enterIdle();
            throw postMergeFailed;
        } catch (SegmentRegistryException postMergeFailed) {
            mergeFailuresTotal.incrementAndGet();
            long failedAt = clock.getAsLong();
            LOGGER.log(Level.WARNING,
                    "post-merge publish/deprecate failed for index " + indexUuid
                            + " (output=" + output.getSegmentUuid()
                            + "); progress reset, recording failure", postMergeFailed);
            recordMergeHistory(mergeId, indexUuid, mergeStartMs, failedAt, "failed",
                    candidates.size(), totalInputVectors, 0L, peakHeapUsed, peakDirectUsed);
            mergeProgress.enterIdle();
            throw postMergeFailed;
        }
        return observed;
    }

    /**
     * Revalidates that every input segment from the merge-candidate set is still
     * ACTIVE at the same ZK version we observed at pick-time. If any drifted (state
     * change, version bump, znode deletion) we return {@code false} and the caller
     * aborts the run.
     *
     * <p>Hot-path note: this is one extra ZK read per candidate per merge cycle.
     * Merges run every 5 min by default and process 4–200 candidates each — the
     * extra reads are inconsequential.
     */
    private boolean revalidateInputsStillActive(String indexUuid,
                                                List<VersionedSegmentMetadata> candidates) {
        for (VersionedSegmentMetadata v : candidates) {
            try {
                java.util.Optional<VersionedSegmentMetadata> latest = registry.getSegment(
                        tablespaceUuid, indexUuid, v.metadata().getSegmentUuid());
                if (!latest.isPresent()) {
                    LOGGER.log(Level.INFO,
                            "input {0} disappeared from registry between pick and revalidate",
                            v.metadata().getSegmentUuid());
                    return false;
                }
                VersionedSegmentMetadata l = latest.get();
                if (l.zkVersion() != v.zkVersion() || l.metadata().getState() != SegmentState.ACTIVE) {
                    LOGGER.log(Level.INFO,
                            "input {0} drifted between pick (state=ACTIVE, version={1}) and revalidate"
                                    + " (state={2}, version={3})",
                            new Object[]{v.metadata().getSegmentUuid(), v.zkVersion(),
                                    l.metadata().getState(), l.zkVersion()});
                    return false;
                }
            } catch (SegmentRegistryException e) {
                LOGGER.log(Level.WARNING,
                        "revalidate failed for input " + v.metadata().getSegmentUuid()
                                + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    private void deprecateInputSegment(VersionedSegmentMetadata v, String replacementUuid,
                                       long retentionUntilEpochMillis) throws SegmentRegistryException {
        SegmentMetadata next = v.metadata().toBuilder()
                .state(SegmentState.DEPRECATED)
                .replacedBy(Collections.singletonList(replacementUuid))
                .retentionUntilEpochMillis(retentionUntilEpochMillis)
                .build();
        registry.casUpdateSegment(v, next);
    }

    private void deleteRetainedSegment(VersionedSegmentMetadata v) {
        // Reaping order (review-item B3 from second pr-reviewer pass): keep the znode
        // in DEPRECATED until the very last step, then casDelete it directly. The
        // previous design transitioned DEPRECATED → DELETED → casDelete, but
        // runForIndex() does NOT iterate DELETED segments, so a crash between the
        // state-change CAS and the casDelete left an orphan znode that no future
        // tick would clean up. The new ordering is:
        //   1. delete multipart files (idempotent on missing files)
        //   2. casDeleteSegment using the DEPRECATED-state version
        // A crash between (1) and (2) leaves the znode DEPRECATED with files
        // maybe-gone; the next tick re-enters this method via the DEPRECATED branch
        // of runForIndex, re-runs (1) (idempotent), and finishes (2).
        try {
            deleteSegmentFilesBestEffort(v.metadata());
            registry.casDeleteSegment(v);
            segmentsDeleted.incrementAndGet();
        } catch (SegmentRegistryException.VersionMismatch retry) {
            // Defense-in-depth: no current production code path bumps a DEPRECATED
            // znode's version (OwnershipTransfer.initiate requires ACTIVE state),
            // so this catch is unreachable today. If a future caller starts
            // mutating DEPRECATED znodes, the next tick re-enters this method via
            // the DEPRECATED branch and retries idempotently.
        } catch (SegmentRegistryException e) {
            LOGGER.log(Level.WARNING,
                    "failed to delete retained segment " + v.metadata().getSegmentUuid()
                            + ": " + e.getMessage());
        }
    }

    /**
     * Deletes the multipart files referenced by a segment (graph, map, tombstone
     * overlay) via {@link DataStorageManager#deleteMultipartIndexFile}. Best-effort:
     * an undeletable file is logged and tolerated (the znode is still removed). The
     * DSM contract requires {@code deleteMultipartIndexFile} to be idempotent on
     * missing files, so this is safe to retry. Skipped entirely when the engine
     * was constructed without a DSM (unit-test convenience).
     */
    private void deleteSegmentFilesBestEffort(SegmentMetadata m) {
        if (dataStorageManager == null) {
            return;
        }
        if (safeModeFileDeletion) {
            // Safe-mode default (review-item B1): the IS-side ownership-change
            // listener is not yet wired in production — deleting files now would
            // corrupt any IS that still references the segment in its IndexStatus.
            // Skip silently; the znode lifecycle still progresses (DEPRECATED →
            // DELETED) so registry observability is unaffected. A future cleanup
            // pass (or operator) can sweep orphan files once the wiring exists.
            LOGGER.log(Level.FINE,
                    "safeMode: skipping multipart-file deletion for segment {0}",
                    m.getSegmentUuid());
            return;
        }
        if (m.getSegmentId() != SegmentMetadata.NO_SEGMENT_ID) {
            // Graph + map: written by PersistentVectorStore as
            //   uuid = indexUUID + "_seg" + segmentId, fileType = "graph" | "map"
            String multipartUuid = m.getIndexUuid() + "_seg" + m.getSegmentId();
            tryDeleteMultipart(m.getTablespaceUuid(), multipartUuid, "graph", m.getSegmentUuid());
            tryDeleteMultipart(m.getTablespaceUuid(), multipartUuid, "map", m.getSegmentUuid());
        }
        // Tombstone overlay file (if any): written by TombstoneOverlayManager as
        //   uuid = indexUuid + "_seg_" + segmentUuid, fileType = "tombstones-{generation}"
        // We only know the *current* tombstonePath/generation from the znode; older
        // generations are deleted on each successful flush (review item A4) so the
        // current one is the only file still around.
        if (m.getTombstonePath() != null) {
            String tombstoneUuid = TombstoneOverlayManager.multipartUuidFor(
                    m.getIndexUuid(), m.getSegmentUuid());
            // Issue #484: SegmentMetadata now records the overlay generation
            // explicitly so the reaper deletes the correct file directly
            // instead of probing a fixed window of generations. Older payloads
            // (overlayGeneration == 0) fall back to a bounded probe so a v1
            // znode written before the field existed still gets cleaned up.
            long latestGen = m.getOverlayGeneration();
            if (latestGen > 0L) {
                tryDeleteMultipart(m.getTablespaceUuid(), tombstoneUuid,
                        TombstoneOverlayManager.fileTypeFor(latestGen), m.getSegmentUuid());
            } else {
                // Compatibility probe for znodes written before overlayGeneration
                // was added. Hot-path note: this only runs at retention (every
                // retentionMs, default 10 min) — never on the search/insert path.
                for (long gen = 1; gen <= 32; gen++) {
                    tryDeleteMultipart(m.getTablespaceUuid(), tombstoneUuid,
                            TombstoneOverlayManager.fileTypeFor(gen), m.getSegmentUuid());
                }
            }
        }
    }

    private void tryDeleteMultipart(String tableSpace, String uuid, String fileType, String segmentUuid) {
        try {
            dataStorageManager.deleteMultipartIndexFile(tableSpace, uuid, fileType);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.WARNING,
                    "deleteMultipartIndexFile({0},{1},{2}) failed for segment {3}: {4}",
                    new Object[]{tableSpace, uuid, fileType, segmentUuid, e.getMessage()});
        }
    }

    // -------------------------------------------------------------------------
    // Merge history helpers (issue #503)
    // -------------------------------------------------------------------------

    /**
     * Convenience overload for non-success outcomes where no phase timings are
     * available (failed / declined / aborted).
     */
    private void recordMergeHistory(String mergeId, String indexUuid, long startMs, long endMs,
                                    String outcome, int inputSegments, long inputVectors,
                                    long outputVectors, long peakHeapUsed, long peakDirectUsed) {
        recordMergeHistory(mergeId, indexUuid, startMs, endMs, outcome, inputSegments, inputVectors,
                outputVectors, peakHeapUsed, peakDirectUsed, null, 0L, 0L);
    }

    /**
     * Full overload used for the "success" outcome where per-phase timings,
     * ZK-register duration, and deprecate duration are available.
     */
    private void recordMergeHistory(String mergeId, String indexUuid, long startMs, long endMs,
                                    String outcome, int inputSegments, long inputVectors,
                                    long outputVectors, long peakHeapUsed, long peakDirectUsed,
                                    RemoteSegmentGraphMerger.MergePhaseTimings timings,
                                    long zkRegisterMs, long deprecateMs) {
        long downloadMs  = timings != null ? timings.downloadMs   : 0L;
        long pqMs        = timings != null ? timings.pqTrainingMs : 0L;
        long compactMs   = timings != null ? timings.compactionMs : 0L;
        long uploadMs    = timings != null ? timings.uploadMs     : 0L;

        MergeRecord record = new MergeRecord(
                mergeId, indexUuid, startMs, endMs, outcome,
                inputSegments, inputVectors, outputVectors,
                downloadMs, pqMs, compactMs, uploadMs,
                zkRegisterMs, deprecateMs,
                peakHeapUsed, peakDirectUsed);
        synchronized (mergeHistory) {
            mergeHistory.addLast(record);
            while (mergeHistory.size() > MERGE_HISTORY_MAX) {
                mergeHistory.pollFirst();
            }
        }
    }
}
