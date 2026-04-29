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
import herddb.indexing.segment.TombstoneOverlayManager;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

    private final AtomicLong runs = new AtomicLong();
    private final AtomicLong segmentsMerged = new AtomicLong();
    private final AtomicLong segmentsDeprecated = new AtomicLong();
    private final AtomicLong segmentsDeleted = new AtomicLong();

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
     * and an {@link OptimizerLeaderLock} (for C2 singleton enforcement).
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
        this.registry = Objects.requireNonNull(registry, "registry");
        this.merger = Objects.requireNonNull(merger, "merger");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.mergePolicy = Objects.requireNonNull(mergePolicy, "mergePolicy");
        this.retentionMillis = retentionMillis;
        this.ownerSelector = Objects.requireNonNull(ownerSelector, "ownerSelector");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.dataStorageManager = dataStorageManager;
        this.leaderLock = leaderLock;
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

    public long getTicksSkippedNotLeader() {
        return ticksSkippedNotLeader.get();
    }

    public void runOnce() throws Exception {
        // Singleton enforcement (review item C2). When a lock is configured, only
        // the leader runs work; everyone else short-circuits. The runs counter still
        // ticks so observability sees liveness regardless of leadership.
        runs.incrementAndGet();
        if (leaderLock != null && !leaderLock.tryAcquire()) {
            ticksSkippedNotLeader.incrementAndGet();
            return;
        }
        long now = clock.getAsLong();
        List<String> indexes = registry.listIndexes(tablespaceUuid);
        for (String indexUuid : indexes) {
            try {
                runForIndex(indexUuid, now);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "optimizer tick failed for index " + indexUuid, e);
                // Continue with the next index — a single index failure must not stop the
                // whole tick. Broad catch here is intentional: the engine is the supervision
                // boundary for per-index work.
            }
        }
    }

    private void runForIndex(String indexUuid, long now) throws Exception {
        List<VersionedSegmentMetadata> all = registry.listSegments(tablespaceUuid, indexUuid);
        List<VersionedSegmentMetadata> active = new ArrayList<>();
        List<VersionedSegmentMetadata> deprecated = new ArrayList<>();
        for (VersionedSegmentMetadata v : all) {
            switch (v.metadata().getState()) {
                case ACTIVE:
                    active.add(v);
                    break;
                case DEPRECATED:
                    deprecated.add(v);
                    break;
                default:
                    // PROVISIONAL/TRANSFERRING/DELETED: ignored for merge picking.
                    break;
            }
        }

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
            return;
        }

        // 3. Run the merger.
        List<SegmentMetadata> inputMetadata = new ArrayList<>(candidates.size());
        for (VersionedSegmentMetadata v : candidates) {
            inputMetadata.add(v.metadata());
        }
        SegmentMetadata output;
        try {
            output = merger.merge(inputMetadata, ownerSelector.getAsInt());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "merger failed for index " + indexUuid + " (" + candidates.size()
                            + " candidates); leaving inputs untouched", e);
            return;
        }
        if (output == null) {
            // Merger declined this batch (e.g., not enough live entries); skip publishing.
            return;
        }

        // 4. Pre-publish validation (review item A9). Re-read each input's znode and
        //    verify it is still ACTIVE at the same version we observed. If any input
        //    drifted (e.g. ownership transfer started, or another optimizer raced and
        //    deprecated it) we ABORT this run — do NOT publish the output. Otherwise
        //    a partial deprecate would leave an orphan ACTIVE output covering some
        //    inputs that get re-merged on the next tick into a SECOND output, with
        //    duplicate PKs across both ACTIVE outputs.
        if (!revalidateInputsStillActive(indexUuid, candidates)) {
            LOGGER.log(Level.INFO,
                    "optimizer aborted merge for index {0}: at least one input drifted under us",
                    indexUuid);
            return;
        }

        // 5. Publish the output and deprecate the inputs. We do this without a true
        //    multi-op transaction — instead each step is a CAS. If we crash between
        //    steps the next tick observes a partial state and recovers:
        //    - if create succeeded but inputs not yet deprecated: re-run will see the
        //      new segment; the inputs are still ACTIVE so they get re-merged with
        //      the (now larger) output. We accept that re-merge cost.
        //    - if create failed: nothing happens; inputs remain candidates.
        try {
            registry.createSegment(output);
            segmentsMerged.incrementAndGet();
        } catch (SegmentRegistryException.SegmentAlreadyExists ok) {
            // Should not happen with random UUIDs, but if it ever did the merged file
            // is already published; fall through to deprecate inputs.
            LOGGER.log(Level.INFO, "merged segment {0} already registered (idempotent)",
                    output.getSegmentUuid());
        }

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
        try {
            VersionedSegmentMetadata afterStateChange = registry.casUpdateSegment(v,
                    v.metadata().toBuilder().state(SegmentState.DELETED).build());
            // Physically delete the multipart files BEFORE removing the znode (review
            // item A8). The order matters: if we crash after znode delete but before
            // file delete, the files become unreachable orphans (no znode points to
            // them). Doing the files first means a crash leaves the znode in DELETED
            // state with the files maybe-gone — the next tick reaches the same code
            // path, calls deleteMultipartIndexFile (which is idempotent on missing
            // files), and removes the znode. Net effect: idempotent recovery.
            deleteSegmentFilesBestEffort(afterStateChange.metadata());
            registry.casDeleteSegment(afterStateChange);
            segmentsDeleted.incrementAndGet();
        } catch (SegmentRegistryException.VersionMismatch retry) {
            // CAS bumped under us; will be re-attempted on next tick.
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
            // We don't know the exact generation at this point (the znode tracks
            // tombstoneLsn but not the generation explicitly). Probe a window of recent
            // generations to clean up any current + a safety window of stragglers. This
            // is bounded to keep the call O(window) — typical N=1.
            // Hot-path note: this only runs at retention time (every retentionMs, default
            // 10 min) — not per search/insert. Worst-case O(generationWindow) DSM calls
            // per reaped segment, all idempotent.
            for (long gen = 1; gen <= 32; gen++) {
                tryDeleteMultipart(m.getTablespaceUuid(), tombstoneUuid,
                        "tombstones-" + gen, m.getSegmentUuid());
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
}
