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
                ownerSelector, System::currentTimeMillis);
    }

    /** Test-friendly constructor accepting an injected clock. */
    public IndexOptimizerEngine(SegmentRegistryClient registry,
                                SegmentMerger merger,
                                String tablespaceUuid,
                                MergePolicy mergePolicy,
                                long retentionMillis,
                                IntSupplier ownerSelector,
                                LongSupplier clock) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.merger = Objects.requireNonNull(merger, "merger");
        this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
        this.mergePolicy = Objects.requireNonNull(mergePolicy, "mergePolicy");
        this.retentionMillis = retentionMillis;
        this.ownerSelector = Objects.requireNonNull(ownerSelector, "ownerSelector");
        this.clock = Objects.requireNonNull(clock, "clock");
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

    public void runOnce() throws Exception {
        runs.incrementAndGet();
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

        // 4. Publish the output and deprecate the inputs. We do this without a true
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
                // Someone else (a transfer? another optimizer?) bumped the znode under us.
                // Skip this input on the current tick; the next tick will re-evaluate.
                LOGGER.log(Level.INFO, "input segment {0} CAS bumped; will retry next tick",
                        v.metadata().getSegmentUuid());
            }
        }
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
            // Optionally an integrator could delete the multipart files here; for the
            // MVP we just remove the znode. A retention reaper can sweep orphan files.
            VersionedSegmentMetadata afterStateChange = registry.casUpdateSegment(v,
                    v.metadata().toBuilder().state(SegmentState.DELETED).build());
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
}
