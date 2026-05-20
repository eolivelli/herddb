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

package herddb.indexing.vector;

import herddb.indexing.vector.PersistentVectorStore.PendingDelete;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Graph-merge compaction for {@link PersistentVectorStore}.
 *
 * <p>Picks a small number of small on-disk segments, rebuilds a single
 * larger jvector graph from the vectors whose primary key is still
 * authoritative in those inputs (i.e. not tombstoned and not
 * superseded by a newer segment or a live shard), atomically swaps the
 * inputs for the merged output, and queues the input files for
 * retention-aware deletion through {@link PendingDelete}.
 *
 * <p>This class owns the <em>policy</em> pieces that are cheap to unit
 * test in isolation:
 * <ul>
 *   <li>{@link #chooseSegmentsToMerge} — trigger + candidate selection.</li>
 *   <li>{@link #buildAuthorityMap} — live-PK filter across the segment
 *       snapshot and live shards.</li>
 *   <li>{@link #partitionReapable} — retention reaper decision.</li>
 * </ul>
 *
 * <p>The heavy rebuild loop (reading input graphs, issuing a fresh
 * {@code GraphIndexBuilder}, writing a new FusedPQ segment) is driven
 * from {@link PersistentVectorStore} because it needs access to the
 * private write paths; this class exposes the decisions it makes so
 * callers and tests can exercise them without a running store.
 */
final class VectorIndexCompactor {

    private static final Logger LOGGER = Logger.getLogger(VectorIndexCompactor.class.getName());

    private VectorIndexCompactor() {
    }

    /**
     * Test-only observer invoked once per {@code rebuildSegment} call with
     * the synthetic {@link PersistentVectorStore.LiveGraphShard} that was
     * constructed for the compaction output. Tests can install a
     * {@link java.util.function.Consumer} that stashes the reference into
     * a {@link java.lang.ref.WeakReference} to prove the shard (and its
     * per-shard {@link VectorStorage}) is reclaimable after
     * {@code rebuildSegment} returns (issue #256).
     *
     * <p>Must remain {@code null} in production. Reset by tests in a
     * {@code @After} to avoid leaking an observer across test cases.
     */
    static volatile java.util.function.Consumer<PersistentVectorStore.LiveGraphShard>
            syntheticShardObserverForTest;

    /**
     * Master switch for the streaming compaction engine introduced in issue
     * #485. When {@code true}, {@link #rebuildSegment} delegates to
     * {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor}
     * instead of building a fresh in-memory {@link GraphIndexBuilder}. The
     * streaming engine bounds memory by {@code taskWindowSize × maxDegree}
     * instead of {@code numTotalNodes × dimension}, lifting the conservative
     * 1 GB cap previously baked into {@code vector.index.compaction.maxBytes}.
     *
     * <p>The same flag governs the optimizer-pod path
     * ({@link RemoteSegmentGraphMerger}).
     *
     * <p>Default: {@code true}. Operators may flip via the
     * {@code herddb.vectorindex.streamingCompactionEnabled} system property.
     * The non-streaming path is retained as an escape hatch for the lifetime
     * of the issue #485 rollout.
     *
     * <p>Non-final to allow tests to flip the flag in a {@code @Before} /
     * {@code @After} block without process restart.
     */
    static volatile boolean streamingCompactionEnabled =
            Boolean.parseBoolean(System.getProperty(
                    "herddb.vectorindex.streamingCompactionEnabled", "true"));

    /**
     * Process-wide counter of times the streaming path fell back to the
     * legacy in-memory rebuild because {@code OnDiskGraphIndexCompactor}'s
     * {@code sources.size() >= 2} precondition was not met (i.e. only one
     * candidate slipped through). Surfaced for operational visibility — a
     * non-zero value with high cadence indicates the compaction policy is
     * picking single-candidate cycles and the streaming path is effectively
     * idle. Per-process (not per-store) since the flag itself is per-process.
     */
    static final java.util.concurrent.atomic.AtomicLong STREAMING_FALLBACK_TO_LEGACY_TOTAL =
            new java.util.concurrent.atomic.AtomicLong(0);

    /** Test-only / metrics accessor for {@link #STREAMING_FALLBACK_TO_LEGACY_TOTAL}. */
    public static long getStreamingFallbackToLegacyTotal() {
        return STREAMING_FALLBACK_TO_LEGACY_TOTAL.get();
    }

    /**
     * Process-wide counter of completed single-segment graph-file downloads (or
     * sequential-reader opens) across all compaction cycles (issue #602).
     *
     * <p>Incremented once per source segment after its graph file has been
     * successfully written to a local temp file (direct-download path) or
     * copied via the multipart reader (fallback path). Both paths bypass the
     * per-node {@code SegmentBlockCache}/gRPC round-trips that previously
     * caused back-pressure during streaming compaction.
     *
     * <p>Tests may read this counter to verify that the eager-download path was
     * exercised. Reset between tests if needed.
     */
    static final AtomicLong COMPACTION_EAGER_DOWNLOAD_COUNT = new AtomicLong(0);

    /**
     * Process-wide gauge of source-segment graph downloads currently in
     * progress across all compaction cycles. Incremented before each download
     * starts, decremented in the enclosing {@code finally} block.
     */
    static final AtomicLong COMPACTION_EAGER_DOWNLOAD_INFLIGHT = new AtomicLong(0);

    /**
     * Process-wide total of bytes of graph files eagerly downloaded (or copied)
     * for streaming compaction across all cycles (issue #602). Incremented by
     * each source segment's {@code graphFileSize} after a successful transfer.
     */
    static final AtomicLong COMPACTION_EAGER_DOWNLOAD_BYTES = new AtomicLong(0);

    /** Reasons a compaction run can fail; carried through to metrics. */
    enum FailureReason {
        READ_IO,
        WRITE_IO,
        METADATA_IO,
        CORRUPTION,
        DISK_FULL,
        ABORTED_INPUT_GONE
    }

    /** Raised when a compaction cycle cannot complete. */
    static final class CompactionException extends Exception {
        private static final long serialVersionUID = 1L;
        final FailureReason reason;
        /**
         * Multipart files that were partially uploaded (or might have been)
         * before the failure was detected. Each entry is {@code {segUuid,
         * fileType}} and is consumed by
         * {@code PersistentVectorStore.runCompactionCycle}'s
         * {@code catch (CompactionException)} arm to queue retention-aware
         * deletes. May be empty but never {@code null}.
         *
         * <p>Issue #485 review item: previously the rebuild methods rethrew
         * {@code IOException} / {@code DataStorageManagerException} on upload
         * failure, dropping the orphan list with the stack frame. Wrapping
         * them into {@link CompactionException} with this field attached
         * lets the outer cycle's existing orphan-handling code fire on
         * every failure path.
         */
        final List<String[]> orphanPaths;

        CompactionException(FailureReason reason, String message) {
            this(reason, message, null, null);
        }

        CompactionException(FailureReason reason, String message, Throwable cause) {
            this(reason, message, cause, null);
        }

        CompactionException(FailureReason reason, String message, Throwable cause,
                            List<String[]> orphanPaths) {
            super(message, cause);
            this.reason = reason;
            this.orphanPaths = (orphanPaths == null)
                    ? java.util.Collections.emptyList()
                    : new ArrayList<>(orphanPaths);
        }
    }

    /** Outcome of a successful rebuild: the merged segment + bookkeeping. */
    static final class RebuildResult {
        final VectorSegment mergedSegment;
        final long bytesWritten;
        final long vectorCount;
        final long filteredCount;
        /**
         * Partial output files that should be deleted if the enclosing
         * compaction run fails between rebuild and swap. Each entry is
         * {@code {segUuid, fileType}}.
         */
        final List<String[]> orphanPaths;

        RebuildResult(VectorSegment mergedSegment, long bytesWritten,
                      long vectorCount, long filteredCount,
                      List<String[]> orphanPaths) {
            this.mergedSegment = mergedSegment;
            this.bytesWritten = bytesWritten;
            this.vectorCount = vectorCount;
            this.filteredCount = filteredCount;
            this.orphanPaths = orphanPaths;
        }
    }

    /**
     * Backward-compatible 5-arg entry point: equivalent to the 6-arg
     * {@link #chooseSegmentsToMerge(List, int, long, long, int, long)}
     * with the micro-segment fast path disabled
     * ({@code microSegmentMaxNodes == 0}).
     *
     * <p>Package-private for unit tests.
     */
    static List<VectorSegment> chooseSegmentsToMerge(
            List<VectorSegment> candidates,
            int minCount,
            long minTotalBytes,
            long maxTotalBytes,
            int maxCount) {
        return chooseSegmentsToMerge(candidates, minCount, minTotalBytes,
                maxTotalBytes, maxCount, /*microSegmentMaxNodes*/ 0L);
    }

    /**
     * Picks the subset of {@code candidates} to merge in this compaction
     * run, applying:
     * <ul>
     *   <li>a minimum-count threshold ({@code minCount});</li>
     *   <li>a minimum total-size threshold ({@code minTotalBytes});</li>
     *   <li>a hard per-run byte cap ({@code maxTotalBytes}) to bound
     *       temporary write amplification;</li>
     *   <li>a count-based secondary trigger ({@code maxCount}): if the
     *       picked set reaches {@code maxCount} segments the byte
     *       threshold is waived and compaction fires regardless — this
     *       prevents unbounded segment accumulation when many small shards
     *       are produced during tailing catch-up (issue #285);</li>
     *   <li>smallest-first ordering to maximise the contraction ratio
     *       and avoid rewriting already-large segments.</li>
     * </ul>
     *
     * <p><b>Micro-segment fast path (issue #570).</b> When this method has
     * already decided that the cycle <em>will</em> compact (one of the
     * triggers above fired) and {@code microSegmentMaxNodes > 0}, the picked
     * set is re-prioritised: if at least two candidates are micro-segments
     * — segments whose live-node count is strictly below
     * {@code microSegmentMaxNodes} — the cycle merges <em>only</em> the
     * micro-segments instead of the byte-capped selection. Micro-segments
     * (typically a handful of nodes, produced by memory-pressure checkpoints
     * flushing a near-empty live shard) each consume a full slot in the
     * segment count yet contribute negligibly to search quality; merging
     * them is essentially free and reclaims slots immediately, relieving
     * segment-count back-pressure in seconds instead of waiting for a
     * multi-minute large merge. Larger segments are deferred to a subsequent
     * cycle (which, once no micro-segments remain, runs the normal policy).
     * The fast path never makes the cycle compact when it otherwise would
     * not — it only changes <em>which</em> segments are merged.
     *
     * <p>The micro-segment classification uses {@link VectorSegment#size()}
     * (the live-node count), not {@code estimatedSizeBytes}: a freshly
     * checkpointed micro-segment has a tiny node count, and a large segment
     * whose PKs have mostly been tombstoned is also cheap to re-merge since
     * the rebuild only re-inserts the authoritative live vectors. A segment
     * with {@code size() == 0} (every PK tombstoned) is therefore also a
     * micro-segment — preferring it for merge is intentional, as it reclaims
     * a full segment-count slot at near-zero rebuild cost.
     *
     * <p><b>Re-merge of the micro-segment output is expected and bounded.</b>
     * The merged output of a micro-segment cycle has a live-node count equal
     * to the sum of its inputs, which may itself still be below
     * {@code microSegmentMaxNodes}. While memory-pressure checkpoints keep
     * producing fresh micro-segments, each subsequent cycle re-merges that
     * growing output with the new arrivals. This is deliberate: every such
     * cycle still reduces the segment count (≥ 2 inputs collapse to 1) and
     * the work per cycle is bounded by {@code microSegmentMaxNodes} live
     * nodes — cheap by construction. Once the output crosses
     * {@code microSegmentMaxNodes} it graduates and is no longer re-merged.
     *
     * <p><b>Large segments are deliberately deprioritised while
     * micro-segments are present.</b> Under sustained micro-segment
     * production (the abnormal, memory-pressure condition this fast path
     * targets) the fast path may fire on every cycle, deferring large-segment
     * compaction. This is the intended trade-off: back-pressure relief —
     * keeping the segment count bounded so the tailer can make progress —
     * takes priority over the search-latency benefit of merging large
     * segments. When micro-segment production stops, no cycle has ≥ 2
     * micro-segments and the normal tiered policy resumes for large segments.
     *
     * <p>Returns an empty list when neither the byte threshold nor the
     * count trigger is satisfied.
     *
     * <p>Package-private for unit tests.
     *
     * @param microSegmentMaxNodes live-node count below which a segment is
     *     treated as a micro-segment; {@code 0} (or negative) disables the
     *     micro-segment fast path.
     */
    static List<VectorSegment> chooseSegmentsToMerge(
            List<VectorSegment> candidates,
            int minCount,
            long minTotalBytes,
            long maxTotalBytes,
            int maxCount,
            long microSegmentMaxNodes) {
        return chooseSegmentsToMerge(candidates, minCount, minTotalBytes,
                maxTotalBytes, maxCount, microSegmentMaxNodes, /*maxInputs*/ 0);
    }

    /**
     * Hard cap on the number of input segments per compaction (issue #587):
     * value below which a positive {@code maxInputs} is clamped. A single-input
     * "merge" is degenerate, so any enabled cap is forced to at least 2.
     *
     * <p>Package-private for unit tests.
     */
    static final int MIN_MAX_INPUTS = 2;

    /**
     * Normalises a configured {@code maxInputs} value: {@code <= 0} disables the
     * cap (returns {@code 0}); {@code 1} is clamped up to {@link #MIN_MAX_INPUTS}
     * because a one-segment merge does no useful work; any larger value is
     * returned unchanged.
     *
     * <p>Package-private for unit tests.
     */
    static int clampMaxInputs(int maxInputs) {
        if (maxInputs <= 0) {
            return 0;
        }
        return Math.max(MIN_MAX_INPUTS, maxInputs);
    }

    /**
     * Truncates {@code picked} to at most {@code maxInputs} segments when a
     * positive cap is configured (issue #587). The list is already sorted
     * smallest-bytes-first, so truncation keeps the smallest segments — which
     * maximises the contraction ratio and bounds the per-cycle remote I/O of
     * the downstream PQ-retraining step (whose cost scales with the number of
     * input segments). A no-op when the cap is disabled or not exceeded.
     *
     * <p>Package-private for unit tests.
     */
    static List<VectorSegment> capInputs(List<VectorSegment> picked, int maxInputs) {
        if (maxInputs > 0 && picked.size() > maxInputs) {
            LOGGER.log(Level.INFO,
                    "compaction: capping merge inputs from {0} to {1} segments "
                            + "(vector.index.compaction.maxInputs); remaining segments "
                            + "compact in subsequent cycles",
                    new Object[]{picked.size(), maxInputs});
            return new ArrayList<>(picked.subList(0, maxInputs));
        }
        return picked;
    }

    /**
     * Full entry point adding a hard cap on the number of input segments per
     * compaction cycle (issue #587). See the 6-arg overload for the trigger and
     * micro-segment semantics — they are unchanged. The cap is applied
     * <em>after</em> the fire/no-fire decision, so it never changes whether a
     * cycle compacts, only how many segments a single cycle merges; the
     * leftover segments are picked up by subsequent cycles.
     *
     * <p><b>The cap does NOT apply to the micro-segment fast path (#570).</b>
     * The fast path exists to reclaim segment-count slots <em>quickly</em>
     * under memory-pressure checkpoint storms; micro-segment merges are cheap
     * by construction (bounded by {@code microSegmentMaxNodes} live nodes), so
     * the PQ-retraining-I/O concern that motivates the cap does not apply to
     * them, and capping the fast path would directly throttle the back-pressure
     * relief valve. The cap therefore truncates only the normal byte-capped
     * selection.
     *
     * <p><b>Starvation safety.</b> The caller
     * ({@link PersistentVectorStore#runCompactionCycle}) tier-scales
     * {@code maxInputs} via {@link #computeTieredMaxInputs} exactly as it scales
     * {@code maxTotalBytes} and {@code maxCount} (issue #354), so the per-cycle
     * drain rate rises with the backlog. A flat cap therefore cannot starve the
     * tailer: by the time the segment count approaches the back-pressure
     * threshold the effective cap has scaled up with it.
     *
     * <p>Package-private for unit tests.
     *
     * @param maxInputs maximum number of input segments a single compaction
     *     cycle may merge; {@code 0} (or negative) disables the cap. Values are
     *     expected to be pre-normalised via {@link #clampMaxInputs} and, where
     *     tiered scaling is enabled, already tier-scaled by the caller.
     */
    static List<VectorSegment> chooseSegmentsToMerge(
            List<VectorSegment> candidates,
            int minCount,
            long minTotalBytes,
            long maxTotalBytes,
            int maxCount,
            long microSegmentMaxNodes,
            int maxInputs) {
        if (candidates == null || candidates.size() < minCount) {
            return new ArrayList<>();
        }

        List<VectorSegment> sorted = new ArrayList<>(candidates);
        sorted.sort(Comparator.comparingLong(s -> s.estimatedSizeBytes));

        List<VectorSegment> picked = new ArrayList<>();
        long total = 0L;
        for (VectorSegment seg : sorted) {
            if (total + seg.estimatedSizeBytes > maxTotalBytes) {
                break;
            }
            picked.add(seg);
            total += seg.estimatedSizeBytes;
        }

        // Decide whether this cycle compacts at all:
        //  - the standard byte-threshold trigger, or
        //  - the count-based trigger (issue #285): fire even if the byte
        //    threshold is not met when too many segments have accumulated.
        //    This guards against the scenario where every segment is
        //    individually small (e.g. catch-up with tiny shards) and the
        //    sum never reaches minTotalBytes despite hundreds of segments
        //    building up.
        boolean byteTrigger = picked.size() >= minCount && total >= minTotalBytes;
        boolean countTrigger = picked.size() >= maxCount;
        if (!byteTrigger && !countTrigger) {
            return new ArrayList<>();
        }

        // Micro-segment fast path (issue #570): the cycle is going to compact
        // anyway — if at least two candidates are micro-segments, merge ONLY
        // those instead of the byte-capped `picked` set above. This is a far
        // cheaper cycle that reclaims segment-count slots quickly; larger
        // segments are deferred to a later cycle. The micro scan walks the
        // full `sorted` candidate list (not `picked`), so `microPicked` may
        // be disjoint from `picked` — that is intentional: `picked` only
        // decided whether the cycle fires, not what it merges. `sorted` is
        // already smallest-bytes-first, so the micro-segment sub-list
        // inherits that ordering. The same maxTotalBytes write-amplification
        // cap is applied (it never bites for genuine micro-segments, but
        // keeps the bound honest for low / mostly-tombstoned large segments).
        if (microSegmentMaxNodes > 0L) {
            List<VectorSegment> microPicked = new ArrayList<>();
            long microTotal = 0L;
            for (VectorSegment seg : sorted) {
                if (seg.size() >= microSegmentMaxNodes) {
                    continue;
                }
                if (!microPicked.isEmpty()
                        && microTotal + seg.estimatedSizeBytes > maxTotalBytes) {
                    break;
                }
                microPicked.add(seg);
                microTotal += seg.estimatedSizeBytes;
            }
            if (microPicked.size() >= 2) {
                // Deliberately NOT capped — see method javadoc: the
                // micro-segment fast path must stay a fast slot-reclaiming
                // cycle (issue #570).
                return microPicked;
            }
        }
        return capInputs(picked, maxInputs);
    }

    // -------------------------------------------------------------------------
    // Tiered compaction scaling (issue #354)
    // -------------------------------------------------------------------------

    /**
     * Segment-count thresholds that trigger higher compaction fan-in.
     * When {@code totalSegmentCount >= TIERED_THRESHOLDS[i]}, the base
     * {@code maxBytes} and {@code maxCount} are multiplied by
     * {@code TIERED_MULTIPLIERS[i]}.  Thresholds are evaluated
     * highest-first so the last matching entry wins.
     *
     * <p>Package-private for unit tests.
     */
    static final int[] TIERED_THRESHOLDS = {500, 300, 100};
    static final int[] TIERED_MULTIPLIERS = {8, 4, 2};

    /**
     * Returns the scaling multiplier for the given total on-disk segment
     * count according to the tiered thresholds.  Returns 1 when the count
     * falls below all thresholds (no scaling).
     *
     * <p>Package-private for unit tests.
     */
    static int tieredMultiplier(int totalSegmentCount) {
        for (int i = 0; i < TIERED_THRESHOLDS.length; i++) {
            if (totalSegmentCount >= TIERED_THRESHOLDS[i]) {
                return TIERED_MULTIPLIERS[i];
            }
        }
        return 1;
    }

    /**
     * Returns the effective per-cycle byte cap after applying the tiered
     * scaling factor for {@code totalSegmentCount}.  The result is capped
     * at {@link Long#MAX_VALUE} to avoid overflow.
     *
     * <p>Package-private for unit tests.
     */
    static long computeTieredMaxBytes(int totalSegmentCount, long baseMaxBytes) {
        int multiplier = tieredMultiplier(totalSegmentCount);
        if (multiplier == 1) {
            return baseMaxBytes;
        }
        // Guard against overflow: if baseMaxBytes * multiplier would exceed MAX_VALUE,
        // return MAX_VALUE so the byte cap becomes effectively unlimited.
        if (baseMaxBytes > Long.MAX_VALUE / multiplier) {
            return Long.MAX_VALUE;
        }
        return baseMaxBytes * multiplier;
    }

    /**
     * Returns the effective per-cycle segment count cap after applying the
     * tiered scaling factor for {@code totalSegmentCount}.  The result is
     * capped at {@link Integer#MAX_VALUE} to avoid overflow.
     *
     * <p>Package-private for unit tests.
     */
    static int computeTieredMaxCount(int totalSegmentCount, int baseMaxCount) {
        int multiplier = tieredMultiplier(totalSegmentCount);
        if (multiplier == 1) {
            return baseMaxCount;
        }
        // Guard against overflow.
        if (baseMaxCount > Integer.MAX_VALUE / multiplier) {
            return Integer.MAX_VALUE;
        }
        return baseMaxCount * multiplier;
    }

    /**
     * Returns the effective per-cycle input-segment cap (issue #587) after
     * applying the tiered scaling factor for {@code totalSegmentCount}.
     *
     * <p>Tier-scaling the cap is what makes it starvation-safe: the per-cycle
     * drain rate rises with the backlog (2×/4×/8× at 100/300/500 segments),
     * mirroring {@link #computeTieredMaxCount}, so the cap can never let the
     * segment count grow unboundedly toward the back-pressure threshold while
     * still bounding worst-case per-cycle PQ-retraining I/O at each tier.
     *
     * <p>A disabled cap ({@code baseMaxInputs <= 0}) is returned unchanged so
     * it stays disabled. The result is capped at {@link Integer#MAX_VALUE} to
     * avoid overflow.
     *
     * <p>Package-private for unit tests.
     */
    static int computeTieredMaxInputs(int totalSegmentCount, int baseMaxInputs) {
        if (baseMaxInputs <= 0) {
            return baseMaxInputs;
        }
        int multiplier = tieredMultiplier(totalSegmentCount);
        if (multiplier == 1) {
            return baseMaxInputs;
        }
        // Guard against overflow.
        if (baseMaxInputs > Integer.MAX_VALUE / multiplier) {
            return Integer.MAX_VALUE;
        }
        return baseMaxInputs * multiplier;
    }

    /**
     * Populates the supplied {@link CompactionAuthorityMap} with the per-PK
     * authority decisions used by the live-PK filter during a compaction
     * cycle.
     *
     * <p>The authority map is intentionally <em>candidate-only</em>: it stores
     * one entry per primary key that appears in any of the {@code candidates},
     * recording the highest-generation source of that PK across:
     *
     * <ul>
     *   <li>the candidates themselves (initial seed),</li>
     *   <li>any non-candidate segment with strictly greater generation than
     *       the max candidate generation that has the PK in its
     *       {@code onDiskPkToNode} BLink,</li>
     *   <li>the live in-memory shards, which always dominate (recorded with
     *       {@link #LIVE_SHARD_SEGMENT_ID}).</li>
     * </ul>
     *
     * <p>PKs that exist in newer segments or live shards but are <strong>not</strong>
     * present in any candidate are deliberately omitted — they are irrelevant
     * to the rebuild ({@link #populateSyntheticShard} only consults the map
     * for candidate PKs). This is the key memory optimisation behind issue #290:
     * the authority map is bounded by the candidate PK count (≈ tens of
     * thousands per cycle) instead of the total PK count across all segments
     * (which can reach hundreds of millions). Combined with the BLink-backed
     * paged storage of {@code CompactionAuthorityMap}, total compaction
     * heap pressure stops growing with the size of the index.
     *
     * <p>Older non-candidate segments are not consulted: candidates cover their
     * own generation by construction and the merged output replaces them.
     *
     * <p>During the rebuild the caller re-inserts a (PK, vector) pair iff
     * {@link CompactionAuthorityMap#getSegmentId} returns the candidate's own
     * segment id. Tombstoned or superseded vectors are silently dropped,
     * reclaiming their storage.
     */
    static void buildAuthorityMap(
            CompactionAuthorityMap authority,
            List<VectorSegment> candidates,
            List<VectorSegment> allSegments,
            Iterable<Bytes> liveShardPks) {

        // Step 1: seed the authority map with candidate PKs. This is the only
        // bulk insertion we perform; its size is bounded by sum(candidate.size).
        for (VectorSegment seg : candidates) {
            insertSegmentPks(seg, authority);
        }

        // Step 2: walk newer non-candidate segments and update the authority
        // entries for any PK that ALSO appears in a candidate. We avoid
        // materialising newer-segment PKs into the BLink: each candidate PK
        // is looked up against each newer segment's onDiskPkToNode BLink (the
        // same paged structure already used by deletePk), and only on a hit
        // do we update the authority map. This is O(|candidate PKs| × |newer
        // segments|) BLink searches in the worst case, which is far cheaper
        // than the previous O(|all PKs|) HashMap inserts and — crucially —
        // does not materialise any per-PK Bytes object outside the inner
        // BLink lookups.
        long maxCandidateGeneration = 0L;
        Set<Integer> candidateIds = new HashSet<>(candidates.size() * 2);
        for (VectorSegment seg : candidates) {
            if (seg.generation > maxCandidateGeneration) {
                maxCandidateGeneration = seg.generation;
            }
            candidateIds.add(seg.segmentId);
        }
        List<VectorSegment> newerSegments = new ArrayList<>();
        for (VectorSegment seg : allSegments) {
            if (seg.generation > maxCandidateGeneration && !candidateIds.contains(seg.segmentId)) {
                newerSegments.add(seg);
            }
        }
        // Sort newer segments by generation descending: the first hit during
        // the lookup loop is the authoritative one, and we can short-circuit.
        newerSegments.sort(Comparator.comparingLong((VectorSegment s) -> s.generation).reversed());

        if (!newerSegments.isEmpty()) {
            for (VectorSegment cand : candidates) {
                checkSupersessionForCandidate(cand, newerSegments, authority);
            }
        }

        // Step 3: live shards always dominate any PK they hold. Walk the
        // live-shard PK iterator once and update only entries that are
        // already in the authority map.
        if (liveShardPks != null) {
            for (Bytes pk : liveShardPks) {
                if (authority.getSegmentId(pk) != null) {
                    authority.updateIfHigherGeneration(pk,
                            CompactionAuthorityMap.LIVE_SHARD_GENERATION_MARKER,
                            LIVE_SHARD_SEGMENT_ID);
                }
            }
        }
    }

    /**
     * Synthetic segment id returned by {@link CompactionAuthorityMap#getSegmentId}
     * when the authoritative source for a PK is a live in-memory shard.
     * Any real {@link VectorSegment} id is non-negative; this sentinel
     * is {@code -1} so callers can check with {@code ownerId < 0}.
     */
    static final int LIVE_SHARD_SEGMENT_ID = -1;

    private static void insertSegmentPks(VectorSegment seg, CompactionAuthorityMap authority) {
        int[] offsets = seg.pkOffsets;
        int[] lengths = seg.pkLengths;
        byte[] data = seg.pkData;
        if (offsets == null || data == null || lengths == null) {
            return;
        }
        long gen = seg.generation;
        int segmentId = seg.segmentId;
        for (int ord = 0; ord < offsets.length; ord++) {
            int off = offsets[ord];
            if (off < 0) {
                continue; // tombstoned — not a candidate for re-insert.
            }
            Bytes pk = Bytes.from_array(data, off, lengths[ord]);
            authority.updateIfHigherGeneration(pk, gen, segmentId);
        }
    }

    /**
     * For each authoritative candidate PK in {@code cand}, looks the PK up
     * in every newer segment's {@code onDiskPkToNode} BLink and, on the first
     * hit (newer segments are sorted highest-generation-first), updates the
     * authority map to record that newer segment as the winner.
     *
     * <p>This is the BLink-driven supersession check that replaces the
     * previous bulk PK iteration over non-candidate segments.
     */
    private static void checkSupersessionForCandidate(VectorSegment cand,
                                                      List<VectorSegment> newerSegmentsDesc,
                                                      CompactionAuthorityMap authority) {
        int[] offsets = cand.pkOffsets;
        int[] lengths = cand.pkLengths;
        byte[] data = cand.pkData;
        if (offsets == null || data == null || lengths == null) {
            return;
        }
        for (int ord = 0; ord < offsets.length; ord++) {
            int off = offsets[ord];
            if (off < 0) {
                continue;
            }
            Bytes pk = Bytes.from_array(data, off, lengths[ord]);
            for (VectorSegment newerSeg : newerSegmentsDesc) {
                herddb.index.blink.BLink<Bytes, Long> p2n = newerSeg.onDiskPkToNode;
                if (p2n == null) {
                    continue;
                }
                if (p2n.search(pk) != null) {
                    // Found the authoritative newer segment — update and
                    // short-circuit since later segments have lower generation.
                    authority.updateIfHigherGeneration(pk, newerSeg.generation, newerSeg.segmentId);
                    break;
                }
            }
        }
    }

    /**
     * Splits {@code pendingDeletes} into two lists:
     * <ul>
     *   <li>{@code reapable} — entries whose {@code deadlineMs} has
     *       passed AND whose {@code sinceGeneration <=
     *       minShadowAckedGeneration}. These files are safe to
     *       physically delete.</li>
     *   <li>{@code retained} — entries still held for retention.</li>
     * </ul>
     *
     * <p>When no shadow replicas are known, the caller passes
     * {@code Long.MAX_VALUE} so the shadow gate never holds deletion
     * back — retention then depends solely on {@code deadlineMs}.
     *
     * <p>Package-private for unit tests.
     */
    static Partition partitionReapable(
            List<PendingDelete> pendingDeletes,
            long nowMs,
            long minShadowAckedGeneration) {

        List<PendingDelete> reapable = new ArrayList<>();
        List<PendingDelete> retained = new ArrayList<>();
        if (pendingDeletes == null || pendingDeletes.isEmpty()) {
            return new Partition(reapable, retained);
        }
        for (PendingDelete pd : pendingDeletes) {
            boolean deadlineElapsed = nowMs >= pd.deadlineMs;
            boolean shadowSafe = pd.sinceGeneration <= minShadowAckedGeneration;
            if (deadlineElapsed && shadowSafe) {
                reapable.add(pd);
            } else {
                retained.add(pd);
            }
        }
        return new Partition(reapable, retained);
    }

    /** Result of {@link #partitionReapable}. */
    static final class Partition {
        final List<PendingDelete> reapable;
        final List<PendingDelete> retained;

        Partition(List<PendingDelete> reapable, List<PendingDelete> retained) {
            this.reapable = reapable;
            this.retained = retained;
        }
    }

    /**
     * Performs the heavy rebuild: reads vectors from every candidate
     * segment, keeps only those whose PK is still authoritative, builds
     * a fresh jvector graph, and writes a new FusedPQ segment via
     * {@link PersistentVectorStore#writeSyntheticShard}.
     *
     * <p>Returns {@code null} when every input vector has been
     * tombstoned or superseded — the caller should still swap the
     * inputs out (they are fully obsolete) but no new segment is
     * produced.
     *
     * @throws CompactionException on READ_IO / CORRUPTION.
     * @throws IOException on WRITE_IO paths bubbling out of the writer.
     * @throws DataStorageManagerException on metadata failures at the
     *     storage layer.
     */
    static RebuildResult rebuildSegment(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            CompactionAuthorityMap authority)
            throws CompactionException, IOException, DataStorageManagerException {

        int dim = store.compactionDimension();
        int totalCandidateVectors = 0;
        for (VectorSegment seg : candidates) {
            totalCandidateVectors += Math.max(0, seg.liveCount.get());
        }

        int keptCount = 0;
        int filteredCount = 0;
        // Per-source live bitsets. Built in lock-step with the count below so the
        // streaming path (issue #485) can hand them straight to OnDiskGraphIndexCompactor
        // without a second walk over each candidate's pkOffsets array.
        List<FixedBitSet> liveBitsets = new ArrayList<>(candidates.size());

        // First pass: count how many vectors we will actually keep, and build
        // per-source live bitsets covering candidate ordinals at the same time.
        for (VectorSegment seg : candidates) {
            int[] offsets = seg.pkOffsets;
            int[] lengths = seg.pkLengths;
            byte[] data = seg.pkData;
            // length == 0 is a valid empty bitset; length < 1 is rejected by FixedBitSet.
            // Use Math.max(1, ...) so an empty candidate still produces a placeholder
            // bitset (it will simply have cardinality 0 — never indexed by liveCount).
            int sourceSize = (offsets == null) ? 0 : offsets.length;
            FixedBitSet live = new FixedBitSet(Math.max(1, sourceSize));
            if (offsets == null || data == null || lengths == null) {
                liveBitsets.add(live);
                continue;
            }
            for (int ord = 0; ord < offsets.length; ord++) {
                int off = offsets[ord];
                if (off < 0) {
                    filteredCount++;
                    continue;
                }
                Bytes pk = Bytes.from_array(data, off, lengths[ord]);
                Integer owner = authority.getSegmentId(pk);
                if (owner == null || owner != seg.segmentId) {
                    filteredCount++;
                } else {
                    keptCount++;
                    live.set(ord);
                }
            }
            liveBitsets.add(live);
        }
        if (keptCount == 0) {
            LOGGER.log(Level.INFO, "compaction: all {0} candidate vectors are obsolete — skipping rebuild",
                    totalCandidateVectors);
            return null;
        }

        // Issue #485 — streaming compaction via jvector's OnDiskGraphIndexCompactor.
        // The on-disk-merge path bounds memory by O(taskWindowSize × maxDegree)
        // instead of O(numTotalNodes × dimension), lifting the in-memory cap that
        // previously forced vector.index.compaction.maxBytes ≤ 1 GB.
        if (streamingCompactionEnabled) {
            return rebuildSegmentStreaming(store, candidates, authority, dim,
                    keptCount, filteredCount, liveBitsets);
        }

        return rebuildSegmentLegacy(store, candidates, authority, dim,
                keptCount, filteredCount);
    }

    /**
     * Fills the synthetic shard by reading authoritative vectors from
     * every candidate segment's on-disk graph.
     */
    private static void populateSyntheticShard(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            CompactionAuthorityMap authority,
            PersistentVectorStore.LiveGraphShard syntheticShard,
            AtomicInteger localOrdCounter) throws CompactionException {

        for (VectorSegment seg : candidates) {
            OnDiskGraphIndex odg = seg.onDiskGraph;
            if (odg == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "candidate segment " + seg.segmentId + " has no on-disk graph");
            }
            int[] offsets = seg.pkOffsets;
            int[] lengths = seg.pkLengths;
            byte[] data = seg.pkData;
            if (offsets == null) {
                continue;
            }

            OnDiskGraphIndex.View view;
            try {
                view = (OnDiskGraphIndex.View) odg.getView();
            } catch (RuntimeException | Error e) {
                // Issue #621: include Error for the metrics-bucketing reason
                // documented at the download-stage catch in
                // rebuildSegmentStreaming. No orphans yet (pre-upload).
                throw new CompactionException(FailureReason.READ_IO,
                        "failed to open view on segment " + seg.segmentId, e);
            }
            try {
                for (int ord = 0; ord < offsets.length; ord++) {
                    int off = offsets[ord];
                    if (off < 0) {
                        continue;
                    }
                    Bytes pk = Bytes.from_array(data, off, lengths[ord]);
                    Integer owner = authority.getSegmentId(pk);
                    if (owner == null || owner != seg.segmentId) {
                        continue;
                    }
                    VectorFloat<?> vec;
                    try {
                        vec = view.getVector(ord);
                    } catch (RuntimeException | Error e) {
                        // Issue #621: see the matching arm at view = ... above.
                        throw new CompactionException(FailureReason.CORRUPTION,
                                "failed to read vector at ord " + ord + " of segment "
                                        + seg.segmentId, e);
                    }
                    if (vec == null) {
                        throw new CompactionException(FailureReason.CORRUPTION,
                                "null vector for authoritative PK in segment " + seg.segmentId);
                    }
                    int localOrd = localOrdCounter.getAndIncrement();
                    // Per-shard storage keyed by local ordinal (issue #256).
                    // No global nodeId arithmetic leaves this call site.
                    syntheticShard.vectorStorage.set(localOrd, vec);
                    syntheticShard.pkToNode.put(pk, localOrd);
                    syntheticShard.nodeToPk.put(localOrd, pk);
                    syntheticShard.vectorCount.incrementAndGet();
                    try {
                        syntheticShard.builder.addGraphNode(localOrd, vec);
                    } catch (RuntimeException | Error e) {
                        // Issue #621: see the matching arm at view = ... above.
                        throw new CompactionException(FailureReason.WRITE_IO,
                                "addGraphNode failed at localOrd " + localOrd, e);
                    }
                }
            } finally {
                try {
                    view.close();
                } catch (IOException | Error e) {
                    // Issue #621: include Error to match the close()-failure
                    // catches in rebuildSegmentStreaming / rebuildSegmentLegacy
                    // — must not let a close-time Error suppress an in-flight
                    // CompactionException carrying an orphan list.
                    LOGGER.log(Level.FINE, "ignoring view close in compaction", e);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Streaming compaction (issue #485) — driven by OnDiskGraphIndexCompactor.
    // -------------------------------------------------------------------------

    /**
     * Asserts the {@code INLINE_VECTORS} feature is present on a streaming
     * compaction candidate's feature map. Extracted so the guard can be
     * unit-tested in isolation (issue #485 review item B.3#2): mocking
     * {@link OnDiskGraphIndex} requires reflective access to its
     * package-private internals; lifting the check to a {@code Map}-keyed
     * helper sidesteps that.
     *
     * <p>Package-private for tests.
     *
     * @param segmentId numeric id of the candidate segment, included in the
     *                  error message for diagnosis
     * @param features  the feature map returned by
     *                  {@link OnDiskGraphIndex#getFeatures()}
     * @throws CompactionException with {@link FailureReason#CORRUPTION} when
     *                  the map does not contain {@code INLINE_VECTORS}
     */
    static void requireInlineVectorsFeature(int segmentId,
            java.util.Map<FeatureId, ?> features) throws CompactionException {
        if (features == null || !features.containsKey(FeatureId.INLINE_VECTORS)) {
            throw new CompactionException(FailureReason.CORRUPTION,
                    "candidate segment " + segmentId
                            + " missing INLINE_VECTORS feature (required by"
                            + " OnDiskGraphIndexCompactor)");
        }
    }

    /**
     * Returns {@code true} when every candidate's on-disk graph has the same
     * {@code FeatureId} keyset as the first. {@code false} indicates heterogeneous
     * inputs that would cause {@code OnDiskGraphIndexCompactor.validateFeatures}
     * to throw (issue #543).
     *
     * <p>Precondition: all candidates have a non-null {@code onDiskGraph}; this
     * is enforced by the null-check loop that always runs before this helper.
     */
    static boolean allCandidatesHaveUniformFeatures(List<VectorSegment> candidates) {
        if (candidates.size() <= 1) {
            return true;
        }
        Set<FeatureId> ref = candidates.get(0).onDiskGraph.getFeatureSet();
        for (int i = 1; i < candidates.size(); i++) {
            if (!candidates.get(i).onDiskGraph.getFeatureSet().equals(ref)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Drives a streaming N:1 compaction via
     * {@link OnDiskGraphIndexCompactor}. Reads each candidate's existing
     * {@link OnDiskGraphIndex}, writes a merged graph with dense output
     * ordinals {@code 0..keptCount-1}, builds the matching map file, and
     * hands off to {@link PersistentVectorStore#writeStreamingCompactedSegment}
     * for upload + bookkeeping. Memory cost is bounded by
     * {@code taskWindowSize × maxDegree × float[dimension]} — independent of
     * the merged graph size — so the per-cycle byte cap is no longer
     * dictated by heap pressure.
     *
     * <p>Every candidate segment is required to carry the
     * {@link FeatureId#INLINE_VECTORS} feature (needed by the compactor to
     * read source vectors during graph rewrite). HerdDB always writes this
     * feature, so a missing entry is surfaced as
     * {@link FailureReason#CORRUPTION}.
     */
    private static RebuildResult rebuildSegmentStreaming(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            CompactionAuthorityMap authority,
            int dim,
            int keptCount,
            int filteredCount,
            List<FixedBitSet> liveBitsets)
            throws CompactionException, IOException, DataStorageManagerException {

        // Surface missing graph references / missing INLINE_VECTORS as CORRUPTION
        // so the outer cycle records the failure cleanly instead of letting an
        // unchecked exception escape the jvector boundary. Guards are split out
        // into package-private helpers so they're testable in isolation
        // (issue #485 review item B.3#2).
        for (VectorSegment seg : candidates) {
            OnDiskGraphIndex odg = seg.onDiskGraph;
            if (odg == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "candidate segment " + seg.segmentId
                                + " has no on-disk graph (streaming compaction)");
            }
            requireInlineVectorsFeature(seg.segmentId, odg.getFeatures());
        }

        // Issue #543: OnDiskGraphIndexCompactor.validateFeatures() requires all
        // source segments to have the same FeatureId keyset. If the candidates
        // were written by different code paths (e.g. some above and some below
        // MIN_VECTORS_FOR_FUSED_PQ) their feature sets can differ. Detect this
        // before handing control to the compactor and fall back to the legacy
        // rebuild path (which reconstructs the graph from raw vectors and does
        // not require feature-set uniformity).
        if (!allCandidatesHaveUniformFeatures(candidates)) {
            LOGGER.log(Level.WARNING,
                    "VectorIndexCompactor (streaming): detected heterogeneous feature"
                            + " sets across {0} candidates — falling back to legacy rebuild",
                    candidates.size());
            return rebuildSegmentLegacy(store, candidates, authority, dim,
                    keptCount, filteredCount);
        }

        // -----------------------------------------------------------------------
        // Step 1: Pre-download / eagerly open all source graph files (issue #602).
        // Bypasses SegmentBlockCache / gRPC for the hot merge path — every read
        // during OnDiskGraphIndexCompactor.compact() and writeStreamingCompactedMapFile
        // comes from a local temp file (direct-download path) or a sequential
        // reader copy, not from per-node block-cache round-trips.
        // -----------------------------------------------------------------------
        DataStorageManager dsm = store.dataStorageManager();
        String tsUUID = store.tableSpaceUUID();
        Path downloadDir = store.tmpDirectory();
        boolean supportsDirectDownload = dsm.supportsDirectMultipartDownload();

        // These lists are parallel: index i corresponds to candidates.get(i).
        List<Path> downloadedTempFiles = new ArrayList<>(candidates.size());
        List<ReaderSupplier> openedReaderSuppliers = new ArrayList<>(candidates.size());
        List<OnDiskGraphIndex> localSources = new ArrayList<>(candidates.size());

        try {
            for (VectorSegment seg : candidates) {
                String segUuid = store.segmentStorageKey(seg);
                long graphFileSize = seg.graphFileSize;
                if (graphFileSize <= 0) {
                    throw new CompactionException(FailureReason.CORRUPTION,
                            "streaming compaction: segment " + seg.segmentId
                                    + " has graphFileSize=" + graphFileSize
                                    + " — graph not yet persisted or storage key is wrong");
                }

                COMPACTION_EAGER_DOWNLOAD_INFLIGHT.incrementAndGet();
                long downloadStart = System.nanoTime();
                try {
                    Path tmpFile = Files.createTempFile(
                            downloadDir, "herddb-compact-src-", ".idx");
                    downloadedTempFiles.add(tmpFile);

                    if (supportsDirectDownload) {
                        // Fast path: stream directly from object storage (S3/GCS/MinIO).
                        dsm.downloadMultipartIndexFile(tsUUID, segUuid, "graph",
                                graphFileSize, tmpFile);
                    } else {
                        // Fallback: copy via multipart reader (file-server or in-memory
                        // for MemoryDataStorageManager) to a local temp file so that
                        // all subsequent reads during compact() and writeMap are local.
                        ReaderSupplier src = dsm.multipartIndexReaderSupplier(
                                tsUUID, segUuid, "graph", graphFileSize);
                        try (RandomAccessReader reader = src.get();
                             FileOutputStream fos = new FileOutputStream(tmpFile.toFile());
                             BufferedOutputStream bos = new BufferedOutputStream(fos,
                                     4 * 1024 * 1024)) {
                            reader.seek(0L);
                            byte[] buf = new byte[4 * 1024 * 1024];
                            long remaining = graphFileSize;
                            while (remaining > 0L) {
                                int toRead = (int) Math.min(buf.length, remaining);
                                byte[] chunk = (toRead == buf.length) ? buf : new byte[toRead];
                                reader.readFully(chunk);
                                bos.write(chunk, 0, toRead);
                                remaining -= toRead;
                            }
                        }
                    }

                    long elapsedMs = (System.nanoTime() - downloadStart) / 1_000_000L;
                    double throughputMBps = elapsedMs > 0
                            ? (graphFileSize / (1024.0 * 1024.0)) / (elapsedMs / 1000.0)
                            : 0.0;
                    LOGGER.log(Level.INFO,
                            "streaming compaction: prepared source segment graph {0}: "
                                    + "{1} bytes in {2} ms ({3,number,#.#} MB/s)",
                            new Object[]{segUuid, graphFileSize, elapsedMs, throughputMBps});
                    COMPACTION_EAGER_DOWNLOAD_COUNT.incrementAndGet();
                    COMPACTION_EAGER_DOWNLOAD_BYTES.addAndGet(graphFileSize);

                    ReaderSupplier rs = ReaderSupplierFactory.open(tmpFile);
                    openedReaderSuppliers.add(rs);
                    localSources.add(OnDiskGraphIndex.load(rs));
                } catch (DataStorageManagerException e) {
                    throw new CompactionException(FailureReason.METADATA_IO,
                            "streaming compaction: failed to prepare source segment graph for "
                                    + segUuid, e);
                } catch (IOException e) {
                    throw new CompactionException(FailureReason.READ_IO,
                            "streaming compaction: failed to prepare source segment graph for "
                                    + segUuid, e);
                } catch (RuntimeException | Error e) {
                    // jvector boundary: OnDiskGraphIndex.load() can throw unchecked exceptions
                    // (e.g. IllegalArgumentException on bad magic bytes, BufferUnderflowException on
                    // truncated data) when the downloaded graph file is corrupt. Wrap so the outer
                    // cycle's catch (CompactionException) arm records the failure cleanly.
                    // Mirrors RemoteSegmentGraphMerger's handling of the same jvector contract.
                    //
                    // Issue #621: include Error so a Netty OutOfDirectMemoryError
                    // from the download path (or any other Error from the jvector
                    // loader) is bucketed as CORRUPTION instead of escaping
                    // uncaught and being attributed by the daemon safety net to
                    // "unexpected compaction failure" with no per-reason metric.
                    // No orphans yet (pre-upload), so this is metrics-bucketing
                    // only — but the bucketing matters for operators triaging
                    // failure patterns from the IS logs.
                    throw new CompactionException(FailureReason.CORRUPTION,
                            "streaming compaction: corrupt or truncated source graph for "
                                    + segUuid, e);
                } finally {
                    COMPACTION_EAGER_DOWNLOAD_INFLIGHT.decrementAndGet();
                }
            }

            // -----------------------------------------------------------------------
            // Step 2: Build per-source dense ordinal mappers using the local graphs.
            // -----------------------------------------------------------------------
            // Build per-source dense ordinal mappers so the merged segment occupies
            // exactly keptCount records on disk (no holes for tombstoned/superseded
            // ordinals). With OffsetMapper the inline level-0 area would be sized
            // for sum(srcSize) ordinals, wasting up to one record per dead slot.
            List<OrdinalMapper> mappers = new ArrayList<>(candidates.size());
            int globalBase = 0;
            for (int s = 0; s < candidates.size(); s++) {
                VectorSegment seg = candidates.get(s);
                FixedBitSet live = liveBitsets.get(s);
                OnDiskGraphIndex odg = localSources.get(s);
                int sourceSize = odg.size(0);
                // Defensive: align bitset length with the source's level-0 size, since
                // OnDiskGraphIndexCompactor's validateLiveNodesBounds rejects any mismatch.
                FixedBitSet aligned;
                if (live.length() == sourceSize) {
                    aligned = live;
                } else {
                    aligned = new FixedBitSet(Math.max(1, sourceSize));
                    int last = Math.min(live.length(), sourceSize);
                    for (int ord = 0; ord < last; ord++) {
                        if (live.get(ord)) {
                            aligned.set(ord);
                        }
                    }
                    liveBitsets.set(s, aligned);
                }
                DenseLiveOrdinalMapper mapper =
                        new DenseLiveOrdinalMapper(aligned, sourceSize, globalBase);
                mappers.add(mapper);
                globalBase += mapper.liveCount();
            }
            if (globalBase != keptCount) {
                // Must hold by construction; surface as CORRUPTION rather than asserting
                // so a hypothetical bitset-vs-counter mismatch fails the cycle, not the JVM.
                throw new CompactionException(FailureReason.CORRUPTION,
                        "streaming compaction bitset/keptCount mismatch: keptCount="
                                + keptCount + " bitsetTotal=" + globalBase);
            }

            // OnDiskGraphIndexCompactor requires at least two sources.  When a single
            // candidate slips through (e.g., the maxCount trigger fired with a single
            // residual segment) fall back to the legacy in-memory path: building an
            // OnDiskGraphIndexCompactor over one source is rejected at construction.
            if (localSources.size() < 2) {
                STREAMING_FALLBACK_TO_LEGACY_TOTAL.incrementAndGet();
                LOGGER.log(Level.INFO,
                        "streaming compaction: only {0} candidate(s); falling back to legacy"
                                + " in-memory rebuild (fallbackTotal={1})",
                        new Object[]{localSources.size(), STREAMING_FALLBACK_TO_LEGACY_TOTAL.get()});
                return rebuildSegmentLegacy(store, candidates, authority, dim,
                        keptCount, filteredCount);
            }

            // PQ reader factory: open a FRESH ReaderSupplier from the local temp file for
            // each PQ-retraining request. We must NOT return the same ReaderSupplier that
            // was used to load the OnDiskGraphIndex: PQRetrainer closes the supplier it
            // receives (after extracting vectors), which would close the underlying mmap
            // Arena and make subsequent graph-node reads in compact() throw
            // "Already closed". A fresh ReaderSupplier has its own independent Arena, so
            // PQRetrainer can close it freely without disturbing the main compaction readers.
            IdentityHashMap<OnDiskGraphIndex, Path> odgToPath = new IdentityHashMap<>();
            for (int i = 0; i < localSources.size(); i++) {
                odgToPath.put(localSources.get(i), downloadedTempFiles.get(i));
            }
            Function<OnDiskGraphIndex, ReaderSupplier> pqReaderFactory = odg -> {
                Path path = odgToPath.get(odg);
                if (path == null) {
                    throw new IllegalStateException(
                            "streaming compaction PQ factory: OnDiskGraphIndex not found"
                                    + " in local-source set");
                }
                try {
                    return ReaderSupplierFactory.open(path);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(
                            "streaming compaction PQ factory: failed to open " + path, e);
                }
            };

            Path graphTemp = Files.createTempFile(
                    store.tmpDirectory(), "herddb-vector-compact-graph-", ".idx");
            Path mapTemp = Files.createTempFile(
                    store.tmpDirectory(), "herddb-vector-compact-map-", ".tmp");
            boolean success = false;
            VectorSegment mergedSegment = null;
            List<String[]> orphans = new ArrayList<>();
            int segmentId = store.newSegmentId();
            // Also reserve a contiguous nodeId range for parity with the legacy path.
            // Issue #255 (live-shard rotation): the rebuild may run alongside a live
            // shard append; allocating the range here forces a rotation so the
            // merged segment's nodeIds never collide with subsequently-allocated
            // live-shard ones. The streaming path doesn't use the actual reserved
            // value (all output ordinals are dense 0..keptCount-1), but the rotation
            // side-effect still matters.
            long startNodeId = store.allocateCompactionNodeIds(keptCount);
            try {
                // OnDiskGraphIndexCompactor is not AutoCloseable: it self-shuts-down
                // a fork-join pool inside compact() iff it owns one. We hand in
                // PhysicalCoreExecutor.pool() so the compactor never owns its executor.
                // Pass pqReaderFactory so PQRetrainer reads from local files (issue #602).
                // Constructor and compact() are in the same try/catch: the jvector
                // constructor also runs validateFeatures/validateLiveNodesBounds and can
                // throw RuntimeException on mismatched inputs (issue #602 round-3 review).
                try {
                    OnDiskGraphIndexCompactor compactor = new OnDiskGraphIndexCompactor(
                            localSources, liveBitsets, mappers, store.compactionSimilarity(),
                            PhysicalCoreExecutor.pool(),
                            pqReaderFactory);
                    compactor.compact(graphTemp);
                } catch (java.io.FileNotFoundException | RuntimeException | Error e) {
                    // jvector boundary — wrap unchecked OR FileNotFoundException
                    // (declared on compact()) failures so they never reach the
                    // outer cycle's orphan-blind catch (IOException) arm. Covers both
                    // constructor-side (validateFeatures / validateLiveNodesBounds) and
                    // compact()-side failures. No orphans yet: this fires before any upload.
                    //
                    // Issue #621: include Error so a Netty OutOfDirectMemoryError
                    // (or any other Error sub-class) thrown from jvector's compact()
                    // is wrapped as a CompactionException and reaches runCompactionCycle's
                    // catch arm, instead of escaping to vectorIndexCompactionLoop and
                    // killing the daemon. We rethrow as a wrapped exception (not as the
                    // original Throwable) so the outer cycle's failure-reason metrics
                    // are recorded correctly; the original cause is preserved as the
                    // CompactionException's cause for diagnosis.
                    throw new CompactionException(FailureReason.WRITE_IO,
                            "OnDiskGraphIndexCompactor constructor/compact failed for segment "
                                    + segmentId, e);
                }

                try {
                    writeStreamingCompactedMapFile(candidates, liveBitsets, mappers,
                            localSources, mapTemp, dim, keptCount);
                } catch (RuntimeException | Error e) {
                    // Issue #621: see the matching catch on OnDiskGraphIndexCompactor
                    // above. No orphans yet — fires before any multipart upload.
                    throw new CompactionException(FailureReason.WRITE_IO,
                            "streaming compaction map-file build failed for segment "
                                    + segmentId, e);
                }

            PersistentVectorStore.SegmentWriteResult swr;
            try {
                swr = store.writeStreamingCompactedSegment(
                        graphTemp, mapTemp, segmentId, keptCount);
            } catch (IOException | DataStorageManagerException | Error e) {
                // Either upload (graph or map) may have completed before the
                // failure surfaced — track both as orphans regardless and let
                // the outer cycle queue retention-aware deletes via
                // pendingDeletes (issue #485 review item B.1#1).
                //
                // Issue #621: include Error so a Netty OutOfDirectMemoryError mid
                // multipart-write produces the orphan-list wrap that
                // runCompactionCycle's catch (CompactionException) arm drains into
                // pendingDeletes. Without this branch, an Error skips the wrap, the
                // outer cycle never sees the orphan list, and partial multipart
                // blocks leak into remote storage — the symmetric compaction-side
                // bug that #616 / #618 fixed on the checkpoint side.
                List<String[]> failureOrphans = new ArrayList<>(2);
                failureOrphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "graph"});
                failureOrphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "map"});
                FailureReason reason = (e instanceof DataStorageManagerException)
                        ? FailureReason.METADATA_IO : FailureReason.WRITE_IO;
                throw new CompactionException(reason,
                        "writeStreamingCompactedSegment failed for segment "
                                + segmentId, e, failureOrphans);
            }
            if (swr == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "writeStreamingCompactedSegment returned null for non-empty"
                                + " streaming compaction shard (keptCount=" + keptCount + ")");
            }

            // Stamp the feature set produced by the streaming compaction onto swr
            // so it can be carried into NewSegmentInfo and eventually SegmentMetadata.
            // The allCandidatesHaveUniformFeatures guard above ensures all candidates
            // share the same feature set (heterogeneous inputs were already redirected
            // to rebuildSegmentLegacy); the first candidate is therefore representative.
            if (!candidates.isEmpty() && candidates.get(0).onDiskGraph != null) {
                swr.jvectorFeatureIds = RemoteSegmentGraphMerger.featureSetToStringList(
                        candidates.get(0).onDiskGraph.getFeatureSet());
            }

            // Upload succeeded — both files exist remotely. Pre-populate orphans
            // so any subsequent failure (preloadCompactedSegment, etc.) routes
            // them through pendingDeletes for retention-aware cleanup.
            //
            // Issue #621 review item: the orphan-routing guarantee is conditional
            // on the failure reaching runCompactionCycle as a CompactionException.
            // Every per-step catch in this method wraps Error → CompactionException
            // (see the issue #621 broadening above), and the analogous
            // post-rebuild path in PersistentVectorStore.runCompactionCycle has its
            // own catch (Error) arm that drains rebuild.orphanPaths into
            // pendingDeletes before rethrowing — both must stay Error-aware for
            // the orphan-routing contract to hold.
            orphans.add(new String[]{
                    store.indexUUID() + "_seg" + segmentId, "graph"});
            orphans.add(new String[]{
                    store.indexUUID() + "_seg" + segmentId, "map"});

            try {
                mergedSegment = store.preloadCompactedSegment(swr);
            } catch (IOException | DataStorageManagerException | Error e) {
                // Issue #621: include Error so a post-upload preload failure
                // (Netty OOM, etc.) still routes the already-uploaded graph + map
                // multipart files through the orphan-list pipeline. Pre-fix, an
                // Error here would leak both files in remote storage even though
                // the upload had succeeded and `orphans` was already populated.
                FailureReason reason = (e instanceof DataStorageManagerException)
                        ? FailureReason.METADATA_IO : FailureReason.READ_IO;
                throw new CompactionException(reason,
                        "preloadCompactedSegment failed for streaming output segment "
                                + segmentId, e, orphans);
            }

            // Issue #485: the streaming engine retrains its own PQ codebook via
            // PQRetrainer (balanced sample over compaction inputs). The store's
            // cachedPQ now reflects a stale distribution; invalidate so the next
            // checkpoint trains fresh. Hoisting the retrained codebook back into
            // cachedPQ would save K-Means cost but bias the cache toward the
            // compaction-input subset of the index (not necessarily a better
            // representative than what getOrTrainPQ would produce next time).
            store.invalidateCachedPq();

            success = true;
            LOGGER.log(Level.INFO,
                    "streaming compaction: merged {0} segment(s) into segment {1} "
                            + "(kept={2}, filtered={3}, startNodeId={4})",
                    new Object[]{candidates.size(), segmentId, keptCount,
                            filteredCount, startNodeId});
            return new RebuildResult(mergedSegment,
                    swr.graphFileSize + swr.mapFileSize, keptCount, filteredCount, orphans);
        } finally {
            try {
                Files.deleteIfExists(graphTemp);
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "ignoring streaming graph temp delete failure", e);
            }
            try {
                Files.deleteIfExists(mapTemp);
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "ignoring streaming map temp delete failure", e);
            }
            if (!success && mergedSegment != null) {
                try {
                    mergedSegment.close();
                } catch (RuntimeException | Error e) {
                    // Issue #621: include Error so an Error from close()
                    // (e.g. OOM on a close-time buffer flush) cannot
                    // suppress an in-flight CompactionException carrying
                    // the orphan list. If close() throws and the in-flight
                    // exception is suppressed, runCompactionCycle's
                    // catch (CompactionException) arm never sees the orphan
                    // list and partial multipart files leak.
                    LOGGER.log(Level.FINE,
                            "ignoring merged-segment close failure (streaming)", e);
                }
            }
        }
        } finally {
            // Close opened reader suppliers before deleting temp files (mmap release).
            // Broad catch (IOException | RuntimeException) is required here for best-effort
            // cleanup safety: jvector's MemorySegmentReader$Supplier.close() throws
            // IllegalStateException (a RuntimeException, not IOException) when the
            // underlying Arena is already closed. While the new fresh-supplier PQ factory
            // means PQRetrainer never closes the suppliers in this list, catching
            // RuntimeException guards against future jvector contract changes and any
            // opaque mmap cleanup order that could leave an arena closed before we reach
            // this finally block. Swallowing both flavors ensures temp-file deletion
            // in the next loop always runs.
            for (ReaderSupplier rs : openedReaderSuppliers) {
                try {
                    rs.close();
                } catch (IOException | RuntimeException e) {
                    LOGGER.log(Level.FINE,
                            "ignoring source reader-supplier close in streaming compaction", e);
                }
            }
            // Delete all source graph temp files (direct-download or copy path).
            for (Path tmpFile : downloadedTempFiles) {
                try {
                    Files.deleteIfExists(tmpFile);
                } catch (IOException e) {
                    LOGGER.log(Level.FINE,
                            "ignoring source graph temp delete in streaming compaction", e);
                }
            }
        }
    }

    /**
     * Writes the per-(newOrdinal, pk, vector) map file expected by
     * {@link PersistentVectorStore#preloadCompactedSegment} / the existing
     * {@code loadFusedPQSegment} reader. Wire format mirrors
     * {@code writeFusedPQMapDataToTempFile}:
     * <pre>
     *   int entryCount
     *   per entry: int newOrdinal, int pkLen, byte[pkLen] pk, int floatCount, int[floatCount] floats
     * </pre>
     *
     * <p>Entries are emitted in (sourceIndex, oldOrdinal) ascending order so
     * vectors are read sequentially from each source's
     * {@link OnDiskGraphIndex.View} — disk-friendly and reproducible.
     */
    /**
     * @param localSources the eagerly-downloaded local-file-backed
     *                     {@link OnDiskGraphIndex} objects that parallel
     *                     {@code candidates}; vectors are read from these
     *                     rather than from {@code seg.onDiskGraph} so that
     *                     the map-file writer also bypasses the block cache
     *                     (issue #602).
     */
    private static void writeStreamingCompactedMapFile(
            List<VectorSegment> candidates,
            List<FixedBitSet> liveBitsets,
            List<OrdinalMapper> mappers,
            List<OnDiskGraphIndex> localSources,
            Path mapTempFile,
            int dimension,
            int keptCount) throws IOException {
        boolean ok = false;
        try (java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(
                java.nio.file.Files.newOutputStream(mapTempFile),
                64 * 1024);
             java.io.DataOutputStream dos = new java.io.DataOutputStream(bos)) {
            dos.writeInt(keptCount);
            for (int s = 0; s < candidates.size(); s++) {
                VectorSegment seg = candidates.get(s);
                FixedBitSet live = liveBitsets.get(s);
                OrdinalMapper mapper = mappers.get(s);
                int[] offsets = seg.pkOffsets;
                int[] lengths = seg.pkLengths;
                byte[] data = seg.pkData;
                if (offsets == null || data == null || lengths == null) {
                    continue;
                }
                // Use the pre-downloaded local-file graph rather than seg.onDiskGraph
                // (which is block-cache backed) so vector reads come from local storage.
                OnDiskGraphIndex localOdg = localSources.get(s);
                OnDiskGraphIndex.View view;
                try {
                    view = (OnDiskGraphIndex.View) localOdg.getView();
                } catch (RuntimeException e) {
                    throw new IOException("failed to open view on candidate segment "
                            + seg.segmentId + " (streaming map writer)", e);
                }
                try {
                    VectorFloat<?> tmp = io.github.jbellis.jvector.vector.VectorizationProvider
                            .getInstance().getVectorTypeSupport()
                            .createFloatVector(dimension);
                    for (int ord = 0; ord < offsets.length; ord++) {
                        if (!live.get(ord)) {
                            continue;
                        }
                        int newOrdinal = mapper.oldToNew(ord);
                        int off = offsets[ord];
                        int len = lengths[ord];
                        try {
                            view.getVectorInto(ord, tmp, 0);
                        } catch (RuntimeException e) {
                            throw new IOException("failed to read vector at ord " + ord
                                    + " of segment " + seg.segmentId
                                    + " (streaming map writer)", e);
                        }
                        if (tmp.length() != dimension) {
                            throw new IOException("dimension mismatch at ord " + ord
                                    + " of segment " + seg.segmentId
                                    + ": expected " + dimension + " got " + tmp.length());
                        }
                        dos.writeInt(newOrdinal);
                        dos.writeInt(len);
                        dos.write(data, off, len);
                        dos.writeInt(dimension);
                        for (int j = 0; j < dimension; j++) {
                            dos.writeInt(Float.floatToIntBits(tmp.get(j)));
                        }
                    }
                } finally {
                    try {
                        view.close();
                    } catch (IOException e) {
                        LOGGER.log(Level.FINE,
                                "ignoring view close in streaming map writer", e);
                    }
                }
            }
            ok = true;
        } finally {
            if (!ok) {
                try {
                    Files.deleteIfExists(mapTempFile);
                } catch (IOException ignored) {
                    // Best-effort cleanup; the outer rebuild will log if needed.
                }
            }
        }
    }

    /**
     * Dense per-source {@link OrdinalMapper}: walks the live-bitset once and
     * assigns each live ordinal a unique global new ordinal in
     * {@code [newBase, newBase + liveCount)}. Dead ordinals are
     * {@link OrdinalMapper#OMITTED}.
     *
     * <p>{@link OnDiskGraphIndexCompactor} only invokes
     * {@link #oldToNew(int)} on ordinals it has already filtered through
     * {@code liveNodes.get(s).get(node)}, so the mapper is only ever asked
     * for live ordinals. {@code newToOld} is exposed for completeness but
     * unused by the compactor; it is implemented correctly anyway so this
     * mapper is a well-formed {@link OrdinalMapper}.
     *
     * <p>Uses primitive {@code int[]} backing arrays — no boxing — so memory
     * cost is {@code 8 × sourceSize} bytes per source instead of
     * {@code ~48 × liveCount} for a {@code Map<Integer, Integer>}-backed
     * {@link OrdinalMapper.MapMapper}. For million-vector segments this
     * difference matters.
     */
    static final class DenseLiveOrdinalMapper implements OrdinalMapper {

        private final int newBase;
        private final int liveCount;
        private final int[] oldToNewArr; // size = sourceSize; OMITTED for dead ordinals.
        private final int[] newToOldArr; // size = liveCount; sparse mapping back.

        DenseLiveOrdinalMapper(FixedBitSet liveBits, int sourceSize, int newBase) {
            if (liveBits == null) {
                throw new IllegalArgumentException("liveBits must not be null");
            }
            if (sourceSize < 0) {
                throw new IllegalArgumentException(
                        "sourceSize must be >= 0, got " + sourceSize);
            }
            if (newBase < 0) {
                throw new IllegalArgumentException(
                        "newBase must be >= 0, got " + newBase);
            }
            this.newBase = newBase;
            this.oldToNewArr = new int[sourceSize];
            Arrays.fill(this.oldToNewArr, OMITTED);
            // Cardinality bounded by sourceSize, so int suffices.
            int counted = 0;
            for (int ord = 0; ord < sourceSize; ord++) {
                if (liveBits.get(ord)) {
                    counted++;
                }
            }
            this.liveCount = counted;
            this.newToOldArr = new int[counted];
            int newOrd = newBase;
            int idx = 0;
            for (int ord = 0; ord < sourceSize; ord++) {
                if (liveBits.get(ord)) {
                    this.oldToNewArr[ord] = newOrd;
                    this.newToOldArr[idx++] = ord;
                    newOrd++;
                }
            }
        }

        int liveCount() {
            return liveCount;
        }

        @Override
        public int maxOrdinal() {
            // jvector takes the global max across all mappers; for a dense layout,
            // (newBase + liveCount - 1) is the highest output ordinal this source
            // contributes. When liveCount == 0 we return newBase - 1 so the global
            // max remains correct (the caller guards against an empty cycle).
            return newBase + liveCount - 1;
        }

        @Override
        public int oldToNew(int oldOrdinal) {
            if (oldOrdinal < 0 || oldOrdinal >= oldToNewArr.length) {
                return OMITTED;
            }
            return oldToNewArr[oldOrdinal];
        }

        @Override
        public int newToOld(int newOrdinal) {
            int idx = newOrdinal - newBase;
            if (idx < 0 || idx >= liveCount) {
                return OMITTED;
            }
            return newToOldArr[idx];
        }
    }

    /**
     * Legacy in-memory rebuild path. Identical to the body that previously
     * lived inside {@link #rebuildSegment} prior to issue #485 — split out so
     * the dispatcher can choose between streaming and legacy paths cleanly.
     */
    private static RebuildResult rebuildSegmentLegacy(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            CompactionAuthorityMap authority,
            int dim,
            int keptCount,
            int filteredCount)
            throws CompactionException, IOException, DataStorageManagerException {

        List<String[]> orphans = new ArrayList<>();

        long startNodeId = store.allocateCompactionNodeIds(keptCount);

        ConcurrentHashMap<Bytes, Integer> pkToNode = new ConcurrentHashMap<>(keptCount);
        ConcurrentHashMap<Integer, Bytes> nodeToPk = new ConcurrentHashMap<>(keptCount);
        VectorStorage syntheticStorage = new VectorStorage(keptCount);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(
                        syntheticStorage, dim, keptCount);
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(
                ravv, store.compactionSimilarity());
        GraphIndexBuilder builder = new GraphIndexBuilder(
                bsp, dim,
                List.of(store.graphBuilderM()),
                store.graphBuilderBeamWidth(),
                store.graphBuilderNeighborOverflow(),
                store.graphBuilderAlpha(),
                /* addHierarchy */ false,
                /* refineFinalGraph */ false,
                ForkJoinPool.commonPool(),
                ForkJoinPool.commonPool(),
                keptCount);

        AtomicInteger localOrdCounter = new AtomicInteger(0);
        PersistentVectorStore.LiveGraphShard synthetic = new PersistentVectorStore.LiveGraphShard(
                pkToNode, nodeToPk, ravv, builder, syntheticStorage, startNodeId);
        java.util.function.Consumer<PersistentVectorStore.LiveGraphShard> observer =
                syntheticShardObserverForTest;
        if (observer != null) {
            observer.accept(synthetic);
        }

        boolean success = false;
        VectorSegment mergedSegment = null;
        try {
            populateSyntheticShard(store, candidates, authority, synthetic,
                    localOrdCounter);

            try {
                builder.cleanup();
            } catch (RuntimeException | Error e) {
                // Issue #621: include Error so an OutOfMemoryError thrown by the
                // jvector builder's cleanup() (which allocates internal scratch
                // buffers) is wrapped as a CompactionException rather than
                // escaping to vectorIndexCompactionLoop. No orphans yet — fires
                // before any multipart upload.
                throw new CompactionException(FailureReason.WRITE_IO,
                        "GraphIndexBuilder.cleanup failed during compaction rebuild", e);
            }

            int segmentId = store.newSegmentId();
            PersistentVectorStore.SegmentWriteResult swr;
            try {
                swr = store.writeSyntheticShard(synthetic, segmentId, dim);
            } catch (IOException | DataStorageManagerException | Error e) {
                // Issue #485 review item B.1#1: wrap the upload failure in a
                // CompactionException carrying the orphan list so the outer
                // cycle's pendingDeletes drainer fires (the previous rethrow
                // dropped orphans with the stack frame).
                //
                // Issue #621: include Error for the same reason as the
                // matching arm in rebuildSegmentStreaming around
                // writeStreamingCompactedSegment — a Netty OutOfDirectMemoryError
                // mid multipart-write must produce the orphan-list wrap that
                // runCompactionCycle's catch (CompactionException) arm drains
                // into pendingDeletes; otherwise partial blocks leak in remote
                // storage.
                List<String[]> failureOrphans = new ArrayList<>(2);
                failureOrphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "graph"});
                failureOrphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "map"});
                FailureReason reason = (e instanceof DataStorageManagerException)
                        ? FailureReason.METADATA_IO : FailureReason.WRITE_IO;
                throw new CompactionException(reason,
                        "writeSyntheticShard failed for segment " + segmentId,
                        e, failureOrphans);
            }
            if (swr == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "writeSyntheticShard returned null for non-empty compaction shard");
            }

            // Upload succeeded — track orphans so a preload failure still
            // routes through pendingDeletes for cleanup.
            orphans.add(new String[]{
                    store.indexUUID() + "_seg" + segmentId, "graph"});
            orphans.add(new String[]{
                    store.indexUUID() + "_seg" + segmentId, "map"});

            try {
                mergedSegment = store.preloadCompactedSegment(swr);
            } catch (IOException | DataStorageManagerException | Error e) {
                // Issue #621: see the matching arm in rebuildSegmentStreaming —
                // a post-upload Error must still route the already-uploaded
                // multipart files through `orphans` for retention-aware cleanup.
                FailureReason reason = (e instanceof DataStorageManagerException)
                        ? FailureReason.METADATA_IO : FailureReason.READ_IO;
                throw new CompactionException(reason,
                        "preloadCompactedSegment failed for legacy output segment "
                                + segmentId, e, orphans);
            }
            success = true;
            return new RebuildResult(mergedSegment,
                    swr.graphFileSize + swr.mapFileSize, keptCount, filteredCount, orphans);
        } finally {
            try {
                builder.close();
            } catch (IOException | Error e) {
                // Issue #621: include Error for the same reason as the
                // matching close() catches in rebuildSegmentStreaming —
                // an Error from close() must not suppress an in-flight
                // CompactionException carrying the orphan list, otherwise
                // runCompactionCycle never sees the orphans and partial
                // multipart files leak.
                LOGGER.log(Level.FINE, "ignoring builder close failure in compaction", e);
            }
            if (!success && mergedSegment != null) {
                try {
                    mergedSegment.close();
                } catch (RuntimeException | Error e) {
                    // Issue #621: see the matching close() catch above.
                    LOGGER.log(Level.FINE, "ignoring merged-segment close failure", e);
                }
            }
        }
    }
}
