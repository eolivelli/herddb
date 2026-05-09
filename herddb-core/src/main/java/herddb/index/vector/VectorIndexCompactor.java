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

package herddb.index.vector;

import herddb.index.vector.PersistentVectorStore.PendingDelete;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
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

        CompactionException(FailureReason reason, String message) {
            super(message);
            this.reason = reason;
        }

        CompactionException(FailureReason reason, String message, Throwable cause) {
            super(message, cause);
            this.reason = reason;
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
     * <p>Returns an empty list when neither the byte threshold nor the
     * count trigger is satisfied.
     *
     * <p>Package-private for unit tests.
     */
    static List<VectorSegment> chooseSegmentsToMerge(
            List<VectorSegment> candidates,
            int minCount,
            long minTotalBytes,
            long maxTotalBytes,
            int maxCount) {
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

        // Standard byte-threshold trigger.
        if (picked.size() >= minCount && total >= minTotalBytes) {
            return picked;
        }
        // Count-based trigger (issue #285): fire even if the byte threshold
        // is not met when too many segments have accumulated.  This guards
        // against the scenario where every segment is individually small
        // (e.g. catch-up with tiny shards) and the sum never reaches
        // minTotalBytes despite hundreds of segments building up.
        if (picked.size() >= maxCount) {
            return picked;
        }
        return new ArrayList<>();
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
            } catch (RuntimeException e) {
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
                    } catch (RuntimeException e) {
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
                    } catch (RuntimeException e) {
                        throw new CompactionException(FailureReason.WRITE_IO,
                                "addGraphNode failed at localOrd " + localOrd, e);
                    }
                }
            } finally {
                try {
                    view.close();
                } catch (IOException e) {
                    LOGGER.log(Level.FINE, "ignoring view close in compaction", e);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Streaming compaction (issue #485) — driven by OnDiskGraphIndexCompactor.
    // -------------------------------------------------------------------------

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
        // unchecked exception escape the jvector boundary.
        for (VectorSegment seg : candidates) {
            OnDiskGraphIndex odg = seg.onDiskGraph;
            if (odg == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "candidate segment " + seg.segmentId
                                + " has no on-disk graph (streaming compaction)");
            }
            if (!odg.getFeatures().containsKey(FeatureId.INLINE_VECTORS)) {
                // HerdDB writes INLINE_VECTORS unconditionally at every site; this
                // is a pure invariant guard.
                throw new CompactionException(FailureReason.CORRUPTION,
                        "candidate segment " + seg.segmentId
                                + " missing INLINE_VECTORS feature (required by"
                                + " OnDiskGraphIndexCompactor)");
            }
        }

        // Build per-source dense ordinal mappers so the merged segment occupies
        // exactly keptCount records on disk (no holes for tombstoned/superseded
        // ordinals). With OffsetMapper the inline level-0 area would be sized
        // for sum(srcSize) ordinals, wasting up to one record per dead slot.
        List<OrdinalMapper> mappers = new ArrayList<>(candidates.size());
        List<OnDiskGraphIndex> sources = new ArrayList<>(candidates.size());
        int globalBase = 0;
        for (int s = 0; s < candidates.size(); s++) {
            VectorSegment seg = candidates.get(s);
            FixedBitSet live = liveBitsets.get(s);
            OnDiskGraphIndex odg = seg.onDiskGraph;
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
            sources.add(odg);
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
        if (sources.size() < 2) {
            LOGGER.log(Level.FINE,
                    "streaming compaction: only {0} candidate(s); falling back to legacy path",
                    sources.size());
            return rebuildSegmentLegacy(store, candidates, authority, dim,
                    keptCount, filteredCount);
        }

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
            OnDiskGraphIndexCompactor compactor = new OnDiskGraphIndexCompactor(
                    sources, liveBitsets, mappers, store.compactionSimilarity(),
                    PhysicalCoreExecutor.pool());
            try {
                compactor.compact(graphTemp);
            } catch (RuntimeException e) {
                // jvector boundary — wrap any unchecked failure to keep the
                // outer cycle's metric/orphan bookkeeping consistent.
                throw new CompactionException(FailureReason.WRITE_IO,
                        "OnDiskGraphIndexCompactor.compact failed for segment "
                                + segmentId, e);
            }

            try {
                writeStreamingCompactedMapFile(candidates, liveBitsets, mappers,
                        mapTemp, dim, keptCount);
            } catch (RuntimeException e) {
                throw new CompactionException(FailureReason.WRITE_IO,
                        "streaming compaction map-file build failed for segment "
                                + segmentId, e);
            }

            PersistentVectorStore.SegmentWriteResult swr;
            try {
                swr = store.writeStreamingCompactedSegment(
                        graphTemp, mapTemp, segmentId, keptCount);
            } catch (IOException | DataStorageManagerException e) {
                orphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "graph"});
                orphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "map"});
                throw e;
            }
            if (swr == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "writeStreamingCompactedSegment returned null for non-empty"
                                + " streaming compaction shard (keptCount=" + keptCount + ")");
            }

            mergedSegment = store.preloadCompactedSegment(swr);

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
                } catch (RuntimeException e) {
                    LOGGER.log(Level.FINE,
                            "ignoring merged-segment close failure (streaming)", e);
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
    private static void writeStreamingCompactedMapFile(
            List<VectorSegment> candidates,
            List<FixedBitSet> liveBitsets,
            List<OrdinalMapper> mappers,
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
                OnDiskGraphIndex.View view;
                try {
                    view = (OnDiskGraphIndex.View) seg.onDiskGraph.getView();
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
            } catch (RuntimeException e) {
                throw new CompactionException(FailureReason.WRITE_IO,
                        "GraphIndexBuilder.cleanup failed during compaction rebuild", e);
            }

            int segmentId = store.newSegmentId();
            PersistentVectorStore.SegmentWriteResult swr;
            try {
                swr = store.writeSyntheticShard(synthetic, segmentId, dim);
            } catch (IOException | DataStorageManagerException e) {
                orphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "graph"});
                orphans.add(new String[]{
                        store.indexUUID() + "_seg" + segmentId, "map"});
                throw e;
            }
            if (swr == null) {
                throw new CompactionException(FailureReason.CORRUPTION,
                        "writeSyntheticShard returned null for non-empty compaction shard");
            }

            mergedSegment = store.preloadCompactedSegment(swr);
            success = true;
            return new RebuildResult(mergedSegment,
                    swr.graphFileSize + swr.mapFileSize, keptCount, filteredCount, orphans);
        } finally {
            try {
                builder.close();
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "ignoring builder close failure in compaction", e);
            }
            if (!success && mergedSegment != null) {
                try {
                    mergedSegment.close();
                } catch (RuntimeException e) {
                    LOGGER.log(Level.FINE, "ignoring merged-segment close failure", e);
                }
            }
        }
    }
}
