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
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     *   <li>smallest-first ordering to maximise the contraction ratio
     *       and avoid rewriting already-large segments.</li>
     * </ul>
     *
     * <p>Returns an empty list when either the count or size thresholds
     * are not met, signalling that compaction should not fire yet.
     *
     * <p>Package-private for unit tests.
     */
    static List<VectorSegment> chooseSegmentsToMerge(
            List<VectorSegment> candidates,
            int minCount,
            long minTotalBytes,
            long maxTotalBytes) {
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

        if (picked.size() < minCount || total < minTotalBytes) {
            return new ArrayList<>();
        }
        return picked;
    }

    /**
     * Builds the PK authority map used by the live-PK filter. For every
     * primary key observed across the input candidates, all later
     * segments (segments with a strictly greater {@code generation}
     * than the max candidate generation), and any live-shard index, the
     * map records the highest-generation source.
     *
     * <p>During the rebuild the caller re-inserts a (PK, vector) pair
     * iff the authority map's value for that PK resolves back to the
     * same input candidate. Tombstoned or superseded vectors are
     * silently dropped, reclaiming their storage.
     *
     * <p>{@code liveShardPks} is the set of PKs currently resident in
     * any live shard; live shards are always the newest source, so
     * their PKs dominate every segment.
     *
     * @return map {@code PK -> authoritative segment id}; a synthetic
     *     segment id of {@link #LIVE_SHARD_SEGMENT_ID} marks PKs owned
     *     by a live shard.
     */
    static Map<Bytes, Integer> buildAuthorityMap(
            List<VectorSegment> candidates,
            List<VectorSegment> allSegments,
            Iterable<Bytes> liveShardPks) {

        Map<Bytes, Long> winnerGeneration = new HashMap<>();
        Map<Bytes, Integer> winnerSegment = new HashMap<>();

        // Scan candidates first — their generation is the baseline.
        for (VectorSegment seg : candidates) {
            visitSegmentPks(seg, winnerGeneration, winnerSegment);
        }

        // Scan segments that are newer than any candidate. Older or
        // equal-generation segments are irrelevant for the authority
        // decision — candidates already cover their own generation and
        // the merged output will replace them.
        long maxCandidateGeneration = 0L;
        for (VectorSegment seg : candidates) {
            if (seg.generation > maxCandidateGeneration) {
                maxCandidateGeneration = seg.generation;
            }
        }
        for (VectorSegment seg : allSegments) {
            if (seg.generation > maxCandidateGeneration && !candidates.contains(seg)) {
                visitSegmentPks(seg, winnerGeneration, winnerSegment);
            }
        }

        // Live shards always dominate — they hold the newest state.
        if (liveShardPks != null) {
            for (Bytes pk : liveShardPks) {
                winnerGeneration.put(pk, Long.MAX_VALUE);
                winnerSegment.put(pk, LIVE_SHARD_SEGMENT_ID);
            }
        }

        return winnerSegment;
    }

    /**
     * Synthetic segment id returned by {@link #buildAuthorityMap} when
     * the authoritative source for a PK is a live in-memory shard.
     * Any real {@link VectorSegment} id is non-negative; this sentinel
     * is {@code -1} so callers can check with {@code ownerId < 0}.
     */
    static final int LIVE_SHARD_SEGMENT_ID = -1;

    private static void visitSegmentPks(VectorSegment seg,
                                        Map<Bytes, Long> winnerGeneration,
                                        Map<Bytes, Integer> winnerSegment) {
        int[] offsets = seg.pkOffsets;
        int[] lengths = seg.pkLengths;
        byte[] data = seg.pkData;
        if (offsets == null || data == null || lengths == null) {
            return;
        }
        long gen = seg.generation;
        for (int ord = 0; ord < offsets.length; ord++) {
            int off = offsets[ord];
            if (off < 0) {
                continue; // tombstoned — not a candidate for re-insert.
            }
            Bytes pk = Bytes.from_array(data, off, lengths[ord]);
            Long existing = winnerGeneration.get(pk);
            if (existing == null || gen > existing) {
                winnerGeneration.put(pk, gen);
                winnerSegment.put(pk, seg.segmentId);
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
            Map<Bytes, Integer> authority)
            throws CompactionException, IOException, DataStorageManagerException {

        int dim = store.compactionDimension();
        List<String[]> orphans = new ArrayList<>();
        int totalCandidateVectors = 0;
        for (VectorSegment seg : candidates) {
            totalCandidateVectors += Math.max(0, seg.liveCount.get());
        }

        int keptCount = 0;
        int filteredCount = 0;

        // First pass: count how many vectors we will actually keep.
        for (VectorSegment seg : candidates) {
            int[] offsets = seg.pkOffsets;
            int[] lengths = seg.pkLengths;
            byte[] data = seg.pkData;
            if (offsets == null || data == null || lengths == null) {
                continue;
            }
            for (int ord = 0; ord < offsets.length; ord++) {
                int off = offsets[ord];
                if (off < 0) {
                    filteredCount++;
                    continue;
                }
                Bytes pk = Bytes.from_array(data, off, lengths[ord]);
                Integer owner = authority.get(pk);
                if (owner == null || owner != seg.segmentId) {
                    filteredCount++;
                } else {
                    keptCount++;
                }
            }
        }
        if (keptCount == 0) {
            LOGGER.log(Level.INFO, "compaction: all {0} candidate vectors are obsolete — skipping rebuild",
                    totalCandidateVectors);
            return null;
        }

        // Reserve a contiguous global nodeId range + force a live-shard
        // rotation (issue #255). The synthetic shard uses its own per-shard
        // VectorStorage keyed by a local ordinal 0..keptCount (issue #256),
        // so no coordination with the live shards' storage is needed.
        long startNodeId = store.allocateCompactionNodeIds(keptCount);

        // Build a synthetic LiveGraphShard anchored at startNodeId with its
        // own isolated VectorStorage of exactly the size we need.
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

            // Cleanup the builder (diversifies edges / refines).
            try {
                builder.cleanup();
            } catch (RuntimeException e) {
                // GraphIndexBuilder.cleanup can throw unchecked on invalid state;
                // treat as WRITE_IO since the rebuild's internal state is corrupted.
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
                // Defensive: writeShardAsFusedPQSegment returns null when
                // the shard is empty. We already checked keptCount > 0, so
                // this shouldn't happen — treat as corruption.
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
            // No store-side release needed: the synthetic shard owns its own
            // VectorStorage (issue #256), so it is reclaimed by the GC when
            // this method returns.
            if (!success && mergedSegment != null) {
                try {
                    mergedSegment.close();
                } catch (RuntimeException e) {
                    LOGGER.log(Level.FINE, "ignoring merged-segment close failure", e);
                }
            }
        }
    }

    /**
     * Fills the synthetic shard by reading authoritative vectors from
     * every candidate segment's on-disk graph.
     */
    private static void populateSyntheticShard(
            PersistentVectorStore store,
            List<VectorSegment> candidates,
            Map<Bytes, Integer> authority,
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
                    Integer owner = authority.get(pk);
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
}
