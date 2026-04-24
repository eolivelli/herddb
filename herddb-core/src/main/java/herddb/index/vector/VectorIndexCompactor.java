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
import herddb.utils.Bytes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private VectorIndexCompactor() {
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
}
