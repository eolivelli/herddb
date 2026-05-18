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

import herddb.indexing.segment.VersionedSegmentMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Decides which {@code ACTIVE} segments to merge on a given tick.
 *
 * <p>Mirrors the logic of {@code herddb.indexing.vector.VectorIndexCompactor#chooseSegmentsToMerge}
 * but operates on {@link VersionedSegmentMetadata} from the registry (no
 * in-memory {@code VectorSegment}s, since the optimizer doesn't have those).
 *
 * <p>A future revision will share code with the existing compactor; for the MVP
 * we re-implement the policy here so the engine can ship without disturbing the
 * legacy in-IS compaction path.
 */
public interface MergePolicy {

    /**
     * @param activeSegments the current ACTIVE segments for the index
     * @return list of segments to merge on this tick. An empty (or single-element)
     *         list means "no merge".
     */
    List<VersionedSegmentMetadata> pickMergeCandidates(List<VersionedSegmentMetadata> activeSegments);

    /**
     * Same as {@link #pickMergeCandidates(List)} but with an exclusion predicate
     * the producer uses to filter out segment UUIDs that are already referenced
     * by an in-flight (non-terminal) task. The default implementation filters
     * the input list and delegates to the single-argument overload, so concrete
     * policies do not need to know about the exclusion concept — they keep
     * working with the filtered view of the segment set. Wired into the engine
     * by step 5 once the task producer lands.
     *
     * @param activeSegments full set of ACTIVE segments for the index.
     * @param excludeSegment {@code true} for each segment UUID to drop from
     *     consideration; {@code null} or always-false predicate is the
     *     no-exclusion case.
     */
    default List<VersionedSegmentMetadata> pickMergeCandidates(
            List<VersionedSegmentMetadata> activeSegments,
            Predicate<String> excludeSegment) {
        if (excludeSegment == null) {
            return pickMergeCandidates(activeSegments);
        }
        List<VersionedSegmentMetadata> filtered = new ArrayList<>(activeSegments.size());
        for (VersionedSegmentMetadata v : activeSegments) {
            if (!excludeSegment.test(v.metadata().getSegmentUuid())) {
                filtered.add(v);
            }
        }
        return pickMergeCandidates(filtered);
    }

    /**
     * Default policy: smallest-first, capped by {@code maxBytes}; fires when
     * either the count of mergeable segments exceeds {@code maxCount}, or the
     * total picked size exceeds {@code minBytes} AND segment count is at least
     * {@code minCount}.
     */
    final class SmallestFirstPolicy implements MergePolicy {
        private final int minCount;
        private final int maxCount;
        private final long minBytes;
        private final long maxBytes;

        public SmallestFirstPolicy(int minCount, int maxCount, long minBytes, long maxBytes) {
            this.minCount = minCount;
            this.maxCount = maxCount;
            this.minBytes = minBytes;
            this.maxBytes = maxBytes;
        }

        @Override
        public List<VersionedSegmentMetadata> pickMergeCandidates(
                List<VersionedSegmentMetadata> activeSegments) {
            if (activeSegments.size() < 2) {
                return new ArrayList<>();
            }
            List<VersionedSegmentMetadata> sorted = new ArrayList<>(activeSegments);
            sorted.sort(Comparator.comparingLong(v -> v.metadata().getSizeBytes()));

            // Force-fire when the segment count gets out of hand (issue #285 ceiling).
            boolean countCeiling = sorted.size() >= maxCount;

            List<VersionedSegmentMetadata> picked = new ArrayList<>();
            long pickedBytes = 0L;
            for (VersionedSegmentMetadata v : sorted) {
                long size = Math.max(0L, v.metadata().getSizeBytes());
                if (!picked.isEmpty() && pickedBytes + size > maxBytes) {
                    break;
                }
                picked.add(v);
                pickedBytes += size;
            }

            if (countCeiling) {
                return picked.size() >= 2 ? picked : new ArrayList<>();
            }
            if (picked.size() >= minCount && pickedBytes >= minBytes) {
                return picked;
            }
            return new ArrayList<>();
        }
    }

    /**
     * Aggressive merge policy (issue #484): the optimizer keeps merging
     * graphs until every segment has reached {@code targetMaxBytes}. Any
     * segment whose {@code sizeBytes} is already at or above the target is
     * "graduated" — left alone forever; everything else is mergeable.
     *
     * <p>Trigger: as long as at least two segments are below
     * {@code targetMaxBytes}, the policy fires. There are deliberately no
     * minimum-count or minimum-bytes thresholds — those would cause the
     * optimizer to leave small graphs un-merged for hours under sustained
     * load, which is precisely the failure mode this policy fixes.
     *
     * <p>Selection (k-way mode, issue #524): when {@code kwayMax >= 2}, the
     * policy picks up to {@code min(kwayMax, maxCount, candidates.size())}
     * segments in a single cycle, smallest-first, <em>ignoring</em>
     * {@code perCycleMaxBytes}. This collapses N sub-target segments into
     * one merge round instead of the O(N) rounds that the byte-cap forces,
     * cutting cumulative vector-processing work from O(N²) to O(N).
     *
     * <p>Selection (legacy mode, {@code kwayMax == 0}): smallest-first; the
     * picked set is capped at {@code perCycleMaxBytes} of input data per
     * cycle (write-amplification budget) and at {@code maxCount} segments.
     * The 2-segment minimum always overrides the byte cap so a cycle with
     * two segments whose sum exceeds the cap still proceeds.
     */
    final class AggressivePolicy implements MergePolicy {
        private final long targetMaxBytes;
        private final long perCycleMaxBytes;
        private final int maxCount;
        /**
         * K-way max (issue #524). When {@code >= 2}: pick up to this many
         * candidates per cycle, bypassing {@code perCycleMaxBytes}. When
         * {@code 0}: legacy byte-cap behaviour.
         */
        private final int kwayMax;

        /**
         * Backward-compatible 3-arg constructor: uses legacy {@code perCycleMaxBytes}
         * candidate selection ({@code kwayMax = 0}).
         */
        public AggressivePolicy(long targetMaxBytes, long perCycleMaxBytes, int maxCount) {
            this(targetMaxBytes, perCycleMaxBytes, maxCount, /* kwayMax */ 0);
        }

        /**
         * Full constructor adding k-way candidate selection (issue #524).
         *
         * @param kwayMax  max inputs per merge cycle in k-way mode (0 = legacy byte-cap
         *                 mode; must be 0 or {@code >= 2})
         */
        public AggressivePolicy(long targetMaxBytes, long perCycleMaxBytes, int maxCount,
                                int kwayMax) {
            if (targetMaxBytes <= 0L) {
                throw new IllegalArgumentException("targetMaxBytes must be positive: " + targetMaxBytes);
            }
            if (perCycleMaxBytes <= 0L) {
                throw new IllegalArgumentException("perCycleMaxBytes must be positive: " + perCycleMaxBytes);
            }
            if (maxCount < 2) {
                throw new IllegalArgumentException("maxCount must be >= 2: " + maxCount);
            }
            if (kwayMax != 0 && kwayMax < 2) {
                throw new IllegalArgumentException("kwayMax must be 0 (disabled) or >= 2: " + kwayMax);
            }
            this.targetMaxBytes = targetMaxBytes;
            this.perCycleMaxBytes = perCycleMaxBytes;
            this.maxCount = maxCount;
            this.kwayMax = kwayMax;
        }

        public long getTargetMaxBytes() {
            return targetMaxBytes;
        }

        public long getPerCycleMaxBytes() {
            return perCycleMaxBytes;
        }

        public int getMaxCount() {
            return maxCount;
        }

        /** Returns the configured k-way max (0 = legacy byte-cap mode). */
        public int getKwayMax() {
            return kwayMax;
        }

        @Override
        public List<VersionedSegmentMetadata> pickMergeCandidates(
                List<VersionedSegmentMetadata> activeSegments) {
            if (activeSegments == null || activeSegments.size() < 2) {
                return new ArrayList<>();
            }
            // Filter out graduated segments; only sub-target ones are mergeable.
            List<VersionedSegmentMetadata> subTarget = new ArrayList<>();
            for (VersionedSegmentMetadata v : activeSegments) {
                long size = Math.max(0L, v.metadata().getSizeBytes());
                if (size < targetMaxBytes) {
                    subTarget.add(v);
                }
            }
            if (subTarget.size() < 2) {
                return new ArrayList<>();
            }

            // Group sub-target candidates by their jvector feature set (issue #543).
            // Segments with heterogeneous feature sets cannot be merged by the streaming
            // path (OnDiskGraphIndexCompactor.validateFeatures rejects them), so we pick
            // candidates only from the largest homogeneous group. Segments whose feature
            // set is unknown (null — pre-#543 metadata) are placed in their own group
            // (keyed by an empty list sentinel) and never mixed with segments that carry
            // a known feature list.
            //
            // Using List<String> as the map key is correct and idiomatic: the feature
            // lists are sorted+unmodifiable (written by featureSetToStringList), so
            // AbstractList.equals/hashCode give value-based keying with no toString
            // format dependency.
            final List<String> nullFeatureKey = Collections.emptyList();
            Map<List<String>, List<VersionedSegmentMetadata>> groups = new LinkedHashMap<>();
            for (VersionedSegmentMetadata v : subTarget) {
                List<String> featureIds = v.metadata().getJvectorFeatureIds();
                List<String> key = featureIds != null ? featureIds : nullFeatureKey;
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
            }

            // Pick the largest homogeneous group (≥ 2 members).
            List<VersionedSegmentMetadata> mergeable = null;
            for (List<VersionedSegmentMetadata> group : groups.values()) {
                if (group.size() >= 2
                        && (mergeable == null || group.size() > mergeable.size())) {
                    mergeable = group;
                }
            }
            if (mergeable == null) {
                return new ArrayList<>();
            }

            mergeable.sort(Comparator.comparingLong(v -> v.metadata().getSizeBytes()));

            if (kwayMax >= 2) {
                // K-way mode (issue #524): pick up to min(kwayMax, maxCount) candidates,
                // ignoring perCycleMaxBytes. This merges all sub-target segments in one
                // pass (O(N) work) instead of sequential 2-way rounds (O(N²) work).
                int limit = Math.min(kwayMax, maxCount);
                // subList(0, n) works for both n == size and n < size; the returned
                // view is then copied into a fresh ArrayList to avoid holding a
                // reference to the sorted intermediate list.
                List<VersionedSegmentMetadata> picked =
                        new ArrayList<>(mergeable.subList(0, Math.min(limit, mergeable.size())));
                // Invariant: mergeable.size() >= 2 (checked above) and limit >= 2
                // (constructor enforces kwayMax >= 2 and maxCount >= 2), so
                // picked.size() >= 2 is always true here. The guard is kept as a
                // defensive belt-and-suspenders check.
                return picked.size() >= 2 ? picked : new ArrayList<>();
            }

            // Legacy byte-cap mode: respect perCycleMaxBytes.
            List<VersionedSegmentMetadata> picked = new ArrayList<>();
            long pickedBytes = 0L;
            for (VersionedSegmentMetadata v : mergeable) {
                long size = Math.max(0L, v.metadata().getSizeBytes());
                // Always allow the first two so a cycle with two large
                // sub-target segments still fires; after that, respect the
                // per-cycle byte and count caps.
                if (picked.size() >= 2) {
                    if (pickedBytes + size > perCycleMaxBytes) {
                        break;
                    }
                    if (picked.size() >= maxCount) {
                        break;
                    }
                }
                picked.add(v);
                pickedBytes += size;
            }
            return picked.size() >= 2 ? picked : new ArrayList<>();
        }
    }
}
