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
import java.util.Comparator;
import java.util.List;

/**
 * Decides which {@code ACTIVE} segments to merge on a given tick.
 *
 * <p>Mirrors the logic of {@code herddb.index.vector.VectorIndexCompactor#chooseSegmentsToMerge}
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
}
