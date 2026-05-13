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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Verifies that {@link MergePolicy.AggressivePolicy} groups sub-target candidates
 * by their {@code jvectorFeatureIds} and picks only from the largest homogeneous
 * group (issue #543: "Each source must have the same features").
 */
public class AggressivePolicyFeatureSetFilterTest {

    private static final String TS = "ts";
    private static final String IDX = "idx";
    private static final long TARGET = 100_000L;

    private VersionedSegmentMetadata seg(String uuid, long sizeBytes, List<String> featureIds) {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid(TS).tableName("t").indexUuid(IDX).indexName("i")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes).vectorCount(1L).generation(1L)
                .createdAtEpochMillis(0L)
                .jvectorFeatureIds(featureIds)
                .build();
        return new VersionedSegmentMetadata(m, /* zkVersion */ 0);
    }

    private static final List<String> FUSED_PQ = Arrays.asList("FUSED_PQ", "INLINE_VECTORS");
    private static final List<String> INLINE_ONLY = Arrays.asList("INLINE_VECTORS");

    /**
     * When all sub-target candidates have the same feature set the policy should
     * behave exactly as before — pick all of them (or up to the caps).
     */
    @Test
    public void homogeneousSetPicksAll() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(
                        seg("a", 1_000L, FUSED_PQ),
                        seg("b", 2_000L, FUSED_PQ),
                        seg("c", 3_000L, FUSED_PQ)));
        assertEquals("all three same-feature segments must be picked", 3, picked.size());
    }

    /**
     * When there are two groups of different sizes, the policy must pick the
     * LARGER group — not a mix.
     */
    @Test
    public void picksLargerHomogeneousGroup() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        // 3 FUSED_PQ segments vs 2 INLINE_ONLY segments
        List<VersionedSegmentMetadata> active = List.of(
                seg("f1", 1_000L, FUSED_PQ),
                seg("f2", 2_000L, FUSED_PQ),
                seg("f3", 3_000L, FUSED_PQ),
                seg("i1", 4_000L, INLINE_ONLY),
                seg("i2", 5_000L, INLINE_ONLY));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(active);
        assertEquals("three FUSED_PQ segments are the largest group", 3, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertEquals("all picked must be from the FUSED_PQ group",
                    FUSED_PQ, v.metadata().getJvectorFeatureIds());
        }
    }

    /**
     * When only one group has ≥ 2 members the policy must pick that group even if
     * the other group is larger as a singleton.
     */
    @Test
    public void singletonGroupIsNotPicked() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        // 1 FUSED_PQ segment + 2 INLINE_ONLY segments
        List<VersionedSegmentMetadata> active = List.of(
                seg("f1", 1_000L, FUSED_PQ),
                seg("i1", 2_000L, INLINE_ONLY),
                seg("i2", 3_000L, INLINE_ONLY));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(active);
        assertEquals("only INLINE_ONLY group has ≥ 2 members", 2, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertEquals(INLINE_ONLY, v.metadata().getJvectorFeatureIds());
        }
    }

    /**
     * When no group has ≥ 2 sub-target members the policy must return empty.
     */
    @Test
    public void noGroupMeetsMergeThresholdReturnsEmpty() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        // One segment of each feature type — no homogeneous group has >= 2
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(
                        seg("f1", 1_000L, FUSED_PQ),
                        seg("i1", 2_000L, INLINE_ONLY)));
        assertEquals("each group has only one segment — no merge possible", 0, picked.size());
    }

    /**
     * Segments with {@code null} feature IDs are isolated in their own group and
     * must not be mixed with segments that have known feature lists.
     */
    @Test
    public void nullFeatureIdsAreIsolatedFromKnownGroups() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        // 2 null-feature segments + 1 FUSED_PQ segment
        List<VersionedSegmentMetadata> active = List.of(
                seg("n1", 1_000L, null),
                seg("n2", 2_000L, null),
                seg("f1", 3_000L, FUSED_PQ));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(active);
        assertEquals("null-feature group has 2 members and is picked", 2, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertTrue("picked segment must have null featureIds",
                    v.metadata().getJvectorFeatureIds() == null);
        }
    }

    /**
     * The feature-set grouping interacts correctly with the k-way mode: the policy
     * still caps by {@code kwayMax} within the chosen homogeneous group.
     */
    @Test
    public void kwayModeRespectsHomogeneousGroup() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100,
                /* kwayMax */ 2);
        // 4 FUSED_PQ + 3 INLINE_ONLY — FUSED_PQ group is larger
        List<VersionedSegmentMetadata> active = List.of(
                seg("f1", 1_000L, FUSED_PQ),
                seg("f2", 2_000L, FUSED_PQ),
                seg("f3", 3_000L, FUSED_PQ),
                seg("f4", 4_000L, FUSED_PQ),
                seg("i1", 1_500L, INLINE_ONLY),
                seg("i2", 2_500L, INLINE_ONLY),
                seg("i3", 3_500L, INLINE_ONLY));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(active);
        assertEquals("kwayMax=2 caps the pick at 2", 2, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertEquals("picked from the FUSED_PQ (larger) group",
                    FUSED_PQ, v.metadata().getJvectorFeatureIds());
        }
    }

    /**
     * A graduated segment (at or above {@code targetMaxBytes}) must never be
     * included even if it happens to belong to the largest group by feature set.
     */
    @Test
    public void graduatedSegmentsAreExcluded() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(TARGET, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> active = List.of(
                seg("big-graduated", TARGET, FUSED_PQ),   // exactly at cap — graduated
                seg("f1", 1_000L, FUSED_PQ),
                seg("f2", 2_000L, FUSED_PQ));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(active);
        assertEquals("graduated segment excluded; remaining 2 sub-target ones are picked",
                2, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertTrue("only sub-target segments", v.metadata().getSizeBytes() < TARGET);
        }
    }
}
