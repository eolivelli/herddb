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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * Pins the step-4 {@link MergePolicy#pickMergeCandidates(List, java.util.function.Predicate)}
 * overload across both production implementations
 * ({@link MergePolicy.AggressivePolicy} and {@link MergePolicy.SmallestFirstPolicy}):
 * segments whose UUIDs are excluded by the predicate are dropped before the
 * policy runs, and the result is otherwise equivalent to running the policy
 * on a pre-filtered list. Wired into the producer in step 5.
 */
public class MergePolicyExclusionTest {

    private static VersionedSegmentMetadata segment(String uuid, long sizeBytes) {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid("ts")
                .tableName("docs")
                .indexUuid("idx")
                .indexName("docs_v1")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes)
                .vectorCount(100L)
                .generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        return new VersionedSegmentMetadata(m, 0);
    }

    private static List<VersionedSegmentMetadata> input(int n) {
        List<VersionedSegmentMetadata> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(segment("seg-" + i, 1_000_000L));
        }
        return out;
    }

    @Test
    public void aggressivePolicyHonorsExcluder() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ Long.MAX_VALUE,
                /* perCycleMaxBytes */ Long.MAX_VALUE,
                /* maxCount */ 100,
                /* kwayMax */ 8);
        List<VersionedSegmentMetadata> all = input(6);
        Set<String> excluded = new HashSet<>();
        excluded.add("seg-1");
        excluded.add("seg-4");
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(all, excluded::contains);
        for (VersionedSegmentMetadata v : picked) {
            assertTrue("excluded segment must not appear: " + v.metadata().getSegmentUuid(),
                    !excluded.contains(v.metadata().getSegmentUuid()));
        }
        assertEquals(4, picked.size());
    }

    @Test
    public void smallestFirstPolicyHonorsExcluder() {
        MergePolicy policy = new MergePolicy.SmallestFirstPolicy(2, 100, 0L, Long.MAX_VALUE);
        List<VersionedSegmentMetadata> all = input(5);
        Set<String> excluded = new HashSet<>();
        excluded.add("seg-0");
        excluded.add("seg-2");
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(all, excluded::contains);
        for (VersionedSegmentMetadata v : picked) {
            assertTrue(!excluded.contains(v.metadata().getSegmentUuid()));
        }
        assertEquals(3, picked.size());
    }

    @Test
    public void nullExcluderDelegatesToSingleArgOverload() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                Long.MAX_VALUE, Long.MAX_VALUE, 100, 8);
        List<VersionedSegmentMetadata> all = input(4);
        List<VersionedSegmentMetadata> withNull = policy.pickMergeCandidates(all, null);
        List<VersionedSegmentMetadata> baseline = policy.pickMergeCandidates(all);
        assertEquals(baseline.size(), withNull.size());
    }

    @Test
    public void alwaysFalseExcluderEqualsBaseline() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                Long.MAX_VALUE, Long.MAX_VALUE, 100, 8);
        List<VersionedSegmentMetadata> all = input(4);
        List<VersionedSegmentMetadata> withExcluder = policy.pickMergeCandidates(all, uuid -> false);
        List<VersionedSegmentMetadata> baseline = policy.pickMergeCandidates(all);
        assertEquals(baseline.size(), withExcluder.size());
    }

    @Test
    public void excludingEverythingLeavesNoCandidates() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                Long.MAX_VALUE, Long.MAX_VALUE, 100, 8);
        List<VersionedSegmentMetadata> all = input(4);
        List<VersionedSegmentMetadata> withExcluder = policy.pickMergeCandidates(all, uuid -> true);
        assertTrue(withExcluder.isEmpty());
    }

    @Test
    public void excludingDownToOneCandidatePreventsMerge() {
        // Aggressive policy requires >= 2 candidates to fire. Excluding all but
        // one must return empty regardless of size.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                Long.MAX_VALUE, Long.MAX_VALUE, 100, 8);
        List<VersionedSegmentMetadata> all = input(4);
        List<VersionedSegmentMetadata> withExcluder = policy.pickMergeCandidates(all,
                uuid -> !uuid.equals("seg-0"));
        assertTrue("single-candidate result must be empty (merges require >= 2)",
                withExcluder.isEmpty());
    }
}
