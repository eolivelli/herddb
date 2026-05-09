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
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link MergePolicy.AggressivePolicy} (issue #484).
 *
 * <p>Verifies the four behaviours that distinguish the aggressive policy from
 * the legacy {@code SmallestFirstPolicy}:
 * <ul>
 *   <li>fires whenever ≥2 sub-target candidates exist (no minimum-bytes /
 *       minimum-count gating);</li>
 *   <li>excludes "graduated" segments at or above {@code targetMaxBytes}
 *       from the candidate set;</li>
 *   <li>caps the picked set at {@code perCycleMaxBytes} and {@code maxCount}
 *       — but always picks at least 2 segments to keep merging unstuck;</li>
 *   <li>returns an empty set when fewer than 2 candidates are sub-target.</li>
 * </ul>
 */
public class AggressivePolicyTest {

    private static final String TS = "ts";
    private static final String IDX = "idx";

    private VersionedSegmentMetadata seg(String uuid, long sizeBytes) {
        SegmentMetadata m = SegmentMetadata.builder()
                .segmentUuid(uuid)
                .tablespaceUuid(TS).tableName("t").indexUuid(IDX).indexName("i")
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(0)
                .baseLsn(new LogSequenceNumber(1L, 100L))
                .sizeBytes(sizeBytes).vectorCount(1L).generation(1L)
                .createdAtEpochMillis(0L)
                .build();
        return new VersionedSegmentMetadata(m, /* zkVersion */ 0);
    }

    @Test
    public void firesWithTwoSubTargetCandidates() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ 1_000L, /* perCycleMaxBytes */ Long.MAX_VALUE,
                /* maxCount */ 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("a", 100), seg("b", 100)));
        assertEquals(2, picked.size());
    }

    @Test
    public void excludesGraduatedSegments() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(1_000L, Long.MAX_VALUE, 100);
        // Two segments at-or-above the cap → "graduated", left alone.
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("graduated-1", 1_000L), seg("graduated-2", 5_000L), seg("small", 10L)));
        assertEquals("only one sub-target segment → no merge", 0, picked.size());
    }

    @Test
    public void mixedSegmentSetMergesOnlySubTarget() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(1_000L, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("graduated", 5_000L), seg("small1", 10L),
                        seg("small2", 20L), seg("small3", 30L)));
        assertEquals(3, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertTrue("only sub-target segments are picked",
                    v.metadata().getSizeBytes() < 1_000L);
        }
    }

    @Test
    public void perCycleMaxBytesCapsThePickedSet() {
        // Three 100-byte sub-target segments + a perCycleMaxBytes of 250 →
        // we can fit two but not three. The first two are always picked
        // (the 2-segment minimum overrides the byte cap).
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ 10_000L,
                /* perCycleMaxBytes */ 250L,
                /* maxCount */ 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("s1", 100L), seg("s2", 100L), seg("s3", 100L)));
        assertEquals("perCycleMaxBytes caps the third pick", 2, picked.size());
    }

    @Test
    public void maxCountCapsThePickedSet() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, Long.MAX_VALUE,
                /* maxCount */ 3);
        List<VersionedSegmentMetadata> activeSegments = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            activeSegments.add(seg("s" + i, 10L));
        }
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(activeSegments);
        assertEquals("maxCount caps the picked set at 3", 3, picked.size());
    }

    @Test
    public void picksAllEightWhenBudgetAllows() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> activeSegments = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            activeSegments.add(seg("s" + i, 10L));
        }
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(activeSegments);
        assertEquals("all 8 sub-target segments are picked when budget is unlimited",
                8, picked.size());
    }

    @Test
    public void singleSegmentReturnsEmpty() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("s", 10L)));
        assertEquals(0, picked.size());
    }

    @Test
    public void emptyInputReturnsEmpty() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, Long.MAX_VALUE, 100);
        assertEquals(0, policy.pickMergeCandidates(List.of()).size());
        assertEquals(0, policy.pickMergeCandidates(null).size());
    }

    @Test
    public void invalidArgsRejected() {
        try {
            new MergePolicy.AggressivePolicy(0L, 1L, 2);
            fail("targetMaxBytes=0 must throw");
        } catch (IllegalArgumentException ok) {
            // expected
        }
        try {
            new MergePolicy.AggressivePolicy(1L, 0L, 2);
            fail("perCycleMaxBytes=0 must throw");
        } catch (IllegalArgumentException ok) {
            // expected
        }
        try {
            new MergePolicy.AggressivePolicy(1L, 1L, 1);
            fail("maxCount<2 must throw");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void smallestFirstOrderingIsPreserved() {
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, Long.MAX_VALUE, 100);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("big", 500L), seg("med", 200L), seg("small", 50L)));
        assertEquals(3, picked.size());
        assertEquals("smallest first", "small", picked.get(0).metadata().getSegmentUuid());
        assertEquals("medium next", "med", picked.get(1).metadata().getSegmentUuid());
        assertEquals("big last", "big", picked.get(2).metadata().getSegmentUuid());
    }
}
