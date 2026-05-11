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
import static org.junit.Assert.fail;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.VersionedSegmentMetadata;
import herddb.log.LogSequenceNumber;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link MergePolicy.AggressivePolicy} k-way mode (issue #524).
 *
 * <p>Verifies that when {@code kwayMax >= 2} the policy:
 * <ul>
 *   <li>picks all mergeable candidates when their count is {@code <= kwayMax}
 *       regardless of the {@code perCycleMaxBytes} cap;</li>
 *   <li>picks exactly the smallest {@code kwayMax} candidates when more exist;</li>
 *   <li>still respects the {@code maxCount} hard ceiling;</li>
 *   <li>falls back to the legacy byte-cap behaviour when {@code kwayMax == 0}.</li>
 * </ul>
 */
public class AggressivePolicyKwayTest {

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

    /** Makes a list of N segments with distinct 1-byte sizes (s1=1, s2=2, …). */
    private List<VersionedSegmentMetadata> makeSegments(int n) {
        List<VersionedSegmentMetadata> list = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            list.add(seg("s" + i, i));
        }
        return list;
    }

    // -------------------------------------------------------------------------
    // k-way mode: ignores perCycleMaxBytes
    // -------------------------------------------------------------------------

    @Test
    public void kwayPicksAllWhenCandidatesLeqKwayMax() {
        // 4 candidates, kwayMax=4, perCycleMaxBytes=1 (far below total) →
        // k-way ignores the byte cap and picks all 4.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ 10_000L,
                /* perCycleMaxBytes */ 1L,       // would block legacy mode after 1st candidate
                /* maxCount */ 100,
                /* kwayMax */ 4);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(4));
        assertEquals("k-way must pick all 4 candidates regardless of perCycleMaxBytes",
                4, picked.size());
    }

    @Test
    public void kwayPicksSmallestKWhenCandidatesExceedKwayMax() {
        // 8 candidates, kwayMax=4 → pick the 4 smallest.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                10_000L, 1L, 100, 4);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(8));
        assertEquals("k-way must pick exactly kwayMax=4 when more candidates exist",
                4, picked.size());
        // Verify smallest-first ordering.
        for (int i = 0; i < picked.size(); i++) {
            assertEquals("must pick smallest k (s1..s4)",
                    "s" + (i + 1), picked.get(i).metadata().getSegmentUuid());
        }
    }

    @Test
    public void kwayPicks8InOnePassForGist1mScenario() {
        // Simulates the gist1m / 8-initial-segment scenario from the issue.
        // With legacy byte-cap of 1 GiB and 8 segments of ~400 MB each, the
        // old policy could only pick 2-3 per cycle. K-way picks all 8 at once.
        long segmentBytes = 400L * 1024L * 1024L;   // 400 MiB each
        long perCycleMax  = 1L * 1024L * 1024L * 1024L; // 1 GiB (old default)
        List<VersionedSegmentMetadata> segments = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            segments.add(seg("seg" + i, segmentBytes + i)); // slightly different sizes
        }
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ 8L * 1024L * 1024L * 1024L, // 8 GiB target
                perCycleMax,
                /* maxCount */ 200,
                /* kwayMax */ 8);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(segments);
        assertEquals("k-way=8 must pick all 8 segments in one pass", 8, picked.size());
    }

    @Test
    public void kwayMaxCountHardCeilingIsStillRespected() {
        // kwayMax=20 but maxCount=5 → maxCount wins.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                10_000L, 1L, /* maxCount */ 5, /* kwayMax */ 20);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(10));
        assertEquals("maxCount must cap the picked set even in k-way mode", 5, picked.size());
    }

    @Test
    public void kwayWithExactlyTwoCandidatesStillFires() {
        // Minimum viable merge: 2 candidates, kwayMax=8 → pick both.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                10_000L, 1L, 100, 8);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(2));
        assertEquals(2, picked.size());
    }

    @Test
    public void kwayGraduatedSegmentsAreExcluded() {
        // 3 sub-target + 2 graduated; kwayMax=8 → picks the 3 sub-target only.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                /* targetMaxBytes */ 100L, 1L, 100, 8);
        List<VersionedSegmentMetadata> all = new ArrayList<>();
        all.add(seg("small1", 10L));
        all.add(seg("small2", 20L));
        all.add(seg("small3", 30L));
        all.add(seg("graduated1", 100L)); // exactly at target → graduated
        all.add(seg("graduated2", 500L));
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(all);
        assertEquals("only sub-target segments are picked in k-way mode", 3, picked.size());
        for (VersionedSegmentMetadata v : picked) {
            assertEquals("all picked must be sub-target",
                    true, v.metadata().getSizeBytes() < 100L);
        }
    }

    // -------------------------------------------------------------------------
    // kwayMax == 0: legacy byte-cap behaviour is preserved
    // -------------------------------------------------------------------------

    @Test
    public void kwayZeroFallsBackToPerCycleBytesLogic() {
        // Same 3-segment, tight-byte-cap scenario as AggressivePolicyTest —
        // kwayMax=0 must reproduce the original perCycleMaxBytes-capped result.
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                10_000L,
                /* perCycleMaxBytes */ 250L,
                100,
                /* kwayMax */ 0);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(
                List.of(seg("s1", 100L), seg("s2", 100L), seg("s3", 100L)));
        assertEquals("kwayMax=0 must respect perCycleMaxBytes (picks 2, not 3)", 2, picked.size());
    }

    @Test
    public void kwayZeroPicksAllWhenBudgetAllows() {
        // kwayMax=0 with unlimited budget → same as before (picks all).
        MergePolicy policy = new MergePolicy.AggressivePolicy(
                10_000L, Long.MAX_VALUE, 100, 0);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(8));
        assertEquals(8, picked.size());
    }

    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    @Test
    public void kwayMaxOneIsRejected() {
        try {
            new MergePolicy.AggressivePolicy(1_000L, 1_000L, 2, 1);
            fail("kwayMax=1 must throw IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void kwayMaxNegativeIsRejected() {
        try {
            new MergePolicy.AggressivePolicy(1_000L, 1_000L, 2, -1);
            fail("kwayMax=-1 must throw IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            // expected
        }
    }

    @Test
    public void kwayMaxTwoIsAccepted() {
        // Minimum valid k-way value.
        MergePolicy policy = new MergePolicy.AggressivePolicy(10_000L, 1L, 100, 2);
        List<VersionedSegmentMetadata> picked = policy.pickMergeCandidates(makeSegments(4));
        assertEquals("kwayMax=2 picks exactly 2 (smallest)", 2, picked.size());
    }
}
