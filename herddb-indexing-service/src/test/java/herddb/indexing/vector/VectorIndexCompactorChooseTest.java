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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Policy tests for {@link VectorIndexCompactor#chooseSegmentsToMerge}.
 */
public class VectorIndexCompactorChooseTest {

    private static VectorSegment seg(int id, long sizeBytes) {
        VectorSegment s = new VectorSegment(id);
        s.estimatedSizeBytes = sizeBytes;
        return s;
    }

    private static VectorSegment seg(int id, long sizeBytes, int liveNodes) {
        VectorSegment s = seg(id, sizeBytes);
        s.liveCount.set(liveNodes);
        return s;
    }

    @Test
    public void belowMinCountYieldsEmpty() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 200L * 1024 * 1024));
        cand.add(seg(2, 200L * 1024 * 1024));
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 4, /*minBytes*/ 10, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE);
        assertTrue(picked.isEmpty());
    }

    @Test
    public void belowMinBytesYieldsEmpty() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            cand.add(seg(i, 1024L)); // 10 KB total
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L * 1024 * 1024, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE);
        assertTrue(picked.isEmpty());
    }

    @Test
    public void picksSmallestFirstUnderByteCap() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 500L * 1024 * 1024));
        cand.add(seg(2, 100L * 1024 * 1024));
        cand.add(seg(3, 50L * 1024 * 1024));
        cand.add(seg(4, 2000L * 1024 * 1024));
        // Cap = 250 MB: we should pick 3 (50) + 2 (100) = 150 MB; 1 (500) would overflow.
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L, /*maxBytes*/ 250L * 1024 * 1024,
                /*maxCount*/ Integer.MAX_VALUE);
        assertEquals(2, picked.size());
        assertEquals(3, picked.get(0).segmentId);
        assertEquals(2, picked.get(1).segmentId);
    }

    @Test
    public void maxBytesCapHonouredWhenAllFit() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 100L));
        cand.add(seg(2, 100L));
        cand.add(seg(3, 100L));
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(3, picked.size());
    }

    @Test
    public void emptyInputReturnsEmpty() {
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                new ArrayList<>(), 1, 1L, Long.MAX_VALUE, Integer.MAX_VALUE);
        assertTrue(picked.isEmpty());
    }

    @Test
    public void nullInputReturnsEmpty() {
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                null, 1, 1L, Long.MAX_VALUE, Integer.MAX_VALUE);
        assertTrue(picked.isEmpty());
    }

    // ---- Tests for the count-based trigger (issue #285) ----

    /**
     * When byte threshold is NOT met but segment count >= maxCount,
     * compaction must fire.
     */
    @Test
    public void countTriggerFiresWhenBytesNotMet() {
        List<VectorSegment> cand = new ArrayList<>();
        // 10 tiny segments (100 bytes each) — total = 1000 bytes.
        // minBytes = 1 MB, so byte threshold is never reached.
        // maxCount = 10 → count-based trigger must fire.
        for (int i = 0; i < 10; i++) {
            cand.add(seg(i, 100L));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L * 1024 * 1024,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 10);
        assertEquals("count trigger must return all 10 candidates", 10, picked.size());
    }

    /**
     * When count is just below maxCount, neither byte nor count trigger fires.
     */
    @Test
    public void countTriggerDoesNotFireBelowMaxCount() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            cand.add(seg(i, 100L));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L * 1024 * 1024,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 10);
        assertTrue("should not fire when count < maxCount", picked.isEmpty());
    }

    /**
     * Byte trigger takes priority: when byte threshold IS met at lower count,
     * the result must be that subset even if maxCount has not been reached.
     */
    @Test
    public void byteThresholdTakesPriorityOverCountTrigger() {
        List<VectorSegment> cand = new ArrayList<>();
        // 5 segments each 50 MB — total = 250 MB >= minBytes = 200 MB.
        // maxCount = 100 (unreachable here), minCount = 2.
        for (int i = 0; i < 5; i++) {
            cand.add(seg(i, 50L * 1024 * 1024));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 200L * 1024 * 1024,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 100);
        assertEquals("byte trigger fires normally with 5 segments", 5, picked.size());
    }

    /**
     * Segments that individually exceed maxBytes are excluded from the picked
     * list even under the count trigger.  The count trigger operates only on
     * the segments that already passed the maxBytes filter.
     */
    @Test
    public void countTriggerRespectsMaxBytesPerSegmentCap() {
        List<VectorSegment> cand = new ArrayList<>();
        // 5 small segments (10 B each) + 5 huge segments (3 GB each).
        // maxBytes cap = 1 GB, so huge segments are cut from picked.
        // Only 5 small fit → count = 5 = maxCount = 5 → trigger fires.
        for (int i = 0; i < 5; i++) {
            cand.add(seg(i, 10L));
        }
        for (int i = 5; i < 10; i++) {
            cand.add(seg(i, 3L * 1024 * 1024 * 1024));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L * 1024 * 1024 * 1024,
                /*maxBytes*/ 1L * 1024 * 1024 * 1024, /*maxCount*/ 5);
        assertEquals("only the 5 small segments should be picked", 5, picked.size());
        for (VectorSegment s : picked) {
            assertTrue("no huge segment should be in the result",
                    s.estimatedSizeBytes <= 10L);
        }
    }

    // ---- Tests for the input-count cap (issue #587) ----

    /**
     * With a positive {@code maxInputs}, a picked set larger than the cap is
     * truncated to exactly {@code maxInputs} segments, keeping the smallest
     * (the list is sorted smallest-bytes-first).
     */
    @Test
    public void maxInputsTruncatesPickedSmallestFirst() {
        List<VectorSegment> cand = new ArrayList<>();
        // 53 segments — the incident's candidate count. Size == id so the
        // smallest-first sort yields ids 1..53 in ascending order.
        for (int i = 1; i <= 53; i++) {
            cand.add(seg(i, i));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 4, /*minBytes*/ 1L, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE, /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 16);
        assertEquals("picked set must be truncated to maxInputs", 16, picked.size());
        for (int i = 0; i < 16; i++) {
            assertEquals("must keep the 16 smallest segments, in order",
                    i + 1, picked.get(i).segmentId);
        }
    }

    /**
     * {@code maxInputs == 0} disables the cap: the full byte-capped selection
     * is returned even when it is very large.
     */
    @Test
    public void maxInputsZeroDisablesCap() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 1; i <= 53; i++) {
            cand.add(seg(i, i));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 4, /*minBytes*/ 1L, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE, /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 0);
        assertEquals("disabled cap must return the full selection", 53, picked.size());
    }

    /**
     * The cap is applied AFTER the fire/no-fire decision: it never makes a
     * non-compacting cycle compact, and never suppresses a cycle that the
     * triggers would fire. Here the count trigger fires on 20 segments and the
     * cap merely limits how many of them this cycle merges.
     */
    @Test
    public void maxInputsDoesNotChangeTriggerDecision() {
        // Below minCount: no trigger — cap must not resurrect the cycle.
        List<VectorSegment> tooFew = new ArrayList<>();
        tooFew.add(seg(1, 100L));
        tooFew.add(seg(2, 100L));
        assertTrue(VectorIndexCompactor.chooseSegmentsToMerge(
                tooFew, /*minCount*/ 4, /*minBytes*/ 1L, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE, /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 16).isEmpty());

        // Count trigger fires (20 >= maxCount 10) although bytes are tiny;
        // the cap then limits the merge to 16 of the 20 segments.
        List<VectorSegment> many = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            many.add(seg(i, i));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                many, /*minCount*/ 2, /*minBytes*/ 1L * 1024 * 1024,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 10,
                /*microSegmentMaxNodes*/ 0L, /*maxInputs*/ 16);
        assertEquals("count trigger fires, capped to maxInputs", 16, picked.size());
    }

    /**
     * The cap deliberately does NOT apply to the micro-segment fast path
     * (issue #570): that path must stay a fast slot-reclaiming cycle, so a
     * large set of micro-segments is returned in full even when it exceeds
     * {@code maxInputs}.
     */
    @Test
    public void microSegmentPathIsNotCapped() {
        List<VectorSegment> cand = new ArrayList<>();
        // 30 micro-segments (liveCount 10 << microSegmentMaxNodes 1000).
        for (int i = 1; i <= 30; i++) {
            cand.add(seg(i, i, /*liveNodes*/ 10));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ 10, /*microSegmentMaxNodes*/ 1000L, /*maxInputs*/ 16);
        assertEquals("micro-segment fast path must NOT be capped by maxInputs",
                30, picked.size());
        for (VectorSegment s : picked) {
            assertTrue("micro path must merge only micro-segments", s.size() < 1000L);
        }
    }

    /**
     * When the picked set is already at or below {@code maxInputs}, the cap is
     * a no-op and the full selection is returned (covers the
     * {@code maxInputs >= picked.size()} branch of {@code capInputs}).
     */
    @Test
    public void maxInputsNoOpWhenPickedFitsUnderCap() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            cand.add(seg(i, i));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L, /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE, /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 16);
        assertEquals("a picked set within the cap must be returned untruncated",
                5, picked.size());
    }

    /**
     * {@link VectorIndexCompactor#clampMaxInputs} normalises configured values:
     * non-positive disables (0); 1 is clamped up to 2; larger values pass
     * through.
     */
    @Test
    public void clampMaxInputsBehaviour() {
        assertEquals(0, VectorIndexCompactor.clampMaxInputs(0));
        assertEquals(0, VectorIndexCompactor.clampMaxInputs(-5));
        assertEquals(2, VectorIndexCompactor.clampMaxInputs(1));
        assertEquals(2, VectorIndexCompactor.clampMaxInputs(2));
        assertEquals(16, VectorIndexCompactor.clampMaxInputs(16));
    }
}
