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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Policy tests for the per-cycle output-node cap added to
 * {@link VectorIndexCompactor#chooseSegmentsToMerge} in issue #643.
 *
 * <p>The cap bounds the sum of {@link VectorSegment#size()} (live-vector count
 * per input, which equals the live-node count contributed to the merged output)
 * across the inputs of a single cycle, so per-cycle wall-clock time tracks the
 * per-cycle work budget rather than the table size. The cap is applied AFTER
 * the trigger / smallest-first / micro-segment / graduation-cap selection
 * logic, never changes whether a cycle compacts, and always keeps at least 2
 * inputs for a meaningful merge. Bypassed by the micro-segment fast path.
 */
public class VectorIndexCompactorMaxOutputNodesTest {

    /**
     * Helper: build a candidate with {@code sizeBytes} bytes and
     * {@code liveNodes} live vectors. Both are independently set so a test
     * can configure each candidate's contribution to the smallest-first
     * byte sort (sizeBytes) and the output-node cap (liveNodes).
     */
    private static VectorSegment seg(int id, long sizeBytes, int liveNodes) {
        VectorSegment s = new VectorSegment(id);
        s.estimatedSizeBytes = sizeBytes;
        s.liveCount.set(liveNodes);
        return s;
    }

    // -------------------------------------------------------------------------
    // The cap is disabled by default
    // -------------------------------------------------------------------------

    @Test
    public void disabledByDefaultPicksFullSelection() {
        // 20 segments of 200k live each. With maxOutputNodes=0 (disabled) the
        // legacy behaviour kicks in and all 20 are picked (no other cap fires).
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            cand.add(seg(i, 1024L, 200_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand,
                /*minCount*/ 2,
                /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 0,
                /*targetMaxBytes*/ Long.MAX_VALUE,
                /*maxOutputNodes*/ 0L);
        assertEquals(20, picked.size());
    }

    @Test
    public void negativeMaxOutputNodesBehavesAsDisabled() {
        // Negative caps are not normalised at the picker boundary (the
        // PersistentVectorStore setter clamps), but the picker must still
        // treat them as disabled rather than throw or pick zero candidates.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cand.add(seg(i, 1024L, 100_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                0L, 0, Long.MAX_VALUE, /*maxOutputNodes*/ -1L);
        assertEquals(5, picked.size());
    }

    // -------------------------------------------------------------------------
    // The cap bounds the output node count
    // -------------------------------------------------------------------------

    @Test
    public void capsOutputAtNodeBudget() {
        // 20 segments of 200k live nodes each (4M total). With
        // maxOutputNodes=1_000_000 the cap allows exactly 5 inputs (5 × 200k
        // = 1M nodes); the 6th would push the total to 1.2M and is rejected.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            cand.add(seg(i, 1024L * (i + 1), 200_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand,
                /*minCount*/ 2,
                /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 0,
                /*targetMaxBytes*/ Long.MAX_VALUE,
                /*maxOutputNodes*/ 1_000_000L);
        assertEquals(5, picked.size());
        // Smallest-first order preserved (the cap trims from the tail).
        assertEquals(0, picked.get(0).segmentId);
        assertEquals(4, picked.get(4).segmentId);
    }

    @Test
    public void capExactlyAtBoundaryKeepsAll() {
        // Total = 5 × 200k = 1M, exactly matching the cap; no truncation.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cand.add(seg(i, 1024L, 200_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                0L, 0, Long.MAX_VALUE, /*maxOutputNodes*/ 1_000_000L);
        assertEquals(5, picked.size());
    }

    // -------------------------------------------------------------------------
    // Always keeps ≥ 2 inputs for a meaningful merge
    // -------------------------------------------------------------------------

    @Test
    public void keepsAtLeastTwoEvenWhenFirstTwoExceedCap() {
        // First two candidates already exceed the cap (2 × 1M = 2M, cap = 500k).
        // The helper must keep both so the cycle still does a meaningful merge.
        // The third would balloon the output further and IS rejected — so the
        // result is exactly 2.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            cand.add(seg(i, 1024L * (i + 1), 1_000_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                0L, 0, Long.MAX_VALUE, /*maxOutputNodes*/ 500_000L);
        assertEquals(2, picked.size());
        assertEquals(0, picked.get(0).segmentId);
        assertEquals(1, picked.get(1).segmentId);
    }

    @Test
    public void capOutputNodesNoOpOnSingleSegment() {
        // capOutputNodes is exposed for direct testing — a one-element list
        // cannot be capped (you can't merge a single segment). Verify the
        // helper short-circuits and returns the same list reference.
        List<VectorSegment> single = new ArrayList<>();
        single.add(seg(1, 1024L, 1_000_000));
        List<VectorSegment> result = VectorIndexCompactor.capOutputNodes(single, 10L);
        assertSame("single-segment input must short-circuit", single, result);
    }

    @Test
    public void capOutputNodesNoOpWhenDisabled() {
        // maxOutputNodes <= 0 — short-circuit, no allocation, return the
        // same list reference.
        List<VectorSegment> picked = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            picked.add(seg(i, 1024L, 100_000));
        }
        List<VectorSegment> result0 = VectorIndexCompactor.capOutputNodes(picked, 0L);
        assertSame(picked, result0);
        List<VectorSegment> resultNeg = VectorIndexCompactor.capOutputNodes(picked, -42L);
        assertSame(picked, resultNeg);
    }

    @Test
    public void capOutputNodesNoOpWhenAlreadyFits() {
        // Total = 5 × 100k = 500k, well under cap of 1M. No truncation, no
        // copy — the helper returns the same reference.
        List<VectorSegment> picked = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            picked.add(seg(i, 1024L, 100_000));
        }
        List<VectorSegment> result = VectorIndexCompactor.capOutputNodes(picked, 1_000_000L);
        assertSame(picked, result);
    }

    @Test
    public void capOutputNodesReturnsCopyWhenTruncating() {
        // When the cap bites, the helper must NOT mutate the caller's list —
        // it must return a fresh ArrayList.
        List<VectorSegment> picked = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            picked.add(seg(i, 1024L, 200_000));
        }
        int sizeBefore = picked.size();
        List<VectorSegment> result = VectorIndexCompactor.capOutputNodes(picked, 600_000L);
        assertEquals("input list must not be mutated", sizeBefore, picked.size());
        assertEquals(3, result.size()); // 3 × 200k = 600k, exactly at cap
        assertNotEquals("must return a fresh list when truncating", picked.size(), result.size());
    }

    // -------------------------------------------------------------------------
    // Micro-segment fast path bypass
    // -------------------------------------------------------------------------

    @Test
    public void microSegmentFastPathIgnoresOutputCap() {
        // 10 micro-segments of 500 live nodes each (5k total). Set
        // maxOutputNodes=100 — far below the micro-segment total. The
        // micro-segment fast path MUST fire anyway and return all 10 inputs;
        // the cap deliberately does not apply (micro-segment merges are
        // already bounded by microSegmentMaxNodes per input).
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            cand.add(seg(i, 1024L * (i + 1), 500));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand,
                /*minCount*/ 2,
                /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*microSegmentMaxNodes*/ 1000L,
                /*maxInputs*/ 0,
                /*targetMaxBytes*/ Long.MAX_VALUE,
                /*maxOutputNodes*/ 100L);
        assertEquals("micro-segment fast path must bypass the output-node cap",
                10, picked.size());
    }

    // -------------------------------------------------------------------------
    // Interaction with the input-count cap (smaller wins)
    // -------------------------------------------------------------------------

    @Test
    public void smallerOfOutputCapAndMaxInputsWins() {
        // 20 segments of 100k live each. With maxInputs=10 only the picker
        // would keep 10 (1M nodes). Add maxOutputNodes=500k → cap forces
        // truncation to 5 inputs.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            cand.add(seg(i, 1024L * (i + 1), 100_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                /*microSegmentMaxNodes*/ 0L, /*maxInputs*/ 10,
                Long.MAX_VALUE, /*maxOutputNodes*/ 500_000L);
        assertEquals(5, picked.size());
    }

    @Test
    public void maxInputsWinsWhenStricterThanOutputCap() {
        // 20 segments of 100k live each. maxOutputNodes=1.2M would allow
        // 12 inputs; maxInputs=3 is stricter and wins.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            cand.add(seg(i, 1024L * (i + 1), 100_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                0L, /*maxInputs*/ 3,
                Long.MAX_VALUE, /*maxOutputNodes*/ 1_200_000L);
        assertEquals(3, picked.size());
    }

    // -------------------------------------------------------------------------
    // Tier scaling (issue #643)
    // -------------------------------------------------------------------------

    @Test
    public void tieredMaxOutputNodesScalesAtThresholds() {
        long base = 1_000_000L;
        // Below the first threshold: no scaling.
        assertEquals(base, VectorIndexCompactor.computeTieredMaxOutputNodes(50, base));
        // 100 ≤ count < 300 → 2×.
        assertEquals(2 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(100, base));
        assertEquals(2 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(299, base));
        // 300 ≤ count < 500 → 4×.
        assertEquals(4 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(300, base));
        assertEquals(4 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(499, base));
        // count ≥ 500 → 8×.
        assertEquals(8 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(500, base));
        assertEquals(8 * base, VectorIndexCompactor.computeTieredMaxOutputNodes(10_000, base));
    }

    @Test
    public void tieredMaxOutputNodesDisabledStaysDisabled() {
        // Base 0 or negative: tier scaling must not turn it on.
        assertEquals(0L, VectorIndexCompactor.computeTieredMaxOutputNodes(50, 0L));
        assertEquals(0L, VectorIndexCompactor.computeTieredMaxOutputNodes(500, 0L));
        assertEquals(-1L, VectorIndexCompactor.computeTieredMaxOutputNodes(500, -1L));
    }

    @Test
    public void tieredMaxOutputNodesOverflowSafe() {
        // base × multiplier would overflow Long.MAX_VALUE → clamp.
        long nearMax = Long.MAX_VALUE / 4;
        // At tier 500 the multiplier is 8 → nearMax × 8 > Long.MAX_VALUE → clamp.
        assertEquals(Long.MAX_VALUE,
                VectorIndexCompactor.computeTieredMaxOutputNodes(500, nearMax));
        // At Long.MAX_VALUE itself (any tier) clamp also fires.
        assertEquals(Long.MAX_VALUE,
                VectorIndexCompactor.computeTieredMaxOutputNodes(100, Long.MAX_VALUE));
    }

    // -------------------------------------------------------------------------
    // Cap is applied AFTER the trigger decision (never changes fire/no-fire)
    // -------------------------------------------------------------------------

    @Test
    public void capDoesNotPreventCycleFromFiring() {
        // Only 1 segment available — below minCount=2 → no fire. The output
        // cap must not change the fire/no-fire decision; the picker still
        // returns empty.
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 1024L, 100_000));
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, 1L, Long.MAX_VALUE, Integer.MAX_VALUE,
                0L, 0, Long.MAX_VALUE, /*maxOutputNodes*/ 10L);
        assertTrue(picked.isEmpty());
    }

    @Test
    public void capLeavesByteTriggerLogicIntact() {
        // 3 segments × 100 MB = 300 MB > minBytes=256 MB → fires. The output
        // cap (50k nodes total) trims to the smallest 1 input — but the
        // "keep at least 2" floor forces 2 inputs to be merged regardless.
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            cand.add(seg(i, 100L * 1024 * 1024, 100_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand,
                /*minCount*/ 2,
                /*minBytes*/ 256L * 1024 * 1024,
                /*maxBytes*/ Long.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*microSegmentMaxNodes*/ 0L,
                /*maxInputs*/ 0,
                /*targetMaxBytes*/ Long.MAX_VALUE,
                /*maxOutputNodes*/ 50_000L);
        assertEquals("must keep ≥ 2 even when first two exceed the output cap",
                2, picked.size());
    }
}
