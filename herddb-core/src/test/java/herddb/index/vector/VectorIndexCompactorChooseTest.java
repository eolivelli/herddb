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
}
