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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for {@link PersistentVectorStore#chooseSegmentsToDemote} — the
 * policy that keeps on-disk segment count bounded by demoting the smallest
 * sealed segments back into the mergeable pool.
 */
public class PersistentVectorStoreMergePolicyTest {

    private static VectorSegment seg(int id, long sizeBytes) {
        VectorSegment s = new VectorSegment(id);
        s.estimatedSizeBytes = sizeBytes;
        return s;
    }

    @Test
    public void belowThresholdYieldsNoDemotions() {
        List<VectorSegment> sealed = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            sealed.add(seg(i, 1_000_000L * (i + 1)));
        }
        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(sealed, demotions, 10, 3);
        assertTrue("no demotions when below threshold", demotions.isEmpty());
    }

    @Test
    public void atThresholdExactlyYieldsNoDemotions() {
        List<VectorSegment> sealed = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sealed.add(seg(i, 1_000_000L));
        }
        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(sealed, demotions, 10, 3);
        assertTrue("no demotions at exactly the threshold", demotions.isEmpty());
    }

    @Test
    public void aboveThresholdDemotesSmallestFirst() {
        // 12 segments, threshold 10: demote the 3 smallest.
        List<VectorSegment> sealed = new ArrayList<>();
        sealed.add(seg(0, 9_000_000L));
        sealed.add(seg(1, 3_000_000L));
        sealed.add(seg(2, 7_000_000L));
        sealed.add(seg(3, 1_000_000L)); // smallest
        sealed.add(seg(4, 8_000_000L));
        sealed.add(seg(5, 2_000_000L)); // 2nd smallest
        sealed.add(seg(6, 5_000_000L));
        sealed.add(seg(7, 6_000_000L));
        sealed.add(seg(8, 4_000_000L)); // 3rd smallest
        sealed.add(seg(9, 10_000_000L));
        sealed.add(seg(10, 11_000_000L));
        sealed.add(seg(11, 12_000_000L));

        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(sealed, demotions, 10, 3);
        assertEquals(3, demotions.size());
        // Verify they are indeed the smallest.
        // With sizes {3:1M, 5:2M, 1:3M, 8:4M, ...}, the 3 smallest are 3, 5, 1.
        assertTrue("seg3 (smallest) must be demoted",
                demotions.stream().anyMatch(s -> s.segmentId == 3));
        assertTrue("seg5 must be demoted",
                demotions.stream().anyMatch(s -> s.segmentId == 5));
        assertTrue("seg1 must be demoted",
                demotions.stream().anyMatch(s -> s.segmentId == 1));
        // The biggest must NOT be demoted.
        assertFalse("seg11 must not be demoted",
                demotions.stream().anyMatch(s -> s.segmentId == 11));
    }

    @Test
    public void batchIsCapAtListSize() {
        // 6 sealed, threshold 2, batch 100 → would pick all 6, but should not
        // overflow.
        List<VectorSegment> sealed = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            sealed.add(seg(i, 1_000L * i));
        }
        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(sealed, demotions, 2, 100);
        assertEquals("demotions capped at number of sealed", 6, demotions.size());
    }

    @Test
    public void disablingMergeViaLargeThresholdDisablesPolicy() {
        List<VectorSegment> sealed = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            sealed.add(seg(i, 1L));
        }
        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(
                sealed, demotions, Integer.MAX_VALUE, 4);
        assertTrue("max threshold disables demotion", demotions.isEmpty());
    }

    @Test
    public void smallestSegmentSetIsDeterministic() {
        // Multiple segments with equal size: policy must still demote a
        // deterministic subset and never pick more than `batch`.
        List<VectorSegment> sealed = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            sealed.add(seg(i, 1_000_000L)); // all equal
        }
        List<VectorSegment> demotions = new ArrayList<>();
        PersistentVectorStore.chooseSegmentsToDemote(sealed, demotions, 10, 4);
        assertEquals(4, demotions.size());
    }

    @Test
    public void builtInConstantsAreSane() {
        assertTrue("threshold must be at least 2",
                PersistentVectorStore.SEGMENT_MERGE_THRESHOLD >= 2);
        assertTrue("batch must be at least 2",
                PersistentVectorStore.SEGMENT_MERGE_BATCH >= 2);
    }
}
