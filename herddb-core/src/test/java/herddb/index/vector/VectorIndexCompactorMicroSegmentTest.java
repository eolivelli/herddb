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
 * Policy tests for the micro-segment fast path of
 * {@link VectorIndexCompactor#chooseSegmentsToMerge} (issue #570).
 *
 * <p>A micro-segment is an on-disk segment whose live-node count is below the
 * configured {@code microSegmentMaxNodes} threshold. When a compaction cycle
 * <em>is going to fire anyway</em> (one of the byte / count triggers is
 * satisfied) and at least two micro-segments are present, the scheduler must
 * merge <em>only</em> the micro-segments — a cheap, fast cycle that reclaims
 * segment-count slots quickly — deferring the larger segments to a later
 * cycle. The fast path must never cause a cycle to compact when the normal
 * triggers would not.
 */
public class VectorIndexCompactorMicroSegmentTest {

    private static final long MICRO_MAX_NODES = 1000L;
    private static final long MB = 1024L * 1024L;

    /** Builds a segment with the given id, byte size and live-node count. */
    private static VectorSegment seg(int id, long sizeBytes, int liveNodes) {
        VectorSegment s = new VectorSegment(id);
        s.estimatedSizeBytes = sizeBytes;
        s.liveCount.set(liveNodes);
        return s;
    }

    private static List<Integer> ids(List<VectorSegment> segs) {
        List<Integer> out = new ArrayList<>(segs.size());
        for (VectorSegment s : segs) {
            out.add(s.segmentId);
        }
        return out;
    }

    /**
     * Headline scenario: a heap of large segments plus several 3-node
     * micro-segments. The byte trigger fires (the large segments push the
     * total over {@code minBytes}); without the fast path the byte-capped
     * selection would include large segments and take minutes. The fast
     * path must instead return only the micro-segments.
     */
    @Test
    public void microSegmentsMergedFirstWhenCompactionFires() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            cand.add(seg(i, 300L * MB, 100_000)); // large, graduated segments
        }
        for (int i = 10; i < 14; i++) {
            cand.add(seg(i, 4096L, 3)); // micro-segments
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 256L * MB,
                /*maxBytes*/ 1024L * MB, /*maxCount*/ 200,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertEquals("only the 4 micro-segments must be picked", 4, picked.size());
        for (VectorSegment s : picked) {
            assertTrue("picked segment must be a micro-segment, got id=" + s.segmentId
                    + " nodes=" + s.size(), s.size() < MICRO_MAX_NODES);
        }
    }

    /**
     * The fast path must NOT make a cycle compact when neither the byte nor
     * the count trigger fires — it only re-prioritises a cycle that is
     * already going to run.
     */
    @Test
    public void noCompactionWhenTriggersDoNotFire() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            cand.add(seg(i, 64L, 3)); // 6 tiny micro-segments
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1024L * MB,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 1000,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertTrue("no trigger fired → no compaction, even with micro-segments",
                picked.isEmpty());
    }

    /** Picked micro-segments are ordered smallest-bytes-first. */
    @Test
    public void microSegmentsReturnedSmallestBytesFirst() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 9000L, 5));
        cand.add(seg(2, 1000L, 5));
        cand.add(seg(3, 5000L, 5));
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 200,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertEquals(List.of(2, 3, 1), ids(picked));
    }

    /**
     * The fast path drains <em>all</em> micro-segments in a single cycle
     * (bounded only by the byte cap, not by {@code maxCount}) — this is what
     * collapses a backlog of micro-segments in one cheap pass.
     */
    @Test
    public void allMicroSegmentsDrainedInOneCycle() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            cand.add(seg(i, 100L + i, 3));
        }
        // Byte threshold unreachable; the issue #285 count trigger fires.
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 1024L * MB,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 10,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertEquals("all 20 micro-segments must be merged in one cycle",
                20, picked.size());
    }

    /**
     * With exactly one micro-segment the fast path cannot form a merge (it
     * needs two inputs), so the cycle falls back to the normal byte-capped
     * selection — which includes the larger segments.
     */
    @Test
    public void singleMicroSegmentYieldsNormalSelection() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(99, 64L, 3)); // the only micro-segment
        for (int i = 0; i < 4; i++) {
            cand.add(seg(i, 50L * MB, 50_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 100L * MB,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 200,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertEquals("normal byte-capped selection includes every candidate",
                5, picked.size());
    }

    /** With no micro-segments at all the normal policy is used unchanged. */
    @Test
    public void noMicroSegmentsUsesNormalPolicy() {
        List<VectorSegment> cand = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cand.add(seg(i, 50L * MB, 50_000));
        }
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 200L * MB,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 100,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertEquals("normal byte trigger fires with all 5 segments", 5, picked.size());
    }

    /**
     * {@code microSegmentMaxNodes == 0} disables the fast path entirely: the
     * cycle then merges the byte-capped selection (large segments included),
     * and the 6-arg overload behaves identically to the legacy 5-arg one.
     */
    @Test
    public void zeroThresholdDisablesFastPath() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 64L, 3));        // micro
        cand.add(seg(2, 64L, 3));        // micro
        for (int i = 3; i < 6; i++) {
            cand.add(seg(i, 100L * MB, 100_000)); // large
        }
        List<VectorSegment> disabled = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 2, /*minBytes*/ 200L * MB,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 200,
                /*microSegmentMaxNodes*/ 0L);
        assertEquals("disabled fast path → normal selection of all 5 segments",
                5, disabled.size());
        // The legacy 5-arg overload must produce the same result.
        List<VectorSegment> legacy = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 200L * MB, Long.MAX_VALUE, 200);
        assertEquals(ids(disabled), ids(legacy));
        // Sanity: with the fast path enabled the same input merges only the
        // two micro-segments instead.
        List<VectorSegment> enabled = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, 2, 200L * MB, Long.MAX_VALUE, 200, MICRO_MAX_NODES);
        assertEquals(2, enabled.size());
    }

    /**
     * The fast path is still gated by the overall {@code minCount} guard: a
     * candidate set smaller than {@code minCount} yields no compaction even
     * if it contains two micro-segments.
     */
    @Test
    public void fastPathHonoursMinCountGuard() {
        List<VectorSegment> cand = new ArrayList<>();
        cand.add(seg(1, 64L, 3));
        cand.add(seg(2, 64L, 3));
        List<VectorSegment> picked = VectorIndexCompactor.chooseSegmentsToMerge(
                cand, /*minCount*/ 4, /*minBytes*/ 1L,
                /*maxBytes*/ Long.MAX_VALUE, /*maxCount*/ 200,
                /*microSegmentMaxNodes*/ MICRO_MAX_NODES);
        assertTrue("below minCount → no compaction at all", picked.isEmpty());
    }
}
