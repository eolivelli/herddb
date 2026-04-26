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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/**
 * Stress test for the BLink-backed {@link CompactionAuthorityMap} introduced
 * for issue #290. Builds an authority map across hundreds of segments and
 * thousands of PKs with a tiny page-replacement budget so the BLink
 * <em>must</em> spill cold pages to disk; verifies that:
 *
 * <ul>
 *   <li>every candidate PK with no newer source resolves to its own
 *       candidate segment id;</li>
 *   <li>every candidate PK that is also present in a higher-generation
 *       non-candidate segment resolves to the highest-generation winner;</li>
 *   <li>every candidate PK present in a live shard resolves to
 *       {@link VectorIndexCompactor#LIVE_SHARD_SEGMENT_ID};</li>
 *   <li>candidate-PK count materialised in the BLink is bounded by the
 *       sum of candidate sizes (NOT by the total PK count across all
 *       segments) — this is the core memory-bound invariant of the fix.</li>
 * </ul>
 */
public class CompactionAuthorityMapBLinkTest {

    private static Bytes pk(int index) {
        // PKs as small UTF-8 strings so the BLink's variable-key path is exercised.
        return Bytes.from_array(("pk_" + index).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Build a {@link VectorSegment} with sequential PKs, a generation, and
     * an in-memory BLink-backed {@code onDiskPkToNode} so the
     * supersession-check path can find the PKs.
     */
    private static VectorSegment makeSegment(int segmentId, long generation,
                                             int pkOffset, int pkCount) {
        VectorSegment s = new VectorSegment(segmentId);
        s.generation = generation;
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        int[] offsets = new int[pkCount];
        int[] lengths = new int[pkCount];
        s.onDiskPkToNode = TestBLinks.inMemoryPkToNode();
        for (int i = 0; i < pkCount; i++) {
            byte[] raw = ("pk_" + (pkOffset + i)).getBytes(StandardCharsets.UTF_8);
            offsets[i] = bos.size();
            lengths[i] = raw.length;
            bos.write(raw, 0, raw.length);
            s.onDiskPkToNode.insert(Bytes.from_array(raw), (long) i);
        }
        s.pkData = bos.toByteArray();
        s.pkOffsets = offsets;
        s.pkLengths = lengths;
        return s;
    }

    @Test
    public void candidateOnlyPksAreAllAuthoritative() throws IOException {
        // 5 candidate segments × 1k PKs = 5000 candidate PKs, plus 50 newer
        // non-candidate segments × 5k PKs = 250 000 non-candidate PKs.
        // The fix must keep the authority map bounded to ~5000 entries.
        List<VectorSegment> candidates = new ArrayList<>();
        int pkCounter = 0;
        for (int s = 0; s < 5; s++) {
            candidates.add(makeSegment(/*id*/ 100 + s, /*gen*/ 1L + s,
                    pkCounter, 1000));
            pkCounter += 1000;
        }
        List<VectorSegment> newer = new ArrayList<>();
        for (int s = 0; s < 50; s++) {
            // Disjoint PK ranges so no candidate is superseded by a non-candidate.
            newer.add(makeSegment(/*id*/ 1000 + s, /*gen*/ 100L + s,
                    pkCounter, 5000));
            pkCounter += 5000;
        }
        List<VectorSegment> all = new ArrayList<>(candidates);
        all.addAll(newer);

        // Tiny page-replacement budget so the BLink must spill.
        try (CompactionAuthorityMap authority =
                     TestBLinks.inMemoryCompactionAuthorityMap(/*pageSize*/ 1024L,
                             /*policyCapacity*/ 4)) {
            VectorIndexCompactor.buildAuthorityMap(authority, candidates, all,
                    Collections.emptyList());
            assertEquals("authority map size must equal sum(candidate PKs)",
                    5000L, authority.size());
            // Every candidate PK resolves to its own candidate segment id.
            int verified = 0;
            for (int s = 0; s < 5; s++) {
                int expectedId = 100 + s;
                int base = s * 1000;
                for (int i = 0; i < 1000; i += 17) {
                    Integer owner = authority.getSegmentId(pk(base + i));
                    assertEquals("PK pk_" + (base + i),
                            Integer.valueOf(expectedId), owner);
                    verified++;
                }
            }
            assertTrue("verified at least 250 PKs", verified > 250);
            // PKs from non-candidate segments are NOT recorded.
            assertNull(authority.getSegmentId(pk(pkCounter - 1)));
        }
    }

    @Test
    public void newerSegmentsSupersedeCandidatePks() throws IOException {
        // 4 candidate segments at generations 1..4, all sharing the same
        // PK range [0, 500). 3 newer non-candidate segments at generations
        // 10, 20, 30 also share the PK range. The highest-generation
        // newer segment must win for every shared PK.
        List<VectorSegment> candidates = new ArrayList<>();
        for (int s = 0; s < 4; s++) {
            candidates.add(makeSegment(/*id*/ 10 + s, /*gen*/ 1L + s, 0, 500));
        }
        List<VectorSegment> newer = new ArrayList<>();
        newer.add(makeSegment(/*id*/ 100, /*gen*/ 10L, 0, 500));
        newer.add(makeSegment(/*id*/ 200, /*gen*/ 20L, 0, 500));
        newer.add(makeSegment(/*id*/ 300, /*gen*/ 30L, 0, 500));
        List<VectorSegment> all = new ArrayList<>(candidates);
        all.addAll(newer);

        try (CompactionAuthorityMap authority =
                     TestBLinks.inMemoryCompactionAuthorityMap(/*pageSize*/ 1024L,
                             /*policyCapacity*/ 4)) {
            VectorIndexCompactor.buildAuthorityMap(authority, candidates, all,
                    Collections.emptyList());
            // Every PK in [0, 500) is owned by segment 300 (highest gen).
            for (int i = 0; i < 500; i += 13) {
                assertEquals("PK pk_" + i, Integer.valueOf(300),
                        authority.getSegmentId(pk(i)));
            }
        }
    }

    @Test
    public void liveShardOverridesEverything() throws IOException {
        // 3 candidate segments × 200 PKs in disjoint ranges. Half the PKs
        // of each candidate are also in the live shards (which always win).
        List<VectorSegment> candidates = new ArrayList<>();
        candidates.add(makeSegment(/*id*/ 1, /*gen*/ 1L, /*pkOffset*/ 0, 200));
        candidates.add(makeSegment(/*id*/ 2, /*gen*/ 2L, /*pkOffset*/ 200, 200));
        candidates.add(makeSegment(/*id*/ 3, /*gen*/ 3L, /*pkOffset*/ 400, 200));
        List<VectorSegment> all = new ArrayList<>(candidates);
        Set<Bytes> liveShard = new HashSet<>();
        for (int i = 0; i < 600; i += 2) {
            liveShard.add(pk(i));
        }

        try (CompactionAuthorityMap authority =
                     TestBLinks.inMemoryCompactionAuthorityMap(/*pageSize*/ 1024L,
                             /*policyCapacity*/ 4)) {
            VectorIndexCompactor.buildAuthorityMap(authority, candidates, all, liveShard);
            for (int i = 0; i < 600; i++) {
                Integer owner = authority.getSegmentId(pk(i));
                if (i % 2 == 0) {
                    assertEquals("even PKs go to live shard, pk_" + i,
                            Integer.valueOf(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID),
                            owner);
                } else {
                    int expectedId = 1 + i / 200;
                    assertEquals("odd PKs stay with their candidate, pk_" + i,
                            Integer.valueOf(expectedId), owner);
                }
            }
        }
    }

    @Test
    public void closedAuthorityMapDropsTempStorage() throws IOException {
        // The cleanup callback must run exactly once on close, even when
        // the BLink has spilled to disk. We verify by counting the number
        // of times the on-drop runnable fires.
        int[] onDropCount = new int[1];
        herddb.index.blink.BLink<Bytes, Long> blink =
                TestBLinks.inMemoryPkToNode();
        try (CompactionAuthorityMap authority =
                     new CompactionAuthorityMap(blink, () -> onDropCount[0]++)) {
            for (int i = 0; i < 100; i++) {
                authority.updateIfHigherGeneration(pk(i), i + 1L, 42);
            }
            assertEquals(100L, authority.size());
        }
        assertEquals("on-drop callback fired exactly once", 1, onDropCount[0]);
    }

    @Test
    public void supersessionWithMissingOnDiskPkToNodeIsSafe() throws IOException {
        // Defensive: a newer segment whose onDiskPkToNode field is null
        // (e.g. mid-load on startup) must not blow up the authority build.
        VectorSegment cand = makeSegment(/*id*/ 1, /*gen*/ 1L, 0, 10);
        VectorSegment newerWithoutBlink = makeSegment(/*id*/ 2, /*gen*/ 5L, 0, 10);
        newerWithoutBlink.onDiskPkToNode = null;

        try (CompactionAuthorityMap authority =
                     TestBLinks.inMemoryCompactionAuthorityMap()) {
            VectorIndexCompactor.buildAuthorityMap(authority,
                    Arrays.asList(cand),
                    Arrays.asList(cand, newerWithoutBlink),
                    Collections.emptyList());
            // The newer segment is skipped silently — every candidate PK
            // remains attributed to its own segment.
            for (int i = 0; i < 10; i++) {
                assertEquals(Integer.valueOf(1), authority.getSegmentId(pk(i)));
            }
        }
    }
}
