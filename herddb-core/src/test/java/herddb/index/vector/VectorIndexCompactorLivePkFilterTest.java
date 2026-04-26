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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

/**
 * Tests for {@link VectorIndexCompactor#buildAuthorityMap} — the
 * live-PK filter that drops tombstoned PKs and PKs superseded by
 * later segments or live shards during compaction.
 *
 * <p>After issue #290 the authority map is intentionally
 * <em>candidate-only</em>: only PKs that appear in at least one
 * candidate are recorded. PKs that exist exclusively in newer
 * non-candidate segments or in live shards are not tracked because
 * they are never queried during the rebuild (the rebuild only
 * consults the map for candidate PKs).
 */
public class VectorIndexCompactorLivePkFilterTest {

    /** Populate a segment with PKs at a given generation. Tombstoned
     * ords get {@code offsets[ord] = -1}. */
    private static VectorSegment seg(int id, long generation, String... pks) {
        VectorSegment s = new VectorSegment(id);
        s.generation = generation;
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        int[] offsets = new int[pks.length];
        int[] lengths = new int[pks.length];
        for (int i = 0; i < pks.length; i++) {
            if (pks[i] == null) {
                offsets[i] = -1;
                lengths[i] = 0;
                continue;
            }
            byte[] raw = pks[i].getBytes(StandardCharsets.UTF_8);
            offsets[i] = bos.size();
            lengths[i] = raw.length;
            bos.write(raw, 0, raw.length);
        }
        s.pkData = bos.toByteArray();
        s.pkOffsets = offsets;
        s.pkLengths = lengths;
        // Wire an in-memory BLink as the pkToNode lookup so the
        // BLink-driven supersession check (issue #290) finds non-tombstoned
        // PKs. Tests that omit this rely on supersession via candidates only.
        s.onDiskPkToNode = TestBLinks.inMemoryPkToNode();
        for (int i = 0; i < pks.length; i++) {
            if (pks[i] != null) {
                s.onDiskPkToNode.insert(pk(pks[i]), (long) i);
            }
        }
        return s;
    }

    private static Bytes pk(String s) {
        return Bytes.from_array(s.getBytes(StandardCharsets.UTF_8));
    }

    private static CompactionAuthorityMap newAuthority() {
        return TestBLinks.inMemoryCompactionAuthorityMap();
    }

    @Test
    public void tombstonedPksAreExcluded() throws IOException {
        // ord 1 is tombstoned; it should NOT appear in the authority map
        // for segment A.
        VectorSegment a = seg(10, 5L, "alpha", null, "gamma");
        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(a), Arrays.asList(a), new ArrayList<>());
            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("alpha")));
            assertNull(owners.getSegmentId(pk("beta")));
            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("gamma")));
        }
    }

    @Test
    public void laterSegmentSupersedesCandidate() throws IOException {
        VectorSegment oldA = seg(10, 3L, "shared", "onlyA");
        VectorSegment newB = seg(20, 7L, "shared", "onlyB");

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(oldA),
                    Arrays.asList(oldA, newB),
                    new ArrayList<>());

            // shared: newer generation (7) beats candidate (3) — B wins.
            assertEquals(Integer.valueOf(20), owners.getSegmentId(pk("shared")));
            // onlyA is only in the candidate, no newer source exists.
            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("onlyA")));
            // onlyB is in a non-candidate segment — after issue #290 the
            // authority map is candidate-only, so it is NOT recorded.
            assertNull(owners.getSegmentId(pk("onlyB")));
        }
    }

    @Test
    public void liveShardAlwaysDominates() throws IOException {
        VectorSegment a = seg(10, 3L, "x", "y");
        List<Bytes> liveShardPks = Arrays.asList(pk("x"));

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(a), Arrays.asList(a), liveShardPks);

            assertEquals(Integer.valueOf(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID),
                    owners.getSegmentId(pk("x")));
            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("y")));
        }
    }

    @Test
    public void liveShardPkNotInCandidatesIsIgnored() throws IOException {
        // Live-shard PK that is NOT in any candidate is ignored — the
        // authority map only tracks PKs that are candidates for re-insert
        // by the rebuild.
        VectorSegment a = seg(10, 3L, "x");
        List<Bytes> liveShardPks = Arrays.asList(pk("foreign"));

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(a), Arrays.asList(a), liveShardPks);

            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("x")));
            assertNull(owners.getSegmentId(pk("foreign")));
        }
    }

    @Test
    public void olderNonCandidateSegmentIsIgnored() throws IOException {
        // A candidate at gen 5; an older non-candidate at gen 2 has no
        // say over anything.
        VectorSegment cand = seg(10, 5L, "k");
        VectorSegment older = seg(20, 2L, "k", "other");

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(cand),
                    Arrays.asList(cand, older),
                    new ArrayList<>());

            // 'k' belongs to the candidate; 'other' from the older
            // non-candidate is NOT added because the older segment is not
            // inspected (candidates cover their own generation and the
            // merged output will replace them).
            assertEquals(Integer.valueOf(10), owners.getSegmentId(pk("k")));
            assertNull(owners.getSegmentId(pk("other")));
        }
    }

    @Test
    public void nullPkArraysAreIgnored() throws IOException {
        // A segment that has never been populated (fresh / pre-load).
        VectorSegment empty = new VectorSegment(99);
        empty.generation = 7L;
        // pkData/offsets/lengths are null.
        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(empty), Arrays.asList(empty), new ArrayList<>());
            assertEquals(0L, owners.size());
        }
    }

    @Test
    public void simpleCandidateHappyPath() throws IOException {
        VectorSegment only = seg(1, 1L, "a", "b", "c");
        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(only), Arrays.asList(only), new ArrayList<>());
            assertEquals(3L, owners.size());
            assertEquals(Integer.valueOf(1), owners.getSegmentId(pk("a")));
            assertEquals(Integer.valueOf(1), owners.getSegmentId(pk("b")));
            assertEquals(Integer.valueOf(1), owners.getSegmentId(pk("c")));
            assertFalse(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID
                    == owners.getSegmentId(pk("a")));
        }
    }

    @Test
    public void supersessionFindsHighestGenerationWinner() throws IOException {
        // Three sources hold the same PK. The candidate is the lowest-gen
        // source; among the newer non-candidates, the highest-gen wins.
        VectorSegment cand = seg(10, 1L, "shared");
        VectorSegment mid = seg(20, 3L, "shared");
        VectorSegment top = seg(30, 5L, "shared");

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(cand),
                    Arrays.asList(cand, mid, top),
                    new ArrayList<>());

            assertEquals(Integer.valueOf(30), owners.getSegmentId(pk("shared")));
        }
    }

    @Test
    public void liveShardBeatsNewerSegment() throws IOException {
        // Live shard always wins, even over a newer non-candidate segment
        // that also has the PK.
        VectorSegment cand = seg(10, 1L, "shared");
        VectorSegment newer = seg(20, 5L, "shared");

        try (CompactionAuthorityMap owners = newAuthority()) {
            VectorIndexCompactor.buildAuthorityMap(owners,
                    Arrays.asList(cand),
                    Arrays.asList(cand, newer),
                    Arrays.asList(pk("shared")));

            assertEquals(Integer.valueOf(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID),
                    owners.getSegmentId(pk("shared")));
        }
    }

    @Test
    public void encoderRoundTripsLiveShardSegmentId() {
        // Defensive: the (gen, segId) -> Long packing must round-trip
        // negative segment ids (LIVE_SHARD_SEGMENT_ID = -1).
        long encoded = CompactionAuthorityMap.encode(42L, VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID);
        assertEquals(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID,
                CompactionAuthorityMap.decodeSegmentId(encoded));
        assertEquals(42L, CompactionAuthorityMap.decodeGeneration(encoded));
        // And ordinary positive ids.
        encoded = CompactionAuthorityMap.encode(7L, 12345);
        assertEquals(12345, CompactionAuthorityMap.decodeSegmentId(encoded));
        assertEquals(7L, CompactionAuthorityMap.decodeGeneration(encoded));
    }

    @Test
    public void encoderRejectsOutOfRangeGeneration() {
        // Defensive: generation must fit in the high 32 bits (0..2^32-1).
        try {
            CompactionAuthorityMap.encode(-1L, 0);
            org.junit.Assert.fail("expected IllegalArgumentException for negative generation");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("generation"));
        }
        try {
            CompactionAuthorityMap.encode(0x1_0000_0000L, 0);
            org.junit.Assert.fail("expected IllegalArgumentException for over-range generation");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("generation"));
        }
    }
}
