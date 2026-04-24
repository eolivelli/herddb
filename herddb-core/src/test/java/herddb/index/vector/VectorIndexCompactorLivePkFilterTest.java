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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for {@link VectorIndexCompactor#buildAuthorityMap} — the
 * live-PK filter that drops tombstoned PKs and PKs superseded by
 * later segments or live shards during compaction.
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
        return s;
    }

    private static Bytes pk(String s) {
        return Bytes.from_array(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void tombstonedPksAreExcluded() {
        // ord 1 is tombstoned; it should NOT appear in the authority map
        // for segment A.
        VectorSegment a = seg(10, 5L, "alpha", null, "gamma");
        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(a), Arrays.asList(a), new ArrayList<>());
        assertEquals(Integer.valueOf(10), owners.get(pk("alpha")));
        assertNull(owners.get(pk("beta")));
        assertEquals(Integer.valueOf(10), owners.get(pk("gamma")));
    }

    @Test
    public void laterSegmentSupersedesCandidate() {
        VectorSegment oldA = seg(10, 3L, "shared", "onlyA");
        VectorSegment newB = seg(20, 7L, "shared", "onlyB");

        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(oldA),
                Arrays.asList(oldA, newB),
                new ArrayList<>());

        // shared: newer generation (7) beats candidate (3) — B wins.
        assertEquals(Integer.valueOf(20), owners.get(pk("shared")));
        // onlyA is only in the candidate, no newer source exists.
        assertEquals(Integer.valueOf(10), owners.get(pk("onlyA")));
        // onlyB is in a non-candidate segment, so it's still recorded.
        assertEquals(Integer.valueOf(20), owners.get(pk("onlyB")));
    }

    @Test
    public void liveShardAlwaysDominates() {
        VectorSegment a = seg(10, 3L, "x", "y");
        List<Bytes> liveShardPks = Arrays.asList(pk("x"));

        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(a), Arrays.asList(a), liveShardPks);

        assertEquals(Integer.valueOf(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID),
                owners.get(pk("x")));
        assertEquals(Integer.valueOf(10), owners.get(pk("y")));
    }

    @Test
    public void olderNonCandidateSegmentIsIgnored() {
        // A candidate at gen 5; an older non-candidate at gen 2 has no
        // say over anything.
        VectorSegment cand = seg(10, 5L, "k");
        VectorSegment older = seg(20, 2L, "k", "other");

        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(cand),
                Arrays.asList(cand, older),
                new ArrayList<>());

        // 'k' belongs to the candidate; 'other' from the older
        // non-candidate is NOT added because the older segment is not
        // inspected (candidates cover their own generation and the
        // merged output will replace them).
        assertEquals(Integer.valueOf(10), owners.get(pk("k")));
        assertNull(owners.get(pk("other")));
    }

    @Test
    public void nullPkArraysAreIgnored() {
        // A segment that has never been populated (fresh / pre-load).
        VectorSegment empty = new VectorSegment(99);
        empty.generation = 7L;
        // pkData/offsets/lengths are null.
        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(empty), Arrays.asList(empty), new ArrayList<>());
        assertTrue(owners.isEmpty());
    }

    @Test
    public void simpleCandidateHappyPath() {
        VectorSegment only = seg(1, 1L, "a", "b", "c");
        Map<Bytes, Integer> owners = VectorIndexCompactor.buildAuthorityMap(
                Arrays.asList(only), Arrays.asList(only), new ArrayList<>());
        assertEquals(3, owners.size());
        assertFalse(owners.containsValue(VectorIndexCompactor.LIVE_SHARD_SEGMENT_ID));
    }
}
