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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SearchResultMergerTest {

    private static Map.Entry<Bytes, Float> entry(int pk, float score) {
        return new AbstractMap.SimpleImmutableEntry<>(Bytes.from_int(pk), score);
    }

    @Test
    public void emptyInputReturnsEmpty() {
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(
                Collections.emptyList(), 10);
        assertTrue(out.isEmpty());
    }

    @Test
    public void singleResponsePassThroughSortedByScore() {
        List<Map.Entry<Bytes, Float>> response = Arrays.asList(
                entry(1, 0.5f), entry(2, 0.9f), entry(3, 0.1f));
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(
                Collections.singletonList(response), 10);
        assertEquals(3, out.size());
        assertEquals(Bytes.from_int(2), out.get(0).getKey());
        assertEquals(0.9f, out.get(0).getValue(), 0f);
        assertEquals(Bytes.from_int(1), out.get(1).getKey());
        assertEquals(Bytes.from_int(3), out.get(2).getKey());
    }

    @Test
    public void duplicatePkAcrossPoolsKeepsHighestScore() {
        // Simulates the segmented-v2 ownership-transfer overlap: PK 42 served by
        // both old and new owner with different scores.
        List<Map.Entry<Bytes, Float>> poolA = Arrays.asList(entry(42, 0.7f), entry(99, 0.3f));
        List<Map.Entry<Bytes, Float>> poolB = Arrays.asList(entry(42, 0.95f), entry(7, 0.5f));

        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(Arrays.asList(poolA, poolB), 10);
        assertEquals(3, out.size());
        // Top result is PK 42 with the higher score (0.95).
        assertEquals(Bytes.from_int(42), out.get(0).getKey());
        assertEquals(0.95f, out.get(0).getValue(), 0f);
        // PK 42 must NOT appear twice.
        long countPk42 = out.stream().filter(e -> e.getKey().equals(Bytes.from_int(42))).count();
        assertEquals(1, countPk42);
    }

    @Test
    public void identicalScoresKeepFirstObservation() {
        // PK 5 in both pools with the SAME score; no duplicate in output.
        List<Map.Entry<Bytes, Float>> poolA = Arrays.asList(entry(5, 0.5f));
        List<Map.Entry<Bytes, Float>> poolB = Arrays.asList(entry(5, 0.5f));
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(Arrays.asList(poolA, poolB), 10);
        assertEquals(1, out.size());
        assertEquals(Bytes.from_int(5), out.get(0).getKey());
    }

    @Test
    public void truncatesToLimit() {
        List<Map.Entry<Bytes, Float>> response = Arrays.asList(
                entry(1, 0.1f), entry(2, 0.2f), entry(3, 0.3f), entry(4, 0.4f), entry(5, 0.5f));
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(
                Collections.singletonList(response), /* limit= */ 2);
        assertEquals(2, out.size());
        assertEquals(Bytes.from_int(5), out.get(0).getKey());
        assertEquals(Bytes.from_int(4), out.get(1).getKey());
    }

    @Test
    public void manyPoolsManyDuplicatesScalesCleanly() {
        // 5 pools, each returning the same 100 PKs with progressively decreasing scores.
        // The merger must collapse these to 100 unique PKs and respect limit.
        java.util.List<List<Map.Entry<Bytes, Float>>> pools = new java.util.ArrayList<>();
        for (int p = 0; p < 5; p++) {
            java.util.List<Map.Entry<Bytes, Float>> response = new java.util.ArrayList<>();
            for (int k = 0; k < 100; k++) {
                response.add(entry(k, 1.0f - 0.001f * (k + p * 1000)));
            }
            pools.add(response);
        }
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(pools, 50);
        assertEquals(50, out.size());
        // No duplicate PKs.
        java.util.Set<Bytes> distinct = new java.util.HashSet<>();
        for (Map.Entry<Bytes, Float> e : out) {
            distinct.add(e.getKey());
        }
        assertEquals(50, distinct.size());
        // Scores must be sorted descending.
        for (int i = 1; i < out.size(); i++) {
            assertTrue("scores must be descending; idx=" + i,
                    out.get(i - 1).getValue() >= out.get(i).getValue());
        }
    }

    @Test
    public void nullPoolEntriesAreSkipped() {
        // A pool that returned nothing (e.g., no segments owned by that instance)
        // is represented as null in the input list and must not break the merger.
        List<Map.Entry<Bytes, Float>> populated = Arrays.asList(entry(1, 0.5f));
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(
                Arrays.asList(null, populated, null), 10);
        assertEquals(1, out.size());
        assertEquals(Bytes.from_int(1), out.get(0).getKey());
    }

    @Test
    public void rejectsNonPositiveLimit() {
        try {
            SearchResultMerger.merge(Collections.emptyList(), 0);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("limit"));
        }
        try {
            SearchResultMerger.merge(Collections.emptyList(), -5);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ok) {
            assertTrue(ok.getMessage().contains("limit"));
        }
    }

    @Test
    public void nullScoreTreatedAsZero() {
        // Server-side might omit scores for legacy responses; merger must not NPE.
        List<Map.Entry<Bytes, Float>> response = Arrays.asList(
                new AbstractMap.SimpleImmutableEntry<>(Bytes.from_int(1), null),
                entry(2, 0.5f));
        List<Map.Entry<Bytes, Float>> out = SearchResultMerger.merge(
                Collections.singletonList(response), 10);
        assertEquals(2, out.size());
        // PK 2 has higher score → comes first.
        assertEquals(Bytes.from_int(2), out.get(0).getKey());
    }
}
