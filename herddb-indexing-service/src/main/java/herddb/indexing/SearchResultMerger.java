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

import herddb.utils.Bytes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges per-instance vector-search responses into a single top-K list,
 * deduplicating by primary key.
 *
 * <p>Why dedup matters under segmented-v2 ownership transfer: there is a
 * short window during {@code TRANSFERRING} when both the previous owner and
 * the new owner have the segment open and may serve the same PK. Without
 * dedup, the merged top-K returns duplicate entries and the caller is forced
 * to filter (or worse, the duplicate occupies a slot that should have gone
 * to the next-best result).
 *
 * <p>Tie-break (review item A7 — explicit determinism): when the same PK
 * appears in multiple sub-results with different scores, the higher score
 * wins. When two distinct PKs have IDENTICAL scores, output order is
 * stable: PKs are sorted by (descending score, ascending PK lex order).
 * Pool iteration order does NOT influence the output, so a request that
 * fans out to instances in different orderings (e.g. after a ZK reconnect
 * shuffles the discovery list) returns the same top-K.
 *
 * <p>This class is a pure function and stateless; safe to call from any
 * thread.
 */
public final class SearchResultMerger {

    private SearchResultMerger() {
    }

    /**
     * Merges the supplied per-instance result lists into a single top-K list
     * sorted by descending score, deduping by primary key.
     *
     * @param perInstanceResults each inner list is one server's response; PK→score entries.
     * @param limit              top-K cap. Must be positive.
     * @return at most {@code limit} entries, sorted descending by score.
     */
    public static List<Map.Entry<Bytes, Float>> merge(
            Collection<? extends List<? extends Map.Entry<Bytes, Float>>> perInstanceResults,
            int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        if (perInstanceResults == null || perInstanceResults.isEmpty()) {
            return new ArrayList<>();
        }

        // PK → max-score-seen. HashMap is fine: the merge happens on the client
        // path which is already off the per-segment hot path.
        Map<Bytes, Float> best = new HashMap<>();
        for (List<? extends Map.Entry<Bytes, Float>> response : perInstanceResults) {
            if (response == null) {
                continue;
            }
            for (Map.Entry<Bytes, Float> entry : response) {
                Bytes pk = entry.getKey();
                Float existing = best.get(pk);
                Float score = entry.getValue();
                if (score == null) {
                    score = 0f;
                }
                if (existing == null || score > existing) {
                    best.put(pk, score);
                }
            }
        }

        // Top-K by descending score, ties broken by ascending PK lex order so the
        // output is fully deterministic across requests regardless of pool iteration
        // order (review item A7). For our typical N=10..100 this comparator-based
        // sort is fine; for truly huge K a partial sort would be cheaper but is
        // overkill here.
        //
        // Hot-path note: this comparator runs on the client search-result merge path,
        // O(N log N) where N is the deduped result count (typically < 1000). The
        // tie-break uses Bytes.compareTo which is a single memcmp. Net cost is
        // dominated by the gRPC call itself; the extra ordering work is sub-µs.
        List<Map.Entry<Bytes, Float>> sorted = new ArrayList<>(best.size());
        for (Map.Entry<Bytes, Float> e : best.entrySet()) {
            sorted.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue()));
        }
        sorted.sort((a, b) -> {
            int byScore = Float.compare(b.getValue(), a.getValue()); // descending
            if (byScore != 0) {
                return byScore;
            }
            return a.getKey().compareTo(b.getKey()); // ascending PK lex order
        });
        if (sorted.size() > limit) {
            return new ArrayList<>(sorted.subList(0, limit));
        }
        return sorted;
    }
}
