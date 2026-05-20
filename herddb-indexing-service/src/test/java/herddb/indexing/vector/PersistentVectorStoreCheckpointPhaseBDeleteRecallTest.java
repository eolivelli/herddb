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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #614: locks the two invariants the issue author
 * explicitly asked us to guard when changing Phase B —
 *
 * <ol>
 *   <li><b>Deletes must remain effective across a checkpoint.</b> A PK deleted
 *       in the live shards before checkpoint must not reappear in search
 *       results after Phase B has flushed those shards to disk.</li>
 *   <li><b>Recall on surviving keys must not degrade catastrophically.</b>
 *       Surviving PKs must still be findable; specifically, the original
 *       vector of a surviving PK must self-match (i.e. its own PK is the
 *       top-1 result, and it appears within the top-K).</li>
 * </ol>
 *
 * <p>This test does NOT measure absolute recall against a ground truth — that
 * is the job of {@code PersistentVectorStoreRecallTest}. Its purpose is to
 * fail loudly if a Phase B refactor either (a) drops surviving vectors from
 * the persisted graph or (b) leaks tombstoned PKs back into search results.
 */
public class PersistentVectorStoreCheckpointPhaseBDeleteRecallTest {

    /** Total vectors inserted before any deletion. */
    private static final int N = 1000;

    /** Vector dimension. */
    private static final int DIM = 32;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void deletesSurviveCheckpointAndSurvivorsRemainRecallable() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(6140);

            // Keep a copy of the original vectors so we can replay each as
            // a query post-checkpoint. Without this we cannot tell whether
            // a missing PK is a delete-leak (good) or a graph-loss (bad).
            List<float[]> originalVectors = new ArrayList<>(N);
            for (int i = 0; i < N; i++) {
                float[] v = randomVector(rng);
                originalVectors.add(v);
                store.addVector(Bytes.from_int(i), v);
            }
            assertEquals(N, store.size());

            // Delete every even-indexed PK (500 deletions) BEFORE the
            // checkpoint, so Phase B sees a frozen shard with active
            // tombstones — the exact configuration where a buggy graph
            // serializer could leak deleted ordinals.
            Set<Integer> deleted = new HashSet<>();
            for (int i = 0; i < N; i += 2) {
                store.removeVector(Bytes.from_int(i));
                deleted.add(i);
            }
            assertEquals(N - deleted.size(), store.size());

            // Run the checkpoint that flushes live shards (with the deletes
            // applied) to a brand-new on-disk segment. This is the exact
            // doCheckpointFusedPQPhaseB → writeShardAsFusedPQSegment path
            // that issue #614 is concerned with.
            store.checkpoint();
            assertEquals(N - deleted.size(), store.size());

            // ----- Invariant 1: deleted PKs do not reappear. -----
            // Replay each deleted vector as a query — if the post-Phase-B
            // segment still indexes a tombstoned ordinal, the deleted PK
            // will surface here.
            int deletedLeaks = 0;
            for (int i : deleted) {
                List<Map.Entry<Bytes, Float>> results =
                        store.search(originalVectors.get(i), 10);
                for (Map.Entry<Bytes, Float> e : results) {
                    if (e.getKey().equals(Bytes.from_int(i))) {
                        deletedLeaks++;
                        break;
                    }
                }
            }
            assertEquals(
                    "Deleted PKs must not reappear in search results after Phase B"
                            + " has flushed the live shard to disk. Leaks=" + deletedLeaks,
                    0, deletedLeaks);

            // ----- Invariant 2: surviving PKs are still recallable. -----
            // For every surviving PK, query its OWN original vector. The
            // graph + PQ are noisy approximations, so we tolerate a small
            // fraction of misses (HNSW with these parameters and random
            // 32-d vectors hits >97% in practice). The point is to catch
            // a graph-loss regression — if Phase B silently drops nodes,
            // the miss rate jumps to large fractions, not a few percent.
            int top1Self = 0;
            int topKContains = 0;
            int topK = 10;
            for (int i = 1; i < N; i += 2) {
                List<Map.Entry<Bytes, Float>> results =
                        store.search(originalVectors.get(i), topK);
                Bytes self = Bytes.from_int(i);
                if (!results.isEmpty() && results.get(0).getKey().equals(self)) {
                    top1Self++;
                }
                for (Map.Entry<Bytes, Float> e : results) {
                    if (e.getKey().equals(self)) {
                        topKContains++;
                        break;
                    }
                }

                // Per-query: deleted PKs must not appear in any survivor
                // query either. Tightens invariant 1 across the survivor
                // search space too.
                for (Map.Entry<Bytes, Float> e : results) {
                    int rk = e.getKey().to_int();
                    assertFalse(
                            "Survivor query for PK=" + i + " returned tombstoned PK=" + rk,
                            deleted.contains(rk));
                }
            }
            int survivors = N - deleted.size();
            double top1Rate = top1Self / (double) survivors;
            double topKRate = topKContains / (double) survivors;
            System.out.printf(
                    "Phase B delete-recall: survivors=%d, self-match top1=%.4f, in-topK=%.4f%n",
                    survivors, top1Rate, topKRate);
            assertTrue(
                    "Surviving PKs must self-match in top-" + topK + " for at least 95% of"
                            + " queries (was " + topKRate + "); a lower rate indicates Phase B"
                            + " is losing nodes from the persisted graph.",
                    topKRate >= 0.95);
            assertTrue(
                    "Surviving PKs must self-match in top-1 for at least 90% of queries"
                            + " (was " + top1Rate + "); a lower rate indicates the persisted"
                            + " graph is materially degraded.",
                    top1Rate >= 0.90);
        }
    }
}
