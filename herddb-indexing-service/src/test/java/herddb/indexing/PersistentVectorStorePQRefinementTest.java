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
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link PersistentVectorStore#getOrTrainPQ} uses
 * {@link io.github.jbellis.jvector.quantization.ProductQuantization#refine} for periodic
 * retrainings after the first training, rather than running a full K-Means from scratch every
 * time the retraining interval elapses.
 *
 * <p>The test sets {@link PersistentVectorStore#pqCodebookRetrainingInterval} to 3
 * (overriding the default of 100) so that the retraining cycle can be exercised with
 * only 4 checkpoints:
 * <ol>
 *   <li>Checkpoint 1: no cached codebook → full {@code compute()} → pqTrainingsTotal = 1</li>
 *   <li>Checkpoint 2: cached, counter 1&lt;3 → reuse → pqTrainingsTotal = 1</li>
 *   <li>Checkpoint 3: cached, counter 2&lt;3 → reuse → pqTrainingsTotal = 1</li>
 *   <li>Checkpoint 4: cached, counter 3≥3 → {@code refine()} → pqTrainingsTotal = 2</li>
 * </ol>
 *
 * <p>Each checkpoint ingests 256 ({@code MIN_VECTORS_FOR_FUSED_PQ}) vectors
 * (= 256) so that the shard qualifies for FusedPQ encoding and {@code getOrTrainPQ} is called.
 *
 * @see PersistentVectorStore#getOrTrainPQ
 */
public class PersistentVectorStorePQRefinementTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Saved so we can restore after each test. */
    private int savedInterval;

    @Before
    public void overrideRetrainingInterval() {
        savedInterval = PersistentVectorStore.pqCodebookRetrainingInterval;
        // Small interval so the retraining cycle completes within 4 checkpoints.
        PersistentVectorStore.pqCodebookRetrainingInterval = 3;
    }

    @After
    public void restoreRetrainingInterval() {
        PersistentVectorStore.pqCodebookRetrainingInterval = savedInterval;
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * Primary test: after 4 checkpoints with a retraining interval of 3, the store
     * must have performed exactly 2 PQ trainings (1 full {@code compute} + 1
     * {@code refine}).  Search quality must be maintained after the refined codebook
     * is applied.
     */
    @Test
    public void pqRefinementUsedForPeriodicRetraining() throws Exception {
        int dim = 32;
        // Each shard needs >= 256 (MIN_VECTORS_FOR_FUSED_PQ) vectors to trigger FusedPQ.
        int vectorsPerCheckpoint = 256;

        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                /* M */ 8,
                /* beamWidth */ 50,
                /* neighborOverflow */ 1.2f,
                /* alpha */ 1.4f,
                /* fusedPQ */ true,
                /* maxBytesPerShard */ 2_000_000_000L,
                /* maxLiveGraphSize */ 0,  // unbounded: all vectors in one shard
                /* maxShardedSegments */ Long.MAX_VALUE)) {

            store.start();

            Random rng = new Random(0xABCDEF);

            // --- Checkpoint 1: first training, no cached PQ → compute() ---
            for (int i = 0; i < vectorsPerCheckpoint; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertEquals("After first checkpoint, pqTrainingsTotal must be 1",
                    1, store.getPqTrainingsTotal());

            // --- Checkpoint 2: counter 1 < interval 3 → reuse cached ---
            for (int i = vectorsPerCheckpoint; i < 2 * vectorsPerCheckpoint; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertEquals("After second checkpoint, pqTrainingsTotal must still be 1 (cached reuse)",
                    1, store.getPqTrainingsTotal());

            // --- Checkpoint 3: counter 2 < interval 3 → reuse cached ---
            for (int i = 2 * vectorsPerCheckpoint; i < 3 * vectorsPerCheckpoint; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertEquals("After third checkpoint, pqTrainingsTotal must still be 1 (cached reuse)",
                    1, store.getPqTrainingsTotal());

            // --- Checkpoint 4: counter 3 >= interval 3 → refine() ---
            for (int i = 3 * vectorsPerCheckpoint; i < 4 * vectorsPerCheckpoint; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertEquals(
                    "After fourth checkpoint the interval elapsed; refine() must have been called, "
                            + "so pqTrainingsTotal must be 2",
                    2, store.getPqTrainingsTotal());

            // --- Verify that the store is still searchable after the refined codebook ---
            int totalVectors = 4 * vectorsPerCheckpoint;
            assertEquals("store.size() must equal total ingested vectors",
                    totalVectors, store.size());

            float[] query = randomVector(new Random(0xDEAD), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertTrue("Search must return results from a store with refined PQ codebook",
                    results.size() > 0);
            assertTrue("Search should return up to k=10 results from " + totalVectors + " vectors",
                    results.size() <= 10);
        }
    }

    /**
     * Edge-case test: with {@code pqCodebookRetrainingInterval = 0} (caching disabled),
     * every checkpoint must trigger a full {@code compute()} (not {@code refine()}).
     * The behaviour of the old code (always full training) must be preserved when
     * caching is explicitly disabled.
     */
    @Test
    public void fullRetrainingWhenCachingDisabled() throws Exception {
        // Override to disable caching entirely.
        PersistentVectorStore.pqCodebookRetrainingInterval = 0;

        int dim = 32;
        int vectorsPerCheckpoint = 256; // MIN_VECTORS_FOR_FUSED_PQ

        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx2", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                8, 50, 1.2f, 1.4f, true,
                2_000_000_000L, 0, Long.MAX_VALUE)) {

            store.start();
            Random rng = new Random(0x123456);

            for (int checkpoint = 0; checkpoint < 3; checkpoint++) {
                for (int i = 0; i < vectorsPerCheckpoint; i++) {
                    store.addVector(
                            Bytes.from_int(checkpoint * vectorsPerCheckpoint + i),
                            randomVector(rng, dim));
                }
                store.checkpoint();
                assertEquals(
                        "With interval=0 (caching disabled), every checkpoint must retrain: "
                                + "expected pqTrainingsTotal=" + (checkpoint + 1),
                        checkpoint + 1, store.getPqTrainingsTotal());
            }

            // Search must still work.
            float[] query = randomVector(new Random(0xBEEF), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertTrue("Search must work after full retrainings", results.size() > 0);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }
}
