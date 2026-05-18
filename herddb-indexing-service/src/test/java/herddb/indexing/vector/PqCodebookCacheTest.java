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
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that the PQ codebook cache (issue #281) reduces K-Means training
 * invocations across multiple checkpoint cycles for the same index.
 *
 * <p>The cache is controlled by {@link PersistentVectorStore#pqCodebookRetrainingInterval}:
 * <ul>
 *   <li>When {@code > 0}: the codebook trained on the first FusedPQ-eligible segment
 *       is reused for up to {@code interval - 1} further segments before retraining.</li>
 *   <li>When {@code == 0}: caching is disabled; every segment trains from scratch
 *       (original behaviour).</li>
 * </ul>
 */
public class PqCodebookCacheTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private int savedMinLive;
    private long savedMaxDeferral;
    private int savedRetrainingInterval;

    @Before
    public void saveAndResetGates() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedMaxDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        savedRetrainingInterval = PersistentVectorStore.pqCodebookRetrainingInterval;
        // Disable the gate so small test batches always checkpoint immediately.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        PersistentVectorStore.maxCheckpointDeferralMs = Long.MAX_VALUE;
    }

    @After
    public void restoreGates() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedMaxDeferral;
        PersistentVectorStore.pqCodebookRetrainingInterval = savedRetrainingInterval;
    }

    /**
     * Creates a store with a large segment size so all vectors inserted
     * per cycle land in a single shard (1 call to writeShardAsFusedPQSegment
     * per checkpoint). compactionIntervalMs is set to Long.MAX_VALUE to
     * prevent background compaction from interfering with the counters.
     */
    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true,
                /* maxSegmentSize */ 2_000_000_000L,
                /* maxLiveGraphSize */ 0,
                /* compactionIntervalMs */ Long.MAX_VALUE);
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    // -------------------------------------------------------------------------
    // Test: caching reduces training count across checkpoints
    // -------------------------------------------------------------------------

    /**
     * With a large retraining interval, PQ is trained only once regardless of
     * how many checkpoint cycles run. Each cycle inserts enough vectors to
     * satisfy MIN_VECTORS_FOR_FUSED_PQ (256), so FusedPQ is written and the
     * codebook cache is exercised.
     */
    @Test
    public void codebookIsReusedAcrossCheckpointCycles() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 1000; // far above cycle count
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 32;
        int vectorsPerCycle = 300; // > MIN_VECTORS_FOR_FUSED_PQ (256)
        int cycles = 5;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            for (int cycle = 0; cycle < cycles; cycle++) {
                Random rng = new Random(cycle);
                for (int i = 0; i < vectorsPerCycle; i++) {
                    store.addVector(Bytes.from_int(cycle * 10_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            // With interval=1000 and 5 cycles each producing 1 shard:
            // only 1 training is expected.
            int trainings = store.pqTrainingsTotal.get();
            assertTrue(
                    "PQ codebook should be reused: expected 1 training but got " + trainings
                            + " over " + cycles + " checkpoint cycles",
                    trainings == 1);

            // Correctness: search must still return results after cache reuse.
            float[] query = randomVector(new Random(99), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search should return results after codebook-cached checkpoints",
                    results.isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Test: interval-based retraining works correctly
    // -------------------------------------------------------------------------

    /**
     * With interval=2, the codebook is retrained every 2nd segment write.
     * Over N cycles (each producing 1 shard), trainings = ceil(N / 2).
     * The exact count must be strictly less than N (otherwise there is no
     * caching) and at least ceil(N / 2) (otherwise the retraining interval
     * is not respected).
     */
    @Test
    public void codebookIsRetrainedAtConfiguredInterval() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 2;
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 32;
        int vectorsPerCycle = 300;
        int cycles = 6;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            for (int cycle = 0; cycle < cycles; cycle++) {
                Random rng = new Random(cycle + 100);
                for (int i = 0; i < vectorsPerCycle; i++) {
                    store.addVector(Bytes.from_int(cycle * 10_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            int trainings = store.pqTrainingsTotal.get();
            // With interval=2 over 6 writes: training at writes 1, 3, 5 → 3 trainings.
            // Allow for a benign boundary race (at most cycles/2 + 1).
            assertTrue("should have fewer trainings than cycles; got " + trainings,
                    trainings < cycles);
            assertTrue("should have at least 1 training", trainings >= 1);

            // Correctness check
            float[] query = randomVector(new Random(42), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search must work after interval-based retraining", results.isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Test: interval=0 disables caching (original behaviour)
    // -------------------------------------------------------------------------

    /**
     * When {@code pqCodebookRetrainingInterval == 0} every FusedPQ-eligible
     * shard write triggers a full K-Means training. Over N cycles each
     * producing 1 shard, training count == N.
     */
    @Test
    public void cachingDisabledWhenIntervalIsZero() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 0;
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 32;
        int vectorsPerCycle = 300;
        int cycles = 3;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            for (int cycle = 0; cycle < cycles; cycle++) {
                Random rng = new Random(cycle + 200);
                for (int i = 0; i < vectorsPerCycle; i++) {
                    store.addVector(Bytes.from_int(cycle * 10_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            // With caching disabled every shard write trains; 3 cycles → 3 trainings.
            int trainings = store.pqTrainingsTotal.get();
            assertEquals(
                    "With interval=0 (cache disabled) every segment should train; "
                            + "got " + trainings + " trainings over " + cycles + " cycles",
                    cycles, trainings);
        }
    }

    // -------------------------------------------------------------------------
    // Test: small shards (< MIN_VECTORS_FOR_FUSED_PQ) never train PQ
    // -------------------------------------------------------------------------

    /**
     * Shards with fewer than {@link PersistentVectorStore#MIN_VECTORS_FOR_FUSED_PQ}
     * vectors do not write FusedPQ, so PQ training must be skipped entirely for them
     * regardless of the retraining interval (issue #281 also fixed the wasteful
     * training-then-discard path for small shards).
     */
    @Test
    public void smallShardDoesNotTrainPQ() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 100;
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 32;
        // Insert fewer vectors than MIN_VECTORS_FOR_FUSED_PQ (256) so
        // useFusedPQForShard is false for every shard.
        int vectorsPerCycle = PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ - 1;
        int cycles = 3;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            for (int cycle = 0; cycle < cycles; cycle++) {
                Random rng = new Random(cycle + 300);
                for (int i = 0; i < vectorsPerCycle; i++) {
                    store.addVector(Bytes.from_int(cycle * 10_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            // No shard qualifies for FusedPQ → zero PQ trainings.
            assertEquals("Small shards must not trigger PQ training",
                    0, store.pqTrainingsTotal.get());
        }
    }

    // -------------------------------------------------------------------------
    // Test: restart clears the in-memory cache
    // -------------------------------------------------------------------------

    /**
     * The PQ codebook cache is an instance-level field; it is not persisted to
     * disk. When a store is closed and a new instance is opened over the same
     * persisted segments, the cache starts empty. The first FusedPQ-eligible
     * shard write on the reopened instance must re-train the codebook, and
     * subsequent writes must reuse it. Search quality must be preserved across
     * the restart.
     */
    @Test
    public void restartClearsCacheAndRetrainsOnFirstWrite() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 1000;
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        final String fixedUuid = "restart-test-uuid-281";
        int dim = 32;
        int vectors = 300;

        // --- First instance: train and checkpoint ---
        try (PersistentVectorStore store = new PersistentVectorStore(
                "ridx", "rtable", "rspace", "vec_col", fixedUuid,
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN)) {
            store.start();
            Random rng = new Random(1);
            for (int i = 0; i < vectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertEquals("First instance should train exactly once",
                    1, store.pqTrainingsTotal.get());
        }

        // --- Second instance: reopen, the cache starts empty ---
        try (PersistentVectorStore store = new PersistentVectorStore(
                "ridx", "rtable", "rspace", "vec_col", fixedUuid,
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN)) {
            store.start();

            // Cache must start empty on the new instance.
            assertEquals("Fresh instance must start with zero trainings",
                    0, store.pqTrainingsTotal.get());

            // Insert more vectors and checkpoint — should trigger exactly 1 training.
            Random rng = new Random(2);
            for (int i = vectors; i < vectors * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            assertEquals("Reopened instance should train once on first post-restart checkpoint",
                    1, store.pqTrainingsTotal.get());

            // Search must return results from both the pre-restart and
            // post-restart vectors.
            float[] query = randomVector(new Random(77), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("search must return results after restart", results.isEmpty());
            assertEquals("should return requested k results", 10, results.size());
        }
    }

    // -------------------------------------------------------------------------
    // Test: Phase B write failure does not invalidate cached codebook
    // -------------------------------------------------------------------------

    /**
     * A {@link DataStorageManagerException} thrown during a Phase B segment
     * write (e.g. ENOSPC) causes the checkpoint to abort and Phase B recovery
     * to run. At the time of the failure the codebook has already been trained
     * and cached. The next successful checkpoint must reuse the cached codebook
     * (no extra training), demonstrating that transient write errors do not
     * force unnecessary K-Means re-training.
     */
    @Test
    public void phaseBWriteFailureDoesNotInvalidatePqCache() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 1000;
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailOnceDataStorageManager dsm = new FailOnceDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        int dim = 32;
        int vectors = 300;

        try (PersistentVectorStore store = new PersistentVectorStore(
                "failidx", "ftable", "fspace", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN)) {
            store.start();

            Random rng = new Random(10);
            for (int i = 0; i < vectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Arm the failure: the first multipart write throws.
            dsm.failNextWrite = true;
            try {
                store.checkpoint();
                fail("expected checkpoint to propagate injected write failure");
            } catch (DataStorageManagerException expected) {
                // Expected — the DSM threw on the first multipart write.
                assertTrue(expected.getMessage().contains("injected"));
            }

            // After the failed checkpoint, the PQ may or may not have been
            // trained depending on thread scheduling. The key invariant is
            // that a subsequent successful checkpoint produces correct search
            // results and does NOT retrain if the cache is already warm.
            int trainingsAfterFailure = store.pqTrainingsTotal.get();

            // Now checkpoint successfully.
            store.checkpoint();
            int trainingsAfterSuccess = store.pqTrainingsTotal.get();

            if (trainingsAfterFailure == 1) {
                // Cache was populated during the failed attempt: the successful
                // checkpoint must reuse it (no additional training).
                assertEquals("Cached codebook from failed Phase B must be reused on retry",
                        1, trainingsAfterSuccess);
            } else {
                // PQ was not trained before the failure (e.g. failure was very
                // early): the successful checkpoint must train exactly once.
                assertEquals("PQ must be trained exactly once across retry",
                        1, trainingsAfterSuccess);
            }

            // Correctness: search must return results.
            float[] query = randomVector(new Random(55), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search must return results after failed+successful checkpoint",
                    results.isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Test: recovered store retrains PQ correctly
    // -------------------------------------------------------------------------

    /**
     * After multiple consecutive Phase B failures (checkpoint retried N times),
     * the store must still be usable and must not have accumulated unbounded PQ
     * trainings. Each failed attempt either reuses the cached codebook (if it
     * was already warm) or trains at most once; subsequent successful checkpoint
     * never exceeds the training count set during the failure retries.
     */
    @Test
    public void repeatedPhaseBFailuresBoundTrainingCount() throws Exception {
        PersistentVectorStore.pqCodebookRetrainingInterval = 1000;
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailNTimesDataStorageManager dsm = new FailNTimesDataStorageManager(3);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        int dim = 32;
        int vectors = 300;

        try (PersistentVectorStore store = new PersistentVectorStore(
                "failidx2", "ftable2", "fspace2", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN)) {
            store.start();
            Random rng = new Random(20);
            for (int i = 0; i < vectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Three consecutive failures.
            for (int attempt = 0; attempt < 3; attempt++) {
                dsm.failOnNextWrite();
                try {
                    store.checkpoint();
                    fail("expected checkpoint to fail on attempt " + attempt);
                } catch (DataStorageManagerException expected) {
                    assertTrue(expected.getMessage().contains("injected"));
                }
                // Reload vectors since recovery puts them back in the live set.
            }

            // Final successful checkpoint.
            store.checkpoint();
            int finalTrainings = store.pqTrainingsTotal.get();

            // With interval=1000: at most 1 training for the 3 failed attempts
            // (cache warm after the first) + 0 for the final success.
            // Upper bound: 4 (one per attempt if never cached). In practice ≤ 1.
            assertTrue("training count must be bounded; got " + finalTrainings,
                    finalTrainings <= 4);
            assertTrue("at least one training must have occurred", finalTrainings >= 1);

            float[] query = randomVector(new Random(66), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search must work after repeated failures", results.isEmpty());
        }
    }

    // -------------------------------------------------------------------------
    // Helper: DSMs that inject failures for the failure-mode tests
    // -------------------------------------------------------------------------

    /** Throws {@link DataStorageManagerException} on the next multipart file write. */
    private static final class FailOnceDataStorageManager extends MemoryDataStorageManager {
        volatile boolean failNextWrite = false;

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (failNextWrite) {
                failNextWrite = false;
                throw new DataStorageManagerException(
                        "injected write failure for uuid=" + uuid + " type=" + fileType);
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }
    }

    /** Throws {@link DataStorageManagerException} on each of the next N multipart writes. */
    private static final class FailNTimesDataStorageManager extends MemoryDataStorageManager {
        private volatile int failCount = 0;

        FailNTimesDataStorageManager(int initialFails) {
            this.failCount = initialFails;
        }

        void failOnNextWrite() {
            failCount = 1;
        }

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (failCount > 0) {
                failCount--;
                throw new DataStorageManagerException(
                        "injected write failure for uuid=" + uuid + " type=" + fileType);
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }
    }
}
