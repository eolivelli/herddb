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

import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #614: locks the invariant that Phase B duration
 * scales with the size of the snapshotted live shards, not with the size of
 * the on-disk index.
 *
 * <p>The test inserts {@code BATCH_SIZE} vectors three times in a row, running
 * a checkpoint after each batch, and asserts that Phase B for batches 2 and 3
 * is not dramatically slower than batch 1 — even though batches 2 and 3 run
 * with progressively more on-disk vectors already persisted.
 *
 * <p>If a future change introduces a Phase B code path that consults the
 * global on-disk graph (e.g. inter-segment edge building, full-graph PQ
 * retraining over previously-written segments), Phase B duration would start
 * growing with the on-disk vector count and this test would fail. That is
 * exactly the regression vector this test is designed to catch — see
 * {@code writeShardAsFusedPQSegment} in {@link PersistentVectorStore}, which
 * by construction only references the per-shard {@code OnHeapGraphIndex}.
 */
public class PersistentVectorStoreCheckpointPhaseBLocalScopeTest {

    /** Vectors inserted per batch. Three batches → three Phase B measurements. */
    private static final int BATCH_SIZE = 500;

    /** Vector dimension; kept small so each per-shard write is fast. */
    private static final int DIM = 32;

    /**
     * Slack ratio: later Phase B may be up to {@code RATIO_LIMIT}× slower than
     * the first one before the test fails. The expected ratio is ~1.0 — Phase
     * B is purely per-shard. The slack absorbs JVM warmup, GC, and noise on
     * the very first run.
     */
    private static final double RATIO_LIMIT = 5.0;

    /**
     * Absolute floor (ms): when the first Phase B is very short (single-digit
     * ms), small absolute jitter would otherwise blow the ratio. Below this
     * floor the test ignores the ratio and only asserts an upper-bound on the
     * later duration.
     */
    private static final long ABSOLUTE_FLOOR_MS = 200L;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE); // compaction disabled — keeps Phase B the only
                                 // code path that writes on-disk segments, so
                                 // any regression has nowhere to hide.
    }

    private float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private void insertBatch(PersistentVectorStore store, Random rng, int base) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            store.addVector(Bytes.from_int(base + i), randomVector(rng));
        }
    }

    @Test
    public void phaseBDurationDoesNotGrowWithOnDiskSize() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(614);

            // --- Batch 1: 500 vectors → 1 segment on disk after checkpoint.
            insertBatch(store, rng, 0);
            store.checkpoint();
            long phaseB1Ms = store.getLastCheckpointPhaseBDurationMs();

            // --- Batch 2: another 500. Now Phase B writes 500 fresh live
            // vectors WHILE 500 already exist on disk. Local-scope Phase B
            // should not care.
            insertBatch(store, rng, BATCH_SIZE);
            store.checkpoint();
            long phaseB2Ms = store.getLastCheckpointPhaseBDurationMs();

            // --- Batch 3: another 500. 1000 already on disk. Same story.
            insertBatch(store, rng, 2 * BATCH_SIZE);
            store.checkpoint();
            long phaseB3Ms = store.getLastCheckpointPhaseBDurationMs();

            System.out.printf(
                    "PhaseB durations (ms): batch1=%d batch2=%d batch3=%d%n",
                    phaseB1Ms, phaseB2Ms, phaseB3Ms);

            // Sanity check: all three checkpoints actually ran Phase B.
            assertTrue("Phase B #1 should report a non-negative duration, was " + phaseB1Ms,
                    phaseB1Ms >= 0);
            assertTrue("Phase B #2 should report a non-negative duration, was " + phaseB2Ms,
                    phaseB2Ms >= 0);
            assertTrue("Phase B #3 should report a non-negative duration, was " + phaseB3Ms,
                    phaseB3Ms >= 0);

            // Core invariant: Phase B for batches 2 and 3 must not balloon
            // relative to batch 1. We allow up to RATIO_LIMIT× when batch 1
            // is above ABSOLUTE_FLOOR_MS; below the floor, the ratio is
            // dominated by noise so we only assert an absolute upper bound.
            long ratioBaselineMs = Math.max(phaseB1Ms, ABSOLUTE_FLOOR_MS);
            long allowedMs = (long) Math.ceil(ratioBaselineMs * RATIO_LIMIT);
            assertTrue(
                    "Phase B #2 (" + phaseB2Ms + " ms) must not exceed "
                            + allowedMs + " ms (= " + RATIO_LIMIT + "× baseline of "
                            + ratioBaselineMs + " ms) — Phase B appears to scale with"
                            + " on-disk size, which is the issue #614 regression.",
                    phaseB2Ms <= allowedMs);
            assertTrue(
                    "Phase B #3 (" + phaseB3Ms + " ms) must not exceed "
                            + allowedMs + " ms (= " + RATIO_LIMIT + "× baseline of "
                            + ratioBaselineMs + " ms) — Phase B appears to scale with"
                            + " on-disk size, which is the issue #614 regression.",
                    phaseB3Ms <= allowedMs);
        }
    }
}
