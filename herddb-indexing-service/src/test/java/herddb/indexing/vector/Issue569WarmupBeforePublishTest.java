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
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression tests for issue #569 — the warmup→checkpoint→warmup death spiral.
 *
 * <p>Before the fix, {@code IndexingServiceEngine} re-warmed the block cache of
 * <em>every</em> loaded segment after <em>every</em> checkpoint. Once the
 * indexing service accumulated 150+ segments this took ~135 s, spiked the
 * heap, immediately re-triggered a memory-pressure checkpoint, and starved
 * compaction — the segment count then grew without bound and the tailer froze.
 *
 * <p>The fix warms each segment exactly once, AT CREATION TIME, before it is
 * published into the searchable {@code segments} list:
 * <ul>
 *   <li>checkpoint Phase C-prep warms the freshly-built segment(s);</li>
 *   <li>compaction warms the merged output before the atomic swap;</li>
 * </ul>
 * which makes the post-checkpoint warm-all sweep an idempotent no-op in steady
 * state. The only segments warmed by the sweep are restart-loaded ones.
 */
public class Issue569WarmupBeforePublishTest {

    /** A budget large enough to fully warm every test segment. */
    private static final long WARMUP_BUDGET = Long.MAX_VALUE;

    private static final int DIM = 8;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        // Allow checkpoints to run with the small batches used here.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    private PersistentVectorStore newStore(Path tmpDir, DataStorageManager dsm, String indexUuid) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // Explicit indexUUID so a reopened store recovers the same checkpoint.
        return new PersistentVectorStore(
                "vidx569", "vectable", "tstblspace", "vec", indexUuid, tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0, /*compactionIntervalMs*/ Long.MAX_VALUE);
    }

    private static void addBatch(PersistentVectorStore store, int checkpointIdx,
                                 int count, Random rng) throws Exception {
        for (int i = 0; i < count; i++) {
            float[] vec = new float[DIM];
            for (int d = 0; d < DIM; d++) {
                vec[d] = rng.nextFloat();
            }
            store.addVector(Bytes.from_int(checkpointIdx * 100_000 + i), vec);
        }
    }

    /**
     * Every segment produced by a checkpoint must be warmed BEFORE it joins
     * the searchable segment list — i.e. {@code warmedUp} is already {@code
     * true} for every segment visible after {@code checkpoint()} returns.
     */
    @Test
    public void checkpointSegmentsAreWarmedBeforePublish() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-checkpoint")) {
            store.setWarmupBytesPerSegment(WARMUP_BUDGET);
            store.start();

            Random rng = new Random(569);
            addBatch(store, 0, 300, rng);
            assertEquals("nothing is warmed before the first checkpoint",
                    0, store.getWarmedSegmentsTotal());

            store.checkpoint();

            List<VectorSegment> segs = store.getOnDiskSegmentsSnapshotForTest();
            assertFalse("checkpoint must publish at least one segment", segs.isEmpty());
            for (VectorSegment s : segs) {
                assertTrue("every published segment must be warmed before it is searchable",
                        s.warmedUp);
            }
            assertEquals("each new segment warmed exactly once at creation",
                    segs.size(), (int) store.getWarmedSegmentsTotal());
        }
    }

    /**
     * The core death-spiral regression: across many checkpoints the total
     * number of segments actually warmed must equal the number of segments
     * that exist (each warmed exactly once) — it must NOT grow with the
     * running segment count, and the post-checkpoint warm-all sweep must be
     * an idempotent no-op.
     */
    @Test
    public void warmupCostStaysBoundedAcrossCheckpoints() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-spiral")) {
            store.setWarmupBytesPerSegment(WARMUP_BUDGET);
            store.start();

            Random rng = new Random(42);
            int checkpoints = 6;
            for (int c = 0; c < checkpoints; c++) {
                addBatch(store, c, 300, rng);
                store.checkpoint();
                // Each segment is warmed exactly once, at creation. If the old
                // warm-all-after-every-checkpoint behaviour regressed this
                // counter would grow quadratically (1+2+...+N) and far exceed
                // the live segment count.
                assertEquals("checkpoint " + c + ": every segment warmed exactly once",
                        store.getOnDiskSegmentCount(), (int) store.getWarmedSegmentsTotal());
            }

            long warmedAfterBuild = store.getWarmedSegmentsTotal();
            // The post-checkpoint warm-all sweep — what IndexingServiceEngine
            // invokes — must now find every segment already warm and do no I/O.
            store.warmUpBlockCache(WARMUP_BUDGET);
            assertEquals("warm-all sweep must be idempotent — no segment re-warmed",
                    warmedAfterBuild, store.getWarmedSegmentsTotal());
            store.warmUpBlockCache(WARMUP_BUDGET);
            assertEquals("repeated warm-all sweeps stay idempotent",
                    warmedAfterBuild, store.getWarmedSegmentsTotal());
        }
    }

    /**
     * Compaction must warm its merged output segment BEFORE the atomic swap
     * publishes it — and exactly once (one extra warmed segment per cycle,
     * never a re-warm of the surviving segments).
     */
    @Test
    public void compactionMergedOutputIsWarmedBeforePublish() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-compaction")) {
            store.setWarmupBytesPerSegment(WARMUP_BUDGET);
            // Count-triggered compaction: fires once >= 6 mergeable segments
            // accumulate (byte trigger kept unreachably high).
            store.configureCompaction(
                    /*intervalMs*/ Long.MAX_VALUE,
                    /*minBytes*/ Long.MAX_VALUE / 2,
                    /*maxBytes*/ Long.MAX_VALUE,
                    /*minCount*/ 2,
                    /*maxCount*/ 6,
                    /*retentionMs*/ 0);
            store.start();

            Random rng = new Random(285);
            for (int c = 0; c < 8; c++) {
                addBatch(store, c, 300, rng);
                store.checkpoint();
            }

            int segmentsBefore = store.getOnDiskSegmentCount();
            assertTrue("need several segments to exercise a merge, got " + segmentsBefore,
                    segmentsBefore >= 6);
            long warmedBefore = store.getWarmedSegmentsTotal();
            assertEquals("all pre-compaction segments were warmed at creation",
                    segmentsBefore, (int) warmedBefore);

            store.runCompactionCycle();

            assertTrue("compaction must reduce the segment count",
                    store.getOnDiskSegmentCount() < segmentsBefore);
            assertEquals("exactly the merged output is warmed — before it is published",
                    warmedBefore + 1, store.getWarmedSegmentsTotal());
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                assertTrue("every segment (including the merged output) is warm",
                        s.warmedUp);
            }
        }
    }

    /**
     * Segments reloaded by {@code loadFromStatus} on restart start COLD
     * (they were not created by this store instance). The warm-all sweep
     * warms them once; a second sweep is a no-op.
     */
    @Test
    public void warmAllWarmsRestartLoadedColdSegments() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DataStorageManager dsm = new MemoryDataStorageManager();

        int segmentCount;
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-restart")) {
            store.setWarmupBytesPerSegment(WARMUP_BUDGET);
            store.start();
            Random rng = new Random(7);
            for (int c = 0; c < 4; c++) {
                addBatch(store, c, 300, rng);
                store.checkpoint();
            }
            segmentCount = store.getOnDiskSegmentCount();
            assertTrue("checkpoints must build some segments", segmentCount > 0);
        }

        // Reopen against the same storage + index UUID — segments are restored
        // by loadFromStatus and start cold.
        try (PersistentVectorStore reopened = newStore(tmpDir, dsm, "uuid-restart")) {
            reopened.setWarmupBytesPerSegment(WARMUP_BUDGET);
            reopened.start();

            assertEquals("restart must reload the persisted segments",
                    segmentCount, reopened.getOnDiskSegmentCount());
            assertEquals("restart-loaded segments are not warmed at creation",
                    0, reopened.getWarmedSegmentsTotal());
            for (VectorSegment s : reopened.getOnDiskSegmentsSnapshotForTest()) {
                assertFalse("a restart-loaded segment starts cold", s.warmedUp);
            }

            reopened.warmUpBlockCache(WARMUP_BUDGET);
            assertEquals("warm-all warms every restart-loaded segment exactly once",
                    segmentCount, (int) reopened.getWarmedSegmentsTotal());
            for (VectorSegment s : reopened.getOnDiskSegmentsSnapshotForTest()) {
                assertTrue("restart-loaded segment is warm after the sweep", s.warmedUp);
            }

            reopened.warmUpBlockCache(WARMUP_BUDGET);
            assertEquals("a second warm-all sweep is an idempotent no-op",
                    segmentCount, (int) reopened.getWarmedSegmentsTotal());
        }
    }

    /**
     * When warmup is disabled ({@code warmupBytesPerSegment <= 0}) no segment
     * is warmed and the warm-all sweep is a no-op.
     */
    @Test
    public void warmupDisabledLeavesSegmentsUnwarmed() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        DataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = newStore(tmpDir, dsm, "uuid-disabled")) {
            // warmupBytesPerSegment left at its default of 0 → warmup disabled.
            store.start();
            Random rng = new Random(99);
            addBatch(store, 0, 300, rng);
            store.checkpoint();

            assertEquals("warmup disabled → no segment is warmed",
                    0, store.getWarmedSegmentsTotal());
            for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
                assertFalse("warmup disabled → segment stays cold", s.warmedUp);
            }
            store.warmUpBlockCache(0);
            assertEquals("warm-all with a zero budget is a no-op",
                    0, store.getWarmedSegmentsTotal());
        }
    }
}
