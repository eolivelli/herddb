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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #563.
 *
 * <p>Before the fix, {@link PersistentVectorStore#shouldTriggerMemoryPressureCheckpoint()}
 * compared the <em>total</em> estimated memory — which includes the in-memory
 * footprint of every on-disk {@link VectorSegment} (pkData/pkOffsets/pkLengths
 * arrays + the BLink pk-to-ordinal tree) — against the live-vector memory
 * limit. Once enough segments accumulated, that on-disk footprint alone
 * permanently exceeded the 70% early-checkpoint threshold, so the compaction
 * loop fired a spurious, no-op Phase A every second even though
 * {@code live_node_count} was 0.
 *
 * <p>The fix adds a guard: when there are no live vectors a memory-pressure
 * checkpoint is a no-op (it cannot reduce the on-disk segment footprint), so
 * the trigger is suppressed. It also fixes
 * {@link PersistentVectorStore#getLiveVectorsMemoryBytes()} so the
 * {@code live_vectors_memory_bytes} diagnostic field reports only the
 * live/frozen/deferred shard footprint, with the on-disk segment footprint
 * surfaced separately via
 * {@link PersistentVectorStore#getOnDiskSegmentsEstimatedMemoryBytes()}.
 */
public class MemoryPressureTriggerZeroLiveVectorsTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 32;

    private float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * A global budget that always reports the index is above the 70%
     * early-checkpoint threshold — exactly the steady state the issue
     * describes once 150+ on-disk segments have accumulated — while reporting
     * that hard ingestion back-pressure is NOT active, so {@code addVector}
     * never blocks and the test stays deterministic.
     */
    private static final class AlwaysAboveThresholdBudget implements VectorMemoryBudget {
        @Override
        public long totalEstimatedMemoryUsageBytes() {
            return 1_925_744_608L;
        }

        @Override
        public long maxMemoryBytes() {
            return 2_126_008_811L;
        }

        @Override
        public boolean isMemoryPressureActive() {
            // No hard back-pressure: addVector must never block in this test.
            return false;
        }

        @Override
        public boolean isAboveThreshold(double fraction) {
            // Always above the early-checkpoint threshold.
            return true;
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, VectorMemoryBudget budget) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // compactionIntervalMs = Long.MAX_VALUE: the background compaction loop
        // parks immediately and the test drives checkpoints explicitly.
        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vector_col",
                "vidx_uuid", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true /* fusedPQ */, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN,
                Long.MAX_VALUE /* maxVectorMemoryBytes */, budget, 0, 0);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * The core issue #563 assertion: with the global budget permanently above
     * the early-checkpoint threshold, the trigger must still fire while there
     * are live vectors to checkpoint, but must be suppressed once a checkpoint
     * has drained the live shard ({@code live_node_count == 0}).
     */
    @Test(timeout = 120_000)
    public void triggerSuppressedWhenZeroLiveVectorsButFiresWithLiveVectors() throws Exception {
        Path tmpDir = tmpFolder.newFolder("trigger-guard").toPath();
        try (PersistentVectorStore store = createStore(tmpDir, new AlwaysAboveThresholdBudget())) {
            store.start();

            // Empty store: nothing to checkpoint, so no trigger.
            assertEquals(0, store.getLiveNodeCount());
            assertFalse("an empty store must not trigger an early checkpoint",
                    store.shouldTriggerMemoryPressureCheckpoint());

            // Add live vectors. The budget reports over-threshold, so the
            // trigger MUST fire — the guard must not suppress a legitimate
            // early checkpoint.
            Random rng = new Random(42);
            for (int i = 0; i < 600; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng));
            }
            assertTrue("live vectors must be present before checkpoint",
                    store.getLiveNodeCount() > 0);
            assertTrue("with live vectors and an over-threshold budget the"
                    + " memory-pressure trigger must fire",
                    store.shouldTriggerMemoryPressureCheckpoint());

            // Checkpoint drains the live shard onto disk.
            store.checkpoint();
            assertEquals("checkpoint must drain every live vector",
                    0, store.getLiveNodeCount());
            assertTrue("checkpoint must have produced on-disk segment memory",
                    store.getOnDiskSegmentsEstimatedMemoryBytes() > 0);

            // Issue #563: the budget is STILL over threshold (the on-disk
            // segment footprint never shrinks), but there are 0 live vectors,
            // so a checkpoint would be a no-op — the trigger must be suppressed.
            assertFalse("with 0 live vectors the memory-pressure trigger must be"
                    + " suppressed even when the global budget is over threshold",
                    store.shouldTriggerMemoryPressureCheckpoint());
        }
    }

    /**
     * Verifies that {@link PersistentVectorStore#getLiveVectorsMemoryBytes()}
     * reports only the live/frozen/deferred shard footprint and excludes the
     * on-disk segment footprint, which is surfaced separately. Before the fix
     * it delegated to {@code estimatedMemoryUsageBytes()} and so reported the
     * full total — making {@code live_vectors_memory_bytes} misleadingly large
     * (gigabytes of "live" memory with {@code live_node_count == 0}).
     */
    @Test(timeout = 120_000)
    public void liveVectorsMemoryExcludesOnDiskSegmentFootprint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("live-mem-accounting").toPath();
        // No budget + unlimited per-store limit: no back-pressure and no
        // spurious background checkpoints, so the test is deterministic.
        try (PersistentVectorStore store = createStore(tmpDir, null)) {
            store.start();

            Random rng = new Random(7);
            for (int i = 0; i < 600; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng));
            }

            // Before checkpoint: every vector is live; the on-disk estimate is 0.
            assertEquals("no on-disk segments before the first checkpoint",
                    0, store.getOnDiskSegmentsEstimatedMemoryBytes());
            assertEquals("getLiveVectorsMemoryBytes must equal getLiveShardMemoryBytes",
                    store.getLiveShardMemoryBytes(), store.getLiveVectorsMemoryBytes());

            store.checkpoint();

            assertEquals("checkpoint must drain every live vector",
                    0, store.getLiveNodeCount());

            long live = store.getLiveVectorsMemoryBytes();
            long onDisk = store.getOnDiskSegmentsEstimatedMemoryBytes();
            long total = store.estimatedMemoryUsageBytes();

            assertTrue("checkpoint must produce on-disk segment memory, got " + onDisk,
                    onDisk > 0);
            assertEquals("getLiveVectorsMemoryBytes must report only the live"
                    + " shard footprint", store.getLiveShardMemoryBytes(), live);
            // The decisive check: estimatedMemoryUsageBytes() == live + on-disk.
            // Before the fix getLiveVectorsMemoryBytes() == estimatedMemoryUsageBytes(),
            // so this equality (with onDisk > 0) could not hold.
            assertEquals("estimatedMemoryUsageBytes must be live-shard memory plus"
                    + " the on-disk segment footprint",
                    live + onDisk, total);
        }
    }
}
