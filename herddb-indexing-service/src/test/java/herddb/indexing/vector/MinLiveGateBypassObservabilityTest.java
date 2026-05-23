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
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #646 — operator-visible bypass counter.
 *
 * <p>When the {@code minLiveVectorsForCheckpoint} gate would have deferred a
 * checkpoint (live shard too small + no dirty segments + segments exist) but
 * {@link PersistentVectorStore#shouldTriggerMemoryPressureCheckpoint()} forces
 * it through anyway, the store must emit an INFO log entry AND increment the
 * {@code memory_pressure_checkpoint_bypass_total} counter. Before issue #646
 * this bypass was silent — the only signal was a flood of tiny on-disk
 * segments downstream, by which point the harm was done.
 */
public class MinLiveGateBypassObservabilityTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 16;

    private int savedMinLiveVectorsForCheckpoint;

    @Before
    public void saveGate() {
        savedMinLiveVectorsForCheckpoint = PersistentVectorStore.minLiveVectorsForCheckpoint;
    }

    @After
    public void restoreGate() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectorsForCheckpoint;
    }

    /**
     * A budget whose memory-pressure trigger output (and only that) is gated
     * by an {@link AtomicBoolean}, so the test can decide at what point to
     * activate over-threshold reporting. Hard back-pressure
     * ({@link #isMemoryPressureActive()}) stays off so {@code addVector}
     * never blocks.
     */
    private static final class GatedBudget implements VectorMemoryBudget {
        final AtomicBoolean trigger;

        GatedBudget(AtomicBoolean trigger) {
            this.trigger = trigger;
        }

        @Override
        public long totalEstimatedMemoryUsageBytes() {
            // Pick a usage that matches the trigger state. The exact value
            // does not matter — only its relation to maxMemoryBytes matters
            // for the isAboveThreshold(fraction) reading.
            return trigger.get() ? 95L : 1L;
        }

        @Override
        public long maxMemoryBytes() {
            return 100L;
        }

        @Override
        public boolean isMemoryPressureActive() {
            // Never block addVector — this test exercises the trigger path,
            // not the hard back-pressure path.
            return false;
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, VectorMemoryBudget budget) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vec",
                "vidx_uuid", tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true /* fusedPQ */, 2_000_000_000L, 0,
                /* compactionIntervalMs */ Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN,
                Long.MAX_VALUE /* maxVectorMemoryBytes */, budget, 0, 0);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    private float[] vec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test(timeout = 120_000)
    public void counterIncrementsOnlyWhenMemoryPressureBypassesTheGate() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bypass-counter").toPath();
        AtomicBoolean trigger = new AtomicBoolean(false);
        GatedBudget budget = new GatedBudget(trigger);
        try (PersistentVectorStore store = createStore(tmpDir, budget)) {
            // Force the 0.70 historical fraction: the budget reports
            // usage=95, max=100 → isAboveThreshold(0.70) returns true even
            // under the new 0.90 default, but the explicit setEarlyCheckpointFraction
            // call also exercises the wiring.
            store.setEarlyCheckpointFraction(0.70d);
            store.start();

            // Phase 1: build at least one on-disk segment so the bypass
            // condition (which requires !segments.isEmpty()) can fire.
            PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
            Random rng = new Random(646);
            for (int i = 0; i < 200; i++) {
                store.addVector(Bytes.from_int(i), vec(rng));
            }
            assertTrue("initial checkpoint must drain the shard", store.checkpoint());
            assertTrue("an on-disk segment must exist for the bypass to be possible",
                    store.getSegmentCount() >= 1);
            assertEquals("no bypass yet — first checkpoint was a regular flush",
                    0L, store.getTotalMemoryPressureCheckpointBypassCount());

            // Phase 2: add a tiny trickle of new vectors and re-arm the gate.
            // With the trigger OFF, this checkpoint MUST defer (return false)
            // and the counter MUST remain zero.
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            for (int i = 200; i < 210; i++) {
                store.addVector(Bytes.from_int(i), vec(rng));
            }
            trigger.set(false);
            boolean ranWithoutPressure = store.checkpoint();
            assertFalse("with no memory pressure the gate must defer the tiny shard",
                    ranWithoutPressure);
            assertEquals("no bypass while the budget is below the trigger",
                    0L, store.getTotalMemoryPressureCheckpointBypassCount());

            // Phase 3: flip the trigger on and checkpoint again. The bypass
            // condition is now satisfied (live<gate, !dirty, !empty,
            // memoryPressureTrigger=true) — counter must tick exactly once.
            trigger.set(true);
            assertTrue("memory pressure must force the checkpoint past the gate",
                    store.checkpoint());
            assertEquals("the bypass counter must increment exactly once",
                    1L, store.getTotalMemoryPressureCheckpointBypassCount());

            // Phase 4: add another trickle and trigger another bypass cycle.
            // The counter must accumulate (it is monotonic within the process).
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            for (int i = 210; i < 220; i++) {
                store.addVector(Bytes.from_int(i), vec(rng));
            }
            assertTrue("second pressure-driven cycle must run", store.checkpoint());
            assertEquals("bypass counter must accumulate across cycles",
                    2L, store.getTotalMemoryPressureCheckpointBypassCount());

            // Phase 5: the BIGANN 200M path — segments.size() above the
            // compactionBackpressureThreshold. The sibling deferral gate at
            // ~PersistentVectorStore.java:6336 fires under back-pressure; the
            // memory-pressure trigger must bypass it as well, and the counter
            // must increment. This is the actual scenario the issue describes
            // (the time-window path covered in phases 1-4 is its sibling, not
            // the segment-storm regime). To reach this path deterministically
            // we drop the back-pressure threshold below the current segment
            // count rather than building hundreds of segments.
            //
            // CRITICAL: keep the threshold high (Integer.MAX_VALUE) while
            // calling addVector — addVectorInternal blocks on
            // waitForSegmentCountRelief once segments.size() > threshold, and
            // the compaction loop is parked (Long.MAX_VALUE interval) so the
            // block would never lift. Drop the threshold AFTER the trickle is
            // staged but BEFORE checkpoint() so the deferral gate sees the
            // over-threshold segment count.
            int segs = store.getSegmentCount();
            assertTrue("we need at least 2 segments to make the BP threshold meaningful",
                    segs >= 2);
            store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);
            PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
            for (int i = 220; i < 230; i++) {
                store.addVector(Bytes.from_int(i), vec(rng));
            }
            store.setCompactionBackpressureThreshold(segs - 1);
            // memory-pressure trigger still on from phase 4.
            assertTrue("memory pressure must bypass the segment-count BP gate as well",
                    store.checkpoint());
            assertEquals("bypass counter must increment on the SEGMENT_COUNT_BACKPRESSURE"
                    + " path too — this is the BIGANN 200M regime",
                    3L, store.getTotalMemoryPressureCheckpointBypassCount());
        }
    }
}
