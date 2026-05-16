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
import java.nio.file.Path;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for issue #562: the segment-count back-pressure safety timeout used to
 * let a small trickle of vectors (one per blocked tailer thread) slip through
 * every {@code compactionBackpressureMaxWaitMs}; the next checkpoint then sealed
 * those few vectors into a brand-new on-disk micro-segment (e.g. 3 nodes). Those
 * micro-segments are far below {@code vectorIndexCompactionMinBytes}, so tiered
 * compaction never selects them and the segment count grows unboundedly
 * throughout a long compaction cycle.
 *
 * <p>The fix adds an unconditional deferral gate to {@code doCheckpointUnderLock}:
 * while the on-disk segment count exceeds the back-pressure threshold and the
 * live shard holds fewer than {@code minLiveVectorsForCheckpoint} vectors, the
 * checkpoint is deferred regardless of the time-bounded {@code maxCheckpointDeferralMs}.
 * The trickle waits in the live shard until compaction drains the segment count
 * back below the threshold.
 *
 * <p>This class also covers the companion change that makes the compaction
 * {@code minCount} configurable via the {@code herddb.vectorindex.compactionMinCount}
 * system property.
 */
public class Issue562BackpressureMicroSegmentTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Saved originals so {@code @After} restores without hard-coding constants. */
    private int savedMinLiveVectorsForCheckpoint;
    private long savedMaxCheckpointDeferralMs;

    @Before
    public void saveStatics() {
        savedMinLiveVectorsForCheckpoint = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedMaxCheckpointDeferralMs = PersistentVectorStore.maxCheckpointDeferralMs;
    }

    @After
    public void restoreStatics() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectorsForCheckpoint;
        PersistentVectorStore.maxCheckpointDeferralMs = savedMaxCheckpointDeferralMs;
    }

    private static final int DIM = 8;

    private static float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int d = 0; d < DIM; d++) {
            v[d] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds a store with automatic compaction disabled (the test drives
     * checkpoints manually) and tiered compaction off.
     */
    private PersistentVectorStore newStore(Path tmpDir, String name) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                name, "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        // Never auto-select compaction candidates: the test never wants the
        // background loop to mutate the segment count under it.
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ Integer.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        return store;
    }

    /**
     * Builds {@code threshold + 1} sealed on-disk segments by inserting a batch
     * of vectors and checkpointing, with the deferral gate disabled so every
     * checkpoint actually seals a segment.
     *
     * @return the segment count after the build (always {@code > threshold}).
     */
    private int buildSegmentsAboveThreshold(PersistentVectorStore store, int threshold,
                                            Random rng) throws Exception {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        for (int c = 0; c <= threshold; c++) {
            for (int i = 0; i < 60; i++) {
                store.addVector(Bytes.from_int(c * 1_000 + i), randomVector(rng));
            }
            store.checkpoint();
        }
        int segs = store.getSegmentCount();
        assertTrue("setup must build more than " + threshold + " segments; got " + segs,
                segs > threshold);
        return segs;
    }

    /**
     * The core regression: while the segment count exceeds the back-pressure
     * threshold, checkpointing a tiny live shard (the trickle released by the
     * safety timeout) must be deferred — no micro-segment may be created.
     */
    @Test(timeout = 120_000)
    public void checkpointDeferredDuringSegmentCountBackpressure() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue562-defer").toPath();
        int threshold = 5;
        try (PersistentVectorStore store = newStore(tmpDir, "issue562a")) {
            store.start();
            Random rng = new Random(562_1);

            // Back-pressure disabled while we build the baseline segments so the
            // setup inserts never block.
            store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);
            int segs = buildSegmentsAboveThreshold(store, threshold, rng);

            // Re-enable the deferral gate and inject a tiny trickle of vectors —
            // mimics the ~3 vectors released by the back-pressure safety timeout.
            PersistentVectorStore.minLiveVectorsForCheckpoint = 1_000;
            for (int i = 0; i < 3; i++) {
                store.addVector(Bytes.from_int(900_000 + i), randomVector(rng));
            }

            // Enter segment-count back-pressure and checkpoint. The fix must
            // defer it: no new micro-segment, and the deferral counter ticks.
            store.setCompactionBackpressureThreshold(threshold);
            long deferredBefore = store.getTotalCheckpointsDeferred();

            boolean ran = store.checkpoint();

            assertFalse("checkpoint must be deferred while above the back-pressure threshold",
                    ran);
            assertEquals("no micro-segment may be created during back-pressure",
                    segs, store.getSegmentCount());
            assertEquals("the deferral counter must record the skipped checkpoint",
                    deferredBefore + 1, store.getTotalCheckpointsDeferred());
        }
    }

    /**
     * Once compaction relieves back-pressure (segment count no longer exceeds
     * the threshold) the unconditional gate must release and the normal,
     * time-bounded deferral resumes — the pending trickle is sealed.
     */
    @Test(timeout = 120_000)
    public void checkpointResumesWhenBackpressureRelieved() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue562-resume").toPath();
        int threshold = 5;
        try (PersistentVectorStore store = newStore(tmpDir, "issue562b")) {
            store.start();
            Random rng = new Random(562_2);

            store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);
            int segs = buildSegmentsAboveThreshold(store, threshold, rng);

            PersistentVectorStore.minLiveVectorsForCheckpoint = 1_000;
            for (int i = 0; i < 3; i++) {
                store.addVector(Bytes.from_int(900_000 + i), randomVector(rng));
            }

            // While above the threshold the checkpoint is deferred.
            store.setCompactionBackpressureThreshold(threshold);
            assertFalse("checkpoint must be deferred while above the threshold",
                    store.checkpoint());
            assertEquals("no micro-segment may be created during back-pressure",
                    segs, store.getSegmentCount());

            // Simulate compaction relieving back-pressure: raise the threshold so
            // the segment count no longer exceeds it. The unconditional gate must
            // release; with the time-bounded deferral disabled the checkpoint now
            // runs and seals the pending trickle into exactly one segment.
            store.setCompactionBackpressureThreshold(1_000);
            PersistentVectorStore.maxCheckpointDeferralMs = 0;

            boolean ran = store.checkpoint();

            assertTrue("checkpoint must run once back-pressure is relieved", ran);
            assertEquals("the pending trickle must now be sealed into one segment",
                    segs + 1, store.getSegmentCount());
        }
    }

    /**
     * The compaction {@code minCount} is now sourced from the
     * {@code herddb.vectorindex.compactionMinCount} system property via
     * {@link PersistentVectorStore#DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT}.
     * With the property unset it falls back to {@code 4}, and a freshly built
     * store (no {@code configureCompaction} call) must adopt that constant.
     */
    @Test(timeout = 60_000)
    public void compactionMinCountDefaultsToConfigurableConstant() throws Exception {
        assertEquals("default compactionMinCount must be 4 when the system property is unset",
                4, PersistentVectorStore.DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT);

        Path tmpDir = tmpFolder.newFolder("issue562-mincount").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "issue562c", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE)) {
            // No configureCompaction() call — the field default must come from
            // the configurable constant.
            assertEquals("the vectorIndexCompactionMinCount field must default to the constant",
                    PersistentVectorStore.DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT,
                    store.getVectorIndexCompactionMinCount());
        }
    }
}
