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
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #646.
 *
 * <p>Asserts that {@link PersistentVectorStore#setEarlyCheckpointFraction(double)}
 * (a) rejects values outside {@code (0.0, 1.0]}, (b) defaults to {@code 0.90},
 * and (c) actually drives whether
 * {@link PersistentVectorStore#shouldTriggerMemoryPressureCheckpoint()} fires
 * at intermediate budget pressures. The historical pre-#646 default was 0.70
 * — a value low enough that long ingest workloads spent their middle phase
 * permanently above the threshold, bypassing the
 * {@code minLiveVectorsForCheckpoint} gate and producing the BIGANN 200M
 * micro-segment storm.
 */
public class EarlyCheckpointFractionConfigTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 8;

    /** A budget whose usage and ceiling are owned by the caller. */
    private static final class TunableBudget implements VectorMemoryBudget {
        final AtomicLong usage;
        final long max;

        TunableBudget(long initialUsage, long max) {
            this.usage = new AtomicLong(initialUsage);
            this.max = max;
        }

        @Override
        public long totalEstimatedMemoryUsageBytes() {
            return usage.get();
        }

        @Override
        public long maxMemoryBytes() {
            return max;
        }

        @Override
        public boolean isMemoryPressureActive() {
            // Never report hard back-pressure so addVector stays unblocked
            // even when the budget reports very high usage.
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
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN,
                Long.MAX_VALUE /* maxVectorMemoryBytes */, budget, 0, 0);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    @Test
    public void defaultFractionIsNinetyPercent() throws Exception {
        Path tmpDir = tmpFolder.newFolder("default-fraction").toPath();
        try (PersistentVectorStore store = createStore(tmpDir, null)) {
            store.start();
            // The PVS-side default is the new post-#646 0.90 — applied even
            // when the store is built via a direct constructor (the engine
            // factory passes the same value via setEarlyCheckpointFraction).
            assertEquals("post-#646 default early-checkpoint fraction is 0.90",
                    0.90d, store.getEarlyCheckpointFraction(), 1e-12);
        }
    }

    @Test
    public void setterRejectsOutOfRangeValues() throws Exception {
        Path tmpDir = tmpFolder.newFolder("setter-range").toPath();
        try (PersistentVectorStore store = createStore(tmpDir, null)) {
            store.start();
            for (double bad : new double[]{0.0d, -0.1d, -1.0d, 1.0001d, 2.0d, Double.NaN,
                    Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
                try {
                    store.setEarlyCheckpointFraction(bad);
                    fail("expected IllegalArgumentException for fraction " + bad);
                } catch (IllegalArgumentException expected) {
                    // The operator-visible diagnostic must include the offending
                    // value so a misconfigured IndexingServer (which propagates
                    // the IAE via start()) tells the operator what to fix.
                    assertTrue("IAE message must contain the offending value (was: \""
                                    + expected.getMessage() + "\")",
                            expected.getMessage().contains(String.valueOf(bad)));
                }
            }
            // The boundary value 1.0 is accepted (no bypass until the hard
            // 100% limit is reached).
            store.setEarlyCheckpointFraction(1.0d);
            assertEquals(1.0d, store.getEarlyCheckpointFraction(), 1e-12);
            // And ordinary values round-trip.
            store.setEarlyCheckpointFraction(0.50d);
            assertEquals(0.50d, store.getEarlyCheckpointFraction(), 1e-12);
        }
    }

    /**
     * The decisive end-to-end assertion: with a budget reporting 80% usage
     * (above the pre-#646 0.70 default, below the post-#646 0.90 default), the
     * trigger fires at 0.70 but is suppressed at 0.90 — i.e. the
     * {@code minLiveVectorsForCheckpoint} gate would no longer be bypassed in
     * the very 70&ndash;90% range that produced the BIGANN 200M micro-segment
     * storm.
     */
    @Test
    public void fractionDrivesTriggerInTheMiddleRange() throws Exception {
        Path tmpDir = tmpFolder.newFolder("middle-range").toPath();
        TunableBudget budget = new TunableBudget(80L, 100L); // 80%
        try (PersistentVectorStore store = createStore(tmpDir, budget)) {
            store.start();
            // Plant one live vector so the issue #563 zero-live-vectors guard
            // does not short-circuit the trigger.
            store.addVector(Bytes.from_int(1), new float[DIM]);

            // 0.70: 80 > 70, trigger fires (pre-#646 behaviour).
            store.setEarlyCheckpointFraction(0.70d);
            assertTrue("at 70% threshold, 80% usage must fire the early-checkpoint"
                    + " trigger (pre-#646 behaviour)",
                    store.shouldTriggerMemoryPressureCheckpoint());

            // 0.90 (new default): 80 < 90, trigger suppressed — the
            // minLiveVectorsForCheckpoint gate stays active.
            store.setEarlyCheckpointFraction(0.90d);
            assertFalse("at 90% threshold, 80% usage must NOT fire — this is the"
                    + " whole point of issue #646 (gate stays active in 70-90% range)",
                    store.shouldTriggerMemoryPressureCheckpoint());

            // Crank the usage past 90% and confirm the post-#646 default
            // still fires when warranted.
            budget.usage.set(95L);
            assertTrue("at 90% threshold, 95% usage must fire the trigger",
                    store.shouldTriggerMemoryPressureCheckpoint());
        }
    }
}
