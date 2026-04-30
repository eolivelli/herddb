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
import herddb.file.FileDataStorageManager;
import herddb.index.vector.PersistentVectorStore;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates the pressure-driven IS-local compaction kick threshold introduced
 * alongside the external index-optimizer. When the optimizer is enabled, the
 * IS-local loop must NOT run below {@code kickFraction × backpressureThreshold},
 * but MUST run above it (steady-state stays optimizer-driven; the IS only
 * kicks in as a fallback when accumulation indicates the optimizer is falling
 * behind).
 */
public class LocalCompactionKickThresholdTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "kick";
    private static final String INDEX_UUID = "kick_idx_uuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    @Test
    public void belowThresholdSkipsLocalCompactionWhenOptimizerEnabled() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // 100 segments threshold so 0.7 × 100 = 70 = kick threshold.
            store.setCompactionBackpressureThreshold(100);
            store.setLocalCompactionKickFraction(0.7d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            store.start();

            // Sanity: kick threshold matches expectation.
            assertEquals(70, store.currentLocalCompactionKickThreshold());

            // Drive multiple cycles directly. Segment list is empty, so every
            // cycle must short-circuit and bump the skip counter.
            for (int i = 0; i < 5; i++) {
                store.runCompactionCycle();
            }
            assertEquals("local compaction must NOT run below kick threshold",
                    0L, store.getLocalCompactionPressureRunsTotal());
            assertTrue("the skip counter must show the cycles short-circuited (saw "
                            + store.getLocalCompactionSkippedBelowThresholdTotal() + ")",
                    store.getLocalCompactionSkippedBelowThresholdTotal() >= 5L);
        }
    }

    @Test
    public void disabledLocalFallbackSkipsEvenAtHighPressure() throws Exception {
        // Operator opt-out path: enabledWithOptimizer=false restores the
        // pre-fallback behaviour (full delegation to the optimizer, no IS-local
        // compaction even at extreme pressure).
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(100);
            store.setLocalCompactionKickFraction(0.7d);
            store.setLocalCompactionEnabledWithOptimizer(false);
            store.setExternalCompactionEnabled(true);
            store.start();

            for (int i = 0; i < 5; i++) {
                store.runCompactionCycle();
            }
            // Both counters stay at zero — the early-return is unconditional
            // (it never reaches the threshold check or the work path).
            assertEquals("opt-out path must skip local compaction unconditionally",
                    0L, store.getLocalCompactionPressureRunsTotal());
            assertEquals("opt-out path must NOT bump the threshold-skip counter — that"
                            + " counter is reserved for cycles that genuinely fell below"
                            + " the kick threshold while the fallback was enabled",
                    0L, store.getLocalCompactionSkippedBelowThresholdTotal());
        }
    }

    @Test
    public void kickThresholdRoundsCeilingAndIsAtLeastOne() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Pure unit-style check on the helper (no start needed).
            store.setCompactionBackpressureThreshold(10);
            store.setLocalCompactionKickFraction(0.7d);
            assertEquals("ceil(0.7 * 10) = 7", 7, store.currentLocalCompactionKickThreshold());

            // Edge: very small backpressure threshold + small fraction should still
            // produce a usable threshold (>= 1) — otherwise the gate would be
            // permanently open at any segment count.
            store.setCompactionBackpressureThreshold(1);
            store.setLocalCompactionKickFraction(0.001d);
            assertEquals("threshold floors at 1 even when ceil rounds up below 1",
                    1, store.currentLocalCompactionKickThreshold());

            // Saturation: an extremely large backpressure threshold must not
            // overflow the int return type.
            store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);
            store.setLocalCompactionKickFraction(0.99d);
            int sat = store.currentLocalCompactionKickThreshold();
            assertTrue("saturated value must stay positive and ≤ Integer.MAX_VALUE",
                    sat > 0 && sat <= Integer.MAX_VALUE);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void kickFractionMustBeStrictlyBetweenZeroAndOne() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // 1.0 is rejected: a kick threshold equal to the back-pressure
            // ceiling means the local fallback never runs before the tailer
            // stalls, defeating the whole point of the fallback. Catching
            // this at the setter beats the operator finding it in production.
            store.setLocalCompactionKickFraction(1.0d);
        }
    }
}
