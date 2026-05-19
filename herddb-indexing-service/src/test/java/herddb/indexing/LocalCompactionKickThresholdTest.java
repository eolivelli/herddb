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
import herddb.indexing.vector.PersistentVectorStore;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Random;
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

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
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
        //
        // Crucial that this test ACTUALLY reach high pressure — running cycles
        // against an empty segment list would pass even if the order of the
        // two early-return checks were inverted (since both branches return).
        // To demonstrate that the opt-out short-circuit is unconditional, we
        // accumulate real segments above the kick threshold via repeated tiny
        // checkpoints.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Tiny back-pressure threshold so we can cross it within the test
            // budget. 0.5 × 4 = 2 → kickThreshold = 2.
            store.setCompactionBackpressureThreshold(4);
            store.setLocalCompactionKickFraction(0.5d);
            store.setLocalCompactionEnabledWithOptimizer(false);
            store.setExternalCompactionEnabled(true);
            store.start();

            // Drive segment count above the kick threshold via 4 small
            // checkpoints — each seals one segment.
            Random rng = new Random(7);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }
            assertTrue("test must actually be at high pressure (≥ kickThreshold) for the"
                            + " assertion below to mean anything; saw " + store.getSegmentCount()
                            + " segments vs kickThreshold " + store.currentLocalCompactionKickThreshold(),
                    store.getSegmentCount() >= store.currentLocalCompactionKickThreshold());

            // Run cycles directly. Even though pressure is real, the opt-out
            // path must short-circuit BEFORE the threshold check — so neither
            // counter advances.
            for (int i = 0; i < 5; i++) {
                store.runCompactionCycle();
            }
            assertEquals("opt-out path must skip local compaction even when"
                            + " segment count exceeds the kick threshold",
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

    @Test
    public void cachedKickThresholdMatchesFreshComputationAtConstruction() throws Exception {
        // Defense-in-depth: PersistentVectorStore initializes
        // {@code kickThresholdCached} with a hand-rolled formula that must
        // stay byte-equal to what {@code recomputeKickThresholdCached()} would
        // compute from the configured defaults. If a future commit changes
        // either default ({@code localCompactionKickFraction = 0.7d} or
        // {@code compactionBackpressureThreshold = 500}) but forgets to
        // update the field-initializer formula, this test fails immediately
        // — without it, the cached threshold would silently drift away from
        // the configured defaults until either setter happened to fire.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            int initial = store.currentLocalCompactionKickThreshold();
            // Explicitly re-set to the SAME defaults; either setter triggers
            // recomputeKickThresholdCached(), giving us the canonical value.
            // If the field-initializer drifted from the recompute formula,
            // {@code initial} would NOT equal {@code recomputed}.
            store.setCompactionBackpressureThreshold(500);
            store.setLocalCompactionKickFraction(0.7d);
            int recomputed = store.currentLocalCompactionKickThreshold();
            assertEquals("kickThresholdCached field initializer must match"
                            + " recomputeKickThresholdCached()'s formula at the configured"
                            + " defaults — mismatch means the cache silently drifted from"
                            + " what the setter would compute",
                    recomputed, initial);
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
