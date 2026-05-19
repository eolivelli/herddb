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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the {@code indexing.optimizer.enabled} flag, plumbed through
 * {@link PersistentVectorStore#setExternalCompactionEnabled}.
 *
 * <p>The flag's effect on the local compaction loop changed when the
 * pressure-driven IS-local fallback landed:
 * <ul>
 *   <li>In legacy mode (flag = {@code false}), the loop runs every cycle as
 *       it always has.</li>
 *   <li>With the optimizer enabled (flag = {@code true}), the loop thread
 *       still <em>starts</em> but its body short-circuits below the kick
 *       threshold — steady state is optimizer-driven; the IS only fires
 *       cycles as a fallback when segment count crosses
 *       {@code kickFraction × backpressureThreshold}.</li>
 *   <li>The hard opt-out path (operator setting
 *       {@code localCompactionEnabledWithOptimizer = false}) restores the
 *       pre-fallback "loop runs but does nothing" semantics.</li>
 * </ul>
 */
public class PersistentVectorStoreExternalCompactionFlagTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private Thread compactionThreadOf(PersistentVectorStore store) throws Exception {
        Method m = PersistentVectorStore.class.getDeclaredMethod("getVectorIndexCompactionThread");
        m.setAccessible(true);
        return (Thread) m.invoke(store);
    }

    @Test
    public void legacyModeStartsCompactionLoop() throws Exception {
        Path tmpDir = tmpFolder.newFolder("legacy").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            // No setExternalCompactionEnabled call — default is legacy.
            assertFalse(store.isExternalCompactionEnabled());
            store.start();
            Thread t = compactionThreadOf(store);
            assertNotNull("legacy mode must launch the in-IS compaction thread", t);
            assertTrue("compaction thread must be alive in legacy mode", t.isAlive());
            assertEquals("persistent-vector-store-vidxcompaction-testidx", t.getName());
        }
    }

    @Test
    public void externalCompactionModeRunsLocalLoopAsPressureDrivenFallback() throws Exception {
        // The in-IS compaction thread is no longer suppressed — it runs as a
        // pressure-driven fallback. Below the kick threshold the cycle body
        // short-circuits (skip counter increments), so the loop is harmless
        // even though the thread exists.
        Path tmpDir = tmpFolder.newFolder("external").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.setExternalCompactionEnabled(true);
            assertTrue(store.isExternalCompactionEnabled());
            store.start();
            Thread t = compactionThreadOf(store);
            assertNotNull("external compaction mode must STILL launch the IS-local compaction"
                    + " thread — it is now a pressure-driven fallback that short-circuits"
                    + " below the kick threshold (steady state stays optimizer-driven)", t);
            assertTrue("compaction thread must be alive in external-compaction mode", t.isAlive());

            // Sanity: tailer + checkpoint path still works.
            int dim = 32;
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            assertTrue(store.checkpoint());

            // Drive a cycle directly: with no segments accumulated above the
            // kick threshold the cycle must short-circuit (pressure-runs counter
            // stays at 0; skip counter advances).
            store.runCompactionCycle();
            assertEquals("local compaction must NOT run a pressure cycle below the kick threshold",
                    0L, store.getLocalCompactionPressureRunsTotal());
            assertTrue("the skip counter must have advanced — proves the gate fired",
                    store.getLocalCompactionSkippedBelowThresholdTotal() >= 1L);
        }
    }

    @Test
    public void externalCompactionWithLocalFallbackDisabledIsFullDelegation() throws Exception {
        // Operator hard-opt-out: enabledWithOptimizer = false. The thread is
        // still started (so an operator can flip the master switch back at
        // runtime without restarting the IS), but the cycle body returns
        // unconditionally — neither the pressure-runs nor the skip counter
        // ever advance.
        Path tmpDir = tmpFolder.newFolder("external-disabled").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.setExternalCompactionEnabled(true);
            store.setLocalCompactionEnabledWithOptimizer(false);
            store.start();
            Thread t = compactionThreadOf(store);
            assertNotNull("thread must still launch so the master switch can be flipped at runtime", t);

            store.runCompactionCycle();
            assertEquals(0L, store.getLocalCompactionPressureRunsTotal());
            assertEquals("opt-out path must NOT bump the threshold-skip counter — that"
                            + " counter is reserved for cycles that genuinely fell below"
                            + " the kick threshold while the fallback was enabled",
                    0L, store.getLocalCompactionSkippedBelowThresholdTotal());
        }
    }

    @Test
    public void flagDefaultsToFalse() throws Exception {
        Path tmpDir = tmpFolder.newFolder("default").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            assertFalse("flag must default to false (legacy mode)", store.isExternalCompactionEnabled());
        }
    }

    @Test
    public void flagPropertyConstantIsAvailableInIndexingServerConfiguration() {
        // Smoke check: the new property constant must be wired up where the IS will read it.
        assertEquals("indexing.optimizer.enabled",
                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED);
        assertFalse(IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED_DEFAULT);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }
}
