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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for issue #372: pendingDeletes BLink leaked when
 * {@code createTempCompactionAuthorityMap()} throws in
 * {@code runCompactionCycle()}.
 *
 * <p>Before the fix the two allocations were sequenced as:
 * <pre>
 *   pendingDeletes = createTempPagedPkSet(...);  // outside try
 *   authority      = createTempCompactionAuthorityMap(...);  // outside try, can throw
 *   try { ... } finally { pendingDeletes.close(); }
 * </pre>
 * If the authority-map allocation threw a {@link RuntimeException}, the
 * {@code try} block was never entered, so the {@code finally} was never
 * reached, and {@code pendingDeletes} was never closed, leaking its BLink
 * pages and temp storage index.
 *
 * <p>After the fix {@code authority} is declared {@code null} before the
 * {@code try} and assigned as its very first statement, so any exception
 * from that call still lets the {@code finally} run and close
 * {@code pendingDeletes}.
 */
public class Issue372PendingDeletesLeakTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Saved original so {@code @After} can restore without hard-coding a constant. */
    private int originalMinLiveVectorsForCheckpoint;

    @Before
    public void disableDeferral() {
        originalMinLiveVectorsForCheckpoint = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = originalMinLiveVectorsForCheckpoint;
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int d = 0; d < dim; d++) {
            v[d] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Regression test: verifies that the BLink-backed {@code pendingDeletes} set
     * is properly closed (i.e. {@code dropIndex} is invoked for its temp store
     * name) even when {@code createTempCompactionAuthorityMap()} throws a
     * {@link RuntimeException} wrapping a {@link DataStorageManagerException}.
     *
     * <p>The test injects a failure into {@code initIndex()} for any store name
     * containing {@code "_tmp_cmpaut_"} (the authority-map name pattern) and
     * tracks all {@code dropIndex()} calls via a concurrent set. After the
     * failed compaction cycle it asserts:
     * <ol>
     *   <li>The cycle threw a {@code RuntimeException} as expected.</li>
     *   <li>A {@code dropIndex} call was recorded for a store name containing
     *       {@code "_tmp_pkset_cmpdel_"} — proving that {@code pendingDeletes.close()}
     *       was reached despite the earlier exception.</li>
     * </ol>
     */
    @Test(timeout = 30_000)
    public void pendingDeletesClosedWhenAuthorityMapInitFails() throws Exception {
        Path tmpDir = tmpFolder.newFolder("leak372").toPath();

        // Track which store names have been dropped so we can verify cleanup.
        Set<String> droppedStores = Collections.newSetFromMap(new ConcurrentHashMap<>());

        MemoryDataStorageManager dsm = new MemoryDataStorageManager() {
            @Override
            public void initIndex(String tableSpace, String name)
                    throws DataStorageManagerException {
                // Fail the authority-map allocation to reproduce the leak scenario.
                if (name.contains("_tmp_cmpaut_")) {
                    throw new DataStorageManagerException(
                            "Injected authority-map init failure for test: " + name);
                }
                super.initIndex(tableSpace, name);
            }

            @Override
            public void dropIndex(String tableSpace, String name)
                    throws DataStorageManagerException {
                droppedStores.add(name);
                super.dropIndex(tableSpace, name);
            }
        };

        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx372", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        // Compaction must select candidates (minBytes=1, minCount=2) so the
        // allocation path is reached; the failure is injected at authority-map init.
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE); // no back-pressure

        try (store) {
            store.start();
            Random rng = new Random(372_1);
            int dim = 8;

            // Build several segments so runCompactionCycle() finds candidates.
            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < 20; i++) {
                    store.addVector(Bytes.from_int(c * 100 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            // Drive a compaction cycle; createTempCompactionAuthorityMap will throw.
            try {
                store.runCompactionCycle();
                fail("expected RuntimeException from injected DSM failure");
            } catch (RuntimeException expected) {
                // Good — the injected failure propagated as expected.
            }

            // Verify that pendingDeletes was closed: dropIndex must have been
            // called for the cmpdel_ temp store name.
            boolean pendingDeletesDropped = droppedStores.stream()
                    .anyMatch(name -> name.contains("_tmp_pkset_cmpdel_"));
            assertTrue(
                    "pendingDeletes BLink temp store must be dropped (close() must be called) "
                            + "even when createTempCompactionAuthorityMap throws. "
                            + "Dropped stores: " + droppedStores,
                    pendingDeletesDropped);

            // No authority-map temp store should have been dropped because its
            // allocation failed before the BLink was created — there is nothing to drop.
            boolean authorityDropped = droppedStores.stream()
                    .anyMatch(name -> name.contains("_tmp_cmpaut_"));
            assertFalse(
                    "authority-map temp store must NOT be dropped — its allocation failed "
                            + "before BLink construction. Dropped stores: " + droppedStores,
                    authorityDropped);
        }
    }
}
