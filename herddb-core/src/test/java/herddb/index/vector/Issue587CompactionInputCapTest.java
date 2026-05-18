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
 * End-to-end test for issue #587: the per-cycle input-segment cap must bound
 * how many segments a single compaction cycle merges <em>without</em> starving
 * compaction — successive cycles must keep draining the backlog until it
 * converges.
 *
 * <p>This is the multi-cycle counterpart to {@link VectorIndexCompactorChooseTest},
 * which only exercises the {@code chooseSegmentsToMerge} selection in isolation.
 * Here the cap is left at its default-enabled value (16) and a real
 * {@link PersistentVectorStore} drives several compaction cycles, proving that:
 * <ul>
 *   <li>every cycle merges at most the (tier-scaled) cap of input segments, and</li>
 *   <li>the segment count strictly decreases on every compacting cycle and the
 *       backlog converges — i.e. the cap cannot let the segment count grow
 *       unboundedly toward the back-pressure threshold.</li>
 * </ul>
 */
public class Issue587CompactionInputCapTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    @Test
    public void compactionDrainsBacklogAcrossCyclesWithCapEnabled() throws Exception {
        Path tmpDir = tmpFolder.newFolder("issue-587-cap").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        // Large cache to avoid the MemoryDataStorageManager namespace collision
        // documented in Issue354TieredCompactionTest.
        MemoryManager mm = new MemoryManager(2048L * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx587", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1L,                  // byte trigger fires immediately
                /*maxBytes*/ Long.MAX_VALUE,      // never byte-limited
                /*minCount*/ 2,
                /*maxCount*/ Integer.MAX_VALUE,   // isolate the byte trigger
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(true);
        // Disable backpressure so the backlog can be built freely; the test
        // then proves the cap drains it without relying on back-pressure.
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);
        // Disable the micro-segment fast path: this test's tiny checkpoints
        // would otherwise all be micro-segments and take the (uncapped, by
        // design) #570 fast path. The incident in #587 merged large segments
        // via the normal path, which is the path the cap targets.
        store.setCompactionMicroSegmentMaxNodes(0);
        // Leave the input cap at its default-enabled value — this is the whole
        // point of the test.
        assertEquals("cap must be enabled by default",
                PersistentVectorStore.DEFAULT_VECTOR_INDEX_COMPACTION_MAX_INPUTS,
                store.getCompactionMaxInputs());
        final int cap = store.getCompactionMaxInputs();

        try (store) {
            store.start();
            Random rng = new Random(587);
            int dim = 8;

            // Build 50 on-disk segments — well above the cap of 16, but below
            // the tier-1 threshold (100) so the effective cap stays flat at 16
            // and the test exercises the strict multi-cycle drain.
            int numCheckpoints = 50;
            for (int c = 0; c < numCheckpoints; c++) {
                for (int i = 0; i < 5; i++) {
                    float[] vec = new float[dim];
                    for (int d = 0; d < dim; d++) {
                        vec[d] = rng.nextFloat();
                    }
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec);
                }
                store.checkpoint();
            }

            int segments = store.getSegmentCount();
            assertTrue("expected a backlog well above the cap, got " + segments,
                    segments > cap);

            int compactingCycles = 0;
            long successes = store.getCompactionSuccessesTotal();
            // Drive cycles until the backlog is drained. The bound (60) is far
            // above the ~4 cycles a correct implementation needs; exhausting it
            // means the cap starved compaction — a test failure.
            for (int cycle = 0; cycle < 60 && store.getSegmentCount() > 3; cycle++) {
                int before = store.getSegmentCount();
                store.runCompactionCycle();
                int after = store.getSegmentCount();

                long successesNow = store.getCompactionSuccessesTotal();
                if (successesNow > successes) {
                    // A compaction actually ran this cycle.
                    successes = successesNow;
                    compactingCycles++;
                    long merged = store.getCompactionLastInputSegments();
                    assertTrue("each cycle must merge at most the cap (" + cap
                                    + ") input segments; merged=" + merged,
                            merged <= cap);
                    assertTrue("a compacting cycle must merge at least 2 inputs; merged="
                                    + merged, merged >= 2);
                    assertTrue("segment count must strictly decrease on a compacting "
                                    + "cycle: " + before + " -> " + after,
                            after < before);
                }
            }

            assertTrue("backlog must converge below the cap; got "
                            + store.getSegmentCount(),
                    store.getSegmentCount() <= cap);
            assertTrue("a >cap backlog must take several capped cycles to drain; "
                            + "compactingCycles=" + compactingCycles,
                    compactingCycles >= 2);
        }
    }
}
