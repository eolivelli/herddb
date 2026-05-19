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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Compaction must replay deletes that arrive mid-flight against the
 * freshly-rebuilt merged segment before publishing it.
 */
public class VectorIndexCompactorConcurrentDeleteTest {

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

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void deleteArrivingDuringCompactionLandsOnMergedOutput() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            Random rng = new Random(7);
            int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertTrue(store.getSegmentCount() >= 4);

            // Pick a PK that we *know* survived every prior checkpoint and
            // will be in the merged output unless compaction replays the
            // concurrent delete.
            Bytes targetPk = Bytes.from_int(0); // inserted in first batch

            // Kick compaction on a background thread, delete mid-flight.
            CountDownLatch compactionDone = new CountDownLatch(1);
            Thread compactor = new Thread(() -> {
                try {
                    store.runCompactionCycle();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    compactionDone.countDown();
                }
            }, "compactor-test-thread");
            compactor.start();

            // Wait until compaction is actively running, then delete.
            long deadline = System.currentTimeMillis() + 5000;
            while (store.getCompactionActive() == 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(1);
            }
            assertEquals("compaction should have started", 1,
                    store.getCompactionActive());
            store.removeVector(targetPk);

            assertTrue("compaction must finish",
                    compactionDone.await(30, TimeUnit.SECONDS));
            compactor.join();
            assertEquals(1, store.getCompactionSuccessesTotal());

            // The deleted PK must not appear in the merged segment: every
            // search in a large candidate set that matches should skip it.
            Random probe = new Random(42);
            boolean foundTarget = false;
            for (int i = 0; i < 200; i++) {
                List<Map.Entry<Bytes, Float>> hits = store.search(vec(probe, dim), 50);
                for (Map.Entry<Bytes, Float> h : hits) {
                    if (h.getKey().equals(targetPk)) {
                        foundTarget = true;
                        break;
                    }
                }
                if (foundTarget) {
                    break;
                }
            }
            assertTrue("target PK must be gone from merged output",
                    !foundTarget);
        }
    }
}
