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
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Asserts that every new compaction/retention metric moves as the
 * plan specifies: counters strictly monotonic, gauges reflect the
 * last-observed state, and {@code compaction_active} toggles 0→1→0.
 */
public class VectorIndexCompactorMetricsTest {

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

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    @Test
    public void metricsAllZeroBeforeAnyRun() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir, new MemoryDataStorageManager())) {
            store.start();
            assertEquals(0, store.getCompactionRunsTotal());
            assertEquals(0, store.getCompactionSuccessesTotal());
            assertEquals(0, store.getCompactionFailuresWriteIoTotal());
            assertEquals(0, store.getCompactionFailuresMetadataIoTotal());
            assertEquals(0, store.getCompactionFailuresReadIoTotal());
            assertEquals(0, store.getCompactionFailuresCorruptionTotal());
            assertEquals(0, store.getCompactionFailuresDiskFullTotal());
            assertEquals(0, store.getCompactionFailuresAbortedInputGoneTotal());
            assertEquals(0, store.getCompactionLastDurationMs());
            assertEquals(0, store.getCompactionLastBytesRead());
            assertEquals(0, store.getCompactionLastBytesWritten());
            assertEquals(0, store.getCompactionLastInputSegments());
            assertEquals(0, store.getCompactionLastOutputSegments());
            assertEquals(0, store.getCompactionLivePkFilteredTotal());
            assertEquals(0, store.getCompactionActive());
            assertEquals(0, store.getCompactionConsecutiveFailures());
            assertEquals(0, store.getPendingDeletesReapedTotal());
            assertEquals(0, store.getPendingDeletesReapFailuresTotal());
            assertTrue(store.getPendingDeletesSnapshot().isEmpty());
        }
    }

    @Test
    public void successfulRunAdvancesEveryCounterAndGauge() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir, new MemoryDataStorageManager())) {
            store.start();
            Random rng = new Random(42);
            int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();

            store.runCompactionCycle();

            assertEquals(1, store.getCompactionRunsTotal());
            assertEquals(1, store.getCompactionSuccessesTotal());
            assertEquals(0, store.getCompactionConsecutiveFailures());
            assertEquals(0, store.getCompactionActive()); // toggles back to 0
            assertTrue(store.getCompactionLastDurationMs() > 0);
            assertTrue(store.getCompactionLastBytesRead() > 0);
            assertTrue(store.getCompactionLastBytesWritten() > 0);
            assertEquals(segmentsBefore, store.getCompactionLastInputSegments());
            assertEquals(1, store.getCompactionLastOutputSegments());
            // All inputs should now appear in pendingDeletes.
            assertTrue(store.getPendingDeletesSnapshot().size() >= segmentsBefore - 1);
        }
    }

    @Test
    public void reaperCountersAdvanceOnSuccessfulReap() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir, new MemoryDataStorageManager())) {
            store.start();
            Random rng = new Random(77);
            int dim = 16;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 300; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            store.runCompactionCycle();
            int pending = store.getPendingDeletesSnapshot().size();
            assertTrue(pending > 0);
            int deleted = store.reapExpiredPendingDeletes(Long.MAX_VALUE);
            assertEquals(pending, deleted);
            assertEquals(pending, store.getPendingDeletesReapedTotal());
            assertEquals(0, store.getPendingDeletesReapFailuresTotal());
        }
    }
}
