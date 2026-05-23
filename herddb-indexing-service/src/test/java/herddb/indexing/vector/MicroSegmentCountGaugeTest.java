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
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #646 — micro-segment count gauge.
 *
 * <p>Asserts that {@link PersistentVectorStore#getMicroSegmentCount()} reports
 * the number of currently-loaded segments whose live-node count is strictly
 * below the configured {@code microSegmentMaxNodes} threshold (issue #570).
 * The gauge is the real-time signal operators use to spot micro-segment
 * accumulation before the segment-count back-pressure kicks in.
 */
public class MicroSegmentCountGaugeTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 8;

    private int savedMinLiveVectorsForCheckpoint;

    @Before
    public void disableMinLiveVectorsGate() {
        savedMinLiveVectorsForCheckpoint = PersistentVectorStore.minLiveVectorsForCheckpoint;
        // 0 so every explicit checkpoint() in the test seals the partial
        // shard immediately into a new segment.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreMinLiveVectorsGate() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectorsForCheckpoint;
    }

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vec",
                "vidx_uuid", tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true /* fusedPQ */, 2_000_000_000L, 0,
                /* compactionIntervalMs */ Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN,
                Long.MAX_VALUE /* maxVectorMemoryBytes */, null, 0, 0);
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

    @Test(timeout = 60_000)
    public void gaugeIsZeroWhenThresholdIsDisabled() throws Exception {
        Path tmpDir = tmpFolder.newFolder("disabled").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.setCompactionMicroSegmentMaxNodes(0L); // disabled
            store.start();
            Random rng = new Random(1);
            for (int i = 0; i < 30; i++) {
                store.addVector(Bytes.from_int(i), vec(rng));
            }
            assertTrue(store.checkpoint());
            assertTrue("at least one segment must have been created",
                    store.getSegmentCount() >= 1);
            assertEquals("threshold=0 disables the fast-path and the gauge",
                    0, store.getMicroSegmentCount());
        }
    }

    @Test(timeout = 60_000)
    public void gaugeCountsSegmentsBelowThreshold() throws Exception {
        Path tmpDir = tmpFolder.newFolder("count").toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.setCompactionMicroSegmentMaxNodes(50L);
            store.start();
            Random rng = new Random(42);
            // Three small segments: 20, 25, 30 vectors → all < 50 → all
            // count as micro-segments.
            int next = 0;
            for (int batch : new int[]{20, 25, 30}) {
                for (int i = 0; i < batch; i++, next++) {
                    store.addVector(Bytes.from_int(next), vec(rng));
                }
                assertTrue(store.checkpoint());
            }
            assertEquals("three small batches must produce three segments",
                    3, store.getSegmentCount());
            assertEquals("all three segments are micro-segments (<50 nodes each)",
                    3, store.getMicroSegmentCount());

            // Add a large segment (>= threshold). Gauge stays at 3.
            for (int i = 0; i < 100; i++, next++) {
                store.addVector(Bytes.from_int(next), vec(rng));
            }
            assertTrue(store.checkpoint());
            assertEquals("a fourth segment was added but only the three small ones"
                    + " are micro-segments",
                    3, store.getMicroSegmentCount());
            assertEquals(4, store.getSegmentCount());

            // Tighten the threshold so the 100-node segment also qualifies.
            store.setCompactionMicroSegmentMaxNodes(150L);
            assertEquals("raising the threshold must reclassify the 100-node segment",
                    4, store.getMicroSegmentCount());

            // Loosen it so only the smallest two qualify (< 26).
            store.setCompactionMicroSegmentMaxNodes(26L);
            assertEquals("only the 20- and 25-node segments must remain micro",
                    2, store.getMicroSegmentCount());
        }
    }
}
