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
 * Regression test for issue #285: count-based compaction trigger.
 *
 * <p>In a catch-up scenario the Indexing Service creates many small segments
 * whose individual byte sizes never reach the {@code minBytes} threshold, so
 * the standard byte trigger never fires and segments accumulate indefinitely.
 * The fix adds a {@code maxCount} knob: when the number of non-sealed segments
 * reaches or exceeds {@code maxCount} the compaction cycle fires regardless of
 * the byte total.
 *
 * <p>This test uses a very high {@code minBytes} (effectively unreachable with
 * tiny vectors) and a low {@code maxCount} to verify that compaction fires
 * purely on segment count.
 */
public class Issue285SegmentCountCompactionTest {

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

    /**
     * Builds a store whose byte threshold is unreachably high (Long.MAX_VALUE)
     * but whose maxCount is set to a small number, then inserts enough vectors
     * across multiple checkpoints to exceed maxCount and verifies that
     * {@link PersistentVectorStore#runCompactionCycle()} fires.
     */
    @Test
    public void countTriggerFiresWhenBytesNeverReachThreshold() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // maxCount = 6: compaction fires once we have >= 6 non-sealed segments.
        // minBytes = Long.MAX_VALUE / 2: never reachable with tiny test vectors.
        int maxCount = 6;
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx285", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE / 2,   // unreachably high
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ maxCount,
                /*retentionMs*/ 0);

        try (store) {
            store.start();
            Random rng = new Random(285);
            int dim = 8;

            // Create 8 checkpoints of 200 vectors each = 8 on-disk segments.
            // Byte total is tiny — the byte trigger cannot fire.
            int numCheckpoints = 8;
            for (int c = 0; c < numCheckpoints; c++) {
                for (int i = 0; i < 200; i++) {
                    float[] vec = new float[dim];
                    for (int d = 0; d < dim; d++) {
                        vec[d] = rng.nextFloat();
                    }
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec);
                }
                store.checkpoint();
            }

            int segmentsBefore = store.getSegmentCount();
            assertTrue("need >= maxCount segments to exercise count trigger, got "
                    + segmentsBefore, segmentsBefore >= maxCount);

            long runsBefore = store.getCompactionRunsTotal();
            long successesBefore = store.getCompactionSuccessesTotal();

            store.runCompactionCycle();

            assertEquals("compaction run counter must advance",
                    runsBefore + 1, store.getCompactionRunsTotal());
            assertEquals("count trigger must produce at least one successful compaction",
                    successesBefore + 1, store.getCompactionSuccessesTotal());
            assertTrue("segment count must decrease after count-triggered compaction",
                    store.getSegmentCount() < segmentsBefore);
            assertEquals("no consecutive failures expected",
                    0, store.getCompactionConsecutiveFailures());
        }
    }

    /**
     * Verifies the opposite: when the segment count stays below {@code maxCount}
     * and the byte threshold is also not met, the cycle does NOT fire.
     */
    @Test
    public void neitherTriggerFiresWhenBelowBothThresholds() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // maxCount = 100: we will only create 3 segments.
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx285b", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE / 2,   // unreachably high
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ 100,                  // way above 3 segments
                /*retentionMs*/ 0);

        try (store) {
            store.start();
            Random rng = new Random(286);
            int dim = 8;
            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < 200; i++) {
                    float[] vec = new float[dim];
                    for (int d = 0; d < dim; d++) {
                        vec[d] = rng.nextFloat();
                    }
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec);
                }
                store.checkpoint();
            }

            int segmentsBefore = store.getSegmentCount();
            long successesBefore = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            // The run counter increments unconditionally; what must NOT change
            // is the segment count and the success counter.
            assertEquals("no successful compaction when below both thresholds",
                    successesBefore, store.getCompactionSuccessesTotal());
            assertEquals("segment count must be unchanged when no trigger fires",
                    segmentsBefore, store.getSegmentCount());
        }
    }
}
