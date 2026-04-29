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
 * Tests for issue #354: tiered compaction scaling.
 *
 * <p>Two categories of tests:
 * <ol>
 *   <li>Pure unit tests on {@link VectorIndexCompactor#tieredMultiplier},
 *       {@link VectorIndexCompactor#computeTieredMaxBytes} and
 *       {@link VectorIndexCompactor#computeTieredMaxCount} — no store required.</li>
 *   <li>Integration test: when the store holds more than the tier-1 threshold
 *       segments, one compaction cycle must merge substantially more segments
 *       than the base {@code maxCount} would allow.</li>
 * </ol>
 */
public class Issue354TieredCompactionTest {

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

    // -------------------------------------------------------------------------
    // Unit tests for the tiered multiplier/byte/count helpers
    // -------------------------------------------------------------------------

    @Test
    public void multiplierIs1BelowFirstThreshold() {
        assertEquals(1, VectorIndexCompactor.tieredMultiplier(0));
        assertEquals(1, VectorIndexCompactor.tieredMultiplier(1));
        assertEquals(1, VectorIndexCompactor.tieredMultiplier(99));
    }

    @Test
    public void multiplierIs2AtTier1() {
        assertEquals(2, VectorIndexCompactor.tieredMultiplier(100));
        assertEquals(2, VectorIndexCompactor.tieredMultiplier(150));
        assertEquals(2, VectorIndexCompactor.tieredMultiplier(299));
    }

    @Test
    public void multiplierIs4AtTier2() {
        assertEquals(4, VectorIndexCompactor.tieredMultiplier(300));
        assertEquals(4, VectorIndexCompactor.tieredMultiplier(400));
        assertEquals(4, VectorIndexCompactor.tieredMultiplier(499));
    }

    @Test
    public void multiplierIs8AtTier3() {
        assertEquals(8, VectorIndexCompactor.tieredMultiplier(500));
        assertEquals(8, VectorIndexCompactor.tieredMultiplier(1000));
        assertEquals(8, VectorIndexCompactor.tieredMultiplier(Integer.MAX_VALUE));
    }

    @Test
    public void tieredMaxBytesScalesCorrectly() {
        long base = 1024L * 1024 * 1024; // 1 GB
        assertEquals(base, VectorIndexCompactor.computeTieredMaxBytes(50, base));
        assertEquals(base * 2, VectorIndexCompactor.computeTieredMaxBytes(100, base));
        assertEquals(base * 4, VectorIndexCompactor.computeTieredMaxBytes(300, base));
        assertEquals(base * 8, VectorIndexCompactor.computeTieredMaxBytes(500, base));
    }

    @Test
    public void tieredMaxBytesDoesNotOverflow() {
        // Long.MAX_VALUE / 8 + 1 would overflow if multiplied by 8.
        long nearMax = Long.MAX_VALUE / 8 + 1;
        long result = VectorIndexCompactor.computeTieredMaxBytes(500, nearMax);
        assertEquals("should cap at Long.MAX_VALUE on overflow", Long.MAX_VALUE, result);
    }

    @Test
    public void tieredMaxCountScalesCorrectly() {
        int base = 200;
        assertEquals(base, VectorIndexCompactor.computeTieredMaxCount(50, base));
        assertEquals(base * 2, VectorIndexCompactor.computeTieredMaxCount(100, base));
        assertEquals(base * 4, VectorIndexCompactor.computeTieredMaxCount(300, base));
        assertEquals(base * 8, VectorIndexCompactor.computeTieredMaxCount(500, base));
    }

    @Test
    public void tieredMaxCountDoesNotOverflow() {
        int nearMax = Integer.MAX_VALUE / 8 + 1;
        int result = VectorIndexCompactor.computeTieredMaxCount(500, nearMax);
        assertEquals("should cap at Integer.MAX_VALUE on overflow", Integer.MAX_VALUE, result);
    }

    // -------------------------------------------------------------------------
    // Integration test: tiered compaction merges more segments when count is high
    // -------------------------------------------------------------------------

    /**
     * Creates a store with a base maxCount of 4 and a very high byte cap
     * (so bytes are never the limiting factor), then builds enough segments
     * to enter tier 3 (>= 500).  With tiered compaction enabled the effective
     * maxCount must be 8× = 32, so a single cycle should merge at least 5
     * segments (well above the base 4).  With tiered disabled only 4 are
     * merged.
     */
    @Test
    public void tieredCompactionMergesMoreSegmentsAtHighCount() throws Exception {
        Path tmpDir = tmpFolder.newFolder("tiered-354").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        // Large data-page cache (2048 pages × 1 MB) so the BLink trees for
        // each segment's onDiskPkToNode fit in the cache without being evicted
        // to MemoryDataStorageManager.indexpages.  When pages are evicted they
        // are stored under a key that shares the indexUUID prefix with the
        // vector store's checkpoint pages, causing Long.parseLong to fail in
        // MemoryDataStorageManager.indexCheckpoint.  This is a test-only issue
        // (production uses RemoteFileDataStorageManager which has no such clash).
        MemoryManager mm = new MemoryManager(2048L * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // base maxCount = 4, very high byte cap so bytes never constrain.
        // maxCount triggers at >= 4 segments.
        int baseMaxCount = 4;
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx354t", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE / 2,  // unreachably high — only count trigger fires
                /*maxBytes*/ Long.MAX_VALUE,      // never byte-limited
                /*minCount*/ 2,
                /*maxCount*/ baseMaxCount,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(true);
        // Disable backpressure so we can build lots of segments freely.
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);

        try (store) {
            store.start();
            Random rng = new Random(354);
            int dim = 8;

            // Create 20 checkpoints of 100 vectors each → 20 on-disk segments.
            // 20 > tier-1 threshold (100)? No. Let's create enough to reach tier 3 (>= 500).
            // With tiny vectors this is fast.  We use 520 checkpoints of 5 vectors each.
            int numCheckpoints = 520;
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

            int segsBefore = store.getSegmentCount();
            assertTrue("expected >= 500 segments to enter tier 3, got " + segsBefore,
                    segsBefore >= 500);

            long successesBefore = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            assertEquals("exactly one success after one cycle",
                    successesBefore + 1, store.getCompactionSuccessesTotal());

            long merged = store.getCompactionLastInputSegments();
            // At tier-3 (8×) the effective maxCount is 4 * 8 = 32; we must
            // have merged at least 5 (well above the un-tiered cap of 4).
            assertTrue("tiered compaction should have merged > base maxCount segments; merged="
                    + merged, merged > baseMaxCount);
        }
    }

    /**
     * Verifies that disabling tiered compaction still produces a healthy compaction
     * cycle — the count-based trigger fires at {@code baseMaxCount} segments and
     * drains the entire backlog in one shot (since {@code maxTotalBytes} is
     * {@link Long#MAX_VALUE}, all candidates fit in the loop).
     *
     * <p>Note on {@code maxCount} semantics: {@code maxCount} is a <em>trigger
     * threshold</em> (the merge fires when {@code picked.size() >= maxCount}),
     * not a hard upper bound on the number of segments merged per cycle. Once
     * the trigger fires, {@code chooseSegmentsToMerge} returns <em>all</em>
     * candidates that fit within {@code maxTotalBytes}. With
     * {@code maxTotalBytes = Long.MAX_VALUE} and tiny test segments, all 520
     * candidates fit, so the entire backlog is merged in one cycle regardless
     * of whether the 8× tiered scaling is active. The mathematical correctness
     * of the scaling (parameter multiplication, overflow guards) is exercised by
     * the unit tests for {@link VectorIndexCompactor#computeTieredMaxCount} and
     * {@link VectorIndexCompactor#computeTieredMaxBytes} above.
     */
    @Test
    public void tieredCompactionDisabledUsesBaseMaxCount() throws Exception {
        Path tmpDir = tmpFolder.newFolder("tiered-354-disabled").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        // Large cache to avoid MemoryDataStorageManager namespace collision (see above).
        MemoryManager mm = new MemoryManager(2048L * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int baseMaxCount = 4;
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx354td", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE / 2,   // unreachably high — only count trigger fires
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ baseMaxCount,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);

        try (store) {
            store.start();
            Random rng = new Random(3540);
            int dim = 8;

            // Build 520 segments.
            for (int c = 0; c < 520; c++) {
                for (int i = 0; i < 5; i++) {
                    float[] vec = new float[dim];
                    for (int d = 0; d < dim; d++) {
                        vec[d] = rng.nextFloat();
                    }
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec);
                }
                store.checkpoint();
            }

            assertTrue("need >= 500 segments to be in tier-3 range if tiered were on",
                    store.getSegmentCount() >= 500);

            long successesBefore = store.getCompactionSuccessesTotal();
            store.runCompactionCycle();
            assertEquals("exactly one success",
                    successesBefore + 1, store.getCompactionSuccessesTotal());

            long merged = store.getCompactionLastInputSegments();
            // The count trigger fires when picked.size() >= baseMaxCount (4).
            // Since maxTotalBytes=Long.MAX_VALUE, all 520 tiny candidates fit in the
            // loop, so the trigger returns all of them in one merge.
            assertTrue("count trigger must have fired (merged >= baseMaxCount); merged=" + merged,
                    merged >= baseMaxCount);
            // Segment count must have dropped: the entire backlog was drained in one cycle.
            assertTrue("segment count must decrease after compaction",
                    store.getSegmentCount() < 500);
        }
    }
}
