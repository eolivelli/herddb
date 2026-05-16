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
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end regression test for issue #570: the compaction scheduler must
 * prefer merging micro-segments first.
 *
 * <p>Memory-pressure checkpoints flush a near-empty live shard as a tiny
 * on-disk segment (a "micro-segment"). Each micro-segment consumes a full
 * slot in the segment count, keeping the Indexing Service in segment-count
 * back-pressure. The fix makes a compaction cycle merge only the
 * micro-segments when at least two are present — a cheap, fast cycle that
 * reclaims slots quickly — while leaving larger segments for the normal
 * tiered policy on subsequent cycles.
 *
 * <p>This test wires a {@link PersistentVectorStore} with an unreachable byte
 * threshold and a {@code maxCount} equal to the segment count, so the issue
 * #285 count trigger fires but the byte trigger does not. Without the
 * micro-segment fast path the count trigger would merge the whole backlog
 * (large segments included); with it, only the micro-segments are merged.
 */
public class Issue570MicroSegmentCompactionTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /** Saved so {@link #restoreDeferral()} restores the exact prior value. */
    private int savedMinLiveVectorsForCheckpoint;

    @Before
    public void disableDeferral() {
        // One checkpoint == one on-disk segment, regardless of how few
        // vectors it carries — this is exactly how memory-pressure
        // checkpoints produce micro-segments in production.
        savedMinLiveVectorsForCheckpoint = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectorsForCheckpoint;
    }

    private static void addVectors(PersistentVectorStore store, Random rng,
                                    int dim, int base, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            float[] vec = new float[dim];
            for (int d = 0; d < dim; d++) {
                vec[d] = rng.nextFloat();
            }
            store.addVector(Bytes.from_int(base + i), vec);
        }
    }

    /**
     * Builds 6 micro-segments (3 vectors each) and 2 larger segments (50
     * vectors each), with the micro-segment threshold set to 10 nodes and
     * {@code maxCount} equal to the 8 on-disk segments so the count trigger
     * fires. A single compaction cycle must merge only the 6 micro-segments
     * into one, dropping the segment count from 8 to 3, and a follow-up
     * cycle (no micro-segments left, no trigger satisfied) must be a no-op.
     */
    @Test
    public void microSegmentsAreMergedFirst() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx570", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE / 2,   // unreachably high
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ 8,                    // == segment count: count trigger fires
                /*retentionMs*/ 0);
        // Micro-segment threshold of 10 live nodes: the 3-vector checkpoints
        // are micro-segments, the 50-vector checkpoints are not.
        store.setCompactionMicroSegmentMaxNodes(10);

        try (store) {
            store.start();
            Random rng = new Random(570);
            int dim = 8;

            // 6 micro-segments of 3 vectors each (distinct PKs per segment).
            for (int c = 0; c < 6; c++) {
                addVectors(store, rng, dim, /*base*/ 1_000 + c * 100, /*count*/ 3);
                store.checkpoint();
            }
            // 2 larger, non-micro segments of 50 vectors each.
            for (int c = 0; c < 2; c++) {
                addVectors(store, rng, dim, /*base*/ 100_000 + c * 1_000, /*count*/ 50);
                store.checkpoint();
            }

            int segmentsBefore = store.getSegmentCount();
            assertEquals("expected 8 on-disk segments before compaction",
                    8, segmentsBefore);
            long totalNodesBefore = totalLiveNodes(store);
            assertEquals("setup must hold 6*3 + 2*50 live vectors",
                    118L, totalNodesBefore);
            long successesBefore = store.getCompactionSuccessesTotal();

            store.runCompactionCycle();

            assertEquals("micro-segment fast path must produce one successful compaction",
                    successesBefore + 1, store.getCompactionSuccessesTotal());
            assertEquals("no consecutive failures expected",
                    0, store.getCompactionConsecutiveFailures());
            // 6 micro-segments collapse into 1; the 2 larger segments are
            // untouched → 1 + 2 = 3 segments remain.
            assertEquals("only the 6 micro-segments must have been merged",
                    3, store.getSegmentCount());

            // No live vector may be lost by the merge.
            assertEquals("merge must preserve every live vector",
                    totalNodesBefore, totalLiveNodes(store));

            // The merged output (highest generation) must hold exactly the
            // 6*3 = 18 micro-segment nodes; the two 50-node segments must
            // remain untouched.
            List<VectorSegment> post = store.getOnDiskSegmentsSnapshotForTest();
            VectorSegment merged = post.stream()
                    .max(java.util.Comparator.comparingLong(s -> s.generation))
                    .orElseThrow(() -> new AssertionError("no merged segment"));
            assertEquals("merged segment must hold all 18 micro-segment nodes",
                    18L, merged.size());
            int untouchedLarge = 0;
            for (VectorSegment s : post) {
                if (s != merged) {
                    assertEquals("large segments must be left untouched",
                            50L, s.size());
                    untouchedLarge++;
                }
            }
            assertEquals("both large segments must survive untouched", 2, untouchedLarge);

            // Second cycle: no micro-segments remain (merged output has 18
            // nodes, the two large segments have 50 each — all above the
            // 10-node threshold) and the byte/count triggers are unreachable,
            // so the cycle must be a no-op.
            long successesAfterFirst = store.getCompactionSuccessesTotal();
            int segmentsAfterFirst = store.getSegmentCount();
            store.runCompactionCycle();
            assertEquals("follow-up cycle must not fire — no micro-segments left",
                    successesAfterFirst, store.getCompactionSuccessesTotal());
            assertEquals("segment count must be stable after the follow-up cycle",
                    segmentsAfterFirst, store.getSegmentCount());
        }
    }

    /** Sums the live-node count across every on-disk segment. */
    private static long totalLiveNodes(PersistentVectorStore store) {
        long total = 0L;
        for (VectorSegment s : store.getOnDiskSegmentsSnapshotForTest()) {
            total += s.size();
        }
        return total;
    }
}
