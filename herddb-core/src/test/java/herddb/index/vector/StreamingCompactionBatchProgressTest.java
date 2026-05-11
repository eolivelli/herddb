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
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongBinaryOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link RemoteSegmentGraphMerger} correctly propagates
 * {@link io.github.jbellis.jvector.graph.disk.CompactionProgressListener} callbacks
 * during the streaming merge path (issue #530).
 *
 * <p>Before this fix, {@code GET /status} on the index-optimizer returned
 * {@code batches_written: 0 / pct_complete: 0.00} for the entire duration of a
 * multi-minute streaming compaction because the batch-progress callback was never
 * wired to {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor}.
 *
 * <p>The test builds three real on-disk segments via {@link PersistentVectorStore},
 * runs a streaming merge, and asserts that the {@code batchListener} receives:
 * <ol>
 *   <li>An initial {@code (0, keptCount)} notification immediately when the
 *       "compacting" phase begins — so the HTTP denominator is non-zero from
 *       the first instant.</li>
 *   <li>At least one {@code (completed ≥ 10, total > 0)} notification from
 *       {@code OnDiskGraphIndexCompactor} itself.</li>
 *   <li>A final notification where {@code completed == total} (the 100 %
 *       signal emitted at level completion).</li>
 *   <li>Monotonically non-decreasing {@code completed} values within a level.</li>
 * </ol>
 */
public class StreamingCompactionBatchProgressTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private boolean savedStreamingFlag;
    private int savedMinLiveVectors;

    @Before
    public void setUp() {
        savedMinLiveVectors = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
        savedStreamingFlag = VectorIndexCompactor.streamingCompactionEnabled;
        VectorIndexCompactor.streamingCompactionEnabled = true;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectors;
        VectorIndexCompactor.streamingCompactionEnabled = savedStreamingFlag;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds {@code numSegments} on-disk segments via a real
     * {@link PersistentVectorStore} and returns the merger inputs.
     */
    private List<RemoteSegmentGraphMerger.RemoteSegmentInput> buildInputs(
            Path tmpDir, MemoryDataStorageManager dsm, int dim,
            int numSegments, int perSegment, long seed) throws Exception {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store = new PersistentVectorStore(
                "ts-progress-test", "tbl", "tsuuid-prog", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE)) {
            store.start();
            Random rng = new Random(seed);
            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < perSegment; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertEquals(numSegments, store.getSegmentCount());

            List<VectorSegment> segments = store.getOnDiskSegmentsSnapshotForTest();
            List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs = new ArrayList<>(segments.size());
            String tablespaceUuid = "tsuuid-prog";
            String indexUuid = store.indexUUID();
            for (VectorSegment seg : segments) {
                inputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                        tablespaceUuid, indexUuid,
                        seg.segmentUuid != null ? seg.segmentUuid : indexUuid + "_seg" + seg.segmentId,
                        seg.segmentId,
                        seg.mapFileSize,
                        seg.graphFileSize,
                        seg.generation,
                        new int[0]));
            }
            return inputs;
        }
    }

    @Test
    public void batchProgressIsReportedDuringStreamingCompaction() throws Exception {
        int dim = 16;
        int numSegments = 3;
        int perSegment = 200;
        long expectedKeptCount = (long) numSegments * perSegment; // 600, no tombstones/duplicates

        Path tmpDir = tmpFolder.newFolder("progress-test").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> inputs =
                buildInputs(tmpDir, dsm, dim, numSegments, perSegment, 99L);

        // Verify streaming dispatch can fire (all inputs have real graphFileSize).
        for (RemoteSegmentGraphMerger.RemoteSegmentInput in : inputs) {
            assertTrue("graphFileSize must be > 0 for streaming: " + in.segmentUuid,
                    in.graphFileSize > 0L);
        }

        // Collect all (completed, total) pairs in arrival order.
        // CopyOnWriteArrayList because the jvector compactor fires callbacks from
        // its ForkJoinPool threads while the test thread is blocked in merge().
        List<long[]> progressPairs = new CopyOnWriteArrayList<>();

        RemoteSegmentGraphMerger merger = new RemoteSegmentGraphMerger(
                dsm, tmpDir, 16, 100, 1.2f, 1.4f, VectorSimilarityFunction.COSINE);

        LongBinaryOperator batchListener = (completed, total) -> {
            progressPairs.add(new long[]{completed, total});
            return 0L;
        };
        merger.setBatchListener(batchListener);

        RemoteSegmentGraphMerger.MergeOutput out = merger.merge(
                inputs, "tsuuid-prog", inputs.get(0).indexUuid, 77777L, dim);

        assertTrue("merge must produce output", out != null);
        assertEquals("all 600 vectors kept", expectedKeptCount, out.vectorCount);

        // ---------------------------------------------------------------
        // Assertion 1: the initial (0, keptCount) notification fires so the
        // HTTP /status denominator is non-zero from the first instant of the
        // "compacting" phase.
        // ---------------------------------------------------------------
        assertTrue("at least one progress notification must be received",
                !progressPairs.isEmpty());
        long[] first = progressPairs.get(0);
        assertEquals("first notification: completed must be 0 (initial denominator seed)",
                0L, first[0]);
        assertEquals("first notification: total must equal keptCount",
                expectedKeptCount, first[1]);

        // ---------------------------------------------------------------
        // Assertion 2: at least one notification from the compactor itself
        // with completed >= 10 (first % 10 == 0 fire).
        // ---------------------------------------------------------------
        boolean sawCompactorProgress = false;
        for (long[] pair : progressPairs) {
            if (pair[0] >= 10) {
                sawCompactorProgress = true;
                break;
            }
        }
        assertTrue("compactor must fire at least one progress notification with completed >= 10",
                sawCompactorProgress);

        // ---------------------------------------------------------------
        // Assertion 3: a final (completed == total) notification is present
        // for level-0 completion (the "completed == total" branch added in jvector).
        // ---------------------------------------------------------------
        boolean sawLevelCompletion = false;
        for (long[] pair : progressPairs) {
            if (pair[0] == pair[1] && pair[1] > 0) {
                sawLevelCompletion = true;
                break;
            }
        }
        assertTrue("a completed==total notification must be received at level completion",
                sawLevelCompletion);

        // ---------------------------------------------------------------
        // Assertion 4: within a contiguous run sharing the same total,
        // completed must be monotonically non-decreasing.
        // ---------------------------------------------------------------
        long prevCompleted = 0L;
        long prevTotal = 0L;
        for (long[] pair : progressPairs) {
            long completed = pair[0];
            long total = pair[1];
            assertTrue("completed must always be >= 0", completed >= 0);
            assertTrue("total must always be > 0", total > 0);
            assertTrue("completed must always be <= total", completed <= total);
            if (total == prevTotal) {
                // Same level: completed must not go backwards.
                assertTrue("completed must be non-decreasing within a level",
                        completed >= prevCompleted);
            }
            prevCompleted = completed;
            prevTotal = total;
        }
    }
}
