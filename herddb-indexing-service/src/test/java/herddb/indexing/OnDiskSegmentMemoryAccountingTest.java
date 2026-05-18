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

import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link PersistentVectorStore#getOnDiskSegmentsEstimatedMemoryBytes()}
 * (and by extension {@link PersistentVectorStore#estimatedMemoryUsageBytes()}) includes
 * the in-memory footprint of on-disk {@link herddb.indexing.vector.VectorSegment} objects
 * after a checkpoint (issue #360).
 *
 * <p>Before the fix, {@code estimatedMemoryUsageBytes()} only summed the live /
 * frozen / deferred in-memory shards.  Once those shards were snapshotted and
 * written to disk during a checkpoint, their vectors moved into
 * {@code VectorSegment} objects that retained significant heap:
 * the {@code pkData/pkOffsets/pkLengths} arrays and the
 * {@code BLink<Bytes,Long> onDiskPkToNode} tree (whose internal BLink Node
 * structures were the single largest contributor to the 6-8 GiB accounting gap
 * observed in the GKE BIGANN benchmark).  After the checkpoint the live shards
 * are reset to empty, so the old estimate fell to near-zero even though on-disk
 * segments were holding several GiB — making the back-pressure gate blind to
 * this heap pressure.
 *
 * <p>Tests assert against {@link PersistentVectorStore#getOnDiskSegmentsEstimatedMemoryBytes()}
 * rather than {@code estimatedMemoryUsageBytes()} so that the empty post-checkpoint
 * live shards (which retain a small but non-zero builder overhead) cannot mask a
 * regression where the on-disk contribution is actually zero.
 */
public class OnDiskSegmentMemoryAccountingTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, boolean fusedPQ) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // compactionIntervalMs = Long.MAX_VALUE: disable background compaction so
        // the test controls checkpoints explicitly.
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, fusedPQ,
                2_000_000_000L /* maxSegmentSize */, 0 /* maxLiveGraphSize (auto) */,
                Long.MAX_VALUE /* compactionIntervalMs — no auto-compaction */,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * After a checkpoint, all live-shard vectors move to on-disk segments.
     * The live shards are reset to empty, so without the fix the on-disk
     * segment estimate would be zero.  With the fix, the on-disk segment
     * contributions (pkData / pkOffsets / pkLengths / BLink tree) are
     * included and exceed a meaningful lower bound.
     *
     * <p>We assert against {@code getOnDiskSegmentsEstimatedMemoryBytes()}
     * directly to avoid the false positive that arises when an empty live
     * shard's {@code GraphIndexBuilder} contributes a small but non-zero
     * overhead — which would make a naive {@code estimatedMemoryUsageBytes()
     * > 0} assertion pass even without the fix.
     *
     * <p>Lower bound: pkData (4 bytes × N) + pkOffsets (4 bytes × N) + pkLengths
     * (4 bytes × N) = 12 bytes per vector minimum, plus the BLink's
     * per-entry overhead.
     */
    @Test
    public void testOnDiskSegmentMemoryIsIncludedAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem").toPath();

        int numVectors = 500;
        int dim = 16;
        // Lower bound: pkData + pkOffsets + pkLengths each contribute
        // at least numVectors * Integer.BYTES bytes (4 bytes per entry).
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(42);

        try (PersistentVectorStore store = createStore(tmpDir, false /* fusedPQ */)) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Before checkpoint: live shards hold the vectors; on-disk estimate is 0.
            long onDiskBefore = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("expected zero on-disk estimate before first checkpoint, got "
                    + onDiskBefore, onDiskBefore == 0);

            store.checkpoint();

            // After checkpoint: live shards are reset to empty; all vectors now
            // reside in on-disk VectorSegments.
            int onDiskCount = store.getOnDiskNodeCount();
            assertTrue("checkpoint must have produced at least one on-disk node, got "
                    + onDiskCount, onDiskCount > 0);

            // Without the fix, getOnDiskSegmentsEstimatedMemoryBytes() returns 0
            // because VectorSegment.estimatedInMemoryBytes() is absent.  With the
            // fix, each segment contributes pkData + pkOffsets*4 + pkLengths*4 +
            // BLink.getUsedMemory(), well above the pkArrayLowerBound.
            long onDiskEstimate = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("getOnDiskSegmentsEstimatedMemoryBytes() must be at least "
                    + pkArrayLowerBound + " bytes (pkData+pkOffsets+pkLengths for "
                    + numVectors + " vectors), got " + onDiskEstimate
                    + ". Without the fix this returns 0 because VectorSegment"
                    + ".estimatedInMemoryBytes() does not sum the pk arrays or BLink tree.",
                    onDiskEstimate >= pkArrayLowerBound);

            // Total estimate must include the on-disk contribution.
            long totalEstimate = store.estimatedMemoryUsageBytes();
            assertTrue("estimatedMemoryUsageBytes() must be >= on-disk segment estimate ("
                    + onDiskEstimate + "), got " + totalEstimate,
                    totalEstimate >= onDiskEstimate);
        }
    }

    /**
     * Same assertion as {@link #testOnDiskSegmentMemoryIsIncludedAfterCheckpoint}
     * but with {@code fusedPQ=true}.  When the shard has fewer than
     * {@code MIN_VECTORS_FOR_FUSED_PQ=256} vectors the graph is written with
     * InlineVectors only (no FusedPQ); once we supply ≥ 512 vectors the PQ
     * codebook is built and the segment is sealed with FusedPQ.  Either way
     * the pkData/pkOffsets/pkLengths and BLink contributions are present and
     * must appear in the on-disk estimate.
     */
    @Test
    public void testOnDiskSegmentMemoryFusedPQ() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem-fpq").toPath();

        int numVectors = 512;
        int dim = 32;
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(7);

        try (PersistentVectorStore store = createStore(tmpDir, true /* fusedPQ */)) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            store.checkpoint();

            int onDiskCount = store.getOnDiskNodeCount();
            assertTrue("checkpoint must have produced at least one on-disk node (FusedPQ path), got "
                    + onDiskCount, onDiskCount > 0);

            long onDiskEstimate = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("getOnDiskSegmentsEstimatedMemoryBytes() must be >= "
                    + pkArrayLowerBound + " bytes (FusedPQ=true, "
                    + numVectors + " vectors), got " + onDiskEstimate,
                    onDiskEstimate >= pkArrayLowerBound);

            long totalEstimate = store.estimatedMemoryUsageBytes();
            assertTrue("estimatedMemoryUsageBytes() must be >= on-disk segment estimate (FusedPQ), "
                    + "onDisk=" + onDiskEstimate + " total=" + totalEstimate,
                    totalEstimate >= onDiskEstimate);
        }
    }

    /**
     * Confirms the monotonic relationship: after each of two successive checkpoints
     * the on-disk segment estimate stays positive, because each cycle moves vectors
     * from live shards to on-disk segments and the on-disk segment memory is counted.
     */
    @Test
    public void testEstimateRemainsPositiveAcrossMultipleCheckpoints() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem-multi").toPath();

        int batchSize = 300;
        int dim = 8;
        long pkArrayLowerBound = (long) batchSize * Integer.BYTES * 3;
        Random rng = new Random(99);

        try (PersistentVectorStore store = createStore(tmpDir, false /* fusedPQ */)) {
            store.start();

            // First batch and checkpoint.
            for (int i = 0; i < batchSize; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            long onDiskAfterFirst = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("on-disk estimate must be >= " + pkArrayLowerBound
                    + " bytes after first checkpoint, got " + onDiskAfterFirst,
                    onDiskAfterFirst >= pkArrayLowerBound);

            // Second batch and checkpoint.
            for (int i = batchSize; i < batchSize * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            long onDiskAfterSecond = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("on-disk estimate must be >= " + pkArrayLowerBound
                    + " bytes after second checkpoint, got " + onDiskAfterSecond,
                    onDiskAfterSecond >= pkArrayLowerBound);
        }
    }

    /**
     * Verifies that the on-disk segment memory estimate is correctly reported
     * after the store is closed and reopened from persisted state.
     *
     * <p>This exercises the recovery path: when a {@code PersistentVectorStore}
     * is restarted it calls {@code loadFromStatus()} which reconstructs the
     * {@code VectorSegment} list from the stored {@code IndexStatus}.  The
     * segment's pkData/pkOffsets/pkLengths and BLink must be populated during
     * loading, and {@code getOnDiskSegmentsEstimatedMemoryBytes()} must return
     * a positive value immediately after {@code start()}.
     */
    @Test
    public void testOnDiskEstimateAfterRestart() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem-restart").toPath();

        int numVectors = 400;
        int dim = 16;
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(123);

        // Use a stable indexUUID and the same MemoryDataStorageManager instance
        // so that the second store opening can read back the persisted state.
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUUID = "restart-test-uuid";

        // Phase 1: insert + checkpoint + close.
        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                indexUUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, false /* fusedPQ */,
                2_000_000_000L, 0, Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            int onDiskCount = store.getOnDiskNodeCount();
            assertTrue("Phase 1: checkpoint must produce at least one on-disk node, got "
                    + onDiskCount, onDiskCount > 0);
        }

        // Phase 2: reopen with the same DSM and UUID; verify on-disk estimate
        // is non-zero immediately after start().
        MemoryManager mm2 = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store2 = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                indexUUID, tmpDir, dsm, mm2,
                16, 100, 1.2f, 1.4f, false /* fusedPQ */,
                2_000_000_000L, 0, Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            store2.start();

            long onDiskEstimate = store2.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("After restart, getOnDiskSegmentsEstimatedMemoryBytes() must be >= "
                    + pkArrayLowerBound + " bytes (pkData+pkOffsets+pkLengths for "
                    + numVectors + " vectors loaded from checkpoint), got " + onDiskEstimate
                    + ". Without the fix, the on-disk segment in-memory footprint is"
                    + " not counted and this returns 0.",
                    onDiskEstimate >= pkArrayLowerBound);

            long totalEstimate = store2.estimatedMemoryUsageBytes();
            assertTrue("After restart, estimatedMemoryUsageBytes() must be >= on-disk estimate ("
                    + onDiskEstimate + "), got " + totalEstimate,
                    totalEstimate >= onDiskEstimate);
        }
    }
}
