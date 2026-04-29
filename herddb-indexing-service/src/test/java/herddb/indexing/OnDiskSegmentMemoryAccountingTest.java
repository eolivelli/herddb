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
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that {@link PersistentVectorStore#estimatedMemoryUsageBytes()} includes
 * the in-memory footprint of on-disk {@link herddb.index.vector.VectorSegment} objects
 * after a checkpoint (issue #360).
 *
 * <p>Before the fix, {@code estimatedMemoryUsageBytes()} only summed the live /
 * frozen / deferred in-memory shards.  Once those shards were snapshotted and
 * written to disk during a checkpoint, their vectors moved into
 * {@code VectorSegment} objects that retained significant heap:
 * the {@code pkData/pkOffsets/pkLengths} arrays and the
 * {@code BLink<Bytes,Long> onDiskPkToNode} tree (whose internal {@code TreeMap}
 * nodes were the single largest contributor to the 6-8 GiB accounting gap
 * observed in the GKE BIGANN benchmark).  After the checkpoint the live shards
 * are reset to empty, so the old estimate fell to zero even though on-disk
 * segments were holding several GiB — making the back-pressure gate blind to
 * this heap pressure.
 */
public class OnDiskSegmentMemoryAccountingTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // compactionIntervalMs = Long.MAX_VALUE: disable background compaction so
        // the test controls checkpoints explicitly.
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, false /* fusedPQ */,
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
     * The live shards are reset to empty, so without the fix the estimated
     * memory drops to zero.  With the fix, the on-disk segment contributions
     * (pkData / pkOffsets / pkLengths / BLink tree / OnDiskGraphIndex upper
     * layers) keep the estimate positive.
     */
    @Test
    public void testOnDiskSegmentMemoryIsIncludedAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem").toPath();

        int numVectors = 500;
        int dim = 16;
        Random rng = new Random(42);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Before checkpoint: live shards hold the vectors; estimate > 0.
            long memBefore = store.estimatedMemoryUsageBytes();
            assertTrue("expected positive estimate from live shards before checkpoint, got "
                    + memBefore, memBefore > 0);

            store.checkpoint();

            // After checkpoint: live shards are reset to empty; all vectors now
            // reside in on-disk VectorSegments.
            int onDiskCount = store.getOnDiskNodeCount();
            assertTrue("checkpoint must have produced at least one on-disk node, got "
                    + onDiskCount, onDiskCount > 0);

            // The estimate must remain positive because on-disk segments carry
            // pkData/pkOffsets/pkLengths arrays and a BLink pk-to-ordinal tree.
            // Without the fix this would return 0 (empty live shards = zero
            // contribution from the old shardMemoryBytes-only logic).
            long memAfter = store.estimatedMemoryUsageBytes();
            assertTrue("estimatedMemoryUsageBytes() must account for on-disk segment "
                    + "in-memory footprint after checkpoint (got " + memAfter + " bytes, "
                    + "expected > 0). Without the fix this returns 0 because the "
                    + "pkData/pkOffsets/pkLengths arrays and BLink tree in each "
                    + "VectorSegment are not counted.",
                    memAfter > 0);
        }
    }

    /**
     * Confirms the monotonic relationship: after each of two successive checkpoints
     * the estimate stays positive, because each cycle moves vectors from live shards
     * to on-disk segments and the on-disk segment memory is now included.
     */
    @Test
    public void testEstimateRemainsPositiveAcrossMultipleCheckpoints() throws Exception {
        Path tmpDir = tmpFolder.newFolder("seg-mem-multi").toPath();

        int batchSize = 300;
        int dim = 8;
        Random rng = new Random(99);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // First batch and checkpoint.
            for (int i = 0; i < batchSize; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            long memAfterFirst = store.estimatedMemoryUsageBytes();
            assertTrue("estimate must be positive after first checkpoint, got "
                    + memAfterFirst, memAfterFirst > 0);

            // Second batch and checkpoint.
            for (int i = batchSize; i < batchSize * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            long memAfterSecond = store.estimatedMemoryUsageBytes();
            assertTrue("estimate must be positive after second checkpoint, got "
                    + memAfterSecond, memAfterSecond > 0);
        }
    }
}
