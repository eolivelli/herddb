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
import static org.junit.Assert.assertFalse;

import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #605:
 * {@code IndexOutOfBoundsException: Ordinal N out of bounds for vector count N}
 * in {@code PQVectors.get()} during FusedPQ segment write (checkpoint Phase B and
 * compaction) when a live shard contains at least one deleted vector.
 *
 * <p>Root cause: {@code writeShardAsFusedPQSegment} built the
 * {@code VectorStorageShardView} with {@code size = nodeToPk.size()} (the
 * <em>live</em> count). After a deletion, the jvector graph's
 * {@code idUpperBound} remains {@code liveCount + 1} because {@code markNodeDeleted}
 * does not lower the {@code maxNodeId} high-water mark. The writer then calls
 * {@code pqv.get(idUpperBound - 1)} using the original graph ordinal, which equals
 * {@code liveCount} and is out of bounds for the {@code PQVectors} that was built
 * with {@code count = liveCount}.
 *
 * <p>Fix: use {@code shardGraph.getIdUpperBound()} (not {@code nodeToPk.size()}) as
 * the {@code VectorStorageShardView} size so that {@code pqv.count == idUpperBound}.
 */
public class Issue605PQVectorsOobTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Creates a store with a large enough {@code maxLiveGraphSize} (2000) so that all
     * test vectors land in a single live shard, and with FusedPQ enabled (the default).
     * Dimension is 16 (≥ {@link PersistentVectorStore#MIN_DIM_FOR_FUSED_PQ}).
     */
    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(256 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                /* m= */ 16, /* beamWidth= */ 100, /* neighborOverflow= */ 1.2f, /* alpha= */ 1.4f,
                /* fusedPQ= */ true,
                /* maxSegmentSize= */ 2_000_000_000L,
                /* maxLiveGraphSize= */ 2000,
                /* compactionIntervalMs= */ Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Regression: insert exactly {@code MIN_VECTORS_FOR_FUSED_PQ} (256) live vectors
     * with one additional vector deleted, leaving:
     * <ul>
     *   <li>{@code nodeToPk.size() = 256} (live count, triggers FusedPQ)</li>
     *   <li>{@code shardGraph.getIdUpperBound() = 257} (high-water mark)</li>
     * </ul>
     * The jvector writer calls {@code pqv.get(256)} (the last original ordinal).
     * Without the fix this throws {@code IndexOutOfBoundsException: Ordinal 256
     * out of bounds for vector count 256}.
     */
    @Test
    public void checkpointSucceedsAfterDeletionFromFusedPQShard() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 16; // >= MIN_DIM_FOR_FUSED_PQ (8)
        // Need at least MIN_VECTORS_FOR_FUSED_PQ (256) live vectors for FusedPQ.
        // Insert 257 vectors so that after deleting one low-ordinal vector,
        // the live count is exactly 256 (= MIN_VECTORS_FOR_FUSED_PQ) while
        // idUpperBound remains 257.
        int totalInserted = 257;
        Random rng = new Random(605);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < totalInserted; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(totalInserted, store.size());

            // Delete a LOW-ordinal vector (pk=0, local ordinal 0) so that:
            //   nodeToPk.size() = 256  (still >= MIN_VECTORS_FOR_FUSED_PQ → FusedPQ fires)
            //   idUpperBound     = 257  (maxNodeId unchanged; highest live ordinal = 256)
            // Before the fix, pqv.get(256) throws IOOBE because pqv.count == 256.
            store.removeVector(Bytes.from_int(0));
            assertEquals(totalInserted - 1, store.size());

            // Must not throw IndexOutOfBoundsException.
            store.checkpoint();

            // Sanity: deleted pk is gone from on-disk segments.
            List<Map.Entry<Bytes, Float>> results =
                    store.search(randomVector(rng, dim), totalInserted);
            Set<Bytes> returnedPks = new HashSet<>();
            for (Map.Entry<Bytes, Float> e : results) {
                returnedPks.add(e.getKey());
            }
            assertFalse("deleted pk=0 must not appear in search results",
                    returnedPks.contains(Bytes.from_int(0)));
        }
    }

    /**
     * Variant: delete the LAST inserted vector (highest ordinal) instead.
     * This exercises the no-deletion-before-last-ordinal path — the highest
     * original ordinal is now below idUpperBound and the issue does not fire,
     * but the checkpoint must still succeed cleanly (guard against regressions
     * in the trivial direction).
     */
    @Test
    public void checkpointSucceedsAfterDeletionOfLastInsertedVector() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 16;
        int totalInserted = 257;
        Random rng = new Random(606);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < totalInserted; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Delete the last vector: highest ordinal removed; idUpperBound is reduced
            // by cleanup() since maxNodeId walks down.  Live count = 256, idUpperBound
            // now has highest live ordinal = 255 → no IOOBE even without the fix.
            // Test ensures the fix does not regress this direction.
            store.removeVector(Bytes.from_int(totalInserted - 1));
            assertEquals(totalInserted - 1, store.size());

            // Must succeed.
            store.checkpoint();
        }
    }

    /**
     * Stress variant: delete multiple low-ordinal vectors so that the gap between
     * live count and idUpperBound is larger than 1.  With 5 deletions:
     * live count = 252 (< MIN_VECTORS_FOR_FUSED_PQ=256) → FusedPQ is NOT triggered
     * for this shard so the PQ path is skipped.  This test guards that InlineVectors-
     * only mode still works correctly after deletions.
     */
    @Test
    public void checkpointSucceedsWithMultipleDeletionsBelow256() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int dim = 16;
        int totalInserted = 257;
        Random rng = new Random(607);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int i = 0; i < totalInserted; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Delete 5 low-ordinal vectors → live count = 252 < 256 → InlineVectors path.
            for (int i = 0; i < 5; i++) {
                store.removeVector(Bytes.from_int(i));
            }
            assertEquals(totalInserted - 5, store.size());

            // Must succeed without any exception.
            store.checkpoint();
        }
    }
}
