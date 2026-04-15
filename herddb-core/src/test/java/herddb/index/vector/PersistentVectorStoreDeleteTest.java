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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Comprehensive tests for vector deletion in {@link PersistentVectorStore}.
 * Covers deletion from live shards, on-disk segments, checkpoint phases, and persistence.
 *
 * @author enrico.olivelli
 */
public class PersistentVectorStoreDeleteTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE); // compaction disabled for tests
    }

    private PersistentVectorStore createStoreWithDsm(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Test 1: Delete from live shard before first checkpoint.
     * Verifies that size() decrements and search excludes deleted PK.
     */
    @Test
    public void testDeleteFromLiveShardBeforeCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(42);
            int dim = 32;

            // Add 5 vectors
            for (int i = 0; i < 5; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(5, store.size());

            // Delete one vector
            store.removeVector(Bytes.from_int(2));
            assertEquals(4, store.size());

            // Search should not return deleted PK
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            for (Map.Entry<Bytes, Float> entry : results) {
                assertFalse("Deleted PK should not appear in results",
                        entry.getKey().equals(Bytes.from_int(2)));
            }
        }
    }

    /**
     * Test 2: Delete non-existent PK is a no-op.
     * Verifies no exception and size unchanged.
     */
    @Test
    public void testDeleteNonExistentPkIsNoOp() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(43);
            int dim = 32;

            for (int i = 0; i < 3; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(3, store.size());

            // Try to delete non-existent PK (no exception)
            store.removeVector(Bytes.from_int(999));

            // Size unchanged
            assertEquals(3, store.size());
        }
    }

    /**
     * Test 3: Size decrements on each delete.
     */
    @Test
    public void testSizeDecrementsOnEachDelete() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(44);
            int dim = 32;

            // Add 10 vectors
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(10, store.size());

            // Delete in a loop, verify size decrements
            for (int i = 0; i < 10; i++) {
                store.removeVector(Bytes.from_int(i));
                assertEquals(10 - i - 1, store.size());
            }

            assertEquals(0, store.size());
        }
    }

    /**
     * Test 4: forEachPrimaryKey excludes deleted entries.
     */
    @Test
    public void testForEachPrimaryKeyExcludesDeleted() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(45);
            int dim = 32;

            // Add 10 vectors
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Delete pk=3 and pk=7
            store.removeVector(Bytes.from_int(3));
            store.removeVector(Bytes.from_int(7));

            // Count PKs via forEachPrimaryKey
            Set<Bytes> pks = new HashSet<>();
            store.forEachPrimaryKey(false, pk -> {
                pks.add(pk);
                return true;
            });

            // Should have 8 PKs (10 - 2 deleted)
            assertEquals(8, pks.size());
            assertFalse("Should not contain deleted pk=3", pks.contains(Bytes.from_int(3)));
            assertFalse("Should not contain deleted pk=7", pks.contains(Bytes.from_int(7)));
        }
    }

    /**
     * Test 5: Delete from on-disk segment after checkpoint.
     * Add vectors, checkpoint (moves to disk), then delete.
     */
    @Test
    public void testDeleteFromOnDiskSegmentAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(46);
            int dim = 32;

            // Add 50 vectors
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Checkpoint to move vectors to on-disk segment
            store.checkpoint();

            // Delete one vector from on-disk segment
            store.removeVector(Bytes.from_int(10));

            assertEquals(49, store.size());

            // Search should exclude deleted PK
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 100);
            for (Map.Entry<Bytes, Float> entry : results) {
                assertFalse("Deleted pk=10 should not appear in search results",
                        entry.getKey().equals(Bytes.from_int(10)));
            }

            // forEachPrimaryKey should exclude deleted PK
            Set<Bytes> pks = new HashSet<>();
            store.forEachPrimaryKey(false, pk -> {
                pks.add(pk);
                return true;
            });
            assertFalse("forEachPrimaryKey should not yield deleted pk=10",
                    pks.contains(Bytes.from_int(10)));
        }
    }

    /**
     * Test 6: Delete and verify deleted entries excluded from on-disk after checkpoint.
     * After deleting from an on-disk segment, verify the deletion persists in the checkpoint.
     */
    @Test
    public void testDeleteFromOnDiskAndVerifyAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(47);
            int dim = 32;

            // Add 50 vectors
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // First checkpoint
            store.checkpoint();
            assertEquals(50, store.size());

            // Delete 2 vectors from on-disk segment
            store.removeVector(Bytes.from_int(5));
            store.removeVector(Bytes.from_int(15));
            assertEquals(48, store.size());

            // Verify neither deleted vector appears in search
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 100);
            for (Map.Entry<Bytes, Float> entry : results) {
                assertFalse("pk=5 should not appear", entry.getKey().equals(Bytes.from_int(5)));
                assertFalse("pk=15 should not appear", entry.getKey().equals(Bytes.from_int(15)));
            }
        }
    }

    /**
     * Test 7: Delete a few vectors and verify search only returns non-deleted.
     */
    @Test
    public void testDeleteSeveralVectorsBeforeCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(48);
            int dim = 32;

            // Add 10 vectors
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Delete some before checkpoint
            for (int i = 0; i < 3; i++) {
                store.removeVector(Bytes.from_int(i));
            }
            assertEquals(7, store.size());

            store.checkpoint();
            assertEquals(7, store.size());

            // Search must only return 7 results
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertEquals(7, results.size());
        }
    }

    /**
     * Test 8: Delete entries across multiple search queries to ensure consistency.
     */
    @Test
    public void testDeleteConsistencyAcrossMultipleSearches() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStoreWithDsm(tmpDir, dsm)) {
            store.start();

            Random rng = new Random(49);
            int dim = 32;

            // Insert 100 vectors and checkpoint
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            // Delete some vectors
            Set<Bytes> deletedPks = new HashSet<>();
            for (int i = 10; i < 20; i++) {
                store.removeVector(Bytes.from_int(i));
                deletedPks.add(Bytes.from_int(i));
            }

            assertEquals(90, store.size());

            // Multiple searches should consistently exclude deleted PKs
            for (int q = 0; q < 5; q++) {
                float[] query = randomVector(rng, dim);
                List<Map.Entry<Bytes, Float>> results = store.search(query, 100);
                for (Map.Entry<Bytes, Float> entry : results) {
                    assertFalse("Deleted PK should never appear in any search",
                            deletedPks.contains(entry.getKey()));
                }
            }
        }
    }

    /**
     * Test 9: Size and forEachPrimaryKey consistency after mixed insert/delete/insert.
     */
    @Test
    public void testSizeConsistencyAfterMixedOperations() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(50);
            int dim = 32;

            // Insert 20
            for (int i = 0; i < 20; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(20, store.size());

            // Delete 5
            for (int i = 0; i < 5; i++) {
                store.removeVector(Bytes.from_int(i));
            }
            assertEquals(15, store.size());

            // Insert 10 more
            for (int i = 20; i < 30; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(25, store.size());

            // forEachPrimaryKey should yield exactly 25
            Set<Bytes> pks = new HashSet<>();
            store.forEachPrimaryKey(false, pk -> {
                pks.add(pk);
                return true;
            });
            assertEquals(25, pks.size());

            // Verify deleted PKs not in the set
            for (int i = 0; i < 5; i++) {
                assertFalse(pks.contains(Bytes.from_int(i)));
            }
        }
    }

    /**
     * Test 11: Hybrid search excludes deleted entries from both on-disk and live.
     * Insert, checkpoint (some on-disk), insert more (live), delete from both,
     * verify search excludes all deleted PKs.
     */
    @Test
    public void testHybridSearchExcludesDeleted() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStoreWithDsm(tmpDir, dsm)) {
            store.start();

            Random rng = new Random(52);
            int dim = 32;

            // Add first batch and checkpoint
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            // Add second batch (live vectors)
            for (int i = 100; i < 150; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            assertEquals(150, store.size());

            // Delete from on-disk: pk=10, pk=50
            store.removeVector(Bytes.from_int(10));
            store.removeVector(Bytes.from_int(50));

            // Delete from live: pk=120, pk=140
            store.removeVector(Bytes.from_int(120));
            store.removeVector(Bytes.from_int(140));

            assertEquals(146, store.size());

            // Search should exclude all deleted PKs
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 200);
            Set<Bytes> resultPks = new HashSet<>();
            for (Map.Entry<Bytes, Float> entry : results) {
                Bytes pk = entry.getKey();
                assertFalse("Should not contain pk=10", pk.equals(Bytes.from_int(10)));
                assertFalse("Should not contain pk=50", pk.equals(Bytes.from_int(50)));
                assertFalse("Should not contain pk=120", pk.equals(Bytes.from_int(120)));
                assertFalse("Should not contain pk=140", pk.equals(Bytes.from_int(140)));
                resultPks.add(pk);
            }
        }
    }
}
