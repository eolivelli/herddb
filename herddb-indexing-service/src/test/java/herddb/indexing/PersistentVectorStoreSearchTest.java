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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link PersistentVectorStore} search functionality.
 *
 * @author enrico.olivelli
 */
public class PersistentVectorStoreSearchTest {

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

    private PersistentVectorStore createStoreWithMaxLiveSize(Path tmpDir, int maxLiveGraphSize) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                4, 10, 1.2f, 1.4f, false, 2_000_000_000L, maxLiveGraphSize,
                Long.MAX_VALUE);
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

    private float[] normalize(float[] v) {
        float norm = 0;
        for (float f : v) {
            norm += f * f;
        }
        norm = (float) Math.sqrt(norm);
        float[] result = new float[v.length];
        for (int i = 0; i < v.length; i++) {
            result[i] = v[i] / norm;
        }
        return result;
    }

    @Test
    public void testBasicAddAndSearch() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            Random rng = new Random(42);
            int dim = 32;
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            assertEquals(100, store.size());

            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);

            assertEquals(5, results.size());

            // Verify scores are in descending order
            for (int i = 1; i < results.size(); i++) {
                assertTrue("Scores should be descending",
                        results.get(i - 1).getValue() >= results.get(i).getValue());
            }
        }
    }

    @Test
    public void testSearchReturnsCorrectResults() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int dim = 32;
            // Use normalized vectors for cosine similarity
            float[] target = normalize(new float[]{
                    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            });

            Random rng = new Random(123);
            // Add 4 random vectors
            for (int i = 0; i < 4; i++) {
                store.addVector(Bytes.from_int(i), normalize(randomVector(rng, dim)));
            }
            // Add the target vector with pk=99
            store.addVector(Bytes.from_int(99), target);

            List<Map.Entry<Bytes, Float>> results = store.search(target, 5);

            assertFalse("Results should not be empty", results.isEmpty());
            // The identical vector should be first with score close to 1.0
            assertEquals(Bytes.from_int(99), results.get(0).getKey());
            assertTrue("Score of identical vector should be >= 0.99, was " + results.get(0).getValue(),
                    results.get(0).getValue() >= 0.99f);
        }
    }

    @Test
    public void testAddAndRemove() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int dim = 32;
            Random rng = new Random(77);
            for (int i = 0; i < 10; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(10, store.size());

            // Remove 3 vectors
            Set<Bytes> removed = new HashSet<>();
            for (int i = 0; i < 3; i++) {
                Bytes pk = Bytes.from_int(i);
                store.removeVector(pk);
                removed.add(pk);
            }
            assertEquals(7, store.size());

            // Search should not return removed PKs
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            for (Map.Entry<Bytes, Float> entry : results) {
                assertFalse("Removed PK should not appear in results: " + entry.getKey(),
                        removed.contains(entry.getKey()));
            }
        }
    }

    @Test
    public void testSearchEmptyStore() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            float[] query = new float[32];
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertTrue("Search on empty store should return empty list", results.isEmpty());
        }
    }

    @Test
    public void testSearchTopKGreaterThanSize() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int dim = 32;
            Random rng = new Random(55);
            for (int i = 0; i < 3; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            List<Map.Entry<Bytes, Float>> results = store.search(randomVector(rng, dim), 10);
            assertEquals("Should return only 3 results when store has 3 vectors", 3, results.size());
        }
    }

    @Test
    public void testCheckpointCreatesOnDiskSegment() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            int dim = 32;
            Random rng = new Random(100);
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            store.checkpoint();

            assertTrue("Segment count should be > 0 after checkpoint",
                    store.getSegmentCount() > 0);
        }
    }

    @Test
    public void testHybridSearch() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStoreWithDsm(tmpDir, dsm)) {
            store.start();

            int dim = 32;
            Random rng = new Random(200);

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

            // Search should return results from both on-disk and live
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("Hybrid search should return results", results.isEmpty());
            assertTrue("Should return up to 10 results", results.size() <= 10);
        }
    }

    @Test
    public void testDeleteFromOnDiskSegment() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStoreWithDsm(tmpDir, dsm)) {
            store.start();

            int dim = 32;
            Random rng = new Random(300);
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            // Delete 10 vectors
            Set<Bytes> deleted = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                Bytes pk = Bytes.from_int(i);
                store.removeVector(pk);
                deleted.add(pk);
            }

            // Search should exclude deleted vectors
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 100);
            for (Map.Entry<Bytes, Float> entry : results) {
                assertFalse("Deleted PK should not appear: " + entry.getKey(),
                        deleted.contains(entry.getKey()));
            }
        }
    }

    /**
     * Regression test for the shard-rotation off-by-one bug.
     *
     * Before the fix, getAndIncrement() was called before rotateLiveShard(), so the new
     * shard's startNodeId ended up one ahead of the already-allocated nodeId, producing
     * localNodeId = -1 and an ArrayIndexOutOfBoundsException inside jvector's DenseIntMap.
     *
     * This test uses maxLiveGraphSize=5 so rotation triggers on the 6th insert, and
     * verifies that all inserted vectors remain searchable afterwards.
     */
    @Test
    public void testInsertAcrossShardRotation() throws Exception {
        int shardSize = 5;
        int dim = 4;
        Path tmpDir = tmpFolder.newFolder().toPath();
        Random rng = new Random(1234);
        try (PersistentVectorStore store = createStoreWithMaxLiveSize(tmpDir, shardSize)) {
            store.start();

            // Insert shardSize+1 vectors: the last one triggers the first shard rotation.
            int totalVectors = shardSize + 1;
            Set<Bytes> inserted = new HashSet<>();
            for (int i = 0; i < totalVectors; i++) {
                Bytes pk = Bytes.from_int(i);
                store.addVector(pk, randomVector(rng, dim));
                inserted.add(pk);
            }

            assertEquals(totalVectors, store.size());
            assertEquals("Should have 2 live shards after one rotation", 2, store.getLiveShardCount());

            // Every inserted PK should be reachable via search.
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, totalVectors);
            Set<Bytes> found = new HashSet<>();
            for (Map.Entry<Bytes, Float> e : results) {
                found.add(e.getKey());
            }
            for (Bytes pk : inserted) {
                assertTrue("PK " + pk + " should be findable after shard rotation", found.contains(pk));
            }
        }
    }

    /**
     * Verifies that multiple shard rotations (shardSize*3+1 inserts) all produce valid
     * local node IDs — i.e., no ArrayIndexOutOfBoundsException across any rotation boundary.
     */
    @Test
    public void testInsertWithMultipleRotations() throws Exception {
        int shardSize = 5;
        int dim = 4;
        Path tmpDir = tmpFolder.newFolder().toPath();
        Random rng = new Random(5678);
        try (PersistentVectorStore store = createStoreWithMaxLiveSize(tmpDir, shardSize)) {
            store.start();

            int totalVectors = shardSize * 3 + 1; // triggers 3 rotations
            for (int i = 0; i < totalVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            assertEquals(totalVectors, store.size());
            assertTrue("Should have at least 4 live shards after 3 rotations",
                    store.getLiveShardCount() >= 4);

            List<Map.Entry<Bytes, Float>> results = store.search(randomVector(rng, dim), totalVectors);
            assertFalse("Search across multiple rotated shards should return results", results.isEmpty());
        }
    }

    /**
     * Concurrent inserts racing across the shard-rotation boundary must not produce
     * negative local node IDs (which would cause an ArrayIndexOutOfBoundsException).
     */
    @Test
    public void testConcurrentInsertAcrossShardRotation() throws Exception {
        int shardSize = 5;
        int dim = 4;
        int threads = 8;
        int insertsPerThread = 10; // each thread crosses rotation multiple times
        Path tmpDir = tmpFolder.newFolder().toPath();

        try (PersistentVectorStore store = createStoreWithMaxLiveSize(tmpDir, shardSize)) {
            store.start();

            CyclicBarrier barrier = new CyclicBarrier(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(pool.submit(() -> {
                    try {
                        Random rng = new Random(threadId);
                        barrier.await(); // start all threads at the same time
                        for (int i = 0; i < insertsPerThread; i++) {
                            Bytes pk = Bytes.from_string("t" + threadId + "_" + i);
                            store.addVector(pk, randomVector(rng, dim));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }));
            }
            pool.shutdown();
            // Propagate any exceptions (including the ArrayIndexOutOfBoundsException before the fix).
            for (Future<?> f : futures) {
                f.get();
            }

            int expected = threads * insertsPerThread;
            assertEquals(expected, store.size());
        }
    }

    @Test
    public void testMultipleCheckpoints() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStoreWithDsm(tmpDir, dsm)) {
            store.start();

            int dim = 32;
            Random rng = new Random(400);

            // First batch + checkpoint
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            // Second batch + checkpoint
            for (int i = 100; i < 200; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            // Verify search covers all data
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("Search after multiple checkpoints should return results",
                    results.isEmpty());
            assertEquals("Total size should be 200", 200, store.size());
        }
    }
}
