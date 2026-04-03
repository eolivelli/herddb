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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link PersistentVectorStore} lifecycle: persistence, recovery, concurrency.
 *
 * @author enrico.olivelli
 */
public class PersistentVectorStoreLifecycleTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final String FIXED_UUID = "testidx_testtable_fixed";

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
    }

    private PersistentVectorStore createStoreWithFixedUUID(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", FIXED_UUID, tmpDir, dsm, mm,
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

    @Test
    public void testRestartRecovery() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int dim = 32;
        Random rng = new Random(42);

        float[] queryVector = randomVector(rng, dim);

        // First instance: add vectors, checkpoint, close
        List<Map.Entry<Bytes, Float>> resultsBefore;
        try (PersistentVectorStore store = createStoreWithFixedUUID(tmpDir, dsm)) {
            store.start();
            // Need >= 256 vectors for FusedPQ, but also test with fewer
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(new Random(i), dim));
            }
            store.checkpoint();
            resultsBefore = store.search(queryVector, 10);
        }

        // Second instance: same DSM, should recover
        try (PersistentVectorStore store = createStoreWithFixedUUID(tmpDir, dsm)) {
            store.start();

            assertEquals("Size should be restored after recovery", 300, store.size());

            List<Map.Entry<Bytes, Float>> resultsAfter = store.search(queryVector, 10);
            assertFalse("Search after recovery should return results", resultsAfter.isEmpty());
            assertEquals("Same number of results before and after recovery",
                    resultsBefore.size(), resultsAfter.size());

            // The same top-1 PK should be returned
            assertEquals("Top result should be the same after recovery",
                    resultsBefore.get(0).getKey(), resultsAfter.get(0).getKey());
        }
    }

    @Test
    public void testSimpleCheckpointSmallDataset() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            int dim = 32;
            Random rng = new Random(99);
            // Add fewer than 256 vectors (below FusedPQ threshold)
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Checkpoint with small dataset should work (simple format, no FusedPQ)
            store.checkpoint();

            assertEquals(100, store.size());

            // Verify search still works after checkpoint
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("Search should work after simple checkpoint", results.isEmpty());
        }
    }

    @Test
    public void testConcurrentDMLDuringCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            int dim = 32;
            Random rng = new Random(500);

            // Add initial batch (>= 256 for FusedPQ)
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Set hook to add vectors during Phase B
            AtomicBoolean hookExecuted = new AtomicBoolean(false);
            store.setCheckpointPhaseBHook(() -> {
                Random hookRng = new Random(999);
                for (int i = 1000; i < 1050; i++) {
                    float[] v = new float[dim];
                    for (int j = 0; j < dim; j++) {
                        v[j] = hookRng.nextFloat();
                    }
                    store.addVector(Bytes.from_int(i), v);
                }
                hookExecuted.set(true);
            });

            store.checkpoint();

            assertTrue("Phase B hook should have been executed", hookExecuted.get());

            // Verify total size includes both batches
            int totalSize = store.size();
            assertTrue("Total size should be >= 350 (300 original + 50 during checkpoint), was " + totalSize,
                    totalSize >= 350);

            // Search should find vectors from both batches
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("Search should return results after concurrent DML", results.isEmpty());
        }
    }

    @Test
    public void testMemoryEstimation() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();

            long memBefore = store.estimatedMemoryUsageBytes();
            assertEquals("Empty store should have 0 memory", 0, memBefore);

            int dim = 32;
            Random rng = new Random(42);
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            long memAfter50 = store.estimatedMemoryUsageBytes();
            assertTrue("Memory should be > 0 after adding vectors, was " + memAfter50,
                    memAfter50 > 0);

            for (int i = 50; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            long memAfter100 = store.estimatedMemoryUsageBytes();
            assertTrue("Memory should grow with more vectors: " + memAfter50 + " vs " + memAfter100,
                    memAfter100 > memAfter50);
        }
    }
}
