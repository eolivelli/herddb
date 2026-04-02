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
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link PersistentVectorStore} configuration parameters.
 *
 * @author enrico.olivelli
 */
public class PersistentVectorStoreConfigTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void testDefaultParameters() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, 5.0)) {
            store.start();

            assertEquals("M should be 16", 16, store.getM());
            assertEquals("beamWidth should be 100", 100, store.getBeamWidth());
            assertEquals("neighborOverflow should be 1.2", 1.2f, store.getNeighborOverflow(), 0.001f);
            assertEquals("alpha should be 1.4", 1.4f, store.getAlpha(), 0.001f);
            assertEquals("similarityFunction should be COSINE", "COSINE", store.getSimilarityFunction());
        }
    }

    @Test
    public void testFusedPQDisabled() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Create store with fusedPQ=false
        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, false, 2_000_000_000L, 0,
                Long.MAX_VALUE, 5.0)) {
            store.start();

            int dim = 32;
            Random rng = new Random(42);
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            // Checkpoint with fusedPQ disabled should use simple format
            store.checkpoint();

            assertEquals(300, store.size());

            // Search should still work
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertEquals(5, results.size());
        }
    }

    @Test
    public void testMaxLiveGraphSizeTriggersRotation() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Create store with maxLiveGraphSize=50 to trigger shard rotation
        try (PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 50,
                Long.MAX_VALUE, 5.0)) {
            store.start();

            int dim = 32;
            Random rng = new Random(42);
            for (int i = 0; i < 100; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            assertTrue("With maxLiveGraphSize=50 and 100 vectors, should have > 1 live shard, got "
                    + store.getLiveShardCount(),
                    store.getLiveShardCount() > 1);

            assertEquals(100, store.size());

            // Search should still work across multiple shards
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertTrue("Search should return results", results.size() > 0);
        }
    }
}
