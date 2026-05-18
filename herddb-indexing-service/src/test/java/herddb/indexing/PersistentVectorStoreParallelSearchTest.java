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
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the parallel search path added in issue #245. Verifies that
 * parallel search produces results identical to serial search on a matched
 * dataset, that the parallel path behaves correctly across on-disk segments
 * and live shards, and that the serial fallback (parallelism=1) is
 * preserved.
 */
public class PersistentVectorStoreParallelSearchTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 32;

    private PersistentVectorStore createStore(Path tmpDir, String name, int searchParallelism) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // Explicit indexUUID so both serial and parallel stores use the same
        // auto-generated UUID shape; start() does not care about collisions
        // because each store owns its own MemoryDataStorageManager.
        return new PersistentVectorStore(name, "testtable", "tstblspace",
                "vector_col", name + "-uuid", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, // compaction disabled for tests
                VectorSimilarityFunction.COSINE,
                Long.MAX_VALUE, null, 0L, 0L,
                searchParallelism);
    }

    private float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Builds a store with the given parallelism, inserts {@code totalVectors}
     * using the given seed, and forces {@code segmentCount} checkpoints so
     * the store ends up with that many on-disk segments (plus whatever is in
     * the active live shard after the final checkpoint).
     */
    private PersistentVectorStore buildPopulatedStore(String name, int searchParallelism,
                                                      long seed, int totalVectors,
                                                      int segmentCount) throws Exception {
        Path tmpDir = tmpFolder.newFolder(name).toPath();
        PersistentVectorStore store = createStore(tmpDir, name, searchParallelism);
        store.start();
        Random rng = new Random(seed);
        int perBatch = totalVectors / segmentCount;
        int id = 0;
        for (int s = 0; s < segmentCount; s++) {
            int end = (s == segmentCount - 1) ? totalVectors : id + perBatch;
            for (; id < end; id++) {
                store.addVector(Bytes.from_int(id), randomVector(rng));
            }
            store.checkpoint();
        }
        return store;
    }

    private void assertSameResults(List<Map.Entry<Bytes, Float>> expected,
                                    List<Map.Entry<Bytes, Float>> actual) {
        assertEquals("result size differs", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("pk differs at rank " + i,
                    expected.get(i).getKey(), actual.get(i).getKey());
            assertEquals("score differs at rank " + i,
                    expected.get(i).getValue(), actual.get(i).getValue(), 0.0f);
        }
    }

    @Test
    public void parallelMatchesSerialOnSingleSegment() throws Exception {
        try (PersistentVectorStore serial = buildPopulatedStore(
                     "serial-single", 1, 42L, 200, 1);
             PersistentVectorStore parallel = buildPopulatedStore(
                     "parallel-single", 4, 42L, 200, 1)) {
            Random rng = new Random(99);
            for (int q = 0; q < 5; q++) {
                float[] query = randomVector(rng);
                List<Map.Entry<Bytes, Float>> s = serial.search(query, 10);
                List<Map.Entry<Bytes, Float>> p = parallel.search(query, 10);
                assertFalse("serial returned empty", s.isEmpty());
                assertSameResults(s, p);
            }
        }
    }

    @Test
    public void parallelMatchesSerialOnMultipleSegments() throws Exception {
        // 4 on-disk segments. Parallelism 4 fans out across them.
        try (PersistentVectorStore serial = buildPopulatedStore(
                     "serial-multi", 1, 1234L, 400, 4);
             PersistentVectorStore parallel = buildPopulatedStore(
                     "parallel-multi", 4, 1234L, 400, 4)) {
            Random rng = new Random(321);
            for (int q = 0; q < 8; q++) {
                float[] query = randomVector(rng);
                List<Map.Entry<Bytes, Float>> s = serial.search(query, 20);
                List<Map.Entry<Bytes, Float>> p = parallel.search(query, 20);
                assertFalse("serial returned empty", s.isEmpty());
                assertSameResults(s, p);
            }
        }
    }

    @Test
    public void parallelMatchesSerialOnHybrid() throws Exception {
        // Build 2 segments, then leave 80 vectors in the live shard (no
        // trailing checkpoint). This exercises the on-disk + live-shard
        // fan-out together.
        Path serialTmp = tmpFolder.newFolder("serial-hybrid").toPath();
        Path parallelTmp = tmpFolder.newFolder("parallel-hybrid").toPath();
        try (PersistentVectorStore serial = createStore(serialTmp, "serial-hybrid", 1);
             PersistentVectorStore parallel = createStore(parallelTmp, "parallel-hybrid", 4)) {
            serial.start();
            parallel.start();

            Random rngS = new Random(55L);
            Random rngP = new Random(55L);
            int id = 0;
            for (int s = 0; s < 2; s++) {
                for (int i = 0; i < 160; i++, id++) {
                    float[] v1 = randomVector(rngS);
                    serial.addVector(Bytes.from_int(id), v1);
                    float[] v2 = randomVector(rngP);
                    // same RNG seed → same vector stream
                    parallel.addVector(Bytes.from_int(id), v2);
                }
                serial.checkpoint();
                parallel.checkpoint();
            }
            // Trailing inserts stay in the live shard.
            for (int i = 0; i < 80; i++, id++) {
                float[] v1 = randomVector(rngS);
                serial.addVector(Bytes.from_int(id), v1);
                float[] v2 = randomVector(rngP);
                parallel.addVector(Bytes.from_int(id), v2);
            }

            Random qrng = new Random(777);
            for (int q = 0; q < 5; q++) {
                float[] query = randomVector(qrng);
                List<Map.Entry<Bytes, Float>> sRes = serial.search(query, 15);
                List<Map.Entry<Bytes, Float>> pRes = parallel.search(query, 15);
                assertFalse("serial returned empty", sRes.isEmpty());
                assertSameResults(sRes, pRes);
            }
        }
    }

    @Test
    public void concurrentQueriesOnParallelStoreStayConsistent() throws Exception {
        try (PersistentVectorStore reference = buildPopulatedStore(
                     "serial-ref", 1, 7L, 300, 3);
             PersistentVectorStore parallel = buildPopulatedStore(
                     "parallel-conc", 4, 7L, 300, 3)) {
            // Precompute reference top-10 for a fixed set of queries.
            Random qrng = new Random(424242L);
            int numQueries = 8;
            List<float[]> queries = new ArrayList<>(numQueries);
            List<List<Map.Entry<Bytes, Float>>> expected = new ArrayList<>(numQueries);
            for (int i = 0; i < numQueries; i++) {
                float[] q = randomVector(qrng);
                queries.add(q);
                expected.add(reference.search(q, 10));
            }

            ExecutorService clients = Executors.newFixedThreadPool(6);
            try {
                List<Callable<List<Map.Entry<Bytes, Float>>>> tasks = new ArrayList<>();
                List<Integer> queryIdx = new ArrayList<>();
                // Fire each query several times from multiple client threads.
                for (int rep = 0; rep < 6; rep++) {
                    for (int i = 0; i < numQueries; i++) {
                        final int idx = i;
                        tasks.add(() -> parallel.search(queries.get(idx), 10));
                        queryIdx.add(idx);
                    }
                }
                List<Future<List<Map.Entry<Bytes, Float>>>> futures = clients.invokeAll(tasks);
                for (int i = 0; i < futures.size(); i++) {
                    List<Map.Entry<Bytes, Float>> got = futures.get(i).get(60, TimeUnit.SECONDS);
                    assertSameResults(expected.get(queryIdx.get(i)), got);
                }
            } finally {
                clients.shutdownNow();
                clients.awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void serialFallbackProducesSameResults() throws Exception {
        // parallelism = 1 must not allocate a pool and must return the same
        // results as the default (serial) constructor path.
        Path defaultTmp = tmpFolder.newFolder("default-ctor").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore fallback = buildPopulatedStore(
                     "fallback-p1", 1, 11L, 100, 2);
             PersistentVectorStore defaultStore = new PersistentVectorStore(
                     "default", "testtable", "tstblspace",
                     "vector_col", defaultTmp, dsm, mm,
                     16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                     Long.MAX_VALUE,
                     VectorSimilarityFunction.COSINE)) {
            defaultStore.start();
            Random rng = new Random(11L);
            int id = 0;
            for (int batch = 0; batch < 2; batch++) {
                for (int i = 0; i < 50; i++, id++) {
                    defaultStore.addVector(Bytes.from_int(id), randomVector(rng));
                }
                defaultStore.checkpoint();
            }
            Random qrng = new Random(13);
            for (int q = 0; q < 4; q++) {
                float[] query = randomVector(qrng);
                assertSameResults(defaultStore.search(query, 8), fallback.search(query, 8));
            }
        }
    }
}
