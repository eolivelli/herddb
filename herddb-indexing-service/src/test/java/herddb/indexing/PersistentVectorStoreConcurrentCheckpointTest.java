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
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that concurrent checkpoint() calls do not lose vectors.
 */
public class PersistentVectorStoreConcurrentCheckpointTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, 5.0,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void testConcurrentCheckpointsDoNotLoseVectors() throws Exception {
        Path tmpDir = tmpFolder.newFolder("concurrent-cp").toPath();
        int numVectors = 10_000;
        int dim = 128;
        Random rng = new Random(42);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Insert all vectors
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }

            assertEquals(numVectors, store.size());

            // Launch two threads that both call checkpoint() concurrently
            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicReference<Exception> error = new AtomicReference<>();

            Thread t1 = new Thread(() -> {
                try {
                    barrier.await();
                    store.checkpoint();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                }
            }, "cp-thread-1");

            Thread t2 = new Thread(() -> {
                try {
                    barrier.await();
                    store.checkpoint();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                }
            }, "cp-thread-2");

            t1.start();
            t2.start();
            t1.join(60_000);
            t2.join(60_000);

            if (error.get() != null) {
                throw error.get();
            }

            // All vectors must still be present
            assertEquals("vectors lost during concurrent checkpoint",
                    numVectors, store.size());
        }
    }
}
