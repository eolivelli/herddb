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
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that multipart-mode segments (used with remote file storage) can be
 * checkpointed, recovered after restart, and re-checkpointed correctly.
 *
 * <p>Uses the in-memory multipart-capable {@link MemoryDataStorageManager} so
 * the full multipart code path is exercised without a real remote file server.
 */
public class PersistentVectorStoreMultipartRecoveryTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final String FIXED_UUID = "multipart_test_fixed";
    private static final int DIM = 32;

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("mpidx", "mptable", "mpspace",
                "vector_col", FIXED_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE / 2);
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static void addVectors(PersistentVectorStore store, int count, int dim, int seed) {
        Random rng = new Random(seed);
        for (int i = 0; i < count; i++) {
            store.addVector(Bytes.from_int(seed * 100000 + i), randomVector(rng, dim));
        }
    }

    @Test
    public void multipartCheckpointAndSearchWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 1);
            store.checkpoint();

            assertEquals(500, store.size());
            float[] query = randomVector(new Random(99), DIM);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("search should return results after multipart checkpoint",
                    results.isEmpty());
            assertEquals(10, results.size());
        }
    }

    @Test
    public void multipartRestartRecoveryWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        float[] query = randomVector(new Random(42), DIM);
        List<Map.Entry<Bytes, Float>> before;

        // First run: add vectors and checkpoint
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 2);
            store.checkpoint();
            before = store.search(query, 5);
            assertEquals(500, store.size());
        }

        // Reopen with same DSM (simulates pod restart with same PVC)
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(500, store.size());
            List<Map.Entry<Bytes, Float>> after = store.search(query, 5);
            assertFalse(after.isEmpty());
            assertEquals("top-1 result must survive restart",
                    before.get(0).getKey(), after.get(0).getKey());
        }
    }

    @Test
    public void multipartCheckpointAfterRecoveryWorks() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        // First run
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 3);
            store.checkpoint();
        }

        // Second run: recover, add more vectors, checkpoint again
        float[] query = randomVector(new Random(77), DIM);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(500, store.size());

            addVectors(store, 300, DIM, 4);
            store.checkpoint();

            assertEquals(800, store.size());
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse(results.isEmpty());
            assertEquals(10, results.size());
        }

        // Third run: verify everything persisted correctly
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals(800, store.size());
        }
    }

    /**
     * In-memory multipart DSM that can be armed to fail the next write.
     */
    private static final class FailableMultipartDSM extends MemoryDataStorageManager {
        final AtomicBoolean failNext = new AtomicBoolean(false);

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (failNext.compareAndSet(true, false)) {
                throw new DataStorageManagerException("injected multipart write failure");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }
    }

    @Test
    public void multipartPhaseBFailureRecovery() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailableMultipartDSM dsm = new FailableMultipartDSM();

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 500, DIM, 5);

            // Arm failure for next multipart write
            dsm.failNext.set(true);

            // Checkpoint should fail but recover gracefully
            try {
                store.checkpoint();
                // If it succeeds, the failure was on a non-critical path or was retried
            } catch (Exception e) {
                // Expected: Phase B failure
                assertTrue("should be storage exception",
                        e instanceof DataStorageManagerException
                        || e.getCause() instanceof DataStorageManagerException);
            }

            // Store should still be usable after failure recovery
            assertEquals(500, store.size());
            List<Map.Entry<Bytes, Float>> results = store.search(
                    randomVector(new Random(55), DIM), 5);
            assertFalse("search should work after failed checkpoint", results.isEmpty());

            // A subsequent checkpoint should succeed
            store.checkpoint();
            assertEquals(500, store.size());
        }
    }
}
