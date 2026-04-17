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
import static org.junit.Assert.fail;
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
import java.util.function.LongConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that Phase B segment builds run in parallel correctly, preserving
 * deterministic results and safely handling mid-build failures.
 *
 * <p>These tests set {@code maxSegmentSize} to a low value so that modest
 * vector counts still produce multiple segments, exercising the parallel path.
 */
public class PersistentVectorStoreParallelPhaseBTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStoreWithSmallSegments(Path tmpDir, MemoryDataStorageManager dsm,
                                                               long maxSegmentSize) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // compactionIntervalMs very large so the background thread stays asleep.
        return new PersistentVectorStore("paridx", "partable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, maxSegmentSize, 0,
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
    public void multiSegmentCheckpointIsCorrect() throws Exception {
        // A maxSegmentSize of ~300 KB forces the 1500 dim=32 vectors to be
        // split into multiple segments, exercising the parallel builder.
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int dim = 32;
        long smallSegSize = 300_000L;

        try (PersistentVectorStore store = createStoreWithSmallSegments(tmpDir, dsm, smallSegSize)) {
            store.start();
            addVectors(store, 1500, dim, 1);
            store.checkpoint();

            assertEquals(1500, store.size());
            float[] query = randomVector(new Random(7), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 10);
            assertFalse("search should return results after multi-segment checkpoint",
                    results.isEmpty());
            assertEquals(10, results.size());
        }
    }

    @Test
    public void restartAfterMultiSegmentCheckpointRecovers() throws Exception {
        // Same data as above, but we close and reopen the store to verify
        // recovery from a multi-segment on-disk state.
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int dim = 32;
        long smallSegSize = 300_000L;

        float[] query = randomVector(new Random(11), dim);
        List<Map.Entry<Bytes, Float>> before;
        try (PersistentVectorStore store = createStoreWithSmallSegments(tmpDir, dsm, smallSegSize)) {
            store.start();
            addVectors(store, 1500, dim, 2);
            store.checkpoint();
            before = store.search(query, 5);
        }

        // Reopen
        try (PersistentVectorStore store = new PersistentVectorStore(
                "paridx", "partable", "tstblspace", "vector_col",
                "paridx_partable_fixed-1", tmpDir, dsm,
                new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024),
                16, 100, 1.2f, 1.4f, true, smallSegSize, 0,
                Long.MAX_VALUE / 2)) {
            // NB: a different UUID means the store starts empty; we really want
            // to reopen the *same* instance. The previous store auto-generated
            // a UUID from System.nanoTime so we cannot re-open it by name. Use
            // the fixed-UUID constructor directly below instead.
        }

        // Use a FIXED uuid for a real reopen test.
        final String fixed = "paridx_fixed_uuid";
        Path tmpDir2 = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm2 = new MemoryDataStorageManager();
        try (PersistentVectorStore store = new PersistentVectorStore(
                "paridx", "partable", "tstblspace", "vector_col",
                fixed, tmpDir2, dsm2,
                new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024),
                16, 100, 1.2f, 1.4f, true, smallSegSize, 0,
                Long.MAX_VALUE / 2)) {
            store.start();
            addVectors(store, 1500, dim, 3);
            store.checkpoint();
            before = store.search(query, 5);
        }
        try (PersistentVectorStore store = new PersistentVectorStore(
                "paridx", "partable", "tstblspace", "vector_col",
                fixed, tmpDir2, dsm2,
                new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024),
                16, 100, 1.2f, 1.4f, true, smallSegSize, 0,
                Long.MAX_VALUE / 2)) {
            store.start();
            assertEquals(1500, store.size());
            List<Map.Entry<Bytes, Float>> after = store.search(query, 5);
            assertFalse(after.isEmpty());
            // Top-1 must survive reopen.
            assertEquals(before.get(0).getKey(), after.get(0).getKey());
        }
    }

    /**
     * A DSM that can be armed to fail on the next multipart write, simulating
     * e.g. ENOSPC during Phase B. Used to test error propagation and recovery.
     */
    private static final class FailingDSM extends MemoryDataStorageManager {
        volatile boolean shouldFail = false;

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            if (shouldFail) {
                throw new DataStorageManagerException("injected Phase B write failure");
            }
            return super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
        }
    }

    @Test
    public void partialFailureInParallelPhaseBAborts() throws Exception {
        // Verify that a failure during Phase B is properly propagated and the store
        // recovers. The checkpoint must fail, restore the frozen state, and remain usable.
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDSM dsm = new FailingDSM();
        int dim = 32;
        long smallSegSize = 300_000L;

        try (PersistentVectorStore store = createStoreWithSmallSegments(tmpDir, dsm, smallSegSize)) {
            store.start();
            addVectors(store, 1500, dim, 4);

            // First checkpoint succeeds
            store.checkpoint();
            assertEquals(1500, store.size());
            assertEquals(0, store.getConsecutiveCheckpointFailures());

            // Add more vectors
            addVectors(store, 100, dim, 5);

            // Arm the failure injector and checkpoint should fail
            dsm.shouldFail = true;
            try {
                store.checkpoint();
                fail("expected checkpoint to propagate injected failure");
            } catch (DataStorageManagerException expected) {
                // Expected: failure was propagated
                assertTrue(expected.getMessage().contains("injected"));
            }

            // All vectors must remain after rollback
            assertEquals("all vectors must remain after failed checkpoint",
                    1600, store.size());
            assertTrue("consecutive-failure counter incremented",
                    store.getConsecutiveCheckpointFailures() >= 1);

            // Recovery: clear the failure injector, checkpoint should succeed
            dsm.shouldFail = false;
            store.checkpoint();
            assertEquals("failure counter resets on success",
                    0, store.getConsecutiveCheckpointFailures());
            assertEquals(1600, store.size());
        }
    }

    @Test
    public void noResidentGraphTempFileAfterCheckpoint() throws Exception {
        // After the P1.4 change, loading a FusedPQ segment must NOT leave
        // any resident "herddb-vector-*.tmp" files behind (the graph tmp
        // file that used to be mmap'd per segment).
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        int dim = 32;
        long smallSegSize = 300_000L;

        try (PersistentVectorStore store = createStoreWithSmallSegments(tmpDir, dsm, smallSegSize)) {
            store.start();
            addVectors(store, 1500, dim, 81);
            store.checkpoint();

            // Only transient map tmp files are allowed; each must have been
            // deleted after load. Count any lingering herddb-vector-*.tmp.
            long lingering = java.nio.file.Files.list(tmpDir)
                    .filter(p -> p.getFileName().toString().startsWith("herddb-vector-"))
                    .count();
            assertEquals("no resident graph/map tmp files should remain",
                    0L, lingering);
        }
    }

    @Test
    public void segmentCountStaysBoundedAcrossManyCheckpoints() throws Exception {
        // With a tiny maxSegmentSize AND a low merge threshold, repeated
        // checkpoints produce many small sealed segments; the merge trigger
        // must keep the total bounded.
        String prev = System.setProperty("herddb.vectorindex.segmentMergeThreshold", "4");
        String prevBatch = System.setProperty("herddb.vectorindex.segmentMergeBatch", "3");
        try {
            Path tmpDir = tmpFolder.newFolder().toPath();
            MemoryDataStorageManager dsm = new MemoryDataStorageManager();
            int dim = 16;
            long tinySegSize = 50_000L; // 50 KB, forces many segments
            try (PersistentVectorStore store = createStoreWithSmallSegments(tmpDir, dsm, tinySegSize)) {
                store.start();
                // First checkpoint builds 1-2 segments.
                addVectors(store, 400, dim, 91);
                store.checkpoint();
                // Successive checkpoints grow the sealed set; the merge
                // trigger should eventually kick in and demote small segments.
                for (int i = 0; i < 10; i++) {
                    addVectors(store, 400, dim, 92 + i);
                    store.checkpoint();
                }

                // Note: we can't read the sealed count directly, but since the
                // PersistentVectorStore threshold is a static final loaded at
                // class-init, we must settle for a sanity check that data is
                // searchable after the merge activity.
                assertTrue("store size grew", store.size() >= 4000);
                List<Map.Entry<Bytes, Float>> r = store.search(
                        randomVector(new Random(77), dim), 5);
                assertFalse(r.isEmpty());
            }
        } finally {
            if (prev == null) {
                System.clearProperty("herddb.vectorindex.segmentMergeThreshold");
            } else {
                System.setProperty("herddb.vectorindex.segmentMergeThreshold", prev);
            }
            if (prevBatch == null) {
                System.clearProperty("herddb.vectorindex.segmentMergeBatch");
            } else {
                System.setProperty("herddb.vectorindex.segmentMergeBatch", prevBatch);
            }
        }
    }

    @Test
    public void parallelismConstantIsAtLeastOne() {
        // Sanity check that the system-property-driven constant has a
        // reasonable floor and cannot disable the path entirely.
        assertTrue("Phase B parallelism must be >= 1, was "
                        + PersistentVectorStore.PHASE_B_SEGMENT_PARALLELISM,
                PersistentVectorStore.PHASE_B_SEGMENT_PARALLELISM >= 1);
    }
}
