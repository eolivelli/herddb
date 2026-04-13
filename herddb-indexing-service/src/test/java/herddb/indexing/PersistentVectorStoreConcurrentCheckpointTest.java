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
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies that concurrent checkpoint() calls do not lose vectors.
 */
public class PersistentVectorStoreConcurrentCheckpointTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private int savedMinLive;
    private long savedMaxDeferral;

    @Before
    public void saveGateState() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        savedMaxDeferral = PersistentVectorStore.maxCheckpointDeferralMs;
        // Disable the min-live gate by default so the pre-existing test
        // (which inserts 10k vectors) is not accidentally deferred.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreGateState() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
        PersistentVectorStore.maxCheckpointDeferralMs = savedMaxDeferral;
    }

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
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

    /**
     * A second checkpoint() call that loses the tryLock race must return
     * {@code false} so the engine does not advance the watermark past vectors
     * that live only in the new live shard (issue #90, Part 3).
     */
    @Test
    public void testCheckpointReturnsFalseOnTryLockSkip() throws Exception {
        Path tmpDir = tmpFolder.newFolder("trylock-skip").toPath();
        int numVectors = 512; // enough to activate FusedPQ path
        int dim = 32;
        Random rng = new Random(7);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertEquals(numVectors, store.size());

            CountDownLatch phaseBEntered = new CountDownLatch(1);
            CountDownLatch releasePhaseB = new CountDownLatch(1);
            store.setCheckpointPhaseBHook(() -> {
                phaseBEntered.countDown();
                try {
                    // Long enough to survive slow CI machines; the test
                    // releases the latch as soon as the second checkpoint
                    // call returns, so the usual wait is sub-millisecond.
                    releasePhaseB.await(120, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            AtomicBoolean firstResult = new AtomicBoolean();
            AtomicReference<Throwable> error = new AtomicReference<>();
            Thread parked = new Thread(() -> {
                try {
                    firstResult.set(store.checkpoint());
                } catch (Throwable t) {
                    error.compareAndSet(null, t);
                }
            }, "cp-parked");
            parked.start();

            assertTrue("first checkpoint did not reach Phase B within 60s",
                    phaseBEntered.await(60, TimeUnit.SECONDS));

            // The second caller races the tryLock and must get false back
            // immediately, without blocking on the parked Phase B.
            long before = System.currentTimeMillis();
            boolean secondResult = store.checkpoint();
            long elapsed = System.currentTimeMillis() - before;
            assertFalse("concurrent checkpoint should return false on tryLock-skip",
                    secondResult);
            assertTrue("tryLock-skip should not block on Phase B (elapsed=" + elapsed + "ms)",
                    elapsed < 5_000);

            releasePhaseB.countDown();
            // Generous timeout: Phase B + Phase C on 512 tiny vectors is
            // usually sub-second locally but can be much slower on CI
            // runners under load.
            parked.join(120_000);
            if (parked.isAlive()) {
                StringBuilder dump = new StringBuilder("cp-parked thread did not finish within 120s, stack:\n");
                for (StackTraceElement f : parked.getStackTrace()) {
                    dump.append("  at ").append(f).append('\n');
                }
                fail(dump.toString());
            }
            if (error.get() != null) {
                throw new AssertionError("parked checkpoint threw", error.get());
            }
            assertTrue("first checkpoint should have returned true", firstResult.get());
            assertEquals("vectors lost after parked checkpoint",
                    numVectors, store.size());
        }
    }

    /**
     * When the min-live-vectors gate is armed and the live shard is below the
     * threshold, {@code checkpoint()} must return {@code false} and leave
     * {@code totalFusedPQCheckpointCount} unchanged (issue #90, Part 2). The
     * bootstrap case (empty segments) and the threshold-crossing case must
     * still run a real Phase B.
     */
    @Test
    public void testMinLiveVectorsGateDefersTrivialCycles() throws Exception {
        Path tmpDir = tmpFolder.newFolder("min-live-gate").toPath();
        int dim = 32;
        Random rng = new Random(11);

        // 10 min deferral — long enough that the time-based backstop does
        // not fire during the test.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 1_000;
        PersistentVectorStore.maxCheckpointDeferralMs = 600_000L;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Bootstrap: 300 vectors, segments empty → must checkpoint.
            // Need >= 256 vectors to exercise the FusedPQ path.
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue("bootstrap checkpoint should run", store.checkpoint());
            long afterBootstrap = store.getTotalFusedPQCheckpointCount();
            long deferredAfterBootstrap = store.getTotalCheckpointsDeferred();
            assertTrue("bootstrap should have run FusedPQ Phase B",
                    afterBootstrap >= 1);

            // Add 500 vectors (below threshold) → gate must defer.
            for (int i = 300; i < 800; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertFalse("gate should defer when live < threshold",
                    store.checkpoint());
            assertEquals("no Phase B should have run during deferral",
                    afterBootstrap, store.getTotalFusedPQCheckpointCount());
            assertEquals("deferred counter should advance",
                    deferredAfterBootstrap + 1, store.getTotalCheckpointsDeferred());

            // Add 600 more vectors (total 1100 live, above threshold) →
            // gate releases, Phase B runs.
            for (int i = 800; i < 1400; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue("gate should release above threshold",
                    store.checkpoint());
            assertEquals("Phase B should have run after threshold crossed",
                    afterBootstrap + 1, store.getTotalFusedPQCheckpointCount());
            assertEquals("total vectors preserved",
                    1400, store.size());
        }
    }

    /**
     * The min-live-vectors gate must not block the very first checkpoint on
     * an empty index, otherwise small indexes would sit entirely in memory.
     */
    @Test
    public void testMinLiveVectorsGateDoesNotBlockBootstrap() throws Exception {
        Path tmpDir = tmpFolder.newFolder("min-live-bootstrap").toPath();
        int dim = 32;
        Random rng = new Random(13);

        PersistentVectorStore.minLiveVectorsForCheckpoint = 10_000;
        PersistentVectorStore.maxCheckpointDeferralMs = 600_000L;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();
            // 300 vectors, far below threshold, but segments are empty.
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue("bootstrap must run even with tiny live shard",
                    store.checkpoint());
            assertTrue("bootstrap should have produced a Phase B",
                    store.getTotalFusedPQCheckpointCount() >= 1);
        }
    }

    /**
     * When the deferral bound elapses, the gate must unconditionally release
     * even if the live shard is still below the min-live threshold. Proves
     * the idle-flush bound that guarantees bounded latency for the
     * "ingest stopped mid-shard" scenario (issue #90).
     */
    @Test
    public void testMaxDeferralReleasesPartialShard() throws Exception {
        Path tmpDir = tmpFolder.newFolder("max-deferral").toPath();
        int dim = 32;
        Random rng = new Random(17);

        PersistentVectorStore.minLiveVectorsForCheckpoint = 1_000;
        PersistentVectorStore.maxCheckpointDeferralMs = 200L;

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Bootstrap with 300 vectors.
            for (int i = 0; i < 300; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertTrue(store.checkpoint());
            long afterBootstrap = store.getTotalFusedPQCheckpointCount();

            // 100 more vectors, still below threshold → defer immediately.
            for (int i = 300; i < 400; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            assertFalse("should defer within the window",
                    store.checkpoint());
            assertEquals(afterBootstrap, store.getTotalFusedPQCheckpointCount());

            // Wait past the deferral bound, then call again → must run.
            Thread.sleep(400);
            assertTrue("gate should release past deferral bound",
                    store.checkpoint());
            assertEquals("a real Phase B should have run after the bound",
                    afterBootstrap + 1, store.getTotalFusedPQCheckpointCount());
            assertEquals(400, store.size());
        }
    }
}
