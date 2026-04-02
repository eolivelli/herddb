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
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reproduces a livelock where memory-pressure interrupts repeatedly abort
 * the compaction thread's checkpoint, preventing memory from ever being freed
 * and permanently blocking ingestion.
 *
 * <p>The fix makes the ForkJoinTask.get() wait in writeFusedPQGraph
 * uninterruptible, so that a checkpoint in progress always completes
 * even when interrupted by backpressure signals.
 */
public class PersistentVectorStoreBackpressureLivelockTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 32;

    private float[] randomVector(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Verifies that interrupting the compaction thread during Phase B
     * (as memory-pressure backpressure does) does NOT abort the checkpoint.
     * The checkpoint should complete successfully despite the interrupts.
     */
    @Test(timeout = 120_000)
    public void testInterruptDuringPhaseBDoesNotAbortCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("backpressure-livelock").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Disable automatic compaction; we trigger checkpoint manually.
        try (PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, 5.0,
                VectorSimilarityFunction.EUCLIDEAN)) {

            AtomicInteger interruptCount = new AtomicInteger();

            // Hook that interrupts the current (compaction) thread during Phase B,
            // simulating what waitForMemoryPressureRelief does via ct.interrupt().
            store.setCheckpointPhaseBHook(() -> {
                interruptCount.incrementAndGet();
                Thread.currentThread().interrupt();
            });

            store.start();

            // Insert enough vectors to build a meaningful graph
            Random rng = new Random(42);
            int numVectors = 500;
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng));
            }

            assertEquals(numVectors, store.size());

            // Manually trigger checkpoint — this should succeed despite the
            // interrupt set by the hook, because the fix makes the
            // ForkJoinTask.get() wait uninterruptibly.
            store.checkpoint();

            // Verify the hook was actually called
            assertTrue("Phase B hook should have been called",
                    interruptCount.get() > 0);

            // Verify the store is consistent — vectors are still searchable
            assertTrue("Store should still have vectors after checkpoint",
                    store.size() > 0);
        }
    }

    /**
     * End-to-end test with a small memory limit and active compaction loop.
     * Verifies that ingestion completes even when memory pressure causes
     * repeated interrupts to the compaction thread.
     */
    @Test(timeout = 120_000)
    public void testIngestionCompletesUnderMemoryPressure() throws Exception {
        Path tmpDir = tmpFolder.newFolder("backpressure-e2e").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        // Small memory limit to trigger backpressure during ingestion.
        // Each vector: DIM * 4 bytes * memoryMultiplier(5.0) = 640 bytes.
        // 256KB allows ~400 vectors before backpressure kicks in.
        long smallMemoryLimit = 256 * 1024;
        long compactionIntervalMs = 100;

        try (PersistentVectorStore store = new PersistentVectorStore(
                "vidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                compactionIntervalMs, 5.0,
                VectorSimilarityFunction.EUCLIDEAN,
                smallMemoryLimit)) {

            store.start();

            // Insert more vectors than fit in the memory limit.
            // This forces multiple checkpoint cycles with backpressure.
            int numVectors = 2000;
            AtomicReference<Exception> ingestionError = new AtomicReference<>();
            CountDownLatch ingestionDone = new CountDownLatch(1);

            Thread ingestionThread = new Thread(() -> {
                try {
                    Random rng = new Random(42);
                    for (int i = 0; i < numVectors; i++) {
                        store.addVector(Bytes.from_int(i), randomVector(rng));
                    }
                } catch (Exception e) {
                    ingestionError.set(e);
                } finally {
                    ingestionDone.countDown();
                }
            }, "ingestion-thread");

            ingestionThread.start();

            // Wait for ingestion to complete. Before the fix, this would
            // timeout because the livelock prevents checkpoints from
            // completing, so memory pressure is never relieved.
            boolean completed = ingestionDone.await(90, TimeUnit.SECONDS);
            assertTrue("Ingestion should complete without permanent block", completed);

            if (ingestionError.get() != null) {
                fail("Ingestion failed: " + ingestionError.get());
            }

            assertTrue("Store should have vectors after ingestion", store.size() > 0);
        }
    }
}
