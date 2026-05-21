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

package herddb.indexing.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Risk cover for issue #256: the compactor now owns the synthetic
 * shard's {@link VectorStorage} (instead of reusing the store's global
 * one). If any code path — happy or error — forgets to drop the
 * reference, we leak heap. This test proves reclaimability on both
 * paths and checks that concurrent ingest is isolated from the
 * compactor's private storage.
 */
public class Issue256CompactionReleaseTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
        VectorIndexCompactor.syntheticShardObserverForTest = null;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, Integer.MAX_VALUE, 0);
        return store;
    }

    /**
     * Seeds five segments with {@em heterogeneous} feature sets so the
     * streaming compactor's {@code allCandidatesHaveUniformFeatures} guard
     * sends control to the legacy in-memory rebuild — the path whose
     * synthetic-shard reclamation contract this test class exists to lock
     * in. Four small checkpoints stay below
     * {@code MIN_VECTORS_FOR_FUSED_PQ} (InlineVectors only); one large
     * checkpoint produces a FusedPQ + InlineVectors segment. Together they
     * form a 5-segment mix that compaction picks up wholesale.
     */
    private void seedFiveSegments(PersistentVectorStore store) throws Exception {
        Random rng = new Random(1);
        int dim = 16;
        // Four small segments (InlineVectors only).
        for (int c = 0; c < 4; c++) {
            for (int i = 0; i < 30; i++) {
                store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
            }
            store.checkpoint();
        }
        // One large segment (FusedPQ + InlineVectors) so the candidate set is
        // heterogeneous — required to route compaction through the legacy path
        // after the {@code streamingCompactionEnabled} toggle was removed.
        for (int i = 0; i < PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ; i++) {
            store.addVector(Bytes.from_int(4 * 10_000 + i), vec(rng, dim));
        }
        store.checkpoint();
        assertTrue(store.getSegmentCount() >= 4);
    }

    /**
     * Happy path: a successful compaction must release the synthetic
     * shard (and therefore its {@link VectorStorage}) so the GC can
     * reclaim it.
     */
    @Test
    public void syntheticShardIsReclaimedAfterSuccessfulCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        AtomicReference<WeakReference<PersistentVectorStore.LiveGraphShard>> capturedRef =
                new AtomicReference<>();
        AtomicReference<WeakReference<VectorStorage>> capturedStorageRef =
                new AtomicReference<>();
        VectorIndexCompactor.syntheticShardObserverForTest = shard -> {
            capturedRef.set(new WeakReference<>(shard));
            capturedStorageRef.set(new WeakReference<>(shard.vectorStorage));
        };
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);
            store.runCompactionCycle();
            assertEquals("compaction must have succeeded",
                    1, store.getCompactionSuccessesTotal());
        }
        // Unregister before forcing GC so the observer lambda itself
        // cannot retain a strong reference to the shard.
        VectorIndexCompactor.syntheticShardObserverForTest = null;

        assertTrue("observer must have fired", capturedRef.get() != null);
        assertTrue("observer must have captured a storage ref",
                capturedStorageRef.get() != null);
        for (int gc = 0; gc < 15; gc++) {
            System.gc();
            Thread.sleep(30);
            if (capturedRef.get().get() == null && capturedStorageRef.get().get() == null) {
                break;
            }
        }
        assertTrue("synthetic shard must be reclaimable after successful compaction",
                capturedRef.get().get() == null);
        assertTrue("synthetic VectorStorage must be reclaimable after successful compaction",
                capturedStorageRef.get().get() == null);
    }

    /**
     * Failure path: if the compactor throws mid-way, the synthetic
     * shard must still be reclaimable. Before issue #256, the compactor
     * coordinated with the store's global vectorStorage and the
     * release path hinged on {@code releaseCompactionNodeIds}. Now the
     * shard owns its own storage, so an exception anywhere past the
     * {@code new LiveGraphShard(...)} call must still let GC collect it.
     */
    @Test
    public void syntheticShardIsReclaimedOnWriteFailure() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        AtomicReference<WeakReference<PersistentVectorStore.LiveGraphShard>> capturedRef =
                new AtomicReference<>();
        AtomicReference<WeakReference<VectorStorage>> capturedStorageRef =
                new AtomicReference<>();
        VectorIndexCompactor.syntheticShardObserverForTest = shard -> {
            capturedRef.set(new WeakReference<>(shard));
            capturedStorageRef.set(new WeakReference<>(shard.vectorStorage));
        };
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);
            int segmentsBefore = store.getSegmentCount();

            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();

            assertEquals("segment list must not change on write failure",
                    segmentsBefore, store.getSegmentCount());
            assertEquals("no compaction success on injected write failure",
                    0, store.getCompactionSuccessesTotal());
            assertTrue("a compaction failure must have been recorded",
                    store.getCompactionFailuresWriteIoTotal()
                            + store.getCompactionFailuresMetadataIoTotal() > 0);
        }
        VectorIndexCompactor.syntheticShardObserverForTest = null;

        assertTrue("observer must have fired even on the failure path",
                capturedRef.get() != null);
        for (int gc = 0; gc < 15; gc++) {
            System.gc();
            Thread.sleep(30);
            if (capturedRef.get().get() == null && capturedStorageRef.get().get() == null) {
                break;
            }
        }
        assertTrue("synthetic shard must be reclaimable even after write failure",
                capturedRef.get().get() == null);
        assertTrue("synthetic VectorStorage must be reclaimable even after write failure",
                capturedStorageRef.get().get() == null);
    }

    /**
     * Runs many back-to-back compactions (alternating happy + failing)
     * and asserts each produced synthetic shard is a fresh instance.
     * Before the refactor the compactor shared the store's global
     * {@code vectorStorage}, so repeated compactions would accumulate
     * entries in one growing array. Now each synthetic shard owns its
     * own {@link VectorStorage}, and back-to-back compactions must
     * produce DIFFERENT storage instances.
     */
    @Test
    public void backToBackCompactionsUseFreshSyntheticStorage() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        FailingDsm dsm = new FailingDsm();
        AtomicInteger observerFires = new AtomicInteger();
        AtomicReference<VectorStorage> previousStorage = new AtomicReference<>();
        AtomicBoolean storageReused = new AtomicBoolean(false);
        VectorIndexCompactor.syntheticShardObserverForTest = shard -> {
            observerFires.incrementAndGet();
            VectorStorage prev = previousStorage.getAndSet(shard.vectorStorage);
            if (prev != null && prev == shard.vectorStorage) {
                storageReused.set(true);
            }
        };
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);

            // 3 cycles: fail, succeed, succeed (with fresh ingestion between).
            dsm.failNextMultipartWrites.set(1);
            store.runCompactionCycle();

            Random rng = new Random(99);
            for (int i = 0; i < 200; i++) {
                store.addVector(Bytes.from_int(700_000 + i), vec(rng, 16));
            }
            store.checkpoint();

            store.runCompactionCycle();
            store.runCompactionCycle();
        }
        VectorIndexCompactor.syntheticShardObserverForTest = null;
        assertTrue("compactor must have fired at least twice", observerFires.get() >= 2);
        assertTrue("each compaction must get its own VectorStorage instance",
                !storageReused.get());
    }

    /**
     * Concurrent-ingest isolation: while a compaction is in flight the
     * ingest path must be able to keep writing into the live shards
     * without touching the compactor's private storage.
     */
    @Test(timeout = 60_000)
    public void ingestContinuesDuringCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            seedFiveSegments(store);

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicInteger inserts = new AtomicInteger();
            CountDownLatch started = new CountDownLatch(1);
            ExecutorService exec = Executors.newSingleThreadExecutor();
            try {
                exec.submit(() -> {
                    started.countDown();
                    Random rng = new Random(55);
                    while (!stop.get()) {
                        store.addVector(Bytes.from_int(800_000 + inserts.incrementAndGet()),
                                vec(rng, 16));
                    }
                });
                started.await();
                // Burn a compaction cycle concurrently with the ingest thread.
                store.runCompactionCycle();
                assertEquals(1, store.getCompactionSuccessesTotal());
            } finally {
                stop.set(true);
                exec.shutdown();
                if (!exec.awaitTermination(30, TimeUnit.SECONDS)) {
                    fail("ingest thread did not stop in time");
                }
            }
            assertTrue("ingest must have progressed during compaction, inserts=" + inserts.get(),
                    inserts.get() > 50);
        }
    }

    /**
     * Failure-injection DSM modeled on
     * {@code VectorIndexCompactorFailureTest.FailingDsm}. Only covers
     * the hook we need for this test (failNextMultipartWrites).
     */
    private static final class FailingDsm extends MemoryDataStorageManager {
        final AtomicInteger failNextMultipartWrites = new AtomicInteger(0);

        @Override
        public String writeMultipartIndexFile(
                String tableSpace, String segmentUuid, String kind,
                Path tempFile, LongConsumer bytesWrittenConsumer)
                throws DataStorageManagerException, IOException {
            if (failNextMultipartWrites.get() > 0) {
                failNextMultipartWrites.decrementAndGet();
                throw new DataStorageManagerException(
                        new IOException("injected multipart write failure for " + kind));
            }
            return super.writeMultipartIndexFile(tableSpace, segmentUuid, kind, tempFile,
                    bytesWrittenConsumer);
        }
    }
}
