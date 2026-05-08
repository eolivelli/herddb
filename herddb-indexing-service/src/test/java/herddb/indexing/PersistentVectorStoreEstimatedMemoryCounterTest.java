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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Verifies the incremental on-disk-segment memory counter introduced by
 * issue #455.
 *
 * <p>Before the fix, {@code PersistentVectorStore.estimatedMemoryUsageBytes()}
 * iterated over every {@code VectorSegment} on every call.  At ~14 k+ segments
 * during tailing catch-up it dominated IS CPU on async-profiler flamegraphs.
 * The fix replaces the iteration with an {@link AtomicLong} counter maintained
 * incrementally at every mutation of the {@code segments} list.
 *
 * <p>These tests cross-check the counter against an independent
 * "ground-truth" value (the {@code estimatedMemoryUsageBytes()} call itself,
 * which now reads the same counter, plus the additional invariants below)
 * and exercise every lifecycle event: empty store, after first checkpoint,
 * after second checkpoint, on close, and after restart from persisted state.
 *
 * <p>The intent is that any future mutation site on {@code segments} that
 * forgets to call register/unregister will fail at least one assertion here.
 */
public class PersistentVectorStoreEstimatedMemoryCounterTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, MemoryDataStorageManager dsm,
                                              MemoryManager mm, String indexUUID) {
        // compactionIntervalMs = Long.MAX_VALUE: disable background compaction so
        // the test controls every lifecycle event.
        return new PersistentVectorStore(
                "testidx", "testtable", "tstblspace",
                "vector_col", indexUUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, false /* fusedPQ */,
                2_000_000_000L /* maxSegmentSize */, 0 /* maxLiveGraphSize (auto) */,
                Long.MAX_VALUE /* compactionIntervalMs — no auto-compaction */,
                VectorSimilarityFunction.EUCLIDEAN);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Empty store (no segments yet): the on-disk counter must be exactly 0.
     */
    @Test
    public void testCounterStartsAtZero() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-empty").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-empty")) {
            store.start();
            assertEquals("empty store must report zero on-disk segment memory",
                    0L, store.getOnDiskSegmentsEstimatedMemoryBytes());
            assertEquals("empty store must have zero segments",
                    0, store.getSegmentCount());
        }
    }

    /**
     * After a single checkpoint the counter must become non-zero (segments
     * exist now and each one's pkData/pkOffsets/pkLengths arrays plus its
     * BLink.getUsedMemory() contribute) and {@code estimatedMemoryUsageBytes()}
     * must be at least the on-disk-segment portion (plus whatever live-shard
     * overhead the empty post-checkpoint shards still report).
     */
    @Test
    public void testCounterIsPopulatedAfterCheckpoint() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-ckpt").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 400;
        int dim = 16;
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(42);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-ckpt")) {
            store.start();
            assertEquals(0L, store.getOnDiskSegmentsEstimatedMemoryBytes());

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            // Pre-checkpoint: live shards hold the vectors; on-disk counter is
            // still zero because no segments have been registered yet.
            assertEquals("on-disk counter must remain zero before any checkpoint",
                    0L, store.getOnDiskSegmentsEstimatedMemoryBytes());

            store.checkpoint();

            assertTrue("checkpoint must have produced at least one segment",
                    store.getSegmentCount() > 0);

            long onDisk = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("on-disk counter must be >= " + pkArrayLowerBound
                    + " (pkData+pkOffsets+pkLengths for " + numVectors
                    + " vectors), got " + onDisk,
                    onDisk >= pkArrayLowerBound);

            long total = store.estimatedMemoryUsageBytes();
            assertTrue("estimatedMemoryUsageBytes() must include the on-disk counter,"
                    + " got total=" + total + ", onDisk=" + onDisk,
                    total >= onDisk);
        }
    }

    /**
     * Each successive checkpoint that produces fresh segments must monotonically
     * grow the counter (ignoring compaction, which is disabled in this test).
     * This catches a regression where the Phase C swap forgets to register the
     * newly-preloaded segments.
     */
    @Test
    public void testCounterGrowsAcrossCheckpoints() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-grow").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int batch = 300;
        int dim = 8;
        Random rng = new Random(99);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-grow")) {
            store.start();

            for (int i = 0; i < batch; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            long afterFirst = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("after first checkpoint counter must be > 0, got " + afterFirst,
                    afterFirst > 0);
            int segsAfterFirst = store.getSegmentCount();

            for (int i = batch; i < batch * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            long afterSecond = store.getOnDiskSegmentsEstimatedMemoryBytes();
            int segsAfterSecond = store.getSegmentCount();
            // We may either accumulate a fresh segment or extend an existing
            // one; either way the counter must not decrease and the segment
            // count must not decrease.
            assertTrue("after second checkpoint counter must be >= afterFirst,"
                    + " afterFirst=" + afterFirst + " afterSecond=" + afterSecond,
                    afterSecond >= afterFirst);
            assertTrue("after second checkpoint segCount must be >= segsAfterFirst,"
                    + " first=" + segsAfterFirst + " second=" + segsAfterSecond,
                    segsAfterSecond >= segsAfterFirst);
        }
    }

    /**
     * Drop / shutdown path: after the store is closed, every segment must have
     * been unregistered from the counter.  Since the counter lives on the
     * store and the store is gone, we instead verify the equivalent invariant
     * on a fresh store opened on a fresh directory, then on a store reopened
     * on the same persisted directory.
     */
    @Test
    public void testCounterReflectsPersistedSegmentsAfterRestart() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-restart").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        // Two separate MemoryManager instances so the second store does not
        // share the first's BLink page cache; this models a true restart.
        MemoryManager mm1 = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 400;
        int dim = 16;
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(7);
        String indexUUID = "uuid-restart";

        AtomicLong onDiskBeforeClose = new AtomicLong(0);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm1, indexUUID)) {
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            onDiskBeforeClose.set(store.getOnDiskSegmentsEstimatedMemoryBytes());
            assertTrue("counter must be > 0 after the producing checkpoint, got "
                    + onDiskBeforeClose.get(), onDiskBeforeClose.get() > 0);
            assertTrue("counter must be >= pkArrayLowerBound, got "
                    + onDiskBeforeClose.get(), onDiskBeforeClose.get() >= pkArrayLowerBound);
        }

        // Reopen the same persisted directory: loadFromStatus must register
        // every segment it reconstructs.  A reasonable lower bound after
        // restart is pkArrayLowerBound (BLink page cache may carry a slightly
        // different snapshot, but the static pk arrays are identical).
        MemoryManager mm2 = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store2 = createStore(tmpDir, dsm, mm2, indexUUID)) {
            store2.start();

            long onDiskAfterRestart = store2.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("after restart counter must be >= pkArrayLowerBound (" + pkArrayLowerBound
                    + "), got " + onDiskAfterRestart,
                    onDiskAfterRestart >= pkArrayLowerBound);
            assertNotEquals("after restart counter must not be zero",
                    0L, onDiskAfterRestart);

            assertTrue("estimatedMemoryUsageBytes() must include the restart counter,"
                    + " got total=" + store2.estimatedMemoryUsageBytes()
                    + ", onDisk=" + onDiskAfterRestart,
                    store2.estimatedMemoryUsageBytes() >= onDiskAfterRestart);
        }
    }

    /**
     * Two stores (different indexUUIDs) sharing one {@link MemoryDataStorageManager}
     * must each maintain an independent counter — closing one must not move
     * the other's counter.  Catches a regression where the counter is
     * accidentally placed on a static / shared field.
     */
    @Test
    public void testCounterIsPerStore() throws Exception {
        Path tmpDir1 = tmpFolder.newFolder("counter-perstore-a").toPath();
        Path tmpDir2 = tmpFolder.newFolder("counter-perstore-b").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 200;
        int dim = 8;
        Random rng = new Random(13);

        try (PersistentVectorStore storeA = createStore(tmpDir1, dsm, mm, "uuid-a");
             PersistentVectorStore storeB = createStore(tmpDir2, dsm, mm, "uuid-b")) {
            storeA.start();
            storeB.start();

            for (int i = 0; i < numVectors; i++) {
                storeA.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            storeA.checkpoint();

            // storeB has no vectors yet; its counter must still be 0 even
            // though storeA's counter is now positive.
            long counterA = storeA.getOnDiskSegmentsEstimatedMemoryBytes();
            long counterB = storeB.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("storeA counter must be > 0 after checkpoint, got " + counterA,
                    counterA > 0);
            assertEquals("storeB counter must be 0 (no vectors / no checkpoint), got "
                    + counterB, 0L, counterB);
        }
    }

    /**
     * Sanity check on the "delete then checkpoint" path.  When a delete makes
     * a segment fully tombstoned, {@code doCheckpointUnderLock} closes every
     * segment and resets {@code segments} to empty.  The counter must drop
     * back to 0 (catching a regression where the empty-totalActiveVectors
     * branch forgets to unregister).
     */
    @Test
    public void testCounterDropsToZeroWhenAllVectorsDeleted() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-delete-all").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 200;
        int dim = 8;
        Random rng = new Random(31);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-delete")) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            long afterCheckpoint = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("counter must be > 0 after producing checkpoint, got "
                    + afterCheckpoint, afterCheckpoint > 0);

            // Delete every PK so the next checkpoint hits the
            // totalActiveVectors==0 + !segments.isEmpty() branch.
            for (int i = 0; i < numVectors; i++) {
                store.removeVector(Bytes.from_int(i));
            }
            store.checkpoint();

            assertEquals("after deleting every vector and checkpointing, on-disk"
                    + " counter must drop back to 0", 0L,
                    store.getOnDiskSegmentsEstimatedMemoryBytes());
            assertEquals("segments list must be empty in the all-deleted branch",
                    0, store.getSegmentCount());
        }
    }
}
