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
import herddb.indexing.vector.PersistentVectorStore;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
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
 * The fix replaces the iteration with an {@code AtomicLong} counter maintained
 * incrementally at every mutation of the {@code segments} list.
 *
 * <p>Each test method exercises one lifecycle path and then calls
 * {@link #assertCounterMatchesIteration} to compare the counter
 * ({@code getOnDiskSegmentsEstimatedMemoryBytes()}) against an independent
 * iterated ground truth
 * ({@code sumOnDiskSegmentMemoryBytesByIterationForTesting()}).  A missed
 * register/unregister at any future mutation site would cause the two values
 * to diverge and the assertion to fail — which is the test contract this
 * class enforces.
 *
 * <p>Because BLink is intentionally excluded from
 * {@code VectorSegment.estimatedInMemoryBytes()} (issue #592), the counter
 * and the iterated sum always agree: both count only the stable
 * pkData/pkOffsets/pkLengths arrays.  The cross-check is therefore valid at
 * any point in the lifecycle, not just at "quiescent BLink" moments.
 */
public class PersistentVectorStoreEstimatedMemoryCounterTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Cross-checks that the incremental counter equals the live iterated sum.
     * Use only at "clean" lifecycle points (see class Javadoc).
     */
    private void assertCounterMatchesIteration(PersistentVectorStore store, String label) {
        long counter = store.getOnDiskSegmentsEstimatedMemoryBytes();
        long iterated = store.sumOnDiskSegmentMemoryBytesByIterationForTesting();
        assertEquals("[" + label + "] on-disk-segment counter must equal the iterated"
                + " sum of seg.estimatedInMemoryBytes() — a missed register/unregister"
                + " at any segments-list mutation site would cause these to diverge",
                iterated, counter);
    }

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
     * Empty store (no segments yet): the on-disk counter must be exactly 0
     * and the iterated sum must agree.
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
            assertCounterMatchesIteration(store, "empty");
        }
    }

    /**
     * After a single checkpoint the counter must become non-zero (segments
     * exist now and each one's pkData/pkOffsets/pkLengths arrays contribute),
     * the live iterated sum must agree exactly, and
     * {@code estimatedMemoryUsageBytes()} must be at least the on-disk
     * portion (plus whatever live-shard overhead the empty post-checkpoint
     * shards still report).
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
            assertCounterMatchesIteration(store, "after start");

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            // Pre-checkpoint: live shards hold the vectors; on-disk counter is
            // still zero because no segments have been registered yet.
            assertEquals("on-disk counter must remain zero before any checkpoint",
                    0L, store.getOnDiskSegmentsEstimatedMemoryBytes());
            assertCounterMatchesIteration(store, "before first checkpoint");

            store.checkpoint();

            assertTrue("checkpoint must have produced at least one segment",
                    store.getSegmentCount() > 0);

            long onDisk = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("on-disk counter must be >= " + pkArrayLowerBound
                    + " (pkData+pkOffsets+pkLengths for " + numVectors
                    + " vectors), got " + onDisk,
                    onDisk >= pkArrayLowerBound);
            assertCounterMatchesIteration(store, "after first checkpoint");

            long total = store.estimatedMemoryUsageBytes();
            assertTrue("estimatedMemoryUsageBytes() must include the on-disk counter,"
                    + " got total=" + total + ", onDisk=" + onDisk,
                    total >= onDisk);
        }
    }

    /**
     * Successive checkpoints must keep the counter in lockstep with the
     * iterated sum.  Catches a regression where the Phase C swap forgets to
     * register the newly-preloaded segments.
     */
    @Test
    public void testCounterMatchesIterationAcrossCheckpoints() throws Exception {
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
            assertCounterMatchesIteration(store, "after first checkpoint");

            for (int i = batch; i < batch * 2; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            long afterSecond = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("after second checkpoint counter must be > 0, got " + afterSecond,
                    afterSecond > 0);
            assertCounterMatchesIteration(store, "after second checkpoint");

            // Third batch + checkpoint to exercise the Phase C swap once more.
            for (int i = batch * 2; i < batch * 3; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertCounterMatchesIteration(store, "after third checkpoint");
        }
    }

    /**
     * Drop / restart path: after the store is closed and reopened against the
     * same persisted directory, {@code loadFromStatus} must register every
     * segment it reconstructs.  The reopened store's counter must equal the
     * iterated sum and be at least {@code pkArrayLowerBound}.
     */
    @Test
    public void testCounterReflectsPersistedSegmentsAfterRestart() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-restart").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm1 = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 400;
        int dim = 16;
        long pkArrayLowerBound = (long) numVectors * Integer.BYTES * 3;
        Random rng = new Random(7);
        String indexUUID = "uuid-restart";

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm1, indexUUID)) {
            store.start();
            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertCounterMatchesIteration(store, "before close");
            assertTrue(store.getOnDiskSegmentsEstimatedMemoryBytes() >= pkArrayLowerBound);
        }

        // Reopen the same persisted directory: loadFromStatus must register
        // every segment it reconstructs.
        MemoryManager mm2 = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        try (PersistentVectorStore store2 = createStore(tmpDir, dsm, mm2, indexUUID)) {
            store2.start();

            long onDiskAfterRestart = store2.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("after restart counter must be >= pkArrayLowerBound (" + pkArrayLowerBound
                    + "), got " + onDiskAfterRestart,
                    onDiskAfterRestart >= pkArrayLowerBound);
            assertNotEquals("after restart counter must not be zero",
                    0L, onDiskAfterRestart);
            assertCounterMatchesIteration(store2, "after restart");
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
            assertCounterMatchesIteration(storeA, "storeA after checkpoint");
            assertCounterMatchesIteration(storeB, "storeB unchanged");
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
            assertCounterMatchesIteration(store, "after producing checkpoint");

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
            assertCounterMatchesIteration(store, "after delete-all checkpoint");
        }
    }

    /**
     * Compaction merge path ({@code atomicSwapCompactionResult}): after
     * {@code runCompactionCycle()} the counter must equal the iterated sum.
     * Specifically catches a regression where the merge swap forgets to
     * unregister the input segments or to register the merged output.
     */
    @Test
    public void testCounterAfterCompactionMerge() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-compact").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int dim = 16;
        int batch = 300;
        Random rng = new Random(2026);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-compact")) {
            // Aggressive compaction policy so the test cycle actually merges
            // all of the small per-checkpoint segments into one output.
            // Args: intervalMs (unused — we drive the cycle ourselves),
            // minBytes=1, maxBytes=Long.MAX_VALUE, minCount=2,
            // maxCount=Integer.MAX_VALUE, retentionMs=0.
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            store.start();

            int totalVectors = 0;
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < batch; i++) {
                    store.addVector(Bytes.from_int(c * 100_000 + i),
                            randomVector(rng, dim));
                    totalVectors++;
                }
                store.checkpoint();
            }
            int beforeMerge = store.getSegmentCount();
            assertTrue("must have several segments before merge to make the test"
                    + " meaningful, got " + beforeMerge, beforeMerge >= 2);
            long counterBefore = store.getOnDiskSegmentsEstimatedMemoryBytes();
            assertTrue("counter must be > 0 with " + beforeMerge + " segments,"
                    + " got " + counterBefore, counterBefore > 0);
            assertCounterMatchesIteration(store, "before compaction merge");

            store.runCompactionCycle();

            int afterMerge = store.getSegmentCount();
            assertTrue("compaction must have reduced segment count: before="
                    + beforeMerge + " after=" + afterMerge,
                    afterMerge < beforeMerge);
            // Most importantly: counter must equal iterated sum after the merge.
            assertCounterMatchesIteration(store, "after compaction merge");

            // Drop a few vectors then checkpoint so the next cycle exercises
            // the "extend an existing segment" flavour of Phase C.
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(999_000 + i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertCounterMatchesIteration(store, "after post-merge checkpoint");
            // totalVectors used only for failure context above.
            assertTrue("sanity: at least " + (totalVectors + 50)
                    + " vectors total were added", totalVectors + 50 >= 1550);
        }
    }

    /**
     * Read-only shadow {@code reloadFromStatus} path: switching the shadow
     * from one persisted {@code IndexStatus} to another must unregister the
     * old segments and register the new ones, so the counter stays in
     * lockstep with the iterated sum after each reload.
     */
    @Test
    public void testCounterAfterReloadFromStatus() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-reload").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mmLeader = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        MemoryManager mmShadow = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        String indexUUID = "uuid-reload";

        int dim = 16;
        int batch = 300;
        Random rng = new Random(11);

        try (PersistentVectorStore leader = createStore(tmpDir, dsm, mmLeader, indexUUID);
             PersistentVectorStore shadow = createStore(tmpDir, dsm, mmShadow, indexUUID)) {
            // Leader is the writer; tighten compaction so it can produce
            // a merged-segment IndexStatus to feed the shadow.
            leader.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            leader.start();
            shadow.setReadOnly(true);
            shadow.start();
            assertCounterMatchesIteration(shadow, "shadow after start");

            // Leader builds a few segments.
            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < batch; i++) {
                    leader.addVector(Bytes.from_int(c * 100_000 + i),
                            randomVector(rng, dim));
                }
                leader.checkpoint();
            }
            int leaderSegmentsPre = leader.getSegmentCount();
            assertTrue("leader must have multiple segments to drive the test, got "
                    + leaderSegmentsPre, leaderSegmentsPre >= 2);

            // Shadow reloads to the pre-compaction state.
            IndexStatus preStatus = dsm.getIndexStatus("tstblspace", indexUUID,
                    LogSequenceNumber.START_OF_TIME);
            shadow.reloadFromStatus(preStatus);
            assertEquals("shadow segment count must match leader pre-compaction",
                    leaderSegmentsPre, shadow.getSegmentCount());
            assertTrue("shadow counter must be > 0 after reload",
                    shadow.getOnDiskSegmentsEstimatedMemoryBytes() > 0);
            assertCounterMatchesIteration(shadow, "shadow after first reload");

            // Leader compacts to fewer segments.
            leader.runCompactionCycle();
            int leaderSegmentsPost = leader.getSegmentCount();
            assertTrue("compaction must have reduced segment count, before="
                    + leaderSegmentsPre + " after=" + leaderSegmentsPost,
                    leaderSegmentsPost < leaderSegmentsPre);

            // Shadow reloads to the post-compaction state.  This exercises the
            // "unregister all old + register all new" branch of reloadFromStatus.
            IndexStatus postStatus = dsm.getIndexStatus("tstblspace", indexUUID,
                    LogSequenceNumber.START_OF_TIME);
            shadow.reloadFromStatus(postStatus);
            assertEquals("shadow segment count must match leader post-compaction",
                    leaderSegmentsPost, shadow.getSegmentCount());
            assertCounterMatchesIteration(shadow, "shadow after second reload");
        }
    }

    /**
     * Regression test for issue #592: verifies that the on-disk memory counter
     * excludes the BLink pk-to-ordinal tree.
     *
     * <p>Before the fix, {@code VectorSegment.estimatedInMemoryBytes()} included
     * {@code onDiskPkToNode.getUsedMemory()}, which added ≈128 B/vector of phantom
     * cost (BLink entry overhead for a 4-byte INT key).  The total estimated cost
     * was ≈140 B/vector instead of the true ≈12 B/vector (pkData 4 B + pkOffsets
     * 4 B + pkLengths 4 B for INT keys), causing {@code waitForMemoryPressureRelief}
     * to stall ingestion workers far below the actual memory limit.
     *
     * <p>After the fix only the heap arrays are counted.  This test inserts
     * {@code numVectors} vectors, checkpoints, and then asserts that the per-vector
     * on-disk cost is between the theoretical minimum (raw array bytes) and 48
     * bytes — a threshold that is comfortably below the BLink contribution of
     * ≈128 B/vector.
     */
    @Test
    public void testOnDiskCounterExcludesBLinkPageCache() throws Exception {
        Path tmpDir = tmpFolder.newFolder("counter-no-blink").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int numVectors = 400;
        int dim = 16;
        // INT keys (Bytes.from_int): 4 bytes per PK, so raw array cost is:
        //   pkData:    4 B/vector
        //   pkOffsets: 4 B/vector
        //   pkLengths: 4 B/vector
        //   ────────────────────
        //   total:    12 B/vector  →  4800 B for 400 vectors
        //
        // BLink contribution (ENTRY_CONSTANT_SIZE + Bytes + Long) ≈ 128 B/vector.
        // If BLink were still included the counter would be ≈ 140 B/vector = 56 000 B.
        long pkArraysLowerBound = (long) numVectors * Integer.BYTES * 3; // 4800 B
        // Upper bound: 48 B/vector leaves generous room for array header overhead
        // while remaining clearly below the BLink contribution of 128 B/vector.
        long blinkFreeUpperBound = (long) numVectors * 48;               // 19 200 B

        Random rng = new Random(592);

        try (PersistentVectorStore store = createStore(tmpDir, dsm, mm, "uuid-no-blink")) {
            store.start();

            for (int i = 0; i < numVectors; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();

            assertTrue("checkpoint must have produced at least one segment",
                    store.getSegmentCount() > 0);

            long onDisk = store.getOnDiskSegmentsEstimatedMemoryBytes();

            assertTrue("on-disk counter must be >= pkArray lower bound ("
                    + pkArraysLowerBound + " B), got " + onDisk,
                    onDisk >= pkArraysLowerBound);
            assertTrue("on-disk counter must be < BLink-free upper bound of "
                    + blinkFreeUpperBound + " B (≈48 B/vector); got " + onDisk
                    + " B — if this fails, BLink.getUsedMemory() was re-introduced"
                    + " into VectorSegment.estimatedInMemoryBytes() (issue #592)",
                    onDisk <= blinkFreeUpperBound);

            assertCounterMatchesIteration(store, "after checkpoint (BLink-free)");
        }
    }
}
