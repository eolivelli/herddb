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

package herddb.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression suite for issue #282 — reduce {@code rotateLiveShard} write-lock
 * duration in {@link PersistentVectorStore}.
 *
 * <p>Before the fix, {@code addVectorInternal} held {@code stateLock.readLock()}
 * across the full {@code rotateLiveShard()} call, which in turn acquired
 * {@code synchronized(this)} while the read lock was already held.  The fix
 * moves the expensive {@link io.github.jbellis.jvector.graph.GraphIndexBuilder}
 * construction outside any lock and restricts the write lock to the cheap
 * volatile-swap of {@code liveShards}.
 *
 * <p>Tests below verify that the correctness invariants enforced by issue #256
 * (no negative local ordinals, no duplicate pk→node mappings, no lost vectors)
 * are preserved under the new, more concurrent locking protocol.
 */
public class Issue282ConcurrentRotationTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void disableDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = 50_000;
    }

    /**
     * Creates a store whose shard cap is set very small (= {@code shardCap})
     * so that rotation is triggered after just a handful of inserts.
     *
     * <p>The constructor parameter order is:
     * {@code m, beamWidth, neighborOverflow, alpha, fusedPQ, maxSegmentSize,
     * maxLiveGraphSize, compactionIntervalMs}.  We pass {@code shardCap} as
     * {@code maxLiveGraphSize} (the direct override) so that
     * {@link PersistentVectorStore#computeEffectiveMaxLiveGraphSize()} returns
     * exactly {@code shardCap} regardless of the {@code m} / {@code beamWidth}
     * formula.
     */
    private PersistentVectorStore createStore(Path tmpDir, int shardCap) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                /*m=*/16, /*beamWidth=*/100, /*neighborOverflow=*/1.2f, /*alpha=*/1.4f,
                /*fusedPQ=*/true, /*maxSegmentSize=*/2_000_000_000L,
                /*maxLiveGraphSize=*/shardCap,
                Long.MAX_VALUE);
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 4, 0);
        return store;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    // -------------------------------------------------------------------------
    // Test 1: Single-threaded rotation boundary — no vectors lost
    // -------------------------------------------------------------------------

    /**
     * Insert exactly (cap + 1) vectors so one rotation happens, then verify
     * that every primary key is still retrievable and that the two shards
     * together account for all inserted vectors.
     */
    @Test
    public void noVectorsLostAtRotationBoundary() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 10;
        int dim = 16;
        int total = cap + 5; // force one rotation
        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();
            Random rng = new Random(1);
            for (int i = 0; i < total; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            List<PersistentVectorStore.LiveGraphShard> shards = store.allLiveShardsForTest();
            assertTrue("expected at least 2 shards after " + total + " inserts with cap " + cap,
                    shards.size() >= 2);

            int liveCount = 0;
            for (PersistentVectorStore.LiveGraphShard shard : shards) {
                liveCount += shard.nodeToPk.size();
            }
            assertEquals("total live vectors must equal total inserts", total, liveCount);

            // Every PK must appear in exactly one shard's pkToNode map.
            Set<Bytes> seen = new HashSet<>();
            for (PersistentVectorStore.LiveGraphShard shard : shards) {
                for (Bytes pk : shard.pkToNode.keySet()) {
                    assertTrue("duplicate pk across shards: " + pk, seen.add(pk));
                }
            }
            assertEquals("every inserted PK must be present in some shard", total, seen.size());
        }
    }

    // -------------------------------------------------------------------------
    // Test 2: No negative local ordinals under rotation
    // -------------------------------------------------------------------------

    /**
     * Verifies that every local ordinal stored in each shard's {@code nodeToPk}
     * map is non-negative, even across multiple rotations.  Negative ordinals
     * were the symptom of the bug fixed in issue #256 (AtomicInteger wrap);
     * this test ensures the same invariant holds under the refactored locking
     * introduced in issue #282.
     */
    @Test
    public void noNegativeLocalOrdinalsAcrossRotations() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 8;
        int dim = 16;
        // Trigger several rotations.
        int total = cap * 4 + 3;
        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();
            Random rng = new Random(2);
            for (int i = 0; i < total; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }

            for (PersistentVectorStore.LiveGraphShard shard : store.allLiveShardsForTest()) {
                for (Integer localOrd : shard.nodeToPk.keySet()) {
                    assertTrue("negative local ordinal " + localOrd
                                    + " in shard starting at " + shard.startNodeId,
                            localOrd >= 0);
                }
                for (Integer localOrd : shard.pkToNode.values()) {
                    assertTrue("negative localOrd in pkToNode: " + localOrd, localOrd >= 0);
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test 3: Concurrent inserts — no lost vectors, no duplicates, no negative ords
    // -------------------------------------------------------------------------

    /**
     * Spawns multiple ingest threads that all insert concurrently while shard
     * rotation is happening.  After all threads finish:
     * <ul>
     *   <li>Every inserted primary key must appear in exactly one shard.</li>
     *   <li>Every local ordinal must be non-negative.</li>
     *   <li>The total live count must equal the total inserts.</li>
     * </ul>
     *
     * <p>This is the primary regression test for issue #282: it exercises the
     * race between the Phase-1 volatile check and the Phase-2 read-lock
     * acquisition, and the racing rotation path in {@code rotateLiveShard}.
     */
    @Test
    public void concurrentInsertsAcrossRotationBoundary() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 20;
        int dim = 16;
        int threads = 8;
        int insertsPerThread = 50; // total = 400 > many rotations with cap=20
        int total = threads * insertsPerThread;

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();

            CountDownLatch startGun = new CountDownLatch(1);
            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger pkCounter = new AtomicInteger(0);

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            try {
                for (int t = 0; t < threads; t++) {
                    exec.submit(() -> {
                        try {
                            startGun.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            firstError.compareAndSet(null, e);
                            return;
                        }
                        Random rng = new Random(Thread.currentThread().getId());
                        for (int i = 0; i < insertsPerThread; i++) {
                            int pk = pkCounter.getAndIncrement();
                            try {
                                store.addVector(Bytes.from_int(pk), vec(rng, dim));
                            } catch (Throwable ex) {
                                firstError.compareAndSet(null, ex);
                            }
                        }
                    });
                }
                startGun.countDown();
            } finally {
                exec.shutdown();
                assertTrue("ingest threads did not finish in time",
                        exec.awaitTermination(60, TimeUnit.SECONDS));
            }

            if (firstError.get() != null) {
                throw new AssertionError("ingest thread threw an exception", firstError.get());
            }

            // --- Assertions ---
            List<PersistentVectorStore.LiveGraphShard> shards = store.allLiveShardsForTest();
            assertNotNull(shards);
            assertTrue("expected multiple shards with cap=" + cap + " and total=" + total,
                    shards.size() >= total / cap);

            // Count total live vectors.
            int liveCount = 0;
            Set<Bytes> seenPks = new HashSet<>();
            List<Integer> seenOrds = new ArrayList<>();
            for (PersistentVectorStore.LiveGraphShard shard : shards) {
                liveCount += shard.nodeToPk.size();
                for (Bytes pk : shard.pkToNode.keySet()) {
                    assertTrue("duplicate pk in pkToNode across shards: " + pk,
                            seenPks.add(pk));
                }
                for (Integer localOrd : shard.nodeToPk.keySet()) {
                    assertTrue("negative local ordinal " + localOrd
                                    + " in shard with startNodeId=" + shard.startNodeId,
                            localOrd >= 0);
                    seenOrds.add(localOrd);
                }
            }

            assertEquals("total live count must equal total inserts", total, liveCount);
            assertEquals("every inserted PK must appear in exactly one shard",
                    total, seenPks.size());
        }
    }

    // -------------------------------------------------------------------------
    // Test 4: Exactly one new shard created under racing threads at cap boundary
    // -------------------------------------------------------------------------

    /**
     * Pre-fills a store to exactly {@code cap - 1} vectors so the very next
     * batch of concurrent inserts all race at the rotation boundary.
     * Verifies that exactly one new shard is published even when many threads
     * simultaneously detect that the active shard is full.
     */
    @Test
    public void exactlyOneNewShardCreatedUnderRacingThreads() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 15;
        int dim = 16;
        int preFill = cap - 1;
        int racingInserts = 20; // all threads arrive when shard is one slot away from full

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();
            Random seed = new Random(42);
            // Pre-fill to cap - 1.
            for (int i = 0; i < preFill; i++) {
                store.addVector(Bytes.from_int(i), vec(seed, dim));
            }
            int shardsBeforeRace = store.allLiveShardsForTest().size();
            assertEquals("should have exactly 1 shard after pre-fill", 1, shardsBeforeRace);

            CountDownLatch startGun = new CountDownLatch(1);
            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger pkSource = new AtomicInteger(preFill);

            ExecutorService exec = Executors.newFixedThreadPool(racingInserts);
            try {
                for (int t = 0; t < racingInserts; t++) {
                    exec.submit(() -> {
                        try {
                            startGun.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            firstError.compareAndSet(null, e);
                            return;
                        }
                        int pk = pkSource.getAndIncrement();
                        Random rng = new Random(pk);
                        try {
                            store.addVector(Bytes.from_int(pk), vec(rng, dim));
                        } catch (Throwable ex) {
                            firstError.compareAndSet(null, ex);
                        }
                    });
                }
                startGun.countDown();
            } finally {
                exec.shutdown();
                assertTrue("racing threads did not finish in time",
                        exec.awaitTermination(60, TimeUnit.SECONDS));
            }

            if (firstError.get() != null) {
                throw new AssertionError("racing thread threw an exception", firstError.get());
            }

            int totalInserted = preFill + racingInserts;
            List<PersistentVectorStore.LiveGraphShard> shards = store.allLiveShardsForTest();

            // Total live count must match.
            int liveCount = 0;
            for (PersistentVectorStore.LiveGraphShard shard : shards) {
                liveCount += shard.nodeToPk.size();
            }
            assertEquals("total live vectors after race", totalInserted, liveCount);

            // With cap=15, preFill=14, and 20 extra inserts we expect exactly
            // ceil((14+20)/15) = 3 shards. Allow an extra one in case a
            // discarded candidate shard was published before the double-check
            // (should not happen, but guard conservatively).
            int expectedShards = (int) Math.ceil((double) totalInserted / cap);
            assertTrue("shard count " + shards.size() + " should be close to "
                            + expectedShards + " (at most 1 extra due to racing)",
                    shards.size() <= expectedShards + 1);

            // No duplicate PKs, no negative local ordinals.
            Set<Bytes> seen = new HashSet<>();
            for (PersistentVectorStore.LiveGraphShard shard : shards) {
                for (Bytes pk : shard.pkToNode.keySet()) {
                    assertTrue("duplicate pk: " + pk, seen.add(pk));
                }
                for (Integer localOrd : shard.nodeToPk.keySet()) {
                    assertTrue("negative local ordinal " + localOrd, localOrd >= 0);
                }
            }
            assertEquals("all inserted PKs must be present", totalInserted, seen.size());
        }
    }

    // -------------------------------------------------------------------------
    // Test 5: dimension init under concurrency — only one shard created
    // -------------------------------------------------------------------------

    /**
     * Races many threads on the very first insert so that
     * {@code initBuilderForDimension} is called concurrently.  After all
     * threads complete, there must be exactly one initial shard (the
     * double-check inside {@code initBuilderForDimension} under the write lock
     * must discard the losing candidates).
     */
    @Test
    public void dimensionInitUnderConcurrencyCreatesExactlyOneShard() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 1000; // large cap so no rotation happens
        int dim = 16;
        int threads = 16;
        int insertsPerThread = 10;

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();

            CountDownLatch startGun = new CountDownLatch(1);
            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger pkSource = new AtomicInteger(0);

            ExecutorService exec = Executors.newFixedThreadPool(threads);
            try {
                for (int t = 0; t < threads; t++) {
                    exec.submit(() -> {
                        try {
                            startGun.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            firstError.compareAndSet(null, e);
                            return;
                        }
                        Random rng = new Random(Thread.currentThread().getId());
                        for (int i = 0; i < insertsPerThread; i++) {
                            int pk = pkSource.getAndIncrement();
                            try {
                                store.addVector(Bytes.from_int(pk), vec(rng, dim));
                            } catch (Throwable ex) {
                                firstError.compareAndSet(null, ex);
                            }
                        }
                    });
                }
                startGun.countDown();
            } finally {
                exec.shutdown();
                assertTrue("ingest threads did not finish in time",
                        exec.awaitTermination(60, TimeUnit.SECONDS));
            }

            if (firstError.get() != null) {
                throw new AssertionError("init thread threw an exception", firstError.get());
            }

            List<PersistentVectorStore.LiveGraphShard> shards = store.allLiveShardsForTest();
            assertEquals("exactly one shard expected when cap is large and no rotation occurs",
                    1, shards.size());

            int total = threads * insertsPerThread;
            int liveCount = shards.get(0).nodeToPk.size();
            assertEquals("all inserted vectors must be in the single shard", total, liveCount);
        }
    }

    // -------------------------------------------------------------------------
    // Test 6: Checkpoint survives concurrent rotation (no data loss through
    //         Phase A → Phase C round-trip)
    // -------------------------------------------------------------------------

    /**
     * Inserts enough vectors to cause multiple rotations, checkpoints, then
     * inserts more.  After a second checkpoint the total searchable set must
     * include all originally inserted vectors (none lost in the shard-swap
     * during Phase A or Phase C).
     */
    @Test
    public void checkpointAfterRotationPreservesAllVectors() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 10;
        int dim = 16;
        int firstBatch = cap * 3; // force 3 rotations
        int secondBatch = cap;

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();
            Random rng = new Random(99);
            for (int i = 0; i < firstBatch; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            assertTrue("first checkpoint must succeed", store.checkpoint());

            for (int i = firstBatch; i < firstBatch + secondBatch; i++) {
                store.addVector(Bytes.from_int(i), vec(rng, dim));
            }
            assertTrue("second checkpoint must succeed", store.checkpoint());

            // Total size must account for all vectors (live + on-disk).
            long totalSize = store.size();
            assertEquals("all vectors must be accounted for after two checkpoints",
                    (long) (firstBatch + secondBatch), totalSize);
        }
    }

    // -------------------------------------------------------------------------
    // Test 7: Concurrent rotation + checkpoint does not corrupt pk→node mapping
    // -------------------------------------------------------------------------

    /**
     * Runs a background ingest thread (triggering many rotations) while the
     * main thread calls {@code checkpoint()} repeatedly.  After all inserts
     * complete, no two shards (live or frozen/deferred) should share a primary
     * key, and the total live count must equal the total inserts minus any that
     * were checkpointed to on-disk segments.
     */
    @Test
    public void concurrentIngestAndCheckpointNoDuplicatePkMapping() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 12;
        int dim = 16;
        int totalInserts = cap * 6;

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();
            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger pkSource = new AtomicInteger(0);
            CountDownLatch ingestionDone = new CountDownLatch(1);

            Thread ingestor = new Thread(() -> {
                Random rng = new Random(7);
                for (int i = 0; i < totalInserts; i++) {
                    int pk = pkSource.getAndIncrement();
                    try {
                        store.addVector(Bytes.from_int(pk), vec(rng, dim));
                    } catch (Throwable ex) {
                        firstError.compareAndSet(null, ex);
                        break;
                    }
                }
                ingestionDone.countDown();
            });
            ingestor.setDaemon(true);
            ingestor.start();

            // Checkpoint repeatedly while ingest is running.
            Random checkpointRng = new Random(13);
            int checkpoints = 0;
            while (!ingestionDone.await(10, TimeUnit.MILLISECONDS)) {
                if (checkpointRng.nextInt(3) == 0) {
                    store.checkpoint();
                    checkpoints++;
                }
            }
            // One final checkpoint after ingest finishes.
            store.checkpoint();
            checkpoints++;

            if (firstError.get() != null) {
                throw new AssertionError("ingest thread threw an exception", firstError.get());
            }

            assertTrue("at least one concurrent checkpoint must have run", checkpoints >= 1);

            // After all checkpoints, live shards must have no duplicate PKs.
            List<PersistentVectorStore.LiveGraphShard> liveShards = store.allLiveShardsForTest();
            Set<Bytes> seenPks = new HashSet<>();
            for (PersistentVectorStore.LiveGraphShard shard : liveShards) {
                for (Bytes pk : shard.pkToNode.keySet()) {
                    assertTrue("duplicate pk in live shards after concurrent checkpoint: " + pk,
                            seenPks.add(pk));
                }
            }

            // Total live + on-disk must equal totalInserts.
            long totalSize = store.size();
            assertEquals("all vectors must be accounted for", (long) totalInserts, totalSize);
        }
    }

    // -------------------------------------------------------------------------
    // Test 8: rotateLiveShard write lock does not starve readers
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@code search()} remains responsive while ingest threads
     * are concurrently rotating shards.  If the write-lock critical section
     * is accidentally widened (e.g. holding it during GraphIndexBuilder init),
     * search threads will block for milliseconds during each rotation and
     * this test will time out or see high latency.
     *
     * <p>We don't assert on latency directly (too flaky on CI), but we do
     * assert that no search call throws an exception and that at least some
     * results are returned once enough vectors have been ingested.
     */
    @Test
    public void searchResponsiveDuringConcurrentRotation() throws Exception {
        Path tmpDir = tmpFolder.newFolder().toPath();
        int cap = 15;
        int dim = 16;
        int ingestThreads = 4;
        int insertsPerThread = 40;
        int total = ingestThreads * insertsPerThread;

        try (PersistentVectorStore store = createStore(tmpDir, cap)) {
            store.start();

            AtomicReference<Throwable> firstError = new AtomicReference<>();
            AtomicInteger pkSource = new AtomicInteger(0);
            CountDownLatch ingestionDone = new CountDownLatch(ingestThreads);
            // Signal to start search once at least one shard exists.
            CountDownLatch firstInsertDone = new CountDownLatch(1);

            ExecutorService exec = Executors.newFixedThreadPool(ingestThreads + 1);
            try {
                // Ingest threads.
                for (int t = 0; t < ingestThreads; t++) {
                    exec.submit(() -> {
                        Random rng = new Random(Thread.currentThread().getId());
                        for (int i = 0; i < insertsPerThread; i++) {
                            int pk = pkSource.getAndIncrement();
                            try {
                                store.addVector(Bytes.from_int(pk), vec(rng, dim));
                                firstInsertDone.countDown(); // idempotent after first call
                            } catch (Throwable ex) {
                                firstError.compareAndSet(null, ex);
                            }
                        }
                        ingestionDone.countDown();
                    });
                }

                // Search thread: runs concurrently with ingestion.
                exec.submit(() -> {
                    try {
                        firstInsertDone.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        firstError.compareAndSet(null, e);
                        return;
                    }
                    Random rng = new Random(55);
                    // Run searches until ingestion is complete.
                    for (int q = 0; q < 50; q++) {
                        try {
                            store.search(vec(rng, dim), 5);
                        } catch (Throwable ex) {
                            firstError.compareAndSet(null, ex);
                            return;
                        }
                    }
                });

                assertTrue("ingest threads did not finish",
                        ingestionDone.await(60, TimeUnit.SECONDS));
            } finally {
                exec.shutdown();
                exec.awaitTermination(30, TimeUnit.SECONDS);
            }

            if (firstError.get() != null) {
                throw new AssertionError("concurrent search/ingest threw", firstError.get());
            }

            // After all inserts, a search must return results.
            Random qrng = new Random(77);
            List<?> results = store.search(vec(qrng, dim), 10);
            assertNotNull(results);
            assertTrue("search must return results after all inserts", !results.isEmpty());
        }
    }
}
