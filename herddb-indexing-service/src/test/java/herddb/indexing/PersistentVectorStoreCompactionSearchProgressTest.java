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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Headline regression test for issue #462: ANN searches must NOT be blocked
 * while a compaction's slow {@code persistIndexStatusMultiSegment} call is
 * in flight.
 *
 * <p>The pre-fix code held {@code stateLock.writeLock()} across that persist,
 * which at 18&nbsp;K segments meant 1&ndash;4 seconds of search starvation
 * per compaction cycle. After the three-stage refactor the writeLock window
 * shrinks to microseconds; this test wraps the in-memory DSM so the
 * {@code indexCheckpoint} call sleeps for &ge;&nbsp;2 seconds, drives a
 * compaction in the background, and asserts that searches issued during the
 * sleep complete with low latency.
 */
public class PersistentVectorStoreCompactionSearchProgressTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Wraps {@link MemoryDataStorageManager} so that the FIRST
     * {@code indexCheckpoint} call (the one inside Stage 2 of
     * {@code atomicSwapCompactionResult}) blocks until a release latch is
     * counted down. Subsequent calls run normally.
     */
    private static final class SlowFirstIndexCheckpoint extends ForwardingDataStorageManager {

        private final CountDownLatch persistEntered = new CountDownLatch(1);
        private final CountDownLatch persistRelease;
        private final AtomicInteger callCount = new AtomicInteger();
        private final AtomicLong persistDurationNanos = new AtomicLong();

        SlowFirstIndexCheckpoint(DataStorageManager delegate, CountDownLatch release) {
            super(delegate);
            this.persistRelease = release;
        }

        @Override
        public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
                IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
            int n = callCount.incrementAndGet();
            // Only the FIRST persist blocks — let setup checkpoints through.
            // The compaction-cycle test triggers compaction explicitly after
            // setup, so its Stage-2 persist is the one that hits the latch.
            // Use callCount's value AT THIS INVOCATION to identify the first
            // call deterministically.
            if (n == firstBlockingCall) {
                long start = System.nanoTime();
                persistEntered.countDown();
                try {
                    persistRelease.await(120, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new DataStorageManagerException("interrupted while sleeping in test", ie);
                }
                persistDurationNanos.set(System.nanoTime() - start);
            }
            return delegate().indexCheckpoint(tableSpace, uuid, indexStatus, pin);
        }

        // Set by the test to the call number at which we want to block.
        // Must be set before the compaction is triggered.
        volatile int firstBlockingCall = Integer.MAX_VALUE;
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void searchProgressDuringSlowCompactionPersist() throws Exception {
        Path tmpDir = tmpFolder.newFolder("compaction-search-progress").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        MemoryDataStorageManager underlying = new MemoryDataStorageManager();
        CountDownLatch release = new CountDownLatch(1);
        SlowFirstIndexCheckpoint dsm = new SlowFirstIndexCheckpoint(underlying, release);
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore("testidx",
                "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            // Aggressive compaction: any 2+ segments must merge.
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            store.start();

            // Setup: build several sealed segments without blocking the DSM.
            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 200; i++) {
                    store.addVector(Bytes.from_int(c * 100_000 + i),
                            randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int beforeMerge = store.getSegmentCount();
            assertTrue("test setup must produce several segments, got " + beforeMerge,
                    beforeMerge >= 2);

            // From this point on, the NEXT indexCheckpoint call (Stage 2 of
            // atomicSwapCompactionResult) blocks on the release latch.
            int blockingCallNumber = dsm.callCount.get() + 1;
            dsm.firstBlockingCall = blockingCallNumber;

            // Drive the compaction on a background thread.
            AtomicReference<Throwable> compactionError = new AtomicReference<>();
            Thread compactor = new Thread(() -> {
                try {
                    store.runCompactionCycle();
                } catch (Throwable t) {
                    compactionError.compareAndSet(null, t);
                }
            }, "compactor-under-test");
            compactor.start();

            // Wait until the compaction has entered the persist (Stage 2).
            assertTrue("compaction did not reach the slow persist within 30s",
                    dsm.persistEntered.await(30, TimeUnit.SECONDS));

            // The compaction is now blocked inside indexCheckpoint. The pre-fix
            // code would have been holding stateLock.writeLock() at this point,
            // starving every search. Issue searches and assert (a) they don't
            // hang and (b) several of them complete WHILE the persist is still
            // blocked — i.e., NOT just after we release the latch.
            float[] query = randomVector(rng, dim);
            int searchesCompletedDuringPersist = 0;
            long maxSingleSearchNanos = 0;
            long progressDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < progressDeadline) {
                // The compaction must still be parked. If not, we have not
                // proven anything.
                if (release.getCount() == 0) {
                    break;
                }
                long t0 = System.nanoTime();
                List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
                long elapsed = System.nanoTime() - t0;
                maxSingleSearchNanos = Math.max(maxSingleSearchNanos, elapsed);
                assertFalse("search returned a null result list", results == null);
                searchesCompletedDuringPersist++;
                // If we have proven the point with a healthy margin, stop.
                if (searchesCompletedDuringPersist >= 5
                        && System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(50)) {
                    // continue collecting up to a small budget to confirm
                    // searches stay fast
                }
            }

            // Release the persist; compaction completes.
            release.countDown();
            compactor.join(60_000);
            if (compactionError.get() != null) {
                throw new AssertionError("compaction failed", compactionError.get());
            }

            // Headline assertion: at least a handful of searches completed
            // while the compaction's persist was still in flight.
            assertTrue("expected >= 5 searches to complete during the slow persist;"
                            + " saw " + searchesCompletedDuringPersist
                            + " (issue #462: writeLock must not be held during the persist)",
                    searchesCompletedDuringPersist >= 5);

            // Latency budget: each search during the persist had to be far
            // shorter than the persist duration itself. 200 ms is a generous
            // bound that still proves the writeLock is not held.
            assertTrue("max search latency during persist (" + (maxSingleSearchNanos / 1_000_000)
                            + " ms) must be far less than the persist duration ("
                            + (dsm.persistDurationNanos.get() / 1_000_000) + " ms)",
                    maxSingleSearchNanos < TimeUnit.MILLISECONDS.toNanos(200));

            // Diagnostic counter sanity: Stage 3's writeLock window is microseconds.
            assertTrue("Stage 3 writeLock window must be tiny (issue #462 SLO);"
                            + " observed " + store.getLastCompactionSwapWriteLockNanos() + " ns",
                    store.getLastCompactionSwapWriteLockNanos() < 200_000_000L);

            // Functional correctness: compaction succeeded.
            int afterMerge = store.getSegmentCount();
            assertTrue("compaction must have reduced segment count: before="
                    + beforeMerge + " after=" + afterMerge,
                    afterMerge < beforeMerge);

            // Search after the compaction returns valid results from the merged segment.
            List<Map.Entry<Bytes, Float>> postCompactResults = store.search(query, 5);
            assertFalse("search after compaction returned empty results",
                    postCompactResults.isEmpty());
        }
    }

    @Test
    public void searchUnblockedAfterPersistFailureRecoversCleanly() throws Exception {
        // Companion test: even if Stage-2 throws, the writeLock should not
        // have been held during it. We don't sleep-block here; we just
        // throw, and verify that searches issued AFTER recovery work.
        Path tmpDir = tmpFolder.newFolder("compaction-search-recovery").toPath();
        int dim = 16;
        Random rng = new Random(11);

        MemoryDataStorageManager underlying = new MemoryDataStorageManager();
        AtomicBoolean throwOnNextIndexCheckpoint = new AtomicBoolean(false);
        ForwardingDataStorageManager dsm = new ForwardingDataStorageManager(underlying) {
            @Override
            public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
                    IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
                if (throwOnNextIndexCheckpoint.compareAndSet(true, false)) {
                    throw new DataStorageManagerException("simulated S3 failure (test)");
                }
                return delegate().indexCheckpoint(tableSpace, uuid, indexStatus, pin);
            }
        };
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore("testidx",
                "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            store.start();
            for (int c = 0; c < 4; c++) {
                for (int i = 0; i < 100; i++) {
                    store.addVector(Bytes.from_int(c * 1000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int before = store.getSegmentCount();
            assertTrue("test setup", before >= 2);

            // Make the next compaction's persist throw.
            throwOnNextIndexCheckpoint.set(true);
            store.runCompactionCycle();

            // Compaction failed — segments unchanged. Searches still work.
            float[] query = randomVector(rng, dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search after failed compaction returned empty",
                    results.isEmpty());
            // The follow-up compaction succeeds.
            store.runCompactionCycle();
            assertTrue("recovery compaction must reduce segments",
                    store.getSegmentCount() < before);
        }
    }
}
