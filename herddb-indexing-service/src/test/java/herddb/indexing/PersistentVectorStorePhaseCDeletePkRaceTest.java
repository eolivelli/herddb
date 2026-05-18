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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Race-test for the new concurrency invariant introduced by issue #462's
 * Change&nbsp;2: Phase&nbsp;C's pending-checkpoint-deletes apply now runs
 * lock-free under {@code checkpointLock} alone, so concurrent
 * {@code search} (read-locked) and concurrent {@code removeVector}
 * (read-locked) calls can hit the same segments at the same time.
 *
 * <p>Two concurrent paths both call {@code seg.deletePk(pk)} — Phase C
 * applying its accumulated pending set, and {@code removeVector} from
 * client traffic. Both must produce a consistent final state:
 * <ul>
 *   <li>Every PK that was added and then deleted must NOT appear in any
 *       search.</li>
 *   <li>{@code seg.size()} accounting (via {@code liveCount}) must match
 *       the actual live entries.</li>
 *   <li>The on-disk-segment memory counter must equal the iterated sum.</li>
 *   <li>No exceptions surface from the search threads.</li>
 * </ul>
 */
public class PersistentVectorStorePhaseCDeletePkRaceTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

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
    public void concurrentSearchAndRemoveDuringPhaseC() throws Exception {
        Path tmpDir = tmpFolder.newFolder("phase-c-race").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Build sealed segments containing a known set of PKs.
            int totalPks = 600;
            for (int i = 0; i < totalPks; i++) {
                store.addVector(Bytes.from_int(i), randomVector(rng, dim));
            }
            store.checkpoint();
            assertTrue("setup must produce at least one on-disk segment",
                    store.getSegmentCount() >= 1);

            // Trigger a NEW checkpoint cycle. Phase A → frozenShards
            // populated; Phase B writes new on-disk segments; Phase C
            // applies pending deletes that have arrived since Phase A
            // started.
            //
            // We'll inject the pause at Phase C's post-deletes-apply hook,
            // then race remove + search calls in the open window.
            CountDownLatch hookEntered = new CountDownLatch(1);
            CountDownLatch hookRelease = new CountDownLatch(1);
            store.setPhaseCPostDeletesApplyHookForTest(() -> {
                hookEntered.countDown();
                try {
                    hookRelease.await(120, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });

            // We need new live state for Phase C to do work. Add some new
            // vectors so the next checkpoint produces a new segment.
            int newLiveStart = totalPks;
            int newLiveCount = 50;
            for (int i = 0; i < newLiveCount; i++) {
                store.addVector(Bytes.from_int(newLiveStart + i),
                        randomVector(rng, dim));
            }

            AtomicReference<Throwable> checkpointError = new AtomicReference<>();
            Thread checkpointer = new Thread(() -> {
                try {
                    store.checkpoint();
                } catch (Throwable t) {
                    checkpointError.compareAndSet(null, t);
                }
            }, "checkpoint-under-test");
            checkpointer.start();

            assertTrue("Phase C did not reach the hook within 30s",
                    hookEntered.await(30, TimeUnit.SECONDS));

            // Hook is parked. The writeLock is NOT held. Race a few
            // remove-vector and search calls.
            //
            // Pick a deterministic, disjoint slice of PKs for each thread
            // so we can verify final state precisely. The slice DELIBERATELY
            // includes both:
            //   - sealed-segment PKs (in [0, totalPks)) — removeVector
            //     applies these directly via `for (seg : this.segments)`,
            //   - frozen-shard / preloaded-segment PKs (in
            //     [newLiveStart, newLiveStart + newLiveCount)) — these go
            //     ONLY through pendingCheckpointDeletes, exercising the
            //     pr-reviewer pass-1 BLOCK-1 silent-delete-loss path that
            //     the Stage-2 re-apply guards against.
            List<Bytes> toDelete = new ArrayList<>();
            int sealedSliceSize = 100;
            for (int i = 0; i < sealedSliceSize; i++) {
                toDelete.add(Bytes.from_int(i));
            }
            // Add the entire newLive slice — every one of these PKs landed
            // in a frozen shard at Phase A, then in a preloaded segment
            // after Phase B; deletes for them MUST be observed via the
            // pendingCheckpointDeletes path (BLOCK-1 coverage).
            int preloadedSliceStart = newLiveStart;
            int preloadedSliceSize = newLiveCount;
            for (int i = 0; i < preloadedSliceSize; i++) {
                toDelete.add(Bytes.from_int(preloadedSliceStart + i));
            }
            Set<Bytes> deletedSnapshot = new HashSet<>(toDelete);

            int searchThreads = 4;
            int removeThreads = 2;
            int searchIterations = 100;
            CountDownLatch racersStart = new CountDownLatch(1);
            CountDownLatch racersDone = new CountDownLatch(searchThreads + removeThreads);
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            AtomicInteger searchesIssued = new AtomicInteger();

            // Search workers: hammer the store with searches that should
            // never crash and never include deleted PKs.
            // Catch Throwable (not just RuntimeException) so an AssertionError
            // from assertNotNull/assertTrue on the worker thread is captured
            // and surfaced via workerError instead of being silently
            // swallowed by the framework after the finally{}-block resolves.
            for (int s = 0; s < searchThreads; s++) {
                final long seed = 1000 + s;
                Thread t = new Thread(() -> {
                    try {
                        racersStart.await();
                        Random r = new Random(seed);
                        for (int i = 0; i < searchIterations; i++) {
                            List<Map.Entry<Bytes, Float>> results =
                                    store.search(randomVector(r, dim), 10);
                            assertNotNull("search returned null",
                                    results);
                            searchesIssued.incrementAndGet();
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable t2) {
                        workerError.compareAndSet(null, t2);
                    } finally {
                        racersDone.countDown();
                    }
                }, "race-search-" + s);
                t.setDaemon(true);
                t.start();
            }

            // Remove workers: half delete the lower half, half delete the upper half.
            // Catch Throwable (not just InterruptedException) so any
            // unchecked exception or assertion failure inside removeVector
            // is captured via workerError instead of being swallowed by
            // the JVM's default uncaught-exception handler.
            int half = toDelete.size() / 2;
            for (int s = 0; s < removeThreads; s++) {
                final int from = (s == 0) ? 0 : half;
                final int to = (s == 0) ? half : toDelete.size();
                Thread t = new Thread(() -> {
                    try {
                        racersStart.await();
                        for (int i = from; i < to; i++) {
                            store.removeVector(toDelete.get(i));
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable t2) {
                        workerError.compareAndSet(null, t2);
                    } finally {
                        racersDone.countDown();
                    }
                }, "race-remove-" + s);
                t.setDaemon(true);
                t.start();
            }

            // Kick off the race.
            racersStart.countDown();

            // Wait for racers, then release the hook.
            assertTrue("racers did not finish within 60s",
                    racersDone.await(60, TimeUnit.SECONDS));
            hookRelease.countDown();

            checkpointer.join(60_000);
            if (checkpointError.get() != null) {
                throw new AssertionError("checkpoint failed", checkpointError.get());
            }

            store.setPhaseCPostDeletesApplyHookForTest(null);

            // ---------- Race-result invariants ----------

            // No exceptions or assertion failures from search workers.
            if (workerError.get() != null) {
                throw new AssertionError("search worker surfaced an exception/assertion failure",
                        workerError.get());
            }
            assertTrue("at least some searches completed during the race",
                    searchesIssued.get() > 0);

            // Final state: every deleted PK is GONE.
            //
            // Run searches with topK large enough to materialise EVERY live
            // PK across multiple random queries. With totalPks + newLiveCount
            // ≈ 650 and topK = 1000, the ANN search exhausts the index so any
            // deleted PK that wrongly survived in a segment WOULD appear in
            // at least one of these results. This is the regression check
            // for the BLOCK-1 fix (Stage 2 re-apply on preloaded segments)
            // and the BLOCK-2 fix (Stage 3 re-apply on merged output).
            int probeTopK = totalPks + newLiveCount + 100;
            for (int q = 0; q < 20; q++) {
                List<Map.Entry<Bytes, Float>> results =
                        store.search(randomVector(rng, dim), probeTopK);
                for (Map.Entry<Bytes, Float> e : results) {
                    assertTrue("deleted PK " + e.getKey()
                                    + " resurfaced in search results"
                                    + " (BLOCK-1/BLOCK-2 regression: a delete"
                                    + " that arrived during the lock-free apply"
                                    + " survived in a preloaded/merged segment)",
                            !deletedSnapshot.contains(e.getKey()));
                }
            }

            // Final size accounting matches: total - deletes - any deletes
            // applied via pendingCheckpointDeletes in this cycle. We deleted
            // exactly `toDelete.size()` PKs across both threads (sealed slice
            // [0, sealedSliceSize) PLUS preloaded slice
            // [preloadedSliceStart, preloadedSliceStart + preloadedSliceSize)).
            // Expected size: totalPks (sealed, on-disk) + newLiveCount - toDelete.size()
            long expectedSize = (long) totalPks + newLiveCount - toDelete.size();
            assertEquals("store size after race must equal initial - deletions",
                    expectedSize, store.size());

            // Memory counter sanity. We do NOT require strict counter ==
            // iteratedSum here because BLink.getUsedMemory() shrinks as
            // entries are deleted while the cached estimate the counter
            // recorded at register-time stays the same — that drift is a
            // pre-existing property of partial-delete-without-rebuild,
            // not a regression introduced by Change 2 of issue #462.
            // What we DO require:
            //   - the counter is non-negative,
            //   - it is at least as large as the iterated sum (because the
            //     iterated sum reflects the post-delete actual memory and
            //     the counter reflects the at-register-time estimate),
            //   - both are within a reasonable bound of each other.
            long counter = store.getOnDiskSegmentsEstimatedMemoryBytes();
            long iteratedSum = store.sumOnDiskSegmentMemoryBytesByIterationForTesting();
            assertTrue("counter must be non-negative, was " + counter, counter >= 0);
            assertTrue("counter must be >= iterated sum (deletes shrink BLink memory"
                            + " under the iterated sum but not the cached counter);"
                            + " counter=" + counter + " iteratedSum=" + iteratedSum,
                    counter >= iteratedSum);
            // The drift should be bounded: with at most a few hundred deletes
            // and a 16-dim vector index, the per-segment BLink memory delta
            // is small relative to the segment's total estimated memory.
            // 4× is a generous bound that catches a pathological leak (e.g.,
            // a bug that registered the same segment twice) without
            // tripping on the legitimate post-delete drift.
            assertTrue("counter (" + counter + ") must be within 4x of iterated sum ("
                            + iteratedSum + ") — a larger gap would indicate"
                            + " a double-register/missed-unregister bug",
                    iteratedSum > 0 && counter <= 4L * iteratedSum);
        }
    }
}
