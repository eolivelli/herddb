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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
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
 * Change&nbsp;1: {@code atomicSwapCompactionResult}'s validate/build/persist
 * (Stages&nbsp;1 + 2) now run lock-free under {@code checkpointLock} alone.
 *
 * <p>The pre-fix code held {@code stateLock.writeLock()} across the entire
 * swap, so a concurrent {@code removeVector(X)} was BLOCKED on
 * {@code readLock} while Stages 1 + 2 ran; once the writeLock released
 * after Stage 3's publish, {@code removeVector} would iterate the NEW
 * {@code this.segments} (with the merged segment in place) and apply the
 * delete to merged directly. <strong>No data loss.</strong>
 *
 * <p>The post-fix code's longer lock-free window (Stages 1 + 2) lets
 * {@code removeVector(X)} run while {@code this.segments} still references
 * the input segments. The delete lands on an input that's about to be
 * discarded. {@code lateDeletes.forEach} (in {@code runCompactionCycle},
 * before atomicSwap) has already finished, so {@code X} is never applied
 * to the merged output.
 *
 * <p>The fix is a Stage&nbsp;3 re-apply of {@code pendingCompactionDeletes}
 * to {@code mergedOutput} under writeLock — this test pins down that
 * invariant by blocking Stage 2 (the persist) on a hook, racing
 * {@code removeVector} on PKs that live in inputs, then asserting the
 * deleted PKs are absent from the merged output once the cycle completes.
 */
public class PersistentVectorStoreCompactionDeletePkRaceTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore("testidx", "testtable",
                "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        // Aggressive compaction: any 2+ segments must merge.
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                Integer.MAX_VALUE, 0);
        return store;
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void concurrentRemoveDuringCompactionStage12IsAppliedToMerged() throws Exception {
        Path tmpDir = tmpFolder.newFolder("compaction-race").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Build several sealed segments. Each batch + checkpoint
            // produces one on-disk segment containing PKs in
            // [c * 100, c * 100 + 100).
            int batches = 4;
            int batchSize = 100;
            int totalPks = batches * batchSize;
            for (int c = 0; c < batches; c++) {
                for (int i = 0; i < batchSize; i++) {
                    store.addVector(Bytes.from_int(c * batchSize + i),
                            randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must produce >= 2 segments to make compaction"
                    + " meaningful, got " + segmentsBefore,
                    segmentsBefore >= 2);

            // Plant the post-build hook: compaction will park BETWEEN Stage 1
            // (validate + build + queue retention) and Stage 2 (persist).
            // While parked: this.segments still references the input segments,
            // and lateDeletes.forEach has already run on merged. A
            // removeVector(X) here:
            //   - finds X in an input segment (still in segments) and applies
            //     deletePk to that input — but the input is about to be
            //     discarded;
            //   - adds X to pendingCompactionDeletes.
            // Without the Stage-3 re-apply (the BLOCK-2 fix), X would survive
            // in merged.
            CountDownLatch hookEntered = new CountDownLatch(1);
            CountDownLatch hookRelease = new CountDownLatch(1);
            store.setAtomicSwapPostBuildHookForTest(() -> {
                hookEntered.countDown();
                try {
                    hookRelease.await(120, TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });

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

            assertTrue("compaction did not reach the post-build hook within 30s",
                    hookEntered.await(30, TimeUnit.SECONDS));

            // Hook is parked. Race a few removeVector calls. Use deterministic
            // slices spread across all input segments to maximise the chance
            // that the surviving-on-merged regression appears for at least
            // one PK.
            List<Bytes> toDelete = new ArrayList<>();
            for (int c = 0; c < batches; c++) {
                // Delete 10 PKs from the head of each batch's range.
                for (int i = 0; i < 10; i++) {
                    toDelete.add(Bytes.from_int(c * batchSize + i));
                }
            }
            Set<Bytes> deletedSnapshot = new HashSet<>(toDelete);

            int removeThreads = 2;
            int searchThreads = 2;
            int searchIterations = 50;
            CountDownLatch racersStart = new CountDownLatch(1);
            CountDownLatch racersDone = new CountDownLatch(searchThreads + removeThreads);
            AtomicReference<Throwable> workerError = new AtomicReference<>();
            AtomicInteger searchesIssued = new AtomicInteger();

            // Search workers: hammer the store; nothing should crash.
            // Catch Throwable so AssertionError surfaces via workerError.
            for (int s = 0; s < searchThreads; s++) {
                final long seed = 1000 + s;
                Thread t = new Thread(() -> {
                    try {
                        racersStart.await();
                        Random r = new Random(seed);
                        for (int i = 0; i < searchIterations; i++) {
                            List<Map.Entry<Bytes, Float>> results =
                                    store.search(randomVector(r, dim), 10);
                            assertNotNull("search returned null", results);
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

            // Remove workers: each takes half of toDelete.
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

            racersStart.countDown();
            assertTrue("racers did not finish within 60s",
                    racersDone.await(60, TimeUnit.SECONDS));

            // Release the hook; compaction proceeds through Stages 2 + 3.
            hookRelease.countDown();
            compactor.join(60_000);
            if (compactionError.get() != null) {
                throw new AssertionError("compaction failed", compactionError.get());
            }
            store.setAtomicSwapPostBuildHookForTest(null);

            if (workerError.get() != null) {
                throw new AssertionError("worker surfaced an exception/assertion failure",
                        workerError.get());
            }
            assertTrue("at least some searches completed during the race",
                    searchesIssued.get() > 0);

            // Compaction succeeded — the merged output is now in segments.
            int segmentsAfter = store.getSegmentCount();
            assertTrue("compaction must have reduced segment count: before="
                    + segmentsBefore + " after=" + segmentsAfter,
                    segmentsAfter < segmentsBefore);

            // Headline assertion (BLOCK-2 regression check): every PK in
            // toDelete must be ABSENT from search results, including PKs
            // that the racing removeVector applied to an input segment
            // (now discarded) but which were NOT in lateDeletes when the
            // pre-atomicSwap forEach ran. Without the Stage-3 re-apply,
            // those PKs survive in merged.
            int probeTopK = totalPks + 100;
            for (int q = 0; q < 20; q++) {
                List<Map.Entry<Bytes, Float>> results =
                        store.search(randomVector(rng, dim), probeTopK);
                for (Map.Entry<Bytes, Float> e : results) {
                    assertTrue("deleted PK " + e.getKey()
                                    + " resurfaced in search results"
                                    + " (BLOCK-2 regression: a delete that arrived"
                                    + " during the lock-free Stages 1+2 of"
                                    + " atomicSwap was not re-applied to merged"
                                    + " in Stage 3)",
                            !deletedSnapshot.contains(e.getKey()));
                }
            }

            // Final size accounting matches.
            long expectedSize = (long) totalPks - toDelete.size();
            assertTrue("store size after race must equal initial - deletions"
                            + " (got " + store.size() + ", expected " + expectedSize + ")",
                    store.size() == expectedSize);
        }
    }
}
