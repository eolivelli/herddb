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
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Headline regression test for Change&nbsp;2 of issue #462: ANN searches
 * must NOT be blocked while Phase&nbsp;C of {@code doCheckpointFusedPQThreePhase}
 * applies pending-checkpoint deletes.
 *
 * <p>The pre-fix code applied {@code pendingCheckpointDeletes.forEach} ×
 * {@code seg.deletePk} <em>under</em> {@code stateLock.writeLock()}. With
 * many deletes accumulated during Phase&nbsp;B and 18&nbsp;K segments to
 * scan per delete, that critical section runs for seconds — long enough to
 * starve concurrent searches. After the two-stage refactor the deletes-apply
 * runs lock-free under {@code checkpointLock} alone; only the brief state
 * swap holds the writeLock.
 *
 * <p>This test plants the {@code phaseCPostDeletesApplyHook} so the test
 * thread can pause Phase&nbsp;C between Stage&nbsp;1 (deletes apply) and
 * Stage&nbsp;2 (writeLock-protected publish) and assert that searches issued
 * during that pause complete with low latency.
 */
public class PersistentVectorStorePhaseCSearchProgressTest {

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
    public void searchProgressDuringSlowPhaseCDeletesApply() throws Exception {
        Path tmpDir = tmpFolder.newFolder("phase-c-search-progress").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Establish on-disk segments via initial checkpoints so a
            // subsequent checkpoint exercises Phase C with non-empty
            // sealed/mergeable lists.
            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < 200; i++) {
                    store.addVector(Bytes.from_int(c * 100_000 + i),
                            randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int segmentsBefore = store.getSegmentCount();
            assertTrue("test setup must have segments, got " + segmentsBefore,
                    segmentsBefore >= 1);

            // Add a few more inserts so the next checkpoint actually has
            // dirty state to flush.
            for (int i = 0; i < 50; i++) {
                store.addVector(Bytes.from_int(999_000 + i), randomVector(rng, dim));
            }

            // Plant the hook: Phase C will park BETWEEN the lock-free
            // deletes-apply (Stage 1) and the writeLock-protected publish
            // (Stage 2). The test simulates the slow deletes-apply at scale.
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

            // Drive the checkpoint on a background thread.
            AtomicReference<Throwable> checkpointError = new AtomicReference<>();
            Thread checkpointer = new Thread(() -> {
                try {
                    store.checkpoint();
                } catch (Throwable t) {
                    checkpointError.compareAndSet(null, t);
                }
            }, "checkpoint-under-test");
            checkpointer.start();

            // Wait until Phase C reached the hook (= deletes-apply done,
            // about to acquire writeLock for Stage 2).
            assertTrue("Phase C did not reach the post-deletes-apply hook within 30s",
                    hookEntered.await(30, TimeUnit.SECONDS));

            // The pre-fix code held the writeLock during the deletes-apply.
            // After the fix, the hook fires WITHOUT the writeLock held —
            // so searches issued from THIS thread complete normally.
            //
            // Timing budgets are deliberately loose to ride out slow CI
            // runners (jvector ANN cold-start can be hundreds of ms before
            // the JIT warms up). The regression assertion is still meaningful
            // because the pre-fix code blocks for >>1 s when there are many
            // pending deletes at scale; here we only need to demonstrate
            // that searches make progress concurrently with a parked Phase C.
            float[] query = randomVector(rng, dim);
            int searchesCompleted = 0;
            long maxSingleSearchNanos = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && searchesCompleted < 5) {
                long t0 = System.nanoTime();
                List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
                long elapsed = System.nanoTime() - t0;
                maxSingleSearchNanos = Math.max(maxSingleSearchNanos, elapsed);
                assertFalse("search returned null", results == null);
                searchesCompleted++;
            }

            // Release the hook; checkpoint completes.
            hookRelease.countDown();
            checkpointer.join(60_000);
            if (checkpointError.get() != null) {
                throw new AssertionError("checkpoint failed", checkpointError.get());
            }

            store.setPhaseCPostDeletesApplyHookForTest(null);

            // Headline assertions: searches ran without blocking on Phase C.
            assertTrue("expected >= 5 searches to complete during the Phase C pause;"
                            + " saw " + searchesCompleted
                            + " (issue #462: writeLock must not be held during deletes apply)",
                    searchesCompleted >= 5);
            // Per-search latency: pre-fix code blocks for >>1 s when there
            // are many pending deletes at scale; we accept up to 1 s here to
            // avoid CI flakes while still proving the writeLock isn't held.
            assertTrue("max search latency during Phase C pause ("
                            + (maxSingleSearchNanos / 1_000_000) + " ms) must be small",
                    maxSingleSearchNanos < TimeUnit.SECONDS.toNanos(1));

            // Diagnostic counter sanity: Stage 2's writeLock window is small.
            // Pre-fix code at scale is multi-second; even at this tiny test
            // scale the post-fix value is microseconds. 1 s is a generous
            // CI-tolerant bound.
            assertTrue("Phase C Stage 2 writeLock window must be small (issue #462 SLO);"
                            + " observed " + store.getLastPhaseCWriteLockNanos() + " ns",
                    store.getLastPhaseCWriteLockNanos() < TimeUnit.SECONDS.toNanos(1));

            // Functional correctness: checkpoint succeeded and produced new on-disk state.
            int segmentsAfter = store.getSegmentCount();
            assertTrue("checkpoint must have produced at least one new segment "
                    + "(before=" + segmentsBefore + " after=" + segmentsAfter + ")",
                    segmentsAfter > segmentsBefore);

            // Search after the checkpoint returns valid results.
            List<Map.Entry<Bytes, Float>> postResults = store.search(query, 5);
            assertFalse("search after checkpoint returned empty",
                    postResults.isEmpty());
        }
    }
}
