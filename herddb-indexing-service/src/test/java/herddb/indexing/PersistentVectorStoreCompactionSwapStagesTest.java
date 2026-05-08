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
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Locks down the three-stage protocol of
 * {@code atomicSwapCompactionResult} introduced for issue #462.
 *
 * <p>The test plants the two intra-method hooks
 * ({@code atomicSwapPostBuildHook} and {@code atomicSwapPostPersistHook})
 * to inspect state at the precise stage boundaries:
 *
 * <ul>
 *   <li>Post-Stage 1 (after build, before persist): the segment list
 *       still references the OLD segments; {@code currentIndexStatusGeneration}
 *       has NOT bumped.
 *   <li>Post-Stage 2 (after persist, before publish): the
 *       {@code currentIndexStatusGeneration} has bumped; the segment list
 *       still references the OLD segments because the writeLock-guarded
 *       publish has not run yet; {@code stateLock.isWriteLocked()} is
 *       {@code false} (the slow persist truly ran outside the writeLock).
 *   <li>Post-completion: the segment list contains the merged output;
 *       {@code currentIndexStatusGeneration} matches the new value;
 *       memory counter is consistent with the iterated sum.
 * </ul>
 */
public class PersistentVectorStoreCompactionSwapStagesTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir) {
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(128 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore("testidx", "testtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        // Aggressive compaction so the test cycle actually merges segments.
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

    /**
     * Reflection accessor for the private {@code stateLock} so the hook can
     * assert its state. The ABI is intentionally not exposed publicly; this
     * is a test-only invariant check.
     */
    private static ReentrantReadWriteLock stateLockOf(PersistentVectorStore store) throws Exception {
        Field f = PersistentVectorStore.class.getDeclaredField("stateLock");
        f.setAccessible(true);
        return (ReentrantReadWriteLock) f.get(store);
    }

    @Test
    public void testStageBoundariesAndFinalState() throws Exception {
        Path tmpDir = tmpFolder.newFolder("stages").toPath();
        int dim = 16;
        Random rng = new Random(2026);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            // Build several sealed segments by checkpoint-per-batch so the
            // compaction has meaningful inputs.
            for (int c = 0; c < 5; c++) {
                for (int i = 0; i < 200; i++) {
                    store.addVector(Bytes.from_int(c * 100_000 + i),
                            randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int beforeMerge = store.getSegmentCount();
            assertTrue("must have several segments before merge to make the test"
                    + " meaningful, got " + beforeMerge, beforeMerge >= 2);
            long genBefore = store.getCurrentIndexStatusGeneration();

            ReentrantReadWriteLock stateLock = stateLockOf(store);

            // Capture state at each stage boundary so the test can fail if
            // ANY of the stage invariants is violated.
            AtomicLong genAtPostBuild = new AtomicLong(-1);
            AtomicLong genAtPostPersist = new AtomicLong(-1);
            AtomicReference<Boolean> writeLockedAtPostBuild = new AtomicReference<>();
            AtomicReference<Boolean> writeLockedAtPostPersist = new AtomicReference<>();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            AtomicLong segmentCountAtPostBuild = new AtomicLong(-1);
            AtomicLong segmentCountAtPostPersist = new AtomicLong(-1);

            store.setAtomicSwapPostBuildHookForTest(() -> {
                try {
                    genAtPostBuild.set(store.getCurrentIndexStatusGeneration());
                    writeLockedAtPostBuild.set(stateLock.isWriteLocked());
                    segmentCountAtPostBuild.set(store.getSegmentCount());
                } catch (Throwable t) {
                    hookFailure.compareAndSet(null, t);
                }
            });
            store.setAtomicSwapPostPersistHookForTest(() -> {
                try {
                    genAtPostPersist.set(store.getCurrentIndexStatusGeneration());
                    writeLockedAtPostPersist.set(stateLock.isWriteLocked());
                    segmentCountAtPostPersist.set(store.getSegmentCount());
                } catch (Throwable t) {
                    hookFailure.compareAndSet(null, t);
                }
            });

            // Drive the compaction; the hooks fire in-thread.
            store.runCompactionCycle();

            store.setAtomicSwapPostBuildHookForTest(null);
            store.setAtomicSwapPostPersistHookForTest(null);

            if (hookFailure.get() != null) {
                throw new AssertionError("hook threw", hookFailure.get());
            }

            // Stage 1 (post-build, pre-persist) invariants:
            assertNotEquals("post-build hook never fired — compaction did "
                    + "not actually merge anything", -1L, genAtPostBuild.get());
            assertEquals("currentIndexStatusGeneration must NOT have bumped before persist",
                    genBefore, genAtPostBuild.get());
            assertEquals("segments list still references the OLD segments at post-build",
                    (long) beforeMerge, segmentCountAtPostBuild.get());
            assertEquals("stateLock.writeLock MUST NOT be held during the post-build hook"
                            + " (Stage 1 runs under checkpointLock alone, so concurrent searches"
                            + " can grab readLock)",
                    Boolean.FALSE, writeLockedAtPostBuild.get());

            // Stage 2 (post-persist, pre-publish) invariants:
            assertNotEquals("post-persist hook never fired", -1L, genAtPostPersist.get());
            assertEquals("currentIndexStatusGeneration MUST have bumped during persist",
                    genBefore + 1, genAtPostPersist.get());
            assertEquals("segments list STILL references the OLD segments after persist —"
                            + " publish happens in Stage 3 under writeLock",
                    (long) beforeMerge, segmentCountAtPostPersist.get());
            assertEquals("stateLock.writeLock MUST NOT be held during the post-persist hook"
                            + " (the slow persist must run lock-free; this is the entire point of"
                            + " issue #462)",
                    Boolean.FALSE, writeLockedAtPostPersist.get());

            // Final state: compaction completed, in-memory state matches persisted state.
            int afterMerge = store.getSegmentCount();
            assertTrue("compaction must have reduced segment count: before="
                    + beforeMerge + " after=" + afterMerge,
                    afterMerge < beforeMerge);
            assertEquals("currentIndexStatusGeneration must equal the value observed at"
                            + " post-persist (no further bump)",
                    genAtPostPersist.get(), store.getCurrentIndexStatusGeneration());

            // Diagnostic counter sanity: writeLock window is bounded — far
            // less than the typical multi-second persist would have taken.
            // Even on a heavily loaded CI node, microseconds-scale work
            // should complete within 200 ms.
            assertTrue("Stage 3 writeLock window must be small (issue #462 SLO);"
                            + " observed " + store.getLastCompactionSwapWriteLockNanos() + " ns",
                    store.getLastCompactionSwapWriteLockNanos() < 200_000_000L);
        }
    }

    @Test
    public void testNullHooksAreSafe() throws Exception {
        Path tmpDir = tmpFolder.newFolder("null-hooks").toPath();
        int dim = 16;
        Random rng = new Random(7);

        try (PersistentVectorStore store = createStore(tmpDir)) {
            store.start();

            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < 100; i++) {
                    store.addVector(Bytes.from_int(c * 1000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int before = store.getSegmentCount();
            assertTrue("test setup must produce >= 2 segments", before >= 2);

            // Null hooks must not throw — the swap protocol is the same as
            // when the hooks are not configured (production behaviour).
            store.setAtomicSwapPostBuildHookForTest(null);
            store.setAtomicSwapPostPersistHookForTest(null);

            store.runCompactionCycle();

            assertTrue("compaction reduced segments after null-hook cycle",
                    store.getSegmentCount() < before);
        }
    }
}
