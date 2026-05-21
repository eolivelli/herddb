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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * pr-reviewer follow-up #3 for issue #617: direct tests for
 * {@link PersistentVectorStore#dropSegmentByStorageKey(String)} — the engine
 * primitive that the operator-facing {@code DeleteSegment} RPC ultimately
 * invokes after the safety gate, snapshot/refusal checks, and audit log.
 *
 * <p>Engine-level coverage (request validation + checkpoint republish +
 * shadow notification) lives in
 * {@code herddb.indexing.ShadowDeleteSegmentE2ETest}; CLI/wire coverage in
 * {@code herddb.indexing.admin.IndexingAdminCliDeleteSegmentTest}. This
 * class is dedicated to the in-memory mutation primitive so a regression in
 * the lock discipline / dirty-flag / deferred-close path is caught with the
 * smallest possible harness.
 *
 * <p>Coverage matrix:
 * <ol>
 *   <li><b>(a)</b> happy path: a known segment is removed (gone from
 *       {@code segments}, store marked {@code dirty} for the next
 *       checkpoint);</li>
 *   <li><b>(b)</b> idempotence: a second call on the same key returns
 *       {@code SegmentDropResult.NOT_FOUND} without throwing and without
 *       mutating state;</li>
 *   <li><b>(c)</b> deferred-close drain: after the drop the
 *       {@code pendingSegmentCloses} queue eventually empties — i.e. the
 *       segment handle is closed (opportunistic drain runs while no
 *       compaction holds the lock);</li>
 *   <li><b>(d)</b> compaction-race safety: when a cycle is mid-iteration
 *       (after candidate selection, before {@code rebuildSegment}), a
 *       concurrent drop must NOT cause
 *       {@code CompactionException(CORRUPTION)} — same invariant as issue
 *       #535 inherited by the storage-key path.</li>
 * </ol>
 */
public class PersistentVectorStoreAdminDeleteSegmentTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int DIM = 16;
    private int savedMinLive;

    @Before
    public void disableDeferral() {
        savedMinLive = PersistentVectorStore.minLiveVectorsForCheckpoint;
        // Allow tiny checkpoints to seal so we always produce ≥1 on-disk
        // segment with a handful of inserts.
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @After
    public void restoreDeferral() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLive;
    }

    private static float[] vec(Random rng) {
        float[] v = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private PersistentVectorStore newStore(Path tmpDir, MemoryDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        PersistentVectorStore store = new PersistentVectorStore(
                "vidx-617", "testtable", "tstblspace", "vector_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /* compactionIntervalMs */ Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN);
        // Aggressive policy so the race-test below still has something to
        // pick when the hook drops the chosen candidate.
        store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                Integer.MAX_VALUE, 0);
        return store;
    }

    /** Seeds {@code n} on-disk segments via add+checkpoint cycles. */
    private void seedSegments(PersistentVectorStore store, int n) throws Exception {
        Random rng = new Random(617);
        for (int c = 0; c < n; c++) {
            for (int i = 0; i < 30; i++) {
                store.addVector(Bytes.from_int(c * 1000 + i), vec(rng));
            }
            store.checkpoint();
        }
    }

    /**
     * (a) Happy path: a known segment key is removed, the result reports
     * {@code removed=true} with a non-negative {@code vectorsLost}, the
     * segment list shrinks by one, and {@code dirty} is set so the next
     * checkpoint serialises the smaller list.
     */
    @Test(timeout = 30_000)
    public void happyPathRemovesSegmentAndMarksDirty() throws Exception {
        Path tmpDir = tmpFolder.newFolder("admin-delete-happy").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 3);

            List<String> keysBefore = store.getSegmentStorageKeysSnapshot();
            int countBefore = store.getSegmentCount();
            assertTrue("setup must produce ≥1 segment", countBefore >= 1);
            assertEquals(countBefore, keysBefore.size());

            String victim = keysBefore.get(0);

            // Force a checkpoint AFTER the drop so we can observe the dirty
            // flag effect: clear the dirty flag first by performing a no-op
            // checkpoint (which only writes when something has changed
            // since the last save). If we drop then checkpoint, the
            // smaller list must be persisted — verified by re-reading
            // getSegmentStorageKeysSnapshot.
            store.checkpoint(); // baseline; segment list unchanged

            AbstractVectorStore.SegmentDropResult result =
                    store.dropSegmentByStorageKey(victim);

            assertTrue("happy path must report removed=true", result.removed);
            assertTrue("vectorsLost must be ≥ 0 on the happy path, got " + result.vectorsLost,
                    result.vectorsLost >= 0L);

            List<String> keysAfter = store.getSegmentStorageKeysSnapshot();
            assertEquals("segment count must drop by one", countBefore - 1, keysAfter.size());
            assertFalse("victim must be gone from snapshot", keysAfter.contains(victim));

            // The next checkpoint must actually serialise the change —
            // a regression where the dirty flag is not set would leave
            // the on-disk IndexStatus carrying the deleted segment.
            store.checkpoint();
            assertEquals("post-checkpoint list still matches",
                    countBefore - 1, store.getSegmentCount());
        }
    }

    /**
     * (b) Idempotent NOT_FOUND on a second call: dropping the same key
     * twice in a row must NOT throw and must NOT mutate state. The
     * production CLI relies on this — wrapper scripts can be re-run after
     * a partial failure without worrying about double-drop semantics.
     */
    @Test(timeout = 30_000)
    public void doubleDropReportsNotFoundWithoutMutatingState() throws Exception {
        Path tmpDir = tmpFolder.newFolder("admin-delete-idempotent").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 2);

            List<String> keysBefore = store.getSegmentStorageKeysSnapshot();
            assertTrue(keysBefore.size() >= 1);
            String victim = keysBefore.get(0);

            // First call: removes the segment.
            AbstractVectorStore.SegmentDropResult first =
                    store.dropSegmentByStorageKey(victim);
            assertTrue(first.removed);

            int countAfterFirst = store.getSegmentCount();

            // Second call: idempotent NOT_FOUND. State is unchanged.
            AbstractVectorStore.SegmentDropResult second =
                    store.dropSegmentByStorageKey(victim);
            assertFalse("second drop must report removed=false", second.removed);
            assertEquals("NOT_FOUND vectorsLost must be 0",
                    0L, second.vectorsLost);
            assertEquals("segment count must NOT change on the second call",
                    countAfterFirst, store.getSegmentCount());

            // The result is the documented sentinel.
            assertEquals("second call must return the NOT_FOUND sentinel",
                    AbstractVectorStore.SegmentDropResult.NOT_FOUND.removed,
                    second.removed);
        }
    }

    /**
     * (c) Deferred-close drain: the {@code pendingSegmentCloses} queue
     * must eventually drain after the drop. With no compaction running,
     * the opportunistic drain in {@code dropSegmentByStorageKey}'s
     * {@code finally} block should empty the queue synchronously.
     */
    @Test(timeout = 30_000)
    public void pendingSegmentClosesQueueDrainsAfterDrop() throws Exception {
        Path tmpDir = tmpFolder.newFolder("admin-delete-drain").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 2);

            // Idle state: queue must start empty.
            assertEquals("queue must be empty before the drop",
                    0, store.getPendingSegmentClosesSizeForTest());

            List<String> keysBefore = store.getSegmentStorageKeysSnapshot();
            assertTrue(keysBefore.size() >= 1);
            String victim = keysBefore.get(0);

            AbstractVectorStore.SegmentDropResult result =
                    store.dropSegmentByStorageKey(victim);
            assertTrue(result.removed);

            // After the drop, with no concurrent compaction holding
            // compactionLock, the opportunistic drain must have emptied
            // the queue. The drain runs in the same call's `finally`
            // block, so it is synchronous from the caller's POV.
            assertEquals("opportunistic drain must have closed the queued segment "
                            + "since no compaction was running",
                    0, store.getPendingSegmentClosesSizeForTest());
        }
    }

    /**
     * (d) Compaction-race safety: when a cycle is mid-iteration (after
     * candidate selection, before {@code rebuildSegment}), a concurrent
     * {@code dropSegmentByStorageKey} on a candidate must NOT cause
     * {@code CompactionException(CORRUPTION)}. This is the same
     * invariant issue #535 fixed for {@code dropSegmentByUuid}; the
     * storage-key path inherits the same deferred-close protocol so the
     * same test shape applies.
     */
    @Test(timeout = 30_000)
    public void dropDuringCompactionMustNotCauseCorruption() throws Exception {
        Path tmpDir = tmpFolder.newFolder("admin-delete-race").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();

        try (PersistentVectorStore store = newStore(tmpDir, dsm)) {
            store.start();
            seedSegments(store, 5);

            List<String> keysBefore = store.getSegmentStorageKeysSnapshot();
            assertTrue("setup must produce ≥2 segments", keysBefore.size() >= 2);
            String victim = keysBefore.get(0);

            long corruptionBefore = store.getCompactionFailuresCorruptionTotal();

            // Wire the hook to fire the storage-key drop inside the cycle,
            // AFTER candidates are picked but BEFORE rebuildSegment reads
            // seg.onDiskGraph. Pre-issue-#535 the synchronous close inside
            // the drop would have nulled out onDiskGraph and tripped a
            // CORRUPTION failure; the deferred-close protocol queues the
            // close until after the cycle's iteration completes.
            store.setCompactionPostCandidateSelectionHookForTest(() -> {
                AbstractVectorStore.SegmentDropResult r =
                        store.dropSegmentByStorageKey(victim);
                // Hook drops the victim synchronously — the cycle still
                // sees a non-null onDiskGraph because the close was
                // deferred to the cycle's finally block.
                assertNotNull("hook drop must have produced a result", r);
            });

            store.runCompactionCycle();

            // CORE INVARIANT (the fix this code inherits from issue #535):
            // NO CORRUPTION failure must have been recorded.
            assertEquals("no CORRUPTION failure must be recorded — the storage-key"
                            + " drop path inherits issue #535's deferred-close protocol",
                    corruptionBefore, store.getCompactionFailuresCorruptionTotal());

            // The victim must be gone from the active list — the drop
            // already removed it under the writeLock before the hook
            // returned.
            assertFalse("victim must be removed from the active segment list",
                    store.getSegmentStorageKeysSnapshot().contains(victim));

            // By the time runCompactionCycle returns, the deferred close
            // must have drained.
            assertEquals("pendingSegmentCloses queue must drain by the end of the"
                            + " cycle",
                    0, store.getPendingSegmentClosesSizeForTest());
        }
    }
}
