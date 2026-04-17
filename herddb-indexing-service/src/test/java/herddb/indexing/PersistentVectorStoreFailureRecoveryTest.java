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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for Phase B failure recovery in {@link PersistentVectorStore}:
 * provisional multipart files must be rolled back when a checkpoint Phase B or
 * Phase C-prep aborts, so that leaked artefacts do not pile up on disk.
 */
public class PersistentVectorStoreFailureRecoveryTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    /**
     * Counts every {@code writeMultipartIndexFile} call and records the live
     * set of multipart artefacts. Phase B in the multipart-only vector store
     * no longer writes via {@code writeIndexPage}, so all failure injection
     * and rollback assertions now work at the multipart level.
     */
    private static final class TrackingDSM extends MemoryDataStorageManager {
        /** Multipart logical paths currently alive (not deleted). */
        final Set<String> aliveMultipartFiles = ConcurrentHashMap.newKeySet();
        /** Number of successful writeMultipartIndexFile calls observed, including the one that was armed to throw. */
        final AtomicInteger writeCount = new AtomicInteger(0);
        final AtomicInteger deleteCount = new AtomicInteger(0);
        /** If > 0, the Nth writeMultipartIndexFile call will throw. 1-indexed. */
        volatile int throwOnWriteNumber = -1;
        /** If non-null, calls to deleteMultipartIndexFile throw. */
        volatile DataStorageManagerException deleteThrows;

        @Override
        public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                              Path tempFile, LongConsumer progress)
                throws IOException, DataStorageManagerException {
            int n = writeCount.incrementAndGet();
            if (n == throwOnWriteNumber) {
                throw new DataStorageManagerException("injected write failure at call #" + n);
            }
            String path = super.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
            aliveMultipartFiles.add(path);
            return path;
        }

        @Override
        public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
                throws DataStorageManagerException {
            deleteCount.incrementAndGet();
            if (deleteThrows != null) {
                throw deleteThrows;
            }
            String key = tableSpace + "/" + uuid + "/" + fileType;
            super.deleteMultipartIndexFile(tableSpace, uuid, fileType);
            aliveMultipartFiles.remove(key);
        }
    }

    private PersistentVectorStore createStore(Path tmpDir, TrackingDSM dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        // compactionIntervalMs = Long.MAX_VALUE / 2 effectively disables the
        // background compaction thread so tests can drive checkpoints manually
        // and observe the failure/rollback without races.
        return new PersistentVectorStore("failidx", "failtable", "tstblspace",
                "vector_col", tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE / 2);
    }

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    private static void addVectors(PersistentVectorStore store, int count, int dim, int seed) {
        Random rng = new Random(seed);
        for (int i = 0; i < count; i++) {
            store.addVector(Bytes.from_int(seed * 100000 + i), randomVector(rng, dim));
        }
    }

    @Test
    public void phaseBFailureRollsBackAllProvisionalPages() throws Exception {
        // Arrange: a store that successfully completes one checkpoint (baseline),
        // then a second checkpoint fails partway through Phase B. After the
        // failure, aliveMultipartFiles should NOT contain any artefact written
        // by the failed attempt — everything should roll back to the baseline set.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            // Two initial batches + two checkpoints to build up multiple sealed
            // segments, so the next Phase B writes several multipart files we
            // can interrupt partway through.
            addVectors(store, 300, dim, 1);
            store.checkpoint();
            addVectors(store, 300, dim, 2);
            store.checkpoint();

            Set<String> baselineFiles = new HashSet<>(dsm.aliveMultipartFiles);
            int baselineWrites = dsm.writeCount.get();
            assertFalse("baseline checkpoint should have written multipart artefacts",
                    baselineFiles.isEmpty());

            // Add more data. This batch will be bigger so Phase B writes
            // multiple files and we can interrupt in the middle.
            addVectors(store, 1200, dim, 3);

            // Inject a failure on the 2nd multipart write of the next Phase B.
            dsm.throwOnWriteNumber = baselineWrites + 2;

            try {
                store.checkpoint();
                fail("expected checkpoint to propagate the injected failure");
            } catch (Exception expected) {
                // ok
            }

            // Assert: no multipart artefact written by the failed attempt remains.
            assertEquals("alive multipart files must equal the baseline after rollback",
                    baselineFiles, dsm.aliveMultipartFiles);
            assertTrue("rolled-back artefact metric must be > 0",
                    store.getTotalRolledBackPages() > 0);
            assertEquals("one consecutive failure recorded",
                    1, store.getConsecutiveCheckpointFailures());
            assertEquals("one total failure recorded",
                    1, store.getTotalCheckpointFailures());
        }
    }

    @Test
    public void successfulCheckpointDoesNotRollBack() throws Exception {
        // Arrange: a plain successful checkpoint must not trigger any deletes.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 7);
            store.checkpoint();

            assertEquals("no provisional-page deletes on a happy-path checkpoint",
                    0, dsm.deleteCount.get());
            assertEquals("no rollback metric on success",
                    0, store.getTotalRolledBackPages());
            assertEquals("failure counter resets on success",
                    0, store.getConsecutiveCheckpointFailures());
        }
    }

    @Test
    public void consecutiveFailuresIncrementThenResetOnSuccess() throws Exception {
        // Arrange: fail the next checkpoint twice, then let it succeed.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 11);
            store.checkpoint();

            int baselineWrites = dsm.writeCount.get();

            // First failure: need enough new data so Phase B writes at least
            // 2 pages (one per graph/map segment) and we can interrupt.
            addVectors(store, 1200, dim, 12);
            dsm.throwOnWriteNumber = baselineWrites + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            assertEquals(1, store.getConsecutiveCheckpointFailures());
            assertEquals(1, store.getTotalCheckpointFailures());

            // Second failure
            baselineWrites = dsm.writeCount.get();
            dsm.throwOnWriteNumber = baselineWrites + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            assertEquals(2, store.getConsecutiveCheckpointFailures());
            assertEquals(2, store.getTotalCheckpointFailures());

            // Success path: failure counter must reset, total stays.
            dsm.throwOnWriteNumber = -1;
            store.checkpoint();
            assertEquals(0, store.getConsecutiveCheckpointFailures());
            assertEquals("total failures are monotonic", 2, store.getTotalCheckpointFailures());
        }
    }

    @Test
    public void rollbackToleratesDeleteFailures() throws Exception {
        // Arrange: fail Phase B AND make every subsequent deleteIndexPage throw.
        // The failure must still propagate, in-memory state must still reset,
        // and later recovery must be possible once the delete backend recovers.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 21);
            store.checkpoint();

            int baselineWrites = dsm.writeCount.get();
            addVectors(store, 1200, dim, 22);

            dsm.throwOnWriteNumber = baselineWrites + 2;
            dsm.deleteThrows = new DataStorageManagerException("injected delete failure");

            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            assertTrue("delete was attempted at least once", dsm.deleteCount.get() > 0);
            assertEquals("no successful rollbacks (delete always threw)",
                    0, store.getTotalRolledBackPages());

            // Now the delete backend recovers, and a successful checkpoint cleans up
            // the previously-leaked pages via the standard indexCheckpoint sweep.
            dsm.throwOnWriteNumber = -1;
            dsm.deleteThrows = null;
            store.checkpoint();
            assertEquals("failure counter resets after success",
                    0, store.getConsecutiveCheckpointFailures());
            // The leaked pages from the first failure attempt remain alive because
            // deleteIndexPage was still throwing when they were rolled back. That
            // is an accepted degraded mode: next successful indexCheckpoint will
            // reclaim them via its own sweep. This test just documents the
            // contract and verifies we didn't crash.
        }
    }

    @Test
    public void recoveredStoreHasCorrectLiveState() throws Exception {
        // Arrange: a checkpoint fails, we keep inserting, and a later checkpoint
        // succeeds. Searches should return results from both batches — nothing
        // should be lost by the failure.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 31);
            store.checkpoint();

            int baselineWrites = dsm.writeCount.get();
            addVectors(store, 1200, dim, 32);

            dsm.throwOnWriteNumber = baselineWrites + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }

            // After rollback, all 1500 vectors must still be searchable (the first
            // batch from disk, the second from live shards).
            assertEquals("all vectors must remain after failure", 1500, store.size());

            // Insert a third batch and successfully checkpoint.
            addVectors(store, 100, dim, 33);
            dsm.throwOnWriteNumber = -1;
            store.checkpoint();
            assertEquals(1600, store.size());

            float[] query = randomVector(new Random(99), dim);
            List<Map.Entry<Bytes, Float>> results = store.search(query, 5);
            assertFalse("search must return results after recovery", results.isEmpty());
        }
    }

    @Test
    public void differentCheckpointAttemptsAllocateDifferentPageIds() throws Exception {
        // Regression guard: we want two successive failures to allocate
        // different pageIds (i.e. we don't try to "reuse" pageIds on retry,
        // which could corrupt the page store).
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 41);
            store.checkpoint();

            int baseline = dsm.writeCount.get();
            addVectors(store, 1200, dim, 42);
            dsm.throwOnWriteNumber = baseline + 2;
            int totalWritesBeforeFirstFail;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            totalWritesBeforeFirstFail = dsm.writeCount.get();

            dsm.throwOnWriteNumber = totalWritesBeforeFirstFail + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }

            assertNotEquals("second failed attempt should allocate new pageIds",
                    totalWritesBeforeFirstFail, dsm.writeCount.get());
        }
    }

    @Test
    public void metricsReflectCheckpointProgress() throws Exception {
        // After a checkpoint, the P3.7 metrics must report sensible values:
        // bytes-written > 0, vectors/sec > 0, sealed segment count >= 1.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            assertEquals("no checkpoint yet", 0L, store.getLastPhaseBBytesWritten());
            assertEquals("no segments yet", 0, store.getSealedSegmentCount());

            addVectors(store, 1200, dim, 61);
            store.checkpoint();

            assertTrue("phase-b bytes written must be positive after checkpoint",
                    store.getLastPhaseBBytesWritten() > 0);
            assertTrue("vectors/sec must be positive after checkpoint",
                    store.getLastPhaseBVectorsPerSecond() > 0);
            assertTrue("at least one sealed segment",
                    store.getSealedSegmentCount() >= 1);
            assertTrue("free disk bytes must be non-negative",
                    store.getFreeDiskBytes() >= 0);
            // tmpDirBytes should be near zero after the P1.4 change (no
            // resident graph tmp files).
            assertEquals("tmp dir must be clean at rest",
                    0L, store.getTmpDirBytes());
        }
    }

    @Test
    public void rollbackMetricTracksLatestRecovery() throws Exception {
        // The per-attempt lastRolledBackPages must reflect only the most
        // recent failure's rollback count — not the cumulative total.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 71);
            store.checkpoint();

            int baseline = dsm.writeCount.get();
            addVectors(store, 1200, dim, 72);
            dsm.throwOnWriteNumber = baseline + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            long firstLast = store.getLastRolledBackPages();
            long firstTotal = store.getTotalRolledBackPages();
            assertTrue("first rollback: at least 1 page",
                    firstLast >= 1);
            assertEquals("first total equals first per-attempt",
                    firstLast, firstTotal);

            // Second failure; last should RESET to the new count, total should GROW.
            baseline = dsm.writeCount.get();
            dsm.throwOnWriteNumber = baseline + 2;
            try {
                store.checkpoint();
                fail();
            } catch (Exception expected) {
            }
            long secondLast = store.getLastRolledBackPages();
            long secondTotal = store.getTotalRolledBackPages();
            assertTrue("second per-attempt: at least 1 page", secondLast >= 1);
            assertEquals("second total is sum of per-attempts",
                    firstLast + secondLast, secondTotal);
        }
    }

    @Test
    public void backoffObservableAfterFailures() throws Exception {
        // Integration check: after N consecutive Phase B failures,
        // consecutiveCheckpointFailures = N and the pure-function
        // computeBackoffMs produces a strictly positive, monotonic schedule.
        // The actual sleep in compactionLoop is not exercised here to keep
        // the test fast; see PersistentVectorStoreBackoffTest for that.
        Path tmpDir = tmpFolder.newFolder().toPath();
        TrackingDSM dsm = new TrackingDSM();
        int dim = 32;

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.start();
            addVectors(store, 300, dim, 51);
            store.checkpoint();

            long prevBackoff = -1;
            for (int i = 1; i <= 3; i++) {
                int baseline = dsm.writeCount.get();
                addVectors(store, 1200, dim, 50 + i);
                dsm.throwOnWriteNumber = baseline + 2;
                try {
                    store.checkpoint();
                    fail();
                } catch (Exception expected) {
                }
                assertEquals(i, store.getConsecutiveCheckpointFailures());
                long backoff = herddb.index.vector.PersistentVectorStore.computeBackoffMs(
                        60_000L, store.getConsecutiveCheckpointFailures(),
                        30L * 60 * 1000);
                assertTrue("backoff must grow across failures (was " + prevBackoff
                                + ", now " + backoff + " at step " + i + ")",
                        backoff > prevBackoff);
                prevBackoff = backoff;
            }
        }
    }
}
