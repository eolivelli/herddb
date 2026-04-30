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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for issue #370: segment-count back-pressure deadlock.
 *
 * <p>Verifies two fixes:
 * <ol>
 *   <li><b>Fix 1 (Bug A):</b> {@code runCompactionCycle()} calls
 *       {@code notifySegmentCountMonitor()} even when
 *       {@code chooseSegmentsToMerge()} returns an empty candidate list.
 *       Without this fix, apply threads stay in back-pressure until the
 *       100 ms poll wakes them — but they never receive an explicit signal
 *       from the cycle that found no work to do.</li>
 *   <li><b>Fix 2:</b> {@link PersistentVectorStore#waitForSegmentCountRelief}
 *       has a configurable safety timeout.  When compaction cannot drain the
 *       segment count (empty candidates or repeated failures) the blocked
 *       apply thread is force-released after {@code compactionBackpressureMaxWaitMs}
 *       milliseconds with a SEVERE log, preventing a permanent IS hang.</li>
 * </ol>
 */
public class Issue370BackpressureDeadlockTest {

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

    private static float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int d = 0; d < dim; d++) {
            v[d] = rng.nextFloat();
        }
        return v;
    }

    /**
     * Fix 1 (Bug A): verifies that {@code runCompactionCycle()} increments the
     * segment-count-monitor generation even when it returns an empty candidate
     * list (i.e. {@code notifySegmentCountMonitor()} is called on the
     * empty-candidates path).
     *
     * <p>The generation counter is incremented inside every
     * {@code notifySegmentCountMonitor()} call.  Before the fix the
     * empty-candidates path skipped the notification, leaving the generation
     * unchanged.
     */
    @Test(timeout = 30_000)
    public void emptyCandidatesPathNotifiesSegmentCountMonitor() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bp370-bug-a").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx370a", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        // Configure compaction so that no candidates are ever chosen:
        // minBytes = Long.MAX_VALUE and minCount = Integer.MAX_VALUE together
        // guarantee chooseSegmentsToMerge() always returns an empty list.
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ Integer.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE); // don't trigger back-pressure

        try (store) {
            store.start();
            Random rng = new Random(370_1);
            int dim = 8;

            // Build a few segments so runCompactionCycle has a non-empty snapshot.
            for (int c = 0; c < 3; c++) {
                for (int i = 0; i < 20; i++) {
                    store.addVector(Bytes.from_int(c * 100 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            int genBefore = store.getSegmentCountGeneration();

            // Drive one compaction cycle — it must find no candidates and still
            // call notifySegmentCountMonitor(), incrementing the generation.
            store.runCompactionCycle();

            int genAfter = store.getSegmentCountGeneration();
            assertTrue(
                    "runCompactionCycle() with empty candidates must call notifySegmentCountMonitor()"
                            + " (generation before=" + genBefore + ", after=" + genAfter + ")",
                    genAfter > genBefore);
        }
    }

    /**
     * Fix 2: verifies that a thread blocked in segment-count back-pressure is
     * force-released after the safety timeout ({@code backpressureMaxWaitMs})
     * even when compaction cannot reduce the segment count.
     *
     * <p>This is the real deadlock scenario from the BIGANN 1B GKE run:
     * segment count > threshold, compaction returns empty candidates,
     * apply threads park forever.  With Fix 2 the apply thread is released
     * after {@code maxWaitMs} and ingestion resumes.
     */
    @Test(timeout = 30_000)
    public void backpressureReleasesAfterTimeoutWhenCompactionCannotHelp() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bp370-timeout").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int threshold = 5;
        // Very short safety timeout so the test runs in < 5 s.
        long maxWaitMs = 1_500L;

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx370b", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        // Compaction configured to never select candidates.
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ Integer.MAX_VALUE,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(threshold);
        store.setCompactionBackpressureMaxWaitMs(maxWaitMs);

        try (store) {
            store.start();
            Random rng = new Random(370_2);
            int dim = 8;

            // Phase 1: build threshold+1 segments — back-pressure will fire.
            for (int c = 0; c <= threshold; c++) {
                for (int i = 0; i < 20; i++) {
                    store.addVector(Bytes.from_int(c * 1_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int segsBefore = store.getSegmentCount();
            assertTrue("need more than threshold segments; got " + segsBefore,
                    segsBefore > threshold);

            // Phase 2: launch a background thread that will hit back-pressure.
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean completed = new AtomicBoolean(false);

            Thread adder = new Thread(() -> {
                started.countDown();
                // This will enter waitForSegmentCountRelief() since segments > threshold.
                store.addVector(Bytes.from_int(999_370), randomVector(rng, dim));
                completed.set(true);
                done.countDown();
            }, "adder-370");
            adder.setDaemon(true);
            adder.start();

            assertTrue("adder thread must start", started.await(5, TimeUnit.SECONDS));

            // Phase 3: periodically drive compaction cycles (which find no
            // candidates) so Fix 1 notifications are interleaved with Fix 2's
            // timeout check.  The timeout alone would also suffice.
            for (int round = 0; round < 5 && !completed.get(); round++) {
                store.runCompactionCycle();
                Thread.sleep(200);
            }

            // Phase 4: the safety timeout must have fired and released the thread.
            assertTrue(
                    "addVector must complete after the back-pressure safety timeout",
                    done.await(10, TimeUnit.SECONDS));
            assertTrue("addVector must actually complete", completed.get());

            // Verify timeout was recorded — the segment count did NOT go below
            // threshold (compaction never selected candidates), so the release
            // was caused by the timeout, not by successful compaction.
            assertEquals("segment count must still be above threshold after timeout release",
                    segsBefore, store.getSegmentCount()); // no segments were merged
            assertTrue(
                    "safety timeout counter must be incremented",
                    store.getSegmentCountBackpressureTimeouts() >= 1);
            // Back-pressure must not be active after the release.
            assertFalse("back-pressure must not be active after release",
                    store.isSegmentCountBackpressureActive());
        }
    }

    /**
     * Regression guard: verifies that the normal (happy-path) back-pressure
     * release — compaction successfully merges segments below the threshold —
     * still works correctly after the Fix 2 changes to
     * {@code waitForSegmentCountRelief()}.  No timeout should fire.
     */
    @Test(timeout = 60_000)
    public void backpressureReleasesAfterSuccessfulCompactionWithoutTimeout() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bp370-happy").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int threshold = 5;

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx370c", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        // Compaction can select candidates (minBytes=1, minCount=2).
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(threshold);
        // Long timeout — must not fire during a normal compaction-driven release.
        store.setCompactionBackpressureMaxWaitMs(60_000L);

        try (store) {
            store.start();
            Random rng = new Random(370_3);
            int dim = 8;

            // Build threshold+1 segments to trigger back-pressure.
            for (int c = 0; c <= threshold; c++) {
                for (int i = 0; i < 50; i++) {
                    store.addVector(Bytes.from_int(c * 1_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            assertTrue("need more than threshold segments",
                    store.getSegmentCount() > threshold);

            // Launch background thread that will block in back-pressure.
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean completed = new AtomicBoolean(false);

            Thread adder = new Thread(() -> {
                started.countDown();
                store.addVector(Bytes.from_int(999_371), randomVector(rng, dim));
                completed.set(true);
                done.countDown();
            }, "adder-370c");
            adder.setDaemon(true);
            adder.start();

            assertTrue("adder thread must start", started.await(5, TimeUnit.SECONDS));

            // Drive compaction cycles until the adder completes.
            int maxRounds = 30;
            for (int round = 0; round < maxRounds && !completed.get(); round++) {
                store.runCompactionCycle();
                Thread.sleep(50);
            }

            assertTrue(
                    "addVector must complete after compaction reduces segment count",
                    done.await(10, TimeUnit.SECONDS));
            assertTrue("addVector must actually complete", completed.get());

            // No timeout should have fired — compaction did the job.
            assertEquals("safety timeout must NOT fire on successful compaction path",
                    0L, store.getSegmentCountBackpressureTimeouts());
            assertFalse("back-pressure must not be active",
                    store.isSegmentCountBackpressureActive());
        }
    }
}
