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
 * Tests for issue #354: segment-count back-pressure in
 * {@link PersistentVectorStore#addVectorInternal}.
 *
 * <p>Verifies:
 * <ol>
 *   <li>When the on-disk segment count exceeds the threshold,
 *       {@code addVector} blocks and the compaction thread is woken.</li>
 *   <li>Back-pressure is released once a compaction cycle reduces the
 *       segment count back below the threshold.</li>
 *   <li>Disabling the threshold (set to {@link Integer#MAX_VALUE}) lets
 *       segment count grow without ever blocking.</li>
 * </ol>
 */
public class Issue354SegmentCountBackpressureTest {

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
     * Builds a store with a low back-pressure threshold (5 segments).
     * After creating 6 segments, a background thread attempts to call
     * {@code addVector}; that call must block until we trigger a
     * compaction cycle on the test thread that reduces segment count
     * below the threshold.
     */
    @Test(timeout = 60_000)
    public void backpressureBlocksAndReleasesAfterCompaction() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bp-354").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        int threshold = 5;
        PersistentVectorStore store = new PersistentVectorStore(
                "testidx354bp", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ 1,
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ 2,
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(threshold);

        try (store) {
            store.start();
            Random rng = new Random(354_2);
            int dim = 8;

            // Phase 1: build threshold+1 = 6 segments without backpressure.
            for (int c = 0; c <= threshold; c++) {
                for (int i = 0; i < 50; i++) {
                    store.addVector(Bytes.from_int(c * 1_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }
            int segsBeforeBp = store.getSegmentCount();
            assertTrue("need more than threshold segments; got " + segsBeforeBp,
                    segsBeforeBp > threshold);

            // Phase 2: launch background thread that calls addVector — it must block.
            CountDownLatch beforeBlock = new CountDownLatch(1);
            CountDownLatch afterRelease = new CountDownLatch(1);
            AtomicBoolean addVectorCompleted = new AtomicBoolean(false);

            Thread adder = new Thread(() -> {
                beforeBlock.countDown();
                // This call must block while segments > threshold and then
                // unblock once the compaction thread (woken by
                // waitForSegmentCountRelief) reduces the segment count.
                store.addVector(Bytes.from_int(999_999), randomVector(rng, dim));
                addVectorCompleted.set(true);
                afterRelease.countDown();
            }, "adder-354");
            adder.setDaemon(true);
            adder.start();

            // Wait until the adder has at least started.
            assertTrue("adder thread did not start", beforeBlock.await(10, TimeUnit.SECONDS));

            // Phase 3: the adder's waitForSegmentCountRelief already notified the
            // background compaction thread when it entered backpressure, so
            // compaction will run (or is already running) on the background thread.
            // We drive additional cycles from here in case the first is in-flight.
            int maxRounds = 20;
            for (int round = 0; round < maxRounds && !addVectorCompleted.get(); round++) {
                store.runCompactionCycle();
                Thread.sleep(50);
            }

            // Phase 4: addVector must have completed within the timeout.
            assertTrue("addVector should have unblocked after compaction reduced segment count",
                    afterRelease.await(10, TimeUnit.SECONDS));
            assertTrue("addVector must eventually complete",
                    addVectorCompleted.get());
            // Verify that back-pressure was recorded at least once — the counter
            // is incremented each time waitForSegmentCountRelief is entered, so
            // even if the whole wait was < 1 ms, the counter must be non-zero.
            assertTrue("segment-count back-pressure event must be recorded",
                    store.getSegmentCountBackpressureTotal() >= 1);
            assertFalse("segment-count back-pressure must no longer be active",
                    store.isSegmentCountBackpressureActive());
        }
    }

    /**
     * Verifies that with the back-pressure threshold disabled
     * ({@link Integer#MAX_VALUE}), segment count can grow freely
     * without ever recording a back-pressure event.
     */
    @Test
    public void noBackpressureWhenThresholdDisabled() throws Exception {
        Path tmpDir = tmpFolder.newFolder("bp-354-disabled").toPath();
        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        PersistentVectorStore store = new PersistentVectorStore(
                "testidx354bpoff", "testtable", "tstblspace", "vec",
                tmpDir, dsm, mm,
                8, 32, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                /*compactionIntervalMs*/ Long.MAX_VALUE);
        store.configureCompaction(
                /*intervalMs*/ Long.MAX_VALUE,
                /*minBytes*/ Long.MAX_VALUE,   // never fires
                /*maxBytes*/ Long.MAX_VALUE,
                /*minCount*/ Integer.MAX_VALUE, // never fires
                /*maxCount*/ Integer.MAX_VALUE,
                /*retentionMs*/ 0);
        store.setTieredCompactionEnabled(false);
        store.setCompactionBackpressureThreshold(Integer.MAX_VALUE); // disabled

        try (store) {
            store.start();
            Random rng = new Random(354_3);
            int dim = 8;

            // Create 20 segments — all go through without any backpressure.
            for (int c = 0; c < 20; c++) {
                for (int i = 0; i < 50; i++) {
                    store.addVector(Bytes.from_int(c * 1_000 + i), randomVector(rng, dim));
                }
                store.checkpoint();
            }

            assertEquals("no back-pressure events expected when threshold is disabled",
                    0L, store.getSegmentCountBackpressureTotal());
            assertFalse("back-pressure must not be active",
                    store.isSegmentCountBackpressureActive());
            assertTrue("segments built up freely", store.getSegmentCount() >= 10);
        }
    }
}
