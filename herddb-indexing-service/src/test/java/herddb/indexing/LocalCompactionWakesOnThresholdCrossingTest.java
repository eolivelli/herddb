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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.utils.Bytes;
import java.nio.file.Path;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Validates the wake-on-threshold-crossing path in {@code addVectorInternal}:
 * when an external optimizer is enabled and the IS-local compaction loop is
 * idle (sleeping on its long interval below the kick threshold), the
 * {@code addVector} fast path must wake the loop the instant segment count
 * crosses the threshold — not wait for the full
 * {@code vectorIndexCompactionIntervalMs} to elapse.
 *
 * <p>The test pins the loop on a deliberately-large interval (10 minutes) so
 * any "wake" we observe is guaranteed to come from the threshold-crossing
 * path, not the periodic tick.
 */
public class LocalCompactionWakesOnThresholdCrossingTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "wake";
    private static final String INDEX_UUID = "wake_idx_uuid";
    private static final long PIN_INTERVAL_MS = 10L * 60_000L;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    private float[] randomVector(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void addVectorWakesCompactionWhenSegmentCountCrossesKickThreshold() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // Tiny back-pressure threshold so the kick threshold is reachable
            // with a handful of test segments. 0.5 × 4 = 2 → kickThreshold = 2.
            store.setCompactionBackpressureThreshold(4);
            store.setLocalCompactionKickFraction(0.5d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            // Pin the periodic interval at 10 minutes — any wake we observe in
            // a few seconds is guaranteed to be from the addVector fast-path
            // wake, not the idle tick.
            store.configureCompaction(PIN_INTERVAL_MS, 0L, Long.MAX_VALUE, 2, 1024, 0L);
            store.start();

            // Build segment count up via repeated checkpoints. Each checkpoint
            // seals the live shard into a fresh segment — small ingest is OK.
            Random rng = new Random(1);
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }

            // After several checkpoints we expect ≥ kickThreshold segments.
            // Track whether the local pressure-run counter advanced (i.e., the
            // loop was woken from idle and ran a cycle that crossed the gate).
            // We poll for up to 5 s — well below the pinned interval.
            long deadline = System.currentTimeMillis() + 5_000L;
            long pressureRuns = 0L;
            while (System.currentTimeMillis() < deadline) {
                pressureRuns = store.getLocalCompactionPressureRunsTotal();
                if (pressureRuns > 0L) {
                    break;
                }
                // Drive an addVector to keep arming the wake path; the kick
                // threshold check is on the addVector hot path.
                store.addVector(Bytes.from_int(50_000 + (int) (System.nanoTime() & 0xFFFF)),
                        randomVector(rng, 32));
                Thread.sleep(20L);
            }
            assertNotEquals("the local compaction loop must wake on threshold crossing"
                            + " — it did NOT advance localCompactionPressureRunsTotal within"
                            + " 5 s, but the pinned periodic interval is " + PIN_INTERVAL_MS
                            + " ms; this can only mean the fast-path wake is broken",
                    0L, pressureRuns);
            assertTrue("kick threshold must be ≥ 1", store.currentLocalCompactionKickThreshold() >= 1);
        }
    }

    @Test
    public void wakeFiresAtExactlyKickThreshold() throws Exception {
        // Boundary case: when segment count is EXACTLY at kickThreshold, the
        // gate in runCompactionCycle runs (now < threshold short-circuits, so
        // now == threshold proceeds) and addVector's wake must fire too.
        // Earlier the wake was {@code currentSegments > kickThreshold} (strict)
        // while the gate was {@code now < kickThreshold} — a one-segment
        // mismatch at the boundary that delayed the wake by one full
        // ingest-cycle. This test pins the boundary.
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);

        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            // 0.5 × 4 = 2 → kickThreshold = 2. Drive segment count to exactly 2.
            store.setCompactionBackpressureThreshold(4);
            store.setLocalCompactionKickFraction(0.5d);
            store.setLocalCompactionEnabledWithOptimizer(true);
            store.setExternalCompactionEnabled(true);
            store.configureCompaction(PIN_INTERVAL_MS, 0L, Long.MAX_VALUE, 2, 1024, 0L);
            store.start();

            Random rng = new Random(2);
            for (int batch = 0; batch < 2; batch++) {
                for (int i = 0; i < 80; i++) {
                    store.addVector(Bytes.from_int(batch * 1000 + i),
                            randomVector(rng, 32));
                }
                store.checkpoint();
            }
            // We should now be exactly at kickThreshold=2.
            assertTrue("test must hit the boundary EXACTLY (saw " + store.getSegmentCount()
                            + " segments, kick=" + store.currentLocalCompactionKickThreshold() + ")",
                    store.getSegmentCount() == store.currentLocalCompactionKickThreshold());

            long deadline = System.currentTimeMillis() + 5_000L;
            long pressureRuns = 0L;
            while (System.currentTimeMillis() < deadline) {
                pressureRuns = store.getLocalCompactionPressureRunsTotal();
                if (pressureRuns > 0L) {
                    break;
                }
                // Keep poking the wake path with adds at the boundary.
                store.addVector(Bytes.from_int(50_000 + (int) (System.nanoTime() & 0xFFFF)),
                        randomVector(rng, 32));
                Thread.sleep(20L);
            }
            assertNotEquals("at exactly kickThreshold segments the wake must still fire"
                            + " (gate-vs-wake boundary alignment); did NOT see a pressure"
                            + " run within 5 s", 0L, pressureRuns);
        }
    }
}
