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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #640 — end-to-end test that {@link PersistentVectorStore#runCompactionCycle}
 * surfaces real-time streaming-compaction progress on the IS side:
 * <ol>
 *   <li>While {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor#compact}
 *       runs, {@link PersistentVectorStore#getCompactionPhase} returns
 *       {@code "compacting-graph"} (NOT {@code "idle"} as it did pre-fix —
 *       the central symptom in the issue's evidence).</li>
 *   <li>{@link PersistentVectorStore#getCompactionBatchesDone()} /
 *       {@link PersistentVectorStore#getCompactionBatchesTotal()} move from
 *       0 to non-zero during the run (instead of staying at the previous
 *       cycle's terminal values).</li>
 *   <li>{@link PersistentVectorStore#getCompactionCycleId()} bumps every
 *       cycle so external pollers can detect a new cycle even when the per-cycle
 *       counters happen to be at 100% from the previous one.</li>
 *   <li>{@link PersistentVectorStore#isCompactionRunning()} is true during
 *       the cycle and false after it ends.</li>
 *   <li>After the cycle ends, the per-cycle counters are LEFT POPULATED so a
 *       post-hoc describe-index still sees the totals — and the NEXT cycle
 *       resets them on begin (not on end).</li>
 * </ol>
 */
public class StreamingCompactionISLiveProgressTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private int savedMinLiveVectors;

    @org.junit.Before
    public void setUp() {
        savedMinLiveVectors = PersistentVectorStore.minLiveVectorsForCheckpoint;
        PersistentVectorStore.minLiveVectorsForCheckpoint = 0;
    }

    @org.junit.After
    public void tearDown() {
        PersistentVectorStore.minLiveVectorsForCheckpoint = savedMinLiveVectors;
    }

    private static float[] vec(Random rng, int dim) {
        float[] v = new float[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        return v;
    }

    @Test
    public void streamingCompactionExposesLiveProgress() throws Exception {
        Path tmpDir = tmpFolder.newFolder("is-live-progress").toPath();
        int dim = 16;
        int numSegments = 3;
        int perSegment = 200;
        Random rng = new Random(640L);

        MemoryDataStorageManager dsm = new MemoryDataStorageManager();
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);

        try (PersistentVectorStore store = new PersistentVectorStore(
                "ts-640", "tbl-640", "tsuuid-640", "vec_col",
                tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE,
                VectorSimilarityFunction.EUCLIDEAN)) {
            // Aggressive compaction: any 2+ segments merge in the next cycle.
            store.configureCompaction(Long.MAX_VALUE, 1L, Long.MAX_VALUE, 2,
                    Integer.MAX_VALUE, 0);
            store.start();

            // Build numSegments on-disk segments via checkpoint.
            for (int c = 0; c < numSegments; c++) {
                for (int i = 0; i < perSegment; i++) {
                    store.addVector(Bytes.from_int(c * 10_000 + i), vec(rng, dim));
                }
                store.checkpoint();
            }
            assertEquals(numSegments, store.getSegmentCount());

            // Pre-cycle state: counters are at whatever the last checkpoint
            // left them at, but no cycle is running.
            assertEquals("compaction must not be running before runCompactionCycle",
                    false, store.isCompactionRunning());
            long preCycleId = store.getCompactionCycleId();

            // Sample the live state from a watcher thread while the cycle
            // runs. Use CAS to capture the max done/total observed and the
            // set of phases seen.
            AtomicBoolean watcherStop = new AtomicBoolean();
            AtomicLong maxBatchesDone = new AtomicLong();
            AtomicLong maxBatchesTotal = new AtomicLong();
            AtomicBoolean sawCompactingPhase = new AtomicBoolean();
            AtomicBoolean sawRunning = new AtomicBoolean();
            AtomicLong observedCycleId = new AtomicLong(-1L);
            Thread watcher = new Thread(() -> {
                while (!watcherStop.get()) {
                    if (store.isCompactionRunning()) {
                        sawRunning.set(true);
                    }
                    String phase = store.getCompactionPhase();
                    if ("compacting-graph".equals(phase)) {
                        sawCompactingPhase.set(true);
                    }
                    long d = store.getCompactionBatchesDone();
                    long t = store.getCompactionBatchesTotal();
                    if (d > maxBatchesDone.get()) {
                        maxBatchesDone.set(d);
                    }
                    if (t > maxBatchesTotal.get()) {
                        maxBatchesTotal.set(t);
                    }
                    long cid = store.getCompactionCycleId();
                    if (cid != observedCycleId.get()) {
                        observedCycleId.set(cid);
                    }
                }
            }, "is-live-progress-watcher");
            watcher.setDaemon(true);
            watcher.start();

            store.runCompactionCycle();

            watcherStop.set(true);
            watcher.join(5_000);

            // Cycle id strictly bumped — even if the watcher never saw the
            // "running" window (very fast machine), this assertion fails the
            // user's "stale counters" scenario where two cycles look identical.
            long postCycleId = store.getCompactionCycleId();
            assertNotEquals("compactionCycleId must bump on a run",
                    preCycleId, postCycleId);
            assertTrue("compactionCycleId is strictly monotonic",
                    postCycleId > preCycleId);

            // The watcher should have observed the cycle: running flag true at
            // some sample, "compacting-graph" phase at some sample, and the
            // batch counters non-zero by the time the listener fires.
            assertTrue("watcher must have observed isCompactionRunning=true at some point",
                    sawRunning.get());
            assertTrue("watcher must have observed compaction_phase=compacting-graph"
                            + " (was 'idle' pre-fix — the central issue #640 symptom)",
                    sawCompactingPhase.get());
            assertTrue("compactionBatchesTotal must reach non-zero during the cycle",
                    maxBatchesTotal.get() > 0L);
            assertTrue("compactionBatchesDone must reach non-zero during the cycle",
                    maxBatchesDone.get() > 0L);

            // Post-cycle: running flag clear, elapsed reads as 0 (idle).
            assertEquals("compaction_running false after cycle",
                    false, store.isCompactionRunning());
            assertEquals("elapsed_ms reads 0 when idle",
                    0L, store.getCompactionElapsedMs());

            // Counters are LEFT POPULATED so a post-hoc describe-index sees
            // the cycle totals — exactly the legacy Phase B contract from
            // issue #80 that the issue #640 fix preserves.
            assertTrue("compactionBatchesDone left populated after end",
                    store.getCompactionBatchesDone() > 0L);
            assertTrue("compactionBatchesTotal left populated after end",
                    store.getCompactionBatchesTotal() > 0L);

            // Input metadata captured at begin reflects the cycle shape.
            assertEquals("input segment count reflects the candidates merged",
                    (long) numSegments, store.getCompactionInputSegmentCount());
            // We don't pin an exact vector count (segment.liveCount depends
            // on jvector internals); the lower bound is the perSegment we
            // wrote and the upper bound is the total — accept anything in
            // that range to keep the test stable.
            long vecs = store.getCompactionInputVectorCount();
            assertTrue("input vector count is positive: " + vecs, vecs > 0L);
            assertTrue("input vector count is at most numSegments*perSegment: " + vecs,
                    vecs <= (long) numSegments * perSegment);
        }
    }
}
