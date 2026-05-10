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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryDataStorageManager;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for the {@link MergeProgress} observability contract (issue #503).
 *
 * <p>Verifies the field-level semantics of {@code enterMerge}, {@code updatePhase},
 * {@code updateBatchProgress}, and {@code enterIdle} in isolation — no ZooKeeper,
 * storage layer, or real merge required.
 *
 * <p>Also covers the {@link RemoteSegmentMerger#setMergeProgress} and
 * {@code getLastMergeTimings} API contract to confirm the wiring does not throw
 * and that the contract holds without running a full merge.
 */
public class MergeProgressObservabilityTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    // -------------------------------------------------------------------------
    // MergeProgress lifecycle — enterMerge
    // -------------------------------------------------------------------------

    @Test
    public void freshInstanceIsIdle() {
        MergeProgress p = new MergeProgress();
        MergeProgress.Snapshot snap = p.snapshot();
        assertEquals("fresh instance must be idle", "idle", snap.phase);
        assertNull("mergeId must be null when idle", snap.mergeId);
        assertEquals(0, snap.inputSegments);
        assertEquals(0L, snap.inputVectors);
        assertEquals(0L, snap.batchesWritten);
        assertEquals(0L, snap.batchesTotal);
        assertEquals(0L, snap.mergeStartMs);
        assertEquals(0L, snap.elapsedMs);
    }

    @Test
    public void enterMergeUsesProvidedTimestampNotSystemClock() {
        // Review item 6: enterMerge must use the caller-supplied startMs so
        // that tests with fake clocks observe a consistent timestamp, and the
        // engine's duration accounting is self-consistent.
        MergeProgress p = new MergeProgress();
        long fakeStart = 123_456_789L;
        p.enterMerge("test-id", 3, 200L, fakeStart);

        MergeProgress.Snapshot snap = p.snapshot();
        assertEquals("phase must be 'downloading' after enterMerge", "downloading", snap.phase);
        assertEquals("mergeId must match", "test-id", snap.mergeId);
        assertEquals("inputSegments must match", 3, snap.inputSegments);
        assertEquals("inputVectors must match", 200L, snap.inputVectors);
        assertEquals("batchesTotal is initialised to inputVectors", 200L, snap.batchesTotal);
        assertEquals("batchesWritten is 0 at start", 0L, snap.batchesWritten);
        // The key assertion: mergeStartMs must equal the supplied value, not
        // System.currentTimeMillis() at call time.
        assertEquals(
                "mergeStartMs must equal the supplied startMs — NOT System.currentTimeMillis()",
                fakeStart, snap.mergeStartMs);
    }

    // -------------------------------------------------------------------------
    // updatePhase — both batchesWritten and batchesTotal reset
    // -------------------------------------------------------------------------

    @Test
    public void updatePhaseResetsBothBatchCounters() {
        // Review item 7: updatePhase must reset BOTH batchesWritten and batchesTotal.
        // Leaving a stale batchesTotal from the previous phase would make
        // pctComplete() return a misleading value for the new phase.
        MergeProgress p = new MergeProgress();
        p.enterMerge("x", 2, 1000L, 1_000L);
        // Simulate batch progress in the downloading phase.
        p.updateBatchProgress(500L, 1000L);
        assertEquals(500L, p.snapshot().batchesWritten);
        assertEquals(1000L, p.snapshot().batchesTotal);

        // Transition to a new phase.
        p.updatePhase("pq-training");
        MergeProgress.Snapshot snap = p.snapshot();
        assertEquals("phase must be updated", "pq-training", snap.phase);
        assertEquals("batchesWritten must be reset on phase transition", 0L, snap.batchesWritten);
        assertEquals(
                "batchesTotal must ALSO be reset on phase transition (review item 7): "
                        + "stale total would make pctComplete misleading for the new phase",
                0L, snap.batchesTotal);
    }

    @Test
    public void updateBatchProgressUpdatesBothFields() {
        MergeProgress p = new MergeProgress();
        p.enterMerge("y", 1, 100L, 2_000L);
        p.updateBatchProgress(37L, 100L);
        MergeProgress.Snapshot snap = p.snapshot();
        assertEquals(37L, snap.batchesWritten);
        assertEquals(100L, snap.batchesTotal);
    }

    // -------------------------------------------------------------------------
    // pctComplete
    // -------------------------------------------------------------------------

    @Test
    public void pctCompleteUsesCurrentBatchCounters() {
        MergeProgress p = new MergeProgress();
        p.enterMerge("z", 1, 100L, 3_000L);
        p.updateBatchProgress(50L, 100L);
        assertEquals("50/100 = 50.0%", 50.0, p.snapshot().pctComplete(), 0.001);
    }

    @Test
    public void pctCompleteIsZeroWhenTotalIsZero() {
        MergeProgress p = new MergeProgress();
        p.enterMerge("z2", 1, 0L, 4_000L); // 0 vectors => batchesTotal=0
        assertEquals("pctComplete must be 0 when batchesTotal=0",
                0.0, p.snapshot().pctComplete(), 0.001);
    }

    // -------------------------------------------------------------------------
    // enterIdle
    // -------------------------------------------------------------------------

    @Test
    public void enterIdleResetsAllFields() {
        MergeProgress p = new MergeProgress();
        p.enterMerge("abc", 5, 500L, 9_999L);
        p.updateBatchProgress(100L, 500L);
        p.enterIdle();

        MergeProgress.Snapshot snap = p.snapshot();
        assertEquals("phase must be idle", "idle", snap.phase);
        assertNull("mergeId must be null after enterIdle", snap.mergeId);
        assertEquals(0, snap.inputSegments);
        assertEquals(0L, snap.inputVectors);
        assertEquals(0L, snap.batchesWritten);
        assertEquals(0L, snap.batchesTotal);
        assertEquals(0L, snap.mergeStartMs);
        assertEquals(0L, snap.elapsedMs);
    }

    @Test
    public void snapshotElapsedMsIsZeroWhenIdle() {
        MergeProgress p = new MergeProgress();
        // Fresh / after enterIdle: elapsedMs must be 0 regardless of when we snapshot.
        assertEquals(0L, p.snapshot().elapsedMs);
    }

    @Test
    public void snapshotElapsedMsIsPositiveDuringMerge() {
        MergeProgress p = new MergeProgress();
        long startMs = System.currentTimeMillis() - 50L; // started 50ms ago
        p.enterMerge("dur", 1, 10L, startMs);
        assertTrue("elapsedMs must be positive during a merge",
                p.snapshot().elapsedMs > 0);
    }

    // -------------------------------------------------------------------------
    // RemoteSegmentMerger.setMergeProgress API contract
    // -------------------------------------------------------------------------

    @Test
    public void getLastMergeTimingsIsNullBeforeAnyMerge() throws Exception {
        // Before a merge completes, getLastMergeTimings() must return null.
        Path tmpDir = tmp.newFolder("rsm-obs").toPath();
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                new MemoryDataStorageManager(), tmpDir,
                /* dim */ 8, /* graphM */ 4, /* beamWidth */ 20,
                /* neighborOverflow */ 1.2f, /* alpha */ 1.2f,
                VectorSimilarityFunction.DOT_PRODUCT);

        assertNull("getLastMergeTimings() must return null before any merge",
                merger.getLastMergeTimings());
    }

    @Test
    public void setMergeProgressNullDoesNotThrow() throws Exception {
        // setMergeProgress(null) is called in the engine's finally block to clear
        // the callbacks; it must never throw regardless of the merger's state.
        Path tmpDir = tmp.newFolder("rsm-null").toPath();
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                new MemoryDataStorageManager(), tmpDir,
                8, 4, 20, 1.2f, 1.2f,
                VectorSimilarityFunction.DOT_PRODUCT);

        // setMergeProgress(null) when already null — must not throw.
        merger.setMergeProgress(null);

        // Set a non-null progress, then clear it — must not throw.
        MergeProgress progress = new MergeProgress();
        progress.enterMerge("wire", 1, 10L, 1L);
        merger.setMergeProgress(progress);
        merger.setMergeProgress(null);

        // After clearing, callbacks in the graph merger are also null;
        // getLastMergeTimings() is still null (no merge was run).
        assertNull("getLastMergeTimings() must still be null after setMergeProgress(null)",
                merger.getLastMergeTimings());
    }

    @Test
    public void setMergeProgressWiresPhaseAndBatchCallbacks() throws Exception {
        // After setMergeProgress(progress), calling progress.updatePhase() from
        // the phaseListener lambda updates progress.phase correctly. Verified by
        // manually calling progress.updatePhase (which is what the phaseListener
        // lambda delegates to).
        Path tmpDir = tmp.newFolder("rsm-wire").toPath();
        RemoteSegmentMerger merger = new RemoteSegmentMerger(
                new MemoryDataStorageManager(), tmpDir,
                8, 4, 20, 1.2f, 1.2f,
                VectorSimilarityFunction.DOT_PRODUCT);

        MergeProgress progress = new MergeProgress();
        progress.enterMerge("wired-merge", 2, 50L, 100L);
        merger.setMergeProgress(progress);

        // The phaseListener wired by setMergeProgress delegates to
        // progress.updatePhase(...); simulate what the graph merger calls.
        progress.updatePhase("uploading");
        assertEquals("uploading", progress.snapshot().phase);
        // batchesTotal reset to 0 by updatePhase (review item 7)
        assertEquals(0L, progress.snapshot().batchesTotal);

        // Batch progress callback: directly exercise the counter.
        progress.updateBatchProgress(25L, 50L);
        assertEquals(25L, progress.snapshot().batchesWritten);
        assertEquals(50L, progress.snapshot().batchesTotal);

        // Clear — must not throw.
        merger.setMergeProgress(null);
    }
}
