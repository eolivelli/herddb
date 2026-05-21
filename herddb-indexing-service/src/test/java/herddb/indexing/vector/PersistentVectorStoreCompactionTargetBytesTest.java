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
import herddb.core.MemoryManager;
import herddb.file.FileDataStorageManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Setter/getter wiring tests for the per-segment graduation cap (issue #631).
 *
 * <p>The cap field {@code vectorIndexCompactionTargetBytes} is what
 * {@link PersistentVectorStore#runCompactionCycle} passes to
 * {@link VectorIndexCompactor#chooseSegmentsToMerge}. The policy-level
 * behaviour is covered by {@link VectorIndexCompactorChooseTest}; this test
 * pins the public API surface — default, positive set, disable via {@code 0} /
 * negative — so the wiring through {@link herddb.indexing.IndexingServiceEngine}
 * (which reads the config key and calls {@link
 * PersistentVectorStore#setCompactionTargetBytes(long)}) stays correct.
 */
public class PersistentVectorStoreCompactionTargetBytesTest {

    private static final String TABLE_SPACE = "tstblspace";
    private static final String INDEX_NAME = "tgt";
    private static final String INDEX_UUID = "tgt_idx_uuid";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private PersistentVectorStore createStore(Path tmpDir, FileDataStorageManager dsm) {
        MemoryManager mm = new MemoryManager(64 * 1024 * 1024, 0, 1024 * 1024, 1024 * 1024);
        return new PersistentVectorStore(INDEX_NAME, "testtable", TABLE_SPACE,
                "vector_col", INDEX_UUID, tmpDir, dsm, mm,
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0, Long.MAX_VALUE);
    }

    @Test
    public void defaultIsEightGibibytes() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            assertEquals("default must match the optimizer-pod cap (8 GiB)",
                    8L * 1024L * 1024L * 1024L, store.getCompactionTargetBytes());
        }
    }

    @Test
    public void positiveValuePassesThrough() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionTargetBytes(123_456_789L);
            assertEquals(123_456_789L, store.getCompactionTargetBytes());

            // Also re-tune to a larger value, simulating a live operator change.
            store.setCompactionTargetBytes(64L * 1024L * 1024L * 1024L);
            assertEquals(64L * 1024L * 1024L * 1024L,
                    store.getCompactionTargetBytes());
        }
    }

    @Test
    public void zeroOrNegativeClampsToMaxValueDisablingTheCap() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionTargetBytes(0L);
            assertEquals("0 must clamp to Long.MAX_VALUE (disabled)",
                    Long.MAX_VALUE, store.getCompactionTargetBytes());

            store.setCompactionTargetBytes(-1L);
            assertEquals("negative must clamp to Long.MAX_VALUE (disabled)",
                    Long.MAX_VALUE, store.getCompactionTargetBytes());

            // And an explicit Long.MAX_VALUE is preserved (already disabled).
            store.setCompactionTargetBytes(Long.MAX_VALUE);
            assertEquals(Long.MAX_VALUE, store.getCompactionTargetBytes());
        }
    }

    // ---- maybeLogConvergence: the two operator-visible log branches ----

    private static VectorSegment seg(int id, long sizeBytes) {
        VectorSegment s = new VectorSegment(id);
        s.estimatedSizeBytes = sizeBytes;
        return s;
    }

    /**
     * Below the back-pressure threshold + non-empty snapshot → STEADY_STATE
     * INFO. Pins the positive convergence signal.
     */
    @Test
    public void maybeLogConvergenceEmitsSteadyStateBelowBackpressure() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(100);
            store.setCompactionTargetBytes(8L * 1024L * 1024L * 1024L);

            List<VectorSegment> snapshot = new ArrayList<>();
            // 3 graduated segments — far below the bp threshold of 100.
            for (int i = 1; i <= 3; i++) {
                snapshot.add(seg(i, 16L * 1024L * 1024L * 1024L));
            }
            assertEquals(PersistentVectorStore.ConvergenceLogKind.STEADY_STATE,
                    store.maybeLogConvergence(snapshot, 1_000L));
        }
    }

    /**
     * At or above the back-pressure threshold + non-empty snapshot →
     * BACKPRESSURE_DEADLOCK WARNING. This is the operator-action alert the
     * user explicitly asked for (a stalled cluster: writes blocked, compaction
     * empty). Also pins that the WARNING fires the very first time it is
     * reached after start (no missed-first-alert risk).
     */
    @Test
    public void maybeLogConvergenceEmitsBackpressureDeadlockAtOrAboveThreshold()
            throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(4);
            store.setCompactionTargetBytes(8L * 1024L * 1024L * 1024L);

            List<VectorSegment> snapshot = new ArrayList<>();
            // 4 graduated segments — at the back-pressure threshold (>= 4).
            for (int i = 1; i <= 4; i++) {
                snapshot.add(seg(i, 16L * 1024L * 1024L * 1024L));
            }
            assertEquals("at the bp threshold the deadlock WARNING must fire",
                    PersistentVectorStore.ConvergenceLogKind.BACKPRESSURE_DEADLOCK,
                    store.maybeLogConvergence(snapshot, 1_000L));
        }
    }

    /**
     * Disabling back-pressure (Integer.MAX_VALUE) must suppress the deadlock
     * WARNING regardless of how many segments accumulate — the steady-state
     * INFO is still allowed. This pins the sentinel branch the reviewer
     * flagged.
     */
    @Test
    public void maybeLogConvergenceNeverEmitsDeadlockWhenBackpressureDisabled()
            throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(Integer.MAX_VALUE);

            List<VectorSegment> snapshot = new ArrayList<>();
            for (int i = 1; i <= 1000; i++) {
                snapshot.add(seg(i, 16L * 1024L * 1024L * 1024L));
            }
            // With bp disabled, even a 1000-segment all-graduated index emits
            // the STEADY_STATE line — never the DEADLOCK line.
            assertEquals(
                    PersistentVectorStore.ConvergenceLogKind.STEADY_STATE,
                    store.maybeLogConvergence(snapshot, 1_000L));
        }
    }

    /**
     * 5-minute throttle: a second call within the window must return NONE,
     * a call past the window must re-emit. Both the deadlock-WARN and the
     * steady-state-INFO branches use independent throttles, so this also
     * pins that they do not stomp on each other.
     */
    @Test
    public void maybeLogConvergenceThrottlesRepeatedCalls() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            store.setCompactionBackpressureThreshold(100);

            List<VectorSegment> steady = new ArrayList<>();
            for (int i = 1; i <= 3; i++) {
                steady.add(seg(i, 16L * 1024L * 1024L * 1024L));
            }
            // First call: STEADY_STATE.
            assertEquals(PersistentVectorStore.ConvergenceLogKind.STEADY_STATE,
                    store.maybeLogConvergence(steady, 0L));
            // Immediate re-call within the throttle window: NONE.
            assertEquals(PersistentVectorStore.ConvergenceLogKind.NONE,
                    store.maybeLogConvergence(steady, 1_000L));
            // Past the throttle window: re-emits.
            long pastWindow = PersistentVectorStore.STEADY_STATE_LOG_INTERVAL_NANOS + 1L;
            assertEquals(PersistentVectorStore.ConvergenceLogKind.STEADY_STATE,
                    store.maybeLogConvergence(steady, pastWindow));

            // Independent throttle: a DEADLOCK call right after the
            // STEADY_STATE re-emit must still fire because the deadlock
            // counter has its own AtomicLong.
            store.setCompactionBackpressureThreshold(4);
            List<VectorSegment> overloaded = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                overloaded.add(seg(i, 16L * 1024L * 1024L * 1024L));
            }
            assertEquals("deadlock throttle is independent of steady-state throttle",
                    PersistentVectorStore.ConvergenceLogKind.BACKPRESSURE_DEADLOCK,
                    store.maybeLogConvergence(overloaded, pastWindow + 1L));
        }
    }

    /** Empty snapshot returns NONE — the helper must not emit any log line. */
    @Test
    public void maybeLogConvergenceEmptySnapshotIsNone() throws Exception {
        Path baseDir = tmpFolder.newFolder("data").toPath();
        Path tmpDir = tmpFolder.newFolder("tmp").toPath();
        FileDataStorageManager dsm = new FileDataStorageManager(baseDir);
        dsm.initTablespace(TABLE_SPACE);
        try (PersistentVectorStore store = createStore(tmpDir, dsm)) {
            assertEquals(PersistentVectorStore.ConvergenceLogKind.NONE,
                    store.maybeLogConvergence(new ArrayList<>(), 0L));
        }
    }
}
