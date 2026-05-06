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

package herddb.core;

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.codec.RecordSerializer;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.Record;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #431: verifies that {@link TableManager#writeFromDump}
 * correctly uses the {@code checkpointPhaseCGate} + {@code activeCommitApplies} mechanism
 * (via {@code acquireCommitApplySlot()}) rather than {@code checkpointLock.asReadLock()}.
 *
 * <p>If a future change accidentally replaces {@code acquireCommitApplySlot()} in
 * {@code writeFromDump} with {@code checkpointLock.asReadLock().tryLock(300s)}, the
 * StampedLock writer-priority starvation would reappear: Phase C's write-lock queue
 * entry would block all new read-lock attempts for up to 300 s. This test would then
 * fail its {@value #WFD_DEADLINE_MS}-ms deadline, catching the regression.</p>
 *
 * <p>The test uses the {@code duringPhaseCGateAction} hook to hold the gate open for
 * {@value #GATE_HOLD_MS} ms while a {@code writeFromDump} call is made. The assertion
 * verifies that the call completes within {@value #WFD_DEADLINE_MS} ms — a tight bound
 * that rules out the 300 s StampedLock starvation while absorbing CI I/O noise.</p>
 */
public class WriteFromDumpPhaseCGateTest {

    /** Duration the gate hook sleeps to simulate slow Phase C write-lock acquisition. */
    private static final long GATE_HOLD_MS = 500L;

    /**
     * writeFromDump must complete within this deadline.
     * Set to 10× {@link #GATE_HOLD_MS} so partial regressions still trip it,
     * while absorbing CI I/O noise.
     */
    private static final long WFD_DEADLINE_MS = 5_000L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void writeFromDumpCompletesWithinGateWindow() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10_000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());

            // Pre-populate a few rows so checkpoint has pages to flush.
            for (int i = 0; i < 20; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("pre" + i, i));
            }
            // Clean first checkpoint (no hook yet).
            manager.checkpoint();

            // Grab the TableManager and Table so we can arm the hook and build records.
            TableManager t1Manager = (TableManager) manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            Table table = t1Manager.getTable();

            // Build the dump records we will inject during the gate hold.
            List<Record> dumpRecords = Arrays.asList(
                    RecordSerializer.makeRecord(table, "k1", "wfd0", "n1", 100),
                    RecordSerializer.makeRecord(table, "k1", "wfd1", "n1", 101)
            );

            /*
             * The gate hook fires AFTER checkpointPhaseCGate is set to true and
             * activeCommitApplies has drained to zero, but BEFORE Phase C acquires
             * checkpointLock.asWriteLock(). We sleep to simulate slow write-lock
             * acquisition. During this sleep, writeFromDump must spin on the gate
             * (not on checkpointLock) — that is what we measure.
             */
            CountDownLatch gateEntered = new CountDownLatch(1);
            AtomicReference<Throwable> hookError = new AtomicReference<>();
            t1Manager.setDuringPhaseCGateAction(() -> {
                try {
                    t1Manager.setDuringPhaseCGateAction(null); // one-shot
                    gateEntered.countDown();
                    Thread.sleep(GATE_HOLD_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    hookError.set(e);
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            // Run checkpoint on a background thread; it blocks inside the gate hook.
            AtomicReference<Throwable> ckptError = new AtomicReference<>();
            Thread ckptThread = new Thread(() -> {
                try {
                    manager.checkpoint();
                } catch (Throwable t) {
                    // Broad catch: this runs on a worker thread and any failure
                    // (checked or unchecked) must be captured for the main thread.
                    // CLAUDE.md: unavoidable broad catch in background thread death guard.
                    ckptError.set(t);
                }
            }, "test-ckpt-thread");
            ckptThread.start();

            // Wait until the gate hook is sleeping (gate is held open).
            assertTrue("gate hook never entered", gateEntered.await(30, TimeUnit.SECONDS));

            // Call writeFromDump while the gate is held. Before the fix, this would call
            // checkpointLock.asReadLock().tryLock(300 s) and stall for ~300 s because the
            // StampedLock write-lock request (queued by Phase C) blocks all new readers.
            // After the fix, writeFromDump spins on checkpointPhaseCGate and unblocks
            // within GATE_HOLD_MS + Phase C critical-section (a few ms).
            AtomicLong wfdElapsedMs = new AtomicLong(-1);
            AtomicReference<Throwable> wfdError = new AtomicReference<>();
            Thread wfdThread = new Thread(() -> {
                try {
                    long t0 = System.currentTimeMillis();
                    t1Manager.writeFromDump(dumpRecords);
                    wfdElapsedMs.set(System.currentTimeMillis() - t0);
                } catch (Throwable t) {
                    // Broad catch: background thread must surface any exception type.
                    wfdError.set(t);
                }
            }, "test-wfd-thread");

            long wallT0 = System.currentTimeMillis();
            wfdThread.start();

            // writeFromDump must complete within WFD_DEADLINE_MS.
            wfdThread.join(WFD_DEADLINE_MS);
            long wallElapsed = System.currentTimeMillis() - wallT0;

            // Force-interrupt if still running to avoid leaking into DBManager.close().
            if (wfdThread.isAlive()) {
                wfdThread.interrupt();
                wfdThread.join(2_000);
            }

            ckptThread.join(GATE_HOLD_MS * 6);
            if (ckptError.get() != null) {
                throw new AssertionError("checkpoint thread failed", ckptError.get());
            }

            assertNull("writeFromDump threw an unexpected exception", wfdError.get());
            assertNull("gate hook threw an unexpected exception", hookError.get());

            // Lower-bound: the gate must have actually held writeFromDump for at least
            // most of GATE_HOLD_MS (i.e. it didn't slip through before the gate was set).
            assertTrue("writeFromDump finished too fast — gate did not hold it "
                            + "(wallElapsed=" + wallElapsed + " ms, GATE_HOLD_MS=" + GATE_HOLD_MS + ")",
                    wallElapsed >= GATE_HOLD_MS - 100);

            assertTrue(
                    "writeFromDump took " + wallElapsed + " ms while gate was held for "
                            + GATE_HOLD_MS + " ms; expected to complete within "
                            + WFD_DEADLINE_MS + " ms after gate release. "
                            + "Issue #431 starvation fix may have regressed in the writeFromDump path.",
                    !wfdThread.isAlive() && wallElapsed < WFD_DEADLINE_MS);
        }
    }
}
