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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression test for issue #431: TableManager.doCheckpoint Phase C StampedLock
 * starvation blocks all DML commits.
 *
 * <p>Before the fix, Phase C's {@code checkpointLock.asWriteLock().tryLock()} request
 * triggered StampedLock writer-priority semantics, which blocked every new
 * {@code checkpointLock.asReadLock().tryLock()} call in {@code onTransactionCommit}.
 * With 50+ concurrent commit threads, all their 300-second timeouts would expire
 * while Phase C was merely waiting for existing readers to drain.</p>
 *
 * <p>The fix (option 4) replaces the checkpoint read lock in {@code onTransactionCommit}
 * with a lightweight gate mechanism ({@code checkpointPhaseCGate} + {@code activeCommitApplies}).
 * Phase C closes the gate and drains the counter before acquiring
 * {@code checkpointLock.asWriteLock()}, so commit threads only spin while the gate is
 * open — a window bounded by Phase C's write-lock hold (typically &lt; 50 ms), not
 * the 300-second tryLock timeout.</p>
 *
 * <p>This test uses the {@code duringPhaseCGateAction} test hook to hold the gate open
 * for {@value #GATE_HOLD_MS} ms while 50 concurrent transactions commit. The assertion
 * verifies that all commits complete well within {@value #COMMIT_DEADLINE_MS} ms — a
 * tight bound that would be impossible without the fix (commits would block for 300 s).</p>
 */
public class Issue431CommitNotStarvedByPhaseCTest {

    /** Duration the gate hook sleeps to simulate slow Phase C write-lock acquisition. */
    private static final long GATE_HOLD_MS = 500L;

    /**
     * All 50 commits must finish within this deadline. Set to 10× {@link #GATE_HOLD_MS}
     * so a partial regression (e.g. restoring a read-lock whose tryLock blocks for the
     * write-lock duration, or whose gate is open far too long) would still trip the bound,
     * while absorbing CI I/O noise (which adds at most a few hundred ms on any runner).
     */
    private static final long COMMIT_DEADLINE_MS = 5_000L;

    /** Number of concurrent commit threads. */
    private static final int COMMIT_THREADS = 50;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void commitsCompleteWithinGateWindow() throws Exception {
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
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("pre" + i, i));
            }
            // Clean first checkpoint (no hook yet).
            manager.checkpoint();

            // Grab the TableManager so we can arm the gate hook.
            TableManager t1Manager = (TableManager) manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");

            /*
             * The gate hook fires AFTER checkpointPhaseCGate is set to true and
             * activeCommitApplies has been drained to zero, but BEFORE Phase C acquires
             * checkpointLock.asWriteLock(). We sleep here to simulate slow write-lock
             * acquisition (e.g. many existing executeStatementAsync readers). During this
             * sleep, any onTransactionCommit call that tries acquireCommitApplySlot() will
             * spin on checkpointPhaseCGate — that is the behaviour we want to measure.
             *
             * After the hook returns, Phase C acquires its write lock (fast), completes the
             * snapshot (fast), releases lock + gate, and commit threads unblock immediately.
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

            // Run checkpoint on a background thread; it will block inside the gate hook.
            AtomicReference<Throwable> ckptError = new AtomicReference<>();
            Thread ckptThread = new Thread(() -> {
                try {
                    manager.checkpoint();
                } catch (Throwable t) {
                    // Broad catch required: this runs on a worker thread and any failure
                    // (checked or unchecked) must be captured for the main thread.
                    // CLAUDE.md: unavoidable broad catch in background thread death guard.
                    ckptError.set(t);
                }
            }, "test-ckpt-thread");
            ckptThread.start();

            // Wait until Phase C has closed the gate and is sleeping.
            assertTrue("gate hook never entered", gateEntered.await(30, TimeUnit.SECONDS));

            // Now spawn COMMIT_THREADS concurrent transactions while the gate is held.
            // Before the fix, each commit would call checkpointLock.asReadLock().tryLock(300 s)
            // and block for 300 s (StampedLock writer-priority blocks new readers the moment
            // Phase C queues its write-lock request, even before acquiring it).
            // After the fix, each commit spins on checkpointPhaseCGate and unblocks
            // as soon as the gate is cleared — at most GATE_HOLD_MS + Phase C write-lock
            // hold (a few ms) after the gate was set.
            ExecutorService pool = Executors.newFixedThreadPool(COMMIT_THREADS);
            List<Future<?>> futures = new ArrayList<>(COMMIT_THREADS);
            AtomicReference<Throwable> commitError = new AtomicReference<>();

            long t0 = System.currentTimeMillis();
            for (int i = 0; i < COMMIT_THREADS; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    try {
                        long tx = beginTransaction(manager, "tblspace1");
                        execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                                Arrays.asList("commit" + idx, idx),
                                new TransactionContext(tx));
                        commitTransaction(manager, "tblspace1", tx);
                    } catch (Throwable t) {
                        // Broad catch: background threads must surface any exception type.
                        commitError.compareAndSet(null, t);
                    }
                }));
            }

            // All commits must finish well within the deadline.
            pool.shutdown();
            boolean allDone = pool.awaitTermination(COMMIT_DEADLINE_MS, TimeUnit.MILLISECONDS);
            long elapsed = System.currentTimeMillis() - t0;

            // On timeout, force-stop worker threads so they don't leak into DBManager.close().
            if (!allDone) {
                pool.shutdownNow();
                pool.awaitTermination(2, TimeUnit.SECONDS);
            }

            ckptThread.join(GATE_HOLD_MS * 6);
            if (ckptError.get() != null) {
                throw new AssertionError("checkpoint thread failed", ckptError.get());
            }

            assertNull("a commit thread threw an unexpected exception", commitError.get());
            assertNull("gate hook threw an unexpected exception", hookError.get());

            // Lower-bound: elapsed must be at least GATE_HOLD_MS - 100 ms to prove that
            // commits were not magically fast (i.e. the gate actually held them for a while).
            assertTrue("commits finished too fast — gate did not actually hold them for "
                            + GATE_HOLD_MS + " ms (elapsed=" + elapsed + " ms)",
                    elapsed >= GATE_HOLD_MS - 100);

            assertTrue(
                    COMMIT_THREADS + " concurrent commits took " + elapsed + " ms, expected < "
                            + COMMIT_DEADLINE_MS + " ms. "
                            + "Issue #431 starvation fix is missing or regressed.",
                    allDone && elapsed < COMMIT_DEADLINE_MS);
        }
    }
}
