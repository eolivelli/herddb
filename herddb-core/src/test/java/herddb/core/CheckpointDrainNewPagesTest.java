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
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression tests for the Phase C newPages drain introduced to unblock commits during
 * long checkpoints (follow-up to issue #148).
 *
 * <p>Before the fix, {@link TableManager#checkpoint(boolean, long)} Phase C held
 * {@code checkpointLock.asWriteLock()} while flushing every page that had accumulated in
 * {@link TableManager#newPages} during Phase B and {@code indexManager.checkpoint} — which
 * in production grew to thousands of pages during a long {@code waitForCatchUp}. Concurrent
 * commits timed out on {@link TableManager#onTransactionCommit(herddb.model.Transaction, boolean)}'s
 * read-side {@code tryLock}.
 *
 * <p>The fix adds {@link TableManager#drainPendingNewPages()}: a brief write-lock snapshot
 * + rotate of {@code currentDirtyRecordsPage}, followed by a flush of the snapshot
 * <strong>outside</strong> the checkpoint write lock.
 *
 * <p>These tests use the test-only {@link TableManager#setDuringPhaseBAction(Runnable)}
 * hook to deterministically accumulate newPages during Phase B, then verify:
 *
 * <ul>
 *   <li>the drain actually fires and flushes pages outside the lock
 *       (via {@link TableManager#getDrainedNewPagesOutsideLock()});</li>
 *   <li>the issue #46 invariant still holds: {@code newPages} is empty before
 *       {@code keyToPage.checkpoint()} so every recovered row is in {@code activePages}
 *       and on disk (a broken drain would resurface as recovery failures);</li>
 *   <li>Phase C's final flush pass still handles the residual (pages created between
 *       the drain unlock and Phase C's lock acquire).</li>
 * </ul>
 */
public class CheckpointDrainNewPagesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Many autocommit INSERTs during Phase B fill newPages beyond a single DML page. The
     * drain must absorb them outside the write lock — visible via the test-only counter —
     * and every row must still be readable after restart.
     */
    @Test
    public void testDrainFlushesPagesAccumulatedDuringPhaseB() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";

        // Small page size so a handful of inserts force allocateLivePage to roll a page.
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_MAX_LOGICAL_PAGE_SIZE, 1024);

        final int preRows = 10;
        final int hookInserts = 50;
        long drainedPages;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, s1 string, n1 int)",
                    Collections.emptyList());
            for (int i = 0; i < preRows; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,s1,n1) values(?,?,?)",
                        Arrays.asList("pre" + i, "payload-padding-to-inflate-page-size", i));
            }
            manager.checkpoint();

            // Capture TableManager so we can arm the hook + read the drain counter.
            TableManager t1Manager = (TableManager) manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            long drainedBefore = t1Manager.getDrainedNewPagesOutsideLock();

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            t1Manager.setDuringPhaseBAction(() -> {
                try {
                    // Keep the hook one-shot; if we fire during every per-table phase-B
                    // the recursion gets confusing.
                    t1Manager.setDuringPhaseBAction(null);
                    for (int i = 0; i < hookInserts; i++) {
                        execute(manager, "INSERT INTO tblspace1.t1(k1,s1,n1) values(?,?,?)",
                                Arrays.asList("during" + i, "payload-padding-to-inflate-page-size",
                                        1000 + i));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("insert-during-Phase-B hook failed", hookError.get());
            }

            drainedPages = t1Manager.getDrainedNewPagesOutsideLock() - drainedBefore;
        }

        // The drain must have fired: with 50 inserts at ~1KB page size, at least a couple of
        // pages must have ended up in newPages between Phase A's rotate and the drain call.
        assertTrue("expected drainPendingNewPages() to have flushed at least one page "
                + "outside the write lock, saw " + drainedPages, drainedPages > 0);

        // Recovery must find every row — the drain would be worthless if it broke
        // the issue #46 invariant (keyToPage pointing at unflushed pages).
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1",
                    Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals(preRows + hookInserts, total);

            long duringRows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1>=1000",
                    Collections.emptyList())) {
                duringRows = s.consume().size();
            }
            assertEquals(hookInserts, duringRows);
        }
    }

    /**
     * The drain must be a no-op when {@code newPages} is already empty at the drain call.
     * Using a memory-only setup with no Phase-B DML: Phase A rotates a fresh current page
     * into {@code newPages} so the drain snapshot always contains at least that one page —
     * but nothing should be left behind for Phase C to flush.
     */
    @Test
    public void testDrainIsHarmlessWithoutPhaseBActivity() throws Exception {
        String nodeId = "localhost";
        try (DBManager manager = new DBManager(nodeId,
                new MemoryMetadataStorageManager(), new MemoryDataStorageManager(),
                new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1",
                    Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            for (int i = 0; i < 5; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            // Two back-to-back checkpoints with no DML in between. The second must be a
            // clean no-op: if drain or the final-flush pass double-flushed or corrupted
            // newPages the row count would diverge or recovery would fail.
            manager.checkpoint();
            manager.checkpoint();

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1",
                    Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals(5, total);
        }
    }
}
