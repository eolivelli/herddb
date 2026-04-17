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
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Regression tests for issue #145.
 *
 * <p>Before the fix in {@link TableManager#checkpoint(double, double, long, long, long, boolean, long)},
 * {@link herddb.storage.TableStatus#sequenceNumber} was captured at Phase A and persisted as the
 * per-table {@code bootSequenceNumber}. When a transaction committed during Phase B its pending
 * changes landed in pages flushed by Phase C (the "remainingNewPages" loop), so the on-disk page
 * state reflected entries with LSN &gt; Phase-A — but {@code bootSequenceNumber} still pointed at
 * Phase-A. On recovery, the per-table filter in
 * {@link TableManager#apply(herddb.log.CommitLogResult, herddb.log.LogEntry, boolean)} and
 * {@link TableManager#onTransactionCommit(herddb.model.Transaction, boolean)} did not recognise
 * those entries as already-applied, the COMMIT was replayed, and
 * {@link herddb.core.TableManager#applyInsert(herddb.utils.Bytes, herddb.utils.Bytes, boolean)}
 * threw {@code IllegalStateException: corrupted transaction log: key already present}.
 *
 * <p>These tests use the test-only {@link TableManager#setDuringPhaseBAction(Runnable)} hook to
 * deterministically run DML (including a {@code COMMIT} of an in-flight transaction) while the
 * {@link TableManager#checkpoint(boolean, long)} is between Phase A and Phase B, so the resulting
 * entries end up in pages flushed by Phase C.
 */
public class CheckpointStaleLsnRecoveryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * A transaction committed during Phase B must not produce a duplicate-key error on restart.
     */
    @Test
    public void testRecoveryAfterCommitDuringPhaseB() throws Exception {
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
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            // Start a transaction with pending inserts BEFORE the checkpoint. During Phase B the
            // transaction commits, applies its inserts to main state (which go into newPages), and
            // Phase C flushes those pages to disk. Without the fix, TableStatus.sequenceNumber is
            // the Phase-A snapshot so the flushed-page state disagrees with bootSequenceNumber.
            long tx = beginTransaction(manager, "tblspace1");
            for (int i = 0; i < 5; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("tx" + i, 100 + i),
                        new TransactionContext(tx));
            }

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            AbstractTableManager t1Manager = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            ((TableManager) t1Manager).setDuringPhaseBAction(() -> {
                try {
                    commitTransaction(manager, "tblspace1", tx);
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("commit-during-Phase-B hook failed", hookError.get());
            }
        }

        // Restart: recovery must complete cleanly (no IllegalStateException) and every row must be
        // visible. Without the fix the replay of the COMMIT during recovery re-applies the pending
        // inserts on top of the flushed pages and trips duplicate-key IllegalStateException.
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals(35, total);

            long txRows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1>=100",
                    Collections.emptyList())) {
                txRows = s.consume().size();
            }
            assertEquals(5, txRows);
        }
    }

    /**
     * Autocommit INSERTs executed during Phase B must survive a restart with no duplicate-key
     * errors. The INSERTs land in new pages flushed by Phase C, so their LSN is &gt; Phase-A; on
     * recovery the per-table filter must skip the corresponding log entries.
     */
    @Test
    public void testRecoveryAfterAutocommitDuringPhaseB() throws Exception {
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
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            AbstractTableManager t1Manager = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1");
            ((TableManager) t1Manager).setDuringPhaseBAction(() -> {
                try {
                    for (int i = 0; i < 5; i++) {
                        execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                                Arrays.asList("during" + i, 200 + i));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("autocommit-during-Phase-B hook failed", hookError.get());
            }
        }

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long total;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                total = s.consume().size();
            }
            assertEquals(35, total);
        }
    }
}
