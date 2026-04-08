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

import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
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
 * Tests for recovery after concurrent DML during checkpoint with indexes.
 * These exercise the PageNotFoundException recovery paths in TableManager:
 * <ul>
 *   <li>applyUpdate during recovery when a data page was not flushed (line ~2415)</li>
 *   <li>applyDelete during recovery (non-transactional path, line ~2160)</li>
 *   <li>applyUpdate during transaction commit recovery (line ~2054)</li>
 *   <li>applyDelete during transaction commit recovery (line ~2070)</li>
 * </ul>
 */
public class CheckpointRecoveryWithIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * UPDATE during checkpoint Phase B with a non-unique index, then restart.
     * Exercises the PageNotFoundException recovery path in apply(UPDATE) at WAL replay.
     */
    @Test
    public void testUpdateDuringCheckpointRecoveryWithIndex() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int, v1 string)",
                    Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 20; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1,v1) values(?,?,?)",
                        Arrays.asList("key" + i, i % 5, "initial"));
            }
            manager.checkpoint();

            AtomicReference<Throwable> error = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    // Update indexed column and value during checkpoint Phase B.
                    // The page holding "key0" may not be flushed yet at recovery time.
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=99, v1='updated' where k1=?",
                            Arrays.asList("key0"));
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=99, v1='updated' where k1=?",
                            Arrays.asList("key5"));
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            manager.checkpoint();
            if (error.get() != null) {
                throw new AssertionError("DML during checkpoint failed", error.get());
            }
        }

        // Restart — WAL replay will invoke applyUpdate for the two updates
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            // Both updates should have been recovered
            try (DataScanner s = scan(manager, "SELECT v1 FROM tblspace1.t1 WHERE k1='key0'",
                    Collections.emptyList())) {
                assertEquals("updated", s.consume().get(0).get("v1").toString());
            }
            try (DataScanner s = scan(manager, "SELECT v1 FROM tblspace1.t1 WHERE k1='key5'",
                    Collections.emptyList())) {
                assertEquals("updated", s.consume().get(0).get("v1").toString());
            }
            // Index query should also work
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=99",
                    Collections.emptyList())) {
                assertEquals(2, s.consume().size());
            }
            // Total row count preserved
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(20, s.consume().size());
            }
        }
    }

    /**
     * DELETE during checkpoint Phase B with a non-unique index, then restart.
     * Exercises the PageNotFoundException recovery path in apply(DELETE) at WAL replay.
     */
    @Test
    public void testDeleteDuringCheckpointRecoveryWithIndex() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 20; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("key" + i, i % 5));
            }
            manager.checkpoint();

            AtomicReference<Throwable> error = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    executeUpdate(manager, "DELETE FROM tblspace1.t1 where k1=?",
                            Arrays.asList("key3"));
                    executeUpdate(manager, "DELETE FROM tblspace1.t1 where k1=?",
                            Arrays.asList("key8"));
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            manager.checkpoint();
            if (error.get() != null) {
                throw new AssertionError("DML during checkpoint failed", error.get());
            }
        }

        // Restart — WAL replay will invoke applyDelete
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            // Deleted rows should not be present
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='key3'",
                    Collections.emptyList())) {
                assertEquals(0, s.consume().size());
            }
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='key8'",
                    Collections.emptyList())) {
                assertEquals(0, s.consume().size());
            }
            // Total count: 20 - 2 = 18
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(18, s.consume().size());
            }
        }
    }

    /**
     * Transaction with UPDATE committed during checkpoint Phase B, then restart.
     * Exercises the PageNotFoundException recovery in onTransactionCommit → applyUpdate.
     */
    @Test
    public void testTransactionUpdateDuringCheckpointRecoveryWithIndex() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int, v1 string)",
                    Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 20; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1,v1) values(?,?,?)",
                        Arrays.asList("key" + i, i % 5, "initial"));
            }
            manager.checkpoint();

            AtomicReference<Throwable> error = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    // Begin transaction, update, commit — all during Phase B
                    long txId = TestUtils.beginTransaction(manager, "tblspace1");
                    TransactionContext txCtx = new TransactionContext(txId);
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=88, v1='tx_updated' where k1=?",
                            Arrays.asList("key2"), txCtx);
                    commitTransaction(manager, "tblspace1", txId);
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            manager.checkpoint();
            if (error.get() != null) {
                throw new AssertionError("Transaction DML during checkpoint failed", error.get());
            }
        }

        // Restart — recovery replays the transaction commit
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            try (DataScanner s = scan(manager, "SELECT v1 FROM tblspace1.t1 WHERE k1='key2'",
                    Collections.emptyList())) {
                assertEquals("tx_updated", s.consume().get(0).get("v1").toString());
            }
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=88",
                    Collections.emptyList())) {
                assertEquals(1, s.consume().size());
            }
        }
    }

    /**
     * Transaction with DELETE committed during checkpoint Phase B, then restart.
     * Exercises the PageNotFoundException recovery in onTransactionCommit → applyDelete.
     */
    @Test
    public void testTransactionDeleteDuringCheckpointRecoveryWithIndex() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)",
                    Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 20; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("key" + i, i % 5));
            }
            manager.checkpoint();

            AtomicReference<Throwable> error = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    long txId = TestUtils.beginTransaction(manager, "tblspace1");
                    TransactionContext txCtx = new TransactionContext(txId);
                    executeUpdate(manager, "DELETE FROM tblspace1.t1 where k1=?",
                            Arrays.asList("key7"), txCtx);
                    commitTransaction(manager, "tblspace1", txId);
                } catch (Throwable t) {
                    error.set(t);
                }
            });
            manager.checkpoint();
            if (error.get() != null) {
                throw new AssertionError("Transaction DML during checkpoint failed", error.get());
            }
        }

        // Restart — recovery replays the transaction commit with delete
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='key7'",
                    Collections.emptyList())) {
                assertEquals(0, s.consume().size());
            }
            // 20 - 1 = 19
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                assertEquals(19, s.consume().size());
            }
        }
    }
}
