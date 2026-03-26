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
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests for concurrent DML with secondary indexes during checkpoint,
 * and recovery after concurrent DML across Memory and File backends.
 */
public class ConcurrentCheckpointWithIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Memory backend: INSERT/UPDATE/DELETE with a HASH secondary index during checkpoint.
     * Verifies that index queries return correct results immediately after the checkpoint
     * (no restart — just verifying correctness under concurrency).
     */
    @Test
    public void testConcurrentDMLWithHashIndexDuringCheckpoint() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 40; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i % 4));
            }
            // Force data onto disk pages
            manager.checkpoint();

            AtomicInteger dmlRound = new AtomicInteger(0);
            AtomicReference<Throwable> dmlError = new AtomicReference<>();

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int r = dmlRound.incrementAndGet();
                    // Insert rows with a known n1 value
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + r, 99));
                    // Update an existing row's indexed column
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=99 where k1=?",
                            Arrays.asList("pre0"));
                    // Delete a row
                    executeUpdate(manager, "DELETE FROM tblspace1.t1 where k1=?",
                            Arrays.asList("pre1"));
                } catch (Throwable t) {
                    dmlError.set(t);
                }
            });

            manager.checkpoint();

            if (dmlError.get() != null) {
                throw new AssertionError("DML with HASH index during checkpoint failed", dmlError.get());
            }

            // Verify: n1=99 rows = 1 updated (pre0) + 1 inserted (during1) = 2
            long countN99;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=99", Collections.emptyList())) {
                countN99 = s.consume().size();
            }
            assertEquals("Expected 2 rows with n1=99 (1 updated + 1 inserted during checkpoint)", 2, countN99);

            // pre1 was deleted — should return 0
            long countPre1;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='pre1'", Collections.emptyList())) {
                countPre1 = s.consume().size();
            }
            assertEquals("pre1 should have been deleted", 0, countPre1);
        }
    }

    /**
     * Memory backend: INSERT/UPDATE/DELETE with a BRIN secondary index during checkpoint.
     */
    @Test
    public void testConcurrentDMLWithBRINIndexDuringCheckpoint() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            execute(manager, "CREATE BRIN INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 40; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            AtomicInteger dmlRound = new AtomicInteger(0);
            AtomicReference<Throwable> dmlError = new AtomicReference<>();

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int r = dmlRound.incrementAndGet();
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + r, 500 + r));
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=501 where k1=?",
                            Arrays.asList("pre5"));
                    executeUpdate(manager, "DELETE FROM tblspace1.t1 where k1=?",
                            Arrays.asList("pre10"));
                } catch (Throwable t) {
                    dmlError.set(t);
                }
            });

            manager.checkpoint();

            if (dmlError.get() != null) {
                throw new AssertionError("DML with BRIN index during checkpoint failed", dmlError.get());
            }

            // n1=501: pre5 (updated) + during1 (inserted with n1=501 after 500+1)
            // Actually during1 has n1=501, pre5 was updated to 501 -> 2 rows
            long countN501;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=501", Collections.emptyList())) {
                countN501 = s.consume().size();
            }
            assertEquals("Expected 2 rows with n1=501", 2, countN501);

            long countPre10;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='pre10'", Collections.emptyList())) {
                countPre10 = s.consume().size();
            }
            assertEquals("pre10 should have been deleted", 0, countPre10);
        }
    }

    /**
     * File backend: rows inserted during checkpoint survive a restart with HASH index intact.
     */
    @Test
    public void testRecoveryWithHashIndexAfterConcurrentCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";
        int insertsDuringCheckpoint;

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
            execute(manager, "CREATE INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 50; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, 1));
            }
            manager.checkpoint();

            AtomicInteger concurrentInserts = new AtomicInteger(0);
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int v = concurrentInserts.incrementAndGet();
                    // Insert with n1=2 so we can query by index after restart
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + v, 2));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            manager.checkpoint();
            insertsDuringCheckpoint = concurrentInserts.get();
        }

        // Restart and verify both data and HASH index are consistent
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            // Total rows
            long totalCount;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                totalCount = s.consume().size();
            }
            assertEquals(50 + insertsDuringCheckpoint, totalCount);

            // Rows with n1=1 (pre-inserts)
            long countN1;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=1", Collections.emptyList())) {
                countN1 = s.consume().size();
            }
            assertEquals(50, countN1);

            // Rows with n1=2 (inserted during checkpoint — must survive restart)
            long countN2;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=2", Collections.emptyList())) {
                countN2 = s.consume().size();
            }
            assertEquals(insertsDuringCheckpoint, countN2);
        }
    }

    /**
     * File backend: rows inserted during checkpoint survive a restart with BRIN index intact.
     */
    @Test
    public void testRecoveryWithBRINIndexAfterConcurrentCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        String nodeId = "localhost";
        int insertsDuringCheckpoint;

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
            execute(manager, "CREATE BRIN INDEX idx_n1 ON tblspace1.t1(n1)", Collections.emptyList());
            for (int i = 0; i < 50; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, 1));
            }
            manager.checkpoint();

            AtomicInteger concurrentInserts = new AtomicInteger(0);
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int v = concurrentInserts.incrementAndGet();
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + v, 2));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            manager.checkpoint();
            insertsDuringCheckpoint = concurrentInserts.get();
        }

        // Restart and verify data + BRIN index
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long totalCount;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                totalCount = s.consume().size();
            }
            assertEquals(50 + insertsDuringCheckpoint, totalCount);

            long countN2;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE n1=2", Collections.emptyList())) {
                countN2 = s.consume().size();
            }
            assertEquals("Concurrent inserts should survive restart with BRIN index", insertsDuringCheckpoint, countN2);
        }
    }

    /**
     * Memory backend: transaction started before checkpoint, committed after — data must be visible.
     */
    @Test
    public void testTransactionSpanningCheckpoint() throws Exception {
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

            execute(manager, "CREATE TABLE tblspace1.t1 (k1 string primary key, n1 int)", Collections.emptyList());
            for (int i = 0; i < 30; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            // Begin a transaction before checkpoint
            long txId = TestUtils.beginTransaction(manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(txId);

            execute(manager, "tblspace1",
                    "INSERT INTO tblspace1.t1(k1,n1) values('tx_row', 999)",
                    Collections.emptyList(), txCtx);

            // Checkpoint while transaction is open
            manager.checkpoint();

            // Commit after checkpoint
            TestUtils.commitTransaction(manager, "tblspace1", txId);

            // Verify the transactional row is visible
            long count;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1 WHERE k1='tx_row'", Collections.emptyList())) {
                count = s.consume().size();
            }
            assertEquals("Transaction committed after checkpoint must be visible", 1, count);
        }
    }
}
