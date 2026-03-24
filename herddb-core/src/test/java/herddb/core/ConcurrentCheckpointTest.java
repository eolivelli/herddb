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
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that DML can proceed concurrently with checkpoint, and that recovery
 * is correct after concurrent DML during checkpoint.
 */
public class ConcurrentCheckpointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * INSERT/UPDATE/SELECT operations must not block while a checkpoint iterates over table checkpoints.
     * Uses afterTableCheckPointAction hook to inject DML in between table checkpoints.
     */
    @Test
    public void testCheckpointAllowsConcurrentDML() throws Exception {
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
            execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("a", 1));
            execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("b", 2));
            execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("c", 3));

            // Force data onto disk pages
            manager.checkpoint();

            AtomicInteger dmlCount = new AtomicInteger(0);
            AtomicReference<Throwable> dmlError = new AtomicReference<>();

            // Set hook: after each table checkpoint, perform DML and verify it is not blocked
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    // These should complete without blocking (checkpoint holds no tablespace write lock here)
                    executeUpdate(manager, "UPDATE tblspace1.t1 set n1=? where k1=?",
                            Arrays.asList(dmlCount.incrementAndGet(), "a"));
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("hook" + dmlCount.get(), 99));
                } catch (Throwable t) {
                    dmlError.set(t);
                }
            });

            manager.checkpoint();

            if (dmlError.get() != null) {
                throw new AssertionError("DML during checkpoint failed", dmlError.get());
            }
            // hook should have fired at least once (one non-system table)
            assertEquals(1, dmlCount.get());
        }
    }

    /**
     * Two concurrent checkpoint calls: the second must be skipped (not queued) while the first runs.
     * Verified by counting how many times the afterTableCheckPointAction fires: exactly 1 (one non-system
     * table), not 2 — because the second call exits immediately via checkpointMutex.tryLock().
     */
    @Test
    public void testConcurrentCheckpointSkipped() throws Exception {
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
            for (int i = 0; i < 100; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("k" + i, i));
            }
            manager.checkpoint();

            CountDownLatch firstCheckpointInProgress = new CountDownLatch(1);
            CountDownLatch firstCheckpointMayFinish = new CountDownLatch(1);
            AtomicInteger tableCheckpointCount = new AtomicInteger(0);

            // Hook: signal that first checkpoint is in its per-table phase, then hold until released
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                tableCheckpointCount.incrementAndGet();
                firstCheckpointInProgress.countDown();
                try {
                    firstCheckpointMayFinish.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            ExecutorService pool = Executors.newFixedThreadPool(2);
            Future<?> first = pool.submit(() -> {
                try {
                    manager.getTableSpaceManager("tblspace1").checkpoint(false, false, false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Wait until first is in per-table phase (holding checkpointMutex), then fire second
            firstCheckpointInProgress.await(5, TimeUnit.SECONDS);
            Future<?> second = pool.submit(() -> {
                try {
                    manager.getTableSpaceManager("tblspace1").checkpoint(false, false, false);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Let second complete (should skip immediately), then unblock first
            second.get(5, TimeUnit.SECONDS);
            firstCheckpointMayFinish.countDown();
            first.get(5, TimeUnit.SECONDS);

            pool.shutdown();

            // Only 1 table checkpoint should have fired (from the first call); skipped call fired nothing
            assertEquals("Only one checkpoint should have done per-table work", 1, tableCheckpointCount.get());
        }
    }

    /**
     * Dropping a table while its checkpoint is running must not cause a crash.
     * The checkpoint loop must log a warning and continue.
     */
    @Test
    public void testCheckpointDuringTableDrop() throws Exception {
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
            execute(manager, "CREATE TABLE tblspace1.t2 (k1 string primary key, n1 int)", Collections.emptyList());
            for (int i = 0; i < 50; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("k" + i, i));
                execute(manager, "INSERT INTO tblspace1.t2(k1,n1) values(?,?)", Arrays.asList("k" + i, i));
            }
            manager.checkpoint();

            AtomicReference<Throwable> dropError = new AtomicReference<>();

            // After t1 checkpoints, drop t2 so it is gone when the checkpoint loop reaches it
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(null); // one-shot
                try {
                    execute(manager, "DROP TABLE tblspace1.t2", Collections.emptyList());
                } catch (Throwable t) {
                    dropError.set(t);
                }
            });

            // Must not throw — the dropped table checkpoint is caught internally
            manager.checkpoint();

            if (dropError.get() != null) {
                throw new AssertionError("DROP TABLE during checkpoint failed unexpectedly", dropError.get());
            }
        }
    }

    /**
     * Inserts performed during checkpoint (in the afterTableCheckPointAction hook) must survive
     * a restart — verifying that the recovery LSN is set correctly from Phase A.
     */
    @Test
    public void testRecoveryAfterConcurrentDML() throws Exception {
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
            for (int i = 0; i < 50; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("pre" + i, i));
            }
            manager.checkpoint();

            AtomicInteger concurrentInserts = new AtomicInteger(0);
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    int v = concurrentInserts.incrementAndGet();
                    execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                            Arrays.asList("during" + v, v));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            manager.checkpoint();
            insertsDuringCheckpoint = concurrentInserts.get();
        }

        // Restart and verify all rows (pre-checkpoint + inserted during checkpoint) are present
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long count;
            try (herddb.model.DataScanner scanner = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                count = scanner.consume().size();
            }
            assertEquals(50 + insertsDuringCheckpoint, count);
        }
    }
}
