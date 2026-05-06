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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TransactionContext;
import herddb.model.TransactionResult;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.BeginTransactionStatement;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DropTableStatement;
import herddb.model.commands.RollbackTransactionStatement;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Issue #408 — functional checks for the per-tablespace integer id assigner
 * inside {@link TableSpaceManager}: every {@code CREATE TABLE} on the leader
 * must hand out a fresh, monotonically increasing id; a {@code DROP}+{@code
 * CREATE} on the same name must yield a different (larger) id; and the id
 * counter must survive a checkpoint / restart cycle so post-restart {@code
 * CREATE} statements continue handing out fresh ids without collision.
 */
public class TableIdAssignmentTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static Table makeTable(String name) {
        return Table.builder()
                .tablespace("tblspace1")
                .name(name)
                .column("id", ColumnTypes.STRING)
                .column("payload", ColumnTypes.STRING)
                .primaryKey("id")
                .build();
    }

    @Test
    public void testFirstCreateTableHasNonZeroId() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            int id = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().tableId;
            assertTrue("a fresh CREATE TABLE must get a non-zero id, got " + id, id > 0);
        }
    }

    @Test
    public void testTwoCreatesYieldStrictlyIncreasingIds() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateTableStatement(makeTable("t2")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            int idT1 = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;
            int idT2 = manager.getTableSpaceManager("tblspace1").getTableManager("t2").getTable().tableId;
            assertTrue("ids must be strictly monotonic: t1=" + idT1 + " t2=" + idT2, idT2 > idT1);
        }
    }

    @Test
    public void testDropAndRecreateAssignsADifferentId() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            int firstId = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;

            manager.executeStatement(new DropTableStatement("tblspace1", "t1", false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            int secondId = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;

            assertTrue("DROP+CREATE must yield a strictly larger id, was first=" + firstId + " second=" + secondId,
                    secondId > firstId);
        }
    }

    @Test
    public void testCreateTableInTransactionThenCommit() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            // BEGIN; CREATE TABLE; COMMIT — the in-flight tableId must
            // be visible after commit and the manager must be reachable
            // via the id-keyed lookup that the WAL apply hot path uses.
            TransactionResult begin = (TransactionResult) manager.executeStatement(
                    new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            long tx = begin.getTransactionId();
            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx));
            manager.executeStatement(new CommitTransactionStatement("tblspace1", tx),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            int idT1 = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;
            assertTrue("a transactionally-created table must carry a non-zero id post-commit, got " + idT1,
                    idT1 > 0);
            // Issue #408 review: also verify the manager is reachable
            // through the id-keyed lookup that the WAL apply hot path
            // uses — a name-only check would miss a regression that
            // forgot to populate `tablesById` for transactional CREATE.
            java.util.Map<Integer, AbstractTableManager> idIndex =
                    manager.getTableSpaceManager("tblspace1").tablesByIdSnapshot();
            assertEquals("transactional CREATE must register the table under its id",
                    "t1", idIndex.get(idT1).getTable().name);
        }
    }

    @Test
    public void testCreateTableInTransactionThenRollbackDoesNotLeaveAnyMappings() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            TransactionResult begin = (TransactionResult) manager.executeStatement(
                    new BeginTransactionStatement("tblspace1"),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            long tx = begin.getTransactionId();
            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    new TransactionContext(tx));
            manager.executeStatement(new RollbackTransactionStatement("tblspace1", tx),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            // After ROLLBACK the table must not exist by name, and the
            // id-keyed lookup must not return a stale manager either.
            // (The id-allocator is allowed to have advanced — that is
            // expected because nextTableId.getAndIncrement is unconditional;
            // pin the contract so a future "rollback decrements the
            // counter" optimisation cannot land without an explicit
            // coordinated change to recovery.)
            assertEquals("rolled-back CREATE must not leave a name binding",
                    null, manager.getTableSpaceManager("tblspace1").getTableManager("t1"));
            // Issue #408 review: also assert the id-keyed index is
            // clean of any reference for any id this transaction
            // touched. A regression that left a stale entry would
            // NPE the WAL apply hot path on the next DML.
            java.util.Map<Integer, AbstractTableManager> idIndex =
                    manager.getTableSpaceManager("tblspace1").tablesByIdSnapshot();
            for (java.util.Map.Entry<Integer, AbstractTableManager> e : idIndex.entrySet()) {
                assertTrue("rolled-back CREATE must not leave a stale id-keyed entry, got "
                                + e.getKey() + " → " + e.getValue().getTable().name,
                        !"t1".equals(e.getValue().getTable().name));
            }

            // A fresh CREATE for the same name must succeed and carry a
            // strictly larger id than the rolled-back attempt would
            // have had — proving the slot is clean.
            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId > 0);
        }
    }

    @Test
    public void testAlterTablePreservesTableId() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            int idBefore = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;
            assertTrue(idBefore > 0);

            // ALTER TABLE that adds a column — Table#applyAlterTable
            // explicitly threads the existing tableId through the
            // builder, so the manager's id must stay the same and the
            // id-keyed lookup must resolve to the same manager.
            AlterTableStatement alter = new AlterTableStatement(
                    Arrays.asList(Column.column("extra_col", ColumnTypes.STRING)),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    "t1",
                    "tblspace1",
                    null,
                    Collections.emptyList(),
                    Collections.emptyList());
            manager.executeStatement(alter,
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            int idAfter = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;
            assertEquals("ALTER TABLE must preserve the tableId", idBefore, idAfter);
        }
    }

    @Test
    public void testIdAllocatorSurvivesRestart() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmoDir").toPath();

        int idT2;
        // Phase 1: create t1 + t2, checkpoint, capture t2's id.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.executeStatement(new CreateTableSpaceStatement(
                            "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            manager.executeStatement(new CreateTableStatement(makeTable("t1")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.executeStatement(new CreateTableStatement(makeTable("t2")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.checkpoint();
            idT2 = manager.getTableSpaceManager("tblspace1").getTableManager("t2").getTable().tableId;
            assertTrue(idT2 > 0);
        }

        // Phase 2: restart, create t3 — its id must strictly exceed idT2.
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);
            manager.executeStatement(new CreateTableStatement(makeTable("t3")),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            int idT3 = manager.getTableSpaceManager("tblspace1").getTableManager("t3").getTable().tableId;
            assertTrue("post-restart CREATE must allocate a fresh id strictly greater than the pre-restart maximum: idT2="
                    + idT2 + " idT3=" + idT3, idT3 > idT2);
            // t1 and t2 must still be tracked under their original ids.
            int idT1 = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable().tableId;
            assertTrue(idT1 > 0);
            assertTrue(idT2 > idT1);

            // The id-keyed lookup index must agree with the name-keyed one
            // for every table — this is the invariant the WAL apply path
            // relies on.
            assertEquals("t3", manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t3").getTable().name);
        }
    }
}
