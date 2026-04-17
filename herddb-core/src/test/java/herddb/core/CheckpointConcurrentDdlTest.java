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
 * Guards the tables-metadata snapshot written during checkpoint against concurrent DDL.
 *
 * <p>The Phase-B loop in {@link TableSpaceManager#checkpoint(boolean, boolean, boolean, long)}
 * iterates a snapshot of tables taken in Phase A and tolerates a table being dropped mid-iteration
 * (the per-table checkpoint throws and the loop logs a warning). These tests verify end-to-end
 * recovery after CREATE TABLE / DROP TABLE that races a checkpoint.
 */
public class CheckpointConcurrentDdlTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * CREATE TABLE during Phase B: the new table is not iterated by the Phase-B loop for this
     * checkpoint. The checkpoint for the newly-created table happens at the next checkpoint;
     * the table's state must survive a DBManager restart in both cases.
     */
    @Test
    public void testCreateTableDuringPhaseB() throws Exception {
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
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)",
                        Arrays.asList("a" + i, i));
            }
            manager.checkpoint();

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    execute(manager, "CREATE TABLE tblspace1.t2 (k2 string primary key, n2 int)",
                            Collections.emptyList());
                    for (int i = 0; i < 5; i++) {
                        execute(manager, "INSERT INTO tblspace1.t2(k2,n2) values(?,?)",
                                Arrays.asList("b" + i, 100 + i));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("CREATE TABLE during Phase B failed", hookError.get());
            }

            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(null);
            manager.checkpoint();
        }

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long t1Rows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                t1Rows = s.consume().size();
            }
            assertEquals(10, t1Rows);

            long t2Rows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t2", Collections.emptyList())) {
                t2Rows = s.consume().size();
            }
            assertEquals(5, t2Rows);
        }
    }

    /**
     * DROP TABLE during Phase B: the Phase-B loop's existing try/catch must swallow the
     * DataStorageManagerException for the dropped table and let the checkpoint continue.
     * On restart only the surviving table must be present.
     */
    @Test
    public void testDropTableDuringPhaseB() throws Exception {
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
            execute(manager, "CREATE TABLE tblspace1.t2 (k2 string primary key, n2 int)", Collections.emptyList());
            for (int i = 0; i < 10; i++) {
                execute(manager, "INSERT INTO tblspace1.t1(k1,n1) values(?,?)", Arrays.asList("a" + i, i));
                execute(manager, "INSERT INTO tblspace1.t2(k2,n2) values(?,?)", Arrays.asList("b" + i, 100 + i));
            }
            manager.checkpoint();

            AtomicReference<Throwable> hookError = new AtomicReference<>();
            manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(() -> {
                try {
                    // Drop t2 once, via the first firing of the hook.
                    manager.getTableSpaceManager("tblspace1").setAfterTableCheckPointAction(null);
                    execute(manager, "DROP TABLE tblspace1.t2", Collections.emptyList());
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            if (hookError.get() != null) {
                throw new AssertionError("DROP TABLE during Phase B failed", hookError.get());
            }
        }

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            long t1Rows;
            try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t1", Collections.emptyList())) {
                t1Rows = s.consume().size();
            }
            assertEquals(10, t1Rows);

            // t2 must not exist any more.
            try {
                try (DataScanner s = scan(manager, "SELECT * FROM tblspace1.t2", Collections.emptyList())) {
                    s.consume();
                }
                throw new AssertionError("t2 should have been dropped");
            } catch (Exception expected) {
                // table does not exist — expected
            }
        }
    }
}
