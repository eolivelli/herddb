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
package herddb.sql;

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.DBManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Verifies the issue #471 contract that a {@code CREATE VECTOR INDEX} on a
 * non-empty table inside a transaction must be rejected.
 *
 * <p>Rationale: the rebuild requires a single-table checkpoint of the live
 * data plus a server-side log entry that carries {@code rebuild=true}. A
 * checkpoint cannot be tied to a transaction's commit/rollback boundary —
 * if the transaction rolled back after the checkpoint+log entry were
 * written, the IS would still see a CREATE_INDEX on data that was never
 * meant to be visible. Refusing early with a clear error is the only safe
 * option.
 *
 * <p>Counter-tests verify that the rejection is narrowly scoped:
 * <ul>
 *   <li>Empty table inside a transaction with a vector index → allowed
 *       (no rebuild needed, no checkpoint to take).</li>
 *   <li>Non-empty table inside a transaction with a hash/brin index →
 *       allowed (their rebuild path does not need our server-side
 *       checkpoint).</li>
 *   <li>Non-empty table outside any transaction with a vector index →
 *       allowed (the happy path).</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class TransactionalCreateVectorIndexRejectionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Class-wide timeout (60 s) — see {@link CreateVectorIndexRebuildPropertyTest}
     * for the rationale: a tablespace-lock leak from the rejection path
     * would otherwise hang on {@code commitTransaction} indefinitely.
     */
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    private void bootstrapTablespaceAndTable(DBManager manager) throws Exception {
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager,
                "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null, n int)",
                Collections.emptyList());
    }

    @Test
    public void nonEmptyTable_vectorIndex_inTransaction_isRejected() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // Insert rows OUTSIDE the transaction so the table is non-empty
            // by the time the transactional CREATE INDEX runs.
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}, 1));

            long tx = beginTransaction(manager, "tblspace1");
            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList(), new TransactionContext(tx));
                fail("transactional CREATE VECTOR INDEX on non-empty table must be rejected");
            } catch (StatementExecutionException ex) {
                String msg = ex.getMessage() == null ? "" : ex.getMessage();
                assertTrue("error must mention the rejection rationale, got: " + msg,
                        msg.contains("CREATE VECTOR INDEX")
                                && msg.contains("non-empty")
                                && msg.contains("transaction"));
                // The table identity should be in the message so an
                // operator can find the offending statement quickly.
                assertTrue("error must identify the table, got: " + msg,
                        msg.contains("tblspace1.t1") || msg.contains("\"t1\"") || msg.contains("t1"));
            }
            // The transaction itself can still be committed (no DML was
            // accepted past the rejection point).
            commitTransaction(manager, "tblspace1", tx);
            // No index was created.
            assertTrue("no index must exist after rejection",
                    manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1") == null
                            || manager.getTableSpaceManager("tblspace1")
                                    .getIndexesOnTable("t1").isEmpty());
        }
    }

    @Test
    public void emptyTable_vectorIndex_inTransaction_isAllowed() throws Exception {
        // The empty-table fast path skips the checkpoint-and-mark logic, so
        // a transactional CREATE VECTOR INDEX has nothing to back-fill and
        // is safe to run inside a transaction.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // No INSERTs.

            long tx = beginTransaction(manager, "tblspace1");
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList(), new TransactionContext(tx));
            commitTransaction(manager, "tblspace1", tx);

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull("vector index must exist after transactional create on empty table",
                    idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            // Empty-table path must NOT mark rebuild=true.
            assertNull("empty table must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_hashIndex_inTransaction_isAllowed() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}, 1));

            long tx = beginTransaction(manager, "tblspace1");
            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList(), new TransactionContext(tx));
            commitTransaction(manager, "tblspace1", tx);

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("hidx").getIndex();
            assertNotNull("hash index must exist after transactional create on non-empty table",
                    idx);
            assertEquals(Index.TYPE_HASH, idx.type);
        }
    }

    @Test
    public void nonEmptyTable_brinIndex_inTransaction_isAllowed() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}, 1));

            long tx = beginTransaction(manager, "tblspace1");
            execute(manager,
                    "CREATE BRIN INDEX bidx ON tblspace1.t1(n)",
                    Collections.emptyList(), new TransactionContext(tx));
            commitTransaction(manager, "tblspace1", tx);

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("bidx").getIndex();
            assertNotNull("brin index must exist after transactional create on non-empty table",
                    idx);
            assertEquals(Index.TYPE_BRIN, idx.type);
        }
    }

    @Test
    public void nonEmptyTable_vectorIndex_outsideTransaction_isAllowed() throws Exception {
        // The happy path: NO transaction → the createIndex flow checkpoints
        // the table and marks rebuild=true. This path is also covered by
        // CreateVectorIndexRebuildPropertyTest, but we duplicate the
        // assertion here so this test class is self-contained as the
        // "transactional rejection contract" gate.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}, 1));

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertEquals("non-empty table outside transaction MUST receive rebuild=true",
                    "true", idx.properties.get(VectorIndexManager.PROP_REBUILD));
        }
    }
}
