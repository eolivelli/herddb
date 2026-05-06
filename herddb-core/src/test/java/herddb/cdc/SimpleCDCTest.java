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

package herddb.cdc;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.client.ClientConfiguration;
import herddb.codec.RecordSerializer;
import herddb.core.ClusterTest;
import herddb.core.TestUtils;
import herddb.log.LogSequenceNumber;
import herddb.model.Column;
import herddb.model.ColumnTypes;
import herddb.model.StatementEvaluationContext;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.AlterTableStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.DeleteStatement;
import herddb.model.commands.InsertStatement;
import herddb.model.commands.UpdateStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import herddb.utils.Bytes;
import herddb.utils.ZKTestEnv;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;


/**
 * Tests around backup/restore
 *
 * @author enrico.olivelli
 */
@Category(ClusterTest.class)
public class SimpleCDCTest {

    private static final Logger LOG = Logger.getLogger(SimpleCDCTest.class.getName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ZKTestEnv testEnv;

    @Before
    public void beforeSetup() throws Exception {
        testEnv = new ZKTestEnv(folder.newFolder().toPath());
        testEnv.startBookieAndInitCluster();
    }

    @After
    public void afterTeardown() throws Exception {
        if (testEnv != null) {
            testEnv.close();
        }
    }

    @Test
    public void testBasicCaptureDataChange() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            long tx = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    new ChangeDataCapture.MutationListener() {
                        @Override
                        public void accept(ChangeDataCapture.Mutation mutation) {
                            LOG.log(Level.INFO, "mutation " + mutation);
                            assertTrue(mutation.getTimestamp() > 0);
                            assertNotNull(mutation.getLogSequenceNumber());
                            assertNotNull(mutation.getTable());
                            mutations.add(mutation);
                        }
                    },
                    LogSequenceNumber.START_OF_TIME,
                    new InMemoryTableHistoryStorage());) {

                cdc.start();

                cdc.run();

                // we are missing the last entry, because it is not confirmed yet on BookKeeper at this point
                // also the mutations in the transaction are not visible
                assertEquals(3, mutations.size());

                // commit the transaction
                TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx);

                server_1.getManager().executeUpdate(new UpdateStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

                cdc.run();
                assertEquals(5, mutations.size());

                server_1.getManager().executeStatement(new AlterTableStatement(Arrays.asList(Column.column("e", ColumnTypes.INTEGER)), Collections.emptyList(), Collections.emptyList(), null, table.name, TableSpace.DEFAULT, null, Collections.emptyList(),
                        Collections.emptyList()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                cdc.run();
                assertEquals(6, mutations.size());


                // transaction to be rolled back
                long tx2 = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 30, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx2));
                server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 31, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx2));
                TestUtils.roolbackTransaction(server_1.getManager(), TableSpace.DEFAULT, tx2);

                // nothing is to be sent to CDC
                cdc.run();
                assertEquals(7, mutations.size());

                server_1.getManager().executeUpdate(new DeleteStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                cdc.run();
                assertEquals(7, mutations.size());

                // close the server...close the ledger, now we can read the last mutation
                server_1.close();

                cdc.run();
                assertEquals(8, mutations.size());


                int i = 0;
                assertEquals(ChangeDataCapture.MutationType.CREATE_TABLE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.UPDATE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.ALTER_TABLE, mutations.get(i++).getMutationType());
                assertEquals(ChangeDataCapture.MutationType.DELETE, mutations.get(i++).getMutationType());


            }
        }
    }

    @Test
    public void testBasicCaptureDataChangeWithTransactions() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();

            // create table in transaction
            long tx = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            // work on the table in transaction
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));

            // commit
            TestUtils.commitTransaction(server_1.getManager(), TableSpace.DEFAULT, tx);

            // work on the table outside of the transaction
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // close the server and the ledger
            server_1.close();

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    new ChangeDataCapture.MutationListener() {
                        @Override
                        public void accept(ChangeDataCapture.Mutation mutation) {
                            LOG.log(Level.INFO, "mutation " + mutation);
                            assertTrue(mutation.getTimestamp() > 0);
                            assertNotNull(mutation.getLogSequenceNumber());
                            assertNotNull(mutation.getTable());
                            mutations.add(mutation);
                        }
                    },
                    LogSequenceNumber.START_OF_TIME,
                    new InMemoryTableHistoryStorage());) {
                cdc.start();
                cdc.run();


                assertEquals(3, mutations.size());

                int i = 0;
                ChangeDataCapture.Mutation m1 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.CREATE_TABLE, m1.getMutationType());
                Table tableFromM1 = m1.getTable();
                assertNotNull(tableFromM1);
                // Issue #408: the leader stamps a fresh tableId on every
                // CREATE_TABLE; the test's locally-built Table has tableId=0.
                // Compare on the user-visible identity (name + structure)
                // and assert the CDC carried a non-zero, valid id.
                assertEquals(table.name, tableFromM1.name);
                assertEquals(table.uuid, tableFromM1.uuid);
                assertTrue("CDC must report a leader-assigned tableId, got " + tableFromM1.tableId,
                        tableFromM1.tableId > 0);
                ChangeDataCapture.Mutation m2 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.INSERT, m2.getMutationType());
                assertEquals(m2.getTable(), tableFromM1);
                assertEquals(1, m2.getRecord().get("c"));
                assertEquals(2, m2.getRecord().get("d"));
                ChangeDataCapture.Mutation m3 = mutations.get(i++);
                assertEquals(ChangeDataCapture.MutationType.INSERT, m3.getMutationType());
                assertEquals(m3.getTable(), tableFromM1);
                assertEquals(2, m3.getRecord().get("c"));
                assertEquals(2, m3.getRecord().get("d"));

            }
        }
    }

    @Test
    public void testBasicCaptureDataChangeWithRestart() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 2, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 3, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            InMemoryTableHistoryStorage tableHistoryStorage = new InMemoryTableHistoryStorage();
            LogSequenceNumber currentPosition = LogSequenceNumber.START_OF_TIME;

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            currentPosition = performOneCDCStep(client_configuration, server_1, tableHistoryStorage, currentPosition, mutations);

            // we are missing the last entry, because it is not confirmed yet on BookKeeper at this point
            assertEquals(4, mutations.size());

            server_1.getManager().executeUpdate(new UpdateStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 4, "d", 2), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            currentPosition = performOneCDCStep(client_configuration, server_1, tableHistoryStorage, currentPosition, mutations);
            assertEquals(5, mutations.size());

            server_1.getManager().executeStatement(new AlterTableStatement(Arrays.asList(Column.column("e", ColumnTypes.INTEGER)), Collections.emptyList(), Collections.emptyList(), null, table.name, TableSpace.DEFAULT, null, Collections.emptyList(),
                    Collections.emptyList()), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            currentPosition = performOneCDCStep(client_configuration, server_1, tableHistoryStorage, currentPosition, mutations);
            assertEquals(6, mutations.size());


            server_1.getManager().executeUpdate(new DeleteStatement(TableSpace.DEFAULT, "t1", Bytes.from_int(1), null), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            currentPosition = performOneCDCStep(client_configuration, server_1, tableHistoryStorage, currentPosition, mutations);
            assertEquals(7, mutations.size());

            // close the server...close the ledger, now we can read the last mutation
            server_1.close();

            currentPosition = performOneCDCStep(client_configuration, server_1, tableHistoryStorage, currentPosition, mutations);
            assertEquals(8, mutations.size());

            int i = 0;
            assertEquals(ChangeDataCapture.MutationType.CREATE_TABLE, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.INSERT, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.UPDATE, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.ALTER_TABLE, mutations.get(i++).getMutationType());
            assertEquals(ChangeDataCapture.MutationType.DELETE, mutations.get(i++).getMutationType());
        }
    }

    /**
     * Issue #408 review (3) — verifies that a {@code ROLLBACK} of a
     * transaction that issued a {@code DROP TABLE} on a pre-existing
     * committed table does NOT evict the {@code (id → name)} mapping
     * from {@code ChangeDataCapture#tableIdToName}. {@code DROP TABLE}
     * inside a transaction is the only DDL path in this codebase that
     * does NOT auto-commit (see
     * {@code TableSpaceManager#dropTable}), so it produces a real
     * {@code DROP_TABLE (txn>0)} entry followed by a real
     * {@code ROLLBACKTRANSACTION (txn>0)} entry — exactly the path
     * the rollback scrub touches.
     *
     * <p>This test would FAIL against an over-broad rollback scrub of
     * {@code TransactionHolder#tablesDefinitions.keySet()} (which
     * evicts the mapping for the rolled-back DROP target even though
     * the DROP never committed) and PASSES with the
     * {@code newlyCreatedTableIds}-only scrub.
     */
    @Test
    public void testCDCRollbackOfDropDoesNotEvictExistingMapping() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();

            // Phase 1: create + commit table T (autocommit), insert one row.
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1, "d", 2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Phase 2: BEGIN; DROP TABLE t1; ROLLBACK. DROP TABLE keeps
            // the transaction open (verified at
            // TableSpaceManager#dropTable line 2569), so the WAL really
            // contains a `DROP_TABLE (txn=tx)` entry that populates
            // `transaction.tablesDefinitions` (but NOT
            // `newlyCreatedTableIds`) followed by a
            // `ROLLBACKTRANSACTION (txn=tx)` entry.
            long tx = TestUtils.beginTransaction(server_1.getManager(), TableSpace.DEFAULT);
            server_1.getManager().executeStatement(
                    new herddb.model.commands.DropTableStatement(TableSpace.DEFAULT, "t1", false),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(tx));
            server_1.getManager().executeStatement(
                    new herddb.model.commands.RollbackTransactionStatement(TableSpace.DEFAULT, tx),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // Phase 3: INSERT into t1 outside any transaction — t1 still
            // exists because the DROP was rolled back.
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 99, "d", 99)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            server_1.close();

            // Phase 4: drive the CDC across the whole stream with a
            // schema history whose `resolveTableName(int)` is forced to
            // return null. The ONLY way the post-rollback INSERT can
            // surface with a non-null Table is via the `tableIdToName`
            // in-memory cache — the very cache the buggy scrub used to
            // evict.
            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            InMemoryTableHistoryStorage storage = new InMemoryTableHistoryStorage();
            ChangeDataCapture.TableSchemaHistoryStorage idObliviousStorage =
                    new ChangeDataCapture.TableSchemaHistoryStorage() {
                        @Override
                        public void storeSchema(LogSequenceNumber lsn, Table t) {
                            storage.storeSchema(lsn, t);
                        }

                        @Override
                        public Table fetchSchema(LogSequenceNumber lsn, String tableName) {
                            return storage.fetchSchema(lsn, tableName);
                        }
                        // Deliberately does NOT override
                        // resolveTableName(int) — default returns null,
                        // forcing the listener to rely solely on the
                        // CDC's in-memory id → name cache.
                    };
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    mutations::add,
                    LogSequenceNumber.START_OF_TIME,
                    idObliviousStorage)) {
                cdc.start();
                cdc.run();
            }

            // The post-rollback INSERT (c=99, d=99) MUST surface with a
            // non-null Table named "t1". A rollback scrub that evicted
            // the mapping for the rolled-back DROP target would deliver
            // this Mutation with table == null, failing this assertion.
            int inserts = 0;
            ChangeDataCapture.Mutation postRollbackInsert = null;
            for (ChangeDataCapture.Mutation m : mutations) {
                if (m.getMutationType() == ChangeDataCapture.MutationType.INSERT) {
                    inserts++;
                    if (m.getRecord() != null
                            && Integer.valueOf(99).equals(m.getRecord().get("c"))) {
                        postRollbackInsert = m;
                    }
                }
            }
            assertTrue("expected at least 2 INSERTs in the captured stream, got " + inserts, inserts >= 2);
            assertNotNull("expected an INSERT for c=99 after the rolled-back DROP, captured "
                    + mutations.size() + " mutations total", postRollbackInsert);
            assertNotNull("post-rollback INSERT must carry a non-null Table — a rolled-back DROP "
                    + "must NOT evict the (id → name) mapping for the still-existing committed table",
                    postRollbackInsert.getTable());
            assertEquals("t1", postRollbackInsert.getTable().name);
        }
    }

    /**
     * Issue #408 review — pins the contract that when neither the
     * in-memory id → name cache nor the storage's optional
     * {@code resolveTableName(int)} hook can resolve a tableId, the CDC
     * delivers a {@link ChangeDataCapture.Mutation} with
     * {@code getTable() == null} and surfaces a WARNING log line.
     * Reproduces the cold-start scenario where a CDC starts past a
     * CREATE_TABLE and the user-supplied storage does not implement
     * the optional id → name hook.
     */
    @Test
    public void testCDCNullResolveDeliversNullTableMutation() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        ClientConfiguration client_configuration = new ClientConfiguration(folder.newFolder().toPath());
        client_configuration.set(ClientConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        client_configuration.set(ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());

        try (Server server_1 = new Server(serverconfig_1)) {
            server_1.start();
            server_1.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .column("d", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server_1.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 1, "d", 2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // First CDC pass: replays the entire log including
            // CREATE_TABLE, populating the storage's name-keyed map.
            // Capture the LSN we resume from on the second pass.
            InMemoryTableHistoryStorage storage = new InMemoryTableHistoryStorage();
            LogSequenceNumber resumeFrom = LogSequenceNumber.START_OF_TIME;
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    m -> { /* drain */ },
                    resumeFrom, storage)) {
                cdc.start();
                resumeFrom = cdc.run();
            }

            // Now drop the storage's id → name index, simulating a
            // user-provided implementation that does not override
            // resolveTableName(int).
            ChangeDataCapture.TableSchemaHistoryStorage idObliviousStorage =
                    new ChangeDataCapture.TableSchemaHistoryStorage() {
                        @Override
                        public void storeSchema(LogSequenceNumber lsn, Table t) {
                            storage.storeSchema(lsn, t);
                        }

                        @Override
                        public Table fetchSchema(LogSequenceNumber lsn, String tableName) {
                            return storage.fetchSchema(lsn, tableName);
                        }
                        // Note: deliberately does NOT override
                        // resolveTableName(int), so the default
                        // implementation returns null.
                    };

            // Second pass: write a fresh INSERT and resume the CDC past
            // the CREATE_TABLE. The id → name cache is empty in this
            // CDC instance and the storage's hook returns null — the
            // delivered Mutation must have getTable() == null and the
            // CDC must not throw.
            server_1.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1",
                    RecordSerializer.makeRecord(table, "c", 2, "d", 3)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server_1.close();

            List<ChangeDataCapture.Mutation> mutations = new ArrayList<>();
            try (final ChangeDataCapture cdc = new ChangeDataCapture(
                    server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                    client_configuration,
                    mutations::add,
                    resumeFrom, idObliviousStorage)) {
                cdc.start();
                cdc.run();
            }
            // Every INSERT-class mutation must surface with table ==
            // null when the id cannot be resolved (cold-start, no
            // optional hook). The contract the WARNING-log path pins.
            int unresolved = 0;
            for (ChangeDataCapture.Mutation m : mutations) {
                if (m.getMutationType() == ChangeDataCapture.MutationType.INSERT) {
                    if (m.getTable() == null) {
                        unresolved++;
                    }
                }
            }
            assertTrue("at least one cold-start INSERT must surface with table == null when the storage "
                    + "does not implement resolveTableName(int), got " + unresolved, unresolved >= 1);
        }
    }

    private LogSequenceNumber performOneCDCStep(ClientConfiguration client_configuration, Server server_1, InMemoryTableHistoryStorage tableHistoryStorage, LogSequenceNumber currentPosition, List<ChangeDataCapture.Mutation> mutations) throws Exception {
        try (final ChangeDataCapture cdc = new ChangeDataCapture(
                server_1.getManager().getTableSpaceManager(TableSpace.DEFAULT).getTableSpaceUUID(),
                client_configuration,
                new ChangeDataCapture.MutationListener() {
                    @Override
                    public void accept(ChangeDataCapture.Mutation mutation) {
                        LOG.log(Level.INFO, "mutation " + mutation);
                        assertTrue(mutation.getTimestamp() > 0);
                        assertNotNull(mutation.getLogSequenceNumber());
                        assertNotNull(mutation.getTable());
                        mutations.add(mutation);
                    }
                },
                currentPosition,
                tableHistoryStorage)) {
            cdc.start();
            currentPosition = cdc.run();
        }
        return currentPosition;
    }

    private static class InMemoryTableHistoryStorage implements ChangeDataCapture.TableSchemaHistoryStorage {

        private Map<String, SortedMap<LogSequenceNumber, Table>> definitions = new ConcurrentHashMap<>();
        // Issue #408: side-index for the optional id → name resolver hook
        // exercised by the CDC across restart boundaries. The CDC needs
        // to translate an integer tableId to a name even when its own
        // in-memory cache is empty (e.g. resumed from an LSN past the
        // matching CREATE_TABLE). Side-indexing here keeps the public
        // schema API name-keyed.
        private Map<Integer, String> idToName = new ConcurrentHashMap<>();

        @Override
        public void storeSchema(LogSequenceNumber lsn, Table table) {
            LOG.log(Level.INFO, "storeSchema {0} {1}", new Object[] {lsn, table.name});
            SortedMap<LogSequenceNumber, Table> tableHistory = definitions.computeIfAbsent(table.name, (n)-> Collections.synchronizedSortedMap(new TreeMap<>()));
            tableHistory.put(lsn, table);
            if (table.tableId != 0) {
                idToName.put(table.tableId, table.name);
            }
        }

        @Override
        public Table fetchSchema(LogSequenceNumber lsn, String tableName) {
            LOG.log(Level.INFO, "fetchSchema {0} {1}", new Object[] {lsn, tableName});
            SortedMap<LogSequenceNumber, Table> tableHistory = definitions.computeIfAbsent(tableName, (n)-> Collections.synchronizedSortedMap(new TreeMap<>()));
            SortedMap<LogSequenceNumber, Table> after = tableHistory.headMap(lsn);
            if (after.isEmpty()) {
                return after.get(tableHistory.lastKey());
            }
            return after.values().iterator().next();
        }

        @Override
        public String resolveTableName(int tableId) {
            return idToName.get(tableId);
        }
    }
}
