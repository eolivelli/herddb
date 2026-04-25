/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package herddb.cluster.bookkeeper;

import static herddb.core.TestUtils.newServerConfigurationWithAutoPort;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.cluster.BookkeeperCommitLog;
import herddb.codec.RecordSerializer;
import herddb.core.ActivatorRunRequest;
import herddb.core.ClusterTest;
import herddb.core.TableSpaceManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.StatementExecutionResult;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CommitTransactionStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.InsertStatement;
import herddb.server.Server;
import herddb.server.ServerConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ClusterTest.class)
public class FencingTest extends BookkeeperFailuresBase {

    @Test
    public void testFencing() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 1)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            try (BookKeeper bk = createBookKeeper()) {
                try (LedgerHandle fenceLedger = bk.openLedger(ledgerId,
                        BookKeeper.DigestType.CRC32C, "herddb".getBytes(StandardCharsets.UTF_8))) {
                }
            }

            try {
                server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.
                        makeRecord(table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                        TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
            }

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(1000);
            }

            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(4, scan.consume().size());
            }
        }
    }

    @Test
    public void testFencingDuringTransaction() throws Exception {
        ServerConfiguration serverconfig_1 = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig_1.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig_1.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        ServerConfiguration serverconfig_2 = serverconfig_1
                .copy()
                .set(ServerConfiguration.PROPERTY_NODEID, "server2")
                .set(ServerConfiguration.PROPERTY_BASEDIR, folder.newFolder().toPath().toAbsolutePath());

        try (Server server = new Server(serverconfig_1)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            StatementExecutionResult executeStatement = server.getManager().executeUpdate(new InsertStatement(
                    TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(table, "c", 1)), StatementEvaluationContext.
                    DEFAULT_EVALUATION_CONTEXT(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = executeStatement.transactionId;
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 2)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 3)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // we do not want auto-recovery
            server.getManager().setActivatorPauseStatus(true);

            try (BookKeeper bk = createBookKeeper()) {
                try (LedgerHandle fenceLedger = bk.openLedger(ledgerId,
                        BookKeeper.DigestType.CRC32C, "herddb".getBytes(StandardCharsets.UTF_8))) {
                }
            }

            // transaction will continue and see the failure only the time of the commit
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 5)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));
            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 6)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), new TransactionContext(
                    transactionId));

            try {
                server.getManager().executeStatement(new CommitTransactionStatement(TableSpace.DEFAULT, transactionId),
                        StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
                fail();
            } catch (StatementExecutionException expected) {
            }

            while (true) {
                System.out.println("status leader:" + tableSpaceManager.isLeader() + " failed:" + tableSpaceManager.
                        isFailed());
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(100);
            }
            server.getManager().setActivatorPauseStatus(false);
            server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);

            while (true) {
                TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                        TableSpace.DEFAULT);
                System.out.println("tableSpaceManager_after_failure:" + tableSpaceManager_after_failure);
                System.out.println("tableSpaceManager:" + tableSpaceManager);
                if (tableSpaceManager_after_failure != null && tableSpaceManager_after_failure != tableSpaceManager) {
                    break;
                }
                Thread.sleep(1000);
                server.getManager().triggerActivator(ActivatorRunRequest.TABLESPACEMANAGEMENT);
            }

            TableSpaceManager tableSpaceManager_after_failure = server.getManager().getTableSpaceManager(
                    TableSpace.DEFAULT);
            Assert.assertNotNull(tableSpaceManager_after_failure);
            assertNotSame(tableSpaceManager_after_failure, tableSpaceManager);
            assertTrue(!tableSpaceManager_after_failure.isFailed());

            server.getManager().executeUpdate(new InsertStatement(TableSpace.DEFAULT, "t1", RecordSerializer.makeRecord(
                    table, "c", 4)), StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            try (DataScanner scan = scan(server.getManager(), "select * from t1", Collections.emptyList())) {
                assertEquals(1, scan.consume().size());
            }
        }
    }

    /**
     * Regression test for the race between a transient, non-BK write error and a
     * subsequent external ledger-fencing event (issue #265).
     *
     * <p>The flaky scenario in {@link #testFencingDuringTransaction} is caused by a
     * JVM-local transport stale-reference bug: the first test leaves behind a
     * {@code LocalBookiesRegistry} entry for a bookie that is no longer running.  The
     * second test allocates a new bookie on a recycled port, but some write callbacks
     * still fire with {@code RuntimeException("Bookie is not running any more")} from
     * the old entry.  That {@code RuntimeException} is <em>not</em> a
     * {@code BKException}, so before the fix {@code handleBookKeeperFailure} did not
     * call {@code signalLogFailed()}, and the BK
     * {@link org.apache.bookkeeper.client.api.WriteHandle} stayed in the OPEN state.
     *
     * <p>When an external agent later fences the ledger and the next write triggers
     * {@code openNewLedger()}, the old code called {@code closeCurrentWriter()} and
     * — because {@code closeAsync()} in BookKeeper 4.17.x is a pure ZK-metadata
     * operation that succeeds even on a bookie-side fenced ledger when
     * {@code openLedgerNoRecovery} was used — the close succeeded.  The old
     * {@code openNewLedger()} then created a fresh ledger and reset {@code failed=false},
     * allowing the commit to succeed despite the ledger having been fenced.
     *
     * <p>The fix adds a guard at the top of {@code openNewLedger()}: if the previous
     * writer's {@code writeError} is a non-{@link org.apache.bookkeeper.client.api.BKException}
     * (i.e. a {@code RuntimeException} from the local transport layer), the ledger
     * state is unknown and we must not silently create a new one.  Instead,
     * {@code signalLogFailed()} is called and a {@code LogNotAvailableException} is
     * thrown, which propagates back to the caller as a
     * {@link herddb.model.StatementExecutionException}.
     * Additionally, {@code handleBookKeeperFailure} now calls {@code signalLogFailed()}
     * for any non-BK exception encountered in the write callback, providing a
     * second line of defence for the real-runtime path (where the error goes through
     * the callback rather than {@link BookkeeperCommitLog.CommitFileWriter#forceWriteError}).
     *
     * <p>This test reproduces the race deterministically:
     * <ol>
     *   <li>Write a sync entry so the BK write handle is in the OPEN state with all
     *       pending adds completed.</li>
     *   <li>Start a transaction and wait for its async BK writes to be ACKed
     *       ({@code pendingAdds == 0}), leaving the handle OPEN.</li>
     *   <li>Inject a non-BK write error via
     *       {@link BookkeeperCommitLog.CommitFileWriter#forceWriteError} to set
     *       {@code errorOccurredDuringWrite=true} and {@code writeError=RuntimeException}
     *       without touching the BK write handle and without going through
     *       {@code handleBookKeeperFailure} (so {@code failed} stays {@code false}).</li>
     *   <li>Fence the ledger via {@code openLedgerNoRecovery} (bookie-side fence only,
     *       ZK metadata unchanged).</li>
     *   <li>Attempt to commit: {@code openNewLedger()} detects the non-BK write error,
     *       calls {@code signalLogFailed()}, and throws {@code LogNotAvailableException}
     *       instead of creating a new ledger.</li>
     * </ol>
     *
     * <p>Without the fix, step 5 creates a new ledger and resets {@code failed=false},
     * letting the commit succeed.  With the fix the commit correctly throws
     * {@link herddb.model.StatementExecutionException}.
     */
    @Test
    public void testFencingAfterTransientWriteError() throws Exception {
        ServerConfiguration serverconfig = newServerConfigurationWithAutoPort(folder.newFolder().toPath());
        serverconfig.set(ServerConfiguration.PROPERTY_NODEID, "server1");
        serverconfig.set(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_CLUSTER);
        serverconfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS, testEnv.getAddress());
        serverconfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_PATH, testEnv.getPath());
        serverconfig.set(ServerConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, testEnv.getTimeout());
        serverconfig.set(ServerConfiguration.PROPERTY_ENFORCE_LEADERSHIP, false);

        try (Server server = new Server(serverconfig)) {
            server.start();
            server.waitForStandaloneBoot();
            Table table = Table.builder()
                    .name("t1")
                    .column("c", ColumnTypes.INTEGER)
                    .primaryKey("c")
                    .build();
            server.getManager().executeStatement(new CreateTableStatement(table),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            // Sync NO_TRANSACTION write: establishes the ledger and ensures all
            // pending adds complete, leaving the BK write handle in OPEN state.
            server.getManager().executeUpdate(
                    new InsertStatement(TableSpace.DEFAULT, "t1",
                            RecordSerializer.makeRecord(table, "c", 1)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);

            TableSpaceManager tableSpaceManager = server.getManager().getTableSpaceManager(TableSpace.DEFAULT);
            BookkeeperCommitLog log = (BookkeeperCommitLog) tableSpaceManager.getLog();
            long ledgerId = log.getLastSequenceNumber().ledgerId;
            assertTrue(ledgerId >= 0);

            // Start a transaction and insert a row (async BK write, sync=false).
            StatementExecutionResult txResult = server.getManager().executeUpdate(
                    new InsertStatement(TableSpace.DEFAULT, "t1",
                            RecordSerializer.makeRecord(table, "c", 2)),
                    StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long transactionId = txResult.transactionId;

            // Wait for all in-flight BK writes (BEGIN_TX + INSERT) to be ACKed so that
            // pendingAdds == 0.  The BK write handle is then fully OPEN with no
            // outstanding requests — exactly the state produced by the stale-transport
            // scenario (where the RuntimeException fires immediately).
            long ackedDeadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < ackedDeadline) {
                BookkeeperCommitLog.CommitFileWriter w = log.getWriter();
                if (w != null && w.getPendingAdds() == 0) {
                    break;
                }
                Thread.sleep(100);
            }
            BookkeeperCommitLog.CommitFileWriter writer = log.getWriter();
            Assert.assertNotNull("writer must be non-null", writer);
            assertTrue("all BK writes must have been ACKed (pendingAdds == 0)",
                    writer.getPendingAdds() == 0);

            // Inject a non-fencing write error.  This mimics the stale-transport
            // RuntimeException that sets errorOccurredDuringWrite=true / wasFenced=false
            // WITHOUT going through handleBookKeeperFailure (so failed stays false) and
            // WITHOUT closing the BK write handle (so writer.close() can still interact
            // with the bookie and detect fencing later).
            writer.forceWriteError(new RuntimeException("simulated stale-transport error"));
            assertTrue("errorOccurredDuringWrite must be true after forceWriteError",
                    writer.isErrorOccurredDuringWrite());

            // Prevent the tablespace activator from automatically recovering the
            // tablespace so we can verify the commit-path fencing detection.
            server.getManager().setActivatorPauseStatus(true);

            // Fence ledger X using openLedgerNoRecovery: this sends a fence request to
            // all bookies (they mark the ledger as FENCED) without completing the full
            // ZK-metadata update (ledger stays OPEN in ZK).  Keep the handle open so the
            // bookie stays in FENCED state during the commit attempt.
            try (BookKeeper bk = createBookKeeper();
                 LedgerHandle fenced = bk.openLedgerNoRecovery(
                         ledgerId, BookKeeper.DigestType.CRC32C,
                         "herddb".getBytes(StandardCharsets.UTF_8))) {

                // The commit must fail.  Without the fix, openNewLedger() silently creates
                // a new ledger and resets failed=false, so the commit wrongly succeeds.
                // With the fix, openNewLedger() detects the non-BK writeError, calls
                // signalLogFailed(), and throws LogNotAvailableException, which surfaces
                // here as StatementExecutionException.
                try {
                    server.getManager().executeStatement(
                            new CommitTransactionStatement(TableSpace.DEFAULT, transactionId),
                            StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                            TransactionContext.NO_TRANSACTION);
                    fail("commit must fail when the ledger has been externally fenced");
                } catch (StatementExecutionException expected) {
                    // correct: the commit detected the fencing and refused to proceed
                }
            }

            // Verify the tablespace reaches the FAILED state
            long failDeadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < failDeadline) {
                if (tableSpaceManager.isFailed()) {
                    break;
                }
                Thread.sleep(200);
            }
            assertTrue("tablespace must be marked failed after ledger fencing",
                    tableSpaceManager.isFailed());
        }
    }
}
