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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import static org.junit.Assert.assertEquals;

/**
 * Issue #471 — verifies that {@link TableManager#doCheckpoint} rolls back
 * any partial pin it took when a downstream step inside the checkpoint
 * fails. Without the rollback, a foreground caller of
 * {@code tableManager.checkpoint(true)} (e.g. {@code CREATE VECTOR INDEX}
 * on a non-empty table) that fails partway through would leak one or more
 * pins for the leader's process lifetime.
 *
 * <p>The test uses a {@link FailingDsm} wrapper that throws on the
 * {@code tableCheckpoint} call (the LAST pin site inside doCheckpoint),
 * so the secondary-index and BLink-keyToPage pins have already been taken
 * by the time the failure happens. Both pre-failure pins must be rolled
 * back; the table pin (which failed before being recorded) must not be
 * held either.
 *
 * @author enrico.olivelli
 */
public class CheckpointPinRollbackTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    /**
     * {@link FileDataStorageManager} subclass that selectively fails
     * specific calls inside the {@link TableManager#doCheckpoint}
     * pipeline so the partial-pin rollback path can be exercised at
     * each of the three pin sites.
     *
     * <p>All counters filter by table or index identity so a
     * background activator-driven checkpoint cannot accidentally
     * consume the armed failure (the test fixture only arms one
     * counter at a time, but tightening the predicate makes the test
     * resilient to future activator changes).
     */
    private static final class FailingDsm extends FileDataStorageManager {
        /** Fail {@code tableCheckpoint} for tables whose name equals this. */
        volatile String failTableCheckpointForTable;
        final AtomicInteger failNextTableCheckpoints = new AtomicInteger(0);
        /**
         * Fail {@code indexCheckpoint} for index names matching this
         * predicate. {@code null} disables the filter.
         */
        volatile java.util.function.Predicate<String> failIndexCheckpointWhen;
        final AtomicInteger failNextIndexCheckpoints = new AtomicInteger(0);

        FailingDsm(Path baseDirectory) {
            super(baseDirectory);
        }

        @Override
        public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String tableName,
                                                          TableStatus tableStatus, boolean pin)
                throws DataStorageManagerException {
            // The DSM's tableName parameter is the table UUID. Filter on
            // the human-readable name carried by the TableStatus instead.
            if (failTableCheckpointForTable != null
                    && tableStatus != null
                    && failTableCheckpointForTable.equals(tableStatus.tableName)
                    && failNextTableCheckpoints.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new DataStorageManagerException(
                        "injected tableCheckpoint failure for partial-pin rollback test");
            }
            return super.tableCheckpoint(tableSpace, tableName, tableStatus, pin);
        }

        @Override
        public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String indexName,
                                                          IndexStatus indexStatus, boolean pin)
                throws DataStorageManagerException {
            java.util.function.Predicate<String> predicate = failIndexCheckpointWhen;
            if (predicate != null
                    && predicate.test(indexName)
                    && failNextIndexCheckpoints.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new DataStorageManagerException(
                        "injected indexCheckpoint failure for index '" + indexName + "'");
            }
            return super.indexCheckpoint(tableSpace, indexName, indexStatus, pin);
        }
    }

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir,
                                   FailingDsm dsm) {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                dsm,
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    @Test
    public void doCheckpointFailingAtTablePinSite_rollsBackBLinkAndSecondaryIndexPins()
            throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        FailingDsm dsm = new FailingDsm(dataPath);
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir, dsm)) {
            manager.start();
            // Tablespace + non-empty table with one pre-existing secondary index.
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null, n int)",
                    Collections.emptyList());
            for (int i = 0; i < 5; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                        Arrays.asList(i, new float[]{i * 0.1f, i * 0.2f, i * 0.3f}, i));
            }
            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            // Snapshot the pin state BEFORE the failing CREATE VECTOR INDEX
            // so we can compare deltas: any pins added by the failing
            // checkpoint must be rolled back, but pre-existing pins
            // (e.g. the hash index's own CREATE INDEX checkpoint pin)
            // must not be touched.
            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1").getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            String hashIdxUuid = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("hidx").getIndex().uuid;
            String blinkIndexName = BLinkKeyToPageIndex.deriveIndexName(tableUuid);

            Set<LogSequenceNumber> tablePinsBefore = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);

            // Arm the failure on the NEXT tableCheckpoint call for table
            // "t1" only. doCheckpoint will pin the hash index, pin the
            // BLink, then call our overridden tableCheckpoint — which
            // throws. The tableName filter prevents a future
            // activator-path change from accidentally consuming the
            // armed failure on a different table.
            dsm.failTableCheckpointForTable = "t1";
            dsm.failNextTableCheckpoints.set(1);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("CREATE VECTOR INDEX must fail when tableCheckpoint throws");
            } catch (RuntimeException expected) {
                // StatementExecutionException wraps the
                // DataStorageManagerException injected above.
            }

            // After the failure, the pin sets must equal what they were
            // BEFORE the failed CREATE VECTOR INDEX — i.e. doCheckpoint
            // rolled back the hash and BLink pins it had taken at the
            // post-Phase-A LSN, and the table pin (which failed before
            // recording) is not held either.
            Set<LogSequenceNumber> tablePinsAfter = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);

            // Table pin must be empty (failed before recording).
            assertTrue(
                    "table pin set must be empty after failed checkpoint (before="
                            + tablePinsBefore + ", after=" + tablePinsAfter + ")",
                    tablePinsAfter.isEmpty());
            // BLink and hash pin sets must be unchanged from BEFORE the
            // failed CREATE VECTOR INDEX — any new pin taken inside
            // doCheckpoint at the post-Phase-A LSN must have been rolled
            // back to the prior set. Use assertEquals so a regression
            // diff prints "expected [lsn1] but was [lsn1, lsn2]".
            assertEquals("BLink pin set must equal pre-failure state",
                    blinkPinsBefore, blinkPinsAfter);
            assertEquals("hash index pin set must equal pre-failure state",
                    hashPinsBefore, hashPinsAfter);

            // Sanity check: another DDL statement still works (no leaked
            // tablespace write lock or stuck checkPointRunning flag).
            execute(manager,
                    "CREATE TABLE tblspace1.t2 (id int primary key)",
                    Collections.emptyList());
        }
    }

    @Test
    public void doCheckpointFailingAtBLinkPinSite_rollsBackOnlySecondaryIndexPins()
            throws Exception {
        // Inject failure at the SECOND pin site (PK BLink keyToPage).
        // The secondary-index pin (first site) has already been taken
        // when the failure fires; the BLink pin (second site) is the
        // call that throws — so its rollback registration in
        // doCheckpoint never runs. The table pin (third site) is not
        // reached. The rollback contract: only the secondary-index
        // pin is rolled back; nothing else needs to be.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        FailingDsm dsm = new FailingDsm(dataPath);
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir, dsm)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null, n int)",
                    Collections.emptyList());
            for (int i = 0; i < 5; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                        Arrays.asList(i, new float[]{i * 0.1f, 0f, 0f}, i));
            }
            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1").getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            String hashIdxUuid = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("hidx").getIndex().uuid;
            String blinkIndexName = BLinkKeyToPageIndex.deriveIndexName(tableUuid);

            Set<LogSequenceNumber> tablePinsBefore = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);

            // Arm: fail when indexCheckpoint is called for the BLink
            // (whose synthetic name ends in "_primary").
            dsm.failIndexCheckpointWhen = name -> name.endsWith("_primary");
            dsm.failNextIndexCheckpoints.set(1);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("CREATE VECTOR INDEX must fail when BLink indexCheckpoint throws");
            } catch (RuntimeException expected) {
                // expected
            }

            Set<LogSequenceNumber> tablePinsAfter = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);

            assertEquals("table pin set must be unchanged (BLink failure happened before table)",
                    tablePinsBefore, tablePinsAfter);
            assertEquals("BLink pin set must be unchanged (the BLink pin call itself failed)",
                    blinkPinsBefore, blinkPinsAfter);
            assertEquals("hash index pin set must equal pre-failure state",
                    hashPinsBefore, hashPinsAfter);
        }
    }

    @Test
    public void doCheckpointFailingMidSecondaryIndexLoop_rollsBackOnlyEarlierSecondaryIndexPins()
            throws Exception {
        // Inject failure ON THE SECOND secondary-index pin call. Two
        // pre-existing secondary indexes (hash + brin) are present,
        // so doCheckpoint loops over them. The first index pin
        // succeeds; the second throws. The rollback contract: the
        // first index's pin is rolled back; nothing else is held.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        FailingDsm dsm = new FailingDsm(dataPath);
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir, dsm)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null, n1 int, n2 int)",
                    Collections.emptyList());
            for (int i = 0; i < 5; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec, n1, n2) VALUES (?, ?, ?, ?)",
                        Arrays.asList(i, new float[]{i * 0.1f, 0f, 0f}, i, i * 2));
            }
            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n1)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE BRIN INDEX bidx ON tblspace1.t1(n2)",
                    Collections.emptyList());

            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1").getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            String hashIdxUuid = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("hidx").getIndex().uuid;
            String brinIdxUuid = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("bidx").getIndex().uuid;
            String blinkIndexName = BLinkKeyToPageIndex.deriveIndexName(tableUuid);

            Set<LogSequenceNumber> tablePinsBefore = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);
            Set<LogSequenceNumber> brinPinsBefore = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, brinIdxUuid);

            // Arm: fail on the SECOND indexCheckpoint call for ANY
            // user secondary index (i.e. names that do NOT end in
            // "_primary"). Set the counter to 2 so the first call is
            // skipped (counter decrements to 1, no fail) and the
            // second call fires (counter decrements to 0, fail).
            // Wait — getAndUpdate(n -> max(0, n-1)) fails when the
            // current value is > 0, so a counter of 1 fires on the
            // first matching call and a counter of 2 fires on the
            // first AND second. We want only the second, so we use
            // a small wrapping predicate that skips the first match.
            final AtomicInteger matchCount = new AtomicInteger(0);
            dsm.failIndexCheckpointWhen = name -> {
                if (name.endsWith("_primary")) {
                    return false;
                }
                // First user-index call is skipped; second fires.
                return matchCount.incrementAndGet() == 2;
            };
            dsm.failNextIndexCheckpoints.set(1);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("CREATE VECTOR INDEX must fail when 2nd secondary indexCheckpoint throws");
            } catch (RuntimeException expected) {
                // expected
            }

            Set<LogSequenceNumber> tablePinsAfter = dsm
                    .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            Set<LogSequenceNumber> blinkPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            Set<LogSequenceNumber> hashPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);
            Set<LogSequenceNumber> brinPinsAfter = dsm
                    .pinnedIndexCheckpointsForTest(tableSpaceUuid, brinIdxUuid);

            // Table pin: never reached, must be unchanged.
            assertEquals("table pin set must be unchanged",
                    tablePinsBefore, tablePinsAfter);
            // BLink pin: never reached, must be unchanged.
            assertEquals("BLink pin set must be unchanged",
                    blinkPinsBefore, blinkPinsAfter);
            // First secondary index pin (whichever was visited first):
            // its rollback must have run. Both hash and brin pin sets
            // must equal their pre-failure state. We don't know which
            // index doCheckpoint visited first (Map ordering is not
            // guaranteed) — both must be unchanged either way.
            assertEquals("hash index pin set must equal pre-failure state",
                    hashPinsBefore, hashPinsAfter);
            assertEquals("brin index pin set must equal pre-failure state",
                    brinPinsBefore, brinPinsAfter);
        }
    }

    @Test
    public void doCheckpointFailingAtTablePinSite_clearsCheckPointRunningFlag()
            throws Exception {
        // Sibling test — even if the post-tableCheckpoint failure left
        // checkPointRunning=true, TRUNCATE TABLE on this table would
        // permanently reject the call (see issue #403). Verify the flag
        // is cleared after rollback by issuing a TRUNCATE on the same
        // table.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        FailingDsm dsm = new FailingDsm(dataPath);
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir, dsm)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec) VALUES (?, ?)",
                    Arrays.asList(1, new float[]{1.0f, 2.0f, 3.0f}));

            dsm.failTableCheckpointForTable = "t1";
            dsm.failNextTableCheckpoints.set(1);
            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("CREATE VECTOR INDEX must fail when tableCheckpoint throws");
            } catch (RuntimeException expected) {
                // expected
            }

            // TRUNCATE TABLE would throw if checkPointRunning=true had
            // leaked from the failed checkpoint. The fact that this
            // succeeds is the load-bearing assertion that the outer
            // finally still ran the existing (issue #403)
            // checkPointRunning = false branch.
            execute(manager,
                    "TRUNCATE TABLE tblspace1.t1",
                    Collections.emptyList());
        }
    }
}
