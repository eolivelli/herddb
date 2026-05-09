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

import static herddb.core.TestUtils.execute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.core.TableSpaceManager;
import herddb.core.indexes.MockRemoteVectorIndexService;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.RemoteVectorIndexService;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Issue #471 — verifies that the leader's rebuild-checkpoint pin
 * (taken by {@code TableSpaceManager.createIndex} on a non-empty
 * table) is released by the next tablespace checkpoint after the
 * IndexingService publishes a {@code minProcessedLsn} past the
 * pinned LSN.
 *
 * <p>Without this release, every {@code CREATE VECTOR INDEX} on a
 * non-empty table would leave a permanent in-memory pin in the
 * leader's {@link herddb.storage.DataStorageManager} pin maps,
 * preventing the activator's normal page reclamation from cleaning
 * up the pinned table-checkpoint pages until the leader process
 * restarts. Bounded but observable disk leak.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexRebuildPinReleaseTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    /**
     * Stub {@link RemoteVectorIndexService} whose
     * {@code getMinProcessedLsn} returns whatever the test sets via
     * {@link #publishedMinLsn}. Mimics the IndexingService publishing
     * its watermark to the leader's catch-up retention floor logic.
     */
    private static final class StubbableRemoteVectorIndexService
            extends MockRemoteVectorIndexService {
        final AtomicReference<LogSequenceNumber> publishedMinLsn =
                new AtomicReference<>(LogSequenceNumber.START_OF_TIME);

        @Override
        public Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace) {
            LogSequenceNumber lsn = publishedMinLsn.get();
            return lsn == null ? Optional.empty() : Optional.of(lsn);
        }
    }

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir,
                                   RemoteVectorIndexService remoteService) throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE,
                ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmpDir, null, config, null);
        manager.setRemoteVectorIndexService(remoteService);
        return manager;
    }

    private void bootstrapTablespaceAndTable(DBManager manager) throws Exception {
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"),
                "localhost", 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager,
                "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                Collections.emptyList());
    }

    private void insertSomeRows(DBManager manager, int rows) throws Exception {
        for (int i = 0; i < rows; i++) {
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec) VALUES (?, ?)",
                    Arrays.asList(i, new float[]{i * 0.1f, i * 0.2f, i * 0.3f}));
        }
    }

    private LogSequenceNumber readPinnedLsnFromIndex(DBManager manager) {
        Index idx = manager.getTableSpaceManager("tblspace1")
                .getIndexesOnTable("t1").get("vidx").getIndex();
        String encoded = idx.properties.get(VectorIndexManager.PROP_REBUILD_LSN);
        return VectorIndexManager.decodeRebuildLsn(encoded);
    }

    private Set<LogSequenceNumber> pinnedTableCheckpoints(DBManager manager) {
        TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");
        String tableUuid = tsm.getTableManager("t1").getTable().uuid;
        return manager.getDataStorageManager()
                .pinnedTableCheckpointsForTest(tsm.getTableSpaceUUID(), tableUuid);
    }

    @Test
    public void rebuildPin_releasedAfterIsCatchUp_onNextCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        StubbableRemoteVectorIndexService remoteService =
                new StubbableRemoteVectorIndexService();
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir,
                remoteService)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            // CREATE VECTOR INDEX on a non-empty table → server-side
            // pinned checkpoint + rebuild=true marker on the Index.
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            LogSequenceNumber rebuildLsn = readPinnedLsnFromIndex(manager);
            TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");

            // Initial state: pin is held + tracked in
            // pendingRebuildPins; IS has not caught up yet.
            assertEquals("createIndex must register exactly 1 pending rebuild pin",
                    1, tsm.pendingRebuildPinCountForTest());
            assertTrue("rebuild pin at " + rebuildLsn + " must be held",
                    pinnedTableCheckpoints(manager).contains(rebuildLsn));

            // Run a tablespace checkpoint while the IS is still
            // "behind" (publishedMinLsn = START_OF_TIME). The
            // release path must NOT unpin in this state — the IS
            // could still need the pinned pages for its scan.
            remoteService.publishedMinLsn.set(LogSequenceNumber.START_OF_TIME);
            manager.checkpoint();
            assertEquals("IS unreachable / behind: pin must remain held",
                    1, tsm.pendingRebuildPinCountForTest());
            assertTrue("pinned checkpoint must still be held while IS is behind",
                    pinnedTableCheckpoints(manager).contains(rebuildLsn));

            // Now publish an IS catch-up LSN PAST the rebuild LSN.
            // The next checkpoint must release the pin.
            LogSequenceNumber pastRebuild = new LogSequenceNumber(
                    rebuildLsn.ledgerId, rebuildLsn.offset + 100L);
            remoteService.publishedMinLsn.set(pastRebuild);
            manager.checkpoint();

            assertEquals("rebuild pin must be released after IS caught up past it",
                    0, tsm.pendingRebuildPinCountForTest());
            assertTrue("dataStorageManager pin set must no longer contain "
                            + rebuildLsn + " (got " + pinnedTableCheckpoints(manager) + ")",
                    !pinnedTableCheckpoints(manager).contains(rebuildLsn));
        }
    }

    @Test
    public void rebuildPin_NOT_releasedAtExactEqualLsn() throws Exception {
        // Boundary: tailersFloor.equals(pinLsn) must NOT release.
        // Reason: the IS-side rebuild runs synchronously inside
        // applyEntry for the CREATE_INDEX entry; while it is
        // in-flight, lastProcessedLsn has NOT yet advanced past
        // pinLsn. A concurrent IS-side
        // checkpointAndSaveWatermark could publish
        // lastDurableLsn = pinLsn at that moment. An at-equal
        // release would unpin while the rebuild is still
        // mid-scan, leading to PageNotFoundException and
        // permanent rebuild failure. Strict-greater closes the
        // window — see TableSpaceManager.releaseCompletedRebuildPins
        // javadoc for the full rationale.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        StubbableRemoteVectorIndexService remoteService =
                new StubbableRemoteVectorIndexService();
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir,
                remoteService)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            LogSequenceNumber rebuildLsn = readPinnedLsnFromIndex(manager);
            TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");
            assertEquals(1, tsm.pendingRebuildPinCountForTest());

            // Publish exactly the pin LSN — this models the race
            // where the IS publishes lastDurableLsn = pinLsn while
            // the rebuild is still in-flight.
            remoteService.publishedMinLsn.set(rebuildLsn);
            manager.checkpoint();

            assertEquals("pin MUST NOT release at exact-equal IS catch-up LSN — "
                            + "the IS rebuild may still be mid-scan",
                    1, tsm.pendingRebuildPinCountForTest());
            assertTrue("DSM table-checkpoint pin must still hold rebuildLsn",
                    pinnedTableCheckpoints(manager).contains(rebuildLsn));
        }
    }

    @Test
    public void rebuildPin_releasedAtPinLsnPlusOne() throws Exception {
        // Positive boundary symmetric to
        // rebuildPin_NOT_releasedAtExactEqualLsn: a tailersFloor
        // strictly one tick past the pin LSN MUST release. This
        // is the smallest "rebuild has completed" signal: the IS's
        // applyEntry for CREATE_INDEX has returned (rebuild done)
        // AND a subsequent watermark save has captured a later
        // lastProcessedLsn.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        StubbableRemoteVectorIndexService remoteService =
                new StubbableRemoteVectorIndexService();
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir,
                remoteService)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            LogSequenceNumber rebuildLsn = readPinnedLsnFromIndex(manager);
            TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");
            assertEquals(1, tsm.pendingRebuildPinCountForTest());

            LogSequenceNumber pinLsnPlusOne = new LogSequenceNumber(
                    rebuildLsn.ledgerId, rebuildLsn.offset + 1L);
            remoteService.publishedMinLsn.set(pinLsnPlusOne);
            manager.checkpoint();

            assertEquals("pin must release when IS catch-up is strictly past pinLsn",
                    0, tsm.pendingRebuildPinCountForTest());
        }
    }

    @Test
    public void rebuildPin_neverReleasedBeforeIsCatchUp() throws Exception {
        // Three checkpoint cycles with publishedMinLsn one-tick
        // behind the rebuild LSN. The pin must remain held
        // throughout — the IS has not yet durably persisted its
        // post-rebuild state.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        StubbableRemoteVectorIndexService remoteService =
                new StubbableRemoteVectorIndexService();
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir,
                remoteService)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            LogSequenceNumber rebuildLsn = readPinnedLsnFromIndex(manager);
            // One LSN tick before the rebuild LSN.
            LogSequenceNumber justBefore = rebuildLsn.offset > 0
                    ? new LogSequenceNumber(rebuildLsn.ledgerId, rebuildLsn.offset - 1L)
                    : new LogSequenceNumber(rebuildLsn.ledgerId - 1L, Long.MAX_VALUE);
            remoteService.publishedMinLsn.set(justBefore);

            TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");
            for (int i = 0; i < 3; i++) {
                manager.checkpoint();
                assertEquals("checkpoint cycle " + i + ": pin must NOT release before IS catch-up",
                        1, tsm.pendingRebuildPinCountForTest());
            }
        }
    }

    @Test
    public void multipleRebuildPins_onSameTable_releasedIndependently() throws Exception {
        // Two CREATE VECTOR INDEX statements back-to-back on the
        // same non-empty table register two distinct pins. Each
        // pin must release when the IS catches up past its own LSN
        // — they must not block each other.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        StubbableRemoteVectorIndexService remoteService =
                new StubbableRemoteVectorIndexService();
        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir,
                remoteService)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"),
                    "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);
            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, "
                            + "vec1 floata not null, vec2 floata not null)",
                    Collections.emptyList());
            for (int i = 0; i < 4; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec1, vec2) VALUES (?, ?, ?)",
                        Arrays.asList(i,
                                new float[]{i * 0.1f, 0f, 0f},
                                new float[]{0f, i * 0.2f, 0f}));
            }
            execute(manager,
                    "CREATE VECTOR INDEX v1 ON tblspace1.t1(vec1)",
                    Collections.emptyList());
            // INSERT another row between the two CREATE VECTOR
            // INDEX statements so the second one pins at a strictly
            // later LSN. Without this advance the
            // lastAppliedSequenceNumber stays at the post-INSERT
            // LSN (CREATE_INDEX is a tablespace-level DDL that does
            // NOT advance the table-manager's
            // lastAppliedSequenceNumber), so both pins would land
            // at the same LSN and the DSM's pin set would
            // (correctly) contain a single entry.
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec1, vec2) VALUES (?, ?, ?)",
                    Arrays.asList(99,
                            new float[]{0.9f, 0f, 0f},
                            new float[]{0f, 0.9f, 0f}));
            execute(manager,
                    "CREATE VECTOR INDEX v2 ON tblspace1.t1(vec2)",
                    Collections.emptyList());

            TableSpaceManager tsm = manager.getTableSpaceManager("tblspace1");
            assertEquals("two CREATE VECTOR INDEX (with intervening INSERT) "
                            + "must register 2 pending pins at distinct LSNs",
                    2, tsm.pendingRebuildPinCountForTest());

            Index idx1 = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("v1").getIndex();
            Index idx2 = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("v2").getIndex();
            LogSequenceNumber lsn1 = VectorIndexManager.decodeRebuildLsn(
                    idx1.properties.get(VectorIndexManager.PROP_REBUILD_LSN));
            LogSequenceNumber lsn2 = VectorIndexManager.decodeRebuildLsn(
                    idx2.properties.get(VectorIndexManager.PROP_REBUILD_LSN));
            // The second CREATE INDEX's pin must be at a strictly
            // later LSN than the first.
            assertTrue("v2 pin LSN must be after v1 pin LSN", lsn2.after(lsn1));

            // Publish IS catch-up STRICTLY past v1's LSN (using
            // v2's LSN as the catch-up point — it's strictly
            // greater than v1's). Only v1's pin must release; v2's
            // pin must remain (tailersFloor == v2's pinLsn ≠
            // strictly-greater).
            remoteService.publishedMinLsn.set(lsn2);
            manager.checkpoint();
            assertEquals("only v1's pin must release on IS catch-up strictly past v1 LSN",
                    1, tsm.pendingRebuildPinCountForTest());

            // Publish IS catch-up strictly past v2's LSN. v2's pin
            // must release.
            LogSequenceNumber pastV2 = new LogSequenceNumber(lsn2.ledgerId, lsn2.offset + 1L);
            remoteService.publishedMinLsn.set(pastV2);
            manager.checkpoint();
            assertEquals("v2's pin must release after IS caught up strictly past v2 LSN",
                    0, tsm.pendingRebuildPinCountForTest());
        }
    }
}
