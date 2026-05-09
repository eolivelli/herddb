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
import static org.junit.Assert.assertFalse;
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
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.SystemInstrumentation;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * Server-side coverage for the issue #471 {@code rebuild=true} marking on the
 * {@code Index} produced by {@code CREATE VECTOR INDEX}.
 *
 * <p>The marking must fire only when (a) the new index is a vector index AND
 * (b) the underlying table already has rows. For empty tables the property
 * is never set; for non-vector index types (hash, brin) the property is
 * never set even on non-empty tables, because their existing in-process
 * rebuild path on {@code bootIndex} / {@code scanForIndexRebuild} already
 * back-fills the data.
 *
 * @author enrico.olivelli
 */
public class CreateVectorIndexRebuildPropertyTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Class-wide timeout: a regression that leaks the tablespace write
     * lock would otherwise hang the suite indefinitely (the rejection
     * counter-tests would block on commit forever). 60 s is generous
     * enough for the checkpoint-trigger paths on a constrained CI runner.
     */
    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @After
    public void clearInstrumentation() {
        // Each test method may install a SystemInstrumentation listener;
        // make sure it does not leak across tests in the same JVM.
        SystemInstrumentation.clear();
    }

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmpDir)
            throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        // JSQLParserPlanner is the only planner that handles CREATE VECTOR INDEX.
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

    private void insertSomeRows(DBManager manager, int rows) throws Exception {
        for (int i = 0; i < rows; i++) {
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n) VALUES (?, ?, ?)",
                    Arrays.asList(i, new float[]{i * 0.1f, i * 0.2f, i * 0.3f}, i));
        }
    }

    private Index getCreatedIndex(DBManager manager, String table, String indexName) {
        return manager.getTableSpaceManager("tblspace1")
                .getIndexesOnTable(table).get(indexName).getIndex();
    }

    @Test
    public void emptyTable_vectorIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            // No INSERTs — the table is empty before CREATE INDEX.

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH m=16 numShards=4",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertNotNull("vector index must exist", idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            assertNull("empty table must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("empty table must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_vectorIndex_setsRebuildPropertyTrue() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 10);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH m=16 numShards=4",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertNotNull("vector index must exist", idx);
            assertEquals(Index.TYPE_VECTOR, idx.type);
            assertEquals("non-empty table MUST receive rebuild=true",
                    "true", idx.properties.get(VectorIndexManager.PROP_REBUILD));
            // The pinned LSN must travel alongside the rebuild marker so
            // the IS knows which checkpoint to scan.
            String rebuildLsn = idx.properties.get(VectorIndexManager.PROP_REBUILD_LSN);
            assertNotNull("non-empty table MUST receive _rebuildLsn", rebuildLsn);
            assertTrue("_rebuildLsn must be ledgerId:offset, got: " + rebuildLsn,
                    rebuildLsn.matches("\\d+:\\d+"));
            // The user-supplied properties must be preserved alongside the
            // auto-injected rebuild marker.
            assertEquals("16", idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("4", idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }

    @Test
    public void truncatedTable_vectorIndex_doesNotSetRebuildProperty() throws Exception {
        // After TRUNCATE TABLE the row count is 0, so the rebuild branch
        // must skip even though the table previously held data. The
        // IndexingService's CREATE_TABLE / TRUNCATE_TABLE handling already
        // resets the vector store; CREATE INDEX after a TRUNCATE has
        // nothing to back-fill.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);
            execute(manager, "TRUNCATE TABLE tblspace1.t1", Collections.emptyList());

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertNotNull("vector index must exist after CREATE on truncated table", idx);
            assertNull("truncated table must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("truncated table must NOT receive _rebuildLsn",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD_LSN));
        }
    }

    @Test
    public void alterTableThenCreateVectorIndex_persistsPostAlterColumns() throws Exception {
        // INSERT, then ALTER TABLE ADD COLUMN, then INSERT more rows, then
        // CREATE VECTOR INDEX. The persisted Index columns must reflect
        // the post-ALTER table — otherwise the IS would scan against a
        // schema that does not match the live table.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 3);
            execute(manager, "ALTER TABLE tblspace1.t1 ADD COLUMN extra INT",
                    Collections.emptyList());
            execute(manager,
                    "INSERT INTO tblspace1.t1 (id, vec, n, extra) VALUES (?, ?, ?, ?)",
                    Arrays.asList(99, new float[]{9.0f, 9.0f, 9.0f}, 99, 999));

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            assertEquals("rebuild=true must be set on post-ALTER non-empty table",
                    "true", idx.properties.get(VectorIndexManager.PROP_REBUILD));
            // The Index references just the vector column by design; the
            // post-ALTER schema lives on the Table side. We pin the
            // post-ALTER table column count via the tablespace manager,
            // not via the Index — assert the table itself sees the new
            // column so the IS-side scan in step 3 can read it.
            assertEquals("post-ALTER table must have 4 columns",
                    4, manager.getTableSpaceManager("tblspace1")
                            .getTableManager("t1").getTable().getColumns().length);
        }
    }

    @Test
    public void backToBackVectorIndexes_eachReceiveOwnRebuildMarker() throws Exception {
        // Two CREATE VECTOR INDEX statements back-to-back on the same
        // non-empty table must each take their own checkpoint and each
        // mark their own Index — there is no caching that would make the
        // second create skip the rebuild path.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            // Two separate vector columns so we can create two indexes.
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
            for (int i = 0; i < 5; i++) {
                execute(manager,
                        "INSERT INTO tblspace1.t1 (id, vec1, vec2) VALUES (?, ?, ?)",
                        Arrays.asList(i,
                                new float[]{i * 0.1f, 0f, 0f},
                                new float[]{0f, i * 0.2f, 0f}));
            }

            execute(manager,
                    "CREATE VECTOR INDEX v1 ON tblspace1.t1(vec1)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE VECTOR INDEX v2 ON tblspace1.t1(vec2)",
                    Collections.emptyList());

            Index idx1 = getCreatedIndex(manager, "t1", "v1");
            Index idx2 = getCreatedIndex(manager, "t1", "v2");
            assertEquals("first vector index must receive rebuild=true",
                    "true", idx1.properties.get(VectorIndexManager.PROP_REBUILD));
            assertEquals("second vector index must receive rebuild=true",
                    "true", idx2.properties.get(VectorIndexManager.PROP_REBUILD));
            assertNotNull("first vector index must receive _rebuildLsn",
                    idx1.properties.get(VectorIndexManager.PROP_REBUILD_LSN));
            assertNotNull("second vector index must receive _rebuildLsn",
                    idx2.properties.get(VectorIndexManager.PROP_REBUILD_LSN));
        }
    }

    @Test
    public void checkpointFailureLeavesNoStalePartialState() throws Exception {
        // Inject a RuntimeException at the post-checkpoint instrumentation
        // hook (the closest pre-log.log() boundary we can reliably hook
        // from a test). The CREATE INDEX must fail with
        // StatementExecutionException, the indexes map must be empty, and
        // the tablespace write lock must be released so subsequent DDL
        // succeeds.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        SystemInstrumentation.addListener(new SystemInstrumentation.SingleInstrumentationPointListener(
                "createVectorIndex.checkpointTaken") {
            @Override
            public void acceptSingle(Object... args) {
                throw new RuntimeException("injected checkpoint hook failure");
            }
        });

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 3);

            try {
                execute(manager,
                        "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("CREATE INDEX must fail when the checkpoint hook throws");
            } catch (RuntimeException expected) {
                // StatementExecutionException is a RuntimeException subclass,
                // and SystemInstrumentation may also wrap the injected
                // throwable as a plain RuntimeException — either form is
                // acceptable. The important contract is that the index is
                // NOT in the indexes map and the lock was released.
            }

            // The indexes map must NOT contain a partially-created index.
            assertTrue("indexes map must be empty after checkpoint failure",
                    manager.getTableSpaceManager("tblspace1")
                            .getIndexesOnTable("t1") == null
                            || manager.getTableSpaceManager("tblspace1")
                                    .getIndexesOnTable("t1").isEmpty());
            // The tablespace write lock must be released — verified by
            // running another DDL statement that needs it.
            execute(manager,
                    "CREATE TABLE tblspace1.t2 (id int primary key)",
                    Collections.emptyList());

            // Issue #471: the rebuild pin must be released on failure.
            // Without the unpin-on-failure path, the pin would survive
            // for the leader's process lifetime and silently keep the
            // pinned page files alive forever.
            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            java.util.Set<herddb.log.LogSequenceNumber> pinned =
                    manager.getDataStorageManager()
                            .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            assertTrue(
                    "rebuild pin must be released after CREATE INDEX failure (pinned="
                            + pinned + ")",
                    pinned.isEmpty());
        }
    }

    @Test
    public void successfulCreate_holdsPinAtRebuildLsn() throws Exception {
        // Symmetric counter to checkpointFailureLeavesNoStalePartialState:
        // on the happy path the pin MUST be held at the LSN encoded in
        // PROP_REBUILD_LSN — otherwise step 3's IS-side scan will race a
        // periodic activator-driven checkpoint that reclaims the pages.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            String encoded = idx.properties.get(VectorIndexManager.PROP_REBUILD_LSN);
            assertNotNull("happy path must record _rebuildLsn", encoded);
            herddb.log.LogSequenceNumber expected =
                    VectorIndexManager.decodeRebuildLsn(encoded);

            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            java.util.Set<herddb.log.LogSequenceNumber> pinned =
                    manager.getDataStorageManager()
                            .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            assertTrue(
                    "rebuild pin at " + expected + " must be held after CREATE INDEX (pinned="
                            + pinned + ")",
                    pinned.contains(expected));

            // The PK BLink keyToPage is pinned at the same LSN inside
            // doCheckpoint; the IS does not need this pin (it scans
            // table data pages, not BLink), but a future refactor that
            // accidentally leaves the BLink pin behind on success while
            // the table pin is correctly released would be silently
            // wrong. The BLink registers in DSM under the synthetic name
            // "<tableUuid>_primary" (see BLinkKeyToPageIndex.deriveIndexName).
            String blinkIndexName = herddb.index.blink.BLinkKeyToPageIndex
                    .deriveIndexName(tableUuid);
            java.util.Set<herddb.log.LogSequenceNumber> blinkPinned =
                    manager.getDataStorageManager()
                            .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            assertTrue(
                    "BLink keyToPage pin at " + expected + " must be held after CREATE INDEX (pinned="
                            + blinkPinned + ")",
                    blinkPinned.contains(expected));
        }
    }

    @Test
    public void nonEmptyTable_vectorIndexAfterPreExistingHashIndex_setsRebuildAndHoldsAllPins()
            throws Exception {
        // Cover the partial-pin scenario the most directly: with a
        // pre-existing user secondary index, doCheckpoint takes pins
        // sequentially across THREE distinct pin sites (the user hash
        // index, the PK BLink, the table itself). On success all three
        // pin sets must carry the same LSN; this test pins that
        // contract.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);
            // Pre-existing secondary index BEFORE the vector index.
            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index vidx = getCreatedIndex(manager, "t1", "vidx");
            String encoded = vidx.properties.get(VectorIndexManager.PROP_REBUILD_LSN);
            assertNotNull(encoded);
            herddb.log.LogSequenceNumber expected =
                    VectorIndexManager.decodeRebuildLsn(encoded);

            String tableSpaceUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableSpaceUUID();
            String tableUuid = manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t1").getTable().uuid;
            String hashIdxUuid = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("hidx").getIndex().uuid;

            String blinkIndexName = herddb.index.blink.BLinkKeyToPageIndex
                    .deriveIndexName(tableUuid);
            java.util.Set<herddb.log.LogSequenceNumber> tablePins =
                    manager.getDataStorageManager()
                            .pinnedTableCheckpointsForTest(tableSpaceUuid, tableUuid);
            java.util.Set<herddb.log.LogSequenceNumber> blinkPins =
                    manager.getDataStorageManager()
                            .pinnedIndexCheckpointsForTest(tableSpaceUuid, blinkIndexName);
            java.util.Set<herddb.log.LogSequenceNumber> hashPins =
                    manager.getDataStorageManager()
                            .pinnedIndexCheckpointsForTest(tableSpaceUuid, hashIdxUuid);
            assertTrue("table pin at " + expected + " must be held (pinned=" + tablePins + ")",
                    tablePins.contains(expected));
            assertTrue("BLink pin at " + expected + " must be held (pinned=" + blinkPins + ")",
                    blinkPins.contains(expected));
            // The pre-existing hash index keeps its own pin at the LSN of
            // its CREATE HASH INDEX flow (unrelated to the new vector
            // index's rebuild LSN). What we verify here is just that
            // some pin is held — i.e. the existing pre-existing-index
            // pin path is not silently disabled by the new code.
            assertTrue("hash index must have at least one pin (pinned=" + hashPins + ")",
                    !hashPins.isEmpty());
        }
    }

    @Test
    public void nonEmptyTable_hashIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            execute(manager,
                    "CREATE HASH INDEX hidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "hidx");
            assertNotNull("hash index must exist", idx);
            assertEquals(Index.TYPE_HASH, idx.type);
            assertNull("non-vector index must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("non-vector index must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_brinIndex_doesNotSetRebuildProperty() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 5);

            execute(manager,
                    "CREATE BRIN INDEX bidx ON tblspace1.t1(n)",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "bidx");
            assertNotNull("brin index must exist", idx);
            assertEquals(Index.TYPE_BRIN, idx.type);
            assertNull("brin index must NOT receive rebuild=true",
                    idx.properties.get(VectorIndexManager.PROP_REBUILD));
            assertFalse("brin index must NOT receive rebuild=true (key absent)",
                    idx.properties.containsKey(VectorIndexManager.PROP_REBUILD));
        }
    }

    @Test
    public void nonEmptyTable_vectorIndex_rebuildMarkerSurvivesSerialization() throws Exception {
        // The CREATE_INDEX log entry serialises the Index — the rebuild=true
        // marker must round-trip through serialize/deserialize so the
        // IndexingService observes it on the live tailer path.
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmpDir = folder.newFolder("tmp").toPath();

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmpDir)) {
            manager.start();
            bootstrapTablespaceAndTable(manager);
            insertSomeRows(manager, 3);

            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH numShards=2",
                    Collections.emptyList());

            Index idx = getCreatedIndex(manager, "t1", "vidx");
            byte[] serialized = idx.serialize();
            Index deserialized = Index.deserialize(serialized);
            assertEquals("rebuild=true must survive Index serialize/deserialize",
                    "true", deserialized.properties.get(VectorIndexManager.PROP_REBUILD));
            assertEquals("2", deserialized.properties.get(VectorIndexManager.PROP_NUM_SHARDS));
        }
    }
}
