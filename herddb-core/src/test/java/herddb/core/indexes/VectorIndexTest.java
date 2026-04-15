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

package herddb.core.indexes;

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.AbstractIndexManager;
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.VectorIndexManager;
import herddb.model.ColumnTypes;
import herddb.model.DataScanner;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.VectorANNScanOp;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the vector index with remote IndexingService delegation.
 */
public class VectorIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(String nodeId, Path dataPath, Path logsPath,
                                    Path metadataPath, Path tmoDir) throws Exception {
        DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null);
        manager.setRemoteVectorIndexService(new MockRemoteVectorIndexService());
        return manager;
    }

    /**
     * Basic lifecycle: create table + vector index, insert rows, checkpoint, restart.
     */
    @Test
    public void testCreateIndexCheckpointRestart() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        // Phase 1: create, insert, checkpoint
        try (DBManager manager = buildManager(nodeId, dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            Index index = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull("index must be present", index);
            assertEquals(Index.TYPE_VECTOR, index.type);

            for (int i = 1; i <= 10; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(4, i)));
            }

            manager.checkpoint();
        }

        // Phase 2: restart, verify index is present
        try (DBManager manager = buildManager(nodeId, dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            Map<String, AbstractIndexManager> indexes =
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1");
            assertTrue("vidx must be present after restart", indexes.containsKey("vidx"));
            assertEquals(Index.TYPE_VECTOR, indexes.get("vidx").getIndex().type);
        }
    }

    /**
     * Tests that DML operations (insert, update, delete) succeed without errors
     * in remote mode.
     */
    @Test
    public void testInsertUpdateDelete() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert
            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }

            // Update
            executeUpdate(manager, "UPDATE tblspace1.t1 SET vec=? WHERE id=?",
                    Arrays.asList(new float[]{0.0f, 1.0f, 0.0f}, 1));

            // Delete
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(2));

            // Checkpoint should not fail
            manager.checkpoint();
        }
    }

    /**
     * Verifies empty index checkpoint works without errors.
     */
    @Test
    public void testEmptyIndexCheckpointRestart() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            manager.checkpoint();
        }

        // Restart: index must still exist
        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            Map<String, AbstractIndexManager> indexes =
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1");
            assertTrue("vidx must survive restart", indexes.containsKey("vidx"));
        }
    }

    /**
     * Tests that search delegates to remote service via the mock.
     */
    @Test
    public void testVectorIndexSearch() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.setRemoteVectorIndexService(mockService);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Populate mock with vectors (remote mode: DML is no-op on VectorIndexManager)
            float[] vecX = {1.0f, 0.0f, 0.0f};
            float[] vecY = {0.0f, 1.0f, 0.0f};
            float[] vecZ = {0.0f, 0.0f, 1.0f};

            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(1), vecX);
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(2), vecY);
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(3), vecZ);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);

            // Query close to X axis
            float[] query = {0.99f, 0.1f, 0.0f};
            normalize(query);

            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(query, 2);
            assertEquals("Expected 2 results", 2, results.size());
            assertTrue("Top result should have higher score than second",
                    results.get(0).getValue() >= results.get(1).getValue());
        }
    }

    /**
     * Verifies that ann_of() function works for ORDER BY without a vector index
     * (brute-force cosine similarity).
     */
    @Test
    public void testAnnOfBruteForce() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        // Brute-force path doesn't use vector index, so no mock needed
        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            float[] vecX = {1.0f, 0.0f, 0.0f};
            float[] vecY = {0.0f, 1.0f, 0.0f};
            float[] vecZ = {0.0f, 0.0f, 1.0f};
            float[] query = {0.1f, 0.9f, 0.0f};
            normalize(query);

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 10",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                assertEquals(2, results.get(0).get("id"));
            }
        }
    }

    /**
     * Verifies that Index.serialize()/deserialize() round-trips the properties map correctly.
     */
    @Test
    public void testIndexSerializationV2() {
        Index original = Index.builder()
                .name("testidx")
                .table("t1")
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, "32")
                .property(VectorIndexManager.PROP_BEAM_WIDTH, "200")
                .property(VectorIndexManager.PROP_FUSED_PQ, "false")
                .build();

        byte[] bytes = original.serialize();
        Index restored = Index.deserialize(bytes);

        assertEquals("32", restored.properties.get(VectorIndexManager.PROP_M));
        assertEquals("200", restored.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
        assertEquals("false", restored.properties.get(VectorIndexManager.PROP_FUSED_PQ));
    }

    /**
     * Tests that CREATE VECTOR INDEX ... WITH clause correctly stores hyperparameters
     * and they survive checkpoint+restart.
     */
    @Test
    public void testCustomHyperparameters() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH m=32 beamWidth=200 fusedPQ=false",
                    Collections.emptyList());

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull(idx);
            assertEquals("32", idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("200", idx.properties.get(VectorIndexManager.PROP_BEAM_WIDTH));
            assertEquals("false", idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));

            manager.checkpoint();
        }

        // Properties must survive restart
        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            Index idx = manager.getTableSpaceManager("tblspace1")
                    .getIndexesOnTable("t1").get("vidx").getIndex();
            assertEquals("m property must survive restart", "32",
                    idx.properties.get(VectorIndexManager.PROP_M));
            assertEquals("fusedPQ property must survive restart", "false",
                    idx.properties.get(VectorIndexManager.PROP_FUSED_PQ));
        }
    }

    /**
     * Verifies that the execution plan for ORDER BY ann_of() DESC LIMIT k
     * uses VectorANNScanOp with limit pushed down.
     */
    @Test
    public void testVectorIndexUsedInPlan() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            float[] query = {0.05f, 0.99f, 0.0f};
            normalize(query);

            String sql = "SELECT id FROM tblspace1.t1"
                    + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2";
            TranslatedQuery translated = manager.getPlanner().translate(
                    TableSpace.DEFAULT, sql, Arrays.asList((Object) query),
                    true, true, true, -1);
            PlannerOp root = translated.plan.originalRoot;

            VectorANNScanOp vecOp = findVectorANNScanOp(root);
            assertNotNull("Plan must contain VectorANNScanOp, but got: " + root, vecOp);
            assertTrue("Limit must be pushed into VectorANNScanOp", vecOp.hasLimit());
        }
    }

    /**
     * Tests that creating a vector index without a RemoteVectorIndexService fails.
     */
    @Test
    public void testFailsWithoutRemoteService() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            // No remoteVectorIndexService set
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            try {
                execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                        Collections.emptyList());
                fail("Expected exception when creating vector index without RemoteVectorIndexService");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("RemoteVectorIndexService is required")
                        || e.getCause().getMessage().contains("RemoteVectorIndexService is required"));
            }
        }
    }

    /**
     * Simulates the window where the remote indexing service has not yet
     * consumed a DELETE from the CommitLog. The vector index still returns
     * the PK of a row that no longer exists in the table; VectorANNScanOp
     * must silently skip those entries and return fewer rows, not crash.
     */
    @Test
    public void testStalePkFromVectorIndexIsSkipped() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();

        try (DBManager manager = new DBManager("localhost",
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.setRemoteVectorIndexService(mockService);
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            float[] vecX = {1.0f, 0.0f, 0.0f};
            float[] vecY = {0.0f, 1.0f, 0.0f};
            float[] vecZ = {0.0f, 0.0f, 1.0f};

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // Pretend the indexing service has caught up through the inserts.
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(1), vecX);
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(2), vecY);
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(3), vecZ);

            // Delete row id=2 from the table, but leave the entry in the mock:
            // this is the async-lag window the test targets.
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?",
                    Arrays.asList(2));

            // Query vector pointing at vecY — the stale PK ranks first in
            // the mock's result list, so the skip path is exercised on the
            // very first ANN entry.
            float[] queryY = {0.0f, 1.0f, 0.0f};

            // Case 1: explicit LIMIT equal to the number of indexed rows.
            // Limit is pushed into VectorANNScanOp (topK = 3) and the loop
            // must return only the 2 surviving rows, not throw an NPE.
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1"
                            + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 3",
                    Arrays.asList((Object) queryY))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("stale pk must be skipped, expected fewer rows",
                        2, results.size());
                for (DataAccessor row : results) {
                    assertTrue("deleted row id=2 must not appear",
                            ((Number) row.get("id")).intValue() != 2);
                }
            }

            // Case 2: LIMIT larger than the mock's population. Covers the
            // topK = limit + offset path with the same stale-skip outcome.
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1"
                            + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 10",
                    Arrays.asList((Object) queryY))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("stale pk must be skipped under large LIMIT too",
                        2, results.size());
                for (DataAccessor row : results) {
                    assertTrue("deleted row id=2 must not appear",
                            ((Number) row.get("id")).intValue() != 2);
                }
            }

            // Case 3: stale PK sitting in the middle of the result list.
            // Add id=4, delete it, leave the mock untouched. A query close
            // to X ranks id=1 first, id=4 (stale) second, id=2 (stale)
            // third, id=3 last. With LIMIT 10 we fetch all four from the
            // mock and must be left with exactly {1, 3}.
            float[] vecX2 = {0.95f, 0.05f, 0.0f};
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(4, vecX2));
            mockService.addVector("t1", "vidx",
                    herddb.utils.Bytes.from_int(4), vecX2);
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?",
                    Arrays.asList(4));

            float[] queryX = {1.0f, 0.0f, 0.0f};
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1"
                            + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 10",
                    Arrays.asList((Object) queryX))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("two stale pks must be skipped", 2, results.size());
                for (DataAccessor row : results) {
                    int id = ((Number) row.get("id")).intValue();
                    assertTrue("deleted rows 2 and 4 must not appear",
                            id != 2 && id != 4);
                }
            }
        }
    }

    /**
     * Tests transactional DELETE with COMMIT: deleted row's vector must be absent
     * from ANN search results after commit and checkpoint.
     */
    @Test
    public void testTransactionalDeleteCommitRemovesVector() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert 5 rows with distinct vectors
            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }
            manager.checkpoint();

            // Delete row id=3 in a transaction and commit
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(3));
            manager.checkpoint();

            // ANN search should not return id=3
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1"
                            + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 10",
                    Arrays.asList((Object) new float[]{3.0f, 0.0f, 0.0f}))) {
                List<DataAccessor> results = scan.consume();
                for (DataAccessor row : results) {
                    int id = ((Number) row.get("id")).intValue();
                    assertTrue("Deleted id=3 must not appear in ANN results",
                            id != 3);
                }
            }
        }
    }

    /**
     * Tests that multiple independent deletions complete without error.
     * Insert rows with vectors, delete some in individual statements.
     */
    @Test
    public void testMultipleIndependentDeletes() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert 10 rows
            for (int i = 1; i <= 10; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }
            manager.checkpoint();

            // Delete rows 2, 4, 6, 8, 10 in individual statements
            // Verifies that multiple delete operations don't corrupt the vector index
            for (int i = 2; i <= 10; i += 2) {
                executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?",
                        Arrays.asList(i));
            }
            manager.checkpoint();
            // Test passes if no exception is thrown
        }
    }

    /**
     * Tests partial DELETE via WHERE clause: only matching rows are deleted.
     * Insert ids 1–20, delete WHERE id > 10, checkpoint without error.
     */
    @Test
    public void testPartialDeleteByWhereClause() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert ids 1–20
            for (int i = 1; i <= 20; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 0.05f, 0.0f, 0.0f}));
            }
            manager.checkpoint();

            // Delete WHERE id > 10 (deletes ids 11–20)
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id > ?", Arrays.asList(10));
            manager.checkpoint();
            // Test passes if no exception is thrown
        }
    }

    /**
     * Tests that delete and checkpoint sequence completes without error.
     * Insert rows, delete some, checkpoint again.
     */
    @Test
    public void testDeleteAndCheckpointThenSearchIsCorrect() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert 10 rows
            for (int i = 1; i <= 10; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }
            manager.checkpoint();

            // Delete odd ids: 1, 3, 5, 7, 9
            for (int i = 1; i <= 10; i += 2) {
                executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?",
                        Arrays.asList(i));
            }
            manager.checkpoint();
            // Test passes if no exception is thrown
        }
    }

    /**
     * Tests bulk deletion of all rows: verifies DELETE without WHERE clause
     * succeeds and checkpoint completes without error.
     */
    @Test
    public void testBulkDeleteAllRowsAndSearch() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        try (DBManager manager = buildManager("localhost", dataPath, logsPath, metadataPath, tmoDir)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert 20 rows
            for (int i = 1; i <= 20; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(3, i)));
            }
            manager.checkpoint();

            // Delete all rows
            executeUpdate(manager, "DELETE FROM tblspace1.t1", Collections.emptyList());
            manager.checkpoint();
            // Test passes if no exception is thrown
        }
    }

    private static VectorANNScanOp findVectorANNScanOp(PlannerOp op) {
        if (op instanceof VectorANNScanOp) {
            return (VectorANNScanOp) op;
        }
        if (op instanceof ProjectOp) {
            return findVectorANNScanOp(((ProjectOp) op).getInput());
        }
        return null;
    }

    private static void normalize(float[] v) {
        float norm = 0;
        for (float f : v) {
            norm += f * f;
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < v.length; i++) {
                v[i] /= norm;
            }
        }
    }

    private static float[] randomVec(int dim, int seed) {
        float[] v = new float[dim];
        java.util.Random rng = new java.util.Random(seed);
        for (int i = 0; i < dim; i++) {
            v[i] = rng.nextFloat();
        }
        normalize(v);
        return v;
    }
}
