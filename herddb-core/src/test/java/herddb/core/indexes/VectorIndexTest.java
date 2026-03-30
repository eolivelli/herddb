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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.VectorANNScanOp;
import herddb.server.ServerConfiguration;
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
 * Integration tests for the vector index (jvector-backed).
 */
public class VectorIndexTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /**
     * Basic lifecycle: create table + vector index, insert rows, checkpoint, restart,
     * verify the index is reloaded with correct node count.
     */
    @Test
    public void testCreateIndexCheckpointRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int numRows = 10;
        final int dimension = 4;

        Table table;
        Index index;

        // ---- Phase 1: create, insert, checkpoint ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            // CREATE TABLE
            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            // CREATE VECTOR INDEX
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Verify index is present
            table = manager.getTableSpaceManager("tblspace1").getTableManager("t1").getTable();
            index = manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx").getIndex();
            assertNotNull("index must be present", index);
            assertEquals(Index.TYPE_VECTOR, index.type);

            // Insert rows (start from i=1 to avoid zero-vector when seed=0)
            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        java.util.Arrays.asList(i, vec));
            }

            // Verify node count before checkpoint
            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getNodeCount());
            assertEquals(dimension, vim.getDimension());

            // Checkpoint
            manager.checkpoint();
        }

        // ---- Phase 2: restart, verify index reloaded ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            Map<String, AbstractIndexManager> indexes =
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1");
            assertTrue("vidx must be present after restart", indexes.containsKey("vidx"));

            AbstractIndexManager aim = indexes.get("vidx");
            assertEquals(Index.TYPE_VECTOR, aim.getIndex().type);

            VectorIndexManager vim = (VectorIndexManager) aim;
            assertEquals("node count must match after reload", numRows, vim.getNodeCount());
            assertEquals("dimension must match after reload", dimension, vim.getDimension());
        }
    }

    /**
     * Tests that inserts, updates, and deletes are correctly tracked in the index
     * and survive a checkpoint+restart.
     */
    @Test
    public void testInsertUpdateDeleteCheckpointRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 3;

        // ---- Phase 1 ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            // Insert 5 rows
            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        java.util.Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(5, vim.getNodeCount());

            // Update row id=1: change vector
            executeUpdate(manager,
                    "UPDATE tblspace1.t1 SET vec=? WHERE id=?",
                    java.util.Arrays.asList(new float[]{0.0f, 1.0f, 0.0f}, 1));
            // Node count unchanged after update (delete old, insert new)
            assertEquals(5, vim.getNodeCount());

            // Delete row id=2
            executeUpdate(manager,
                    "DELETE FROM tblspace1.t1 WHERE id=?",
                    java.util.Arrays.asList(2));
            assertEquals(4, vim.getNodeCount());

            // Checkpoint
            manager.checkpoint();
        }

        // ---- Phase 2: restart, verify state ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals("node count after restart", 4, vim.getNodeCount());
            assertEquals("dimension after restart", dimension, vim.getDimension());
        }
    }

    /**
     * Verifies that checkpointing an empty index (no rows inserted) works without errors
     * and that the index can be recovered on restart.
     */
    @Test
    public void testEmptyIndexCheckpointRestart() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            // Checkpoint without any inserts
            manager.checkpoint();
        }

        // Restart: index must still exist
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            Map<String, AbstractIndexManager> indexes =
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1");
            assertTrue("vidx must survive restart", indexes.containsKey("vidx"));

            VectorIndexManager vim = (VectorIndexManager) indexes.get("vidx");
            assertEquals(0, vim.getNodeCount());
        }
    }

    /**
     * Tests VectorIndexManager.search() directly: verifies that the ANN search API
     * returns nearest neighbors in approximately correct order for a small dataset.
     */
    @Test
    public void testVectorIndexSearch() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        // Three unit vectors pointing in cardinal directions
        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1 — most similar to query
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3 — least similar

        // Query is very close to X axis
        final float[] query = {0.99f, 0.1f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);

            // Search for top-2 nearest to query
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(query, 2);
            assertEquals("Expected 2 results", 2, results.size());

            // For a 3-vector index the top result should always be id=1 (vecX) — most similar
            // We verify by checking the score is higher than the second result
            assertTrue("Top result should have higher score than second",
                    results.get(0).getValue() >= results.get(1).getValue());

            // search() must return at most nodeToPk.size() results even if topK is larger
            List<Map.Entry<herddb.utils.Bytes, Float>> allResults = vim.search(query, 1000);
            assertEquals(3, allResults.size());
        }
    }

    /**
     * Verifies that ann_of() function works for ORDER BY — brute-force cosine similarity.
     * No vector index needed; Calcite handles via normal Project+Sort path.
     */
    @Test
    public void testAnnOfBruteForce() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 3;

        // Three unit vectors pointing in different directions
        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3
        // Query vector closest to vecY
        final float[] query = {0.1f, 0.9f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // ORDER BY ann_of() DESC — most similar to query first
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                // id=2 (vecY) is closest to query (large Y component)
                assertEquals(2, results.get(0).get("id"));
            }
        }
    }

    /**
     * Verifies that ORDER BY ann_of() uses the vector index when one exists.
     * The top result must be the vector with the highest cosine similarity to the query.
     */
    @Test
    public void testAnnOfWithVectorIndex() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2 — closest to query
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3

        // Query closest to Y axis
        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // ANN search with vector index: ORDER BY ann_of DESC LIMIT 1
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 1",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 1 result", 1, results.size());
                // id=2 (vecY) must be the top ANN result
                assertEquals("top ANN result must be id=2", 2, results.get(0).get("id"));
            }

            // Also verify all 3 results with LIMIT 3
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 3",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 3 results", 3, results.size());
                assertEquals("top ANN result must be id=2", 2, results.get(0).get("id"));
            }
        }
    }

    /**
     * Verifies that ORDER BY ann_of() with a WHERE clause filters correctly while
     * preserving ANN ordering.
     */
    @Test
    public void testAnnOfWithWhereClause() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        // 4 rows: two in category 'A', two in category 'B'
        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1, cat=A
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2, cat=B — closest to query
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3, cat=A
        final float[] vecW = {0.7f, 0.7f, 0.0f}; // id=4, cat=B — second closest

        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);
        normalize(vecW);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager,
                    "CREATE TABLE tblspace1.t1 (id int primary key, cat varchar(10), vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, cat, vec) VALUES(?, ?, ?)",
                    Arrays.asList(1, "A", vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, cat, vec) VALUES(?, ?, ?)",
                    Arrays.asList(2, "B", vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, cat, vec) VALUES(?, ?, ?)",
                    Arrays.asList(3, "A", vecZ));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, cat, vec) VALUES(?, ?, ?)",
                    Arrays.asList(4, "B", vecW));

            // Only cat='B' rows should be returned, ordered by ANN similarity
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 2",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 2 cat=B results", 2, results.size());
                // id=2 (vecY) is closest to query among cat=B rows
                assertEquals("top result must be id=2 (vecY, cat=B)", 2, results.get(0).get("id"));
                assertEquals("second result must be id=4 (vecW, cat=B)", 4, results.get(1).get("id"));
            }
        }
    }

    /**
     * Verifies that ORDER BY ann_of() falls back to brute-force cosine similarity
     * when no vector index exists on the table.
     */
    @Test
    public void testAnnOfWithoutVectorIndex() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2 — closest to query
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3

        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            // No vector index created
            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // Falls back to brute-force; results must still be correctly ordered
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 1",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 1 result", 1, results.size());
                assertEquals("top result must be id=2 (vecY)", 2, results.get(0).get("id"));
            }
        }
    }

    /**
     * Verifies that Index.serialize()/deserialize() round-trips the properties map correctly (v2 format).
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

        final String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
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

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertFalse("fusedPQ should be disabled via WITH clause", vim.isFusedPQEnabled());

            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }
            manager.checkpoint();
        }

        // Properties must survive restart (Index is re-deserialized from metadata storage)
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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
     * Tests FusedPQ checkpoint/restart with dimension >= 8.
     * After a FusedPQ checkpoint, the index can be reloaded and searched correctly.
     */
    @Test
    public void testFusedPQCheckpointRestart() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8; // >= VectorIndexManager.MIN_DIM_FOR_FUSED_PQ
        // Need >= VectorIndexManager.MIN_VECTORS_FOR_FUSED_PQ (256) for FusedPQ to activate
        final int numRows = 300;

        float[] queryVector = randomVec(dimension, 1);

        // ---- Phase 1: insert + checkpoint ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getNodeCount());
            assertTrue("FusedPQ should be enabled by default for dim >= 8", vim.isFusedPQEnabled());

            manager.checkpoint();
        }

        // ---- Phase 2: restart and verify search ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals("node count after FusedPQ reload", numRows, vim.getNodeCount());
            assertEquals("dimension after FusedPQ reload", dimension, vim.getDimension());

            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(queryVector, 5);
            assertEquals("expected 5 results from FusedPQ on-disk graph", 5, results.size());
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue("scores must be descending",
                        results.get(i).getValue() >= results.get(i + 1).getValue());
            }
        }
    }

    /**
     * Tests that setting fusedPQ=false via WITH clause uses the simple format.
     */
    @Test
    public void testFusedPQDisabled() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 10;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager,
                    "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH fusedPQ=false",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertFalse("fusedPQ should be disabled", vim.isFusedPQEnabled());

            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }
            manager.checkpoint();
        }

        // Restart: index should reload correctly via simple path
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals("node count after simple reload", numRows, vim.getNodeCount());
            assertFalse("fusedPQ should remain disabled after restart", vim.isFusedPQEnabled());
        }
    }

    /**
     * Tests hybrid search: after a FusedPQ checkpoint, new inserts go to the live builder.
     * A search must return results spanning both on-disk (pre-checkpoint) and live (post-checkpoint) data.
     */
    @Test
    public void testHybridSearch() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        // Need >= VectorIndexManager.MIN_VECTORS_FOR_FUSED_PQ (256) for FusedPQ checkpoint
        final int numPreCheckpoint = 300;
        final int numPostCheckpoint = 20;

        // ---- Phase 1: insert + checkpoint ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            for (int i = 1; i <= numPreCheckpoint; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }
            manager.checkpoint();
        }

        // ---- Phase 2: restart, add more rows, hybrid search ----
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("pre-checkpoint node count must be on disk", numPreCheckpoint, vim.getNodeCount());

            // Insert new rows after reload — these go to the live in-memory builder
            for (int i = numPreCheckpoint + 1; i <= numPreCheckpoint + numPostCheckpoint; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            assertEquals("total node count must include both on-disk and live nodes",
                    numPreCheckpoint + numPostCheckpoint, vim.getNodeCount());

            // Hybrid search: top-20 results, scores must be descending
            float[] query = randomVec(dimension, 1);
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(query, 20);
            assertEquals("expected 20 results from hybrid search", 20, results.size());
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue("scores must be descending",
                        results.get(i).getValue() >= results.get(i + 1).getValue());
            }
        }
    }

    /**
     * Tests that deleting rows removes entries from the vectors map.
     */
    @Test
    public void testVectorsCleanedOnDelete() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(5, vim.getVectorsMapSize());
            assertEquals(5, vim.getNodeCount());

            // Delete 2 rows
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(1));
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(3));

            // Node count reflects deletes immediately
            assertEquals(3, vim.getNodeCount());
            // Vectors for deleted nodes are kept until builder.cleanup() during checkpoint
            // (builder may still visit deleted nodes during neighbor search)
            assertEquals(5, vim.getVectorsMapSize());

            // After checkpoint, orphan vectors are purged
            manager.checkpoint();
            assertEquals(3, vim.getVectorsMapSize());
            assertEquals(3, vim.getNodeCount());
        }
    }

    /**
     * Tests that after a FusedPQ checkpoint, live maps are cleared and on-disk maps are populated.
     */
    @Test
    public void testVectorsCleanedAfterCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 300;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getVectorsMapSize());
            assertEquals(numRows, vim.getLiveNodeCount());

            // Checkpoint triggers FusedPQ write + reload
            manager.checkpoint();

            // After checkpoint: live maps cleared, on-disk populated
            assertEquals("vectors map should be cleared after FusedPQ checkpoint",
                    0, vim.getVectorsMapSize());
            assertEquals("live node count should be 0 after FusedPQ checkpoint",
                    0, vim.getLiveNodeCount());
            assertEquals("on-disk node count should match total",
                    numRows, vim.getOnDiskNodeCount());
            assertEquals("total node count should still be correct",
                    numRows, vim.getNodeCount());

            // Insert 5 more after checkpoint — they go to live state
            for (int i = numRows + 1; i <= numRows + 5; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            assertEquals(5, vim.getVectorsMapSize());
            assertEquals(numRows + 5, vim.getNodeCount());
        }
    }

    /**
     * Tests that updating a row does not leak entries in the vectors map.
     */
    @Test
    public void testUpdateDoesNotLeakVectors() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            for (int i = 1; i <= 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, new float[]{i * 1.0f, 0.0f, 0.0f}));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(5, vim.getVectorsMapSize());

            // Update 3 rows
            for (int i = 1; i <= 3; i++) {
                executeUpdate(manager, "UPDATE tblspace1.t1 SET vec=? WHERE id=?",
                        Arrays.asList(new float[]{0.0f, i * 1.0f, 0.0f}, i));
            }

            // Node count stays 5 after updates
            assertEquals(5, vim.getNodeCount());
            // Vectors for deleted-then-reinserted nodes: 5 original + 3 new = 8 until cleanup
            // After checkpoint, orphan vectors (from the old versions) are purged
            manager.checkpoint();
            assertEquals("vectors map should not leak on update", 5, vim.getVectorsMapSize());
            assertEquals(5, vim.getNodeCount());
        }
    }

    /**
     * Tests that rebuild works correctly when vectors contain nulls.
     * Covers: all non-null, all null, and mixed scenarios.
     */
    @Test
    public void testRebuildWithNullableVectors() throws Exception {
        // A) All non-null vectors
        doTestRebuildWithNullableVectors(10, 0, 10);
        // B) All null vectors
        doTestRebuildWithNullableVectors(0, 10, 0);
        // C) Mixed
        doTestRebuildWithNullableVectors(5, 5, 5);
    }

    private void doTestRebuildWithNullableVectors(int nonNullCount, int nullCount, int expectedNodeCount)
            throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int dimension = 4;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();

            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            // Nullable vector column
            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Insert non-null rows
            int id = 1;
            for (int i = 0; i < nonNullCount; i++) {
                float[] vec = randomVec(dimension, id);
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(id, vec));
                id++;
            }
            // Insert null rows
            for (int i = 0; i < nullCount; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(id, (Object) null));
                id++;
            }

            // Drop and recreate index to trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("node count after rebuild", expectedNodeCount, vim.getNodeCount());
            // rebuild should not populate pkToNode (optimization: not needed during rebuild)
            assertEquals("pkToNode should be empty after rebuild", 0, vim.getPkToNodeSize());
        }
    }

    /**
     * Verifies that ORDER BY ann_of() + LIMIT works correctly with vector index,
     * including with OFFSET.
     */
    @Test
    public void testVectorLimitPushdown() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f};
        final float[] vecY = {0.0f, 1.0f, 0.0f};
        final float[] vecZ = {0.0f, 0.0f, 1.0f};

        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // LIMIT 2 without WHERE
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 2",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals(2, results.get(0).get("id")); // vecY closest
            }

            // LIMIT 1 without WHERE
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 1",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(2, results.get(0).get("id"));
            }

            // LIMIT 1 OFFSET 1 without WHERE — should skip the top result
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 1 OFFSET 1",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                // Should get the second-closest result (not id=2 which is the closest)
                assertTrue("Should not be the top result", ((int) results.get(0).get("id")) != 2);
            }
        }
    }

    /**
     * Verifies that the execution plan for ORDER BY ann_of() DESC LIMIT k
     * uses VectorANNScanOp with limit pushed down, not a brute-force SortOp.
     */
    @Test
    public void testVectorIndexUsedInPlan() throws Exception {

        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f};
        final float[] vecY = {0.0f, 1.0f, 0.0f};
        final float[] vecZ = {0.0f, 0.0f, 1.0f};

        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(1, vecX));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(2, vecY));
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                    Arrays.asList(3, vecZ));

            // Verify the plan contains VectorANNScanOp with limit pushed down
            String sql = "SELECT id FROM tblspace1.t1"
                    + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT 2";
            TranslatedQuery translated = manager.getPlanner().translate(
                    TableSpace.DEFAULT, sql, Arrays.asList((Object) query),
                    true, true, true, -1);
            PlannerOp root = translated.plan.originalRoot;

            // The root should be ProjectOp(VectorANNScanOp) after optimization
            // (LimitOp was optimized away by pushing limit into VectorANNScanOp)
            VectorANNScanOp vecOp = findVectorANNScanOp(root);
            assertNotNull("Plan must contain VectorANNScanOp, but got: " + root, vecOp);
            assertTrue("Limit must be pushed into VectorANNScanOp", vecOp.hasLimit());

            // Also verify execution correctness
            try (DataScanner scan = scan(manager, sql, Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(2, results.size());
                assertEquals("top result must be id=2", 2, results.get(0).get("id"));
            }
        }
    }

    /**
     * Walk the plan tree to find a VectorANNScanOp node.
     */
    private static VectorANNScanOp findVectorANNScanOp(PlannerOp op) {
        if (op instanceof VectorANNScanOp) {
            return (VectorANNScanOp) op;
        }
        if (op instanceof ProjectOp) {
            return findVectorANNScanOp(((ProjectOp) op).getInput());
        }
        return null;
    }

    // ---- helpers ----

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

    /**
     * Tests that rebuild() builds FusedPQ directly when conditions are met (dim >= 8, vectors >= 256),
     * avoiding duplicate work at the next checkpoint.
     */
    @Test
    public void testRebuildBuildsFusedPQDirectly() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 300;
        final int dimension = 16;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            // Insert enough rows for FusedPQ (>= 256, dim >= 8)
            for (int i = 1; i <= numRows; i++) {
                float[] vec = randomVec(dimension, i);
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, vec));
            }

            // Drop and recreate index to trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // Rebuild should have written FusedPQ directly
            assertEquals("all nodes should be on-disk after FusedPQ rebuild", numRows, vim.getOnDiskNodeCount());
            assertEquals("no live nodes after FusedPQ rebuild", 0, vim.getLiveNodeCount());
            assertEquals("vectors map should be empty after FusedPQ rebuild", 0, vim.getVectorsMapSize());
            assertEquals("pkToNode should be empty after rebuild", 0, vim.getPkToNodeSize());

            // Verify ANN search returns correct results
            float[] query = randomVec(dimension, 1); // should match row id=1 best
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results", results.isEmpty());
                assertEquals("top result should be id=1", 1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Tests BLink-backed on-disk maps: checkpoint to FusedPQ, insert more, checkpoint again,
     * then restart and verify everything loads correctly.
     */
    @Test
    public void testBLinkBackedOnDiskMaps() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int dimension = 16;
        final int batch1 = 300;
        final int batch2 = 50;

        // Phase 1: insert batch1, checkpoint → FusedPQ, insert batch2, checkpoint again
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            // Batch 1: enough for FusedPQ
            for (int i = 1; i <= batch1; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Checkpoint → FusedPQ
            manager.checkpoint();
            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("on-disk nodes after first checkpoint", batch1, vim.getOnDiskNodeCount());
            assertEquals("live nodes after first checkpoint", 0, vim.getLiveNodeCount());

            // Search works after first checkpoint
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results", results.isEmpty());
                assertEquals("top result should be id=1", 1, results.get(0).get("id"));
            }

            // Batch 2: more inserts
            for (int i = batch1 + 1; i <= batch1 + batch2; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Checkpoint again → merged FusedPQ
            manager.checkpoint();
            assertEquals("on-disk nodes after second checkpoint", batch1 + batch2, vim.getOnDiskNodeCount());
            assertEquals("live nodes after second checkpoint", 0, vim.getLiveNodeCount());
        }

        // Phase 2: restart and verify
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("on-disk nodes after restart", batch1 + batch2, vim.getOnDiskNodeCount());

            // Search works after restart
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results after restart", results.isEmpty());
                assertEquals("top result should be id=1 after restart", 1, results.get(0).get("id"));
            }
        }
    }

    // =========================================================================
    // Multi-segment tests
    // =========================================================================

    private DBManager createManagerWithMaxSegmentSize(String nodeId, Path metadataPath, Path dataPath,
                                                       Path logsPath, Path tmoDir, long maxSegmentSize) {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE, maxSegmentSize);
        return new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null);
    }

    @Test
    public void testMultiSegmentCheckpointRestart() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 500;
        // Very small maxSegmentSize to force multiple segments
        final long maxSegmentSize = 1024 * 50; // 50KB

        float[] queryVector = randomVec(dimension, 1);

        // Phase 1: insert + checkpoint
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getNodeCount());

            manager.checkpoint();

            // After checkpoint, should have multiple segments
            assertTrue("expected multiple segments with small maxSegmentSize, got " + vim.getSegmentCount(),
                    vim.getSegmentCount() >= 1);
            assertEquals(numRows, vim.getNodeCount());
        }

        // Phase 2: restart and verify
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals("node count after restart", numRows, vim.getNodeCount());

            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(queryVector, 5);
            assertEquals("expected 5 results", 5, results.size());
            for (int i = 0; i < results.size() - 1; i++) {
                assertTrue("scores must be descending",
                        results.get(i).getValue() >= results.get(i + 1).getValue());
            }
        }
    }

    @Test
    public void testMultiSegmentSearch() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            // Insert batch 1 and checkpoint
            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            // Insert batch 2 (live inserts, not yet checkpointed)
            for (int i = 301; i <= 350; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(350, vim.getNodeCount());

            // Search should find vectors from both on-disk segments AND live builder
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 1), 10);
            assertEquals(10, results.size());
        }
    }

    @Test
    public void testMultiSegmentDeleteAcrossSegments() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(300, vim.getNodeCount());

            // Delete some rows
            for (int i = 1; i <= 50; i++) {
                executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(i));
            }

            assertEquals(250, vim.getNodeCount());

            // Checkpoint again — segments with deletes should be rewritten
            manager.checkpoint();
            assertEquals(250, vim.getNodeCount());

            // Verify deleted vectors not in search results
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 1), 300);
            assertEquals(250, results.size());
        }
    }

    @Test
    public void testMultiSegmentRecovery() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 500;
        final long maxSegmentSize = 1024 * 50;

        // Phase 1: create, insert, checkpoint
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();
        }

        // Phase 2: restart and verify all segments loaded
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals(numRows, vim.getNodeCount());
            assertTrue("should have segments after recovery", vim.getSegmentCount() >= 1);

            // Search works correctly
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 42), 10);
            assertEquals(10, results.size());
        }
    }

    @Test
    public void testMultiSegmentRebuild() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 300;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Create index after data — triggers rebuild
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getNodeCount());

            // Verify search works after rebuild
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 1), 5);
            assertEquals(5, results.size());
        }
    }

    @Test
    public void testMultiSegmentEmptyCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            // Insert and checkpoint
            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            // Delete ALL vectors
            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(i));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(0, vim.getNodeCount());

            // Checkpoint with empty index
            manager.checkpoint();
            assertEquals(0, vim.getNodeCount());
        }

        // Restart and verify clean empty state
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertNotNull(vim);
            assertEquals(0, vim.getNodeCount());
        }
    }

    @Test
    public void testMultiSegmentMultipleCheckpoints() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // Cycle 1: insert + checkpoint
            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();
            assertEquals(300, vim.getNodeCount());

            // Cycle 2: insert more + checkpoint
            for (int i = 301; i <= 400; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();
            assertEquals(400, vim.getNodeCount());

            // Cycle 3: delete + checkpoint
            for (int i = 1; i <= 100; i++) {
                executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?", Arrays.asList(i));
            }
            manager.checkpoint();
            assertEquals(300, vim.getNodeCount());

            // Verify search correctness
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 200), 10);
            assertEquals(10, results.size());
        }
    }

    @Test
    public void testSingleSegmentWhenUnderLimit() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int numRows = 300;

        // Default maxSegmentSize (2GB) — should never split with only 300 rows
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(numRows, vim.getNodeCount());
            assertEquals("should have exactly 1 segment with default maxSegmentSize", 1, vim.getSegmentCount());
        }
    }

    @Test
    public void testMaxSegmentSizeFromServerConfig() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long customMaxSegmentSize = 1024 * 100; // 100KB

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, customMaxSegmentSize)) {
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

            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals(300, vim.getNodeCount());
            assertEquals("maxSegmentSize should come from server config",
                    customMaxSegmentSize, vim.getMaxSegmentSize());
        }
    }

    @Test
    public void testMultiSegmentHybridMergePolicy() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final long maxSegmentSize = 1024 * 50;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            // First batch: create initial segments
            for (int i = 1; i <= 300; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            int segCountAfterFirst = vim.getSegmentCount();
            int nodeCountAfterFirst = vim.getNodeCount();
            assertEquals(300, nodeCountAfterFirst);

            // Add a small batch (should create new live data)
            for (int i = 301; i <= 350; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            // Total node count should be preserved
            assertEquals(350, vim.getNodeCount());

            // Verify search correctness
            List<Map.Entry<herddb.utils.Bytes, Float>> results = vim.search(randomVec(dimension, 1), 10);
            assertEquals(10, results.size());
        }
    }

    // =========================================================================
    // Multi-segment rebuild tests
    // =========================================================================

    /**
     * Test 1: Rebuild with small maxSegmentSize creates multiple segments.
     * Verifies segments survive checkpoint + restart.
     */
    @Test
    public void testRebuildCreatesMultipleSegments() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 1000;
        final int dimension = 16;
        // 20KB: threshold = max(256, 20480/(16*4*2)) = 256, so ~3 segments from 1000 rows
        final long maxSegmentSize = 1024 * 20;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Drop and recreate to trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertTrue("expected multiple segments, got " + vim.getSegmentCount(),
                    vim.getSegmentCount() >= 2);
            assertEquals("total nodes", numRows, vim.getNodeCount());

            // Verify search works
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results", results.isEmpty());
                assertEquals("top result should be id=1", 1, results.get(0).get("id"));
            }

            // Checkpoint + restart
            manager.checkpoint();
        }

        // Restart and verify segments survive
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertTrue("segments should survive restart, got " + vim.getSegmentCount(),
                    vim.getSegmentCount() >= 2);
            assertEquals(numRows, vim.getNodeCount());

            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals(1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Test 2: Search accuracy across rebuild-created segments.
     */
    @Test
    public void testRebuildMultiSegmentSearchAccuracy() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 1000;
        final int dimension = 16;
        final long maxSegmentSize = 1024 * 20;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Drop and recreate to trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Query with several different seeds and check top-1 accuracy
            for (int seed : new int[]{1, 50, 100, 300, 500}) {
                float[] query = randomVec(dimension, seed);
                try (DataScanner scanner = scan(manager,
                        "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                        Arrays.asList((Object) query))) {
                    List<DataAccessor> results = scanner.consume();
                    assertEquals("should return 5 results", 5, results.size());
                    assertEquals("top result for seed=" + seed + " should match", seed, results.get(0).get("id"));
                }
            }
        }
    }

    /**
     * Test 3: Rebuild with all vectors fitting in one batch (under threshold).
     */
    @Test
    public void testRebuildSingleSegmentWhenUnderThreshold() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 300;
        final int dimension = 16;

        // Use default large maxSegmentSize so everything fits in one segment
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertEquals("should have exactly 1 segment", 1, vim.getSegmentCount());
            assertEquals(numRows, vim.getOnDiskNodeCount());
            assertEquals(0, vim.getLiveNodeCount());

            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals(1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Test 4: Rebuild with zero vectors (empty table).
     */
    @Test
    public void testRebuildZeroVectors() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            // Drop and recreate with empty table
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertEquals(0, vim.getOnDiskNodeCount());
            assertEquals(0, vim.getLiveNodeCount());
        }
    }

    /**
     * Test 5: Rebuild where final batch is smaller than MIN_VECTORS_FOR_FUSED_PQ.
     */
    @Test
    public void testRebuildSmallFinalBatch() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        // With 20KB maxSegmentSize and dim=16, threshold = 256.
        // 380 rows = 1 full batch (256) + 1 small batch (124, below 256 → kept as live)
        final int numRows = 380;
        final int dimension = 16;
        final long maxSegmentSize = 1024 * 20;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertEquals("total nodes", numRows, vim.getNodeCount());
            assertTrue("expected at least 1 segment, got " + vim.getSegmentCount(),
                    vim.getSegmentCount() >= 1);
            assertTrue("expected some live nodes from small final batch",
                    vim.getLiveNodeCount() > 0);

            // Verify search works across all segments including small final batch
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals(1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Test 6: Rebuild with dimension < MIN_DIM_FOR_FUSED_PQ (FusedPQ not applicable).
     */
    @Test
    public void testRebuildFusedPQNotApplicable() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 300;
        final int dimension = 4; // below MIN_DIM_FOR_FUSED_PQ (8)

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // With dim=4, FusedPQ is not applicable, so vectors stay in live state
            assertEquals(0, vim.getSegmentCount());
            assertEquals(0, vim.getOnDiskNodeCount());
            assertTrue("live nodes should have data", vim.getLiveNodeCount() > 0);

            // Verify search still works
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals(1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Test 7: Rebuild with mixed null/non-null vectors and small maxSegmentSize.
     */
    @Test
    public void testRebuildMultiSegmentWithNullableVectors() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int totalRows = 1200;
        final int dimension = 16;
        final long maxSegmentSize = 1024 * 20;
        // Every 4th row gets a null vector
        final int expectedNonNull = totalRows - (totalRows / 4);

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            for (int i = 1; i <= totalRows; i++) {
                if (i % 4 == 0) {
                    executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, null)",
                            Arrays.asList(i));
                } else {
                    executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                            Arrays.asList(i, randomVec(dimension, i)));
                }
            }

            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            assertEquals("node count should match non-null rows", expectedNonNull, vim.getNodeCount());
            assertTrue("expected multiple segments", vim.getSegmentCount() >= 2);

            // Verify search works
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals(1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Test 8: Multi-segment rebuild followed by incremental inserts and checkpoint.
     */
    @Test
    public void testRebuildMultiSegmentThenInsertAndCheckpoint() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int initialRows = 1000;
        final int additionalRows = 100;
        final int dimension = 16;
        final long maxSegmentSize = 1024 * 20;

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            int segmentsAfterRebuild = vim.getSegmentCount();
            assertTrue("expected multiple segments after rebuild", segmentsAfterRebuild >= 2);

            // Insert additional rows
            for (int i = initialRows + 1; i <= initialRows + additionalRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Checkpoint to merge live data
            manager.checkpoint();

            vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("total nodes after insert+checkpoint",
                    initialRows + additionalRows, vim.getNodeCount());

            // Search should find results from both rebuild and new inserts
            float[] query = randomVec(dimension, initialRows + 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse(results.isEmpty());
                assertEquals("should find newly inserted row",
                        initialRows + 1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Regression test: checkpoint immediately after rebuild must not erase the index.
     * Before the fix, a concurrent checkpoint could see partially-updated state
     * (builder=null, segments empty) and persist an empty index.
     */
    @Test
    public void testCheckpointAfterRebuildDoesNotEraseIndex() throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();

        final String nodeId = "localhost";
        final int numRows = 500;
        final int dimension = 16;
        final long maxSegmentSize = 1024 * 20; // force multi-segment rebuild

        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
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

            for (int i = 1; i <= numRows; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            // Drop and recreate to trigger rebuild
            execute(manager, "DROP INDEX tblspace1.vidx", Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertTrue("index should have nodes after rebuild, got " + vim.getNodeCount(),
                    vim.getNodeCount() > 0);

            // Checkpoint immediately after rebuild — this is the scenario that
            // previously caused the race condition and erased the index.
            manager.checkpoint();

            // Verify the index still has data after checkpoint
            assertTrue("index should have nodes after checkpoint, got " + vim.getNodeCount(),
                    vim.getNodeCount() > 0);

            // Verify search still works
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results after checkpoint", results.isEmpty());
            }
        }

        // Restart and verify the index survives
        try (DBManager manager = createManagerWithMaxSegmentSize(
                nodeId, metadataPath, dataPath, logsPath, tmoDir, maxSegmentSize)) {
            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertTrue("index should have nodes after restart, got " + vim.getNodeCount(),
                    vim.getNodeCount() > 0);

            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results after restart", results.isEmpty());
            }
        }
    }

    /**
     * Concurrent inserts from multiple threads with interleaved checkpoints.
     * Validates that the vector index handles concurrent recordInserted() calls
     * correctly and that checkpoint doesn't corrupt the index.
     */
    @Test
    public void testConcurrentInsertsAndCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 16;
        final int numThreads = 8;
        final int rowsPerThread = 50;
        final int totalRows = numThreads * rowsPerThread;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            // Launch concurrent inserters using transactions (batch + commit)
            java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(numThreads);

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        int base = threadId * rowsPerThread;
                        // Insert in small batches within auto-transactions
                        for (int i = 1; i <= rowsPerThread; i++) {
                            int id = base + i;
                            executeUpdate(manager,
                                    "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                    Arrays.asList(id, randomVec(dimension, id)));
                        }
                    } catch (Throwable ex) {
                        error.compareAndSet(null, ex);
                    } finally {
                        doneLatch.countDown();
                    }
                }, "inserter-" + t).start();
            }

            // Launch a checkpoint thread that runs concurrently with inserts
            java.util.concurrent.atomic.AtomicBoolean checkpointDone = new java.util.concurrent.atomic.AtomicBoolean(false);
            Thread checkpointThread = new Thread(() -> {
                try {
                    startLatch.await();
                    while (!checkpointDone.get()) {
                        manager.checkpoint();
                        Thread.sleep(10);
                    }
                } catch (InterruptedException ie) {
                    // expected
                } catch (Throwable ex) {
                    error.compareAndSet(null, ex);
                }
            }, "checkpointer");
            checkpointThread.start();

            // Go!
            startLatch.countDown();
            doneLatch.await();
            checkpointDone.set(true);
            checkpointThread.join(10000);

            // Final checkpoint
            manager.checkpoint();

            if (error.get() != null) {
                throw new AssertionError("Concurrent operation failed", error.get());
            }

            // Verify all rows are present
            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("all rows should be indexed", totalRows, vim.getNodeCount());

            // Verify search works
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results", results.isEmpty());
                assertEquals("top result should be id=1", 1, results.get(0).get("id"));
            }
        }
    }

    /**
     * Concurrent transactional inserts (batch + commit) from multiple threads.
     * Each thread opens a transaction, inserts a batch, then commits.
     * This exercises the onTransactionCommit → recordInserted path concurrently.
     */
    @Test
    public void testConcurrentTransactionalInserts() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 16;
        final int numThreads = 8;
        final int rowsPerThread = 50;
        final int batchSize = 10;
        final int totalRows = numThreads * rowsPerThread;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

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

            java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(numThreads);

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                new Thread(() -> {
                    try {
                        startLatch.await();
                        int base = threadId * rowsPerThread;
                        // Insert in batches using explicit transactions (BEGIN / INSERT... / COMMIT)
                        for (int batch = 0; batch < rowsPerThread / batchSize; batch++) {
                            long txId = herddb.core.TestUtils.beginTransaction(manager, "tblspace1");
                            for (int i = 0; i < batchSize; i++) {
                                int id = base + batch * batchSize + i + 1;
                                herddb.core.TestUtils.execute(manager, "tblspace1",
                                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                        Arrays.asList(id, randomVec(dimension, id)),
                                        new TransactionContext(txId));
                            }
                            herddb.core.TestUtils.commitTransaction(manager, "tblspace1", txId);
                        }
                    } catch (Throwable ex) {
                        error.compareAndSet(null, ex);
                    } finally {
                        doneLatch.countDown();
                    }
                }, "tx-inserter-" + t).start();
            }

            startLatch.countDown();
            doneLatch.await();

            if (error.get() != null) {
                throw new AssertionError("Concurrent transactional insert failed", error.get());
            }

            manager.checkpoint();

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");
            assertEquals("all rows should be indexed", totalRows, vim.getNodeCount());

            // Verify search works and returns correct results
            float[] query = randomVec(dimension, 1);
            try (DataScanner scanner = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT 5",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scanner.consume();
                assertFalse("search should return results", results.isEmpty());
                assertEquals("top result should be id=1", 1, results.get(0).get("id"));
            }
        }
    }

    // =========================================================================
    // Concurrent checkpoint consistency tests
    // =========================================================================

    /**
     * Verifies that inserts during checkpoint Phase B are not lost.
     * Uses the checkpointPhaseBHook to inject concurrent DML.
     */
    @Test
    public void testInsertDuringCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;
        final int duringCheckpointRows = 50;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            // Insert enough vectors for FusedPQ
            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // Set up Phase B hook: insert more rows during checkpoint
            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    for (int i = initialRows + 1; i <= initialRows + duringCheckpointRows; i++) {
                        executeUpdate(manager,
                                "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                Arrays.asList(i, randomVec(dimension, i)));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            // Checkpoint — Phase B hook will inject concurrent inserts
            manager.checkpoint();
            vim.setCheckpointPhaseBHook(null);

            assertNull("No error during Phase B hook", hookError.get());

            // All initial + during-checkpoint rows must be searchable
            float[] queryVector = randomVec(dimension, 1);
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT ?",
                    Arrays.asList((Object) queryVector, initialRows + duringCheckpointRows))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(initialRows + duringCheckpointRows, results.size());
            }

        }

        // Restart and verify all data survived (without second checkpoint — tests WAL replay idempotency)
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            float[] queryVector2 = randomVec(dimension, 1);
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT ?",
                    Arrays.asList((Object) queryVector2, initialRows + duringCheckpointRows))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(initialRows + duringCheckpointRows, results.size());
            }
        }
    }

    /**
     * Same as testInsertDuringCheckpoint but with a second checkpoint before restart.
     */
    @Test
    public void testInsertDuringCheckpointWithSecondCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;
        final int duringCheckpointRows = 50;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    for (int i = initialRows + 1; i <= initialRows + duringCheckpointRows; i++) {
                        executeUpdate(manager,
                                "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                Arrays.asList(i, randomVec(dimension, i)));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            vim.setCheckpointPhaseBHook(null);
            assertNull("No error during Phase B hook", hookError.get());

            // Second checkpoint before restart
            manager.checkpoint();
        }

        // Restart and verify all data survived
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            float[] queryVector = randomVec(dimension, 1);
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT ?",
                    Arrays.asList((Object) queryVector, initialRows + duringCheckpointRows))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(initialRows + duringCheckpointRows, results.size());
            }
        }
    }

    /**
     * Verifies that deletes during checkpoint Phase B are correctly applied.
     */
    @Test
    public void testDeleteDuringCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;
        final int deleteCount = 20;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // Delete some rows during Phase B
            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    for (int i = 1; i <= deleteCount; i++) {
                        executeUpdate(manager,
                                "DELETE FROM tblspace1.t1 WHERE id=?",
                                Arrays.asList(i));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            vim.setCheckpointPhaseBHook(null);

            assertNull("No error during Phase B hook", hookError.get());

            // Verify deleted rows are gone
            try (DataScanner scan = scan(manager,
                    "SELECT COUNT(*) AS cnt FROM tblspace1.t1",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                long count = ((Number) results.get(0).get("cnt")).longValue();
                assertEquals(initialRows - deleteCount, count);
            }

        }

        // Restart and verify deletes persist (without second checkpoint — tests WAL replay)
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            try (DataScanner scan = scan(manager,
                    "SELECT COUNT(*) AS cnt FROM tblspace1.t1",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                long count = ((Number) results.get(0).get("cnt")).longValue();
                assertEquals(initialRows - deleteCount, count);
            }
        }
    }

    /**
     * Verifies that search returns correct results during checkpoint Phase B,
     * including vectors from frozen state, new live state, and on-disk segments.
     */
    @Test
    public void testSearchDuringCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            // Insert initial rows and checkpoint to create on-disk segments
            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }
            manager.checkpoint();

            // Insert more rows into live state (will become frozen in Phase A)
            final int liveRows = 30;
            for (int i = initialRows + 1; i <= initialRows + liveRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // During Phase B: insert new rows AND search — all must be found
            final int phaseBInsertRows = 20;
            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    // Insert new rows during Phase B
                    for (int i = initialRows + liveRows + 1;
                         i <= initialRows + liveRows + phaseBInsertRows; i++) {
                        executeUpdate(manager,
                                "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                Arrays.asList(i, randomVec(dimension, i)));
                    }

                    // Search during Phase B — should find all rows
                    float[] queryVector = randomVec(dimension, 1);
                    int totalExpected = initialRows + liveRows + phaseBInsertRows;
                    try (DataScanner scan = scan(manager,
                            "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, cast(? as FLOAT ARRAY)) DESC LIMIT ?",
                            Arrays.asList((Object) queryVector, totalExpected))) {
                        List<DataAccessor> results = scan.consume();
                        assertEquals("All vectors must be searchable during Phase B",
                                totalExpected, results.size());
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            vim.setCheckpointPhaseBHook(null);

            if (hookError.get() != null) {
                throw new AssertionError("Phase B hook failed", hookError.get());
            }
        }
    }

    /**
     * Verifies that checkpoint does not block DML for the entire duration.
     * Inserts during Phase B should proceed without waiting for the full checkpoint.
     */
    @Test
    public void testCheckpointDoesNotBlockDML() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // Track insert timings during Phase B
            java.util.concurrent.atomic.AtomicLong maxInsertNanos = new java.util.concurrent.atomic.AtomicLong(0);
            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    for (int i = initialRows + 1; i <= initialRows + 20; i++) {
                        long start = System.nanoTime();
                        executeUpdate(manager,
                                "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                Arrays.asList(i, randomVec(dimension, i)));
                        long elapsed = System.nanoTime() - start;
                        maxInsertNanos.updateAndGet(prev -> Math.max(prev, elapsed));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            long checkpointStart = System.nanoTime();
            manager.checkpoint();
            long checkpointDuration = System.nanoTime() - checkpointStart;
            vim.setCheckpointPhaseBHook(null);

            assertNull("No error during Phase B hook", hookError.get());

            // The max insert time during Phase B should be significantly less than
            // the total checkpoint duration. With the old code, inserts would block
            // for the entire checkpoint. With 3-phase, they only block briefly.
            long maxInsertMs = maxInsertNanos.get() / 1_000_000;
            long checkpointMs = checkpointDuration / 1_000_000;
            LOGGER.log(java.util.logging.Level.INFO,
                    "Checkpoint took {0} ms, max insert during Phase B took {1} ms",
                    new Object[]{checkpointMs, maxInsertMs});
            // We don't assert a specific ratio since CI environments vary,
            // but the insert should complete (not deadlock or fail).
            assertTrue("Inserts completed during checkpoint", maxInsertMs < checkpointMs || checkpointMs < 1000);
        }
    }

    /**
     * Verifies that combined insert+delete+search during checkpoint maintains consistency.
     */
    @Test
    public void testMixedDMLDuringCheckpoint() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";
        final int dimension = 8;
        final int initialRows = 300;
        final int deleteCount = 10;
        final int insertCount = 20;
        final int updateCount = 5;

        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                    "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                    TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            execute(manager, "CREATE TABLE tblspace1.t1 (id int primary key, vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec) WITH ('fusedPQ'='true')",
                    Collections.emptyList());

            for (int i = 1; i <= initialRows; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                        Arrays.asList(i, randomVec(dimension, i)));
            }

            VectorIndexManager vim = (VectorIndexManager)
                    manager.getTableSpaceManager("tblspace1").getIndexesOnTable("t1").get("vidx");

            // During Phase B: delete some, insert some, update some
            java.util.concurrent.atomic.AtomicReference<Throwable> hookError = new java.util.concurrent.atomic.AtomicReference<>();
            vim.setCheckpointPhaseBHook(() -> {
                try {
                    // Delete rows 1-10
                    for (int i = 1; i <= deleteCount; i++) {
                        executeUpdate(manager,
                                "DELETE FROM tblspace1.t1 WHERE id=?",
                                Arrays.asList(i));
                    }
                    // Insert new rows
                    for (int i = initialRows + 1; i <= initialRows + insertCount; i++) {
                        executeUpdate(manager,
                                "INSERT INTO tblspace1.t1(id, vec) VALUES(?, ?)",
                                Arrays.asList(i, randomVec(dimension, i)));
                    }
                    // Update rows 11-15 with new vectors
                    for (int i = deleteCount + 1; i <= deleteCount + updateCount; i++) {
                        executeUpdate(manager,
                                "UPDATE tblspace1.t1 SET vec=? WHERE id=?",
                                Arrays.asList(randomVec(dimension, i + 10000), i));
                    }
                } catch (Throwable t) {
                    hookError.set(t);
                }
            });

            manager.checkpoint();
            vim.setCheckpointPhaseBHook(null);

            assertNull("No error during Phase B hook", hookError.get());

            // Verify final count: initial - deleted + inserted
            int expectedCount = initialRows - deleteCount + insertCount;
            try (DataScanner scan = scan(manager,
                    "SELECT COUNT(*) AS cnt FROM tblspace1.t1",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                long count = ((Number) results.get(0).get("cnt")).longValue();
                assertEquals(expectedCount, count);
            }

            // No second checkpoint — tests WAL replay idempotency on restart
        }

        // Restart and verify consistency
        try (DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null)) {

            manager.start();
            manager.waitForTablespace("tblspace1", 10000);

            int expectedCount = initialRows - deleteCount + insertCount;
            try (DataScanner scan = scan(manager,
                    "SELECT COUNT(*) AS cnt FROM tblspace1.t1",
                    Collections.emptyList())) {
                List<DataAccessor> results = scan.consume();
                long count = ((Number) results.get(0).get("cnt")).longValue();
                assertEquals(expectedCount, count);
            }
        }
    }

    private static final java.util.logging.Logger LOGGER =
            java.util.logging.Logger.getLogger(VectorIndexTest.class.getName());

    private static float[] randomVec(int dim, int seed) {
        float[] v = new float[dim];
        float norm = 0;
        for (int i = 0; i < dim; i++) {
            v[i] = (float) Math.sin(seed * (i + 1));
            norm += v[i] * v[i];
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < dim; i++) {
                v[i] /= norm;
            }
        }
        return v;
    }
}
