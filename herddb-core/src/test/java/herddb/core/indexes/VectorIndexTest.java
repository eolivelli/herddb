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
import static org.junit.Assert.assertTrue;

import herddb.codec.RecordSerializer;
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
import herddb.model.commands.CreateIndexStatement;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.utils.DataAccessor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        }
    }

    // ---- helpers ----

    private static void normalize(float[] v) {
        float norm = 0;
        for (float f : v) norm += f * f;
        norm = (float) Math.sqrt(norm);
        if (norm > 0) for (int i = 0; i < v.length; i++) v[i] /= norm;
    }

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
