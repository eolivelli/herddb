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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests ORDER BY ann_of() support in JSQLParserPlanner.
 */
public class VectorIndexJSQLParserPlannerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmoDir, String nodeId) throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null);
        return manager;
    }

    /**
     * Vector index present: JSQLParserPlanner must route through VectorANNScanOp and
     * return the correct top result.
     */
    @Test
    public void testAnnOfWithVectorIndexJSQLParser() throws Exception {
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

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId)) {
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

            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, ?) DESC LIMIT 1",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 1 result", 1, results.size());
                assertEquals("top ANN result must be id=2", 2, results.get(0).get("id"));
            }
        }
    }

    /**
     * WHERE clause + ANN ordering with JSQLParserPlanner: only matching rows returned, in ANN order.
     */
    @Test
    public void testAnnOfWithWhereClauseJSQLParser() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f}; // id=1, cat=A
        final float[] vecY = {0.0f, 1.0f, 0.0f}; // id=2, cat=B — closest to query
        final float[] vecZ = {0.0f, 0.0f, 1.0f}; // id=3, cat=A
        final float[] vecW = {0.7f, 0.7f, 0.0f}; // id=4, cat=B — second closest
        normalize(vecW);

        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId)) {
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

            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 2",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("expected 2 cat=B results", 2, results.size());
                assertEquals("top result must be id=2 (vecY, cat=B)", 2, results.get(0).get("id"));
                assertEquals("second result must be id=4 (vecW, cat=B)", 4, results.get(1).get("id"));
            }
        }
    }

    /**
     * No vector index with JSQLParserPlanner: must throw StatementExecutionException.
     */
    @Test
    public void testAnnOfNoIndexJSQLParser() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f};
        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId)) {
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

            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, ?) DESC LIMIT 1",
                    Arrays.asList((Object) query))) {
                scan.consume();
                fail("Expected StatementExecutionException when no vector index exists");
            } catch (StatementExecutionException e) {
                assertTrue("Exception message should mention missing index",
                        e.getMessage().contains("No vector index found"));
            }
        }
    }

    private static void normalize(float[] v) {
        float norm = 0;
        for (float f : v) norm += f * f;
        norm = (float) Math.sqrt(norm);
        if (norm > 0) for (int i = 0; i < v.length; i++) v[i] /= norm;
    }
}
