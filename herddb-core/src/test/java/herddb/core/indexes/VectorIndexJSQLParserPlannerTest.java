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
import herddb.core.DBManager;
import herddb.file.FileCommitLogManager;
import herddb.file.FileDataStorageManager;
import herddb.file.FileMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.planner.PlannerOp;
import herddb.model.planner.ProjectOp;
import herddb.model.planner.VectorANNScanOp;
import herddb.server.ServerConfiguration;
import herddb.sql.TranslatedQuery;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests ORDER BY ann_of() support in JSQLParserPlanner.
 */
public class VectorIndexJSQLParserPlannerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DBManager buildManager(Path dataPath, Path logsPath, Path metadataPath, Path tmoDir, String nodeId,
                                    MockRemoteVectorIndexService mockService) throws Exception {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, ServerConfiguration.PLANNER_TYPE_JSQLPARSER);
        DBManager manager = new DBManager(nodeId,
                new FileMetadataStorageManager(metadataPath),
                new FileDataStorageManager(dataPath),
                new FileCommitLogManager(logsPath),
                tmoDir, null, config, null);
        manager.setRemoteVectorIndexService(mockService);
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

        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        mockService.addVector("t1", "vidx", Bytes.from_int(1), vecX);
        mockService.addVector("t1", "vidx", Bytes.from_int(2), vecY);
        mockService.addVector("t1", "vidx", Bytes.from_int(3), vecZ);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId, mockService)) {
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

        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        mockService.addVector("t1", "vidx", Bytes.from_int(1), vecX);
        mockService.addVector("t1", "vidx", Bytes.from_int(2), vecY);
        mockService.addVector("t1", "vidx", Bytes.from_int(3), vecZ);
        mockService.addVector("t1", "vidx", Bytes.from_int(4), vecW);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId, mockService)) {
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

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId,
                new MockRemoteVectorIndexService())) {
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

    /**
     * Plan-level regression lock: when a WHERE predicate is present, the
     * JSQLParserPlanner must still build a {@link VectorANNScanOp}, and the
     * LIMIT must be pushed into it so that the streaming expansion loop can
     * budget an initial over-fetch. Before this change, LimitOp refused to
     * push the limit into a predicate-bearing VectorANNScanOp and the op
     * fell back to {@code Integer.MAX_VALUE} topK — a latency trap.
     */
    @Test
    public void testAnnOfWithPredicatePushesLimitIntoVectorANNScanOp() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            execute(manager,
                    "CREATE TABLE tblspace1.t1"
                            + " (id int primary key, cat varchar(10), vec floata not null)",
                    Collections.emptyList());
            execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                    Collections.emptyList());

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            String sqlWithWhere = "SELECT id FROM tblspace1.t1 WHERE cat=?"
                    + " ORDER BY ann_of(vec, ?) DESC LIMIT 2";
            TranslatedQuery translated = manager.getPlanner().translate(
                    TableSpace.DEFAULT, sqlWithWhere,
                    Arrays.asList((Object) "A", (Object) query),
                    true, true, true, -1);

            VectorANNScanOp vecOp = findVectorANNScanOp(translated.plan.originalRoot);
            assertNotNull("Plan must contain VectorANNScanOp, got: "
                    + translated.plan.originalRoot, vecOp);
            assertTrue("Limit must be pushed into VectorANNScanOp even with WHERE",
                    vecOp.hasLimit());
            assertNotNull("WHERE predicate must be captured on the op",
                    vecOp.getPredicate());
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

    /**
     * Shared setup helper for the predicate tests below: creates a tablespace,
     * a {@code t1(id, cat, vec)} table with a vector index, and populates both
     * the table and the mock index with {@code n} rows labelled A/B/C… by the
     * supplied lambda.
     */
    private static void seedPredicateDataset(DBManager manager,
            MockRemoteVectorIndexService mockService,
            int n,
            java.util.function.IntFunction<String> categoryFor,
            java.util.function.IntFunction<float[]> vectorFor) throws Exception {
        execute(manager,
                "CREATE TABLE tblspace1.t1 (id int primary key, cat varchar(10), vec floata not null)",
                Collections.emptyList());
        execute(manager, "CREATE VECTOR INDEX vidx ON tblspace1.t1(vec)",
                Collections.emptyList());
        for (int i = 1; i <= n; i++) {
            float[] v = vectorFor.apply(i);
            String cat = categoryFor.apply(i);
            executeUpdate(manager, "INSERT INTO tblspace1.t1(id, cat, vec) VALUES(?, ?, ?)",
                    Arrays.asList(i, cat, v));
            mockService.addVector("t1", "vidx", Bytes.from_int(i), v);
        }
    }

    private static DBManager startSeededManager(TemporaryFolder folder,
            MockRemoteVectorIndexService mockService,
            String nodeId) throws Exception {
        Path dataPath = folder.newFolder().toPath();
        Path logsPath = folder.newFolder().toPath();
        Path metadataPath = folder.newFolder().toPath();
        Path tmoDir = folder.newFolder().toPath();
        DBManager manager = new VectorIndexJSQLParserPlannerTest()
                .buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId, mockService);
        manager.start();
        CreateTableSpaceStatement st1 = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
        manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        return manager;
    }

    /**
     * Predicate that matches every row: the fast path must take a single RPC
     * (initial over-fetch = ceil(3 * 1.5) floored to 16) and return 3 rows.
     */
    @Test
    public void testAnnOfPredicateMatchesAll() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            seedPredicateDataset(manager, mockService, 10,
                    i -> "A",
                    i -> normalized(new float[]{i * 0.01f, 1.0f, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 3",
                    Arrays.asList("A", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
            }
            assertEquals("trivial predicate must take exactly one RPC",
                    1, mockService.getSearchCallCount());
            assertEquals("initial budget must be MIN_INITIAL=16",
                    Integer.valueOf(16), mockService.getRequestedLimits().get(0));
        }
    }

    /**
     * Predicate that filters half of the rows: a single expansion (budget 16
     * → 32) should be enough to find K=5 matches among 20 rows.
     */
    @Test
    public void testAnnOfPredicateFiltersHalf() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            // 20 rows, even ids are cat=B, odd are cat=A. The vector formula
            // below gives a strict (and numerically well-separated) ann order
            // where ann rank descends with the id.
            int total = 20;
            seedPredicateDataset(manager, mockService, total,
                    i -> (i % 2 == 0) ? "B" : "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 5",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("must return 5 cat=B rows", 5, results.size());
                // Expected ann order for cat=B (descending id): 20,18,16,14,12.
                int[] expectedIds = {20, 18, 16, 14, 12};
                for (int k = 0; k < expectedIds.length; k++) {
                    assertEquals("row " + k,
                            expectedIds[k],
                            ((Number) results.get(k).get("id")).intValue());
                }
            }
            assertTrue("at most 2 RPCs",
                    mockService.getSearchCallCount() <= 2);
        }
    }

    /**
     * Very selective predicate (only 5 rows match out of 100): the iterator
     * must expand multiple times.
     */
    @Test
    public void testAnnOfPredicateFiltersMost() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            // 100 rows, only ids 10, 30, 50, 70, 90 are cat=B. Vectors make
            // ann order by descending id.
            final int total = 100;
            seedPredicateDataset(manager, mockService, total,
                    i -> (i == 10 || i == 30 || i == 50 || i == 70 || i == 90) ? "B" : "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 5",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("all 5 cat=B rows must be returned", 5, results.size());
                // Expected order (descending id): 90, 70, 50, 30, 10
                assertEquals(90, ((Number) results.get(0).get("id")).intValue());
                assertEquals(70, ((Number) results.get(1).get("id")).intValue());
                assertEquals(50, ((Number) results.get(2).get("id")).intValue());
                assertEquals(30, ((Number) results.get(3).get("id")).intValue());
                assertEquals(10, ((Number) results.get(4).get("id")).intValue());
            }
            assertTrue("selective predicate must trigger at least one expansion",
                    mockService.getSearchCallCount() >= 2);
            // Budgets should monotonically double.
            List<Integer> limits = mockService.getRequestedLimits();
            for (int i = 1; i < limits.size(); i++) {
                assertEquals("budget must double on each expansion",
                        limits.get(i - 1) * 2, (int) limits.get(i));
            }
        }
    }

    /**
     * Predicate matches nothing: iterator must return zero rows and stop
     * within the expansion budget (no infinite loop).
     */
    @Test
    public void testAnnOfPredicateFiltersAll() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            final int total = 50;
            seedPredicateDataset(manager, mockService, total,
                    i -> "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 5",
                    Arrays.asList("Z", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("no row satisfies the predicate", 0, results.size());
            }
            // The iterator must stop at exhaustion (50 < budget eventually).
            // RPC count is bounded by 1 + MAX_EXPANSIONS = 7.
            assertTrue("RPC count must be finite",
                    mockService.getSearchCallCount() <= 7);
            assertTrue("at least one RPC was issued",
                    mockService.getSearchCallCount() >= 1);
        }
    }

    /**
     * Predicate combined with stale PKs in the middle of the ann order:
     * deleted rows and predicate-filtered rows must both be skipped
     * transparently.
     */
    @Test
    public void testAnnOfPredicateWithStalePk() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            final int total = 10;
            seedPredicateDataset(manager, mockService, total,
                    i -> (i % 2 == 0) ? "B" : "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            // Delete id=8 (a cat=B row that is near the top of ann order) but
            // leave it in the mock index so it still ranks high in the ANN
            // result and must be skipped on fetch.
            executeUpdate(manager, "DELETE FROM tblspace1.t1 WHERE id=?",
                    Arrays.asList(8));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 3",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(3, results.size());
                // Remaining cat=B rows in descending-id order: 10, 6, 4
                // (id 8 was deleted).
                assertEquals(10, ((Number) results.get(0).get("id")).intValue());
                assertEquals(6, ((Number) results.get(1).get("id")).intValue());
                assertEquals(4, ((Number) results.get(2).get("id")).intValue());
            }
        }
    }

    /**
     * LIMIT 1 with a selective predicate exercises the {@code MIN_INITIAL}
     * floor: the initial budget must be 16, not ceil(1 * 1.5) = 2.
     *
     * <p>(JSQLParserPlanner does not currently support OFFSET — see
     * JSQLParserPlanner.java:1622 — so the {@code LIMIT K OFFSET N} variant
     * is exercised in the CalcitePlanner path only.)
     */
    @Test
    public void testAnnOfPredicateTinyLimitHonoursFloor() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            final int total = 50;
            seedPredicateDataset(manager, mockService, total,
                    i -> (i == 25) ? "B" : "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 1",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals(1, results.size());
                assertEquals(25, ((Number) results.get(0).get("id")).intValue());
            }
            // LIMIT 1 → initial budget must be MIN_INITIAL (16), not ceil(1*1.5)=2.
            assertEquals("initial budget must honour MIN_INITIAL floor",
                    Integer.valueOf(16), mockService.getRequestedLimits().get(0));
        }
    }

    /**
     * Small dataset with a large LIMIT: iterator must stop on exhaustion,
     * not loop until MAX_EXPANSIONS.
     */
    @Test
    public void testAnnOfPredicateExhaustsSmallDataset() throws Exception {
        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        try (DBManager manager = startSeededManager(folder, mockService, "localhost")) {
            final int total = 5;
            seedPredicateDataset(manager, mockService, total,
                    i -> (i == 2 || i == 4 || i == 5) ? "B" : "A",
                    i -> normalized(new float[]{(float) (total - i + 1), (float) i, 0.0f}));
            mockService.resetSearchCallCount();

            float[] query = normalized(new float[]{0.0f, 1.0f, 0.0f});
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 WHERE cat=? ORDER BY ann_of(vec, ?) DESC LIMIT 100",
                    Arrays.asList("B", (Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("all 3 matching rows", 3, results.size());
            }
            assertEquals("single RPC is enough because the index is drained immediately",
                    1, mockService.getSearchCallCount());
        }
    }

    /**
     * ORDER BY ann_of(...) without a LIMIT clause must be rejected by the planner.
     * The bounded top-K merge in IndexingServiceClient relies on a finite limit.
     */
    @Test
    public void testAnnOfWithoutLimitIsRejectedJSQLParser() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();
        Path logsPath = folder.newFolder("logs").toPath();
        Path metadataPath = folder.newFolder("metadata").toPath();
        Path tmoDir = folder.newFolder("tmo").toPath();

        final String nodeId = "localhost";

        final float[] vecX = {1.0f, 0.0f, 0.0f};
        final float[] query = {0.05f, 0.99f, 0.0f};
        normalize(query);

        MockRemoteVectorIndexService mockService = new MockRemoteVectorIndexService();
        mockService.addVector("t1", "vidx", Bytes.from_int(1), vecX);

        try (DBManager manager = buildManager(dataPath, logsPath, metadataPath, tmoDir, nodeId, mockService)) {
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

            // No LIMIT → planner must reject.
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, ?) DESC",
                    Arrays.asList((Object) query))) {
                scan.consume();
                fail("Expected StatementExecutionException for unbounded ORDER BY ann_of(...)");
            } catch (StatementExecutionException e) {
                assertTrue("Exception must mention LIMIT requirement: " + e.getMessage(),
                        e.getMessage().contains("LIMIT"));
            }

            // Same query *with* LIMIT must still plan and execute fine.
            try (DataScanner scan = scan(manager,
                    "SELECT id FROM tblspace1.t1 ORDER BY ann_of(vec, ?) DESC LIMIT 10",
                    Arrays.asList((Object) query))) {
                List<DataAccessor> results = scan.consume();
                assertEquals("bounded query must succeed", 1, results.size());
            }
        }
    }

    private static float[] normalized(float[] v) {
        normalize(v);
        return v;
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
}
