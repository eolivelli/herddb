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

import static herddb.core.TestUtils.beginTransaction;
import static herddb.core.TestUtils.commitTransaction;
import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Exercises the {@code SELECT COUNT(*) FROM t} fast-path introduced for
 * issue #186. The fast-path bypasses the row-by-row scan and reads the
 * primary-key index size directly in autocommit; inside an explicit
 * transaction the original scan path runs so that uncommitted inserts /
 * deletes remain visible.
 *
 * <p>The test is parameterised over the two SQL planners so that both the
 * JSQLParser and Calcite plans are covered.</p>
 */
@RunWith(Parameterized.class)
public class CountStarFastPathTest {

    private final String plannerType;

    public CountStarFastPathTest(String plannerType) {
        this.plannerType = plannerType;
    }

    @Parameterized.Parameters(name = "planner={0}")
    public static Iterable<Object[]> planners() {
        return Arrays.asList(
                new Object[]{ServerConfiguration.PLANNER_TYPE_CALCITE},
                new Object[]{ServerConfiguration.PLANNER_TYPE_JSQLPARSER}
        );
    }

    private DBManager newManager() {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, plannerType);
        return new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config, null);
    }

    private static void bootstrap(DBManager manager) throws Exception {
        CreateTableSpaceStatement st = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st,
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager, "CREATE TABLE tblspace1.t (k1 string primary key, n1 int)",
                Collections.emptyList());
    }

    private static long countStar(DBManager manager, TransactionContext tx) throws Exception {
        try (DataScanner s = scan(manager, "SELECT COUNT(*) as cc FROM tblspace1.t",
                Collections.emptyList(), tx)) {
            List<DataAccessor> result = s.consume();
            assertEquals(1, result.size());
            return (Long) result.get(0).get(0);
        }
    }

    @Test
    public void emptyTableReturnsZero() throws Exception {
        try (DBManager manager = newManager()) {
            manager.start();
            bootstrap(manager);
            assertEquals(0L, countStar(manager, TransactionContext.NO_TRANSACTION));
        }
    }

    @Test
    public void autocommitReflectsCommittedRows() throws Exception {
        try (DBManager manager = newManager()) {
            manager.start();
            bootstrap(manager);
            for (int i = 0; i < 17; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }
            assertEquals(17L, countStar(manager, TransactionContext.NO_TRANSACTION));
        }
    }

    @Test
    public void insideTransactionSeesUncommittedInserts() throws Exception {
        try (DBManager manager = newManager()) {
            manager.start();
            bootstrap(manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            long tx = beginTransaction(manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(tx);
            executeUpdate(manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new1", 100), txCtx);
            executeUpdate(manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new2", 101), txCtx);
            executeUpdate(manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new3", 102), txCtx);

            // inside the txn we must see committed + uncommitted (fast-path is
            // deliberately skipped when transactionId > 0)
            assertEquals(13L, countStar(manager, txCtx));
            // another session (autocommit) only sees committed rows
            assertEquals(10L, countStar(manager, TransactionContext.NO_TRANSACTION));

            commitTransaction(manager, "tblspace1", tx);
            assertEquals(13L, countStar(manager, TransactionContext.NO_TRANSACTION));
        }
    }

    @Test
    public void insideTransactionSeesUncommittedDeletes() throws Exception {
        try (DBManager manager = newManager()) {
            manager.start();
            bootstrap(manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            long tx = beginTransaction(manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(tx);
            for (int i = 0; i < 4; i++) {
                executeUpdate(manager, "DELETE FROM tblspace1.t WHERE k1=?",
                        Arrays.asList((Object) ("k" + i)), txCtx);
            }

            assertEquals(6L, countStar(manager, txCtx));
            assertEquals(10L, countStar(manager, TransactionContext.NO_TRANSACTION));

            commitTransaction(manager, "tblspace1", tx);
            assertEquals(6L, countStar(manager, TransactionContext.NO_TRANSACTION));
        }
    }

    @Test
    public void patternRejectionStillReturnsCorrectResults() throws Exception {
        try (DBManager manager = newManager()) {
            manager.start();
            bootstrap(manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            // WHERE predicate -> slow path
            try (DataScanner s = scan(manager,
                    "SELECT COUNT(*) FROM tblspace1.t WHERE n1 >= ?",
                    Arrays.asList((Object) 5))) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Long.valueOf(5), result.get(0).get(0));
            }

            // COUNT(col) -> different aggregate, slow path
            try (DataScanner s = scan(manager,
                    "SELECT COUNT(k1) FROM tblspace1.t",
                    Collections.emptyList())) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Long.valueOf(10), result.get(0).get(0));
            }

            // GROUP BY -> slow path, multiple rows
            try (DataScanner s = scan(manager,
                    "SELECT n1, COUNT(*) FROM tblspace1.t GROUP BY n1",
                    Collections.emptyList())) {
                assertEquals(10, s.consume().size());
            }
        }
    }
}
