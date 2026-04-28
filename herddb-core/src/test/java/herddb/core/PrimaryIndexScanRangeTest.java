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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.file.FileDataStorageManager;
import herddb.index.PrimaryIndexRangeScan;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.DMLStatementExecutionResult;
import herddb.model.DataScanner;
import herddb.model.DataScannerException;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.model.commands.CreateTableStatement;
import herddb.model.commands.ScanStatement;
import herddb.sql.TranslatedQuery;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * @author enrico.olivelli
 * @author diego.salvi
 */
public class PrimaryIndexScanRangeTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void primaryIndexPrefixScanTest() throws Exception {

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',3,6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',4,7,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',5,5,'n2')", Collections.emptyList());

            performBasicPlannerTests(manager);

            // MemoryDataStorageManager uses ConcurrentMapKeyToPageIndex (HashMap-backed),
            // which is unordered regardless of PK type.
            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());
        }

    }

    @Test
    public void scanByPKOfIntsWithNegativeValues() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',-3,6,'n2')", Collections.emptyList());

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

             {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(-3, tuples.get(0).get("n1"));
                    assertEquals(1, tuples.get(1).get("n1"));
                    assertEquals(2, tuples.get(2).get("n1"));
                }
            }
        }
    }

    @Test
    public void scanByPKOfStrings() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',-3,6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n3')", Collections.emptyList());

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

             {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "order by id", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(1, tuples.get(0).get("n1"));
                    assertEquals(2, tuples.get(1).get("n1"));
                    assertEquals(-3, tuples.get(2).get("n1"));
                }
            }
        }
    }

    @Test
    public void primaryIndexPrefixScanFileTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a',1,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b',2,5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c',3,6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d',5,7,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e',6,5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('f',7,7,'n2')", Collections.emptyList());

            performBasicPlannerTests(manager);

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(6, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertNull(scan.getComparator());
                assertNotNull(scan.getLimits());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(6, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(5, tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(6, tuples.get(0).get("n1"));
                    assertEquals(7, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(7, tuples.get(0).get("n1"));
                }
            }

            // add a record in the middle of the sorted recordset
            DMLStatementExecutionResult res = TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',4,6,'n2')", Collections.emptyList(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long tx = res.transactionId;

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(4, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(6, tuples.get(3).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(6, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    for (DataAccessor tuple : tuples) {
                        System.out.println("tuple: " + tuple.toMap());
                    }
                    assertEquals(3, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                    assertEquals(5, tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                    assertEquals(6, tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(3, tuples.get(0).get("n1"));
                    assertEquals(4, tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(5, tuples.get(0).get("n1"));
                    assertEquals(6, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(6, tuples.get(0).get("n1"));
                    assertEquals(7, tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 5,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(7, tuples.get(0).get("n1"));
                }
            }

            // add other records in the context of the transaction
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',8,6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',9,6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g',10,6,'n3')", Collections.emptyList(), new TransactionContext(tx));

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(9, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                    assertEquals(8, tuples.get(6).get("n1"));
                    assertEquals(9, tuples.get(7).get("n1"));
                    assertEquals(10, tuples.get(8).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 7", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(7, tuples.size());
                    assertEquals(2, tuples.get(0).get("n1"));
                    assertEquals(3, tuples.get(1).get("n1"));
                    assertEquals(4, tuples.get(2).get("n1"));
                    assertEquals(5, tuples.get(3).get("n1"));
                    assertEquals(6, tuples.get(4).get("n1"));
                    assertEquals(7, tuples.get(5).get("n1"));
                    assertEquals(8, tuples.get(6).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 200", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(10, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(10, tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 desc", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(10, tuples.get(0).get("n1"));

                    assertEquals(9, tuples.get(1).get("n1"));
                    assertEquals(8, tuples.get(2).get("n1"));

                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>=2 "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(2, tuples.size());
                    assertEquals(8, tuples.get(0).get("n1"));
                    assertEquals(9, tuples.get(1).get("n1"));
                }
            }

        }

    }


    @Test
    public void primaryIndexPrefixScanWithStringsFileTest() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.STRING)
                    .column("n2", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('a','1',5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('b','2',5,'n1')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('c','3',6,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('d','5',7,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('e','6',5,'n2')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('f','7',7,'n2')", Collections.emptyList());


            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertNull(scan.getComparator());
                assertNotNull(scan.getLimits());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("3"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("3"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("5"), tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("6"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("7"), tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(RawString.of("7"), tuples.get(0).get("n1"));
                }
            }

            // add a record in the middle of the sorted recordset
            DMLStatementExecutionResult res = TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g','4',6,'n2')", Collections.emptyList(), TransactionContext.AUTOTRANSACTION_TRANSACTION);
            long tx = res.transactionId;

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(4, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(2).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(3).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(6, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(2).get("n1"));
                    assertEquals(RawString.of("5"), tuples.get(3).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(4).get("n1"));
                    assertEquals(RawString.of("7"), tuples.get(5).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    for (DataAccessor tuple : tuples) {
                        System.out.println("tuple: " + tuple.toMap());
                    }
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("3"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("5"), tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=6 "
                        + "order by n1 "
                        + "limit 1,3", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("3"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(2).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 1,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("3"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(1).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 3,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("5"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 4,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("6"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("7"), tuples.get(1).get("n1"));
                }
            }
            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 5,2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(1, tuples.size());
                    assertEquals(RawString.of("7"), tuples.get(0).get("n1"));
                }
            }

            // add other records in the context of the transaction
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g','8',6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g','9',6,'n3')", Collections.emptyList(), new TransactionContext(tx));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(id,n1,n2,name) values('g','A',6,'n3')", Collections.emptyList(), new TransactionContext(tx));

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(9, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(2).get("n1"));
                    assertEquals(RawString.of("5"), tuples.get(3).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(4).get("n1"));
                    assertEquals(RawString.of("7"), tuples.get(5).get("n1"));
                    assertEquals(RawString.of("8"), tuples.get(6).get("n1"));
                    assertEquals(RawString.of("9"), tuples.get(7).get("n1"));
                    assertEquals(RawString.of("A"), tuples.get(8).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2 "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and n2<=7 "
                        + "order by n1 "
                        + "limit 7", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(7, tuples.size());
                    assertEquals(RawString.of("2"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("3"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("4"), tuples.get(2).get("n1"));
                    assertEquals(RawString.of("5"), tuples.get(3).get("n1"));
                    assertEquals(RawString.of("6"), tuples.get(4).get("n1"));
                    assertEquals(RawString.of("7"), tuples.get(5).get("n1"));
                    assertEquals(RawString.of("8"), tuples.get(6).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 200", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("8"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("9"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("A"), tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and name='n3' "
                        + "order by n1 ", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("8"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("9"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("A"), tuples.get(2).get("n1"));
                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and name='n3' "
                        + "order by n1 desc", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(3, tuples.size());
                    assertEquals(RawString.of("A"), tuples.get(0).get("n1"));

                    assertEquals(RawString.of("9"), tuples.get(1).get("n1"));
                    assertEquals(RawString.of("8"), tuples.get(2).get("n1"));

                }
            }

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT n1,n2,name "
                        + "FROM tblspace1.t1 "
                        + "WHERE n1>='2' "
                        + "and name='n3' "
                        + "order by n1 "
                        + "limit 2", Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, new TransactionContext(tx))) {
                    List<DataAccessor> tuples = scan1.consume();
                    tuples.forEach(t -> {
                        System.out.println("OK sortedByClusteredIndex tuple " + t.toMap());
                    });
                    assertEquals(2, tuples.size());
                    assertEquals(RawString.of("8"), tuples.get(0).get("n1"));
                    assertEquals(RawString.of("9"), tuples.get(1).get("n1"));
                }
            }

        }

    }

    private void performBasicPlannerTests(final DBManager manager) throws StatementExecutionException, DataScannerException {
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>2 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(2, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(2, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1<=3 "
                    + "and n2<=6", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT *"
                            + " FROM tblspace1.t1 "
                            + "WHERE n1>=2 "
                            + "and n2<=6 "
                            + "order by n1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by N1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by N1", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by n1 desc", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
        {
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT, "SELECT *"
                    + "FROM tblspace1.t1 "
                    + "WHERE n1>=2 "
                    + "and n2<=6 "
                    + "order by n1 asc, n2", Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertFalse(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(3, scan1.consume().size());
            }
        }
    }

    @Test
    public void scanByPKOfLongsWithNegativeValues() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.LONG)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            // include Long.MIN_VALUE/MAX_VALUE to exercise the sign-bit boundary
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(?,?)",
                    java.util.Arrays.asList(Long.MIN_VALUE, "min"));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(-1,'minus_one')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(0,'zero')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(1,'one')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(?,?)",
                    java.util.Arrays.asList(Long.MAX_VALUE, "max"));

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            {
                TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                        "SELECT n1 FROM tblspace1.t1 ORDER BY n1",
                        Collections.emptyList(), true, true, false, -1);
                ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
                assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
                try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                    List<DataAccessor> tuples = scan1.consume();
                    assertEquals(5, tuples.size());
                    assertEquals(Long.MIN_VALUE, tuples.get(0).get("n1"));
                    assertEquals(-1L, tuples.get(1).get("n1"));
                    assertEquals(0L, tuples.get(2).get("n1"));
                    assertEquals(1L, tuples.get(3).get("n1"));
                    assertEquals(Long.MAX_VALUE, tuples.get(4).get("n1"));
                }
            }
        }
    }

    @Test
    public void rangeScanByPKBetweenNegativeAndPositive() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            for (int v : new int[] {-100, -10, -5, -1, 0, 1, 5, 10, 100}) {
                TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,name) values(?,?)",
                        java.util.Arrays.asList(v, "v" + v));
            }

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT n1 FROM tblspace1.t1 WHERE n1 BETWEEN -10 AND 10 ORDER BY n1",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getPredicate().getIndexOperation() instanceof PrimaryIndexRangeScan);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                List<DataAccessor> tuples = scan1.consume();
                assertEquals(7, tuples.size());
                assertEquals(-10, tuples.get(0).get("n1"));
                assertEquals(-5, tuples.get(1).get("n1"));
                assertEquals(-1, tuples.get(2).get("n1"));
                assertEquals(0, tuples.get(3).get("n1"));
                assertEquals(1, tuples.get(4).get("n1"));
                assertEquals(5, tuples.get(5).get("n1"));
                assertEquals(10, tuples.get(6).get("n1"));
            }
        }
    }

    @Test
    public void scanByPKOfTimestamps() throws Exception {
        // Note: Bytes.toTimestamp returns null when the decoded long is negative,
        // which makes pre-1970 timestamps unusable (pre-existing limitation).
        // This test only exercises post-1970 values.
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("ts", ColumnTypes.TIMESTAMP)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("ts")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            java.sql.Timestamp tsEarly = new java.sql.Timestamp(1_000_000L);
            java.sql.Timestamp tsMid = new java.sql.Timestamp(2_000_000_000L);
            java.sql.Timestamp tsLate = new java.sql.Timestamp(4_000_000_000L);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(ts,name) values(?,?)",
                    java.util.Arrays.asList(tsLate, "late"));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(ts,name) values(?,?)",
                    java.util.Arrays.asList(tsEarly, "early"));
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(ts,name) values(?,?)",
                    java.util.Arrays.asList(tsMid, "mid"));

            assertTrue(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT name FROM tblspace1.t1 ORDER BY ts",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            assertTrue(scan.getComparator().isOnlyPrimaryKeyAndAscending());
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                List<DataAccessor> tuples = scan1.consume();
                assertEquals(3, tuples.size());
                assertEquals(RawString.of("early"), tuples.get(0).get("name"));
                assertEquals(RawString.of("mid"), tuples.get(1).get("name"));
                assertEquals(RawString.of("late"), tuples.get(2).get("name"));
            }
        }
    }

    @Test
    public void scanByCompositePKContainingInteger() throws Exception {
        Path dataPath = folder.newFolder("data").toPath();

        String nodeId = "localhost";
        try (DBManager manager = new DBManager("localhost", new MemoryMetadataStorageManager(),
                new FileDataStorageManager(dataPath), new MemoryCommitLogManager(), null, null)) {
            manager.start();
            CreateTableSpaceStatement st1 = new CreateTableSpaceStatement("tblspace1", Collections.singleton(nodeId), nodeId, 1, 0, 0);
            manager.executeStatement(st1, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);
            manager.waitForTablespace("tblspace1", 10000);

            Table table = Table
                    .builder()
                    .tablespace("tblspace1")
                    .name("t1")
                    .column("n1", ColumnTypes.INTEGER)
                    .column("id", ColumnTypes.STRING)
                    .column("name", ColumnTypes.STRING)
                    .primaryKey("n1")
                    .primaryKey("id")
                    .build();

            CreateTableStatement st2 = new CreateTableStatement(table);
            manager.executeStatement(st2, StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(), TransactionContext.NO_TRANSACTION);

            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,id,name) values(-5,'a','minus5_a')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,id,name) values(-5,'b','minus5_b')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,id,name) values(0,'a','zero_a')", Collections.emptyList());
            TestUtils.executeUpdate(manager, "INSERT INTO tblspace1.t1(n1,id,name) values(7,'a','seven_a')", Collections.emptyList());

            // composite PK does not advertise sorted ascending
            assertFalse(manager.getTableSpaceManager("tblspace1").getTableManager("t1").isKeyToPageSortedAscending());

            // point lookup must round-trip the negative-int component correctly
            TranslatedQuery translated = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT name FROM tblspace1.t1 WHERE n1=-5 AND id='b'",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement scan = translated.plan.mainStatement.unwrap(ScanStatement.class);
            try (DataScanner scan1 = manager.scan(scan, translated.context, TransactionContext.NO_TRANSACTION)) {
                List<DataAccessor> tuples = scan1.consume();
                assertEquals(1, tuples.size());
                assertEquals(RawString.of("minus5_b"), tuples.get(0).get("name"));
            }

            // prefix scan on the leading int component must return all matching rows
            TranslatedQuery translatedPrefix = manager.getPlanner().translate(TableSpace.DEFAULT,
                    "SELECT id FROM tblspace1.t1 WHERE n1=-5",
                    Collections.emptyList(), true, true, false, -1);
            ScanStatement scanPrefix = translatedPrefix.plan.mainStatement.unwrap(ScanStatement.class);
            try (DataScanner scan1 = manager.scan(scanPrefix, translatedPrefix.context, TransactionContext.NO_TRANSACTION)) {
                assertEquals(2, scan1.consume().size());
            }
        }
    }

}
