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
import static org.junit.Assert.assertTrue;
import herddb.core.stats.TableManagerStats;
import herddb.index.ConcurrentMapKeyToPageIndex;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageIndex;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TableContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import herddb.utils.DataAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
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
 * <p>To prove the fast-path is actually taken (and not just that the
 * returned count happens to be correct), the test wires a counting
 * {@link KeyToPageIndex} into the {@link MemoryDataStorageManager} and
 * asserts that {@code scanner()} is never invoked by the aggregate when
 * the pattern matches, and is invoked at least once when it doesn't.</p>
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

    /** Tracks {@code scanner()} calls against the primary-key index. */
    private static final class IndexInstrumentation {
        final AtomicLong scannerCalls = new AtomicLong();
    }

    /**
     * Instrumented primary-key index: identical to {@link ConcurrentMapKeyToPageIndex}
     * but every {@link #scanner} invocation is counted. A {@code scanner()} call
     * is the unambiguous signal that the aggregate drove a row-by-row scan — the
     * fast-path bypasses it entirely.
     */
    private static final class CountingKeyToPageIndex extends ConcurrentMapKeyToPageIndex {
        private final IndexInstrumentation probe;

        CountingKeyToPageIndex(IndexInstrumentation probe) {
            super(new ConcurrentHashMap<>());
            this.probe = probe;
        }

        @Override
        public Stream<Map.Entry<Bytes, Long>> scanner(IndexOperation operation,
                StatementEvaluationContext context, TableContext tableContext,
                AbstractIndexManager index) throws DataStorageManagerException {
            probe.scannerCalls.incrementAndGet();
            return super.scanner(operation, context, tableContext, index);
        }
    }

    private static final class CountingMemoryDataStorageManager extends MemoryDataStorageManager {
        final IndexInstrumentation probe = new IndexInstrumentation();

        @Override
        public KeyToPageIndex createKeyToPageMap(String tablespace, String name,
                MemoryManager memoryManager) {
            return new CountingKeyToPageIndex(probe);
        }
    }

    private static final class TestRig implements AutoCloseable {
        final DBManager manager;
        final CountingMemoryDataStorageManager storage;

        TestRig(DBManager manager, CountingMemoryDataStorageManager storage) {
            this.manager = manager;
            this.storage = storage;
        }

        long resetProbe() {
            return storage.probe.scannerCalls.getAndSet(0);
        }

        long scannerCalls() {
            return storage.probe.scannerCalls.get();
        }

        @Override
        public void close() {
            manager.close();
        }
    }

    private TestRig newRig() {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, plannerType);
        CountingMemoryDataStorageManager storage = new CountingMemoryDataStorageManager();
        DBManager manager = new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                storage,
                new MemoryCommitLogManager(),
                null, null, config, null);
        return new TestRig(manager, storage);
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
    public void emptyTableTakesFastPath() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            rig.resetProbe();
            assertEquals(0L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals("fast-path must not scan the primary-key index",
                    0L, rig.scannerCalls());
        }
    }

    @Test
    public void autocommitTakesFastPathAndReturnsCommittedCount() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            for (int i = 0; i < 17; i++) {
                executeUpdate(rig.manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }
            rig.resetProbe();
            assertEquals(17L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals("fast-path must not scan the primary-key index",
                    0L, rig.scannerCalls());
        }
    }

    @Test
    public void insideTransactionFallsBackToScanAndSeesUncommittedInserts() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(rig.manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            long tx = beginTransaction(rig.manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(tx);
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new1", 100), txCtx);
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new2", 101), txCtx);
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("new3", 102), txCtx);

            rig.resetProbe();
            assertEquals(13L, countStar(rig.manager, txCtx));
            assertTrue("in-transaction COUNT(*) must NOT take the fast-path",
                    rig.scannerCalls() >= 1);

            rig.resetProbe();
            assertEquals(10L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals("autocommit COUNT(*) from another session must take the fast-path",
                    0L, rig.scannerCalls());

            commitTransaction(rig.manager, "tblspace1", tx);
            rig.resetProbe();
            assertEquals(13L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals(0L, rig.scannerCalls());
        }
    }

    @Test
    public void insideTransactionFallsBackToScanAndSeesUncommittedDeletes() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(rig.manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            long tx = beginTransaction(rig.manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(tx);
            for (int i = 0; i < 4; i++) {
                executeUpdate(rig.manager, "DELETE FROM tblspace1.t WHERE k1=?",
                        Arrays.asList((Object) ("k" + i)), txCtx);
            }

            rig.resetProbe();
            assertEquals(6L, countStar(rig.manager, txCtx));
            assertTrue("in-transaction COUNT(*) must NOT take the fast-path",
                    rig.scannerCalls() >= 1);

            rig.resetProbe();
            assertEquals(10L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals(0L, rig.scannerCalls());

            commitTransaction(rig.manager, "tblspace1", tx);
            rig.resetProbe();
            assertEquals(6L, countStar(rig.manager, TransactionContext.NO_TRANSACTION));
            assertEquals(0L, rig.scannerCalls());
        }
    }

    @Test
    public void patternRejectionFallsBackToScanAndStaysCorrect() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            for (int i = 0; i < 10; i++) {
                executeUpdate(rig.manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            // WHERE predicate -> slow path
            rig.resetProbe();
            try (DataScanner s = scan(rig.manager,
                    "SELECT COUNT(*) FROM tblspace1.t WHERE n1 >= ?",
                    Arrays.asList((Object) 5))) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Long.valueOf(5), result.get(0).get(0));
            }
            assertTrue("COUNT(*) WHERE ... must take the slow scan path",
                    rig.scannerCalls() >= 1);

            // COUNT(col) is allowed to resolve either via the fast-path (when
            // the planner folds COUNT(non-null-col) into COUNT(*)) or via the
            // scan path — we only assert correctness here.
            try (DataScanner s = scan(rig.manager,
                    "SELECT COUNT(k1) FROM tblspace1.t",
                    Collections.emptyList())) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Long.valueOf(10), result.get(0).get(0));
            }

            // GROUP BY -> slow path, multiple rows
            rig.resetProbe();
            try (DataScanner s = scan(rig.manager,
                    "SELECT n1, COUNT(*) FROM tblspace1.t GROUP BY n1",
                    Collections.emptyList())) {
                assertEquals(10, s.consume().size());
            }
            assertTrue("GROUP BY must take the slow scan path",
                    rig.scannerCalls() >= 1);
        }
    }

    @Test
    public void tableSizeStatIsUpdatedByFastPath() throws Exception {
        // Belt-and-suspenders check: the O(1) count we return must match the
        // table stats size (both read keyToPage.size()).
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrap(rig.manager);
            for (int i = 0; i < 5; i++) {
                executeUpdate(rig.manager,
                        "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }
            AbstractTableManager atm = rig.manager.getTableSpaceManager("tblspace1")
                    .getTableManager("t");
            TableManagerStats stats = atm.getStats();
            rig.resetProbe();
            long count = countStar(rig.manager, TransactionContext.NO_TRANSACTION);
            assertEquals(stats.getTablesize(), count);
            assertEquals(0L, rig.scannerCalls());
        }
    }
}
