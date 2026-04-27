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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import herddb.utils.RawString;
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
 * Exercises the {@code SELECT MIN(pk) FROM t} / {@code SELECT MAX(pk) FROM t}
 * fast-path introduced for issue #307. The fast-path bypasses the row-by-row
 * scan and reads the value directly from the primary-key index in autocommit:
 * O(log n) for byte-sortable PK types (STRING / BYTEARRAY) and O(n_keys)
 * with zero data-page reads for numeric PK types.
 *
 * <p>The instrumentation hooks count two distinct events on the primary-key
 * index:
 * <ul>
 *   <li>{@code scanner()} calls — the entry point used by both the slow
 *       scan path and the numeric-PK fast-path "iterate keys" loop;</li>
 *   <li>{@code getMinKey()} / {@code getMaxKey()} calls — the entry point
 *       used only by the byte-sortable fast-path.</li>
 * </ul>
 *
 * <p>For the byte-sortable case (STRING PK) we assert
 * {@code scannerCalls == 0}, proving the slow scan was bypassed entirely.
 * For the numeric case (INTEGER PK) we cannot use a scanner counter to
 * prove the fast-path was taken (both the fast and slow path call
 * {@code scanner()}); instead we use a mixed-sign INTEGER dataset where
 * the byte-extreme answer differs from the natural-extreme answer — a
 * correct result is therefore the smoking gun for the decode-and-compare
 * fast-path.</p>
 *
 * <p>The test is parameterised over the two SQL planners.</p>
 */
@RunWith(Parameterized.class)
public class MinMaxFastPathTest {

    private final String plannerType;

    public MinMaxFastPathTest(String plannerType) {
        this.plannerType = plannerType;
    }

    @Parameterized.Parameters(name = "planner={0}")
    public static Iterable<Object[]> planners() {
        return Arrays.asList(
                new Object[]{ServerConfiguration.PLANNER_TYPE_CALCITE},
                new Object[]{ServerConfiguration.PLANNER_TYPE_JSQLPARSER}
        );
    }

    private static final class IndexInstrumentation {
        final AtomicLong scannerCalls = new AtomicLong();
        final AtomicLong getMinKeyCalls = new AtomicLong();
        final AtomicLong getMaxKeyCalls = new AtomicLong();
    }

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

        @Override
        public Bytes getMinKey() {
            probe.getMinKeyCalls.incrementAndGet();
            return super.getMinKey();
        }

        @Override
        public Bytes getMaxKey() {
            probe.getMaxKeyCalls.incrementAndGet();
            return super.getMaxKey();
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

        void resetProbe() {
            storage.probe.scannerCalls.set(0);
            storage.probe.getMinKeyCalls.set(0);
            storage.probe.getMaxKeyCalls.set(0);
        }

        long scannerCalls() {
            return storage.probe.scannerCalls.get();
        }

        long getMinKeyCalls() {
            return storage.probe.getMinKeyCalls.get();
        }

        long getMaxKeyCalls() {
            return storage.probe.getMaxKeyCalls.get();
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

    private static void bootstrapStringPk(DBManager manager) throws Exception {
        bootstrapWithDdl(manager,
                "CREATE TABLE tblspace1.t (k1 string primary key, n1 int)");
    }

    private static void bootstrapIntegerPk(DBManager manager) throws Exception {
        bootstrapWithDdl(manager,
                "CREATE TABLE tblspace1.t (id int primary key, payload string)");
    }

    private static void bootstrapCompositePk(DBManager manager) throws Exception {
        bootstrapWithDdl(manager,
                "CREATE TABLE tblspace1.t (a int, b string, payload int, primary key(a, b))");
    }

    private static void bootstrapWithDdl(DBManager manager, String ddl) throws Exception {
        CreateTableSpaceStatement st = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st,
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        execute(manager, ddl, Collections.emptyList());
    }

    private static Object selectMin(DBManager manager, String column, TransactionContext tx) throws Exception {
        try (DataScanner s = scan(manager,
                "SELECT MIN(" + column + ") AS mn FROM tblspace1.t",
                Collections.emptyList(), tx)) {
            List<DataAccessor> result = s.consume();
            assertEquals(1, result.size());
            return result.get(0).get(0);
        }
    }

    private static Object selectMax(DBManager manager, String column, TransactionContext tx) throws Exception {
        try (DataScanner s = scan(manager,
                "SELECT MAX(" + column + ") AS mx FROM tblspace1.t",
                Collections.emptyList(), tx)) {
            List<DataAccessor> result = s.consume();
            assertEquals(1, result.size());
            return result.get(0).get(0);
        }
    }

    @Test
    public void emptyTableTakesFastPath() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapStringPk(rig.manager);
            rig.resetProbe();
            assertNull(selectMin(rig.manager, "k1", TransactionContext.NO_TRANSACTION));
            assertNull(selectMax(rig.manager, "k1", TransactionContext.NO_TRANSACTION));
            assertEquals("empty-table fast-path must not scan the primary-key index",
                    0L, rig.scannerCalls());
        }
    }

    @Test
    public void stringPkAutocommitTakesFastPath() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapStringPk(rig.manager);
            // insert a few keys not in alphabetical insert order
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)", Arrays.asList("k_charlie", 3));
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)", Arrays.asList("k_alpha",   1));
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)", Arrays.asList("k_bravo",   2));
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)", Arrays.asList("k_delta",   4));

            rig.resetProbe();
            Object min = selectMin(rig.manager, "k1", TransactionContext.NO_TRANSACTION);
            assertEquals("k_alpha", min instanceof RawString ? min.toString() : min);
            assertEquals("STRING-PK MIN must use the byte-extreme fast-path (no scanner call)",
                    0L, rig.scannerCalls());
            assertEquals("STRING-PK MIN must call getMinKey() exactly once",
                    1L, rig.getMinKeyCalls());

            rig.resetProbe();
            Object max = selectMax(rig.manager, "k1", TransactionContext.NO_TRANSACTION);
            assertEquals("k_delta", max instanceof RawString ? max.toString() : max);
            assertEquals("STRING-PK MAX must use the byte-extreme fast-path (no scanner call)",
                    0L, rig.scannerCalls());
            assertEquals("STRING-PK MAX must call getMaxKey() exactly once",
                    1L, rig.getMaxKeyCalls());
        }
    }

    @Test
    public void integerPkPositiveValuesAutocommit() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            for (int v : new int[]{17, 3, 9001, 5, 42}) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, "p" + v));
            }
            rig.resetProbe();
            assertEquals(Integer.valueOf(3),
                    selectMin(rig.manager, "id", TransactionContext.NO_TRANSACTION));
            rig.resetProbe();
            assertEquals(Integer.valueOf(9001),
                    selectMax(rig.manager, "id", TransactionContext.NO_TRANSACTION));
        }
    }

    /**
     * Mixed-sign INTEGER PK is the strict correctness check: the
     * byte-wise extreme of two's-complement integers does NOT match the
     * natural extreme (negative ints all have the high bit set, so they
     * sort byte-wise *after* the largest positive). A correct answer
     * here is only producible through the decode-and-compare path.
     */
    @Test
    public void integerPkMixedSignAutocommit() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            int[] values = {-2147483648, -100, -1, 0, 1, 17, 9001, 2147483647};
            for (int v : values) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, "p" + v));
            }
            assertEquals("MIN must be Integer.MIN_VALUE (natural order, not byte order)",
                    Integer.valueOf(Integer.MIN_VALUE),
                    selectMin(rig.manager, "id", TransactionContext.NO_TRANSACTION));
            assertEquals("MAX must be Integer.MAX_VALUE (natural order, not byte order)",
                    Integer.valueOf(Integer.MAX_VALUE),
                    selectMax(rig.manager, "id", TransactionContext.NO_TRANSACTION));
        }
    }

    @Test
    public void insideTransactionFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapStringPk(rig.manager);
            for (int i = 0; i < 5; i++) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                        Arrays.asList("k" + i, i));
            }

            long tx = beginTransaction(rig.manager, "tblspace1");
            TransactionContext txCtx = new TransactionContext(tx);
            // uncommitted insert that would change MAX
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(k1,n1) values(?,?)",
                    Arrays.asList("k_zzz", 99), txCtx);

            rig.resetProbe();
            Object maxInTx = selectMax(rig.manager, "k1", txCtx);
            assertEquals("k_zzz", maxInTx instanceof RawString ? maxInTx.toString() : maxInTx);
            assertTrue("in-transaction MAX must NOT take the fast-path",
                    rig.scannerCalls() >= 1);
            assertEquals("in-transaction MAX must NOT call getMaxKey() shortcut",
                    0L, rig.getMaxKeyCalls());

            // commit so we can verify autocommit sees the new MAX afterwards
            commitTransaction(rig.manager, "tblspace1", tx);
            rig.resetProbe();
            Object maxAfterCommit = selectMax(rig.manager, "k1", TransactionContext.NO_TRANSACTION);
            assertEquals("k_zzz",
                    maxAfterCommit instanceof RawString ? maxAfterCommit.toString() : maxAfterCommit);
            assertEquals(0L, rig.scannerCalls());
        }
    }

    @Test
    public void wherePredicateFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            for (int v : new int[]{1, 2, 3, 10, 100}) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, "p"));
            }
            rig.resetProbe();
            try (DataScanner s = scan(rig.manager,
                    "SELECT MAX(id) FROM tblspace1.t WHERE id < ?",
                    Arrays.asList((Object) 50))) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Integer.valueOf(10), result.get(0).get(0));
            }
            assertTrue("WHERE-filtered MAX must take the slow scan path",
                    rig.scannerCalls() >= 1);
            assertEquals("WHERE-filtered MAX must NOT take any fast-path shortcut",
                    0L, rig.getMaxKeyCalls() + rig.getMinKeyCalls());
        }
    }

    @Test
    public void groupByFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            for (int v = 0; v < 10; v++) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, v % 2 == 0 ? "even" : "odd"));
            }
            rig.resetProbe();
            try (DataScanner s = scan(rig.manager,
                    "SELECT payload, MAX(id) FROM tblspace1.t GROUP BY payload",
                    Collections.emptyList())) {
                assertEquals(2, s.consume().size());
            }
            assertTrue("GROUP BY must take the slow scan path",
                    rig.scannerCalls() >= 1);
            assertEquals(0L, rig.getMaxKeyCalls());
        }
    }

    @Test
    public void compositePkFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapCompositePk(rig.manager);
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(a,b,payload) values(?,?,?)",
                    Arrays.asList(1, "x", 10));
            executeUpdate(rig.manager, "INSERT INTO tblspace1.t(a,b,payload) values(?,?,?)",
                    Arrays.asList(2, "y", 20));
            rig.resetProbe();
            // MAX of a single PK column when the PK is composite must fall
            // back to scan, because the byte-extreme of the composite key
            // is not the natural-extreme of column 'a'.
            try (DataScanner s = scan(rig.manager,
                    "SELECT MAX(a) FROM tblspace1.t",
                    Collections.emptyList())) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Integer.valueOf(2), result.get(0).get(0));
            }
            assertTrue("composite PK must take the slow scan path",
                    rig.scannerCalls() >= 1);
            assertEquals(0L, rig.getMaxKeyCalls());
        }
    }

    @Test
    public void minMaxOnNonPkUnindexedFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            for (int v : new int[]{1, 2, 3}) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, "p" + v));
            }
            rig.resetProbe();
            // 'payload' is not the PK and has no BRIN, so the fast-path
            // does not apply and the slow scan must run.
            Object max = selectMax(rig.manager, "payload", TransactionContext.NO_TRANSACTION);
            assertNotNull(max);
            assertEquals("p3", max instanceof RawString ? max.toString() : max);
            assertTrue("MAX on non-PK / non-BRIN column must take the slow scan path",
                    rig.scannerCalls() >= 1);
            assertEquals(0L, rig.getMaxKeyCalls());
        }
    }

    @Test
    public void multiAggregateFallsBackToScan() throws Exception {
        try (TestRig rig = newRig()) {
            rig.manager.start();
            bootstrapIntegerPk(rig.manager);
            for (int v : new int[]{1, 2, 3, 4, 5}) {
                executeUpdate(rig.manager, "INSERT INTO tblspace1.t(id, payload) values(?,?)",
                        Arrays.asList(v, "p"));
            }
            rig.resetProbe();
            // SELECT MIN(id), MAX(id) — two aggregates, fast-path not applicable
            try (DataScanner s = scan(rig.manager,
                    "SELECT MIN(id), MAX(id) FROM tblspace1.t",
                    Collections.emptyList())) {
                List<DataAccessor> result = s.consume();
                assertEquals(1, result.size());
                assertEquals(Integer.valueOf(1), result.get(0).get(0));
                assertEquals(Integer.valueOf(5), result.get(0).get(1));
            }
            assertTrue("multi-aggregate must take the slow scan path",
                    rig.scannerCalls() >= 1);
            assertEquals(0L, rig.getMaxKeyCalls() + rig.getMinKeyCalls());
        }
    }
}
