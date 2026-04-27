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

import static herddb.core.TestUtils.execute;
import static herddb.core.TestUtils.executeUpdate;
import static herddb.core.TestUtils.scan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import herddb.mem.MemoryCommitLogManager;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.DataScanner;
import herddb.model.StatementEvaluationContext;
import herddb.model.TransactionContext;
import herddb.model.commands.CreateTableSpaceStatement;
import herddb.server.ServerConfiguration;
import herddb.utils.DataAccessor;
import herddb.utils.RawString;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Exercises the BRIN-secondary-index branch of the {@code SELECT MIN/MAX(col)}
 * fast-path introduced for issue #307. The fast-path fires only for
 * byte-sortable column types (STRING / BYTEARRAY) because the BRIN
 * orders indexed keys by byte comparison; for two's-complement numeric
 * columns the byte order does not match the natural order so the
 * caller falls back to the regular scan.
 *
 * <p>Test correctness only: the fact that a BRIN-covered MIN/MAX produces
 * the correct natural-order extreme is the smoking gun, given that the
 * BRIN's block-level sorting is exactly what the fast-path reads.</p>
 */
@RunWith(Parameterized.class)
public class BrinMinMaxFastPathTest {

    private final String plannerType;

    public BrinMinMaxFastPathTest(String plannerType) {
        this.plannerType = plannerType;
    }

    @Parameterized.Parameters(name = "planner={0}")
    public static Iterable<Object[]> planners() {
        return Arrays.asList(
                new Object[]{ServerConfiguration.PLANNER_TYPE_CALCITE},
                new Object[]{ServerConfiguration.PLANNER_TYPE_JSQLPARSER}
        );
    }

    private static DBManager newManager(String plannerType) {
        ServerConfiguration config = new ServerConfiguration();
        config.set(ServerConfiguration.PROPERTY_PLANNER_TYPE, plannerType);
        return new DBManager("localhost",
                new MemoryMetadataStorageManager(),
                new MemoryDataStorageManager(),
                new MemoryCommitLogManager(),
                null, null, config, null);
    }

    private static void bootstrapWithBrinOnString(DBManager manager) throws Exception {
        bootstrap(manager,
                "CREATE TABLE tblspace1.t (id int primary key, name string, qty int)",
                "CREATE BRIN INDEX brin_name ON tblspace1.t(name)");
    }

    private static void bootstrapWithBrinOnInt(DBManager manager) throws Exception {
        bootstrap(manager,
                "CREATE TABLE tblspace1.t (id int primary key, name string, qty int)",
                "CREATE BRIN INDEX brin_qty ON tblspace1.t(qty)");
    }

    private static void bootstrap(DBManager manager, String... ddls) throws Exception {
        CreateTableSpaceStatement st = new CreateTableSpaceStatement(
                "tblspace1", Collections.singleton("localhost"), "localhost", 1, 0, 0);
        manager.executeStatement(st,
                StatementEvaluationContext.DEFAULT_EVALUATION_CONTEXT(),
                TransactionContext.NO_TRANSACTION);
        manager.waitForTablespace("tblspace1", 10000);
        for (String ddl : ddls) {
            execute(manager, ddl, Collections.emptyList());
        }
    }

    private static Object selectMin(DBManager manager, String column) throws Exception {
        try (DataScanner s = scan(manager,
                "SELECT MIN(" + column + ") FROM tblspace1.t",
                Collections.emptyList())) {
            List<DataAccessor> result = s.consume();
            assertEquals(1, result.size());
            return result.get(0).get(0);
        }
    }

    private static Object selectMax(DBManager manager, String column) throws Exception {
        try (DataScanner s = scan(manager,
                "SELECT MAX(" + column + ") FROM tblspace1.t",
                Collections.emptyList())) {
            List<DataAccessor> result = s.consume();
            assertEquals(1, result.size());
            return result.get(0).get(0);
        }
    }

    @Test
    public void emptyTableWithBrin() throws Exception {
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            assertNull(selectMin(manager, "name"));
            assertNull(selectMax(manager, "name"));
        }
    }

    @Test
    public void brinOnStringColumn() throws Exception {
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            // insert in non-alphabetical order so we can prove sorting
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(1, "charlie", 30));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(2, "alpha", 10));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(3, "delta", 40));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(4, "bravo", 20));

            Object min = selectMin(manager, "name");
            assertEquals("alpha", min instanceof RawString ? min.toString() : min);
            Object max = selectMax(manager, "name");
            assertEquals("delta", max instanceof RawString ? max.toString() : max);
        }
    }

    @Test
    public void brinOnIntColumnFallsBackToScanButProducesCorrectAnswer() throws Exception {
        // Numeric BRIN: byte order != natural order for two's-complement,
        // so the BRIN fast-path is intentionally NOT taken. The slow scan
        // path must still return the correct natural-order extreme.
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnInt(manager);
            int[] qtys = {-100, -1, 0, 50, 1000, Integer.MAX_VALUE};
            int id = 0;
            for (int q : qtys) {
                executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                        Arrays.asList(++id, "n" + id, q));
            }
            assertEquals(Integer.valueOf(-100), selectMin(manager, "qty"));
            assertEquals(Integer.valueOf(Integer.MAX_VALUE), selectMax(manager, "qty"));
        }
    }

    @Test
    public void minMaxOnNonIndexedColumn() throws Exception {
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            for (int i = 0; i < 5; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                        Arrays.asList(i, "n" + i, i * 10));
            }
            // no BRIN on 'qty', no fast-path; slow scan must still work.
            assertEquals(Integer.valueOf(0), selectMin(manager, "qty"));
            assertEquals(Integer.valueOf(40), selectMax(manager, "qty"));
        }
    }

    @Test
    public void brinFastPathRespectsWherePredicate() throws Exception {
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            for (int i = 0; i < 6; i++) {
                executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                        Arrays.asList(i, "n" + i, i));
            }
            // A WHERE predicate disables the fast path; the slow scan
            // must compute the filtered MIN/MAX correctly.
            try (DataScanner s = scan(manager,
                    "SELECT MAX(name) FROM tblspace1.t WHERE qty < 3",
                    Collections.emptyList())) {
                List<DataAccessor> result = s.consume();
                Object v = result.get(0).get(0);
                assertNotNull(v);
                assertEquals("n2", v instanceof RawString ? v.toString() : v);
            }
        }
    }

    @Test
    public void brinFastPathAfterUpdate() throws Exception {
        // After updating a row, the BRIN's indexed key for that row
        // changes. MIN/MAX must reflect the new key. This exercises the
        // recordUpdated path and confirms the fast-path reads the live
        // BRIN state, not a stale snapshot.
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(1, "alpha", 1));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(2, "bravo", 2));
            assertEquals("alpha",
                    rawToString(selectMin(manager, "name")));
            // promote alpha → zulu
            executeUpdate(manager, "UPDATE tblspace1.t SET name=? WHERE id=?",
                    Arrays.asList("zulu", 1));
            assertEquals("bravo",
                    rawToString(selectMin(manager, "name")));
            assertEquals("zulu",
                    rawToString(selectMax(manager, "name")));
        }
    }

    @Test
    public void brinFastPathAfterDelete() throws Exception {
        try (DBManager manager = newManager(plannerType)) {
            manager.start();
            bootstrapWithBrinOnString(manager);
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(1, "alpha", 1));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(2, "bravo", 2));
            executeUpdate(manager, "INSERT INTO tblspace1.t(id,name,qty) values(?,?,?)",
                    Arrays.asList(3, "delta", 4));
            assertEquals("alpha", rawToString(selectMin(manager, "name")));
            assertEquals("delta", rawToString(selectMax(manager, "name")));

            executeUpdate(manager, "DELETE FROM tblspace1.t WHERE id=?",
                    Arrays.asList(1));
            assertEquals("bravo", rawToString(selectMin(manager, "name")));
            executeUpdate(manager, "DELETE FROM tblspace1.t WHERE id=?",
                    Arrays.asList(3));
            assertEquals("bravo", rawToString(selectMax(manager, "name")));
            assertTrue(true);
        }
    }

    private static String rawToString(Object v) {
        return v instanceof RawString ? v.toString() : (String) v;
    }
}
