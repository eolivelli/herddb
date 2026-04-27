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

package org.herddb.ui.api.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.core.DBManager;
import herddb.model.TableSpace;
import herddb.model.TransactionContext;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.WebApplicationException;
import org.herddb.ui.dto.TableSummaryDTO;
import org.herddb.ui.internal.QueryService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call tests for {@link TablesResource}: verifies the table-listing
 * endpoint against a real embedded server. Individual table metadata (columns,
 * indexes, foreign keys) is covered by {@link TableResourceTest}.
 */
public class TablesResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Before
    public void seedSchema() {
        DBManager manager = serverRule.getServer().getManager();
        ddl(manager,
                "CREATE TABLE parent_t (id INTEGER PRIMARY KEY, label STRING)");
        ddl(manager,
                "CREATE TABLE child_t (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "amount LONG, "
                        + "CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) "
                        + "REFERENCES parent_t (id))");
        ddl(manager, "CREATE INDEX child_t_amount_idx ON child_t (amount)");

        // Seed a couple of rows so systablestats reports a non-zero
        // tablesize / loadedpages.
        ddl(manager, "INSERT INTO parent_t (id, label) VALUES (1, 'a')");
        ddl(manager, "INSERT INTO parent_t (id, label) VALUES (2, 'b')");
        ddl(manager, "INSERT INTO child_t  (id, parent_id, amount) VALUES (10, 1, 100)");
    }

    private static void ddl(DBManager manager, String sql) {
        // Pass null for the StatementEvaluationContext so the planner's
        // own SQLStatementEvaluationContext (which carries jdbcParameters
        // metadata required by INSERT/UPDATE) is used.
        manager.executeSimpleStatement(
                TableSpace.DEFAULT,
                sql,
                Collections.emptyList(),
                -1,
                false,
                TransactionContext.NO_TRANSACTION,
                null);
    }

    @Test
    public void listIncludesUserTablesWithStats() {
        TablesResource resource = new TablesResource(
                new QueryService(serverRule.getServer()));

        List<TableSummaryDTO> tables = resource.list(TableSpace.DEFAULT);

        assertNotNull(tables);
        assertFalse("must list at least the seeded tables", tables.isEmpty());
        TableSummaryDTO parent = tables.stream()
                .filter(t -> "parent_t".equalsIgnoreCase(t.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull("parent_t must appear in the list", parent);
        assertEquals(TableSpace.DEFAULT, parent.getTablespace());
        assertNotNull("uuid must be populated", parent.getUuid());
        assertFalse("system flag must be false on user tables", parent.isSystemTable());
        assertTrue(
                "tableSize must be non-negative, got " + parent.getTableSize(),
                parent.getTableSize() >= 0);
    }

    @Test
    public void listIsAlphabeticallySorted() {
        TablesResource resource = new TablesResource(
                new QueryService(serverRule.getServer()));

        List<TableSummaryDTO> tables = resource.list(TableSpace.DEFAULT);
        for (int i = 1; i < tables.size(); i++) {
            String prev = tables.get(i - 1).getName();
            String curr = tables.get(i).getName();
            assertTrue(
                    "tables must be sorted by name, got " + prev + " before " + curr,
                    prev.compareToIgnoreCase(curr) <= 0);
        }
    }

    @Test
    public void listRejectsIllegalTablespace() {
        TablesResource resource = new TablesResource(
                new QueryService(serverRule.getServer()));

        WebApplicationException ex = assertThrows(
                WebApplicationException.class,
                () -> resource.list("herd; DROP TABLE bad"));
        assertEquals(400, ex.getResponse().getStatus());
    }
}
