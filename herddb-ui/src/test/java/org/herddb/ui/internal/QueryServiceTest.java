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

package org.herddb.ui.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.model.DataScannerException;
import herddb.model.TableSpace;
import java.util.List;
import java.util.Map;
import org.herddb.ui.api.v2.EmbeddedHerdDbServerRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * Verifies that {@link QueryService} runs read-only queries against the
 * server's system tables and rejects any non-SELECT statement.
 */
public class QueryServiceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Test
    public void readsSystablespaces() throws DataScannerException {
        QueryService queryService = new QueryService(serverRule.getServer());

        // SELECT * to avoid Calcite's reserved keyword "uuid" in the
        // projection list.
        List<Map<String, Object>> rows = queryService.selectRows(
                TableSpace.DEFAULT,
                "SELECT * FROM systablespaces");

        assertNotNull("rows must not be null", rows);
        assertFalse("rows must not be empty (expected at least the default tablespace)", rows.isEmpty());
        Map<String, Object> first = rows.get(0);
        assertTrue(
                "row must contain tablespace_name, got: " + first.keySet(),
                first.containsKey("tablespace_name"));
        assertTrue(
                "row must contain uuid, got: " + first.keySet(),
                first.containsKey("uuid"));
    }

    @Test
    public void rejectsInsertStatement() {
        QueryService queryService = new QueryService(serverRule.getServer());

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows(
                        TableSpace.DEFAULT,
                        "INSERT INTO systablespaces VALUES (?, ?)"));
        assertTrue(
                "error message must mention SELECT-only restriction, got: " + ex.getMessage(),
                ex.getMessage().contains("SELECT"));
    }

    @Test
    public void rejectsUpdateStatement() {
        QueryService queryService = new QueryService(serverRule.getServer());
        assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows(TableSpace.DEFAULT, "UPDATE foo SET x = 1"));
    }

    @Test
    public void rejectsDeleteStatement() {
        QueryService queryService = new QueryService(serverRule.getServer());
        assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows(TableSpace.DEFAULT, "DELETE FROM foo"));
    }

    @Test
    public void rejectsDropStatement() {
        QueryService queryService = new QueryService(serverRule.getServer());
        assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows(TableSpace.DEFAULT, "DROP TABLE foo"));
    }

    @Test
    public void rejectsBlankQuery() {
        QueryService queryService = new QueryService(serverRule.getServer());
        assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows(TableSpace.DEFAULT, "   "));
    }

    @Test
    public void rejectsBlankTablespace() {
        QueryService queryService = new QueryService(serverRule.getServer());
        assertThrows(
                IllegalArgumentException.class,
                () -> queryService.selectRows("", "SELECT * FROM systablespaces"));
    }

    @Test
    public void columnsAreLowerCased() throws DataScannerException {
        QueryService queryService = new QueryService(serverRule.getServer());
        List<Map<String, Object>> rows = queryService.selectRows(
                TableSpace.DEFAULT,
                "SELECT * FROM systablespaces");
        assertFalse(rows.isEmpty());
        // All column keys must be lower-cased, regardless of the SQL grammar
        // case folding rules used by the active planner.
        for (Map<String, Object> row : rows) {
            for (String key : row.keySet()) {
                assertEquals(
                        "key must be lower-cased, got: " + key,
                        key.toLowerCase(java.util.Locale.ROOT),
                        key);
            }
        }
    }
}
