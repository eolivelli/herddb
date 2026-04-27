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
import herddb.server.ServerConfiguration;
import java.util.Collections;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import org.herddb.ui.dto.DataPageInfoDTO;
import org.herddb.ui.dto.ForeignKeyDTO;
import org.herddb.ui.dto.IndexDetailDTO;
import org.herddb.ui.dto.IndexSummaryDTO;
import org.herddb.ui.dto.PrimaryIndexDTO;
import org.herddb.ui.dto.TableDetailDTO;
import org.herddb.ui.internal.ServerLocator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call tests for {@link TableResource}: covers the table detail
 * endpoint (columns, secondary indexes, foreign keys) as well as the
 * storage-layout endpoints (data pages, primary key index, BRIN index).
 *
 * <p>Standalone mode is required so the table is backed by a real
 * {@code FileDataStorageManager} — local mode uses an in-memory store with
 * {@code ConcurrentMapKeyToPageIndex} (no B-Link snapshot to read).
 */
public class TableResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule =
            new EmbeddedHerdDbServerRule(ServerConfiguration.PROPERTY_MODE_STANDALONE);

    @Before
    public void seedSchema() {
        DBManager manager = serverRule.getServer().getManager();
        ddl(manager, "CREATE TABLE parent_t (id INTEGER PRIMARY KEY, label STRING)");
        ddl(manager,
                "CREATE TABLE child_t (id INTEGER PRIMARY KEY, parent_id INTEGER, "
                        + "amount LONG, "
                        + "CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) "
                        + "REFERENCES parent_t (id))");
        ddl(manager, "CREATE BRIN INDEX child_t_amount_idx ON child_t (amount)");

        // Seed rows so systablestats reports non-zero values and so the
        // B-Link primary-key index has something to snapshot.
        for (int i = 0; i < 50; i++) {
            ddl(manager, "INSERT INTO parent_t (id, label) VALUES (" + i + ", 'row" + i + "')");
            ddl(manager,
                    "INSERT INTO child_t (id, parent_id, amount) VALUES ("
                            + i + ", " + i + ", " + (i * 100) + ")");
        }
    }

    private static void ddl(DBManager manager, String sql) {
        manager.executeSimpleStatement(
                TableSpace.DEFAULT,
                sql,
                Collections.emptyList(),
                -1,
                false,
                TransactionContext.NO_TRANSACTION,
                null);
    }

    // -----------------------------------------------------------------------
    // Table detail tests
    // -----------------------------------------------------------------------

    @Test
    public void getDetailReturnsColumnsIndexesAndForeignKeys() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        TableDetailDTO detail = resource.getDetail(TableSpace.DEFAULT, "child_t");

        assertNotNull(detail);
        assertNotNull(detail.getSummary());
        assertEquals("child_t",
                detail.getSummary().getName().toLowerCase(java.util.Locale.ROOT));

        // Columns: id, parent_id, amount in declaration order.
        assertEquals(3, detail.getColumns().size());
        assertEquals("id",
                detail.getColumns().get(0).getName().toLowerCase(java.util.Locale.ROOT));
        detail.getColumns().forEach(c -> {
            assertNotNull(c.getDataType());
            assertNotNull(c.getTypeName());
        });

        // Secondary index: child_t_amount_idx (BRIN).
        IndexSummaryDTO userIndex = detail.getIndexes().stream()
                .filter(i -> "child_t_amount_idx".equalsIgnoreCase(i.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "child_t_amount_idx missing from " + detail.getIndexes()));
        assertNotNull(userIndex.getColumns());
        assertEquals(1, userIndex.getColumns().size());
        assertEquals("amount",
                userIndex.getColumns().get(0).toLowerCase(java.util.Locale.ROOT));

        // Foreign key: child_t.parent_id -> parent_t.id.
        assertFalse("child_t must have at least one FK",
                detail.getForeignKeys().isEmpty());
        ForeignKeyDTO fk = detail.getForeignKeys().get(0);
        assertEquals("parent_t", fk.getParentTable().toLowerCase(java.util.Locale.ROOT));
        assertEquals(java.util.List.of("parent_id"),
                fk.getChildColumns().stream()
                        .map(c -> c.toLowerCase(java.util.Locale.ROOT))
                        .toList());
        assertEquals(java.util.List.of("id"),
                fk.getParentColumns().stream()
                        .map(c -> c.toLowerCase(java.util.Locale.ROOT))
                        .toList());
    }

    @Test
    public void getDetail404OnUnknownTable() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        assertThrows(NotFoundException.class,
                () -> resource.getDetail(TableSpace.DEFAULT, "missing_t"));
    }

    @Test
    public void getDetailRejectsIllegalTablespace() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        WebApplicationException ex = assertThrows(WebApplicationException.class,
                () -> resource.getDetail("herd; DROP TABLE bad", "child_t"));
        assertEquals(400, ex.getResponse().getStatus());
    }

    @Test
    public void getDetailRejectsIllegalTableName() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        WebApplicationException ex = assertThrows(WebApplicationException.class,
                () -> resource.getDetail(TableSpace.DEFAULT, "child_t; DROP"));
        assertEquals(400, ex.getResponse().getStatus());
    }

    // -----------------------------------------------------------------------
    // Storage layout tests
    // -----------------------------------------------------------------------

    @Test
    public void dataPagesReturnsActivePages() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        DataPageInfoDTO.PageLayoutDTO layout =
                resource.dataPages(TableSpace.DEFAULT, "child_t");

        assertNotNull(layout);
        assertNotNull(layout.getPages());
        assertEquals(layout.getPages().size(), layout.getTotalPages());
        assertTrue(
                "totalSizeBytes must be non-negative, got " + layout.getTotalSizeBytes(),
                layout.getTotalSizeBytes() >= 0);
    }

    @Test
    public void primaryIndexReturnsBlinkSnapshot() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        PrimaryIndexDTO snapshot =
                resource.primaryIndex(TableSpace.DEFAULT, "child_t");

        assertNotNull(snapshot);
        assertEquals("blink", snapshot.getType());
        assertEquals(50L, snapshot.getEntries());
        assertTrue(
                "loadedNodes must be > 0, got " + snapshot.getLoadedNodes(),
                snapshot.getLoadedNodes() > 0);
        assertTrue(
                "usedMemoryBytes must be > 0, got " + snapshot.getUsedMemoryBytes(),
                snapshot.getUsedMemoryBytes() > 0);
    }

    @Test
    public void indexDetailReturnsBrinBlocks() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        IndexDetailDTO detail = resource.indexDetail(
                TableSpace.DEFAULT, "child_t", "child_t_amount_idx");

        assertNotNull(detail);
        assertNotNull(detail.getSummary());
        assertEquals("child_t_amount_idx",
                detail.getSummary().getName().toLowerCase(java.util.Locale.ROOT));
        assertNotNull("blocks list must not be null", detail.getBlocks());
        assertFalse("BRIN index must report at least one block", detail.getBlocks().isEmpty());
        IndexDetailDTO.BrinBlockDTO firstBlock = detail.getBlocks().get(0);
        assertTrue(
                "block id must be non-negative, got " + firstBlock.getBlockId(),
                firstBlock.getBlockId() >= 0);
    }

    @Test
    public void indexDetail404OnUnknownIndex() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        assertThrows(NotFoundException.class,
                () -> resource.indexDetail(TableSpace.DEFAULT, "child_t", "missing_idx"));
    }

    @Test
    public void dataPages404OnUnknownTable() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        assertThrows(NotFoundException.class,
                () -> resource.dataPages(TableSpace.DEFAULT, "missing_t"));
    }

    @Test
    public void primaryIndexRejectsIllegalIdentifier() {
        TableResource resource = new TableResource(
                ServerLocator.of(serverRule.getServer()));

        WebApplicationException ex = assertThrows(WebApplicationException.class,
                () -> resource.primaryIndex("herd; DROP", "child_t"));
        assertEquals(400, ex.getResponse().getStatus());
    }
}
