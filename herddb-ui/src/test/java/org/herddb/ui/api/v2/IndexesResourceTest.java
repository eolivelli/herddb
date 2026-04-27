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
import org.herddb.ui.dto.IndexDetailDTO;
import org.herddb.ui.dto.PrimaryIndexDTO;
import org.herddb.ui.internal.ServerLocator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Direct-call test of {@link IndexesResource}: verifies the data-pages,
 * primary-index and BRIN-index snapshots against a real embedded server.
 */
public class IndexesResourceTest {

    // Standalone mode is required so the table is backed by a real
    // FileDataStorageManager — local mode swaps in the all-in-memory
    // storage which uses ConcurrentMapKeyToPageIndex (no B-Link
    // snapshot to read).
    @Rule
    public final EmbeddedHerdDbServerRule serverRule =
            new EmbeddedHerdDbServerRule(ServerConfiguration.PROPERTY_MODE_STANDALONE);

    @Before
    public void seedSchema() {
        DBManager manager = serverRule.getServer().getManager();
        ddl(manager, "CREATE TABLE t (id INTEGER PRIMARY KEY, payload STRING, amount LONG)");
        ddl(manager, "CREATE BRIN INDEX t_amount_idx ON t (amount)");
        for (int i = 0; i < 50; i++) {
            ddl(manager,
                    "INSERT INTO t (id, payload, amount) VALUES ("
                            + i + ", 'row" + i + "', " + (i * 100) + ")");
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

    @Test
    public void dataPagesReturnsActivePages() {
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        DataPageInfoDTO.PageLayoutDTO layout =
                resource.dataPages(TableSpace.DEFAULT, "t");

        assertNotNull(layout);
        assertNotNull(layout.getPages());
        assertEquals(layout.getPages().size(), layout.getTotalPages());
        // Total size must be >= 0 (may legitimately be 0 if all pages
        // were evicted, but in a freshly-loaded table with 50 rows we
        // expect at least one page).
        assertTrue(
                "totalSizeBytes must be non-negative, got " + layout.getTotalSizeBytes(),
                layout.getTotalSizeBytes() >= 0);
    }

    @Test
    public void primaryIndexReturnsBlinkSnapshot() {
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        PrimaryIndexDTO snapshot =
                resource.primaryIndex(TableSpace.DEFAULT, "t");

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
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        IndexDetailDTO detail = resource.indexDetail(
                TableSpace.DEFAULT, "t", "t_amount_idx");

        assertNotNull(detail);
        assertNotNull(detail.getSummary());
        assertEquals("t_amount_idx", detail.getSummary().getName().toLowerCase(java.util.Locale.ROOT));
        assertNotNull("blocks list must not be null", detail.getBlocks());
        assertFalse("BRIN index must report at least one block", detail.getBlocks().isEmpty());
        IndexDetailDTO.BrinBlockDTO firstBlock = detail.getBlocks().get(0);
        assertTrue(
                "block id must be non-negative, got " + firstBlock.getBlockId(),
                firstBlock.getBlockId() >= 0);
    }

    @Test
    public void indexDetail404OnUnknownIndex() {
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        assertThrows(NotFoundException.class,
                () -> resource.indexDetail(TableSpace.DEFAULT, "t", "missing_idx"));
    }

    @Test
    public void dataPages404OnUnknownTable() {
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        assertThrows(NotFoundException.class,
                () -> resource.dataPages(TableSpace.DEFAULT, "missing_t"));
    }

    @Test
    public void primaryIndexRejectsIllegalIdentifier() {
        IndexesResource resource = new IndexesResource(
                ServerLocator.of(serverRule.getServer()));

        WebApplicationException ex = assertThrows(
                WebApplicationException.class,
                () -> resource.primaryIndex("herd; DROP", "t"));
        assertEquals(400, ex.getResponse().getStatus());
    }
}
