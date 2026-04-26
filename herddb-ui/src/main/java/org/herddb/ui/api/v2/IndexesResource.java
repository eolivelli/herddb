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

import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.DBManager;
import herddb.core.DataPageLayoutInfo;
import herddb.core.TableManager;
import herddb.core.TableSpaceManager;
import herddb.index.KeyToPageIndex;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.index.blink.IncrementalBLinkKeyToPageIndex;
import herddb.index.brin.BRINIndexManager;
import herddb.index.brin.BlockRangeIndex;
import herddb.server.Server;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.herddb.ui.dto.DataPageInfoDTO;
import org.herddb.ui.dto.IndexDetailDTO;
import org.herddb.ui.dto.IndexSummaryDTO;
import org.herddb.ui.dto.PrimaryIndexDTO;
import org.herddb.ui.internal.ServerLocator;

/**
 * REST endpoints for visualisations of a table's storage layout: data
 * pages, primary key index summary and BRIN block layout.
 *
 * <p>Mounted at {@code /api/v2/tablespaces/{ts}/tables/{table}/...}.
 * Reads run through narrow read-only helpers added on
 * {@link TableManager}, {@link BLinkKeyToPageIndex} and
 * {@link BlockRangeIndex} that do not lock and do not force checkpoints.
 */
@Path("tablespaces/{tablespace}/tables/{table}")
public class IndexesResource {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private final Server server;

    @Inject
    public IndexesResource(ServerLocator locator) {
        this.server = locator.getServer();
    }

    @GET
    @Path("data-pages")
    @Produces(MediaType.APPLICATION_JSON)
    public DataPageInfoDTO.PageLayoutDTO dataPages(
            @PathParam("tablespace") String tablespace,
            @PathParam("table") String tableName) {
        TableManager tableManager = resolveTableManager(tablespace, tableName);
        List<DataPageLayoutInfo> snapshot = tableManager.snapshotPagesLayout();
        List<DataPageInfoDTO> pages = new ArrayList<>(snapshot.size());
        long totalSize = 0L;
        long totalDirt = 0L;
        int loaded = 0;
        for (DataPageLayoutInfo info : snapshot) {
            pages.add(new DataPageInfoDTO(
                    info.getPageId(),
                    info.getSizeBytes(),
                    info.getAverageRecordSize(),
                    info.getDirtBytes(),
                    info.isLoaded()));
            totalSize += info.getSizeBytes();
            totalDirt += info.getDirtBytes();
            if (info.isLoaded()) {
                loaded++;
            }
        }
        DataPageInfoDTO.PageLayoutDTO out = new DataPageInfoDTO.PageLayoutDTO();
        out.setPages(pages);
        out.setTotalPages(pages.size());
        out.setLoadedPages(loaded);
        out.setTotalSizeBytes(totalSize);
        out.setTotalDirtBytes(totalDirt);
        return out;
    }

    @GET
    @Path("primary-index")
    @Produces(MediaType.APPLICATION_JSON)
    public PrimaryIndexDTO primaryIndex(
            @PathParam("tablespace") String tablespace,
            @PathParam("table") String tableName) {
        TableManager tableManager = resolveTableManager(tablespace, tableName);
        KeyToPageIndex pkIndex = tableManager.getKeyToPageIndex();
        BLinkKeyToPageIndex.PrimaryIndexSnapshot snapshot = null;
        if (pkIndex instanceof BLinkKeyToPageIndex) {
            snapshot = ((BLinkKeyToPageIndex) pkIndex).snapshotInfo();
        } else if (pkIndex instanceof IncrementalBLinkKeyToPageIndex) {
            snapshot = ((IncrementalBLinkKeyToPageIndex) pkIndex).snapshotInfo();
        }
        if (snapshot != null) {
            return new PrimaryIndexDTO(
                    "blink",
                    snapshot.getEntries(),
                    snapshot.getLoadedNodes(),
                    snapshot.getUsedMemoryBytes());
        }
        // Fall-back: report the implementing class so the UI can show
        // *something*, even for index types we do not know how to drill
        // into yet (the in-memory ConcurrentMapKeyToPageIndex used in
        // local mode falls into this branch).
        return new PrimaryIndexDTO(
                pkIndex.getClass().getSimpleName(),
                pkIndex.size(),
                0,
                0L);
    }

    @GET
    @Path("indexes/{index}")
    @Produces(MediaType.APPLICATION_JSON)
    public IndexDetailDTO indexDetail(
            @PathParam("tablespace") String tablespace,
            @PathParam("table") String tableName,
            @PathParam("index") String indexName) {
        validateIdentifier("index", indexName);

        TableSpaceManager tsm = resolveTableSpaceManager(tablespace);
        validateTableExists(tsm, tableName);

        Map<String, AbstractIndexManager> indexes = tsm.getIndexesOnTable(tableName);
        if (indexes == null || !indexes.containsKey(indexName)) {
            throw new NotFoundException(
                    "No index '" + indexName + "' on table '" + tableName + "'");
        }
        AbstractIndexManager index = indexes.get(indexName);

        IndexSummaryDTO summary = new IndexSummaryDTO();
        summary.setName(index.getIndexName());
        summary.setType(index.getIndex().type);
        summary.setUuid(index.getIndex().uuid);
        summary.setUnique(index.getIndex().unique);
        summary.setColumns(java.util.List.of(index.getColumnNames()));

        IndexDetailDTO out = new IndexDetailDTO();
        out.setSummary(summary);

        if (index instanceof BRINIndexManager) {
            BRINIndexManager brin = (BRINIndexManager) index;
            List<BlockRangeIndex.BlockSnapshot> snapshot = brin.snapshotBlocks();
            List<IndexDetailDTO.BrinBlockDTO> blocks = new ArrayList<>(snapshot.size());
            for (BlockRangeIndex.BlockSnapshot block : snapshot) {
                blocks.add(new IndexDetailDTO.BrinBlockDTO(
                        block.getBlockId(),
                        block.getPageId(),
                        block.getEntries(),
                        block.isLoaded(),
                        block.isDirty()));
            }
            out.setBlocks(blocks);
        } else {
            out.setBlocks(java.util.List.of());
        }
        return out;
    }

    // -- helpers --------------------------------------------------------

    private TableManager resolveTableManager(String tablespace, String tableName) {
        TableSpaceManager tsm = resolveTableSpaceManager(tablespace);
        AbstractTableManager generic = tsm.getTableManager(tableName);
        if (generic == null) {
            throw new NotFoundException(
                    "No table '" + tableName + "' in tablespace '" + tablespace + "'");
        }
        if (!(generic instanceof TableManager)) {
            throw new WebApplicationException(
                    "Table '" + tableName + "' is not a regular user table",
                    Response.Status.BAD_REQUEST);
        }
        return (TableManager) generic;
    }

    private TableSpaceManager resolveTableSpaceManager(String tablespace) {
        validateIdentifier("tablespace", tablespace);
        DBManager manager = server.getManager();
        TableSpaceManager tsm = manager.getTableSpaceManager(tablespace);
        if (tsm == null) {
            throw new NotFoundException(
                    "No tablespace '" + tablespace + "' on this node");
        }
        return tsm;
    }

    private static void validateTableExists(TableSpaceManager tsm, String tableName) {
        validateIdentifier("table", tableName);
        if (tsm.getTableManager(tableName) == null) {
            throw new NotFoundException("No table '" + tableName + "'");
        }
    }

    private static void validateIdentifier(String label, String value) {
        if (value == null || value.isEmpty()) {
            throw new WebApplicationException(
                    label + " cannot be empty",
                    Response.Status.BAD_REQUEST);
        }
        if (!IDENTIFIER.matcher(value).matches()) {
            throw new WebApplicationException(
                    label + " contains illegal characters: " + value,
                    Response.Status.BAD_REQUEST);
        }
    }
}
