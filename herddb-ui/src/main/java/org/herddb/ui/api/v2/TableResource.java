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
import herddb.model.DataScannerException;
import herddb.server.Server;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
import org.herddb.ui.dto.ColumnDTO;
import org.herddb.ui.dto.DataPageInfoDTO;
import org.herddb.ui.dto.ForeignKeyDTO;
import org.herddb.ui.dto.IndexDetailDTO;
import org.herddb.ui.dto.IndexSummaryDTO;
import org.herddb.ui.dto.PrimaryIndexDTO;
import org.herddb.ui.dto.TableDetailDTO;
import org.herddb.ui.dto.TableSummaryDTO;
import org.herddb.ui.internal.QueryService;
import org.herddb.ui.internal.ServerLocator;

/**
 * REST endpoints for a single table: metadata (columns, secondary indexes,
 * foreign keys) and storage-layout visualisations (data pages, primary key
 * index, BRIN index blocks).
 *
 * <p>Mounted at {@code /api/v2/tablespaces/{tablespace}/tables/{table}}.
 * Having this resource own the full class-level path for an individual table
 * ensures that JAX-RS path-matching selects it (as the most specific
 * template) for <em>all</em> requests under that prefix, including the bare
 * {@code GET} that returns the table detail.
 */
@Path("tablespaces/{tablespace}/tables/{table}")
public class TableResource {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private final Server server;
    private final QueryService queryService;

    @Inject
    public TableResource(ServerLocator locator) {
        this.server = locator.getServer();
        this.queryService = new QueryService(this.server);
    }

    // -----------------------------------------------------------------------
    // Table detail (columns, secondary indexes, foreign keys)
    // -----------------------------------------------------------------------

    /**
     * Returns full metadata for the named table: summary statistics, column
     * definitions, secondary index list and foreign key constraints.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public TableDetailDTO getDetail(
            @PathParam("tablespace") String tablespace,
            @PathParam("table") String tableName) {
        validateIdentifier("tablespace", tablespace);
        validateIdentifier("table", tableName);
        try {
            TableSummaryDTO summary = loadSummary(tablespace, tableName);
            List<ColumnDTO> columns = loadColumns(tablespace, tableName);
            List<IndexSummaryDTO> indexes = loadIndexes(tablespace, tableName);
            List<ForeignKeyDTO> foreignKeys = loadForeignKeys(tablespace, tableName);

            TableDetailDTO detail = new TableDetailDTO();
            detail.setSummary(summary);
            detail.setColumns(columns);
            detail.setIndexes(indexes);
            detail.setForeignKeys(foreignKeys);
            return detail;
        } catch (DataScannerException e) {
            throw new WebApplicationException(
                    "Failed to read table " + tablespace + "." + tableName + ": " + e.getMessage(),
                    e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    // -----------------------------------------------------------------------
    // Storage layout endpoints
    // -----------------------------------------------------------------------

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
            List<PrimaryIndexDTO.BLinkNodeDTO> nodes = new ArrayList<>(snapshot.getNodes().size());
            for (herddb.index.blink.BLink.NodeInfo info : snapshot.getNodes()) {
                nodes.add(new PrimaryIndexDTO.BLinkNodeDTO(
                        info.getNodeId(),
                        info.getStoreId(),
                        info.isLeaf(),
                        info.getKeys(),
                        info.getSizeBytes(),
                        info.isLoaded(),
                        info.isDirty()));
            }
            return new PrimaryIndexDTO(
                    "blink",
                    snapshot.getEntries(),
                    snapshot.getLoadedNodes(),
                    snapshot.getTotalNodes(),
                    snapshot.getUsedMemoryBytes(),
                    nodes,
                    snapshot.isTruncated());
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

        // Populate statusProperties from sysindexstatus so the UI can show
        // runtime properties (numBlocks for BRIN, vector counts, etc.).
        out.setStatusProperties(loadIndexStatusProperties(tablespace, tableName, indexName));

        return out;
    }

    /**
     * Queries {@code sysindexstatus} for the given index and returns the raw
     * JSON {@code properties} string, or {@code null} if no row is found.
     */
    private String loadIndexStatusProperties(
            String tablespace, String tableName, String indexName) {
        try {
            List<Map<String, Object>> rows = queryService.selectRows(
                    tablespace, "SELECT * FROM sysindexstatus");
            for (Map<String, Object> row : rows) {
                if (tableName.equalsIgnoreCase(asString(row.get("table_name")))
                        && indexName.equalsIgnoreCase(asString(row.get("index_name")))) {
                    return asString(row.get("properties"));
                }
            }
        } catch (DataScannerException e) {
            // Non-fatal: log status is best-effort; the rest of the DTO is
            // already populated.  Return null so the UI omits the section.
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Private helpers — table detail SQL queries
    // -----------------------------------------------------------------------

    private TableSummaryDTO loadSummary(String tablespace, String tableName)
            throws DataScannerException {
        List<Map<String, Object>> tables = queryService.selectRows(
                tablespace, "SELECT * FROM systables");
        Map<String, Map<String, Object>> statsByName = indexByLowerCase(
                queryService.selectRows(tablespace, "SELECT * FROM systablestats"),
                "table_name");

        for (Map<String, Object> row : tables) {
            String name = asString(row.get("table_name"));
            if (tableName.equalsIgnoreCase(name)) {
                Map<String, Object> stats = statsByName.getOrDefault(
                        name == null ? "" : name.toLowerCase(Locale.ROOT),
                        Map.of());
                return toSummary(tablespace, row, stats);
            }
        }
        throw new NotFoundException(
                "No table '" + tableName + "' in tablespace '" + tablespace + "'");
    }

    private List<ColumnDTO> loadColumns(String tablespace, String tableName)
            throws DataScannerException {
        List<Map<String, Object>> rows = queryService.selectRows(
                tablespace, "SELECT * FROM syscolumns");
        List<ColumnDTO> out = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (!tableName.equalsIgnoreCase(asString(row.get("table_name")))) {
                continue;
            }
            ColumnDTO dto = new ColumnDTO();
            dto.setName(asString(row.get("column_name")));
            dto.setOrdinalPosition(asInt(row.get("ordinal_position")));
            dto.setNullable(asInt(row.get("is_nullable")) != 0);
            dto.setDataType(asString(row.get("data_type")));
            dto.setTypeName(asString(row.get("type_name")));
            dto.setAutoIncrement(asInt(row.get("auto_increment")) != 0);
            dto.setDefaultValue(asString(row.get("default_value")));
            out.add(dto);
        }
        out.sort(Comparator.comparingInt(ColumnDTO::getOrdinalPosition));
        return out;
    }

    private List<IndexSummaryDTO> loadIndexes(String tablespace, String tableName)
            throws DataScannerException {
        List<Map<String, Object>> indexRows = queryService.selectRows(
                tablespace, "SELECT * FROM sysindexes");
        List<Map<String, Object>> indexCols = queryService.selectRows(
                tablespace, "SELECT * FROM sysindexcolumns");

        // group columns by index uuid (uuid is unique; index_name may
        // collide across tables) so we can attach them to the right index.
        Map<String, List<Map<String, Object>>> colsByUuid = new HashMap<>();
        for (Map<String, Object> row : indexCols) {
            colsByUuid
                    .computeIfAbsent(asString(row.get("index_uuid")), k -> new ArrayList<>())
                    .add(row);
        }

        List<IndexSummaryDTO> out = new ArrayList<>();
        for (Map<String, Object> idx : indexRows) {
            if (!tableName.equalsIgnoreCase(asString(idx.get("table_name")))) {
                continue;
            }
            String uuid = asString(idx.get("index_uuid"));
            List<Map<String, Object>> cols = colsByUuid.getOrDefault(uuid, List.of());
            cols = new ArrayList<>(cols);
            cols.sort(Comparator.comparingInt(c -> asInt(c.get("ordinal_position"))));
            List<String> columnNames = new ArrayList<>(cols.size());
            for (Map<String, Object> c : cols) {
                columnNames.add(asString(c.get("column_name")));
            }
            IndexSummaryDTO dto = new IndexSummaryDTO();
            dto.setName(asString(idx.get("index_name")));
            dto.setUuid(uuid);
            dto.setType(asString(idx.get("index_type")));
            dto.setUnique(asInt(idx.get("unique")) != 0);
            dto.setColumns(columnNames);
            out.add(dto);
        }
        out.sort(Comparator.comparing(
                IndexSummaryDTO::getName,
                Comparator.nullsLast(String::compareToIgnoreCase)));
        return out;
    }

    private List<ForeignKeyDTO> loadForeignKeys(String tablespace, String tableName)
            throws DataScannerException {
        List<Map<String, Object>> rows = queryService.selectRows(
                tablespace, "SELECT * FROM sysforeignkeys");

        // Each row in sysforeignkeys describes a single column pair; group
        // the rows belonging to the same constraint together.
        Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            if (!tableName.equalsIgnoreCase(asString(row.get("child_table_name")))) {
                continue;
            }
            String name = asString(row.get("child_table_cons_name"));
            grouped.computeIfAbsent(name == null ? "" : name, k -> new ArrayList<>())
                    .add(row);
        }

        List<ForeignKeyDTO> out = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : grouped.entrySet()) {
            List<Map<String, Object>> fkRows = new ArrayList<>(entry.getValue());
            fkRows.sort(Comparator.comparingInt(r -> asInt(r.get("ordinal_position"))));
            ForeignKeyDTO dto = new ForeignKeyDTO();
            dto.setName(entry.getKey());
            Map<String, Object> first = fkRows.get(0);
            dto.setParentTable(asString(first.get("parent_table_name")));
            dto.setOnDeleteAction(asString(first.get("on_delete_action")));
            dto.setOnUpdateAction(asString(first.get("on_update_action")));
            List<String> childColumns = new ArrayList<>(fkRows.size());
            List<String> parentColumns = new ArrayList<>(fkRows.size());
            for (Map<String, Object> row : fkRows) {
                childColumns.add(asString(row.get("child_table_column_name")));
                parentColumns.add(asString(row.get("parent_table_column_name")));
            }
            dto.setChildColumns(childColumns);
            dto.setParentColumns(parentColumns);
            out.add(dto);
        }
        return out;
    }

    private static TableSummaryDTO toSummary(
            String tablespace,
            Map<String, Object> tableRow,
            Map<String, Object> stats) {
        TableSummaryDTO dto = new TableSummaryDTO();
        dto.setTablespace(tablespace);
        dto.setName(asString(tableRow.get("table_name")));
        dto.setUuid(asString(tableRow.get("table_uuid")));
        dto.setSystemTable(asBoolean(tableRow.get("systemtable")));
        dto.setTableSize(asLong(stats.get("tablesize")));
        dto.setLoadedPages(asInt(stats.get("loadedpages")));
        dto.setLoadedPagesCount(asLong(stats.get("loadedpagescount")));
        dto.setUnloadedPagesCount(asLong(stats.get("unloadedpagescount")));
        dto.setDirtyPages(asInt(stats.get("dirtypages")));
        dto.setDirtyRecords(asLong(stats.get("dirtyrecords")));
        dto.setMaxLogicalPageSize(asLong(stats.get("maxlogicalpagesize")));
        dto.setKeysMemory(asLong(stats.get("keysmemory")));
        dto.setBuffersMemory(asLong(stats.get("buffersmemory")));
        dto.setDirtyMemory(asLong(stats.get("dirtymemory")));
        return dto;
    }

    private static Map<String, Map<String, Object>> indexByLowerCase(
            List<Map<String, Object>> rows, String column) {
        Map<String, Map<String, Object>> out = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String key = asString(row.get(column));
            if (key != null) {
                out.put(key.toLowerCase(Locale.ROOT), row);
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------
    // Private helpers — storage layout (direct server API)
    // -----------------------------------------------------------------------

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

    // -----------------------------------------------------------------------
    // Type-coercion utilities (private static)
    // -----------------------------------------------------------------------

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

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    private static int asInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static long asLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static boolean asBoolean(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        String s = value.toString();
        return "true".equalsIgnoreCase(s) || "1".equals(s);
    }
}
