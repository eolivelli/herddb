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

import herddb.model.DataScannerException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.herddb.ui.dto.ColumnDTO;
import org.herddb.ui.dto.ForeignKeyDTO;
import org.herddb.ui.dto.IndexSummaryDTO;
import org.herddb.ui.dto.TableDetailDTO;
import org.herddb.ui.dto.TableSummaryDTO;
import org.herddb.ui.internal.QueryService;
import org.herddb.ui.internal.ServerLocator;

/**
 * REST endpoints for tables and their immediate metadata
 * (columns, indexes, foreign keys).
 *
 * <p>Mounted at {@code /api/v2/tablespaces/{tablespace}/tables}.
 */
@Path("tablespaces/{tablespace}/tables")
public class TablesResource {

    /**
     * Identifier whitelist applied to user-supplied path segments before
     * they are interpolated into SQL. Matches HerdDB's identifier syntax
     * (letters, digits, underscores). This is defence-in-depth on top of
     * the SELECT-only guard in {@link QueryService}.
     */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private final QueryService queryService;

    @Inject
    public TablesResource(ServerLocator locator) {
        this(new QueryService(locator.getServer()));
    }

    public TablesResource(QueryService queryService) {
        if (queryService == null) {
            throw new IllegalArgumentException("queryService cannot be null");
        }
        this.queryService = queryService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TableSummaryDTO> list(@PathParam("tablespace") String tablespace) {
        validateIdentifier("tablespace", tablespace);
        try {
            // systables and systablestats are joined in Java rather than
            // SQL: the planner does not always optimise the join over
            // virtual tables, and the UI is happy with a single per-table
            // pass.
            List<Map<String, Object>> tables = queryService.selectRows(
                    tablespace, "SELECT * FROM systables");
            Map<String, Map<String, Object>> statsByName = indexByLowerCase(
                    queryService.selectRows(tablespace, "SELECT * FROM systablestats"),
                    "table_name");
            List<TableSummaryDTO> out = new ArrayList<>(tables.size());
            for (Map<String, Object> tableRow : tables) {
                String tableName = asString(tableRow.get("table_name"));
                Map<String, Object> stats = statsByName.getOrDefault(
                        tableName == null ? "" : tableName.toLowerCase(java.util.Locale.ROOT),
                        Map.of());
                out.add(toSummary(tablespace, tableRow, stats));
            }
            out.sort(Comparator.comparing(
                    TableSummaryDTO::getName,
                    Comparator.nullsLast(String::compareToIgnoreCase)));
            return out;
        } catch (DataScannerException e) {
            throw new WebApplicationException(
                    "Failed to read tables from " + tablespace + ": " + e.getMessage(),
                    e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TableDetailDTO get(
            @PathParam("tablespace") String tablespace,
            @PathParam("name") String name) {
        validateIdentifier("tablespace", tablespace);
        validateIdentifier("name", name);
        try {
            TableSummaryDTO summary = list(tablespace).stream()
                    .filter(s -> name.equalsIgnoreCase(s.getName()))
                    .findFirst()
                    .orElseThrow(() -> new NotFoundException(
                            "No table '" + name + "' in tablespace '" + tablespace + "'"));

            List<ColumnDTO> columns = loadColumns(tablespace, name);
            List<IndexSummaryDTO> indexes = loadIndexes(tablespace, name);
            List<ForeignKeyDTO> foreignKeys = loadForeignKeys(tablespace, name);

            TableDetailDTO detail = new TableDetailDTO();
            detail.setSummary(summary);
            detail.setColumns(columns);
            detail.setIndexes(indexes);
            detail.setForeignKeys(foreignKeys);
            return detail;
        } catch (DataScannerException e) {
            throw new WebApplicationException(
                    "Failed to read table " + tablespace + "." + name + ": " + e.getMessage(),
                    e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    // -- helpers --------------------------------------------------------

    private List<ColumnDTO> loadColumns(String tablespace, String table)
            throws DataScannerException {
        // syscolumns has no tablespace column; filter by table name in
        // Java to keep the SQL trivial. The set is small (one row per
        // column).
        List<Map<String, Object>> rows = queryService.selectRows(
                tablespace, "SELECT * FROM syscolumns");
        List<ColumnDTO> out = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (!table.equalsIgnoreCase(asString(row.get("table_name")))) {
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

    private List<IndexSummaryDTO> loadIndexes(String tablespace, String table)
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
            if (!table.equalsIgnoreCase(asString(idx.get("table_name")))) {
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

    private List<ForeignKeyDTO> loadForeignKeys(String tablespace, String table)
            throws DataScannerException {
        List<Map<String, Object>> rows = queryService.selectRows(
                tablespace, "SELECT * FROM sysforeignkeys");

        // Each row in sysforeignkeys describes a single column pair; group
        // the rows belonging to the same constraint together.
        Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            if (!table.equalsIgnoreCase(asString(row.get("child_table_name")))) {
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
                out.put(key.toLowerCase(java.util.Locale.ROOT), row);
            }
        }
        return out;
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
