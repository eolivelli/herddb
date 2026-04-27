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
import herddb.model.TableSpace;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.herddb.ui.dto.TablespaceDTO;
import org.herddb.ui.internal.QueryService;
import org.herddb.ui.internal.ServerLocator;

/**
 * Lists tablespaces visible from the running server by querying the
 * {@code systablespaces} virtual table.
 *
 * <p>Mounted at {@code GET /api/v2/tablespaces}.
 */
@Path("tablespaces")
public class TablespacesResource {

    private final QueryService queryService;

    @Inject
    public TablespacesResource(ServerLocator locator) {
        this(new QueryService(locator.getServer()));
    }

    /**
     * Test-friendly constructor for direct-call tests that already have a
     * {@link QueryService}.
     *
     * @param queryService the read-only query gateway
     */
    public TablespacesResource(QueryService queryService) {
        if (queryService == null) {
            throw new IllegalArgumentException("queryService cannot be null");
        }
        this.queryService = queryService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TablespaceDTO> list() {
        List<Map<String, Object>> rows;
        try {
            // Run the query against the default tablespace; system tables are
            // not tied to any particular tablespace and the planner accepts
            // the global table reference. We use SELECT * because some of
            // the column names (notably "uuid") are reserved keywords in
            // the Calcite SQL grammar and cannot be referenced by name in
            // the projection list without quoting.
            rows = queryService.selectRows(
                    TableSpace.DEFAULT,
                    "SELECT * FROM systablespaces");
        } catch (DataScannerException e) {
            throw new WebApplicationException(
                    "Failed to read systablespaces: " + e.getMessage(),
                    e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }

        List<TablespaceDTO> out = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            out.add(toDto(row));
        }
        return out;
    }

    @GET
    @Path("{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public TablespaceDTO get(@PathParam("name") String name) {
        if (name == null || name.isEmpty()) {
            throw new WebApplicationException(
                    "tablespace name cannot be empty",
                    Response.Status.BAD_REQUEST);
        }
        // Reuse list() and filter in Java; the systablespaces virtual table
        // is small (one row per tablespace) so this is cheap and avoids the
        // SQL-injection surface that would come with embedding the path
        // segment into the WHERE clause.
        List<TablespaceDTO> all = list();
        for (TablespaceDTO ts : all) {
            if (name.equals(ts.getName())) {
                return ts;
            }
        }
        throw new NotFoundException("No tablespace named '" + name + "'");
    }

    private static TablespaceDTO toDto(Map<String, Object> row) {
        String replicaJoined = asString(row.get("replica"));
        List<String> replicas = replicaJoined == null || replicaJoined.isEmpty()
                ? Collections.emptyList()
                : List.of(replicaJoined.split(","));
        return new TablespaceDTO(
                asString(row.get("tablespace_name")),
                asString(row.get("uuid")),
                asString(row.get("leader")),
                replicas,
                asInt(row.get("expectedreplicacount")),
                asLong(row.get("maxleaderinactivitytime")));
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
}
