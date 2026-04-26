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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import herddb.model.DataScannerException;
import herddb.model.TableSpace;
import java.util.ArrayList;
import java.util.Collection;
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
import org.herddb.ui.dto.IndexingServiceIndexDTO;
import org.herddb.ui.dto.IndexingServicesOverviewDTO;
import org.herddb.ui.internal.QueryService;
import org.herddb.ui.internal.ServerLocator;

/**
 * REST endpoints for the indexing-services view. Reads {@code sysindexstatus},
 * filters out non-vector entries (BRIN indexes have their own visualisation),
 * parses the JSON {@code properties} column into structured DTO fields and
 * returns either a flat overview or the detail of a single index.
 *
 * <p>Mounted at {@code /api/v2/indexing-services} and
 * {@code /api/v2/indexing-services/{indexName}}.
 */
@Path("indexing-services")
public class IndexingServicesResource {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final QueryService queryService;

    @Inject
    public IndexingServicesResource(ServerLocator locator) {
        this(new QueryService(locator.getServer()));
    }

    public IndexingServicesResource(QueryService queryService) {
        if (queryService == null) {
            throw new IllegalArgumentException("queryService cannot be null");
        }
        this.queryService = queryService;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public IndexingServicesOverviewDTO list() {
        Collection<IndexingServiceIndexDTO> all = collectVectorIndexes();
        long totalVectors = 0L;
        for (IndexingServiceIndexDTO idx : all) {
            totalVectors += idx.getVectorCount();
        }
        IndexingServicesOverviewDTO out = new IndexingServicesOverviewDTO();
        out.setIndexes(new ArrayList<>(all));
        out.setTotalIndexes(all.size());
        out.setTotalVectorCount(totalVectors);
        return out;
    }

    @GET
    @Path("{indexName}")
    @Produces(MediaType.APPLICATION_JSON)
    public IndexingServiceIndexDTO get(@PathParam("indexName") String indexName) {
        if (indexName == null || indexName.isEmpty()) {
            throw new WebApplicationException(
                    "indexName cannot be empty",
                    Response.Status.BAD_REQUEST);
        }
        Collection<IndexingServiceIndexDTO> all = collectVectorIndexes();
        for (IndexingServiceIndexDTO idx : all) {
            if (indexName.equalsIgnoreCase(idx.getIndexName())) {
                return idx;
            }
        }
        throw new NotFoundException("No vector index named '" + indexName + "'");
    }

    // -- helpers --------------------------------------------------------

    private Collection<IndexingServiceIndexDTO> collectVectorIndexes() {
        List<Map<String, Object>> rows;
        try {
            // Read the running server's view of every index. sysindexstatus
            // is a virtual table on every tablespace; querying via the
            // default tablespace returns the rows produced by *that*
            // tablespace's manager. Other (non-default) tablespaces are
            // visible because the system table iterates every visible
            // table on each tablespace manager — see SysindexstatusTableManager.
            rows = queryService.selectRows(
                    TableSpace.DEFAULT,
                    "SELECT * FROM sysindexstatus");
        } catch (DataScannerException e) {
            throw new WebApplicationException(
                    "Failed to read sysindexstatus: " + e.getMessage(),
                    e,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
        List<IndexingServiceIndexDTO> out = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String type = asString(row.get("index_type"));
            // The vector index type reported by VectorIndexManager is
            // "vector"; we tolerate anything that contains that token to
            // accommodate future qualifiers.
            if (type == null || !type.toLowerCase(java.util.Locale.ROOT).contains("vector")) {
                continue;
            }
            out.add(toDto(row));
        }
        // Stable order so the UI can render a deterministic list.
        out.sort((a, b) -> {
            int byTablespace = nullsLastCompare(a.getTablespace(), b.getTablespace());
            if (byTablespace != 0) {
                return byTablespace;
            }
            int byTable = nullsLastCompare(a.getTableName(), b.getTableName());
            if (byTable != 0) {
                return byTable;
            }
            return nullsLastCompare(a.getIndexName(), b.getIndexName());
        });
        return Collections.unmodifiableList(out);
    }

    private static IndexingServiceIndexDTO toDto(Map<String, Object> row) {
        IndexingServiceIndexDTO dto = new IndexingServiceIndexDTO();
        dto.setTablespace(asString(row.get("tablespace")));
        dto.setTableName(asString(row.get("table_name")));
        dto.setIndexName(asString(row.get("index_name")));
        dto.setIndexType(asString(row.get("index_type")));
        dto.setIndexUuid(asString(row.get("index_uuid")));
        String props = asString(row.get("properties"));
        dto.setRawProperties(props);
        if (props != null && !props.isEmpty()) {
            try {
                JsonNode tree = JSON.readTree(props);
                if (tree.has("vectorCount")) {
                    dto.setVectorCount(tree.get("vectorCount").asLong(0L));
                }
                if (tree.has("segmentCount")) {
                    dto.setSegmentCount(tree.get("segmentCount").asInt(0));
                }
                if (tree.has("lastLsnLedger")) {
                    dto.setLastLsnLedger(tree.get("lastLsnLedger").asLong(0L));
                }
                if (tree.has("lastLsnOffset")) {
                    dto.setLastLsnOffset(tree.get("lastLsnOffset").asLong(0L));
                }
                if (tree.has("status")) {
                    dto.setStatus(tree.get("status").asText(null));
                }
                if (tree.has("error")) {
                    dto.setError(tree.get("error").asText(null));
                }
            } catch (JsonProcessingException e) {
                // Surface the parse failure so the UI can flag the row,
                // but do not let it abort the whole request — other
                // indexes are still useful to render.
                dto.setError("malformed properties JSON: " + e.getOriginalMessage());
            }
        }
        return dto;
    }

    private static int nullsLastCompare(String a, String b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return 1;
        }
        if (b == null) {
            return -1;
        }
        return a.compareToIgnoreCase(b);
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }
}
