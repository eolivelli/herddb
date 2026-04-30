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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import herddb.model.DataScannerException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import org.herddb.ui.dto.IndexingServiceIndexDTO;
import org.herddb.ui.dto.IndexingServicesOverviewDTO;
import org.herddb.ui.internal.QueryService;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit test for {@link IndexingServicesResource}.
 *
 * <p>The path that lights up vector-index rows in {@code sysindexstatus}
 * requires a connected indexing service, which is out of scope for the
 * herddb-ui unit-test rig (a separate cluster integration test would own
 * that). Instead, this test exercises the SELECT + JSON-property parsing
 * path with hand-crafted fixture rows wrapped in a stub
 * {@link QueryService}, and the empty-list path against a real embedded
 * server (which has no vector indexes by default).
 */
public class IndexingServicesResourceTest {

    @Rule
    public final EmbeddedHerdDbServerRule serverRule = new EmbeddedHerdDbServerRule();

    @Test
    public void emptyOverviewWhenNoVectorIndexes() {
        IndexingServicesResource resource = new IndexingServicesResource(
                new QueryService(serverRule.getServer()));

        IndexingServicesOverviewDTO overview = resource.list();

        assertNotNull(overview);
        assertNotNull(overview.getIndexes());
        assertEquals(
                "no vector indexes are present on a freshly booted server",
                0,
                overview.getTotalIndexes());
        assertEquals(0L, overview.getTotalVectorCount());
        assertTrue(overview.getIndexes().isEmpty());
    }

    @Test
    public void parsesVectorPropertiesIntoStructuredFields() {
        // Wrap the embedded server's QueryService and return our own
        // synthetic sysindexstatus rows (one vector index + one BRIN
        // index) so we exercise the JSON parsing path without needing a
        // real indexing service.
        IndexingServicesResource resource = new IndexingServicesResource(
                fixtureQueryService(
                        row("herd", "embeddings_t", "embeddings_idx", "vector",
                                "11111111-1111-1111-1111-111111111111",
                                "{\"vectorCount\":42,"
                                        + "\"segmentCount\":3,"
                                        + "\"tailerLsnLedger\":7,"
                                        + "\"tailerLsnOffset\":113,"
                                        + "\"durableLsnLedger\":5,"
                                        + "\"durableLsnOffset\":97,"
                                        + "\"status\":\"ready\"}"),
                        row("herd", "amounts_t", "brin_idx", "brin",
                                "22222222-2222-2222-2222-222222222222",
                                "{\"numBlocks\":4}")));

        IndexingServicesOverviewDTO overview = resource.list();

        assertEquals(1, overview.getTotalIndexes());
        assertEquals(42L, overview.getTotalVectorCount());

        IndexingServiceIndexDTO dto = overview.getIndexes().get(0);
        assertEquals("herd", dto.getTablespace());
        assertEquals("embeddings_t", dto.getTableName());
        assertEquals("embeddings_idx", dto.getIndexName());
        assertEquals("vector", dto.getIndexType());
        assertEquals("ready", dto.getStatus());
        assertEquals(42L, dto.getVectorCount());
        assertEquals(3, dto.getSegmentCount());
        assertEquals(7L, dto.getTailerLsnLedger());
        assertEquals(113L, dto.getTailerLsnOffset());
        assertEquals(5L, dto.getDurableLsnLedger());
        assertEquals(97L, dto.getDurableLsnOffset());
    }

    @Test
    public void getReturnsOneIndexByName() {
        IndexingServicesResource resource = new IndexingServicesResource(
                fixtureQueryService(
                        row("herd", "embeddings_t", "embeddings_idx", "vector",
                                "uuid-1",
                                "{\"vectorCount\":10,\"status\":\"ready\"}")));

        IndexingServiceIndexDTO dto = resource.get("embeddings_idx");
        assertNotNull(dto);
        assertEquals("embeddings_idx", dto.getIndexName());
        assertEquals(10L, dto.getVectorCount());
    }

    @Test
    public void getReturns404OnMissingIndex() {
        IndexingServicesResource resource = new IndexingServicesResource(
                fixtureQueryService(/* no rows */));
        assertThrows(NotFoundException.class, () -> resource.get("missing"));
    }

    @Test
    public void getRejectsBlankName() {
        IndexingServicesResource resource = new IndexingServicesResource(
                fixtureQueryService());
        WebApplicationException ex = assertThrows(
                WebApplicationException.class,
                () -> resource.get(""));
        assertEquals(400, ex.getResponse().getStatus());
    }

    @Test
    public void malformedPropertiesAreReportedAsErrorButDoNotCrash() {
        IndexingServicesResource resource = new IndexingServicesResource(
                fixtureQueryService(
                        row("herd", "embeddings_t", "embeddings_idx", "vector",
                                "uuid-1", "{not-json")));

        IndexingServicesOverviewDTO overview = resource.list();
        assertEquals(1, overview.getTotalIndexes());
        IndexingServiceIndexDTO dto = overview.getIndexes().get(0);
        assertNotNull(dto.getError());
        assertTrue(
                "error must mention the JSON parse failure, got: " + dto.getError(),
                dto.getError().toLowerCase(java.util.Locale.ROOT).contains("json")
                        || dto.getError().toLowerCase(java.util.Locale.ROOT)
                                .contains("malformed"));
    }

    // -- helpers --------------------------------------------------------

    private QueryService fixtureQueryService(Map<String, Object>... rows) {
        List<Map<String, Object>> snapshot = new ArrayList<>(List.of(rows));
        // Anonymous override that returns our fixture rows for any
        // sysindexstatus query and falls back to the real query service
        // for everything else (kept for future tests).
        return new QueryService(serverRule.getServer()) {
            @Override
            public List<Map<String, Object>> selectRows(
                    String tablespace, String query) throws DataScannerException {
                if (query.toUpperCase(java.util.Locale.ROOT)
                        .contains("SYSINDEXSTATUS")) {
                    return Collections.unmodifiableList(snapshot);
                }
                return super.selectRows(tablespace, query);
            }
        };
    }

    private static Map<String, Object> row(
            String tablespace, String tableName, String indexName,
            String type, String uuid, String properties) {
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("tablespace", tablespace);
        map.put("table_name", tableName);
        map.put("index_name", indexName);
        map.put("index_type", type);
        map.put("index_uuid", uuid);
        map.put("properties", properties);
        return map;
    }
}
