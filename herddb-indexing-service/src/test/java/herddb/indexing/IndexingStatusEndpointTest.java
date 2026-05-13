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
package herddb.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.PersistentVectorStore;
import herddb.mem.MemoryDataStorageManager;
import herddb.mem.MemoryMetadataStorageManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration test for the {@code GET /status} HTTP endpoint on the Indexing
 * Service (issue #541).
 *
 * <p>Starts an {@link IndexingServer} with {@code indexing.http.enable=true}
 * (port 0 so the OS picks a free port), verifies:
 * <ul>
 *   <li>HTTP 200 with {@code Content-Type: application/json}</li>
 *   <li>JSON contains all sections from GetInstanceInfo, GetEngineStats,
 *       GetShadowStatus, and the new search metric totals</li>
 *   <li>{@code search_requests_total} increments after a gRPC search call</li>
 *   <li>{@code search_segments_queried_total} increments after the search</li>
 * </ul>
 */
public class IndexingStatusEndpointTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private IndexingServiceEngine engine;
    private IndexingServer server;
    private MemoryMetadataStorageManager engineMeta;

    @Before
    public void setUp() throws Exception {
        Path logDir = folder.newFolder("log").toPath();
        Path dataDir = folder.newFolder("data").toPath();

        Properties props = new Properties();
        props.setProperty(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE, "memory");
        props.setProperty(IndexingServerConfiguration.PROPERTY_HTTP_ENABLE, "true");
        // Port 0 → OS picks a free port; read back via server.getHttpPort().
        props.setProperty(IndexingServerConfiguration.PROPERTY_HTTP_PORT, "0");
        props.setProperty(IndexingServerConfiguration.PROPERTY_HTTP_HOST, "127.0.0.1");
        IndexingServerConfiguration config = new IndexingServerConfiguration(props);

        engineMeta = new MemoryMetadataStorageManager();
        engineMeta.start();
        engineMeta.ensureDefaultTableSpace("local", "local", 0, 1);

        engine = new IndexingServiceEngine(logDir, dataDir, config);
        engine.setMetadataStorageManager(engineMeta);

        server = new IndexingServer("localhost", 0, engine, config);
        server.start();
        engine.start();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (engine != null) {
            engine.close();
        }
        if (engineMeta != null) {
            engineMeta.close();
        }
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String fetch(String path) throws Exception {
        int httpPort = server.getHttpPort();
        assertTrue("HTTP server must be running (port > 0)", httpPort > 0);
        URL url = new URL("http://127.0.0.1:" + httpPort + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        assertEquals("Expected HTTP 200 from " + path, 200, conn.getResponseCode());
        String contentType = conn.getContentType();
        assertNotNull("Content-Type header must be set", contentType);
        assertTrue("Content-Type must be application/json; got: " + contentType,
                contentType.contains("application/json"));
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * GET /status must return HTTP 200 application/json even before any
     * search has been made, and must contain all expected JSON keys.
     */
    @Test
    public void testStatusEndpointReturns200WithAllSections() throws Exception {
        String json = fetch("/status");
        assertNotNull("JSON body must not be null", json);
        assertFalse("JSON body must not be empty", json.trim().isEmpty());

        // --- GetInstanceInfo section ---
        assertContainsKey(json, "instance_id");
        assertContainsKey(json, "grpc_host");
        assertContainsKey(json, "grpc_port");
        assertContainsKey(json, "storage_type");
        assertContainsKey(json, "data_dir");
        assertContainsKey(json, "tablespace_name");
        assertContainsKey(json, "tablespace_uuid");
        assertContainsKey(json, "instance_ordinal");
        assertContainsKey(json, "num_instances");
        assertContainsKey(json, "role");
        assertContainsKey(json, "shadow_of");
        assertContainsKey(json, "jvm_max_heap_bytes");

        // --- GetEngineStats section ---
        assertContainsKey(json, "uptime_millis");
        assertContainsKey(json, "tailer_running");
        assertContainsKey(json, "tailer_watermark_ledger");
        assertContainsKey(json, "tailer_watermark_offset");
        assertContainsKey(json, "tailer_entries_processed");
        assertContainsKey(json, "tailer_entries_accepted");
        assertContainsKey(json, "tailer_entries_skipped");
        assertContainsKey(json, "tailer_entries_shard_filtered");
        assertContainsKey(json, "tailer_inserts");
        assertContainsKey(json, "tailer_updates");
        assertContainsKey(json, "tailer_deletes");
        assertContainsKey(json, "tailer_ddl");
        assertContainsKey(json, "tailer_batches");
        assertContainsKey(json, "apply_queue_size");
        assertContainsKey(json, "apply_queue_capacity");
        assertContainsKey(json, "apply_parallelism");
        assertContainsKey(json, "loaded_index_count");
        assertContainsKey(json, "total_estimated_memory_bytes");
        assertContainsKey(json, "jvm_heap_used_bytes");
        assertContainsKey(json, "jvm_heap_max_bytes");
        assertContainsKey(json, "jvm_heap_used_pct");

        // --- GetShadowStatus section ---
        assertContainsKey(json, "is_shadow");
        assertContainsKey(json, "shadow_ready");
        assertContainsKey(json, "shadow_loaded_ledger_id");
        assertContainsKey(json, "shadow_loaded_offset");
        assertContainsKey(json, "primary_advertised_ledger_id");
        assertContainsKey(json, "primary_advertised_offset");
        assertContainsKey(json, "shadow_last_reload_timestamp_ms");
        assertContainsKey(json, "shadow_reload_count");
        assertContainsKey(json, "applied_index_status_generation");
        assertContainsKey(json, "shadow_loaded_entry_timestamp_ms");

        // --- Search metrics section (issue #541) ---
        assertContainsKey(json, "search_requests_total");
        assertContainsKey(json, "search_errors_total");
        assertContainsKey(json, "search_readfilerange_calls_total");
        assertContainsKey(json, "search_readfilerange_bytes_total");
        assertContainsKey(json, "search_readfilerange_wait_nanos_total");
        assertContainsKey(json, "search_segments_queried_total");
        assertContainsKey(json, "search_cache_hits_total");
        assertContainsKey(json, "search_cache_misses_total");
    }

    /**
     * After one gRPC search call, {@code search_requests_total} must be 1
     * and {@code search_segments_queried_total} must be >= 1 in /status.
     */
    @Test
    public void testSearchRequestsTotalIncrementsAfterSearch() throws Exception {
        // Register a vector index with some vectors so the search has something to query.
        Index index = Index.builder()
                .name("emb")
                .table("docs")
                .tablespace("default")
                .type(Index.TYPE_VECTOR)
                .column("embedding", ColumnTypes.FLOATARRAY)
                .build();

        Path dataDir = folder.newFolder("store-data").toPath();
        AbstractVectorStore store = new PersistentVectorStore(
                "emb", "docs", "default", "embedding",
                dataDir, new MemoryDataStorageManager(),
                server.buildMemoryManager(),
                16, 100, 1.2f, 1.4f, true, 2_000_000_000L, 0,
                Long.MAX_VALUE, VectorSimilarityFunction.EUCLIDEAN);

        for (int i = 0; i < 20; i++) {
            store.addVector(Bytes.from_int(i), new float[]{i * 0.1f, (20 - i) * 0.1f});
        }
        engine.registerIndexForTest(index, store);

        // Baseline: search_requests_total must be 0.
        String before = fetch("/status");
        long beforeRequests = extractLong(before, "search_requests_total");
        assertEquals("search_requests_total must start at 0", 0L, beforeRequests);
        long beforeSegments = extractLong(before, "search_segments_queried_total");
        assertEquals("search_segments_queried_total must start at 0", 0L, beforeSegments);

        // Perform one gRPC search via IndexingServiceClient.
        try (IndexingServiceClient client = IndexingServiceClient.fromAddresses(
                Arrays.asList("localhost:" + server.getPort()), 30)) {
            List<Map.Entry<Bytes, Float>> results =
                    client.search("default", "docs", "emb", new float[]{0.5f, 0.5f}, 5);
            assertNotNull("Search must return results", results);
        }

        // After search: search_requests_total must be 1, segments_queried >= 1.
        String after = fetch("/status");
        long afterRequests = extractLong(after, "search_requests_total");
        assertEquals("search_requests_total must be 1 after one search", 1L, afterRequests);

        long afterSegments = extractLong(after, "search_segments_queried_total");
        assertTrue("search_segments_queried_total must be >= 1 after search (got " + afterSegments + ")",
                afterSegments >= 1L);
    }

    /**
     * Asserts that a JSON string contains the given field key.
     */
    private static void assertContainsKey(String json, String key) {
        assertTrue("JSON must contain key \"" + key + "\"", json.contains("\"" + key + "\""));
    }

    /**
     * Extracts a long value from a naive JSON string for the given key.
     * Expects the pattern {@code "key": <long>} in the body.
     */
    private static long extractLong(String json, String key) {
        String needle = "\"" + key + "\": ";
        int idx = json.indexOf(needle);
        assertTrue("Key '" + key + "' not found in JSON", idx >= 0);
        int start = idx + needle.length();
        int end = start;
        // Consume digits (and optional leading minus).
        if (end < json.length() && json.charAt(end) == '-') {
            end++;
        }
        while (end < json.length() && Character.isDigit(json.charAt(end))) {
            end++;
        }
        String raw = json.substring(start, end).trim();
        return Long.parseLong(raw);
    }
}
