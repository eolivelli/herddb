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
package herddb.vectortesting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #401: end-to-end tests for the new
 * {@code GET}/{@code POST /ingestion/config/batch-size} admin API endpoints.
 *
 * <p>Each test starts an {@link AdminApiServer} on an ephemeral port and
 * performs synchronous HTTP requests with bounded timeouts — no sleeps, no
 * shared state, no port collisions.
 */
class BatchSizeAdminApiTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BenchRuntime runtime;
    private AdminApiServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void start() throws Exception {
        Config cfg = new Config();
        cfg.ingestThreads = 1;
        cfg.batchSize = 500;
        cfg.transactionSize = 0; // default: track batchSize
        cfg.ingestMaxOpsPerSecond = 100_000;
        runtime = new BenchRuntime(cfg);
        server = new AdminApiServer(runtime, 0);
        int port = server.start();
        baseUrl = "http://127.0.0.1:" + port;
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    // ---- GET ----

    @Test
    void getBatchSizeReturnsCurrentValue() throws Exception {
        HttpResponse<String> resp = get("/ingestion/config/batch-size");
        assertEquals(200, resp.statusCode());
        assertEquals(500, MAPPER.readTree(resp.body()).get("batch-size").asInt());
    }

    @Test
    void getIngestionConfigIncludesBatchAndTransactionSize() throws Exception {
        HttpResponse<String> resp = get("/ingestion/config");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(500, json.get("batch-size").asInt());
        // When transactionSize is unset (0), the API surfaces effectiveTransactionSize() == batch-size.
        assertEquals(500, json.get("transaction-size").asInt());
    }

    // ---- POST: happy path ----

    @Test
    void postBatchSizeUpdatesConfig() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 250}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        // Response is the full ingestion config, same shape as GET /ingestion/config.
        assertEquals(250, json.get("batch-size").asInt());
        assertEquals(250, runtime.config().batchSize);
    }

    @Test
    void postBatchSizeReflectedInGetIngestionConfig() throws Exception {
        postJson("/ingestion/config/batch-size", "{\"value\": 100}");
        HttpResponse<String> resp = get("/ingestion/config");
        assertEquals(200, resp.statusCode());
        assertEquals(100, MAPPER.readTree(resp.body()).get("batch-size").asInt());
    }

    @Test
    void postBatchSizeAcceptsPlainNumericBody() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "42");
        assertEquals(200, resp.statusCode());
        assertEquals(42, runtime.config().batchSize);
    }

    @Test
    void postBatchSizeAtTransactionBoundaryIsAccepted() throws Exception {
        runtime.setTransactionSize(1000);
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 1000}");
        assertEquals(200, resp.statusCode());
        assertEquals(1000, runtime.config().batchSize);
    }

    // ---- POST: validation rejects ----

    @Test
    void postBatchSizeRejectsZero() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 0}");
        assertEquals(400, resp.statusCode());
        // Config untouched on rejection.
        assertEquals(500, runtime.config().batchSize);
    }

    @Test
    void postBatchSizeRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": -10}");
        assertEquals(400, resp.statusCode());
        assertEquals(500, runtime.config().batchSize);
    }

    @Test
    void postBatchSizeRejectsAboveTransactionSize() throws Exception {
        runtime.setTransactionSize(1000);
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 1001}");
        assertEquals(400, resp.statusCode());
        // Rejection leaves both fields untouched.
        assertEquals(500, runtime.config().batchSize);
        assertEquals(1000, runtime.config().transactionSize);
    }

    @Test
    void postBatchSizeRejectsAboveIngestMaxOpsWhenTransactionSizeUnset() throws Exception {
        // batchSize == effectiveTransactionSize when transactionSize == 0.
        runtime.setIngestMaxOps(1000);
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 1001}");
        assertEquals(400, resp.statusCode());
        assertEquals(500, runtime.config().batchSize);
    }

    @Test
    void postBatchSizeAcceptedWhenIngestMaxOpsUnlimited() throws Exception {
        runtime.setIngestMaxOps(0); // unlimited
        HttpResponse<String> resp = postJson("/ingestion/config/batch-size", "{\"value\": 50000}");
        assertEquals(200, resp.statusCode());
        assertEquals(50_000, runtime.config().batchSize);
    }

    // ---- helpers ----

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(), BodyHandlers.ofString());
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString(body))
                .build(), BodyHandlers.ofString());
    }
}
