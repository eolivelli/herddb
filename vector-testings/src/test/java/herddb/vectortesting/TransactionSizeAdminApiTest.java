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
 * {@code GET}/{@code POST /ingestion/config/transaction-size} admin API
 * endpoints. The transaction size is the JDBC commit unit; it must always be
 * {@code >= batch-size} (the per-flush unit) and {@code <= ingest-max-ops}
 * (when finite, since the rate limiter is acquired per commit).
 */
class TransactionSizeAdminApiTest {

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
    void getTransactionSizeWithDefaultReturnsBatchSize() throws Exception {
        // transactionSize defaults to 0 ("track batchSize"); the API surfaces
        // effectiveTransactionSize() so the user sees a meaningful integer.
        HttpResponse<String> resp = get("/ingestion/config/transaction-size");
        assertEquals(200, resp.statusCode());
        assertEquals(500, MAPPER.readTree(resp.body()).get("transaction-size").asInt());
    }

    @Test
    void getTransactionSizeAfterUpdate() throws Exception {
        runtime.setTransactionSize(2500);
        HttpResponse<String> resp = get("/ingestion/config/transaction-size");
        assertEquals(200, resp.statusCode());
        assertEquals(2500, MAPPER.readTree(resp.body()).get("transaction-size").asInt());
    }

    // ---- POST: happy path ----

    @Test
    void postTransactionSizeUpdatesConfig() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 5000}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(5000, json.get("transaction-size").asInt());
        assertEquals(5000, runtime.config().transactionSize);
        // batch-size is unchanged.
        assertEquals(500, runtime.config().batchSize);
    }

    @Test
    void postTransactionSizeAcceptsPlainNumericBody() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "750");
        assertEquals(200, resp.statusCode());
        assertEquals(750, runtime.config().transactionSize);
    }

    @Test
    void postTransactionSizeAtBatchSizeBoundaryIsAccepted() throws Exception {
        // transaction-size == batch-size must be allowed.
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 500}");
        assertEquals(200, resp.statusCode());
        assertEquals(500, runtime.config().transactionSize);
    }

    @Test
    void postTransactionSizeAtIngestMaxOpsBoundaryIsAccepted() throws Exception {
        runtime.setIngestMaxOps(1000);
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 1000}");
        assertEquals(200, resp.statusCode());
        assertEquals(1000, runtime.config().transactionSize);
    }

    @Test
    void postTransactionSizeAcceptedWhenIngestMaxOpsUnlimited() throws Exception {
        runtime.setIngestMaxOps(0); // unlimited
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 1_000_000}".replace("_", ""));
        assertEquals(200, resp.statusCode());
        assertEquals(1_000_000, runtime.config().transactionSize);
    }

    // ---- POST: validation rejects ----

    @Test
    void postTransactionSizeRejectsZero() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 0}");
        assertEquals(400, resp.statusCode());
        assertEquals(0, runtime.config().transactionSize);
    }

    @Test
    void postTransactionSizeRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": -1}");
        assertEquals(400, resp.statusCode());
        assertEquals(0, runtime.config().transactionSize);
    }

    @Test
    void postTransactionSizeRejectsBelowBatchSize() throws Exception {
        // batch-size = 500; transaction-size = 100 violates the invariant.
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 100}");
        assertEquals(400, resp.statusCode());
        assertEquals(0, runtime.config().transactionSize);
        assertEquals(500, runtime.config().batchSize);
    }

    @Test
    void postTransactionSizeRejectsAboveIngestMaxOps() throws Exception {
        // ingest-max-ops = 100_000; transaction-size = 200_000 violates the invariant.
        HttpResponse<String> resp = postJson("/ingestion/config/transaction-size", "{\"value\": 200000}");
        assertEquals(400, resp.statusCode());
        assertEquals(0, runtime.config().transactionSize);
        assertEquals(100_000, runtime.config().ingestMaxOpsPerSecond);
    }

    // ---- BenchRuntime unit-level validation ----

    @Test
    void benchRuntimeSetTransactionSizeRejectsBelowBatchSize() {
        try {
            runtime.setTransactionSize(100);
        } catch (IllegalArgumentException e) {
            assertEquals(0, runtime.config().transactionSize); // unchanged
            return;
        }
        throw new AssertionError("expected IllegalArgumentException");
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
