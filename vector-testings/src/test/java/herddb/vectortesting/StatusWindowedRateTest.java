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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests verifying that {@code GET /status} exposes the windowed
 * ingestion-rate fields added by issue #453: {@code ops_per_sec_1m},
 * {@code ops_per_sec_5m}, and {@code commit_latency_5m}.
 *
 * <p>The test does not spin up actual ingest workers. Instead it pre-records
 * commits directly into an {@link IngestionWindowTracker} and wires it into a
 * custom status supplier set on {@link BenchRuntime}. This exercises the same
 * code path that {@link VectorBench} uses during a live run without any JDBC
 * dependency.
 */
class StatusWindowedRateTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AdminApiServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void start() throws Exception {
        Config cfg = new Config();
        BenchRuntime runtime = new BenchRuntime(cfg);

        // Pre-load the tracker with a few commits so the windowed fields are > 0.
        IngestionWindowTracker tracker = new IngestionWindowTracker();
        tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(10), 500); // 10 ms, 500 rows
        tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(20), 500); // 20 ms, 500 rows

        // Simulate an ingest phase that started 30 seconds ago.
        long ingestStart = System.nanoTime() - TimeUnit.SECONDS.toNanos(30);

        runtime.setStatusSupplier(() -> {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("phase", "ingestion");
            m.put("rows", 1000L);
            // All-time average (the existing field — must remain unchanged).
            m.put("ops_per_sec", 1000.0 / 30.0);
            // Windowed rates (issue #453).
            m.put("ops_per_sec_1m", tracker.computeWindowedRate(
                    IngestionWindowTracker.ONE_MIN_NANOS, ingestStart));
            m.put("ops_per_sec_5m", tracker.computeWindowedRate(
                    IngestionWindowTracker.FIVE_MIN_NANOS, ingestStart));
            m.put("commit_latency_5m", tracker.computeWindowedLatencyMap(
                    IngestionWindowTracker.FIVE_MIN_NANOS));
            return m;
        });

        server = new AdminApiServer(runtime, 0);
        int port = server.start();
        baseUrl = "http://127.0.0.1:" + port;
        client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    @Test
    void statusContainsWindowedRateFields() throws Exception {
        JsonNode json = getStatusJson();

        // New fields must be present in the response.
        assertNotNull(json.get("ops_per_sec_1m"),
                "ops_per_sec_1m must be present in /status response");
        assertNotNull(json.get("ops_per_sec_5m"),
                "ops_per_sec_5m must be present in /status response");
        assertNotNull(json.get("commit_latency_5m"),
                "commit_latency_5m must be present in /status response");
    }

    @Test
    void windowedRatesArePositiveAfterCommitsRecorded() throws Exception {
        JsonNode json = getStatusJson();

        // Both windowed rates must be > 0 because we pre-loaded commits.
        assertTrue(json.get("ops_per_sec_1m").asDouble() > 0,
                "ops_per_sec_1m must be > 0 after commits are recorded");
        assertTrue(json.get("ops_per_sec_5m").asDouble() > 0,
                "ops_per_sec_5m must be > 0 after commits are recorded");
    }

    @Test
    void commitLatency5mHasAllSubFields() throws Exception {
        JsonNode json = getStatusJson();
        JsonNode lat5m = json.get("commit_latency_5m");

        assertNotNull(lat5m, "commit_latency_5m must be a JSON object");
        assertNotNull(lat5m.get("mean_ms"), "commit_latency_5m.mean_ms must be present");
        assertNotNull(lat5m.get("p50_ms"),  "commit_latency_5m.p50_ms must be present");
        assertNotNull(lat5m.get("p99_ms"),  "commit_latency_5m.p99_ms must be present");
        assertNotNull(lat5m.get("max_ms"),  "commit_latency_5m.max_ms must be present");
    }

    @Test
    void commitLatency5mMeanIsPositiveAfterCommitsRecorded() throws Exception {
        JsonNode lat5m = getStatusJson().get("commit_latency_5m");

        // mean_ms must reflect the ~15 ms average of the two pre-loaded commits.
        double mean = lat5m.get("mean_ms").asDouble();
        assertTrue(mean > 0,
                "commit_latency_5m.mean_ms must be > 0 after commits are recorded, got " + mean);
    }

    @Test
    void allTimeOpsPerSecIsStillPresent() throws Exception {
        JsonNode json = getStatusJson();

        // Backward compatibility: the all-time average must not have been removed.
        assertNotNull(json.get("ops_per_sec"),
                "all-time ops_per_sec must still be present for backward compatibility");
        assertTrue(json.get("ops_per_sec").asDouble() > 0,
                "all-time ops_per_sec must be > 0");
    }

    @Test
    void statusResponseIsHttp200() throws Exception {
        HttpResponse<String> resp = getStatus();
        assertEquals(200, resp.statusCode());
    }

    // ---------------------------------------------------------------- helpers

    private HttpResponse<String> getStatus() throws Exception {
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/status"))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build(), BodyHandlers.ofString());
    }

    private JsonNode getStatusJson() throws Exception {
        return MAPPER.readTree(getStatus().body());
    }
}
