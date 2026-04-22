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
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link AdminApiServer}. Each test starts the server on
 * an ephemeral port, makes synchronous HTTP requests with bounded timeouts,
 * then stops the server — no sleeps, no shared state, no port collisions.
 */
class AdminApiServerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BenchRuntime runtime;
    private AdminApiServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void start() throws Exception {
        Config cfg = new Config();
        cfg.ingestMaxOpsPerSecond = 100_000;
        cfg.queryMaxOpsPerSecond = 10;
        cfg.topK = 10;
        cfg.queryThreads = 4;
        cfg.queryCount = 1000;
        cfg.ingestThreads = 4;
        cfg.batchSize = 500;
        cfg.numRows = 100_000L;
        cfg.resumeFrom = 0L;
        cfg.ingestCommitRetries = 3;
        runtime = new BenchRuntime(cfg);
        server = new AdminApiServer(runtime, 0); // port 0 = ephemeral
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
    void getIngestionConfigReturnsExpectedFields() throws Exception {
        HttpResponse<String> resp = get("/ingestion/config");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(100_000, json.get("ingest-max-ops").asInt());
        assertEquals(4, json.get("ingest-threads").asInt());
        assertEquals(500, json.get("batch-size").asInt());
        assertEquals(100_000L, json.get("rows").asLong());
        assertEquals(0L, json.get("resume-from").asLong());
        assertEquals(3, json.get("ingest-commit-retries").asInt());
    }

    @Test
    void getQueryConfigReturnsExpectedFields() throws Exception {
        HttpResponse<String> resp = get("/query/config");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(10, json.get("query-max-ops").asInt());
        assertEquals(4, json.get("query-threads").asInt());
        assertEquals(1000, json.get("queries").asInt());
        assertEquals(10, json.get("top-k").asInt());
    }

    @Test
    void postIngestMaxOpsUpdatesConfigAndRateLimiter() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "{\"value\": 5000}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(5000, json.get("ingest-max-ops").asInt());
        assertEquals(5000, runtime.config().ingestMaxOpsPerSecond);
        // Guava RateLimiter.getRate() returns exactly the value passed to setRate().
        assertEquals(5000.0, runtime.ingestRateLimiter().getRate(), 1e-6);
    }

    @Test
    void postIngestMaxOpsWithZeroMeansUnlimited() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "{\"value\": 0}");
        assertEquals(200, resp.statusCode());
        assertEquals(0, runtime.config().ingestMaxOpsPerSecond);
        assertEquals(BenchRuntime.UNLIMITED_RATE, runtime.ingestRateLimiter().getRate(), 1.0);
    }

    @Test
    void postQueryMaxOpsUpdatesConfigAndRateLimiter() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-max-ops", "{\"value\": 250}");
        assertEquals(200, resp.statusCode());
        assertEquals(250, runtime.config().queryMaxOpsPerSecond);
        assertEquals(250.0, runtime.queryRateLimiter().getRate(), 1e-6);
    }

    @Test
    void postTopKUpdatesConfig() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/top-k", "{\"value\": 32}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(32, json.get("top-k").asInt());
        assertEquals(32, runtime.config().topK);
    }

    @Test
    void postTopKRejectsNonPositive() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/top-k", "{\"value\": 0}");
        assertEquals(400, resp.statusCode());
        // Config must be untouched on reject.
        assertEquals(10, runtime.config().topK);
    }

    @Test
    void postAcceptsPlainNumericBody() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "1234");
        assertEquals(200, resp.statusCode());
        assertEquals(1234, runtime.config().ingestMaxOpsPerSecond);
    }

    @Test
    void postWithInvalidBodyReturnsBadRequest() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "not-a-number");
        assertEquals(400, resp.statusCode());
        // Unchanged from @BeforeEach setup.
        assertEquals(100_000, runtime.config().ingestMaxOpsPerSecond);
    }

    @Test
    void postWithEmptyBodyReturnsBadRequest() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "");
        assertEquals(400, resp.statusCode());
    }

    @Test
    void unknownEndpointReturns404() throws Exception {
        HttpResponse<String> resp = get("/does/not/exist");
        assertEquals(404, resp.statusCode());
    }

    @Test
    void statusEndpointReturnsSuppliedMap() throws Exception {
        AtomicReference<Integer> callCount = new AtomicReference<>(0);
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("phase", "ingestion");
        snapshot.put("rows", 42L);
        snapshot.put("ops_per_sec", 1234.5);
        runtime.setStatusSupplier(() -> {
            callCount.set(callCount.get() + 1);
            return snapshot;
        });

        HttpResponse<String> resp = get("/status");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals("ingestion", json.get("phase").asText());
        assertEquals(42L, json.get("rows").asLong());
        assertEquals(1234.5, json.get("ops_per_sec").asDouble());
        assertEquals(1, callCount.get());
    }

    @Test
    void statusEndpointDefaultsToIdle() throws Exception {
        // Fresh runtime — no supplier set.
        HttpResponse<String> resp = get("/status");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals("idle", json.get("phase").asText());
    }

    @Test
    void serverStartsAndStopsCleanly() throws Exception {
        // Separately start a second server on another ephemeral port to verify
        // lifecycle independence. Stops at end to free the port.
        BenchRuntime r2 = new BenchRuntime(new Config());
        AdminApiServer s2 = new AdminApiServer(r2, 0);
        int port = s2.start();
        assertTrue(port > 0);
        try {
            HttpClient c = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = c.send(HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + port + "/status"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build(), BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertNotNull(resp.body());
        } finally {
            s2.stop();
        }
    }

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

    @Test
    void benchRuntimeConstructorMapsUnlimitedToLargeRate() {
        Config c = new Config();
        c.ingestMaxOpsPerSecond = 0;
        c.queryMaxOpsPerSecond = 0;
        BenchRuntime r = new BenchRuntime(c);
        assertEquals(BenchRuntime.UNLIMITED_RATE, r.ingestRateLimiter().getRate(), 1.0);
        assertEquals(BenchRuntime.UNLIMITED_RATE, r.queryRateLimiter().getRate(), 1.0);
    }

    @Test
    void benchRuntimeConstructorKeepsConfiguredRate() {
        Config c = new Config();
        c.ingestMaxOpsPerSecond = 7777;
        c.queryMaxOpsPerSecond = 111;
        BenchRuntime r = new BenchRuntime(c);
        assertEquals(7777.0, r.ingestRateLimiter().getRate(), 1e-6);
        assertEquals(111.0, r.queryRateLimiter().getRate(), 1e-6);
    }

    @Test
    void readPortFromSystemPropertyDefaultIsEightyEighty() {
        // Guard: we never mutate the real system property during tests to avoid
        // cross-test contamination, but we can read the default when it's unset.
        // This test only runs assertions if the property is not set externally.
        String existing = System.getProperty(AdminApiServer.PORT_SYSTEM_PROPERTY);
        if (existing == null) {
            assertEquals(AdminApiServer.DEFAULT_PORT, AdminApiServer.readPortFromSystemProperty());
        } else {
            // Respect externally-set property; just assert it parses cleanly.
            assertEquals(Integer.parseInt(existing), AdminApiServer.readPortFromSystemProperty());
        }
    }

    @Test
    void postIngestMaxOpsRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops", "{\"value\": -1}");
        assertEquals(400, resp.statusCode());
        assertEquals(100_000, runtime.config().ingestMaxOpsPerSecond);
        assertEquals(100_000.0, runtime.ingestRateLimiter().getRate(), 1e-6);
    }

    @Test
    void postQueryMaxOpsRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-max-ops", "{\"value\": -5}");
        assertEquals(400, resp.statusCode());
        assertEquals(10, runtime.config().queryMaxOpsPerSecond);
        assertEquals(10.0, runtime.queryRateLimiter().getRate(), 1e-6);
    }

    @Test
    void postTopKRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/top-k", "{\"value\": -3}");
        assertEquals(400, resp.statusCode());
        assertEquals(10, runtime.config().topK);
    }

    /**
     * Behavioral test: after POST lowers the rate, subsequent acquisitions
     * are throttled to match; after POST raises the rate, they complete
     * quickly.
     *
     * <p>Guava {@code RateLimiter.acquire()} uses pay-forward semantics: each
     * call waits for the previously-reserved ticket, not for its own permits.
     * So a single {@code acquire(N)} can return instantly even on a starved
     * limiter. The reliable way to observe the rate is a loop of sequential
     * {@code acquire(1)} calls. With N iterations at {@code rate} permits/s,
     * the total elapsed time converges to {@code (N-1) / rate} seconds
     * regardless of starting burst, because the first call's wait time is
     * absorbed into the loop-start fixed cost.
     *
     * <p>Bounds are asymmetric and generous (roughly an order of magnitude
     * apart) so scheduler jitter or a cold JIT cannot collapse them.
     */
    @Test
    void liveRateChangeTakesEffectImmediately() throws Exception {
        // Clamp to 2 permits/s via the admin API.
        postJson("/query/config/query-max-ops", "{\"value\": 2}");
        assertEquals(2.0, runtime.queryRateLimiter().getRate(), 1e-6);

        // 5 sequential acquires at 2 permits/s → ~4 × 500ms = 2000ms elapsed.
        // Lower bound 1500ms leaves ~500ms margin for jitter.
        long t0 = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            runtime.queryRateLimiter().acquire(1);
        }
        long slowNanos = System.nanoTime() - t0;
        assertTrue(slowNanos >= 1_500_000_000L,
                "5 acquires at 2 ops/s should take >= 1.5s, got " + (slowNanos / 1e6) + " ms");

        // Raise the rate to 10000 via the admin API.
        postJson("/query/config/query-max-ops", "{\"value\": 10000}");
        assertEquals(10000.0, runtime.queryRateLimiter().getRate(), 1e-6);

        // Drain any accumulated reservation from the slow period — at 10k
        // permits/s this itself takes at most a few ms.
        runtime.queryRateLimiter().acquire(1);

        // 5 sequential acquires at 10k permits/s → ~0.5ms elapsed. 500ms
        // ceiling is ~1000× the expected value, comfortably above any GC
        // or cache-miss pause on normal CI.
        long t1 = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            runtime.queryRateLimiter().acquire(1);
        }
        long fastNanos = System.nanoTime() - t1;
        assertTrue(fastNanos < 500_000_000L,
                "5 acquires after rate raise should take < 500ms, got " + (fastNanos / 1e6) + " ms");
    }

    /** Mirrors {@link #liveRateChangeTakesEffectImmediately()} for the ingest side. */
    @Test
    void liveIngestRateChangeTakesEffectImmediately() throws Exception {
        postJson("/ingestion/config/ingest-max-ops", "{\"value\": 2}");
        assertEquals(2.0, runtime.ingestRateLimiter().getRate(), 1e-6);

        long t0 = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            runtime.ingestRateLimiter().acquire(1);
        }
        long slowNanos = System.nanoTime() - t0;
        assertTrue(slowNanos >= 1_500_000_000L,
                "5 acquires at 2 ops/s should take >= 1.5s, got " + (slowNanos / 1e6) + " ms");

        postJson("/ingestion/config/ingest-max-ops", "{\"value\": 10000}");
        assertEquals(10000.0, runtime.ingestRateLimiter().getRate(), 1e-6);
        runtime.ingestRateLimiter().acquire(1);

        long t1 = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            runtime.ingestRateLimiter().acquire(1);
        }
        long fastNanos = System.nanoTime() - t1;
        assertTrue(fastNanos < 500_000_000L,
                "5 acquires after rate raise should take < 500ms, got " + (fastNanos / 1e6) + " ms");
    }

    /**
     * Critical non-flaky guarantee: after a large pay-forward reservation on
     * a very low rate, raising the rate via admin must NOT leave workers
     * stuck waiting on the old reservation. {@link BenchRuntime} handles
     * this by replacing the {@link com.google.common.util.concurrent.RateLimiter}
     * on every {@code setRate} call, so a worker that re-reads the limiter
     * reference sees a fresh one with no lingering reservation.
     *
     * <p>Scenario: set rate to 2/s, make a big acquire that reserves ~50s
     * into the future. Raise rate to 10000. A new acquire from the swapped
     * limiter must complete in well under 1s — nowhere near the ~50s that a
     * held-over reservation would cause.
     */
    @Test
    void rateRaiseUnblocksFutureAcquiresDespiteBigPayForwardReservation() throws Exception {
        postJson("/query/config/query-max-ops", "{\"value\": 2}");
        com.google.common.util.concurrent.RateLimiter slow = runtime.queryRateLimiter();

        // First acquire returns fast (no prior reservation) but reserves
        // 100 permits * 500ms = 50 seconds into the future for the NEXT caller.
        slow.acquire(100);

        // Second acquire on the SAME limiter would now block ~50s — that's
        // the pay-forward penalty. We don't actually call it; we verify the
        // swap on setRate prevents this.

        // Raise rate via admin. BenchRuntime swaps the limiter.
        postJson("/query/config/query-max-ops", "{\"value\": 10000}");
        com.google.common.util.concurrent.RateLimiter fast = runtime.queryRateLimiter();

        // Different instance: proves the swap happened.
        assertTrue(slow != fast, "rate change must swap the RateLimiter instance, not mutate in place");

        // New acquires from the swapped limiter must NOT inherit the 50s
        // reservation. Allow a 500ms ceiling — vastly under the 50s that
        // would fire if the reservation had carried over.
        long t0 = System.nanoTime();
        fast.acquire(1);
        fast.acquire(1);
        long elapsedNanos = System.nanoTime() - t0;
        assertTrue(elapsedNanos < 500_000_000L,
                "new limiter should not inherit old reservation; took " + (elapsedNanos / 1e6) + " ms");
    }

    @Test
    void supplierWithConcurrentMapInStatusIsSerializedUnchanged() throws Exception {
        // Guard: LinkedHashMap iteration order must be preserved through
        // Jackson — tests ordering stability of /status payloads.
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        m.put("phase", "query");
        m.put("queries_done", 5L);
        m.put("extra", Collections.singletonMap("nested", "value"));
        runtime.setStatusSupplier(() -> m);
        HttpResponse<String> resp = get("/status");
        assertEquals(200, resp.statusCode());
        // Ordering check: "phase" key appears before "queries_done" in the raw JSON.
        int phaseIdx = resp.body().indexOf("\"phase\"");
        int queriesIdx = resp.body().indexOf("\"queries_done\"");
        assertTrue(phaseIdx >= 0 && queriesIdx > phaseIdx,
                "expected phase before queries_done in " + resp.body());
    }
}
