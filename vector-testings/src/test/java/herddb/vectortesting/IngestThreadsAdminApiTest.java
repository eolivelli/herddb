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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the ingest-threads and query-threads admin API endpoints.
 *
 * <p>Each test starts an {@link AdminApiServer} on an ephemeral port, exercises
 * the four new endpoints ({@code GET} and {@code POST} for both
 * {@code /ingestion/config/ingest-threads} and {@code /query/config/query-threads}),
 * then stops the server. No shared state, no fixed ports, no sleeps.
 */
class IngestThreadsAdminApiTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BenchRuntime runtime;
    private AdminApiServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void start() throws Exception {
        Config cfg = new Config();
        cfg.ingestThreads = 4;
        cfg.queryThreads = 4;
        cfg.ingestMaxOpsPerSecond = 100_000;
        cfg.queryMaxOpsPerSecond = 10;
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

    // ---- GET /ingestion/config/ingest-threads ----

    @Test
    void getIngestThreadsReturnsCurrentValue() throws Exception {
        HttpResponse<String> resp = get("/ingestion/config/ingest-threads");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(4, json.get("ingest-threads").asInt());
    }

    @Test
    void getIngestThreadsReflectsConfigUpdate() throws Exception {
        runtime.config().ingestThreads = 8;
        HttpResponse<String> resp = get("/ingestion/config/ingest-threads");
        assertEquals(200, resp.statusCode());
        assertEquals(8, MAPPER.readTree(resp.body()).get("ingest-threads").asInt());
    }

    // ---- POST /ingestion/config/ingest-threads ----

    @Test
    void postIngestThreadsUpdatesConfig() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "{\"value\": 8}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        // Response body is the full ingestion config (same shape as GET /ingestion/config).
        assertEquals(8, json.get("ingest-threads").asInt());
        assertEquals(8, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsRejectsZero() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "{\"value\": 0}");
        assertEquals(400, resp.statusCode());
        // Config must be unchanged.
        assertEquals(4, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "{\"value\": -2}");
        assertEquals(400, resp.statusCode());
        assertEquals(4, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsRejectsAboveMax() throws Exception {
        HttpResponse<String> resp = postJson(
                "/ingestion/config/ingest-threads",
                "{\"value\": " + (BenchRuntime.MAX_INGEST_THREADS + 1) + "}");
        assertEquals(400, resp.statusCode());
        assertEquals(4, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsAcceptsMaxBoundary() throws Exception {
        HttpResponse<String> resp = postJson(
                "/ingestion/config/ingest-threads",
                "{\"value\": " + BenchRuntime.MAX_INGEST_THREADS + "}");
        assertEquals(200, resp.statusCode());
        assertEquals(BenchRuntime.MAX_INGEST_THREADS, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsAcceptsOne() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "{\"value\": 1}");
        assertEquals(200, resp.statusCode());
        assertEquals(1, runtime.config().ingestThreads);
    }

    @Test
    void postIngestThreadsAcceptsPlainNumericBody() throws Exception {
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "6");
        assertEquals(200, resp.statusCode());
        assertEquals(6, runtime.config().ingestThreads);
    }

    // ---- GET /query/config/query-threads ----

    @Test
    void getQueryThreadsReturnsCurrentValue() throws Exception {
        HttpResponse<String> resp = get("/query/config/query-threads");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(4, json.get("query-threads").asInt());
    }

    // ---- POST /query/config/query-threads ----

    @Test
    void postQueryThreadsUpdatesConfig() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-threads", "{\"value\": 6}");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(6, json.get("query-threads").asInt());
        assertEquals(6, runtime.config().queryThreads);
    }

    @Test
    void postQueryThreadsRejectsZero() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-threads", "{\"value\": 0}");
        assertEquals(400, resp.statusCode());
        assertEquals(4, runtime.config().queryThreads);
    }

    @Test
    void postQueryThreadsRejectsNegative() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-threads", "{\"value\": -1}");
        assertEquals(400, resp.statusCode());
        assertEquals(4, runtime.config().queryThreads);
    }

    @Test
    void postQueryThreadsAcceptsOne() throws Exception {
        HttpResponse<String> resp = postJson("/query/config/query-threads", "{\"value\": 1}");
        assertEquals(200, resp.statusCode());
        assertEquals(1, runtime.config().queryThreads);
    }

    // ---- GET /ingestion/config reflects the thread count ----

    @Test
    void getIngestionConfigIncludesIngestThreads() throws Exception {
        HttpResponse<String> resp = get("/ingestion/config");
        assertEquals(200, resp.statusCode());
        JsonNode json = MAPPER.readTree(resp.body());
        assertEquals(4, json.get("ingest-threads").asInt());
    }

    @Test
    void postIngestThreadsReflectedInGetIngestionConfig() throws Exception {
        postJson("/ingestion/config/ingest-threads", "{\"value\": 7}");
        HttpResponse<String> resp = get("/ingestion/config");
        assertEquals(200, resp.statusCode());
        assertEquals(7, MAPPER.readTree(resp.body()).get("ingest-threads").asInt());
    }

    // ---- Live ingest-context: reduction via poison pills ----

    /**
     * Sets up a minimal live ingest context (a real blocking queue + growable pool +
     * worker factory) and verifies that reducing the thread count via the admin API
     * causes the excess workers to exit promptly via poison pills.
     *
     * <p>Workers are lightweight stubs that block on the queue and exit on a poison
     * pill, mimicking the real {@link IngestionWorker} pattern without needing a
     * real database connection.
     */
    @Test
    void liveReductionSendsEnoughPoisonPillsForExcessWorkers() throws Exception {
        int initialThreads = 4;
        int targetThreads = 2;
        int excess = initialThreads - targetThreads;

        BlockingQueue<float[]> queue = new ArrayBlockingQueue<>(128);
        // Latch released when a worker exits (one per excess worker expected).
        CountDownLatch exitLatch = new CountDownLatch(excess);

        ExecutorService pool = Executors.newFixedThreadPool(initialThreads);
        // Stub workers: block on queue, exit on poison pill, count-down latch.
        Supplier<Runnable> factory = () -> () -> {
            runtime.activeIngestWorkers.incrementAndGet();
            try {
                while (true) {
                    float[] item = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (item != null && item.length == 0) {
                        // poison pill — retire
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                runtime.activeIngestWorkers.decrementAndGet();
                exitLatch.countDown();
            }
        };

        runtime.setIngestContext(queue, pool, factory);
        // Start all workers.
        for (int i = 0; i < initialThreads; i++) {
            pool.submit(factory.get());
        }

        // Wait until all workers are live.
        long deadline = System.currentTimeMillis() + 5000;
        while (runtime.activeIngestWorkers.get() < initialThreads
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(initialThreads, runtime.activeIngestWorkers.get(),
                "all " + initialThreads + " workers should be live before reduction");

        // Reduce via admin API.
        HttpResponse<String> resp = postJson(
                "/ingestion/config/ingest-threads", "{\"value\": " + targetThreads + "}");
        assertEquals(200, resp.statusCode());
        assertEquals(targetThreads, runtime.config().ingestThreads);

        // Excess workers should exit after receiving their poison pills.
        boolean exited = exitLatch.await(5, TimeUnit.SECONDS);
        assertTrue(exited, "excess workers should have exited within 5s after poison pill injection");

        pool.shutdownNow();
        pool.awaitTermination(2, TimeUnit.SECONDS);
    }

    /**
     * Verifies that increasing the thread count via the admin API causes new
     * workers to be submitted to the pool by checking {@code activeIngestWorkers}
     * grows to the new target.
     */
    @Test
    void liveIncreaseSpawnsNewWorkers() throws Exception {
        int initialThreads = 2;
        int targetThreads = 4;
        int extra = targetThreads - initialThreads;

        BlockingQueue<float[]> queue = new ArrayBlockingQueue<>(128);
        // A latch to know when newly spawned workers are live.
        CountDownLatch spawnLatch = new CountDownLatch(extra);

        ExecutorService pool = new ThreadPoolExecutor(
                initialThreads, BenchRuntime.MAX_INGEST_THREADS,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>());

        AtomicBoolean stopAll = new AtomicBoolean(false);
        Supplier<Runnable> factory = () -> () -> {
            runtime.activeIngestWorkers.incrementAndGet();
            spawnLatch.countDown();
            try {
                while (!stopAll.get()) {
                    queue.poll(100, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                runtime.activeIngestWorkers.decrementAndGet();
            }
        };

        runtime.setIngestContext(queue, pool, factory);
        // Start initial workers.
        for (int i = 0; i < initialThreads; i++) {
            pool.submit(factory.get());
        }

        // Wait for initial workers.
        long deadline = System.currentTimeMillis() + 5000;
        while (runtime.activeIngestWorkers.get() < initialThreads
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(initialThreads, runtime.activeIngestWorkers.get());

        // Increase via admin API.
        HttpResponse<String> resp = postJson(
                "/ingestion/config/ingest-threads", "{\"value\": " + targetThreads + "}");
        assertEquals(200, resp.statusCode());
        assertEquals(targetThreads, runtime.config().ingestThreads);

        // New workers should appear.
        boolean spawned = spawnLatch.await(5, TimeUnit.SECONDS);
        assertTrue(spawned, "new workers should have been spawned within 5s");

        assertEquals(targetThreads, runtime.activeIngestWorkers.get(),
                "activeIngestWorkers should equal target after increase");

        stopAll.set(true);
        pool.shutdownNow();
        pool.awaitTermination(2, TimeUnit.SECONDS);
    }

    // ---- setIngestThreads without live context (no-op for pills/spawn) ----

    @Test
    void setIngestThreadsWithoutContextStillUpdatesConfig() throws Exception {
        // No setIngestContext — queue/pool/factory are null.
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-threads", "{\"value\": 3}");
        assertEquals(200, resp.statusCode());
        assertEquals(3, runtime.config().ingestThreads);
    }

    // ---- BenchRuntime unit-level validation ----

    @Test
    void benchRuntimeSetIngestThreadsRejectsZero() {
        try {
            runtime.setIngestThreads(0);
        } catch (IllegalArgumentException e) {
            assertEquals(4, runtime.config().ingestThreads); // unchanged
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        throw new AssertionError("expected IllegalArgumentException");
    }

    @Test
    void benchRuntimeSetQueryThreadsRejectsZero() {
        try {
            runtime.setQueryThreads(0);
        } catch (IllegalArgumentException e) {
            assertEquals(4, runtime.config().queryThreads); // unchanged
            return;
        }
        throw new AssertionError("expected IllegalArgumentException");
    }

    @Test
    void benchRuntimeSetIngestThreadsUpdatesTargetField() throws Exception {
        runtime.setIngestThreads(7);
        assertEquals(7, runtime.getTargetIngestThreads());
        assertEquals(7, runtime.config().ingestThreads);
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
