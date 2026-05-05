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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #402, acceptance criterion 2: a rate change via
 * {@code POST /ingestion/config/ingest-max-ops} must take effect within one
 * commit cycle for <em>all</em> worker threads, including those currently
 * sleeping on the rate limiter.
 *
 * <p>Test scenario: a worker calls {@code group.acquire(BIG_PERMITS)} on a
 * limiter configured at a very low rate (long park). An admin POST raises
 * the rate; the worker must complete its acquire promptly thereafter, not
 * after the original sleep computed at the old rate would have ended.
 */
class AdminApiRateChangeWakesSleepingWorkersTest {

    private BenchRuntime runtime;
    private AdminApiServer server;
    private HttpClient client;
    private String baseUrl;

    @BeforeEach
    void start() throws Exception {
        Config cfg = new Config();
        cfg.ingestThreads = 1; // one slot in the rate-limiter group
        cfg.batchSize = 1; // make the safety invariant easy to satisfy
        cfg.transactionSize = 0;
        cfg.ingestMaxOpsPerSecond = 1; // 1 permit/s — every permit is 1 s of park time
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

    @Test
    void postIngestMaxOpsWakesWorkerSleepingOnLowRate() throws Exception {
        IngestRateLimiterGroup group = runtime.ingestRateLimiterGroup();
        // Drain initial slot.
        group.acquire(0, 1);

        // Spawn worker that will park for ~10 s on a 10-permit acquire at 1 permit/s.
        AtomicLong elapsedNanos = new AtomicLong(-1);
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch workerDone = new CountDownLatch(1);
        Thread worker = new Thread(() -> {
            group.attachThread(0, Thread.currentThread());
            workerStarted.countDown();
            long t0 = System.nanoTime();
            try {
                group.acquire(0, 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                workerDone.countDown();
                return;
            }
            elapsedNanos.set(System.nanoTime() - t0);
            workerDone.countDown();
        });
        worker.start();

        assertTrue(workerStarted.await(2, TimeUnit.SECONDS), "worker should have started");

        // Wait until the worker is actually parked inside acquire() before
        // issuing the admin POST. Polling Thread.getState() rather than
        // Thread.sleep() avoids the race where the POST lands before the
        // worker has entered parkNanos and the unpark would be queued for a
        // subsequent (immediate) park rather than waking the current one.
        long deadline = System.currentTimeMillis() + 5000;
        while (worker.getState() != Thread.State.TIMED_WAITING
                && worker.getState() != Thread.State.WAITING
                && System.currentTimeMillis() < deadline) {
            Thread.yield();
        }
        assertTrue(worker.getState() == Thread.State.TIMED_WAITING
                        || worker.getState() == Thread.State.WAITING,
                "worker should have entered parked state; was " + worker.getState());

        // POST a high rate. The worker must wake within ~1.5 s.
        long t0 = System.nanoTime();
        HttpResponse<String> resp = postJson("/ingestion/config/ingest-max-ops",
                "{\"value\": 100000}");
        assertEquals(200, resp.statusCode());

        assertTrue(workerDone.await(3, TimeUnit.SECONDS),
                "worker should have woken within 3 s after rate raise");
        long elapsedSinceRaise = System.nanoTime() - t0;
        assertTrue(elapsedSinceRaise < 1_500_000_000L,
                "worker should wake within 1.5s of POST (way under the 10s old-rate park); "
                        + "took " + (elapsedSinceRaise / 1e6) + " ms");

        worker.join(2_000);
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
