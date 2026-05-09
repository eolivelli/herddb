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
package herddb.indexing.optimizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.indexing.segment.SegmentRegistryClient;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Smoke test for the optimizer's admin HTTP endpoint (review item E1+E3).
 * Brings up an {@link OptimizerHttpServer} bound to an ephemeral port,
 * exercises the engine's counters, and asserts {@code /health} and
 * {@code /metrics} return the expected payload.
 */
public class OptimizerHttpServerTest {

    private static final String BASE_PATH = "/herd-test-E1";
    private static final String TS_UUID = "tsuid";

    private TestingServer zkServer;
    private ZooKeeper zk;
    private SegmentRegistryClient registry;
    private IndexOptimizerEngine engine;
    private OptimizerHttpServer http;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer(true);
        CountDownLatch connected = new CountDownLatch(1);
        zk = new ZooKeeper(zkServer.getConnectString(), 30000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        assertTrue(connected.await(30, TimeUnit.SECONDS));
        zk.create(BASE_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        registry = new SegmentRegistryClient(() -> zk, BASE_PATH);
        registry.ensureRoot();

        engine = new IndexOptimizerEngine(registry, new InMemorySegmentMerger(), TS_UUID,
                new MergePolicy.SmallestFirstPolicy(99, 99, Long.MAX_VALUE, Long.MAX_VALUE),
                60_000L, () -> 0);

        // Bind to port 0 so the OS picks a free port; we read it back via getBoundPort.
        http = new OptimizerHttpServer("127.0.0.1", 0, engine);
        http.start();
    }

    @After
    public void tearDown() throws Exception {
        if (http != null) {
            http.close();
        }
        if (zk != null) {
            zk.close();
        }
        if (zkServer != null) {
            zkServer.close();
        }
    }

    private String fetch(String path) throws Exception {
        URL url = new URL("http://127.0.0.1:" + http.getBoundPort() + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        assertEquals(200, conn.getResponseCode());
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

    @Test
    public void healthEndpointReturnsOk() throws Exception {
        String body = fetch("/health");
        assertEquals("OK\n", body);
    }

    @Test
    public void metricsEndpointExposesEngineCounters() throws Exception {
        // Run the engine once so counters move.
        engine.runOnce();

        String body = fetch("/metrics");
        assertTrue("metrics must include runs_total: " + body,
                body.contains("herddb_optimizer_runs_total 1"));
        assertTrue("metrics must include segments_merged_total",
                body.contains("herddb_optimizer_segments_merged_total"));
        assertTrue("metrics must include segments_deprecated_total",
                body.contains("herddb_optimizer_segments_deprecated_total"));
        assertTrue("metrics must include segments_deleted_total",
                body.contains("herddb_optimizer_segments_deleted_total"));
        assertTrue("metrics must include ticks_skipped_not_leader_total",
                body.contains("herddb_optimizer_ticks_skipped_not_leader_total"));
        // Prometheus exposition format requires HELP/TYPE comments; a Prometheus
        // server will reject the page if they're malformed.
        assertTrue("metrics must include HELP comments", body.contains("# HELP "));
        assertTrue("metrics must include TYPE comments", body.contains("# TYPE "));
    }

    @Test
    public void portZeroSelectsFreePort() {
        assertTrue("bound port should be > 0", http.getBoundPort() > 0);
    }

    @Test
    public void healthReturns503WhenEngineHasNotTicked() throws Exception {
        // Review-item B6: bring up a separate server with an aggressive 100ms staleness
        // threshold + a manual clock. Run the engine once to seed the heartbeat, then
        // advance the clock past the threshold WITHOUT ticking again — the next /health
        // call must return 503.
        java.util.concurrent.atomic.AtomicLong fakeClock =
                new java.util.concurrent.atomic.AtomicLong(0L);
        OptimizerHttpServer livenessHttp = new OptimizerHttpServer("127.0.0.1", 0, engine,
                /* stalenessThresholdMillis */ 100L, fakeClock::get);
        try {
            livenessHttp.start();
            // First call seeds the heartbeat.
            engine.runOnce();
            int code = headStatus("http://127.0.0.1:" + livenessHttp.getBoundPort() + "/health");
            assertEquals(200, code);

            // Advance the fake clock past the threshold without further ticks.
            fakeClock.addAndGet(500L);
            int code2 = headStatus("http://127.0.0.1:" + livenessHttp.getBoundPort() + "/health");
            assertEquals("/health must return 503 when engine is stale", 503, code2);

            // Tick the engine again, then advance the fake clock past the staleness
            // threshold AGAIN. With the engine's run counter strictly higher than the
            // last observation, the heartbeat-refresh branch (currentRuns > observedRuns)
            // must reset the timer and return 200. Without the new tick the call
            // would return 503 — which means a passing assertion here exclusively
            // validates the heartbeat-refresh branch (review-pass-3 P3-2).
            engine.runOnce();
            fakeClock.addAndGet(700L);
            int code3 = headStatus("http://127.0.0.1:" + livenessHttp.getBoundPort() + "/health");
            assertEquals("after a fresh tick, /health must refresh and return 200 even"
                    + " though the absolute clock is past the threshold",
                    200, code3);
        } finally {
            livenessHttp.close();
        }
    }

    @Test
    public void healthReturns503WhenCriticalFailureFlagSet() throws Exception {
        // Issue #484 (review item B.3 from the first pr-reviewer pass): the
        // /health endpoint must consult an injected critical-failure
        // predicate so a ZK session expiry can trip Helm's liveness probe
        // even if the engine is still ticking on the periodic safety-net.
        java.util.concurrent.atomic.AtomicBoolean criticalFailure =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        OptimizerHttpServer http2 = new OptimizerHttpServer("127.0.0.1", 0, engine,
                /* stalenessThresholdMillis */ Long.MAX_VALUE,
                System::currentTimeMillis,
                criticalFailure::get);
        try {
            http2.start();
            // Pre-condition: engine OK, no critical failure → 200.
            engine.runOnce();
            int ok = headStatus("http://127.0.0.1:" + http2.getBoundPort() + "/health");
            assertEquals(200, ok);

            // Trip the critical-failure flag (simulating ZK session expiry).
            criticalFailure.set(true);
            int failed = headStatus("http://127.0.0.1:" + http2.getBoundPort() + "/health");
            assertEquals("/health must return 503 when critical-failure flag is set",
                    503, failed);

            // Clearing the flag returns the endpoint to 200.
            criticalFailure.set(false);
            int recovered = headStatus("http://127.0.0.1:" + http2.getBoundPort() + "/health");
            assertEquals("/health must recover to 200 once the flag clears",
                    200, recovered);
        } finally {
            http2.close();
        }
    }

    private static int headStatus(String url) throws Exception {
        java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                new java.net.URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        conn.connect();
        int code = conn.getResponseCode();
        try {
            conn.getInputStream().close();
        } catch (Exception ignored) {
            // 503 path produces an error stream; drain it.
            try {
                if (conn.getErrorStream() != null) {
                    conn.getErrorStream().close();
                }
            } catch (Exception ignored2) {
            }
        }
        return code;
    }
}
