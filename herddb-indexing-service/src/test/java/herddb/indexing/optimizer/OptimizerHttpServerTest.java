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
}
