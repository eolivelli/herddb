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
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentState;
import herddb.log.LogSequenceNumber;
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
    public void healthEndpointAlwaysReturnsOk() throws Exception {
        // Issue #504: /health is a pure liveness signal — it must return 200
        // regardless of engine state. Verify it returns OK before the engine
        // ticks once, then again after several ticks. The previous staleness
        // gate would have flipped to 503 in the second window; the new
        // contract keeps it at 200 so long merges don't trip a kubelet SIGKILL.
        assertEquals("OK\n", fetch("/health"));
        engine.runOnce();
        engine.runOnce();
        assertEquals("OK\n", fetch("/health"));
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

    // -------------------------------------------------------------------------
    // Issue #503: /status endpoint
    // -------------------------------------------------------------------------

    @Test
    public void statusEndpointReturnsJsonWhenIdle() throws Exception {
        String body = fetch("/status");
        // When idle, phase should be "idle" and key fields should be present.
        assertTrue("/status must return JSON with phase field: " + body,
                body.contains("\"phase\""));
        assertTrue("/status must contain 'idle' when no merge running: " + body,
                body.contains("\"idle\""));
        assertTrue("/status must include input_segments: " + body,
                body.contains("\"input_segments\""));
        assertTrue("/status must include elapsed_ms: " + body,
                body.contains("\"elapsed_ms\""));
        assertTrue("/status must include heap_used_bytes: " + body,
                body.contains("\"heap_used_bytes\""));
        assertTrue("/status must include gc_count: " + body,
                body.contains("\"gc_count\""));
    }

    @Test
    public void statusEndpointReturnsApplicationJson() throws Exception {
        URL url = new URL("http://127.0.0.1:" + http.getBoundPort() + "/status");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(2000);
        conn.setReadTimeout(2000);
        assertEquals(200, conn.getResponseCode());
        String contentType = conn.getContentType();
        assertTrue("Content-Type must be application/json: " + contentType,
                contentType != null && contentType.startsWith("application/json"));
        conn.disconnect();
    }

    // -------------------------------------------------------------------------
    // Issue #503: /merge-history endpoint
    // -------------------------------------------------------------------------

    @Test
    public void mergeHistoryEndpointReturnsEmptyArrayInitially() throws Exception {
        String body = fetch("/merge-history");
        assertTrue("/merge-history must return a JSON array: " + body,
                body.trim().startsWith("[") && body.trim().endsWith("]"));
        // Engine hasn't run yet — history should be empty.
        assertTrue("/merge-history must be empty before any merge: " + body,
                body.trim().equals("[]") || body.trim().equals("[\n]"));
    }

    @Test
    public void mergeHistoryEndpointContainsDeclinedEntryAfterNoopMerge() throws Exception {
        // Register two segments so the policy fires, then let InMemorySegmentMerger
        // decline by returning null.
        SegmentMetadata seg1 = SegmentMetadata.builder()
                .segmentUuid("http-hist-seg-a").tablespaceUuid(TS_UUID).tableName("docs")
                .indexUuid("http-hist-idx").indexName("docs_v1")
                .state(SegmentState.ACTIVE).ownerInstanceId(0)
                .graphPath("g/a").mapPath("m/a")
                .baseLsn(new LogSequenceNumber(1L, 1L))
                .sizeBytes(100L).vectorCount(5L).generation(1L).createdAtEpochMillis(0L)
                .build();
        SegmentMetadata seg2 = seg1.toBuilder()
                .segmentUuid("http-hist-seg-b")
                .graphPath("g/b").mapPath("m/b")
                .build();
        registry.createSegment(seg1);
        registry.createSegment(seg2);

        InMemorySegmentMerger merger = new InMemorySegmentMerger();
        merger.setReturnNull(true);   // declined merge

        IndexOptimizerEngine histEngine = new IndexOptimizerEngine(registry, merger,
                TS_UUID,
                new MergePolicy.SmallestFirstPolicy(2, 1000, 0L, Long.MAX_VALUE),
                60_000L, () -> 0);
        OptimizerHttpServer histHttp = new OptimizerHttpServer("127.0.0.1", 0, histEngine);
        histHttp.start();
        try {
            histEngine.runOnce();

            String body = fetch(histHttp, "/merge-history");
            // Basic presence checks
            assertTrue("/merge-history must not be empty after a run: " + body,
                    body.contains("\"outcome\""));
            assertTrue("outcome must be 'declined': " + body,
                    body.contains("\"declined\""));
            assertTrue("/merge-history must include merge_id: " + body,
                    body.contains("\"merge_id\""));
            assertTrue("/merge-history must include input_segments: " + body,
                    body.contains("\"input_segments\""));
            assertTrue("/merge-history must include started_at_ms: " + body,
                    body.contains("\"started_at_ms\""));
            // All per-phase timing fields must be present (zero for declined, but present)
            assertTrue("/merge-history must include finished_at_ms: " + body,
                    body.contains("\"finished_at_ms\""));
            assertTrue("/merge-history must include total_ms: " + body,
                    body.contains("\"total_ms\""));
            assertTrue("/merge-history must include output_vectors: " + body,
                    body.contains("\"output_vectors\""));
            assertTrue("/merge-history must include download_ms: " + body,
                    body.contains("\"download_ms\""));
            assertTrue("/merge-history must include pq_training_ms: " + body,
                    body.contains("\"pq_training_ms\""));
            assertTrue("/merge-history must include compaction_ms: " + body,
                    body.contains("\"compaction_ms\""));
            assertTrue("/merge-history must include upload_ms: " + body,
                    body.contains("\"upload_ms\""));
            assertTrue("/merge-history must include zk_register_ms: " + body,
                    body.contains("\"zk_register_ms\""));
            assertTrue("/merge-history must include deprecate_ms: " + body,
                    body.contains("\"deprecate_ms\""));
            assertTrue("/merge-history must include peak_heap_used_bytes: " + body,
                    body.contains("\"peak_heap_used_bytes\""));
            assertTrue("/merge-history must include peak_direct_used_bytes: " + body,
                    body.contains("\"peak_direct_used_bytes\""));
        } finally {
            histHttp.close();
        }
    }

    // -------------------------------------------------------------------------
    // Issue #503: /metrics extended fields
    // -------------------------------------------------------------------------

    @Test
    public void metricsEndpointIncludesPhaseTimingAndMemoryGauges() throws Exception {
        engine.runOnce();
        String body = fetch("/metrics");

        // Per-phase timing counters
        assertTrue("phase_ms_total download expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"download\"}"));
        assertTrue("phase_ms_total compaction expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"compaction\"}"));
        assertTrue("phase_ms_total pq_training expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"pq_training\"}"));
        assertTrue("phase_ms_total upload expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"upload\"}"));
        assertTrue("phase_ms_total zk_register expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"zk_register\"}"));
        assertTrue("phase_ms_total deprecate expected: " + body,
                body.contains("herddb_optimizer_phase_ms_total{phase=\"deprecate\"}"));

        // Current phase gauges
        assertTrue("current_phase idle expected: " + body,
                body.contains("herddb_optimizer_current_phase{phase=\"idle\"} 1"));

        // JVM heap / direct memory
        assertTrue("heap_used_bytes expected: " + body,
                body.contains("herddb_jvm_heap_used_bytes"));
        assertTrue("heap_max_bytes expected: " + body,
                body.contains("herddb_jvm_heap_max_bytes"));
        assertTrue("netty_used_direct_memory_bytes expected: " + body,
                body.contains("netty_used_direct_memory_bytes"));
        assertTrue("netty_max_direct_memory_bytes expected: " + body,
                body.contains("netty_max_direct_memory_bytes"));

        // GC metrics
        assertTrue("gc_count_total expected: " + body,
                body.contains("herddb_jvm_gc_count_total"));
        assertTrue("gc_time_ms_total expected: " + body,
                body.contains("herddb_jvm_gc_time_ms_total"));

        // Current merge progress gauges
        assertTrue("merge_input_segments expected: " + body,
                body.contains("herddb_optimizer_merge_input_segments"));
        assertTrue("merge_elapsed_ms expected: " + body,
                body.contains("herddb_optimizer_merge_elapsed_ms"));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String fetch(OptimizerHttpServer target, String path) throws Exception {
        URL url = new URL("http://127.0.0.1:" + target.getBoundPort() + path);
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
}
