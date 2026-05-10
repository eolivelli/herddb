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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Minimal HTTP endpoint for observability of the optimizer service. Exposes:
 * <ul>
 *   <li>{@code GET /health} — always returns 200 OK. Used by Helm
 *       liveness/readiness probes; the probe must only fire when the JVM
 *       itself is unresponsive, not when a long merge tick is running
 *       (issue #504). Engine progress is tracked via the
 *       {@code herddb_optimizer_last_run_at_seconds} gauge on /metrics
 *       for alerting.</li>
 *   <li>{@code GET /metrics} — returns Prometheus-style plain-text counters
 *       (runs, segments_merged, segments_deprecated, segments_deleted,
 *       ticks_skipped_not_leader). Scrape-friendly.</li>
 * </ul>
 *
 * <p>Built on the JDK's {@link HttpServer} so the optimizer doesn't drag in
 * Jetty. The endpoint is single-threaded — these probes run at most a few
 * times per second.
 */
public final class OptimizerHttpServer implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(OptimizerHttpServer.class.getName());

    private final HttpServer server;
    private final IndexOptimizerEngine engine;

    public OptimizerHttpServer(String bindHost, int port, IndexOptimizerEngine engine) throws IOException {
        this.engine = engine;
        this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        server.createContext("/health", new HealthHandler());
        server.createContext("/metrics", new MetricsHandler());
        // Single-threaded executor — these endpoints are admin-grade, not on the hot path.
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "optimizer-http");
            t.setDaemon(true);
            return t;
        }));
    }

    public void start() {
        server.start();
        LOGGER.log(Level.INFO, "optimizer admin endpoint listening on {0}",
                server.getAddress());
    }

    /** Returns the bound port — useful for tests that bind to port 0. */
    public int getBoundPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        // Stop with a 0-second grace; this is a daemon admin endpoint.
        server.stop(0);
    }

    private static final class HealthHandler implements HttpHandler {
        private static final byte[] OK_BODY = "OK\n".getBytes(StandardCharsets.UTF_8);

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, OK_BODY.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(OK_BODY);
            }
        }
    }

    private final class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder sb = new StringBuilder(256);
            // Prometheus exposition format. HELP/TYPE comments are optional but cheap.
            sb.append("# HELP herddb_optimizer_runs_total Total optimizer ticks attempted.\n");
            sb.append("# TYPE herddb_optimizer_runs_total counter\n");
            sb.append("herddb_optimizer_runs_total ").append(engine.getRuns()).append('\n');

            sb.append("# HELP herddb_optimizer_segments_merged_total Output segments produced.\n");
            sb.append("# TYPE herddb_optimizer_segments_merged_total counter\n");
            sb.append("herddb_optimizer_segments_merged_total ")
                    .append(engine.getSegmentsMerged()).append('\n');

            sb.append("# HELP herddb_optimizer_segments_deprecated_total Inputs marked DEPRECATED.\n");
            sb.append("# TYPE herddb_optimizer_segments_deprecated_total counter\n");
            sb.append("herddb_optimizer_segments_deprecated_total ")
                    .append(engine.getSegmentsDeprecated()).append('\n');

            sb.append("# HELP herddb_optimizer_segments_deleted_total Reaped segments after retention.\n");
            sb.append("# TYPE herddb_optimizer_segments_deleted_total counter\n");
            sb.append("herddb_optimizer_segments_deleted_total ")
                    .append(engine.getSegmentsDeleted()).append('\n');

            sb.append("# HELP herddb_optimizer_ticks_skipped_not_leader_total Ticks short-circuited because we lost the leader lock.\n");
            sb.append("# TYPE herddb_optimizer_ticks_skipped_not_leader_total counter\n");
            sb.append("herddb_optimizer_ticks_skipped_not_leader_total ")
                    .append(engine.getTicksSkippedNotLeader()).append('\n');

            // ----- Compaction failure / abort breakdown -----
            sb.append("# HELP herddb_optimizer_merge_failures_total Merger.merge() invocations that threw.\n");
            sb.append("# TYPE herddb_optimizer_merge_failures_total counter\n");
            sb.append("herddb_optimizer_merge_failures_total ")
                    .append(engine.getMergeFailuresTotal()).append('\n');

            sb.append("# HELP herddb_optimizer_merge_aborts_revalidate_failed_total"
                    + " Merges aborted because input drifted under the optimizer between"
                    + " candidate-pick and the post-merge revalidate.\n");
            sb.append("# TYPE herddb_optimizer_merge_aborts_revalidate_failed_total counter\n");
            sb.append("herddb_optimizer_merge_aborts_revalidate_failed_total ")
                    .append(engine.getMergeAbortsRevalidateFailedTotal()).append('\n');

            sb.append("# HELP herddb_optimizer_merge_declined_total"
                    + " Merger declined a candidate batch (e.g., not enough live entries).\n");
            sb.append("# TYPE herddb_optimizer_merge_declined_total counter\n");
            sb.append("herddb_optimizer_merge_declined_total ")
                    .append(engine.getMergeDeclinedTotal()).append('\n');

            sb.append("# HELP herddb_optimizer_last_merge_duration_ms"
                    + " Wall-clock millis of the last completed merge cycle (-1 if never run).\n");
            sb.append("# TYPE herddb_optimizer_last_merge_duration_ms gauge\n");
            sb.append("herddb_optimizer_last_merge_duration_ms ")
                    .append(engine.getLastMergeDurationMs()).append('\n');

            sb.append("# HELP herddb_optimizer_last_run_at_seconds"
                    + " Unix-time seconds at which the last tick body started (-1 if never run).\n");
            sb.append("# TYPE herddb_optimizer_last_run_at_seconds gauge\n");
            long lastRunMs = engine.getLastRunAtMillis();
            sb.append("herddb_optimizer_last_run_at_seconds ")
                    .append(lastRunMs >= 0 ? lastRunMs / 1000L : -1L).append('\n');

            // ----- Observed segments (last-tick snapshot, by state) -----
            sb.append("# HELP herddb_optimizer_observed_indexes"
                    + " Number of indexes seen in the most recent tick.\n");
            sb.append("# TYPE herddb_optimizer_observed_indexes gauge\n");
            sb.append("herddb_optimizer_observed_indexes ")
                    .append(engine.getObservedIndexes()).append('\n');

            sb.append("# HELP herddb_optimizer_observed_segments"
                    + " Per-state segment count from the most recent tick across all"
                    + " observed indexes. Labels: state in {active, deprecated, transferring,"
                    + " provisional}.\n");
            sb.append("# TYPE herddb_optimizer_observed_segments gauge\n");
            sb.append("herddb_optimizer_observed_segments{state=\"active\"} ")
                    .append(engine.getObservedActiveSegments()).append('\n');
            sb.append("herddb_optimizer_observed_segments{state=\"deprecated\"} ")
                    .append(engine.getObservedDeprecatedSegments()).append('\n');
            sb.append("herddb_optimizer_observed_segments{state=\"transferring\"} ")
                    .append(engine.getObservedTransferringSegments()).append('\n');
            sb.append("herddb_optimizer_observed_segments{state=\"provisional\"} ")
                    .append(engine.getObservedProvisionalSegments()).append('\n');

            // ----- Segment relocations (ownership transfers driven by the optimizer) -----
            // Counters are wired but stay at 0 until the production relocate-trigger
            // path is enabled — the panel in Grafana is plumbed now so it lights up
            // automatically when the wiring lands.
            sb.append("# HELP herddb_optimizer_relocations_initiated_total"
                    + " Ownership-transfer initiate (ACTIVE -> TRANSFERRING) CAS calls"
                    + " issued by the optimizer.\n");
            sb.append("# TYPE herddb_optimizer_relocations_initiated_total counter\n");
            sb.append("herddb_optimizer_relocations_initiated_total ")
                    .append(engine.getRelocationsInitiatedTotal()).append('\n');

            sb.append("# HELP herddb_optimizer_relocations_completed_total"
                    + " Ownership-transfer complete (TRANSFERRING -> ACTIVE) CAS calls"
                    + " observed at the registry.\n");
            sb.append("# TYPE herddb_optimizer_relocations_completed_total counter\n");
            sb.append("herddb_optimizer_relocations_completed_total ")
                    .append(engine.getRelocationsCompletedTotal()).append('\n');

            sb.append("# HELP herddb_optimizer_relocations_aborted_total"
                    + " Transfer attempts that aborted before {@code complete()} fired"
                    + " (revalidate failure, takeover-side timeout, optimizer crash).\n");
            sb.append("# TYPE herddb_optimizer_relocations_aborted_total counter\n");
            sb.append("herddb_optimizer_relocations_aborted_total ")
                    .append(engine.getRelocationsAbortedTotal()).append('\n');

            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
