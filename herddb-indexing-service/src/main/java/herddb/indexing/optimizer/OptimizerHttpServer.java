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
 * Minimal HTTP endpoint for observability of the optimizer service (review
 * items E1 + E3). Exposes:
 * <ul>
 *   <li>{@code GET /health} — returns 200 with {@code OK} once the engine is
 *       running. Used by Helm liveness/readiness probes.</li>
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
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] body = "OK\n".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
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

            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
