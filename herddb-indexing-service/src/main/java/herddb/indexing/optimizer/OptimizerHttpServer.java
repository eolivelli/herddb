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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 *   <li>{@code GET /metrics} — returns Prometheus-style plain-text counters.
 *       Includes per-phase timing totals, current merge progress gauges,
 *       JVM heap/direct-memory gauges, and tmp-disk gauges (issue #503).</li>
 *   <li>{@code GET /status} — returns a JSON snapshot of the live merge
 *       progress (phase, vectors written, ETA, JVM memory) (issue #503).</li>
 *   <li>{@code GET /merge-history} — returns a JSON array of the last 20
 *       completed merge
 *       records with per-phase timings and outcome (issue #503).</li>
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
    /** Single-threaded executor backing the JDK HttpServer; shut down in {@link #close()}. */
    private final ExecutorService executor;
    /** Optional: when non-null, disk-free gauges are computed for this path. */
    private final Path tmpDirectory;

    /**
     * Backward-compatible constructor that does not emit tmp-disk metrics.
     * Use {@link #OptimizerHttpServer(String, int, IndexOptimizerEngine, Path)}
     * to enable tmp-disk gauges.
     */
    public OptimizerHttpServer(String bindHost, int port, IndexOptimizerEngine engine)
            throws IOException {
        this(bindHost, port, engine, null);
    }

    /**
     * Full constructor.
     *
     * @param tmpDirectory  when non-null, the {@code herddb_optimizer_tmp_disk_*}
     *                      gauges in {@code /metrics} report the free / total bytes
     *                      of the file-store backing this directory. Pass
     *                      {@code null} to omit those metrics (e.g. in unit tests
     *                      that don't set up a real tmp dir).
     */
    public OptimizerHttpServer(String bindHost, int port,
                               IndexOptimizerEngine engine,
                               Path tmpDirectory) throws IOException {
        this.engine = engine;
        this.tmpDirectory = tmpDirectory;
        this.server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        server.createContext("/health", new HealthHandler());
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/status", new StatusHandler());
        server.createContext("/merge-history", new MergeHistoryHandler());
        // Single-threaded executor — these endpoints are admin-grade, not on the hot path.
        // Kept in a field so close() can shut it down cleanly.
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "optimizer-http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
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
        // Stop the server with a 0-second grace; this is a daemon admin endpoint.
        server.stop(0);
        // Shut down the executor so the HTTP thread exits cleanly.
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "optimizer-http executor did not terminate within 5s");
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    // -------------------------------------------------------------------------
    // /health
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // /metrics (Prometheus text format)
    // -------------------------------------------------------------------------

    private final class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            StringBuilder sb = new StringBuilder(1024);

            // ----- Counters: runs / merges / segments -----
            appendCounter(sb, "herddb_optimizer_runs_total",
                    "Total optimizer ticks attempted.",
                    engine.getRuns());
            appendCounter(sb, "herddb_optimizer_segments_merged_total",
                    "Output segments produced.",
                    engine.getSegmentsMerged());
            appendCounter(sb, "herddb_optimizer_segments_deprecated_total",
                    "Inputs marked DEPRECATED.",
                    engine.getSegmentsDeprecated());
            appendCounter(sb, "herddb_optimizer_segments_deleted_total",
                    "Reaped segments after retention.",
                    engine.getSegmentsDeleted());
            appendCounter(sb, "herddb_optimizer_ticks_skipped_not_leader_total",
                    "Ticks short-circuited because we lost the leader lock.",
                    engine.getTicksSkippedNotLeader());

            // ----- Counters: merge outcomes -----
            appendCounter(sb, "herddb_optimizer_merge_failures_total",
                    "Merger.merge() invocations that threw.",
                    engine.getMergeFailuresTotal());
            appendCounter(sb, "herddb_optimizer_merge_aborts_revalidate_failed_total",
                    "Merges aborted because input drifted under the optimizer"
                            + " between candidate-pick and the post-merge revalidate.",
                    engine.getMergeAbortsRevalidateFailedTotal());
            appendCounter(sb, "herddb_optimizer_merge_declined_total",
                    "Merger declined a candidate batch (e.g. not enough live entries).",
                    engine.getMergeDeclinedTotal());

            // ----- Gauge: last merge / last run -----
            appendGauge(sb, "herddb_optimizer_last_merge_duration_ms",
                    "Wall-clock millis of the last completed merge cycle (-1 if never run).",
                    engine.getLastMergeDurationMs());
            long lastRunMs = engine.getLastRunAtMillis();
            appendGauge(sb, "herddb_optimizer_last_run_at_seconds",
                    "Unix-time seconds at which the last tick body started (-1 if never run).",
                    lastRunMs >= 0 ? lastRunMs / 1000L : -1L);

            // ----- Gauge: observed segment counts -----
            appendGauge(sb, "herddb_optimizer_observed_indexes",
                    "Number of indexes seen in the most recent tick.",
                    engine.getObservedIndexes());
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

            // ----- Counters: ownership relocations -----
            appendCounter(sb, "herddb_optimizer_relocations_initiated_total",
                    "Ownership-transfer initiate (ACTIVE -> TRANSFERRING) CAS calls"
                            + " issued by the optimizer.",
                    engine.getRelocationsInitiatedTotal());
            appendCounter(sb, "herddb_optimizer_relocations_completed_total",
                    "Ownership-transfer complete (TRANSFERRING -> ACTIVE) CAS calls"
                            + " observed at the registry.",
                    engine.getRelocationsCompletedTotal());
            appendCounter(sb, "herddb_optimizer_relocations_aborted_total",
                    "Transfer attempts that aborted before complete() fired"
                            + " (revalidate failure, takeover-side timeout, optimizer crash).",
                    engine.getRelocationsAbortedTotal());

            // ----- Counters: per-phase cumulative timing (issue #503) -----
            sb.append("# HELP herddb_optimizer_phase_ms_total"
                    + " Cumulative time spent in each merge phase (milliseconds)."
                    + " Labels: phase in {download, compaction, pq_training, upload,"
                    + " zk_register, deprecate}.\n");
            sb.append("# TYPE herddb_optimizer_phase_ms_total counter\n");
            sb.append("herddb_optimizer_phase_ms_total{phase=\"download\"} ")
                    .append(engine.getPhaseDownloadMsTotal()).append('\n');
            sb.append("herddb_optimizer_phase_ms_total{phase=\"compaction\"} ")
                    .append(engine.getPhaseCompactionMsTotal()).append('\n');
            sb.append("herddb_optimizer_phase_ms_total{phase=\"pq_training\"} ")
                    .append(engine.getPhasePqTrainingMsTotal()).append('\n');
            sb.append("herddb_optimizer_phase_ms_total{phase=\"upload\"} ")
                    .append(engine.getPhaseUploadMsTotal()).append('\n');
            sb.append("herddb_optimizer_phase_ms_total{phase=\"zk_register\"} ")
                    .append(engine.getPhaseZkRegisterMsTotal()).append('\n');
            sb.append("herddb_optimizer_phase_ms_total{phase=\"deprecate\"} ")
                    .append(engine.getPhaseDeprecateMsTotal()).append('\n');

            // ----- Gauges: current merge progress (issue #503) -----
            MergeProgress.Snapshot snap = engine.getMergeProgress().snapshot();
            sb.append("# HELP herddb_optimizer_current_phase"
                    + " 1 when the optimizer is in the labeled phase; 0 otherwise.\n");
            sb.append("# TYPE herddb_optimizer_current_phase gauge\n");
            for (String phase : new String[]{"idle", "downloading", "pq-training",
                    "compacting", "uploading", "registering-zk", "deprecating-inputs"}) {
                sb.append("herddb_optimizer_current_phase{phase=\"").append(phase).append("\"} ")
                        .append(phase.equals(snap.phase) ? 1 : 0).append('\n');
            }
            appendGauge(sb, "herddb_optimizer_merge_input_segments",
                    "Number of input segments in the current (or last) merge.",
                    snap.inputSegments);
            appendGauge(sb, "herddb_optimizer_merge_input_vectors",
                    "Total input vectors in the current (or last) merge.",
                    snap.inputVectors);
            appendGauge(sb, "herddb_optimizer_merge_batches_written",
                    "Vectors (or batches) written so far in the current phase.",
                    snap.batchesWritten);
            appendGauge(sb, "herddb_optimizer_merge_batches_total",
                    "Total batches for the current phase (0 when unknown).",
                    snap.batchesTotal);
            appendGauge(sb, "herddb_optimizer_merge_elapsed_ms",
                    "Elapsed milliseconds since the current merge started (0 when idle).",
                    snap.elapsedMs);
            appendGauge(sb, "herddb_optimizer_merge_eta_ms",
                    "Estimated remaining milliseconds for the current phase (-1 when unknown).",
                    snap.etaMs);

            // ----- Gauges: JVM heap (issue #503) -----
            appendGauge(sb, "herddb_jvm_heap_used_bytes",
                    "JVM heap memory currently in use (bytes).",
                    snap.heapUsedBytes);
            appendGauge(sb, "herddb_jvm_heap_max_bytes",
                    "JVM heap memory maximum (bytes).",
                    snap.heapMaxBytes);

            // ----- Gauges: JVM direct memory via Netty PlatformDependent (issue #503) -----
            appendGauge(sb, "netty_used_direct_memory_bytes",
                    "Netty direct (off-heap) memory currently allocated (bytes);"
                            + " -1 when Netty defers to JDK accounting.",
                    snap.directUsedBytes);
            appendGauge(sb, "netty_max_direct_memory_bytes",
                    "JVM maximum direct memory permitted (bytes).",
                    snap.directMaxBytes);

            // ----- Gauges: JVM GC (issue #503) -----
            appendCounter(sb, "herddb_jvm_gc_count_total",
                    "Total garbage-collection count across all collectors.",
                    snap.gcCount);
            appendCounter(sb, "herddb_jvm_gc_time_ms_total",
                    "Total garbage-collection wall-clock time across all collectors (milliseconds).",
                    snap.gcTimeMs);

            // ----- Gauges: tmp disk (issue #503) -----
            appendTmpDiskMetrics(sb);

            byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }

        private void appendTmpDiskMetrics(StringBuilder sb) {
            if (tmpDirectory == null) {
                return;
            }
            try {
                FileStore fs = Files.getFileStore(tmpDirectory);
                long free  = fs.getUsableSpace();
                long total = fs.getTotalSpace();
                appendGauge(sb, "herddb_optimizer_tmp_disk_free_bytes",
                        "Usable free bytes on the file-store backing the optimizer's tmp directory.",
                        free);
                appendGauge(sb, "herddb_optimizer_tmp_disk_total_bytes",
                        "Total bytes on the file-store backing the optimizer's tmp directory.",
                        total);
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "could not read tmp dir file-store stats: {0}", e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // /status — JSON live merge state (issue #503)
    // -------------------------------------------------------------------------

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            MergeProgress.Snapshot snap = engine.getMergeProgress().snapshot();
            StringBuilder sb = new StringBuilder(512);
            sb.append("{\n");
            appendJsonStr(sb, "phase", snap.phase, true);
            appendJsonStr(sb, "merge_id", snap.mergeId, true);
            appendJsonNum(sb, "input_segments", snap.inputSegments, true);
            appendJsonNum(sb, "input_vectors", snap.inputVectors, true);
            appendJsonNum(sb, "batches_written", snap.batchesWritten, true);
            appendJsonNum(sb, "batches_total", snap.batchesTotal, true);
            appendJsonDouble(sb, "pct_complete", snap.pctComplete(), true);
            appendJsonNum(sb, "merge_start_ms", snap.mergeStartMs, true);
            appendJsonNum(sb, "elapsed_ms", snap.elapsedMs, true);
            appendJsonNum(sb, "eta_ms", snap.etaMs, true);
            appendJsonNum(sb, "heap_used_bytes", snap.heapUsedBytes, true);
            appendJsonNum(sb, "heap_max_bytes", snap.heapMaxBytes, true);
            appendJsonNum(sb, "direct_used_bytes", snap.directUsedBytes, true);
            appendJsonNum(sb, "direct_max_bytes", snap.directMaxBytes, true);
            appendJsonNum(sb, "gc_count", snap.gcCount, true);
            appendJsonNum(sb, "gc_time_ms", snap.gcTimeMs, false);
            sb.append("}\n");
            sendJson(exchange, sb.toString());
        }
    }

    // -------------------------------------------------------------------------
    // /merge-history — JSON array of completed merge records (issue #503)
    // -------------------------------------------------------------------------

    private final class MergeHistoryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            List<MergeRecord> history = engine.getMergeHistory();
            StringBuilder sb = new StringBuilder(history.size() * 256 + 4);
            sb.append("[\n");
            for (int i = 0; i < history.size(); i++) {
                MergeRecord r = history.get(i);
                sb.append("  {\n");
                appendJsonStr(sb, "merge_id", r.mergeId, true);
                appendJsonStr(sb, "index_uuid", r.indexUuid, true);
                appendJsonNum(sb, "started_at_ms", r.startedAtMs, true);
                appendJsonNum(sb, "finished_at_ms", r.finishedAtMs, true);
                appendJsonNum(sb, "total_ms", r.totalMs(), true);
                appendJsonStr(sb, "outcome", r.outcome, true);
                appendJsonNum(sb, "input_segments", r.inputSegments, true);
                appendJsonNum(sb, "input_vectors", r.inputVectors, true);
                appendJsonNum(sb, "output_vectors", r.outputVectors, true);
                appendJsonNum(sb, "download_ms", r.downloadMs, true);
                appendJsonNum(sb, "pq_training_ms", r.pqTrainingMs, true);
                appendJsonNum(sb, "compaction_ms", r.compactionMs, true);
                appendJsonNum(sb, "upload_ms", r.uploadMs, true);
                appendJsonNum(sb, "zk_register_ms", r.zkRegisterMs, true);
                appendJsonNum(sb, "deprecate_ms", r.deprecateMs, true);
                appendJsonNum(sb, "peak_heap_used_bytes", r.peakHeapUsedBytes, true);
                appendJsonNum(sb, "peak_direct_used_bytes", r.peakDirectUsedBytes, false);
                sb.append("  }");
                if (i < history.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
            sb.append("]\n");
            sendJson(exchange, sb.toString());
        }
    }

    // -------------------------------------------------------------------------
    // JSON / Prometheus helpers
    // -------------------------------------------------------------------------

    private static void appendCounter(StringBuilder sb, String name, String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void appendGauge(StringBuilder sb, String name, String help, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append(' ').append(value).append('\n');
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    /**
     * Appends a JSON string field with proper escaping of the value.
     * Null values are written as {@code null} (no quotes).
     *
     * @param trailingComma whether to append a comma after the value
     */
    private static void appendJsonStr(StringBuilder sb, String key, String value,
                                      boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ");
        if (value == null) {
            sb.append("null");
        } else {
            sb.append('"');
            // Escape JSON special chars — values here are UUIDs / phase names,
            // none of which contain backslash or control chars, but we handle them
            // correctly for robustness.
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"') {
                    sb.append("\\\"");
                } else if (c == '\\') {
                    sb.append("\\\\");
                } else {
                    sb.append(c);
                }
            }
            sb.append('"');
        }
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendJsonNum(StringBuilder sb, String key, long value,
                                      boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ").append(value);
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendJsonDouble(StringBuilder sb, String key, double value,
                                         boolean trailingComma) {
        sb.append("  \"").append(key).append("\": ");
        // Format to 2 decimal places without bringing in a format library.
        long hundredths = Math.round(value * 100);
        sb.append(hundredths / 100).append('.').append(Math.abs(hundredths % 100) / 10)
          .append(Math.abs(hundredths % 10));
        if (trailingComma) {
            sb.append(',');
        }
        sb.append('\n');
    }
}
