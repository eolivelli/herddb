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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Output abstraction for the vector benchmark. Three flavours:
 * <ul>
 *   <li>{@link SpinnerBenchOutput} — interactive spinner with {@code \r} overwrite (legacy default).</li>
 *   <li>{@link PlainBenchOutput} — one {@code \n}-terminated line per sample, supervisor-friendly.</li>
 *   <li>{@link JsonBenchOutput} — NDJSON, one JSON object per line.</li>
 * </ul>
 *
 * <p>The factory {@link #create(Config, PrintStream)} picks the right implementation from a
 * {@link Config}. Injecting the {@link PrintStream} makes unit tests straightforward.</p>
 */
abstract class BenchOutput {

    /** Minimum interval between consecutive progress lines in plain/json modes. */
    static final long PLAIN_PROGRESS_INTERVAL_MS = 5_000L;

    /** Prints a section header (e.g. {@code "=== INGESTION PHASE ==="}). */
    abstract void header(String title);

    /** Prints a free-form informational message. */
    abstract void info(String message);

    /** Prints the effective configuration at startup. */
    abstract void config(Config config);

    /** Signals the start of a phase. */
    abstract void phaseStart(String phase);

    /**
     * Emits a progress sample for the currently running phase. The {@code statusLine}
     * is the legacy human-readable spinner line used by the spinner mode only;
     * {@code fields} is the structured payload used by plain/json modes.
     */
    abstract void progress(String phase, double elapsedSecs, String statusLine, LinkedHashMap<String, Object> fields);

    /** Prints the "done in NNs" line at the end of a phase. */
    abstract void phaseDone(String phase, double elapsedSecs);

    /** Emits end-of-phase metrics (a single {@code phase=...} line in text mode). */
    abstract void phaseEnd(String phase, LinkedHashMap<String, Object> fields);

    /** Emits the final top-level summary. */
    abstract void summary(LinkedHashMap<String, Object> fields);

    /** Final "Benchmark complete." marker. */
    abstract void done();

    /** Emits a fatal error — only json mode uses this; other modes just let the exception propagate. */
    abstract void error(Throwable t);

    /**
     * Returns {@code true} when section headers, info lines and textual phase summaries should
     * be suppressed because the caller is driving a structured mode (NDJSON).
     */
    boolean suppressesText() {
        return false;
    }

    static BenchOutput create(Config config) {
        return create(config, System.out);
    }

    static BenchOutput create(Config config, PrintStream out) {
        if (config.outputFormat == Config.OutputFormat.JSON) {
            return new JsonBenchOutput(out);
        }
        if (config.noProgress) {
            return new PlainBenchOutput(out);
        }
        return new SpinnerBenchOutput(out);
    }

    // ---------------------------------------------------------------- Spinner

    /** Legacy animated spinner using carriage-return overwrite. */
    static final class SpinnerBenchOutput extends BenchOutput {

        private static final char[] SPINNER = {'|', '/', '-', '\\'};

        private final PrintStream out;
        private int spin;

        SpinnerBenchOutput(PrintStream out) {
            this.out = out;
        }

        @Override
        void header(String title) {
            out.println(title);
        }

        @Override
        void info(String message) {
            out.println(message);
        }

        @Override
        void config(Config config) {
            out.println("Vector Benchmark Configuration:");
            out.println(config);
            out.println();
        }

        @Override
        void phaseStart(String phase) {
            // Header already covers this for text modes.
        }

        @Override
        void progress(String phase, double elapsedSecs, String statusLine, LinkedHashMap<String, Object> fields) {
            int filled = Math.min(40, (int) (elapsedSecs / 5));
            String bar = "=".repeat(filled) + " ".repeat(40 - filled);
            if (statusLine == null || statusLine.isEmpty()) {
                out.printf("\r  [%s] %c %.0fs...", bar, SPINNER[spin++ % 4], elapsedSecs);
            } else {
                out.printf("\r  [%s] %c %.0fs | %s", bar, SPINNER[spin++ % 4], elapsedSecs, statusLine);
            }
            out.flush();
        }

        @Override
        void phaseDone(String phase, double elapsedSecs) {
            out.printf("\r  [%s] done in %.1fs%n", "=".repeat(40), elapsedSecs);
        }

        @Override
        void phaseEnd(String phase, LinkedHashMap<String, Object> fields) {
            out.println(PlainBenchOutput.formatPhaseLine(phase, fields));
        }

        @Override
        void summary(LinkedHashMap<String, Object> fields) {
            out.println(PlainBenchOutput.formatSummary(fields));
        }

        @Override
        void done() {
            out.println();
            out.println("Benchmark complete.");
        }

        @Override
        void error(Throwable t) {
            // Let the exception propagate on the caller side; nothing structured to emit here.
        }
    }

    // ------------------------------------------------------------------ Plain

    /** Plain \n-terminated output, one progress line every {@link #PLAIN_PROGRESS_INTERVAL_MS}. */
    static class PlainBenchOutput extends BenchOutput {

        private final PrintStream out;
        private long lastProgressMs;
        private boolean firstProgressEmitted;

        PlainBenchOutput(PrintStream out) {
            this.out = out;
        }

        long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        void header(String title) {
            out.println(title);
        }

        @Override
        void info(String message) {
            out.println(message);
        }

        @Override
        void config(Config config) {
            out.println("Vector Benchmark Configuration:");
            out.println(config);
            out.println();
        }

        @Override
        void phaseStart(String phase) {
            firstProgressEmitted = false;
            lastProgressMs = 0;
        }

        @Override
        void progress(String phase, double elapsedSecs, String statusLine, LinkedHashMap<String, Object> fields) {
            long now = currentTimeMillis();
            if (firstProgressEmitted && (now - lastProgressMs) < PLAIN_PROGRESS_INTERVAL_MS) {
                return;
            }
            firstProgressEmitted = true;
            lastProgressMs = now;
            emitProgress(phase, elapsedSecs, fields);
        }

        void emitProgress(String phase, double elapsedSecs, LinkedHashMap<String, Object> fields) {
            LinkedHashMap<String, Object> line = new LinkedHashMap<>();
            line.put("phase", phase);
            line.put("elapsed_s", roundOneDecimal(elapsedSecs));
            if (fields != null) {
                line.putAll(fields);
            }
            out.println("  [progress] " + renderKeyValues(line));
        }

        @Override
        void phaseDone(String phase, double elapsedSecs) {
            out.printf("  [%s] done in %.1fs%n", phase, elapsedSecs);
        }

        @Override
        void phaseEnd(String phase, LinkedHashMap<String, Object> fields) {
            out.println(formatPhaseLine(phase, fields));
        }

        @Override
        void summary(LinkedHashMap<String, Object> fields) {
            out.println(formatSummary(fields));
        }

        @Override
        void done() {
            out.println();
            out.println("Benchmark complete.");
        }

        @Override
        void error(Throwable t) {
            // Propagation handles the stack trace.
        }

        static String formatPhaseLine(String phase, LinkedHashMap<String, Object> fields) {
            LinkedHashMap<String, Object> all = new LinkedHashMap<>();
            all.put("phase", phase);
            if (fields != null) {
                all.putAll(fields);
            }
            return renderKeyValues(all);
        }

        static String formatSummary(LinkedHashMap<String, Object> fields) {
            StringBuilder sb = new StringBuilder();
            sb.append(System.lineSeparator());
            sb.append("========================================").append(System.lineSeparator());
            sb.append("          BENCHMARK SUMMARY").append(System.lineSeparator());
            sb.append("========================================").append(System.lineSeparator());
            if (fields != null) {
                for (Map.Entry<String, Object> e : fields.entrySet()) {
                    sb.append(e.getKey()).append('=').append(renderValue(e.getValue())).append(System.lineSeparator());
                }
            }
            sb.append("========================================");
            return sb.toString();
        }

        private static String renderKeyValues(LinkedHashMap<String, Object> fields) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Map.Entry<String, Object> e : fields.entrySet()) {
                if (!first) {
                    sb.append(' ');
                }
                first = false;
                sb.append(e.getKey()).append('=').append(renderValue(e.getValue()));
            }
            return sb.toString();
        }

        private static String renderValue(Object v) {
            if (v == null) {
                return "null";
            }
            if (v instanceof Double) {
                double d = (Double) v;
                if (d == Math.floor(d) && !Double.isInfinite(d)) {
                    return String.format("%.0f", d);
                }
                return String.format("%.2f", d);
            }
            if (v instanceof Float) {
                return String.format("%.2f", (Float) v);
            }
            return String.valueOf(v);
        }

        private static double roundOneDecimal(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    // ------------------------------------------------------------------- JSON

    /** NDJSON — one JSON object per line. Suppresses all human-formatted text. */
    static class JsonBenchOutput extends BenchOutput {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final PrintStream out;
        private long lastProgressMs;
        private boolean firstProgressEmitted;

        JsonBenchOutput(PrintStream out) {
            this.out = out;
        }

        long currentTimeMillis() {
            return System.currentTimeMillis();
        }

        @Override
        boolean suppressesText() {
            return true;
        }

        @Override
        void header(String title) {
            // No-op; phaseStart carries the structured equivalent.
        }

        @Override
        void info(String message) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "info");
            m.put("message", message);
            write(m);
        }

        @Override
        void config(Config config) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "config");
            m.put("jdbc_url", config.jdbcUrl);
            m.put("dataset", config.dataset.name());
            m.put("table", config.tableName);
            m.put("rows", (long) config.numRows);
            m.put("ingest_threads", config.ingestThreads);
            m.put("batch_size", config.batchSize);
            m.put("query_threads", config.queryThreads);
            m.put("queries", config.queryCount);
            m.put("top_k", config.topK);
            m.put("index_m", config.indexM);
            m.put("index_beam_width", config.indexBeamWidth);
            m.put("similarity", config.effectiveSimilarity());
            m.put("index_before_ingest", config.indexBeforeIngest);
            m.put("skip_ingest", config.skipIngest);
            m.put("skip_index", config.skipIndex);
            m.put("checkpoint", config.checkpoint);
            m.put("checkpoint_timeout_seconds", config.checkpointTimeoutSeconds);
            m.put("ingest_max_ops_per_second", config.ingestMaxOpsPerSecond);
            write(m);
        }

        @Override
        void phaseStart(String phase) {
            firstProgressEmitted = false;
            lastProgressMs = 0;
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "phase_start");
            m.put("phase", phase);
            write(m);
        }

        @Override
        void progress(String phase, double elapsedSecs, String statusLine, LinkedHashMap<String, Object> fields) {
            long now = currentTimeMillis();
            if (firstProgressEmitted && (now - lastProgressMs) < PLAIN_PROGRESS_INTERVAL_MS) {
                return;
            }
            firstProgressEmitted = true;
            lastProgressMs = now;
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "progress");
            m.put("phase", phase);
            m.put("elapsed_s", elapsedSecs);
            if (fields != null) {
                m.putAll(fields);
            }
            write(m);
        }

        @Override
        void phaseDone(String phase, double elapsedSecs) {
            // phase_end carries the definitive metrics; a bare "done" is noise in NDJSON.
        }

        @Override
        void phaseEnd(String phase, LinkedHashMap<String, Object> fields) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "phase_end");
            m.put("phase", phase);
            if (fields != null) {
                m.putAll(fields);
            }
            write(m);
        }

        @Override
        void summary(LinkedHashMap<String, Object> fields) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "summary");
            if (fields != null) {
                m.putAll(fields);
            }
            write(m);
        }

        @Override
        void done() {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "done");
            write(m);
        }

        @Override
        void error(Throwable t) {
            LinkedHashMap<String, Object> m = new LinkedHashMap<>();
            m.put("event", "error");
            m.put("message", t.getMessage() == null ? t.getClass().getName() : t.getMessage());
            m.put("class", t.getClass().getName());
            write(m);
        }

        private void write(LinkedHashMap<String, Object> payload) {
            try {
                out.println(MAPPER.writeValueAsString(payload));
                out.flush();
            } catch (JsonProcessingException e) {
                // Fall back to a minimal synthetic object so the stream stays valid NDJSON.
                out.println("{\"event\":\"error\",\"message\":\"json encode failed: "
                        + e.getMessage().replace('"', '\'') + "\"}");
                out.flush();
            }
        }
    }
}
