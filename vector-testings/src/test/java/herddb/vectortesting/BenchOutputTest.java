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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

class BenchOutputTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static PrintStream printStream(ByteArrayOutputStream sink) {
        return new PrintStream(sink, true, StandardCharsets.UTF_8);
    }

    private static String captureUtf8(ByteArrayOutputStream sink) {
        return sink.toString(StandardCharsets.UTF_8);
    }

    @Test
    void factoryPicksSpinnerByDefault() throws Exception {
        Config cfg = Config.parse(new String[]{});
        BenchOutput out = BenchOutput.create(cfg, System.out);
        assertInstanceOf(BenchOutput.SpinnerBenchOutput.class, out);
        assertFalse(out.suppressesText());
    }

    @Test
    void factoryPicksPlainForNoProgress() throws Exception {
        Config cfg = Config.parse(new String[]{"--no-progress"});
        BenchOutput out = BenchOutput.create(cfg, System.out);
        assertInstanceOf(BenchOutput.PlainBenchOutput.class, out);
        assertFalse(out.suppressesText());
    }

    @Test
    void factoryPicksJsonForOutputFormatJson() throws Exception {
        Config cfg = Config.parse(new String[]{"--output-format", "json"});
        BenchOutput out = BenchOutput.create(cfg, System.out);
        assertInstanceOf(BenchOutput.JsonBenchOutput.class, out);
        assertTrue(out.suppressesText());
    }

    @Test
    void spinnerEmitsCarriageReturnDuringProgress() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.SpinnerBenchOutput out = new BenchOutput.SpinnerBenchOutput(printStream(sink));
        out.progress("ingest", 1.0, "Ingested 100", new LinkedHashMap<>());
        out.progress("ingest", 2.0, "Ingested 200", new LinkedHashMap<>());
        String captured = captureUtf8(sink);
        assertTrue(captured.contains("\r"), "spinner must use \\r overwrite");
        assertTrue(captured.contains("Ingested 100"));
        assertTrue(captured.contains("Ingested 200"));
    }

    @Test
    void plainProgressIsThrottledTo5SecondIntervals() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        FakeClockPlain out = new FakeClockPlain(printStream(sink));
        out.phaseStart("ingest");

        // First sample is always emitted.
        out.now = 1000L;
        LinkedHashMap<String, Object> f1 = new LinkedHashMap<>();
        f1.put("rows", 100L);
        out.progress("ingest", 1.0, "ignored", f1);

        // Inside the 5s window: suppressed.
        out.now = 3000L;
        LinkedHashMap<String, Object> f2 = new LinkedHashMap<>();
        f2.put("rows", 300L);
        out.progress("ingest", 3.0, "ignored", f2);

        // Exactly at 5s: still suppressed (must be >=5000ms elapsed).
        out.now = 5999L;
        LinkedHashMap<String, Object> f3 = new LinkedHashMap<>();
        f3.put("rows", 500L);
        out.progress("ingest", 5.0, "ignored", f3);

        // 6s later: emitted.
        out.now = 6500L;
        LinkedHashMap<String, Object> f4 = new LinkedHashMap<>();
        f4.put("rows", 700L);
        out.progress("ingest", 6.5, "ignored", f4);

        String captured = captureUtf8(sink);
        long progressLines = captured.lines()
                .filter(l -> l.contains("[progress]"))
                .count();
        assertEquals(2, progressLines, "expected one first-sample line + one throttled line");
        assertFalse(captured.contains("\r"), "plain mode must not emit carriage return");
    }

    @Test
    void plainPhaseEndEmitsGreppablePhaseLine() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.PlainBenchOutput out = new BenchOutput.PlainBenchOutput(printStream(sink));

        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("wall_time_s", 12.3);
        fields.put("rows", 1000L);
        fields.put("throughput_ops", 81.3);
        out.phaseEnd("ingestion", fields);

        String captured = captureUtf8(sink).trim();
        assertTrue(captured.startsWith("phase=ingestion"),
                "write-report.sh greps for lines starting with 'phase='; got: " + captured);
        assertTrue(captured.contains("wall_time_s=12.30"));
        assertTrue(captured.contains("rows=1000"));
    }

    @Test
    void plainDoesNotEmitAnyCarriageReturn() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        FakeClockPlain out = new FakeClockPlain(printStream(sink));
        out.phaseStart("ingest");
        out.header("=== INGESTION PHASE ===");
        out.info("Loading query vectors...");

        out.now = 0L;
        out.progress("ingest", 0.0, "some spinner content", linkedMap("rows", 10L, "total", 1000L));

        out.phaseDone("ingest", 1.5);

        LinkedHashMap<String, Object> end = new LinkedHashMap<>();
        end.put("wall_time_s", 1.5);
        out.phaseEnd("ingestion", end);

        LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
        summary.put("dataset", "SIFT1M");
        summary.put("total_wall_time_s", 10.0);
        out.summary(summary);
        out.done();

        String captured = captureUtf8(sink);
        assertFalse(captured.contains("\r"), "plain mode must not emit \\r");
        assertTrue(captured.contains("=== INGESTION PHASE ==="));
        assertTrue(captured.contains("Loading query vectors..."));
        assertTrue(captured.contains("[progress] phase=ingest"));
        assertTrue(captured.contains("[ingest] done in 1.5s"));
        assertTrue(captured.contains("phase=ingestion"));
        assertTrue(captured.contains("BENCHMARK SUMMARY"));
        assertTrue(captured.contains("Benchmark complete."));
    }

    @Test
    void jsonEmitsOneValidObjectPerLine() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        FakeClockJson out = new FakeClockJson(printStream(sink));

        Config cfg = Config.parse(new String[]{"--output-format", "json"});
        out.config(cfg);

        out.phaseStart("ingest");

        out.now = 1000L;
        out.progress("ingest", 1.0, "spinner ignored",
                linkedMap("rows", 100L, "total", 1000L, "ops_per_sec", 100.0));

        // Inside the throttle window → suppressed.
        out.now = 2500L;
        out.progress("ingest", 2.5, "spinner ignored",
                linkedMap("rows", 250L, "total", 1000L, "ops_per_sec", 100.0));

        // Past the window → emitted.
        out.now = 7000L;
        out.progress("ingest", 7.0, "spinner ignored",
                linkedMap("rows", 700L, "total", 1000L, "ops_per_sec", 100.0));

        out.phaseEnd("ingestion", linkedMap("wall_time_s", 10.0, "rows", 1000L));
        out.summary(linkedMap("dataset", "SIFT1M", "total_wall_time_s", 10.0));
        out.done();

        String captured = captureUtf8(sink);
        List<JsonNode> lines = new ArrayList<>();
        for (String line : captured.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            lines.add(MAPPER.readTree(line));
        }
        // config, phase_start, progress (x2: first + post-throttle), phase_end, summary, done
        assertEquals(7, lines.size(), "unexpected NDJSON line count: " + captured);

        assertEquals("config", lines.get(0).get("event").asText());
        assertEquals("SIFT1M", lines.get(0).get("dataset").asText());

        assertEquals("phase_start", lines.get(1).get("event").asText());
        assertEquals("ingest", lines.get(1).get("phase").asText());

        JsonNode firstProgress = lines.get(2);
        assertEquals("progress", firstProgress.get("event").asText());
        assertEquals("ingest", firstProgress.get("phase").asText());
        assertTrue(firstProgress.get("rows").isNumber(), "rows must be a JSON number");
        assertTrue(firstProgress.get("ops_per_sec").isNumber());
        assertEquals(100L, firstProgress.get("rows").asLong());

        // The throttled sample is gone — the next progress event is the 7s one.
        JsonNode secondProgress = lines.get(3);
        assertEquals("progress", secondProgress.get("event").asText());
        assertEquals(700L, secondProgress.get("rows").asLong());

        JsonNode phaseEnd = lines.get(4);
        assertEquals("phase_end", phaseEnd.get("event").asText());
        assertEquals("ingestion", phaseEnd.get("phase").asText());
        assertTrue(phaseEnd.get("wall_time_s").isNumber());

        assertEquals("summary", lines.get(5).get("event").asText());
        assertEquals("done", lines.get(6).get("event").asText());
    }

    @Test
    void plainStatusEmitsFlattenedKeyValueLine() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.PlainBenchOutput out = new BenchOutput.PlainBenchOutput(printStream(sink));

        LinkedHashMap<String, Object> commitlog = new LinkedHashMap<>();
        commitlog.put("ledger", 145L);
        commitlog.put("offset", 12345L);
        LinkedHashMap<String, Object> checkpoint = new LinkedHashMap<>();
        checkpoint.put("ledger", 130L);
        checkpoint.put("lag_ledgers", 15);
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("commitlog", commitlog);
        fields.put("checkpoint", checkpoint);

        out.status("ingest", 1200.0, fields);

        String captured = captureUtf8(sink).trim();
        assertTrue(captured.startsWith("[status] phase=ingest"),
                "status line must start with [status] tag; got: " + captured);
        assertTrue(captured.contains("elapsed_s=1200"));
        assertTrue(captured.contains("commitlog_ledger=145"));
        assertTrue(captured.contains("commitlog_offset=12345"));
        assertTrue(captured.contains("checkpoint_ledger=130"));
        assertTrue(captured.contains("checkpoint_lag_ledgers=15"));
    }

    @Test
    void jsonStatusEventHasNestedStructure() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.JsonBenchOutput out = new BenchOutput.JsonBenchOutput(printStream(sink));

        LinkedHashMap<String, Object> commitlog = new LinkedHashMap<>();
        commitlog.put("ledger", 145L);
        commitlog.put("offset", 12345L);
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        fields.put("commitlog", commitlog);

        out.status("ingest", 1200.0, fields);

        String captured = captureUtf8(sink).trim();
        JsonNode node = MAPPER.readTree(captured);
        assertEquals("status", node.get("event").asText());
        assertEquals("ingest", node.get("phase").asText());
        assertEquals(1200.0, node.get("elapsed_s").asDouble(), 0.001);
        JsonNode cl = node.get("commitlog");
        assertNotNull(cl, "commitlog object must survive JSON serialization: " + captured);
        assertEquals(145L, cl.get("ledger").asLong());
        assertEquals(12345L, cl.get("offset").asLong());
    }

    @Test
    void jsonSuppressesTextHeadersAndInfo() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.JsonBenchOutput out = new BenchOutput.JsonBenchOutput(printStream(sink));
        out.header("=== INGESTION PHASE ===");
        out.info("loading query vectors");

        String captured = captureUtf8(sink);
        assertFalse(captured.contains("=== INGESTION PHASE ==="),
                "json mode must not print human section headers");
        // info still goes through, but as a structured event, not as the raw string alone.
        JsonNode node = MAPPER.readTree(captured.split("\n")[0]);
        assertEquals("info", node.get("event").asText());
        assertEquals("loading query vectors", node.get("message").asText());
    }

    @Test
    void jsonErrorEventIsValid() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BenchOutput.JsonBenchOutput out = new BenchOutput.JsonBenchOutput(printStream(sink));
        out.error(new IllegalStateException("boom"));

        String captured = captureUtf8(sink).trim();
        JsonNode node = MAPPER.readTree(captured);
        assertEquals("error", node.get("event").asText());
        assertEquals("boom", node.get("message").asText());
        assertNotNull(node.get("class"));
        assertEquals("java.lang.IllegalStateException", node.get("class").asText());
    }

    private static LinkedHashMap<String, Object> linkedMap(Object... kv) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Test subclass that lets tests advance the throttle clock deterministically. */
    private static final class FakeClockPlain extends BenchOutput.PlainBenchOutput {
        long now;

        FakeClockPlain(PrintStream out) {
            super(out);
        }

        @Override
        long currentTimeMillis() {
            return now;
        }
    }

    private static final class FakeClockJson extends BenchOutput.JsonBenchOutput {
        long now;

        FakeClockJson(PrintStream out) {
            super(out);
        }

        @Override
        long currentTimeMillis() {
            return now;
        }
    }
}
