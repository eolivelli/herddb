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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IngestionWindowTracker} (issue #453).
 *
 * <p>All tests use an injectable {@link java.util.function.LongSupplier} clock so
 * time can be advanced deterministically without sleeping — the same pattern
 * used by {@link BenchOutput.PlainBenchOutput} for {@code currentTimeMillis()}.
 */
class IngestionWindowTrackerTest {

    /**
     * HdrHistogram with 3 significant digits has ~0.1 % quantization error
     * on each recorded value. Latency assertions use this tolerance band.
     */
    private static final double HDR_TOLERANCE = 0.002; // 0.2 % — 2× bucket precision

    private static void assertWithinTolerance(double expected, double actual, String label) {
        double band = Math.max(0.01, expected * HDR_TOLERANCE);
        assertTrue(Math.abs(actual - expected) <= band,
                label + ": expected " + expected + " ±" + band + ", got " + actual);
    }

    // ---------------------------------------------------------------- rate tests

    @Test
    void emptyTrackerReturnsZeroRate() {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(30));
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        assertEquals(0.0,
                tracker.computeWindowedRate(IngestionWindowTracker.ONE_MIN_NANOS, 0L));
        assertEquals(0.0,
                tracker.computeWindowedRate(IngestionWindowTracker.FIVE_MIN_NANOS, 0L));
    }

    @Test
    void singleCommitWithinWindowGivesCorrectRate() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        long ingestStart = 0L;

        // Record 200 rows at t = 30 s.
        clock.set(TimeUnit.SECONDS.toNanos(30));
        tracker.recordCommit(5_000_000L, 200);

        // At t = 60 s the 1-min window is [0 s, 60 s]. The run started at t = 0,
        // so the denominator is min(60 s, 60 s) = 60 s.
        clock.set(TimeUnit.SECONDS.toNanos(60));
        double rate = tracker.computeWindowedRate(TimeUnit.MINUTES.toNanos(1), ingestStart);
        assertEquals(200.0 / 60.0, rate, 0.001,
                "rate = rows / windowSeconds");
    }

    @Test
    void multipleCommitsAccumulateRowsForRateComputation() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        long ingestStart = 0L;

        // Three commits of 100 rows each, all within the 1-min window.
        clock.set(TimeUnit.SECONDS.toNanos(10));
        tracker.recordCommit(5_000_000L, 100);
        clock.set(TimeUnit.SECONDS.toNanos(20));
        tracker.recordCommit(5_000_000L, 100);
        clock.set(TimeUnit.SECONDS.toNanos(30));
        tracker.recordCommit(5_000_000L, 100);

        // At t = 60 s: rate = 300 / 60 = 5.0 rows/s.
        clock.set(TimeUnit.SECONDS.toNanos(60));
        double rate = tracker.computeWindowedRate(TimeUnit.MINUTES.toNanos(1), ingestStart);
        assertEquals(300.0 / 60.0, rate, 0.001);
    }

    @Test
    void entryBeforeCutoffIsExcludedFromWindowedRate() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        long ingestStart = 0L;

        // Record 500 rows at t = 0 — this is outside the 1-min window at t = 90 s.
        clock.set(0L);
        tracker.recordCommit(5_000_000L, 500);

        // Record 100 rows at t = 60 s — inside the 1-min window at t = 90 s.
        clock.set(TimeUnit.SECONDS.toNanos(60));
        tracker.recordCommit(5_000_000L, 100);

        // At t = 90 s: window = [30 s, 90 s]. The t = 0 entry is excluded.
        // Denominator = min(60 s, 90 s) = 60 s. Rate = 100 / 60 ≈ 1.67.
        clock.set(TimeUnit.SECONDS.toNanos(90));
        double rate = tracker.computeWindowedRate(TimeUnit.MINUTES.toNanos(1), ingestStart);
        assertEquals(100.0 / 60.0, rate, 0.001,
                "entry at t=0 is outside the 1-min window at t=90s");
    }

    @Test
    void runShorterThanWindowUsesElapsedTimeAsDenominator() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);

        // Ingest started at t = 10 s.
        long ingestStart = TimeUnit.SECONDS.toNanos(10);

        // Record 300 rows at t = 20 s (10 s after ingestion start).
        clock.set(TimeUnit.SECONDS.toNanos(20));
        tracker.recordCommit(5_000_000L, 300);

        // At t = 40 s, compute 5-min rate. The run has only been going for 30 s,
        // so the denominator is 30 s (elapsed), not 300 s (window).
        // Rate = 300 / 30 = 10.0 rows/s.
        clock.set(TimeUnit.SECONDS.toNanos(40));
        double rate = tracker.computeWindowedRate(IngestionWindowTracker.FIVE_MIN_NANOS, ingestStart);
        assertEquals(300.0 / 30.0, rate, 0.001,
                "denominator must clamp to elapsed time when run < window");
    }

    @Test
    void expiredEntriesAreTrimmedOnWrite() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        long ingestStart = 0L;

        // Record 100 rows at t = 0.
        clock.set(0L);
        tracker.recordCommit(5_000_000L, 100);

        // Advance past MAX_WINDOW and record another commit — this triggers trimming.
        long past5Min = IngestionWindowTracker.MAX_WINDOW_NANOS + 1L;
        clock.set(past5Min);
        tracker.recordCommit(5_000_000L, 50);

        // The t = 0 entry has been trimmed. Only the 50-row commit remains.
        // At the same t, the 5-min window is [1 ns, past5Min]. t = 0 is outside.
        // Denominator = min(300 s, past5Min / 1e9) ≈ 300 s.
        // Rate ≈ 50 / 300 ≈ 0.167.
        double rate = tracker.computeWindowedRate(IngestionWindowTracker.FIVE_MIN_NANOS, ingestStart);
        double expectedRate = 50.0 / (IngestionWindowTracker.MAX_WINDOW_NANOS / 1e9);
        assertEquals(expectedRate, rate, 0.001,
                "old entry must be trimmed; only the recent commit contributes");
    }

    @Test
    void onlyCurrentWindowRowsCountedNotAll() {
        // Regression guard: commit inside 5-min but outside 1-min must appear
        // in ops_per_sec_5m but NOT in ops_per_sec_1m.
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        long ingestStart = 0L;

        // Commit 1000 rows at t = 2 min (inside 5-min, outside 1-min at t = 4 min).
        clock.set(TimeUnit.MINUTES.toNanos(2));
        tracker.recordCommit(5_000_000L, 1000);

        // Commit 200 rows at t = 3 min 30 s (inside both windows at t = 4 min).
        clock.set(TimeUnit.MINUTES.toNanos(3) + TimeUnit.SECONDS.toNanos(30));
        tracker.recordCommit(5_000_000L, 200);

        // Query at t = 4 min.
        clock.set(TimeUnit.MINUTES.toNanos(4));

        // 5-min window: both commits inside → 1200 rows / 240 s = 5.0 rows/s.
        double rate5m = tracker.computeWindowedRate(IngestionWindowTracker.FIVE_MIN_NANOS, ingestStart);
        assertEquals(1200.0 / 240.0, rate5m, 0.01,
                "5-min window must include both commits");

        // 1-min window: only the 200-row commit at t = 3m30s is inside → 200 rows / 60 s ≈ 3.33.
        double rate1m = tracker.computeWindowedRate(IngestionWindowTracker.ONE_MIN_NANOS, ingestStart);
        assertEquals(200.0 / 60.0, rate1m, 0.01,
                "1-min window must exclude the early commit");
    }

    // -------------------------------------------------------------- latency tests

    @Test
    void windowedLatencyMapIsAllZerosWhenEmpty() {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(5));
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);

        Map<String, Object> m = tracker.computeWindowedLatencyMap(IngestionWindowTracker.FIVE_MIN_NANOS);

        assertEquals(0.0, m.get("mean_ms"), "mean_ms must be 0 when empty");
        assertEquals(0.0, m.get("p50_ms"),  "p50_ms must be 0 when empty");
        assertEquals(0.0, m.get("p99_ms"),  "p99_ms must be 0 when empty");
        assertEquals(0.0, m.get("max_ms"),  "max_ms must be 0 when empty");
    }

    @Test
    void windowedLatencyMapReflectsPercentilesForCommitsInWindow() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);

        // Record 100 commits with latencies 1 ms … 100 ms, all at t = 0.
        clock.set(0L);
        for (int i = 1; i <= 100; i++) {
            tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(i), 1);
        }

        // Query at t = 30 s (well inside the 5-min window).
        clock.set(TimeUnit.SECONDS.toNanos(30));
        Map<String, Object> m = tracker.computeWindowedLatencyMap(IngestionWindowTracker.FIVE_MIN_NANOS);

        double p50 = (Double) m.get("p50_ms");
        double p99 = (Double) m.get("p99_ms");
        double max = (Double) m.get("max_ms");

        assertTrue(p50 >= 49.0 && p50 <= 51.0,
                "p50 should be ~50 ms, got " + p50);
        assertTrue(p99 >= 98.0 && p99 <= 100.5,
                "p99 should be ~99 ms, got " + p99);
        assertTrue(max >= 99.0 && max <= 101.0,
                "max should be ~100 ms, got " + max);
    }

    @Test
    void windowedLatencyMapExcludesCommitsOutsideWindow() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);

        // Record a very slow commit (500 ms) at t = 0 — outside the 1-min window.
        clock.set(0L);
        tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(500), 1);

        // Record a fast commit (10 ms) at t = 60 s — inside the 1-min window at t = 90 s.
        clock.set(TimeUnit.SECONDS.toNanos(60));
        tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(10), 1);

        // Query at t = 90 s over the 1-min window.
        clock.set(TimeUnit.SECONDS.toNanos(90));
        Map<String, Object> m = tracker.computeWindowedLatencyMap(TimeUnit.MINUTES.toNanos(1));

        // Only the 10-ms commit should contribute; max must not be near 500 ms.
        double max = (Double) m.get("max_ms");
        assertWithinTolerance(10.0, max, "max_ms");

        double p50 = (Double) m.get("p50_ms");
        assertWithinTolerance(10.0, p50, "p50_ms");
    }

    @Test
    void windowedLatencyMapHasAllRequiredKeys() {
        AtomicLong clock = new AtomicLong(0L);
        IngestionWindowTracker tracker = new IngestionWindowTracker(clock::get);
        clock.set(0L);
        tracker.recordCommit(TimeUnit.MILLISECONDS.toNanos(15), 100);
        clock.set(TimeUnit.SECONDS.toNanos(10));

        Map<String, Object> m = tracker.computeWindowedLatencyMap(IngestionWindowTracker.FIVE_MIN_NANOS);

        assertTrue(m.containsKey("mean_ms"), "must contain mean_ms");
        assertTrue(m.containsKey("p50_ms"),  "must contain p50_ms");
        assertTrue(m.containsKey("p99_ms"),  "must contain p99_ms");
        assertTrue(m.containsKey("max_ms"),  "must contain max_ms");
    }
}
