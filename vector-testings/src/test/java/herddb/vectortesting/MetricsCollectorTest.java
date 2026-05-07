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
import org.junit.jupiter.api.Test;

class MetricsCollectorTest {

    /**
     * HdrHistogram with 3 significant digits has ~0.1 % quantization error
     * on each recorded value. We assert percentile / max values within this
     * band rather than exact-equal — see issue #443.
     */
    private static final double HDR_TOLERANCE = 0.002; // 0.2 %, 2x the bucket precision

    private static void assertWithinTolerance(long expected, long actual) {
        long band = Math.max(1L, (long) (expected * HDR_TOLERANCE));
        assertTrue(Math.abs(actual - expected) <= band,
                "expected " + expected + " ±" + band + ", got " + actual);
    }

    @Test
    void computeStatsEmpty() {
        MetricsCollector mc = new MetricsCollector();
        MetricsCollector.Stats stats = mc.computeStats();
        assertEquals(0, stats.count());
    }

    @Test
    void computeStatsSingleEntry() {
        MetricsCollector mc = new MetricsCollector();
        mc.record(5_000_000L);
        MetricsCollector.Stats stats = mc.computeStats();
        assertEquals(1, stats.count());
        // HdrHistogram quantizes to 3 significant digits — ~0.1% band.
        assertWithinTolerance(5_000_000L, stats.p50Nanos());
        assertWithinTolerance(5_000_000L, stats.p99Nanos());
        assertWithinTolerance(5_000_000L, stats.maxNanos());
    }

    @Test
    void computeStatsMultipleEntries() {
        MetricsCollector mc = new MetricsCollector();
        for (int i = 1; i <= 100; i++) {
            mc.record(i * 1_000_000L);
        }
        MetricsCollector.Stats stats = mc.computeStats();
        assertEquals(100, stats.count());
        // p50 should be around 50ms, p99 around 99ms, allowing for HdrHistogram's
        // bucket boundary effects (a sample may land in the next-higher bucket).
        assertTrue(stats.p50Nanos() >= 49_000_000L && stats.p50Nanos() <= 51_000_000L,
                "p50=" + stats.p50Nanos());
        assertTrue(stats.p99Nanos() >= 98_000_000L && stats.p99Nanos() <= 100_500_000L,
                "p99=" + stats.p99Nanos());
        assertWithinTolerance(100_000_000L, stats.maxNanos());
    }

    @Test
    void recordClampsBelowLowestAndAboveHighest() {
        // HdrHistogram is configured for 1 ns – 1 hour at 3 sig digits;
        // record() must clamp values outside that range rather than
        // throw. Also asserts getLastNanos() returns the same clamped
        // value the histogram saw (issue #443 review feedback).
        MetricsCollector mcLow = new MetricsCollector();
        mcLow.record(0L);
        MetricsCollector.Stats lowStats = mcLow.computeStats();
        assertEquals(1L, lowStats.count());
        assertEquals(1L, lowStats.maxNanos(),
                "record(0) should clamp to LOWEST_NANOS=1 ns");
        assertEquals(1L, mcLow.getLastNanos(),
                "getLastNanos() must reflect the clamped value, not the raw input");

        MetricsCollector mcNeg = new MetricsCollector();
        mcNeg.record(-42L);
        assertEquals(1L, mcNeg.computeStats().maxNanos());
        assertEquals(1L, mcNeg.getLastNanos());

        long oneHourNanos = java.util.concurrent.TimeUnit.HOURS.toNanos(1);
        MetricsCollector mcHigh = new MetricsCollector();
        mcHigh.record(Long.MAX_VALUE);
        MetricsCollector.Stats highStats = mcHigh.computeStats();
        assertEquals(1L, highStats.count());
        // HdrHistogram quantizes to bucket boundaries — assert the value
        // is within tolerance of HIGHEST_NANOS rather than exact-equal.
        assertWithinTolerance(oneHourNanos, highStats.maxNanos());
        assertEquals(oneHourNanos, mcHigh.getLastNanos(),
                "getLastNanos() must reflect the HIGHEST_NANOS clamp");
    }

    @Test
    void percentileIndexDoesNotOverflowWithLargeSize() {
        // Original (pre-issue-443) regression guard: with the old reservoir-based
        // implementation, percentileIndex could overflow when size > ~21M because
        // of int×int multiplication. HdrHistogram does its percentile lookup over
        // bucket counts (longs internally) so the overflow class is gone, but we
        // keep the test to assert the public API still returns sane percentile
        // values for small samples.
        MetricsCollector mc = new MetricsCollector();
        for (int i = 0; i < 10; i++) {
            mc.record((i + 1) * 1_000_000L);
        }
        MetricsCollector.Stats stats = mc.computeStats();
        assertTrue(stats.p50Nanos() > 0);
        assertTrue(stats.p95Nanos() > 0);
        assertTrue(stats.p99Nanos() > 0);
        assertTrue(stats.maxNanos() > 0);
    }
}
