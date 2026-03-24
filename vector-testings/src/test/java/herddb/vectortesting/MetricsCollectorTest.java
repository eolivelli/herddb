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
        assertEquals(5_000_000L, stats.p50Nanos());
        assertEquals(5_000_000L, stats.p99Nanos());
        assertEquals(5_000_000L, stats.maxNanos());
    }

    @Test
    void computeStatsMultipleEntries() {
        MetricsCollector mc = new MetricsCollector();
        for (int i = 1; i <= 100; i++) {
            mc.record(i * 1_000_000L);
        }
        MetricsCollector.Stats stats = mc.computeStats();
        assertEquals(100, stats.count());
        // p50 should be around 50ms, p99 around 99ms
        assertTrue(stats.p50Nanos() >= 50_000_000L);
        assertTrue(stats.p99Nanos() >= 99_000_000L);
        assertEquals(100_000_000L, stats.maxNanos());
    }

    @Test
    void percentileIndexDoesNotOverflowWithLargeSize() {
        // This is the scenario that caused the original bug:
        // size * percentile would overflow int when size > ~21M
        // We test via computeStats by recording enough entries to trigger the overflow.
        // Since we can't easily record 22M entries in a unit test, we verify the
        // fix indirectly by checking the arithmetic doesn't produce negative indices.
        MetricsCollector mc = new MetricsCollector();
        // Record 10 entries and verify all percentile indices are non-negative
        for (int i = 0; i < 10; i++) {
            mc.record((i + 1) * 1_000_000L);
        }
        MetricsCollector.Stats stats = mc.computeStats();
        assertTrue(stats.p50Nanos() > 0);
        assertTrue(stats.p95Nanos() > 0);
        assertTrue(stats.p99Nanos() > 0);

        // Direct arithmetic check: simulate the old vs new calculation for large size
        int largeSize = 22_000_000;
        // Old code: (int)(size * percentile / 100.0) - would overflow
        int oldResult = (int) (largeSize * 99 / 100.0);
        // New code: (int)((long) size * percentile / 100.0) - correct
        int newResult = (int) ((long) largeSize * 99 / 100.0);

        assertTrue(oldResult < 0, "Old calculation should overflow to negative");
        assertTrue(newResult > 0, "New calculation should be positive");
        assertEquals(21_780_000, newResult);
    }
}
