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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

/**
 * Validates the short-TTL cache on {@link MetricsCollector#computeStats()} —
 * see issue #443. Concurrent status / progress threads call computeStats at
 * 500 ms cadence; we want them to share one snapshot per refresh tick rather
 * than each repeat the (now cheap, but still non-zero) HdrHistogram buffer
 * swap.
 */
class MetricsCollectorCacheTest {

    @Test
    void repeatedCallsWithinTtlReturnSameInstance() {
        MetricsCollector mc = new MetricsCollector();
        mc.record(5_000_000L);
        // Two back-to-back calls should hit the cache and return the same
        // immutable snapshot reference. HdrHistogram's recompute is fast but
        // not free — sharing the result avoids redundant buffer-swaps.
        MetricsCollector.Stats first = mc.computeStats();
        MetricsCollector.Stats second = mc.computeStats();
        assertSame(first, second, "cached snapshot should be returned for in-window calls");
    }

    @Test
    void newRecordsBecomeVisibleAfterTtlExpiry() throws InterruptedException {
        // The cache deliberately does NOT invalidate on record() — under
        // continuous benchmark load that would prevent the cache from ever
        // serving a hit. Instead, the TTL bounds staleness; once it
        // expires the next computeStats() reflects all values recorded
        // since the previous snapshot.
        MetricsCollector mc = new MetricsCollector();
        mc.record(5_000_000L);
        MetricsCollector.Stats first = mc.computeStats();
        assertEquals(1, first.count());

        mc.record(20_000_000L);
        // Sleep past the cache TTL so the next computeStats refreshes.
        Thread.sleep(300L);
        MetricsCollector.Stats afterTtl = mc.computeStats();
        assertEquals(2, afterTtl.count(), "second record must be visible after TTL expiry");
        assertNotEquals(first, afterTtl, "snapshot must be recomputed after TTL expiry");
        assertTrue(afterTtl.maxNanos() >= 19_900_000L,
                "max should reflect the new value, got " + afterTtl.maxNanos());
    }

    @Test
    void cacheEventuallyExpires() throws InterruptedException {
        // The cache TTL inside MetricsCollector is 200 ms; sleeping 300 ms
        // guarantees we cross the boundary. After that, a no-op call (no
        // record between) should still recompute and return a fresh
        // snapshot instance — we can't assert different *contents* without
        // new data, but we can assert the cached identity changed once the
        // TTL elapses.
        MetricsCollector mc = new MetricsCollector();
        mc.record(5_000_000L);
        MetricsCollector.Stats snapshot1 = mc.computeStats();
        Thread.sleep(300L);
        MetricsCollector.Stats snapshot2 = mc.computeStats();
        // Contents must be equal (no new data), but the snapshot reference
        // is recomputed because the TTL expired.
        assertEquals(snapshot1, snapshot2);
        assertTrue(snapshot1 != snapshot2,
                "TTL expiry should produce a fresh Stats instance");
    }

    @Test
    void emptyHistogramHonoursCache() {
        // The fast-path for an empty collector still routes through the
        // cache. Two back-to-back calls on an empty collector should not
        // diverge.
        MetricsCollector mc = new MetricsCollector();
        MetricsCollector.Stats first = mc.computeStats();
        MetricsCollector.Stats second = mc.computeStats();
        assertEquals(0, first.count());
        assertSame(first, second);
    }

    @Test
    void uncachedAlwaysRecomputesAndIncludesEveryRecord() {
        // The canonical run-summary path uses computeStatsUncached() to
        // bypass the 200 ms TTL — otherwise the very last batch of
        // record() calls (made between the progress thread's last tick
        // and the workers finishing) would be silently omitted from the
        // printed final stats.
        MetricsCollector mc = new MetricsCollector();
        for (int i = 1; i <= 50; i++) {
            mc.record(i * 1_000_000L);
        }
        // Populate the cache via the live-UI path.
        MetricsCollector.Stats cached = mc.computeStats();
        assertEquals(50, cached.count());

        // Record additional values; computeStats() within TTL will return
        // the stale cached snapshot, but computeStatsUncached() must see
        // every value recorded so far.
        for (int i = 51; i <= 100; i++) {
            mc.record(i * 1_000_000L);
        }
        MetricsCollector.Stats stale = mc.computeStats();
        assertEquals(50, stale.count(), "TTL cache should still report the pre-record snapshot");

        MetricsCollector.Stats fresh = mc.computeStatsUncached();
        assertEquals(100, fresh.count(),
                "uncached snapshot must reflect every record() up to this moment");
        assertNotEquals(stale, fresh,
                "uncached path must produce a distinct snapshot from the stale cached one");
        assertTrue(fresh.maxNanos() >= 99_000_000L,
                "max should reflect the highest recorded value, got " + fresh.maxNanos());

        // After computeStatsUncached(), the cache holds the fresh snapshot,
        // so a subsequent computeStats() within TTL hits the new value
        // (no redundant buffer-swap).
        MetricsCollector.Stats afterUncached = mc.computeStats();
        assertSame(fresh, afterUncached,
                "uncached path should also refresh the cache for subsequent live-UI reads");
    }
}
