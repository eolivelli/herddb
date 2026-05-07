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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Stress test for the HdrHistogram-backed {@link MetricsCollector} — see
 * issue #443. Validates that:
 * <ul>
 *   <li>{@link MetricsCollector#record(long)} is safe under heavy concurrent
 *       load (HdrHistogram's {@code Recorder} is lock-free for writers).</li>
 *   <li>Concurrent {@link MetricsCollector#computeStats()} callers do not
 *       deadlock and observe a self-consistent view (count never decreases,
 *       max never decreases).</li>
 *   <li>A sentinel max value injected by one thread shows up in the final
 *       snapshot, proving the snapshot path actually merges in-flight data
 *       from all writers.</li>
 * </ul>
 */
class MetricsCollectorConcurrencyTest {

    private static final int RECORDER_THREADS = 16;
    private static final int RECORDS_PER_THREAD = 100_000;
    private static final int SNAPSHOT_THREADS = 4;

    @Test
    void concurrentRecordAndComputeStats() throws Exception {
        MetricsCollector mc = new MetricsCollector();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(RECORDER_THREADS);
        AtomicBoolean snapshotsRunning = new AtomicBoolean(true);
        AtomicBoolean failure = new AtomicBoolean(false);

        ExecutorService pool = Executors.newFixedThreadPool(RECORDER_THREADS + SNAPSHOT_THREADS);
        try {
            // Recorder threads
            for (int t = 0; t < RECORDER_THREADS; t++) {
                final int threadIdx = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 1; i <= RECORDS_PER_THREAD; i++) {
                            // Spread values across a wide range to populate
                            // many HdrHistogram buckets. Last record from one
                            // thread (idx == 0) is a sentinel large value.
                            long nanos;
                            if (threadIdx == 0 && i == RECORDS_PER_THREAD) {
                                nanos = 500_000_000L; // 500 ms — the "max"
                            } else {
                                nanos = ((threadIdx + 1L) * 1_000L) + (i % 1_000L) * 100L;
                            }
                            mc.record(nanos);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RuntimeException e) {
                        // Anything thrown by HdrHistogram — record() should
                        // be allocation-free and lock-free, so a failure
                        // here means we broke the contract.
                        failure.set(true);
                    } finally {
                        done.countDown();
                    }
                });
            }

            // Snapshot threads — concurrent readers
            long[] lastCount = new long[SNAPSHOT_THREADS];
            long[] lastMax = new long[SNAPSHOT_THREADS];
            for (int t = 0; t < SNAPSHOT_THREADS; t++) {
                final int snapIdx = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        while (snapshotsRunning.get()) {
                            MetricsCollector.Stats s = mc.computeStats();
                            // Monotonicity: count and max can only grow.
                            if (s.count() < lastCount[snapIdx]) {
                                failure.set(true);
                            }
                            if (s.maxNanos() < lastMax[snapIdx]) {
                                failure.set(true);
                            }
                            lastCount[snapIdx] = s.count();
                            lastMax[snapIdx] = s.maxNanos();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(60, TimeUnit.SECONDS),
                    "recorder threads should finish within 60 s");
            // Stop snapshot threads.
            snapshotsRunning.set(false);
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertTrue(!failure.get(), "monotonicity / record exception observed");

        // Sleep past the cache TTL so the final computeStats is forced to
        // recompute from the recorder rather than serving a snapshot taken
        // while recorders were still mid-flight.
        Thread.sleep(250);

        // Final snapshot must reflect every recorded value.
        MetricsCollector.Stats finalStats = mc.computeStats();
        assertEquals((long) RECORDER_THREADS * RECORDS_PER_THREAD,
                finalStats.count(),
                "every record() must show up in the final count");

        // The sentinel 500 ms value injected by thread 0 must dominate the
        // max — proves the snapshot path actually merged data from all
        // writers (and isn't reading a stale per-thread buffer).
        assertTrue(finalStats.maxNanos() >= 499_000_000L,
                "max should reflect the sentinel 500 ms value, got " + finalStats.maxNanos());
        assertTrue(finalStats.maxNanos() <= 501_000_000L,
                "max should be close to 500 ms (HdrHistogram bucket precision), got "
                        + finalStats.maxNanos());
    }
}
