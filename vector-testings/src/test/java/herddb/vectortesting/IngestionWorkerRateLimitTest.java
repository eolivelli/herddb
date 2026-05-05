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

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #220 (now updated for issue #402): the
 * {@code --ingest-max-ops} cap must be a true global cap across all ingest
 * workers — and after #402 it must <em>also</em> not serialise N threads
 * behind a shared lock.
 *
 * <p>The new design splits the global rate evenly across N per-thread
 * limiters owned by an {@link IngestRateLimiterGroup}. Two properties
 * matter, and this test covers both:
 *
 * <ul>
 *   <li><b>Global cap.</b> Sum of per-thread acquires equals the configured
 *       global rate. With N threads each acquiring at {@code rate / N}, the
 *       aggregate elapsed time matches what a single shared limiter at
 *       {@code rate} would produce — but without the inter-thread blocking.</li>
 *   <li><b>No serialisation.</b> N threads each acquire {@code N × P} permits
 *       concurrently in roughly the same wall-clock time it would take a
 *       single thread to acquire {@code P} permits, because each thread has
 *       its own limiter. (Bug 1 of #402: a shared limiter blew this up by
 *       a factor of N.)</li>
 * </ul>
 */
class IngestionWorkerRateLimitTest {

    @Test
    void perThreadGroupBoundsAggregateThroughput() throws Exception {
        final double rate = 500.0; // permits/s, global
        final int threads = 4;
        final int acquisitionsPerThread = 250;
        final int totalAcquisitions = threads * acquisitionsPerThread;

        IngestRateLimiterGroup group = new IngestRateLimiterGroup(threads, rate);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        group.attachThread(idx, Thread.currentThread());
                        start.await();
                        for (int j = 0; j < acquisitionsPerThread; j++) {
                            group.acquire(idx, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish in time");
            double elapsedSecs = (System.nanoTime() - t0) / 1e9;

            // Per-thread rate is rate / threads. With acquisitionsPerThread acquires
            // per worker, each worker takes ~ (acquisitionsPerThread × threads) / rate
            // = totalAcquisitions / rate seconds — same as a shared limiter, but the
            // workers run in parallel without inter-thread blocking.
            double expectedMinSecs = (totalAcquisitions / rate) * 0.85;
            assertTrue(elapsedSecs >= expectedMinSecs,
                    "per-thread group failed to bound aggregate throughput: elapsed="
                            + elapsedSecs + "s, expected >= " + expectedMinSecs + "s");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Issue #402, bug 1 regression: with the per-thread group, N concurrent
     * workers acquiring P permits each finish in approximately the time it
     * takes one worker to acquire P × N permits at the per-thread rate —
     * <em>not</em> N times longer (which is what a shared limiter caused).
     */
    @Test
    void perThreadGroupDoesNotSerialiseNThreads() throws Exception {
        final double globalRate = 1000.0;
        final int threads = 8;
        final int permitsPerThread = 200;

        IngestRateLimiterGroup group = new IngestRateLimiterGroup(threads, globalRate);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        group.attachThread(idx, Thread.currentThread());
                        start.await();
                        for (int j = 0; j < permitsPerThread; j++) {
                            group.acquire(idx, 1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }

            long t0 = System.nanoTime();
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "workers did not finish in time");
            double elapsedSecs = (System.nanoTime() - t0) / 1e9;

            // Per-thread rate = globalRate / threads; each worker takes
            // permitsPerThread / (globalRate / threads) = permitsPerThread * threads / globalRate.
            // Total expected ≈ 200 × 8 / 1000 = 1.6 s.
            // A SHARED limiter would force 8× that = 12.8 s. We assert well under that bound.
            double sharedLimiterUpperBound = (double) permitsPerThread * threads * threads / globalRate * 0.6;
            assertTrue(elapsedSecs < sharedLimiterUpperBound,
                    "per-thread group should not serialise " + threads + " threads; elapsed="
                            + elapsedSecs + "s, would-be-shared-bound=" + sharedLimiterUpperBound + "s");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
