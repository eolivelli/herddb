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
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Regression test for issue #220: {@code --ingest-max-ops} must be a global cap
 * across all ingestion workers, not a per-thread cap. The guarantee comes from
 * constructing a single {@link RateLimiter} in {@code VectorBench} and passing
 * the same reference into every {@link IngestionWorker}; this test verifies
 * that a shared limiter does in fact throttle aggregate throughput across
 * multiple threads.
 */
class IngestionWorkerRateLimitTest {

    @Test
    void sharedRateLimiterBoundsAggregateThroughput() throws Exception {
        final double rate = 500.0; // permits/s
        final int threads = 4;
        final int acquisitionsPerThread = 250;
        final int totalAcquisitions = threads * acquisitionsPerThread;

        RateLimiter limiter = RateLimiter.create(rate);
        // Consume the initial burst so it doesn't skew the measurement.
        limiter.acquire(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int j = 0; j < acquisitionsPerThread; j++) {
                            limiter.acquire(1);
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

            // With a truly global limiter, total elapsed >= totalAcquisitions / rate.
            // If the limiter were per-thread, elapsed would be ~acquisitionsPerThread / rate
            // (i.e. 4x faster). Allow a 10% tolerance for scheduling jitter.
            double expectedMinSecs = (totalAcquisitions / rate) * 0.90;
            assertTrue(elapsedSecs >= expectedMinSecs,
                    "shared RateLimiter failed to bound aggregate throughput: elapsed="
                            + elapsedSecs + "s, expected >= " + expectedMinSecs + "s");
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
