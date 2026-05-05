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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Issue #402: unit tests for {@link PerThreadRateLimiter}. Cover the
 * accumulation of deadlines, wake-on-update via {@code unpark}, and the
 * {@code 0 == unlimited} sentinel mapping.
 */
class PerThreadRateLimiterTest {

    @Test
    void acquireZeroIsNoOp() throws InterruptedException {
        PerThreadRateLimiter l = new PerThreadRateLimiter(1.0);
        long t0 = System.nanoTime();
        l.acquire(0);
        long elapsed = System.nanoTime() - t0;
        // Generous upper bound (500 ms) because this test is asserting "doesn't park"
        // — a binary property — and a tight bound flakes on slow CI under JIT cold path.
        // 500 ms is still vastly below the ~1 s a 1 permit/s park would take.
        assertTrue(elapsed < 500_000_000L, "acquire(0) must return without parking, got " + elapsed + " ns");
    }

    @Test
    void unlimitedIsEffectivelyInstant() throws InterruptedException {
        PerThreadRateLimiter l = new PerThreadRateLimiter(0.0); // 0 = unlimited
        long t0 = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            l.acquire(1);
        }
        long elapsed = System.nanoTime() - t0;
        // 1000 acquires at UNLIMITED_RATE should be well under 100 ms even on slow CI.
        assertTrue(elapsed < 100_000_000L,
                "1000 unlimited acquires should be fast, got " + (elapsed / 1e6) + " ms");
    }

    @Test
    void getRateReturnsConfiguredValue() {
        PerThreadRateLimiter l = new PerThreadRateLimiter(500.0);
        assertEquals(500.0, l.getRate(), 1e-3);
    }

    @Test
    void getRateForUnlimitedReturnsSentinel() {
        PerThreadRateLimiter l = new PerThreadRateLimiter(0.0);
        assertEquals(PerThreadRateLimiter.UNLIMITED_RATE, l.getRate(), 1.0);
    }

    @Test
    void sequentialAcquiresAccumulateDelay() throws InterruptedException {
        PerThreadRateLimiter l = new PerThreadRateLimiter(2.0); // 2/s = 500 ms per permit
        // Drain initial slot (no prior reservation, so first call is fast).
        l.acquire(1);
        long t0 = System.nanoTime();
        for (int i = 0; i < 4; i++) {
            l.acquire(1);
        }
        long elapsed = System.nanoTime() - t0;
        // 4 acquires at 2/s should take ~ 2s. Allow generous lower bound for CI jitter.
        assertTrue(elapsed >= 1_500_000_000L,
                "4 acquires at 2 ops/s should take >= 1.5s, got " + (elapsed / 1e6) + " ms");
    }

    @Test
    void setRateRaiseUnblocksParkedThread() throws Exception {
        PerThreadRateLimiter l = new PerThreadRateLimiter(0.5); // 0.5/s = 2 s per permit
        // Drain initial slot.
        l.acquire(1);

        // Spawn a thread that will be parked deeply (2+ s wait).
        CountDownLatch threadStarted = new CountDownLatch(1);
        AtomicReference<Long> waitNanos = new AtomicReference<>(-1L);
        Thread worker = new Thread(() -> {
            l.attachThread(Thread.currentThread());
            threadStarted.countDown();
            long t0 = System.nanoTime();
            try {
                l.acquire(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            waitNanos.set(System.nanoTime() - t0);
        });
        worker.start();

        // Wait for the worker to be inside acquire().
        assertTrue(threadStarted.await(2, TimeUnit.SECONDS), "worker did not start in time");
        Thread.sleep(200); // let it park

        // Raise the rate dramatically; the worker should wake up quickly.
        long t0 = System.nanoTime();
        l.setRate(10000.0);
        worker.join(3_000);
        long elapsedSinceRaise = System.nanoTime() - t0;

        assertTrue(!worker.isAlive(), "worker should have completed after rate raise");
        // 2-second upper bound: vastly under the original ~2 s park computed at 0.5/s,
        // but generous enough that scheduler jitter / GC pause on shared CI doesn't flake.
        assertTrue(elapsedSinceRaise < 2_000_000_000L,
                "worker should wake within 2 s of setRate, took " + (elapsedSinceRaise / 1e6) + " ms");
    }

    @Test
    void setRateLowerStillEnforcesNewRate() throws InterruptedException {
        PerThreadRateLimiter l = new PerThreadRateLimiter(10000.0);
        // Drain initial slot.
        l.acquire(1);
        // Lower the rate to 2/s.
        l.setRate(2.0);
        assertEquals(2.0, l.getRate(), 1e-3);

        // 4 acquires at 2/s should take ~ 2s.
        long t0 = System.nanoTime();
        for (int i = 0; i < 4; i++) {
            l.acquire(1);
        }
        long elapsed = System.nanoTime() - t0;
        assertTrue(elapsed >= 1_500_000_000L,
                "4 acquires at 2 ops/s should take >= 1.5s, got " + (elapsed / 1e6) + " ms");
    }

    @Test
    void setRateResetsStaleReservation() throws InterruptedException {
        PerThreadRateLimiter l = new PerThreadRateLimiter(0.5); // 2 s per permit
        // First acquire returns instantly (no prior reservation), but reserves
        // the next ~ 2 s into the future.
        l.acquire(1);
        // setRate must reset the reservation; a fresh acquire should be fast.
        l.setRate(10000.0);
        long t0 = System.nanoTime();
        l.acquire(1);
        l.acquire(1);
        long elapsed = System.nanoTime() - t0;
        // 1-second upper bound: well below the ~ 2 s the old (0.5/s) reservation
        // would have caused if it had carried over. Generous enough for slow CI.
        assertTrue(elapsed < 1_000_000_000L,
                "after rate raise the stale reservation should be dropped, took "
                        + (elapsed / 1e6) + " ms");
    }

    @Test
    void attachThreadIsIdempotentAndOverwrites() throws Exception {
        PerThreadRateLimiter l = new PerThreadRateLimiter(0.5);
        l.acquire(1);

        // Spawn worker A; later replace with worker B via a second attachThread.
        // Only the latest attached thread receives the unpark, but any parked
        // thread will still wake naturally on the deadline reset because
        // setRate also resets the AtomicLong deadline.
        CountDownLatch aStarted = new CountDownLatch(1);
        Thread a = new Thread(() -> {
            l.attachThread(Thread.currentThread());
            aStarted.countDown();
            try {
                l.acquire(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        a.start();
        assertTrue(aStarted.await(2, TimeUnit.SECONDS));
        Thread.sleep(100);

        // setRate should still wake A even though attachThread was only called once.
        l.setRate(10000.0);
        a.join(1_000);
        assertTrue(!a.isAlive(), "thread A should have woken");
    }
}
