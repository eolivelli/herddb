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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Single-threaded rate limiter built around {@link LockSupport#parkNanos}.
 *
 * <p>One instance is owned per ingestion worker (see
 * {@link IngestRateLimiterGroup}). Splitting the global rate across N
 * per-thread limiters removes the inter-thread serialisation that the
 * shared Guava {@code RateLimiter} caused (issue #402, bug 1).
 *
 * <p>The deadline-based design also fixes #402 bug 2: when the admin API
 * calls {@link #setRate(double)}, the limiter resets the deadline to "now"
 * (drops the old reservation) and {@code unpark}s the registered worker
 * thread. The thread re-evaluates its sleep against the new rate
 * <em>immediately</em>, instead of finishing its old sleep computed at the
 * old rate.
 *
 * <p>This class is intended to be called by a single owner thread (the
 * worker registered via {@link #attachThread(Thread)}). The deadline is
 * advanced via {@link AtomicLong#compareAndSet} for defensive thread-safety
 * but the design assumes only the owner ever calls {@link #acquire(int)}.
 *
 * <p>{@code 0} permits/s maps to {@link #UNLIMITED_RATE} so {@code acquire}
 * becomes effectively a no-op while the limiter remains live for later
 * admin overrides.
 */
public final class PerThreadRateLimiter {

    /**
     * Effectively-unlimited rate, mirroring {@link BenchRuntime#UNLIMITED_RATE}.
     * One billion permits/s yields a 1-ns delay per permit, which is below the
     * resolution of {@link System#nanoTime} on every supported JVM.
     */
    public static final double UNLIMITED_RATE = 1_000_000_000.0;

    /** Cached nanos-per-permit; {@code volatile} so {@link #setRate} is visible. */
    private volatile double nanosPerPermit;

    /**
     * Earliest {@link System#nanoTime} value at which the next permit becomes
     * available. {@code AtomicLong} so concurrent callers (defensive) cannot
     * race; in normal use only one thread (the owner) ever advances it.
     */
    private final AtomicLong deadlineNanos = new AtomicLong(0L);

    /**
     * The thread registered to this limiter. {@link #setRate} unparks this
     * thread so a sleeping owner re-evaluates its deadline against the new
     * rate immediately. {@code volatile} so a concurrent admin call sees
     * the latest reference.
     */
    private volatile Thread parkedThread;

    public PerThreadRateLimiter(double permitsPerSecond) {
        setRate(permitsPerSecond);
    }

    /**
     * Registers the worker thread that calls {@link #acquire(int)} on this
     * limiter. {@link #setRate} unparks this thread on rate change. Calling
     * twice is fine — the latest registration wins.
     */
    public void attachThread(Thread t) {
        this.parkedThread = t;
    }

    /**
     * Returns the configured rate in permits per second. Reads
     * {@code nanosPerPermit} once and converts; if the rate is at or above
     * {@link #UNLIMITED_RATE} the function returns exactly that constant
     * (avoids small floating-point drift in the round-trip through nanos).
     */
    public double getRate() {
        double n = nanosPerPermit;
        if (n <= 1.0) {
            // 1 ns per permit means we treated this as "unlimited".
            return UNLIMITED_RATE;
        }
        return 1_000_000_000.0 / n;
    }

    /**
     * Updates the rate. {@code 0} or negative is mapped to
     * {@link #UNLIMITED_RATE}. The deadline is reset to "now" so any
     * stale reservation from the previous rate is dropped — workers that
     * were parked for a long sleep at the old rate will return promptly
     * and use the new rate on their next call.
     *
     * <p>Also unparks the registered worker thread (if any) so an owner
     * already inside {@link #acquire(int)} immediately re-evaluates its
     * sleep against the new rate.
     */
    public void setRate(double permitsPerSecond) {
        double effective = (permitsPerSecond > 0.0) ? permitsPerSecond : UNLIMITED_RATE;
        if (effective > UNLIMITED_RATE) {
            effective = UNLIMITED_RATE;
        }
        this.nanosPerPermit = 1_000_000_000.0 / effective;
        // Drop any stale future reservation: parked workers re-anchor on "now".
        this.deadlineNanos.set(System.nanoTime());
        Thread t = this.parkedThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * Block the calling thread until {@code permits} can be acquired at the
     * current rate. {@code permits == 0} is a no-op. Re-reads the rate inside
     * the park loop so an admin-issued rate change wakes the call promptly.
     *
     * <p>Unlike Guava's {@code acquire}, this method respects thread
     * interruption: if the thread is interrupted while parked, it raises
     * {@link InterruptedException}. Note that the deadline reservation
     * already advanced by this acquire is <em>not</em> rolled back — in
     * the bench an interrupt always means shutdown, so the abandoned
     * reservation is moot. The next caller (if any) of the same limiter
     * will see the abandoned slot via the deadline and park accordingly,
     * which is harmless.
     *
     * @param permits number of permits to acquire; must be {@code >= 0}
     * @throws InterruptedException if the thread is interrupted while parked
     */
    public void acquire(int permits) throws InterruptedException {
        if (permits <= 0) {
            return;
        }
        while (true) {
            long now = System.nanoTime();
            long perPermit = (long) Math.ceil(nanosPerPermit);
            // Reserve our slot atomically: max(now, deadline) + permits * perPermit.
            long current = deadlineNanos.get();
            long anchor = Math.max(now, current);
            long target = anchor + (long) permits * perPermit;
            if (!deadlineNanos.compareAndSet(current, target)) {
                // Another (defensive) caller advanced the deadline; retry.
                continue;
            }
            long sleepNanos = anchor - now;
            if (sleepNanos <= 0L) {
                return;
            }
            // Park; re-check on wake-up so a setRate() that resets the
            // deadline causes us to recompute and (typically) return.
            long parkUntil = now + sleepNanos;
            while (true) {
                long remaining = parkUntil - System.nanoTime();
                if (remaining <= 0L) {
                    return;
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                LockSupport.parkNanos(this, remaining);
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                // setRate may have reset the deadline below our reservation.
                // If our reserved slot is now in the past relative to the
                // (reset) deadline, just return — the rate change has freed us.
                long currentDeadline = deadlineNanos.get();
                if (currentDeadline < target) {
                    // Deadline was reset by setRate; the reservation is moot.
                    return;
                }
            }
        }
    }
}
