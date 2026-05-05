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

import java.util.ArrayList;
import java.util.List;

/**
 * Holds one {@link PerThreadRateLimiter} per ingestion worker, distributing
 * the global ingest-max-ops rate across the live workers.
 *
 * <p>Issue #402: a single shared {@code RateLimiter} forced N threads to
 * serialise on a per-commit acquire, capping aggregate throughput well below
 * the configured rate. Splitting the rate evenly across N per-thread limiters
 * removes the contention while preserving the global cap (per-thread rate ×
 * N == total).
 *
 * <p>The group is the single point of truth for the effective rate, so
 * {@link BenchRuntime#setIngestMaxOps(int)} and
 * {@link BenchRuntime#setIngestThreads(int)} both flow through here.
 *
 * <p>Thread-safety: {@link #setEffectiveRate(double)} and {@link #resize(int)}
 * are synchronised on this instance because they walk the list of children
 * to publish a new rate atomically. {@link #acquire(int, int)} reads the
 * (volatile) child reference unsynchronised — workers attach once at startup
 * and the list is only ever grown, never reordered, so a stale read of the
 * size still yields a correct child for any in-range index.
 */
public final class IngestRateLimiterGroup {

    /**
     * Mirrors {@link BenchRuntime#UNLIMITED_RATE}; kept here so the group
     * can be instantiated without referencing {@code BenchRuntime} (which
     * helps unit tests that exercise the group in isolation).
     */
    public static final double UNLIMITED_RATE = PerThreadRateLimiter.UNLIMITED_RATE;

    private final List<PerThreadRateLimiter> children = new ArrayList<>();

    /**
     * Last total rate set via {@link #setEffectiveRate(double)}. Used to
     * recompute the per-child share when {@link #resize(int)} changes the
     * worker count.
     */
    private volatile double currentTotalRate;

    public IngestRateLimiterGroup(int initialSize, double totalRate) {
        if (initialSize <= 0) {
            throw new IllegalArgumentException("initialSize must be > 0, got " + initialSize);
        }
        synchronized (this) {
            this.currentTotalRate = totalRate;
            double share = perChildRate(totalRate, initialSize);
            for (int i = 0; i < initialSize; i++) {
                children.add(new PerThreadRateLimiter(share));
            }
        }
    }

    /** Number of per-thread limiters currently registered. */
    public synchronized int size() {
        return children.size();
    }

    /**
     * Returns the per-thread limiter for {@code index}. Used by
     * {@link IngestionWorker} to {@code acquire} from its own slot. The
     * caller is expected to use a fixed index for the lifetime of the
     * worker.
     */
    public synchronized PerThreadRateLimiter limiterFor(int index) {
        if (index < 0 || index >= children.size()) {
            throw new IndexOutOfBoundsException(
                    "index " + index + " out of range [0, " + children.size() + ")");
        }
        return children.get(index);
    }

    /**
     * Convenience: acquire {@code permits} from the per-thread limiter at
     * {@code index}. Equivalent to {@code limiterFor(index).acquire(permits)}
     * but tolerates an out-of-range index by returning immediately (defensive
     * during a {@link #resize(int)} race).
     */
    public void acquire(int index, int permits) throws InterruptedException {
        PerThreadRateLimiter limiter;
        synchronized (this) {
            if (index < 0 || index >= children.size()) {
                return;
            }
            limiter = children.get(index);
        }
        limiter.acquire(permits);
    }

    /**
     * Attach the calling worker thread to its slot so admin-issued rate
     * changes can unpark it. Must be called by every worker on entry to
     * its run-loop.
     */
    public synchronized void attachThread(int index, Thread t) {
        if (index < 0 || index >= children.size()) {
            return;
        }
        children.get(index).attachThread(t);
    }

    /**
     * Push a new total rate into every child. {@code 0} (or negative) means
     * unlimited; the per-child rate is still {@code UNLIMITED_RATE} so the
     * arithmetic stays consistent.
     */
    public synchronized void setEffectiveRate(double totalRate) {
        this.currentTotalRate = totalRate;
        double share = perChildRate(totalRate, children.size());
        for (PerThreadRateLimiter c : children) {
            c.setRate(share);
        }
    }

    /**
     * Returns the last total rate set via {@link #setEffectiveRate(double)}.
     * Used by {@link BenchRuntime} to expose the configured rate.
     *
     * <p>Synchronised on this instance to pair with the synchronised setters
     * (SpotBugs {@code UG_SYNC_SET_UNSYNC_GET}). The {@code volatile} backing
     * field would already give us safe publication, but the explicit lock
     * keeps the read consistent with concurrent {@link #resize(int)} and
     * {@link #setEffectiveRate(double)} calls.
     */
    public synchronized double getEffectiveRate() {
        return currentTotalRate;
    }

    /**
     * Resize the group to {@code newSize} children, redistributing the
     * configured rate so each child gets {@code currentTotalRate / newSize}.
     * Existing children retain their identity (so attached threads stay
     * attached).
     *
     * <p>Growing appends new {@link PerThreadRateLimiter} instances at the
     * end. Shrinking truncates the tail and rebalances the remaining
     * children to the new (larger) per-child share. Truncated workers are
     * expected to have already exited (poison-pill semantics in
     * {@link BenchRuntime#setIngestThreads(int)}).
     */
    public synchronized void resize(int newSize) {
        if (newSize <= 0) {
            throw new IllegalArgumentException("newSize must be > 0, got " + newSize);
        }
        double share = perChildRate(currentTotalRate, newSize);
        if (newSize >= children.size()) {
            // Grow or no-op: rebalance existing children, then append new ones.
            for (PerThreadRateLimiter c : children) {
                c.setRate(share);
            }
            for (int i = children.size(); i < newSize; i++) {
                children.add(new PerThreadRateLimiter(share));
            }
        } else {
            // Shrink: rebalance the surviving children to the new share, then
            // drop the tail. Wake any thread parked in a truncated child so it
            // doesn't sit on a stale deadline (the worker should have exited
            // via the poison-pill path before we got here, but defence-in-depth).
            for (int i = newSize; i < children.size(); i++) {
                children.get(i).setRate(share);
            }
            children.subList(newSize, children.size()).clear();
            for (PerThreadRateLimiter c : children) {
                c.setRate(share);
            }
        }
    }

    private static double perChildRate(double totalRate, int size) {
        if (totalRate <= 0.0) {
            return UNLIMITED_RATE;
        }
        if (size <= 0) {
            return UNLIMITED_RATE;
        }
        return totalRate / size;
    }
}
