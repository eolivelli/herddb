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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Latency collector for the VectorBench client.
 *
 * <p>Backed by HdrHistogram (issue #443). The previous reservoir-sampling
 * implementation allocated an 800 KB {@code long[]} on every
 * {@link #computeStats()} call and sorted it ({@code O(n log n)}); when the
 * benchmark workers ran that path from their hot loops it dominated the CPU
 * profile and left no headroom for actual ingestion / query work.
 *
 * <p>HdrHistogram replaces both the reservoir and the sort:
 * <ul>
 *   <li>{@link #record(long)} is allocation-free and lock-free — it is a
 *       constant-time atomic increment on a single bucket counter.</li>
 *   <li>{@link #computeStats()} produces an immutable snapshot in
 *       {@code O(buckets)} time (a few microseconds) — no array copy, no
 *       sort, no boxing.</li>
 *   <li>Memory footprint is bounded (~24 KB) regardless of how many values
 *       were recorded.</li>
 * </ul>
 *
 * <p>A short TTL cache on {@link #computeStats()} lets concurrent status /
 * progress threads share one snapshot per refresh window so they don't all
 * pay the buffer-swap cost.
 */
public class MetricsCollector {

    /**
     * Histogram value range. Latencies below 1 ns and above 1 hour are
     * clamped to the bounds; in practice every commit / query latency is
     * well within this range, so clamping is a defensive guard rather than
     * an accuracy concern.
     */
    private static final long LOWEST_NANOS = 1L;
    private static final long HIGHEST_NANOS = TimeUnit.HOURS.toNanos(1);
    /** 3 significant digits → ±0.1% precision; ~24 KB per histogram. */
    private static final int SIGNIFICANT_DIGITS = 3;

    /**
     * Cache TTL for {@link #computeStats()}. Multiple status / progress
     * threads refresh on roughly 500 ms intervals, so a 200 ms cache means
     * concurrent callers within the same refresh tick reuse one snapshot.
     * Snapshots can be at most {@code CACHE_TTL_NANOS} stale — well under
     * the 500 ms display cadence — which is the trade-off for not paying
     * the buffer-swap cost on every progress tick.
     */
    private static final long CACHE_TTL_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

    /** Sentinel used to mark the cache as initially empty. */
    private static final Snapshot INVALID = new Snapshot(null, Long.MIN_VALUE);

    private final LongAdder count = new LongAdder();
    private final Recorder recorder = new Recorder(LOWEST_NANOS, HIGHEST_NANOS, SIGNIFICANT_DIGITS);
    /**
     * Accumulator merged with each {@link Recorder#getIntervalHistogram(Histogram)}
     * call so {@link #computeStats()} reports running totals rather than just
     * the most recent interval.
     */
    private final Histogram cumulative = new Histogram(LOWEST_NANOS, HIGHEST_NANOS, SIGNIFICANT_DIGITS);
    /** Reusable receiver for the recorder swap; {@code null} on first call. */
    private Histogram intervalRecycle;
    /** Guards {@link #cumulative} and {@link #intervalRecycle}. */
    private final Object snapshotLock = new Object();

    /** Nanoseconds of the most recently recorded event; 0 when none recorded yet. */
    private final AtomicLong lastNanos = new AtomicLong(0);

    /**
     * Cached snapshot. Reads via {@link AtomicReference#get()} are lock-free;
     * writes happen only under {@link #snapshotLock} so the visible cache is
     * monotone (a later lock holder has merged at least as much data as
     * earlier holders into {@link #cumulative}, so the count / max stored
     * here only ever grows).
     */
    private final AtomicReference<Snapshot> cached = new AtomicReference<>(INVALID);

    public void record(long nanos) {
        long clamped = nanos;
        if (clamped < LOWEST_NANOS) {
            clamped = LOWEST_NANOS;
        } else if (clamped > HIGHEST_NANOS) {
            clamped = HIGHEST_NANOS;
        }
        recorder.recordValue(clamped);
        count.increment();
        // Store the clamped value so getLastNanos() and the histogram
        // agree on what was recorded — otherwise record(0) would put
        // 1 ns in the histogram but expose 0 via getLastNanos(), which
        // is confusing and arguably wrong.
        lastNanos.set(clamped);
        // Deliberately do NOT invalidate the cache: under the workloads
        // this collector serves (continuous record() from many worker
        // threads), invalidating per-record would prevent the cache from
        // ever serving a hit. The TTL alone bounds staleness to
        // CACHE_TTL_NANOS, which is well below the 500 ms progress-display
        // cadence — fresh enough for the benchmark UI.
    }

    public long getCount() {
        return count.sum();
    }

    /** Nanoseconds of the most recently recorded event; 0 when none recorded yet. */
    public long getLastNanos() {
        return lastNanos.get();
    }

    public Stats computeStats() {
        Snapshot prev = cached.get();
        if (prev != INVALID && System.nanoTime() - prev.timestampNanos < CACHE_TTL_NANOS) {
            return prev.stats;
        }
        return computeStatsLocked(/* bypassCache */ false);
    }

    /**
     * Snapshot the current state without consulting (or honouring) the
     * TTL cache. Use this for canonical end-of-run summary lines where
     * stats up to {@link #CACHE_TTL_NANOS} stale would silently omit the
     * last batch of recorded latencies. The freshly-computed snapshot is
     * still installed in the cache so a subsequent live-UI {@link
     * #computeStats()} call does not have to redo the work.
     */
    public Stats computeStatsUncached() {
        return computeStatsLocked(/* bypassCache */ true);
    }

    private Stats computeStatsLocked(boolean bypassCache) {
        synchronized (snapshotLock) {
            // Re-check the cache under the lock: another caller may have
            // refreshed it while we were waiting. This avoids redundant
            // buffer swaps when many readers contend for the lock at once.
            // Bypassed when the caller explicitly requested a fresh snapshot
            // (final run summary) — see computeStatsUncached().
            if (!bypassCache) {
                Snapshot prev = cached.get();
                long checkNow = System.nanoTime();
                if (prev != INVALID && checkNow - prev.timestampNanos < CACHE_TTL_NANOS) {
                    return prev.stats;
                }
            }
            // Atomically swap the active recorder buffer and merge the
            // just-completed interval into the running cumulative histogram.
            // The recycled receiver avoids an allocation on every snapshot.
            intervalRecycle = recorder.getIntervalHistogram(intervalRecycle);
            cumulative.add(intervalRecycle);
            long total = cumulative.getTotalCount();
            Stats fresh;
            if (total == 0) {
                fresh = new Stats(0, 0, 0, 0, 0, 0);
            } else {
                fresh = new Stats(
                        total,
                        cumulative.getMean(),
                        cumulative.getValueAtPercentile(50.0),
                        cumulative.getValueAtPercentile(95.0),
                        cumulative.getValueAtPercentile(99.0),
                        cumulative.getMaxValue()
                );
            }
            // Capture the timestamp AFTER the buffer-swap+merge so the
            // cached entry's age reflects the data it actually contains
            // (not the moment we entered the lock — a few μs earlier in
            // contended cases).
            long now = System.nanoTime();
            // Cache write inside the lock: write order = lock acquisition
            // order = monotonically increasing cumulative state, so the
            // cached snapshot's count and max never go backwards.
            cached.set(new Snapshot(fresh, now));
            return fresh;
        }
    }

    public record Stats(long count, double meanNanos, long p50Nanos, long p95Nanos, long p99Nanos, long maxNanos) {

        public void print(String label) {
            if (count == 0) {
                System.out.println("=== " + label + " ===");
                System.out.println("No data recorded.");
                return;
            }
            System.out.println("=== " + label + " ===");
            System.out.printf("Count: %d%n", count);
            System.out.printf("Latency mean: %.2f ms | p50: %.2f ms | p95: %.2f ms | p99: %.2f ms | max: %.2f ms%n",
                    meanNanos / 1_000_000.0,
                    p50Nanos / 1_000_000.0,
                    p95Nanos / 1_000_000.0,
                    p99Nanos / 1_000_000.0,
                    maxNanos / 1_000_000.0);
        }
    }

    /** Cache slot — pairs a snapshot with the nanoTime it was produced at. */
    private static final class Snapshot {
        final Stats stats;
        final long timestampNanos;

        Snapshot(Stats stats, long timestampNanos) {
            this.stats = stats;
            this.timestampNanos = timestampNanos;
        }
    }
}
