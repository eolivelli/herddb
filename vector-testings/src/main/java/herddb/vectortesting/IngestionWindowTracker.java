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

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.HdrHistogram.Histogram;

/**
 * Tracks per-commit {@code (timestamp, rowCount, latencyNanos)} entries in a
 * sliding window of up to 5 minutes, enabling windowed ingestion-rate and
 * commit-latency statistics for the {@code GET /status} endpoint (issue #453).
 *
 * <h3>Motivation</h3>
 * <p>The all-time {@code ops_per_sec} field in {@code /status} is always a
 * lagging average: after a BookKeeper stall or a rate-limiter change the
 * global average climbs very slowly even if the current throughput has already
 * normalised. A 1-minute and a 5-minute windowed rate allow operators and the
 * supervision agent to detect recovery (or degradation) within one supervision
 * tick.
 *
 * <h3>Data model</h3>
 * <p>A bounded {@code ArrayDeque} holds one {@link Entry} per successful commit.
 * Entries older than {@link #MAX_WINDOW_NANOS} (5 minutes) are trimmed from
 * the head on every {@link #recordCommit} call, so the deque never accumulates
 * more than {@code ~commits/s × 300 s} entries — typically well under 1000
 * even at peak throughput.
 *
 * <h3>Thread safety</h3>
 * <p>Written concurrently by multiple ingest workers (one entry per commit),
 * read by the status supplier on each {@code /status} request. All access is
 * guarded by {@link #lock}. Commit frequency is at most a few hundred per
 * second, so contention is negligible.
 *
 * <h3>Windowed latency</h3>
 * <p>A transient {@link Histogram} is built from the deque entries inside the
 * window on every {@link #computeWindowedLatencyMap} call and discarded
 * immediately. This avoids the complexity of maintaining a persistent
 * per-window histogram while keeping the work proportional to the (small)
 * number of entries in the window.
 */
public class IngestionWindowTracker {

    /** Maximum window kept in the deque — also the 5-minute window duration. */
    public static final long MAX_WINDOW_NANOS = TimeUnit.MINUTES.toNanos(5);

    /** 5-minute rate window. */
    public static final long FIVE_MIN_NANOS = MAX_WINDOW_NANOS;

    /** 1-minute rate window. */
    public static final long ONE_MIN_NANOS = TimeUnit.MINUTES.toNanos(1);

    private static final long LOWEST_NANOS = 1L;
    private static final long HIGHEST_NANOS = TimeUnit.HOURS.toNanos(1);
    private static final int SIGNIFICANT_DIGITS = 3;

    /** Immutable carrier for one commit's recorded data. */
    private static final class Entry {
        final long timestampNanos;
        final long rowCount;
        final long latencyNanos;

        Entry(long timestampNanos, long rowCount, long latencyNanos) {
            this.timestampNanos = timestampNanos;
            this.rowCount = rowCount;
            this.latencyNanos = latencyNanos;
        }
    }

    private final ArrayDeque<Entry> window = new ArrayDeque<>();
    private final Object lock = new Object();
    private final LongSupplier clock;

    /**
     * Constructs a tracker backed by {@link System#nanoTime()}.
     * This is the production constructor.
     */
    public IngestionWindowTracker() {
        this(System::nanoTime);
    }

    /**
     * Constructs a tracker with an injectable clock. Package-private so unit
     * tests can drive time deterministically without sleeping.
     *
     * @param clock supplier of the current time in nanoseconds (monotonic)
     */
    IngestionWindowTracker(LongSupplier clock) {
        this.clock = clock;
    }

    /**
     * Records a successful commit. Called by each ingest worker after its
     * transaction is committed.
     *
     * <p>Also trims entries that have fallen outside the maximum window
     * ({@link #MAX_WINDOW_NANOS}) to keep memory bounded.
     *
     * @param latencyNanos wall-clock nanoseconds from transaction start to
     *                     commit acknowledgement (clamped to the histogram
     *                     range on read)
     * @param rows         number of rows committed in this transaction
     */
    public void recordCommit(long latencyNanos, long rows) {
        synchronized (lock) {
            // Capture the clock inside the lock so the deque stays monotonically
            // ordered by timestamp — preventing the race where two threads capture
            // their timestamps before either enters the lock and then insert in
            // reverse order (which would break the trim loop's head-removal
            // assumption for the rare case of a stale entry at the tail).
            long now = clock.getAsLong();
            window.addLast(new Entry(now, rows, latencyNanos));
            // Trim entries that are outside the 5-minute retention window.
            // This runs on every write so the deque is bounded even if /status
            // is never queried.
            long cutoff = now - MAX_WINDOW_NANOS;
            while (!window.isEmpty() && window.peekFirst().timestampNanos < cutoff) {
                window.removeFirst();
            }
        }
    }

    /**
     * Computes the average ingestion rate (rows/s) over the last
     * {@code windowNanos} nanoseconds.
     *
     * <p>The denominator is clamped to the actual elapsed time since
     * {@code ingestStartNanos} when the run is shorter than the requested
     * window, so the rate is well-defined from the very first commit and
     * does not inflate during the ramp-up period.
     *
     * @param windowNanos      window duration in nanoseconds (e.g.
     *                         {@link #ONE_MIN_NANOS}, {@link #FIVE_MIN_NANOS})
     * @param ingestStartNanos {@link System#nanoTime()} value at the start of
     *                         the ingestion phase — used to clamp the
     *                         denominator for young runs
     * @return rows per second, or {@code 0.0} if no commits have been
     *         recorded within the window
     */
    public double computeWindowedRate(long windowNanos, long ingestStartNanos) {
        long now = clock.getAsLong();
        long cutoff = now - windowNanos;
        long totalRows = 0;
        synchronized (lock) {
            for (Entry e : window) {
                if (e.timestampNanos >= cutoff) {
                    totalRows += e.rowCount;
                }
            }
        }
        if (totalRows == 0) {
            return 0.0;
        }
        // Use the larger of cutoff and ingestStart as the window's effective
        // start: when the run is shorter than the window, ingestStart > cutoff
        // and the denominator is the actual elapsed time; when the run is
        // longer, ingestStart <= cutoff and the denominator is windowNanos.
        long windowStartNanos = Math.max(cutoff, ingestStartNanos);
        double windowSecs = (now - windowStartNanos) / 1e9;
        return windowSecs > 0 ? totalRows / windowSecs : 0.0;
    }

    /**
     * Computes commit-latency statistics (mean, p50, p99, max in milliseconds)
     * for commits that occurred within the last {@code windowNanos}.
     *
     * <p>Builds a transient {@link Histogram} from the deque entries that fall
     * inside the window and discards it after the call — no persistent state.
     * Latency values outside {@code [1 ns, 1 h]} are clamped to the histogram
     * bounds, which is consistent with {@link MetricsCollector#record(long)}.
     *
     * @param windowNanos window duration in nanoseconds
     * @return {@link LinkedHashMap} with keys {@code mean_ms}, {@code p50_ms},
     *         {@code p99_ms}, {@code max_ms}; all values are {@code 0.0} when
     *         no commits have been recorded within the window
     */
    public Map<String, Object> computeWindowedLatencyMap(long windowNanos) {
        long now = clock.getAsLong();
        long cutoff = now - windowNanos;
        Histogram h = new Histogram(LOWEST_NANOS, HIGHEST_NANOS, SIGNIFICANT_DIGITS);
        synchronized (lock) {
            for (Entry e : window) {
                if (e.timestampNanos >= cutoff) {
                    long clamped = e.latencyNanos;
                    if (clamped < LOWEST_NANOS) {
                        clamped = LOWEST_NANOS;
                    } else if (clamped > HIGHEST_NANOS) {
                        clamped = HIGHEST_NANOS;
                    }
                    h.recordValue(clamped);
                }
            }
        }
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        if (h.getTotalCount() == 0) {
            m.put("mean_ms", 0.0);
            m.put("p50_ms", 0.0);
            m.put("p99_ms", 0.0);
            m.put("max_ms", 0.0);
        } else {
            m.put("mean_ms", round2(h.getMean() / 1e6));
            m.put("p50_ms", round2(h.getValueAtPercentile(50.0) / 1e6));
            m.put("p99_ms", round2(h.getValueAtPercentile(99.0) / 1e6));
            m.put("max_ms", round2(h.getMaxValue() / 1e6));
        }
        return m;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
