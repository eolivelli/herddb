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

import herddb.jdbc.PreparedStatementAsync;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IngestionWorker implements Runnable {

    /** Row held in memory until its batch is successfully committed. */
    static final class PendingRow {
        final long id;
        final float[] vec;

        PendingRow(long id, float[] vec) {
            this.id = id;
            this.vec = vec;
        }
    }

    private final Config config;
    private final BlockingQueue<float[]> queue;
    private final AtomicBoolean done;
    private final AtomicLong rowId;
    private final MetricsCollector metrics;
    private final AtomicReference<String> statusLine;
    private final long ingestStartNanos;
    private final AtomicLong commitsTotal;
    private final AtomicLong commitsRecovered;
    private final AtomicLong rowsCommitted;
    /**
     * Per-thread rate limiter group (issue #402). May be {@code null} in unit
     * tests that exercise flush/retry paths without rate limiting.
     */
    private final IngestRateLimiterGroup rateLimiterGroup;
    /**
     * Index into the rate-limiter group that this worker uses. Stays fixed for
     * the lifetime of the worker; new workers spawned by {@code setIngestThreads}
     * are assigned a fresh index.
     */
    private final int rateLimiterIndex;
    /**
     * Optional runtime reference for tracking the live worker count.
     * When non-null, {@link BenchRuntime#activeIngestWorkers} is incremented
     * at worker start and decremented at worker exit (in a {@code finally}
     * block). May be {@code null} in unit tests that construct the worker
     * directly without a full {@link BenchRuntime}.
     */
    private final BenchRuntime runtime;

    /**
     * Optional sliding-window tracker for issue #453 (windowed ingestion rate
     * and commit latency in {@code GET /status}). Receives one
     * {@link IngestionWindowTracker#recordCommit} call after every successful
     * commit. May be {@code null} in unit tests that do not exercise the
     * windowed-rate feature.
     */
    private final IngestionWindowTracker windowTracker;

    /**
     * Base of the exponential back-off between commit retries, in milliseconds.
     * Defaults to 10 seconds per issue #153 (10s, 20s, 40s, ...); tests override
     * this to a tiny value to keep the suite fast.
     */
    long backoffBaseMillis = 10_000L;

    /**
     * Constructs an {@code IngestionWorker} without a {@link BenchRuntime}
     * reference or window tracker. Used in unit tests that do not need live
     * thread-count tracking or windowed rate reporting.
     */
    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId,
                           MetricsCollector metrics, AtomicReference<String> statusLine, long ingestStartNanos,
                           AtomicLong commitsTotal, AtomicLong commitsRecovered, AtomicLong rowsCommitted,
                           IngestRateLimiterGroup rateLimiterGroup, int rateLimiterIndex) {
        this(config, queue, done, rowId, metrics, statusLine, ingestStartNanos,
                commitsTotal, commitsRecovered, rowsCommitted,
                rateLimiterGroup, rateLimiterIndex, null, null);
    }

    /**
     * Convenience constructor used by unit tests that don't exercise the rate
     * limiter at all (flush / retry tests). Equivalent to passing
     * {@code rateLimiterGroup == null} and {@code windowTracker == null}.
     */
    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId,
                           MetricsCollector metrics, AtomicReference<String> statusLine, long ingestStartNanos,
                           AtomicLong commitsTotal, AtomicLong commitsRecovered, AtomicLong rowsCommitted) {
        this(config, queue, done, rowId, metrics, statusLine, ingestStartNanos,
                commitsTotal, commitsRecovered, rowsCommitted, null, 0, null, null);
    }

    /**
     * Constructs an {@code IngestionWorker} with an optional {@link BenchRuntime}
     * for live thread-count tracking and an optional {@link IngestionWindowTracker}
     * for windowed rate reporting (issue #453). Pass {@code null} for either when
     * constructing workers in unit tests that do not need those features.
     */
    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId,
                           MetricsCollector metrics, AtomicReference<String> statusLine, long ingestStartNanos,
                           AtomicLong commitsTotal, AtomicLong commitsRecovered, AtomicLong rowsCommitted,
                           IngestRateLimiterGroup rateLimiterGroup, int rateLimiterIndex,
                           BenchRuntime runtime, IngestionWindowTracker windowTracker) {
        this.config = config;
        this.queue = queue;
        this.done = done;
        this.rowId = rowId;
        this.metrics = metrics;
        this.statusLine = statusLine;
        this.ingestStartNanos = ingestStartNanos;
        this.commitsTotal = commitsTotal;
        this.commitsRecovered = commitsRecovered;
        this.rowsCommitted = rowsCommitted;
        this.rateLimiterGroup = rateLimiterGroup;
        this.rateLimiterIndex = rateLimiterIndex;
        this.runtime = runtime;
        this.windowTracker = windowTracker;
    }

    /** Package-private so {@link VectorBench}'s progress thread can format ETA consistently. */
    static String formatEta(double seconds) {
        if (seconds <= 0) {
            return "N/A";
        }
        long s = (long) seconds;
        long h = s / 3600;
        long m = (s % 3600) / 60;
        long sec = s % 60;
        if (h > 0) {
            return String.format("%dh%02dm%02ds", h, m, sec);
        }
        if (m > 0) {
            return String.format("%dm%02ds", m, sec);
        }
        return String.format("%ds", sec);
    }

    /**
     * Marker exception used by {@link #flushBatch} to signal that the server
     * acknowledged the batch but reported fewer inserted rows than were
     * submitted. Re-attempting the same batch would produce primary-key
     * collisions on the rows that did succeed, so {@link #commitWithRetry}
     * surfaces this immediately instead of looping.
     *
     * <p>Issue #251: a 100 M ingest finished with the bench's row counter at
     * exactly 100 M but {@code COUNT(*)} returning 99,999,991. The previous
     * code bumped {@code rowsCommitted} by {@code batch.size()} as soon as
     * {@code conn.commit()} returned, without verifying that
     * {@code executeBatchAsync()} actually reported one insert per submitted
     * row. This exception is the bench's loud failure mode for that scenario.
     */
    static final class BatchPartialSuccessException extends SQLException {
        private static final long serialVersionUID = 1L;

        BatchPartialSuccessException(String message) {
            super(message);
        }
    }

    /**
     * Flush an accumulated batch via {@code executeBatchAsync().get()} but do
     * <em>not</em> commit. Used by the multi-flush transaction loop to push
     * a sub-batch to the server before more rows are added in the same
     * transaction. Returns the number of nanoseconds spent in the async
     * batch + the per-flush partial-success guard.
     *
     * @param ps prepared statement with the accumulated sub-batch
     * @param expectedRows the number of rows in this sub-batch — must equal
     *        the sum of the per-row update counts returned by the server,
     *        otherwise the server silently dropped some inserts and we throw
     *        {@link BatchPartialSuccessException}.
     * @param batchStartNanos nanosecond timestamp when the sub-batch started
     * @param conn connection — used only to roll back on a partial-success
     *        violation; never committed by this method
     * @return latency of the executeBatch call in nanoseconds
     * @throws SQLException on a partial-success violation
     * @throws ExecutionException if the async batch itself failed
     * @throws InterruptedException if the async batch wait was interrupted
     */
    private long flushSubBatchNoCommit(PreparedStatement ps, Connection conn,
                                       int expectedRows, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        int[] updateCounts = ps.unwrap(PreparedStatementAsync.class).executeBatchAsync().get();
        long inserted = 0;
        for (int n : updateCounts) {
            inserted += (n == java.sql.Statement.SUCCESS_NO_INFO) ? 1L : Math.max(0, n);
        }
        if (inserted != expectedRows) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
                // best-effort; the BatchPartialSuccessException carries the real signal
            }
            throw new BatchPartialSuccessException(String.format(
                    "Server confirmed %d of %d row inserts in batch (updateCounts=%s)"
                            + " — silent partial success, refusing to over-count rowsCommitted",
                    inserted, expectedRows, java.util.Arrays.toString(updateCounts)));
        }
        return System.nanoTime() - batchStartNanos;
    }

    /**
     * Flush accumulated batch using {@code executeBatchAsync().get()} and commit.
     * Equivalent to one sub-batch flush followed by a commit — used when
     * {@code transactionSize == batchSize} (the legacy path) and for the final
     * flush of a multi-flush transaction.
     *
     * @param ps prepared statement with accumulated batch
     * @param conn connection for commit
     * @param expectedRows the number of rows in the batch — must equal the sum
     *        of the per-row update counts returned by the server, otherwise the
     *        server silently dropped some inserts and we throw
     *        {@link BatchPartialSuccessException}.
     * @param batchStartNanos nanosecond timestamp when batch started
     * @return latency in nanoseconds (batch + commit)
     * @throws SQLException if batch execution or commit fails, or if the server
     *         reports a partial-success batch ({@link BatchPartialSuccessException})
     * @throws ExecutionException if async batch execution fails
     * @throws InterruptedException if async batch execution is interrupted
     */
    private long flushBatch(PreparedStatement ps, Connection conn, int expectedRows, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        flushSubBatchNoCommit(ps, conn, expectedRows, batchStartNanos);
        conn.commit();
        return System.nanoTime() - batchStartNanos;
    }

    /**
     * Package-private method for testing flushBatch behavior.
     */
    long testFlushBatch(PreparedStatement ps, Connection conn, int expectedRows, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        return flushBatch(ps, conn, expectedRows, batchStartNanos);
    }

    /**
     * Commit the accumulated batch, retrying on transient {@link SQLException} or
     * {@link ExecutionException}. On each failure the connection is rolled back,
     * the prepared statement's batch is cleared, and the in-memory batch is
     * replayed into the statement before the next attempt. Back-off is
     * exponential: {@code backoffBaseMillis * 2^attempt} (10s, 20s, 40s...).
     *
     * <p>{@link InterruptedException} is never retried — it surfaces immediately
     * so pool shutdowns terminate the worker promptly.
     *
     * @return the latency (batch + commit) of the successful attempt, in nanos
     */
    long commitWithRetry(PreparedStatement ps, Connection conn, List<PendingRow> batch, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        int maxRetries = Math.max(0, config.ingestCommitRetries);
        Exception lastError = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                long latencyNanos = flushBatch(ps, conn, batch.size(), batchStartNanos);
                commitsTotal.incrementAndGet();
                rowsCommitted.addAndGet(batch.size());
                if (attempt > 0) {
                    commitsRecovered.incrementAndGet();
                }
                return latencyNanos;
            } catch (BatchPartialSuccessException e) {
                // Don't retry — replaying the batch would PK-collide on the rows
                // the server already accepted. Surface immediately so the bench
                // fails fast with the diagnostic from flushBatch.
                safelyClearBatch(ps);
                throw e;
            } catch (SQLException | ExecutionException e) {
                lastError = e;
                safelyRollback(conn);
                safelyClearBatch(ps);
                if (attempt == maxRetries) {
                    break;
                }
                long backoffMs = backoffBaseMillis * (1L << attempt);
                statusLine.set(String.format(
                        "commit failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1, maxRetries + 1, backoffMs / 1000,
                        e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
                System.err.printf("Commit attempt %d/%d failed, retrying in %d ms: %s%n",
                        attempt + 1, maxRetries + 1, backoffMs, e);
                Thread.sleep(backoffMs);
                replayBatch(ps, batch);
            }
        }
        if (lastError instanceof SQLException) {
            throw (SQLException) lastError;
        }
        throw (ExecutionException) lastError;
    }

    /**
     * Commit a multi-flush transaction with retries. The transaction has
     * already had zero or more sub-batches flushed via
     * {@link #flushSubBatchNoCommit}; the final flush + commit happens here,
     * and on retryable failure the entire transaction is replayed.
     *
     * <p>The cumulative per-flush partial-success guards run inside
     * {@link #flushSubBatchNoCommit}. This method enforces the global
     * accounting (rowsCommitted, commitsTotal, commitsRecovered) once per
     * successful transaction, not per sub-flush.
     *
     * @param ps prepared statement
     * @param conn JDBC connection
     * @param fullTransactionRows all rows accumulated in the current
     *        transaction (across all sub-batches), used for retry replay
     * @param finalSubBatchRows the rows in the final sub-batch (those just
     *        added since the last sub-flush)
     * @param transactionStartNanos nanosecond timestamp when the transaction
     *        started; the returned latency is measured from this point
     * @return the latency of the successful transaction, in nanos
     */
    long commitTransactionWithRetry(PreparedStatement ps, Connection conn,
                                    List<PendingRow> fullTransactionRows,
                                    int finalSubBatchRows,
                                    long transactionStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        int maxRetries = Math.max(0, config.ingestCommitRetries);
        Exception lastError = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                // First attempt: only the final (un-flushed) sub-batch is in the
                // PS. On retry, the PS has been cleared and the entire
                // transaction's rows have been replayed, so this single flush
                // sees the full transaction.
                //
                // Edge case: when the transaction's last row landed exactly on a
                // sub-flush boundary, the PS is empty at this point — skip the
                // no-op executeBatch and go straight to commit. This keeps the
                // commit_size = transaction_size invariant intact for the
                // not-a-multiple case (e.g. 1000 rows split as 250+250 with no
                // trailing remainder beyond the last sub-flush).
                int rowsInPs = (attempt == 0) ? finalSubBatchRows : fullTransactionRows.size();
                if (rowsInPs > 0) {
                    flushSubBatchNoCommit(ps, conn, rowsInPs, transactionStartNanos);
                }
                conn.commit();
                long latencyNanos = System.nanoTime() - transactionStartNanos;
                commitsTotal.incrementAndGet();
                rowsCommitted.addAndGet(fullTransactionRows.size());
                if (attempt > 0) {
                    commitsRecovered.incrementAndGet();
                }
                return latencyNanos;
            } catch (BatchPartialSuccessException e) {
                safelyClearBatch(ps);
                throw e;
            } catch (SQLException | ExecutionException e) {
                lastError = e;
                safelyRollback(conn);
                safelyClearBatch(ps);
                if (attempt == maxRetries) {
                    break;
                }
                long backoffMs = backoffBaseMillis * (1L << attempt);
                statusLine.set(String.format(
                        "commit failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1, maxRetries + 1, backoffMs / 1000,
                        e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
                System.err.printf("Commit attempt %d/%d failed, retrying in %d ms: %s%n",
                        attempt + 1, maxRetries + 1, backoffMs, e);
                Thread.sleep(backoffMs);
                // Replay the full transaction (all sub-batches together).
                replayBatch(ps, fullTransactionRows);
            }
        }
        if (lastError instanceof SQLException) {
            throw (SQLException) lastError;
        }
        throw (ExecutionException) lastError;
    }

    private static void replayBatch(PreparedStatement ps, List<PendingRow> batch) throws SQLException {
        for (PendingRow row : batch) {
            ps.setLong(1, row.id);
            ps.setObject(2, row.vec);
            ps.addBatch();
        }
    }

    private static void safelyRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException rollbackError) {
            System.err.println("Rollback failed during retry: " + rollbackError.getMessage());
        }
    }

    private static void safelyClearBatch(PreparedStatement ps) {
        try {
            ps.clearBatch();
        } catch (SQLException clearError) {
            System.err.println("clearBatch failed during retry: " + clearError.getMessage());
        }
    }

    @Override
    public void run() {
        if (runtime != null) {
            runtime.activeIngestWorkers.incrementAndGet();
        }
        // Attach this thread to its per-thread limiter so an admin-issued rate
        // change can unpark it (issue #402).
        if (rateLimiterGroup != null) {
            rateLimiterGroup.attachThread(rateLimiterIndex, Thread.currentThread());
        }
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                runIngestLoop(conn, ps);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Ingestion worker interrupted", e);
        } catch (SQLException | ExecutionException e) {
            // Propagate: awaitTermination ignores task exceptions, so swallowing here
            // would silently drop the uncommitted pendingBatch.
            throw new RuntimeException("Ingestion worker failed", e);
        } finally {
            if (runtime != null) {
                runtime.activeIngestWorkers.decrementAndGet();
            }
        }
    }

    /**
     * Main ingest loop. Visible for testing so unit tests can drive it with
     * a stubbed {@link Connection}/{@link PreparedStatement} without going
     * through {@code DriverManager}.
     *
     * <p>Splits ingestion into <em>transactions</em> (commit unit) of size
     * {@link Config#effectiveTransactionSize()}, each composed of one or more
     * <em>sub-batches</em> of size {@link Config#batchSize}. The last sub-batch
     * in a transaction handles the remainder when transaction-size is not a
     * multiple of batch-size.
     */
    void runIngestLoop(Connection conn, PreparedStatement ps)
            throws SQLException, ExecutionException, InterruptedException {
        // pendingBatch holds the entire current transaction's worth of rows
        // so commitTransactionWithRetry can replay them on a transient failure.
        List<PendingRow> pendingBatch = new ArrayList<>(Math.max(1, config.batchSize));
        // Number of rows added to ps since the last sub-flush (i.e. rows in the
        // PS that have not yet hit executeBatch).
        int rowsInCurrentSubBatch = 0;
        long lastCommitTime = System.currentTimeMillis();
        final long maxCommitIntervalMs = 30_000; // 30 seconds
        long transactionStartNanos = 0;
        long rowsIngested = 0;

        // Pre-commit acquire: pace entry into the next transaction (issue #402,
        // proposed fix B). We acquire the *upcoming* transaction's permits up
        // front so that workers spread out at transaction boundaries; their
        // commits then overlap during the commit_ms window instead of
        // serialising on a shared post-commit acquire.
        preAcquireForNextTransaction();

        while (true) {
            float[] vec = queue.poll(100, TimeUnit.MILLISECONDS);
            if (vec == null) {
                if (done.get() && queue.isEmpty()) {
                    break;
                } else {
                    continue;
                }
            }
            if (vec.length == 0) {
                break; // poison pill
            }
            long id = rowId.getAndIncrement();
            if (pendingBatch.isEmpty()) {
                transactionStartNanos = System.nanoTime();
            }
            ps.setLong(1, id);
            ps.setObject(2, vec);
            ps.addBatch();
            pendingBatch.add(new PendingRow(id, vec));
            rowsInCurrentSubBatch++;

            int batchSize = Math.max(1, config.batchSize);
            int transactionSize = Math.max(batchSize, config.effectiveTransactionSize());

            // Mid-transaction sub-flush boundary: flush this sub-batch (no commit)
            // when we hit batch-size, but not if we've also hit the transaction-size
            // boundary (in which case we want a single flush+commit).
            if (rowsInCurrentSubBatch >= batchSize && pendingBatch.size() < transactionSize) {
                // Sub-flush: executeBatch but no commit. The cumulative
                // partial-success guard runs inside flushSubBatchNoCommit.
                flushSubBatchNoCommit(ps, conn, rowsInCurrentSubBatch, transactionStartNanos);
                rowsInCurrentSubBatch = 0;
            }

            long now = System.currentTimeMillis();
            boolean transactionFull = pendingBatch.size() >= transactionSize;
            boolean safetyNetFires = !pendingBatch.isEmpty()
                    && now - lastCommitTime >= maxCommitIntervalMs;
            if (transactionFull || safetyNetFires) {
                int txnRows = pendingBatch.size();
                long latencyNanos = commitTransactionWithRetry(
                        ps, conn, pendingBatch, rowsInCurrentSubBatch, transactionStartNanos);
                metrics.record(latencyNanos);
                if (windowTracker != null) {
                    windowTracker.recordCommit(latencyNanos, txnRows);
                }
                rowsIngested += txnRows;
                pendingBatch.clear();
                rowsInCurrentSubBatch = 0;
                lastCommitTime = now;

                // Next pre-commit acquire: pace entry into the next
                // transaction. The PerThreadRateLimiter's deadline already
                // accumulates across calls, so each per-transaction acquire
                // contributes its own pacing — we must call this on every
                // transaction boundary, not just the first.
                preAcquireForNextTransaction();
            }

            // Status-line publication moved to VectorBench's progress thread
            // (issue #443): every worker calling computeStats() and
            // overwriting the shared statusLine on its own cadence dominated
            // the CPU profile and produced racy partial views. The progress
            // thread publishes a single coherent line every 500 ms using the
            // global counters and an HdrHistogram-backed snapshot.
        }
        // Final drain at end-of-input: flush any remaining sub-batch and
        // commit whatever remains in the transaction. The commit unit may be
        // smaller than the configured transaction-size — that's fine.
        if (!pendingBatch.isEmpty()) {
            int finalRows = pendingBatch.size();
            long latencyNanos = commitTransactionWithRetry(
                    ps, conn, pendingBatch, rowsInCurrentSubBatch, transactionStartNanos);
            metrics.record(latencyNanos);
            if (windowTracker != null) {
                windowTracker.recordCommit(latencyNanos, finalRows);
            }
            pendingBatch.clear();
        }
    }

    /**
     * Acquire enough permits to cover the next transaction.
     *
     * <p>Pre-commit acquire (issue #402, proposed fix B): reserving permits
     * before we start buffering rows for the next transaction spreads
     * workers apart in time so their commits overlap during the
     * {@code commit_ms} window, rather than serialising behind a shared
     * post-commit acquire.
     *
     * <p>The {@link PerThreadRateLimiter}'s deadline accumulates across
     * calls (each {@code acquire(N)} pushes the deadline forward by
     * {@code N × nanosPerPermit}), so calling this method on every
     * transaction boundary correctly paces every transaction at the
     * configured per-thread rate. Skipping the call would silently disable
     * rate limiting for all subsequent transactions.
     *
     * <p>If the rate limiter is unset (e.g. unit tests), this is a no-op.
     */
    private void preAcquireForNextTransaction() throws InterruptedException {
        if (rateLimiterGroup == null) {
            return;
        }
        int wanted = Math.max(1, config.effectiveTransactionSize());
        rateLimiterGroup.acquire(rateLimiterIndex, wanted);
    }
}
