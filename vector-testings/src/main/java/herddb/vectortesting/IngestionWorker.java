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
     * Base of the exponential back-off between commit retries, in milliseconds.
     * Defaults to 10 seconds per issue #153 (10s, 20s, 40s, ...); tests override
     * this to a tiny value to keep the suite fast.
     */
    long backoffBaseMillis = 10_000L;

    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId,
                           MetricsCollector metrics, AtomicReference<String> statusLine, long ingestStartNanos,
                           AtomicLong commitsTotal, AtomicLong commitsRecovered, AtomicLong rowsCommitted) {
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
    }

    private static String formatEta(double seconds) {
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
     * Flush accumulated batch using executeBatchAsync and commit.
     *
     * @param ps prepared statement with accumulated batch
     * @param conn connection for commit
     * @param batchStartNanos nanosecond timestamp when batch started
     * @return latency in nanoseconds (batch + commit)
     * @throws SQLException if batch execution or commit fails
     * @throws ExecutionException if async batch execution fails
     * @throws InterruptedException if async batch execution is interrupted
     */
    private long flushBatch(PreparedStatement ps, Connection conn, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        ps.unwrap(PreparedStatementAsync.class).executeBatchAsync().get();
        conn.commit();
        return System.nanoTime() - batchStartNanos;
    }

    /**
     * Package-private method for testing flushBatch behavior.
     */
    long testFlushBatch(PreparedStatement ps, Connection conn, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        return flushBatch(ps, conn, batchStartNanos);
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
                long latencyNanos = flushBatch(ps, conn, batchStartNanos);
                commitsTotal.incrementAndGet();
                rowsCommitted.addAndGet(batch.size());
                if (attempt > 0) {
                    commitsRecovered.incrementAndGet();
                }
                return latencyNanos;
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
            // The PS may have been closed or be in a bad state; surface for debugging
            // but continue — the replay loop will re-populate its batch.
            System.err.println("clearBatch failed during retry: " + clearError.getMessage());
        }
    }

    @Override
    public void run() {
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                List<PendingRow> pendingBatch = new ArrayList<>(Math.max(1, config.batchSize));
                long lastCommitTime = System.currentTimeMillis();
                final long maxCommitIntervalMs = 30_000; // 30 seconds
                long batchStartNanos = 0;
                long rowsIngested = 0;
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
                        batchStartNanos = System.nanoTime();
                    }
                    ps.setLong(1, id);
                    ps.setObject(2, vec);
                    ps.addBatch();
                    pendingBatch.add(new PendingRow(id, vec));

                    long now = System.currentTimeMillis();
                    if (pendingBatch.size() >= config.batchSize
                            || (!pendingBatch.isEmpty() && now - lastCommitTime >= maxCommitIntervalMs)) {
                        long latencyNanos = commitWithRetry(ps, conn, pendingBatch, batchStartNanos);
                        metrics.record(latencyNanos);
                        rowsIngested += pendingBatch.size();
                        pendingBatch.clear();
                        lastCommitTime = now;
                    }

                    // Throttle if max ops/s is configured
                    if (config.ingestMaxOpsPerSecond > 0) {
                        double expectedElapsedSecs = (double) rowsIngested / config.ingestMaxOpsPerSecond;
                        double actualElapsedSecs = (System.nanoTime() - ingestStartNanos) / 1e9;
                        double sleepSecs = expectedElapsedSecs - actualElapsedSecs;
                        if (sleepSecs > 0.001) {
                            Thread.sleep((long) (sleepSecs * 1000));
                        }
                    }

                    if (rowsIngested % 10_000 == 0 && rowsIngested > 0) {
                        MetricsCollector.Stats s = metrics.computeStats();
                        double elapsedSecs = (System.nanoTime() - ingestStartNanos) / 1e9;
                        double rowsPerSec = rowsIngested / elapsedSecs;
                        long remaining = config.numRows - rowsIngested;
                        double etaSecs = rowsPerSec > 0 ? remaining / rowsPerSec : 0;
                        String etaStr = formatEta(etaSecs);
                        statusLine.set(String.format(
                                "Ingested %d/%d rows | %.0f ops/s | commits: %d (recovered: %d) | "
                                        + "batch mean: %.2f ms | batch p50: %.2f ms | batch p99: %.2f ms | ETA: %s",
                                rowsIngested,
                                config.numRows,
                                rowsPerSec,
                                commitsTotal.get(),
                                commitsRecovered.get(),
                                s.meanNanos() / 1_000_000.0,
                                s.p50Nanos() / 1_000_000.0,
                                s.p99Nanos() / 1_000_000.0,
                                etaStr));
                    }
                }
                if (!pendingBatch.isEmpty()) {
                    long latencyNanos = commitWithRetry(ps, conn, pendingBatch, batchStartNanos);
                    metrics.record(latencyNanos);
                    rowsIngested += pendingBatch.size();
                    pendingBatch.clear();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Ingestion worker interrupted", e);
        } catch (SQLException | ExecutionException e) {
            // Propagate: awaitTermination ignores task exceptions, so swallowing here
            // would silently drop the uncommitted pendingBatch.
            throw new RuntimeException("Ingestion worker failed", e);
        }
    }
}
