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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class IngestionWorker implements Runnable {

    private final Config config;
    private final BlockingQueue<float[]> queue;
    private final AtomicBoolean done;
    private final AtomicLong rowId;
    private final MetricsCollector metrics;
    private final AtomicReference<String> statusLine;
    private final long ingestStartNanos;

    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId, MetricsCollector metrics, AtomicReference<String> statusLine, long ingestStartNanos) {
        this.config = config;
        this.queue = queue;
        this.done = done;
        this.rowId = rowId;
        this.metrics = metrics;
        this.statusLine = statusLine;
        this.ingestStartNanos = ingestStartNanos;
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
     * Records batch+commit latency to metrics.
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
        // Execute accumulated batch asynchronously
        ps.unwrap(PreparedStatementAsync.class).executeBatchAsync().get();
        // Commit the transaction
        conn.commit();
        // Calculate and return latency
        return System.nanoTime() - batchStartNanos;
    }

    /**
     * Package-private method for testing flushBatch behavior.
     * Delegates to the private flushBatch method.
     *
     * @param ps prepared statement with accumulated batch
     * @param conn connection for commit
     * @param batchStartNanos nanosecond timestamp when batch started
     * @return latency in nanoseconds (batch + commit)
     * @throws SQLException if batch execution or commit fails
     * @throws ExecutionException if async batch execution fails
     * @throws InterruptedException if async batch execution is interrupted
     */
    long testFlushBatch(PreparedStatement ps, Connection conn, long batchStartNanos)
            throws SQLException, ExecutionException, InterruptedException {
        return flushBatch(ps, conn, batchStartNanos);
    }

    @Override
    public void run() {
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
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
                    if (batchCount == 0) {
                        batchStartNanos = System.nanoTime();
                    }
                    ps.setLong(1, id);
                    ps.setObject(2, vec);
                    ps.addBatch();
                    batchCount++;

                    long now = System.currentTimeMillis();
                    if (batchCount >= config.batchSize || (batchCount > 0 && now - lastCommitTime >= maxCommitIntervalMs)) {
                        long latencyNanos = flushBatch(ps, conn, batchStartNanos);
                        metrics.record(latencyNanos);
                        rowsIngested += batchCount;
                        batchCount = 0;
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
                        statusLine.set(String.format("Ingested %d/%d rows | %.0f ops/s | batch mean: %.2f ms | batch p50: %.2f ms | batch p99: %.2f ms | ETA: %s",
                                rowsIngested,
                                config.numRows,
                                rowsPerSec,
                                s.meanNanos() / 1_000_000.0,
                                s.p50Nanos() / 1_000_000.0,
                                s.p99Nanos() / 1_000_000.0,
                                etaStr));
                    }
                }
                if (batchCount > 0) {
                    long latencyNanos = flushBatch(ps, conn, batchStartNanos);
                    metrics.record(latencyNanos);
                    rowsIngested += batchCount;
                }
            } catch (SQLException | ExecutionException | InterruptedException e) {
                System.err.println("Ingestion error: " + e.getMessage());
                e.printStackTrace();
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    System.err.println("Rollback failed: " + rollbackError.getMessage());
                    rollbackError.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
