package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
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
        if (seconds <= 0) return "N/A";
        long s = (long) seconds;
        long h = s / 3600;
        long m = (s % 3600) / 60;
        long sec = s % 60;
        if (h > 0) return String.format("%dh%02dm%02ds", h, m, sec);
        if (m > 0) return String.format("%dm%02ds", m, sec);
        return String.format("%ds", sec);
    }

    @Override
    public void run() {
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
                long lastCommitTime = System.currentTimeMillis();
                final long MAX_COMMIT_INTERVAL_MS = 30_000; // 30 seconds
                long totalCommitTimeNanos = 0;
                long commitCount = 0;
                long lastCommitDurationNanos = 0;
                while (true) {
                    float[] vec = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (vec == null) {
                        if (done.get() && queue.isEmpty()) break;
                        else continue;
                    }
                    if (vec.length == 0) break; // poison pill
                    long id = rowId.getAndIncrement();
                    long start = System.nanoTime();
                    ps.setLong(1, id);
                    ps.setObject(2, vec);
                    ps.executeUpdate();
                    batchCount++;

                    long now = System.currentTimeMillis();
                    if (batchCount >= config.batchSize || (batchCount > 0 && now - lastCommitTime >= MAX_COMMIT_INTERVAL_MS)) {
                        long commitStart = System.nanoTime();
                        conn.commit();
                        lastCommitDurationNanos = System.nanoTime() - commitStart;
                        totalCommitTimeNanos += lastCommitDurationNanos;
                        commitCount++;
                        batchCount = 0;
                        lastCommitTime = now;
                    }

                    long elapsed = System.nanoTime() - start;
                    metrics.record(elapsed);

                    long total = metrics.getCount();
                    if (total % 10_000 == 0) {
                        MetricsCollector.Stats s = metrics.computeStats();
                        double avgCommitMs = commitCount > 0 ? (totalCommitTimeNanos / commitCount) / 1_000_000.0 : 0;
                        double lastCommitMs = lastCommitDurationNanos / 1_000_000.0;
                        double elapsedSecs = (System.nanoTime() - ingestStartNanos) / 1e9;
                        double rowsPerSec = total / elapsedSecs;
                        long remaining = config.numRows - total;
                        double etaSecs = rowsPerSec > 0 ? remaining / rowsPerSec : 0;
                        String etaStr = formatEta(etaSecs);
                        statusLine.set(String.format("Ingested %d/%d rows | mean: %.2f ms | p50: %.2f ms | p99: %.2f ms | commit avg: %.1f ms last: %.1f ms | ETA: %s",
                                total,
                                config.numRows,
                                s.meanNanos() / 1_000_000.0,
                                s.p50Nanos() / 1_000_000.0,
                                s.p99Nanos() / 1_000_000.0,
                                avgCommitMs,
                                lastCommitMs,
                                etaStr));
                    }
                }
                if (batchCount > 0) {
                    long commitStart = System.nanoTime();
                    conn.commit();
                    lastCommitDurationNanos = System.nanoTime() - commitStart;
                    totalCommitTimeNanos += lastCommitDurationNanos;
                    commitCount++;
                }
            } catch (Exception e) {
                System.err.println("Ingestion error: " + e.getMessage());
                e.printStackTrace();
                conn.rollback();
            }
        } catch (Exception e) {
            System.err.println("Connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
