package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ArrayBlockingQueue;
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

    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId, MetricsCollector metrics, AtomicReference<String> statusLine) {
        this.config = config;
        this.queue = queue;
        this.done = done;
        this.rowId = rowId;
        this.metrics = metrics;
        this.statusLine = statusLine;
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
                        conn.commit();
                        batchCount = 0;
                        lastCommitTime = now;
                    }

                    long elapsed = System.nanoTime() - start;
                    metrics.record(elapsed);

                    long total = metrics.getCount();
                    if (total % 10_000 == 0) {
                        MetricsCollector.Stats s = metrics.computeStats();
                        statusLine.set(String.format("Ingested %d rows | mean: %.2f ms | p50: %.2f ms | p95: %.2f ms | p99: %.2f ms | max: %.2f ms",
                                total,
                                s.meanNanos() / 1_000_000.0,
                                s.p50Nanos() / 1_000_000.0,
                                s.p95Nanos() / 1_000_000.0,
                                s.p99Nanos() / 1_000_000.0,
                                s.maxNanos() / 1_000_000.0));
                    }
                }
                if (batchCount > 0) {
                    conn.commit();
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
