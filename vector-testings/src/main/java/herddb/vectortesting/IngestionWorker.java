package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class IngestionWorker implements Runnable {

    private final Config config;
    private final BlockingQueue<float[]> queue;
    private final AtomicBoolean done;
    private final AtomicLong rowId;
    private final MetricsCollector metrics;

    public IngestionWorker(Config config, BlockingQueue<float[]> queue, AtomicBoolean done, AtomicLong rowId, MetricsCollector metrics) {
        this.config = config;
        this.queue = queue;
        this.done = done;
        this.rowId = rowId;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
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
                    if (batchCount >= config.batchSize) {
                        conn.commit();
                        batchCount = 0;
                    }
                    long elapsed = System.nanoTime() - start;
                    metrics.record(elapsed);

                    long total = metrics.getCount();
                    if (total % 10_000 == 0) {
                        System.out.println("  Ingested " + total + " rows...");
                    }
                }
                if (batchCount > 0) {
                    conn.commit();
                }
            }
        } catch (Exception e) {
            System.err.println("Ingestion error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
