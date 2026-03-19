package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

public class IngestionWorker implements Runnable {

    private final Config config;
    private final List<float[]> vectors;
    private final int startId;
    private final int endId;
    private final MetricsCollector metrics;

    public IngestionWorker(Config config, List<float[]> vectors, int startId, int endId, MetricsCollector metrics) {
        this.config = config;
        this.vectors = vectors;
        this.startId = startId;
        this.endId = endId;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        String sql = "INSERT INTO " + config.tableName + "(id, vec) VALUES(?, ?)";
        try (Connection conn = DriverManager.getConnection(config.jdbcUrl, config.username, config.password)) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;
                for (int id = startId; id < endId; id++) {
                    long start = System.nanoTime();
                    ps.setInt(1, id);
                    ps.setObject(2, vectors.get(id % vectors.size()));
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
            System.err.println("Ingestion error in range [" + startId + ", " + endId + "): " + e.getMessage());
            e.printStackTrace();
        }
    }
}
