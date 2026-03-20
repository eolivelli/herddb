package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class QueryWorker implements Runnable {

    private final Config config;
    private final List<float[]> queryVectors;
    private final int startIdx;
    private final int endIdx;
    private final MetricsCollector metrics;
    private final List<List<Integer>> allResults;

    public QueryWorker(Config config, List<float[]> queryVectors, int startIdx, int endIdx,
                       MetricsCollector metrics, List<List<Integer>> allResults) {
        this.config = config;
        this.queryVectors = queryVectors;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.metrics = metrics;
        this.allResults = allResults;
    }

    @Override
    public void run() {
        String sql = "SELECT id FROM " + config.tableName
                + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT " + config.topK;
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = startIdx; i < endIdx; i++) {
                    long start = System.nanoTime();
                    ps.setObject(1, queryVectors.get(i));
                    List<Integer> ids = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getInt(1));
                        }
                    }
                    long elapsed = System.nanoTime() - start;
                    metrics.record(elapsed);
                    allResults.set(i, ids);

                    long total = metrics.getCount();
                    if (total % 100 == 0) {
                        System.out.println("  Executed " + total + " queries...");
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Query error in range [" + startIdx + ", " + endIdx + "): " + e.getMessage());
            e.printStackTrace();
        }
    }
}
