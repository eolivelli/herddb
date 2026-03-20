package herddb.vectortesting;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class VectorBench {

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        System.out.println("Vector Benchmark Configuration:");
        System.out.println(config);
        System.out.println();

        // Phase 1: Dataset
        DatasetLoader loader = new DatasetLoader(config.datasetDir, config.dataset, config.datasetUrl);
        loader.ensureDataset();

        System.out.println("Loading query vectors...");
        loader.ensureQueryAndGroundTruth();
        List<float[]> queryVectors = loader.loadQueryVectors(config.queryCount);
        System.out.println("Loaded " + queryVectors.size() + " query vectors");

        List<int[]> groundTruth = null;
        try {
            groundTruth = loader.loadGroundTruth(queryVectors.size());
            System.out.println("Loaded " + groundTruth.size() + " ground truth entries");
        } catch (Exception e) {
            System.out.println("Ground truth not available: " + e.getMessage());
        }
        System.out.println();

        int actualRows = config.numRows;

        // Phase 2: Drop table (if requested)
        if (config.dropTable) {
            System.out.println("Dropping table " + config.tableName + "...");
            try (Connection conn = DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + config.tableName);
            }
            System.out.println("Table dropped.");
        }

        // Phase 3: Schema creation
        System.out.println("Creating table " + config.tableName + "...");
        try (Connection conn = DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + config.tableName
                    + " (id int primary key, vec floata not null)");
        }
        System.out.println("Table ready.");
        System.out.println();

        // Phase 4: Ingestion
        if (!config.skipIngest) {
            System.out.println("=== INGESTION PHASE ===");
            MetricsCollector ingestMetrics = new MetricsCollector();
            long ingestStart = System.nanoTime();

            BlockingQueue<float[]> ingestQueue = new ArrayBlockingQueue<>(1000);
            AtomicBoolean producerDone = new AtomicBoolean(false);
            AtomicLong rowId = new AtomicLong(0);

            ExecutorService ingestPool = Executors.newFixedThreadPool(config.ingestThreads);
            for (int t = 0; t < config.ingestThreads; t++) {
                ingestPool.submit(new IngestionWorker(config, ingestQueue, producerDone, rowId, ingestMetrics));
            }

            try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(actualRows)) {
                for (float[] vec : stream) {
                    ingestQueue.put(vec);
                }
            }
            producerDone.set(true);
            for (int t = 0; t < config.ingestThreads; t++) {
                ingestQueue.put(new float[0]); // poison pills
            }
            ingestPool.shutdown();
            ingestPool.awaitTermination(24, TimeUnit.HOURS);

            long ingestWall = System.nanoTime() - ingestStart;
            double ingestSecs = ingestWall / 1_000_000_000.0;
            System.out.println();
            System.out.printf("=== INGESTION RESULTS ===%n");
            System.out.printf("Rows: %d | Wall time: %.1fs | Throughput: %.0f ops/s%n",
                    ingestMetrics.getCount(), ingestSecs, ingestMetrics.getCount() / ingestSecs);
            System.out.printf("Threads: %d | Batch size: %d%n", config.ingestThreads, config.batchSize);
            ingestMetrics.computeStats().print("INGESTION LATENCY");

            // Verify row count matches ingested records
            if (!config.skipVerify) {
                long expectedRows = ingestMetrics.getCount();
                try (Connection conn = DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
                     Statement stmt = conn.createStatement();
                     java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + config.tableName)) {
                    rs.next();
                    long actualCount = rs.getLong(1);
                    if (actualCount != expectedRows) {
                        throw new IllegalStateException("Row count mismatch after ingestion: expected "
                                + expectedRows + " but table has " + actualCount);
                    }
                    System.out.printf("Verification OK: %d rows in table%n", actualCount);
                }
            }
            System.out.println();
        } else {
            System.out.println("Skipping ingestion phase.");
            System.out.println();
        }

        // Phase 5: Index creation
        if (!config.skipIndex) {
            System.out.println("=== INDEX CREATION ===");
            long indexStart = System.nanoTime();
            try (Connection conn = DriverManager.getConnection(config.jdbcUrl, config.username, config.password);
                 Statement stmt = conn.createStatement()) {
                String indexSql = "CREATE VECTOR INDEX vidx ON " + config.tableName + "(vec)"
                        + " WITH m=" + config.indexM
                        + " beamWidth=" + config.indexBeamWidth
                        + " similarity=euclidean fusedPQ=true";
                System.out.println("Executing: " + indexSql);
                stmt.execute(indexSql);
            }
            long indexWall = System.nanoTime() - indexStart;
            System.out.printf("Index creation time: %.1fs%n%n", indexWall / 1_000_000_000.0);
        } else {
            System.out.println("Skipping index creation.");
            System.out.println();
        }

        // Phase 6: Queries
        System.out.println("=== QUERY PHASE ===");
        int actualQueries = Math.min(config.queryCount, queryVectors.size());
        MetricsCollector queryMetrics = new MetricsCollector();
        List<List<Integer>> queryResults = new ArrayList<>(Collections.nCopies(actualQueries, null));

        long queryStart = System.nanoTime();
        ExecutorService queryPool = Executors.newFixedThreadPool(config.queryThreads);
        int qChunk = actualQueries / config.queryThreads;
        for (int t = 0; t < config.queryThreads; t++) {
            int start = t * qChunk;
            int end = (t == config.queryThreads - 1) ? actualQueries : start + qChunk;
            queryPool.submit(new QueryWorker(config, queryVectors, start, end, queryMetrics, queryResults));
        }
        queryPool.shutdown();
        queryPool.awaitTermination(24, TimeUnit.HOURS);

        long queryWall = System.nanoTime() - queryStart;
        double querySecs = queryWall / 1_000_000_000.0;
        System.out.println();
        System.out.printf("=== QUERY RESULTS ===%n");
        System.out.printf("Queries: %d | Wall time: %.1fs | Throughput: %.0f qps%n",
                queryMetrics.getCount(), querySecs, queryMetrics.getCount() / querySecs);
        System.out.printf("Threads: %d | Top-K: %d%n", config.queryThreads, config.topK);
        queryMetrics.computeStats().print("QUERY LATENCY");

        // Phase 7: Recall
        if (groundTruth != null && !groundTruth.isEmpty()) {
            double recall = computeRecall(queryResults, groundTruth, config.topK);
            System.out.printf("%nRecall@%d: %.4f%n", config.topK, recall);
        }

        System.out.println("\nBenchmark complete.");
        System.exit(0);
    }

    private static double computeRecall(List<List<Integer>> results, List<int[]> groundTruth, int k) {
        int totalRelevant = 0;
        int totalFound = 0;
        int count = Math.min(results.size(), groundTruth.size());
        for (int i = 0; i < count; i++) {
            List<Integer> result = results.get(i);
            if (result == null) continue;
            int[] truth = groundTruth.get(i);
            Set<Integer> truthSet = new HashSet<>();
            for (int j = 0; j < Math.min(k, truth.length); j++) {
                truthSet.add(truth[j]);
            }
            totalRelevant += truthSet.size();
            for (int id : result) {
                if (truthSet.contains(id)) {
                    totalFound++;
                }
            }
        }
        return totalRelevant == 0 ? 0.0 : (double) totalFound / totalRelevant;
    }
}
