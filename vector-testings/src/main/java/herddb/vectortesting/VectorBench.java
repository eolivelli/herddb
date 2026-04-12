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
import java.util.concurrent.atomic.AtomicReference;

public class VectorBench {

    @FunctionalInterface
    interface SqlTask {
        void run() throws Exception;
    }

    /** Runs a task with a progress spinner and returns elapsed wall-clock seconds. */
    private static double runWithProgress(String label, SqlTask task) throws Exception {
        System.out.println(label);
        char[] spinner = {'|', '/', '-', '\\'};
        int[] spinIdx = {0};
        long startNs = System.nanoTime();
        Exception[] err = {null};

        Thread worker = new Thread(() -> {
            try {
                task.run();
            } catch (Exception e) {
                err[0] = e;
            }
        });
        worker.start();

        while (worker.isAlive()) {
            double elapsed = (System.nanoTime() - startNs) / 1e9;
            int filled = Math.min(40, (int) (elapsed / 5));
            String bar = "=".repeat(filled) + " ".repeat(40 - filled);
            System.out.printf("\r  [%s] %c %.0fs...", bar, spinner[spinIdx[0] % 4], elapsed);
            System.out.flush();
            spinIdx[0]++;
            worker.join(500);
        }

        double totalSecs = (System.nanoTime() - startNs) / 1e9;
        System.out.printf("\r  [%s] done in %.1fs%n", "=".repeat(40), totalSecs);

        if (err[0] != null) {
            throw err[0];
        }
        return totalSecs;
    }

    public static void main(String[] args) throws Exception {
        long benchmarkStartNs = System.nanoTime();
        Config config = Config.parse(args);
        System.out.println("Vector Benchmark Configuration:");
        System.out.println(config);
        System.out.println();

        // Summary accumulators
        double ingestionWallSecs = -1, indexWallSecs = -1, queryWallSecs = -1;
        double checkpointPostIngestSecs = -1, checkpointPostIndexSecs = -1;
        long ingestionRows = 0;
        double ingestionThroughput = 0;
        MetricsCollector.Stats ingestionLatency = null;
        long queriesRun = 0;
        double queryThroughput = 0;
        MetricsCollector.Stats queryLatency = null;
        double recall = -1;
        int recallK = config.topK;
        int recallQueries = 0;

        // Phase 1: Dataset
        DatasetLoader loader = new DatasetLoader(config.datasetDir, config.dataset, config.datasetUrl);
        loader.ensureDataset();

        // For CUSTOM datasets, load descriptor and auto-configure
        if (config.dataset == DatasetLoader.DatasetPreset.CUSTOM) {
            DatasetLoader.DatasetDescriptor desc = loader.loadDescriptor();
            if (config.similarity == null) {
                config.similarity = desc.similarity;
                System.out.println("Auto-configured similarity from descriptor: " + desc.similarity);
            }
            if (config.numRows == 100_000 && desc.totalVectors > 0) {
                config.numRows = desc.totalVectors;
                System.out.println("Auto-configured rows from descriptor: " + desc.totalVectors);
            }
            if (!config.topKExplicit && desc.groundTruthK > 0) {
                config.topK = desc.groundTruthK;
                System.out.println("Auto-configured topK from descriptor groundTruthK: " + desc.groundTruthK);
            }
            System.out.println();
        }

        System.out.println("Loading query vectors...");
        loader.ensureQueryAndGroundTruth();
        List<float[]> queryVectors = loader.loadQueryVectors(config.queryCount);
        System.out.println("Loaded " + queryVectors.size() + " query vectors from dataset");

        // Cycle query vectors if requested count exceeds dataset size
        if (config.queryCount > queryVectors.size()) {
            int originalSize = queryVectors.size();
            queryVectors = cycleVectors(queryVectors, config.queryCount);
            System.out.println("Cycling " + originalSize + " query vectors to reach " + config.queryCount + " queries");
        }

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
            try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + config.tableName);
            }
            System.out.println("Table dropped.");
        }

        // Phase 3: Schema creation
        System.out.println("Creating table " + config.tableName + "...");
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + config.tableName
                    + " (id int primary key, vec floata not null)");
        }
        System.out.println("Table ready.");
        System.out.println();

        // Phase 4a: Index creation before ingestion (if requested)
        if (config.indexBeforeIngest && !config.skipIndex) {
            String indexSql = "CREATE VECTOR INDEX vidx ON " + config.tableName + "(vec)"
                    + " WITH m=" + config.indexM
                    + " beamWidth=" + config.indexBeamWidth
                    + " similarity=" + config.effectiveSimilarity() + " fusedPQ=true";
            System.out.println("Executing (pre-ingest): " + indexSql);
            indexWallSecs = runWithProgress("=== INDEX CREATION (pre-ingest) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(indexSql);
                }
            });
            System.out.println();
        }

        // Phase 4: Ingestion
        if (!config.skipIngest) {
            int toIngest = actualRows - config.resumeFrom;
            if (toIngest <= 0) {
                System.out.println("resumeFrom (" + config.resumeFrom + ") >= rows (" + actualRows + "), nothing to ingest.");
                System.out.println();
            } else {
            System.out.println("=== INGESTION PHASE ===");
            if (config.resumeFrom > 0) {
                System.out.println("Resuming from position " + config.resumeFrom + ", ingesting " + toIngest + " rows.");
            }
            MetricsCollector ingestMetrics = new MetricsCollector();
            AtomicReference<String> ingestStatus = new AtomicReference<>("");

            BlockingQueue<float[]> ingestQueue = new ArrayBlockingQueue<>(1000);
            AtomicBoolean producerDone = new AtomicBoolean(false);
            AtomicLong rowId = new AtomicLong(config.resumeFrom);

            long ingestStart = System.nanoTime();

            ExecutorService ingestPool = Executors.newFixedThreadPool(config.ingestThreads);
            for (int t = 0; t < config.ingestThreads; t++) {
                ingestPool.submit(new IngestionWorker(config, ingestQueue, producerDone, rowId, ingestMetrics, ingestStatus, ingestStart));
            }

            // Progress display thread runs during the entire ingestion
            char[] ingestSpinner = {'|', '/', '-', '\\'};
            AtomicBoolean ingestDone = new AtomicBoolean(false);
            Thread progressThread = new Thread(() -> {
                int spin = 0;
                Runtime rt = Runtime.getRuntime();
                while (!ingestDone.get()) {
                    double elapsed = (System.nanoTime() - ingestStart) / 1e9;
                    int filled = Math.min(40, (int) (elapsed / 5));
                    String bar = "=".repeat(filled) + " ".repeat(40 - filled);
                    long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                    long maxMb = rt.maxMemory() / (1024 * 1024);
                    System.out.printf("\r  [%s] %c %.0fs | heap: %d/%d MB | %s",
                            bar, ingestSpinner[spin++ % 4], elapsed, usedMb, maxMb, ingestStatus.get());
                    System.out.flush();
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            });
            progressThread.setDaemon(true);
            progressThread.start();

            try (DatasetLoader.VectorStream stream = loader.streamBaseVectors(config.resumeFrom, toIngest)) {
                for (float[] vec : stream) {
                    ingestQueue.put(vec);
                }
            }
            producerDone.set(true);
            for (int t = 0; t < config.ingestThreads; t++) {
                ingestQueue.put(new float[0]); // poison pills
            }
            ingestPool.shutdown();
            ingestPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            ingestDone.set(true);
            progressThread.join();
            double ingestSecs = (System.nanoTime() - ingestStart) / 1e9;
            System.out.printf("\r  [%s] done in %.1fs%n", "=".repeat(40), ingestSecs);

            ingestionWallSecs = ingestSecs;
            ingestionRows = ingestMetrics.getCount();
            ingestionThroughput = ingestionRows / ingestSecs;
            ingestionLatency = ingestMetrics.computeStats();

            System.out.printf("=== INGESTION RESULTS ===%n");
            System.out.printf("Rows: %d | Wall time: %.1fs | Throughput: %.0f ops/s%n",
                    ingestionRows, ingestSecs, ingestionThroughput);
            System.out.printf("Threads: %d | Batch size: %d | Max ops/s: %s%n", config.ingestThreads, config.batchSize,
                    config.ingestMaxOpsPerSecond > 0 ? config.ingestMaxOpsPerSecond : "unlimited");
            ingestionLatency.print("INGESTION LATENCY");

            // Verify row count matches ingested records
            if (!config.skipVerify) {
                long expectedRows = config.resumeFrom + ingestMetrics.getCount();
                long[] actualCount = {0};
                runWithProgress("=== VERIFICATION (COUNT) ===", () -> {
                    try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                         Statement stmt = conn.createStatement();
                         java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + config.tableName)) {
                        rs.next();
                        actualCount[0] = rs.getLong(1);
                    }
                });
                if (actualCount[0] != expectedRows) {
                    throw new IllegalStateException("Row count mismatch after ingestion: expected "
                            + expectedRows + " but table has " + actualCount[0]);
                }
                System.out.printf("Verification OK: %d rows in table%n", actualCount[0]);
            }
            System.out.println();
            } // end toIngest > 0
        } else {
            System.out.println("Skipping ingestion phase.");
            System.out.println();
        }

        // Phase 4b: Checkpoint after ingestion
        if (config.checkpoint && !config.skipIngest) {
            System.out.println("Executing checkpoint with timeout " + config.checkpointTimeoutSeconds + "s ...");
            checkpointPostIngestSecs = runWithProgress("=== CHECKPOINT (post-ingest) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("EXECUTE CHECKPOINT 'herd', " + config.checkpointTimeoutSeconds);
                }
            });
            System.out.println();
        }

        // Phase 5: Index creation (post-ingest, unless already created before ingestion)
        if (!config.skipIndex && !config.indexBeforeIngest) {
            String indexSql = "CREATE VECTOR INDEX vidx ON " + config.tableName + "(vec)"
                    + " WITH m=" + config.indexM
                    + " beamWidth=" + config.indexBeamWidth
                    + " similarity=" + config.effectiveSimilarity() + " fusedPQ=true";
            System.out.println("Executing: " + indexSql);
            indexWallSecs = runWithProgress("=== INDEX CREATION ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(indexSql);
                }
            });
            System.out.println();
        } else if (config.skipIndex) {
            System.out.println("Skipping index creation.");
            System.out.println();
        }

        // Phase 5b: Checkpoint after index creation
        if (config.checkpoint && !config.skipIndex && !config.indexBeforeIngest) {
            System.out.println("Executing checkpoint with timeout " + config.checkpointTimeoutSeconds + "s ...");
            checkpointPostIndexSecs = runWithProgress("=== CHECKPOINT (post-index) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("EXECUTE CHECKPOINT 'herd', " + config.checkpointTimeoutSeconds);
                }
            });
            System.out.println();
        }

        // Phase 6: Queries
        System.out.println("=== QUERY PHASE ===");
        String queryTemplate = "SELECT id FROM " + config.tableName
                + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT " + config.topK;
        System.out.println("Query template: " + queryTemplate);
        int actualQueries = queryVectors.size();
        MetricsCollector queryMetrics = new MetricsCollector();
        List<List<Integer>> queryResults = new ArrayList<>(Collections.nCopies(actualQueries, null));
        AtomicReference<String> queryStatus = new AtomicReference<>("");

        ExecutorService queryPool = Executors.newFixedThreadPool(config.queryThreads);
        int qChunk = actualQueries / config.queryThreads;
        for (int t = 0; t < config.queryThreads; t++) {
            int start = t * qChunk;
            int end = (t == config.queryThreads - 1) ? actualQueries : start + qChunk;
            queryPool.submit(new QueryWorker(config, queryVectors, start, end, queryMetrics, queryResults, queryStatus));
        }
        queryPool.shutdown();

        long queryStart = System.nanoTime();
        char[] querySpinner = {'|', '/', '-', '\\'};
        int querySpin = 0;
        while (!queryPool.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            double elapsed = (System.nanoTime() - queryStart) / 1e9;
            int filled = Math.min(40, (int) (elapsed / 5));
            String bar = "=".repeat(filled) + " ".repeat(40 - filled);
            System.out.printf("\r  [%s] %c %.0fs | %s", bar, querySpinner[querySpin++ % 4], elapsed, queryStatus.get());
            System.out.flush();
        }
        double querySecs = (System.nanoTime() - queryStart) / 1e9;
        System.out.printf("\r  [%s] done in %.1fs%n", "=".repeat(40), querySecs);

        queryWallSecs = querySecs;
        queriesRun = queryMetrics.getCount();
        queryThroughput = queriesRun / querySecs;
        queryLatency = queryMetrics.computeStats();

        System.out.printf("=== QUERY RESULTS ===%n");
        System.out.printf("Queries: %d | Wall time: %.1fs | Throughput: %.0f qps%n",
                queriesRun, querySecs, queryThroughput);
        System.out.printf("Threads: %d | Top-K: %d%n", config.queryThreads, config.topK);
        queryLatency.print("QUERY LATENCY");

        // Phase 7: Recall
        if (groundTruth != null && !groundTruth.isEmpty()) {
            // Only compute recall for queries that have ground truth (non-cycled portion)
            List<List<Integer>> recallResults = queryResults.subList(0, Math.min(queryResults.size(), groundTruth.size()));
            recall = computeRecall(recallResults, groundTruth, config.topK);
            recallQueries = recallResults.size();
            System.out.printf("%nRecall@%d: %.4f (computed on %d queries with ground truth)%n",
                    config.topK, recall, recallQueries);
            if (config.dataset == DatasetLoader.DatasetPreset.CUSTOM && loader.getCustomDescriptor() != null) {
                int gtK = loader.getCustomDescriptor().groundTruthK;
                if (config.topK > gtK) {
                    System.out.printf("WARNING: topK=%d exceeds ground truth K=%d from descriptor — "
                            + "recall may be unreliable (ground truth has fewer neighbors than requested)%n",
                            config.topK, gtK);
                } else if (config.topKExplicit && config.topK != gtK) {
                    System.out.printf("NOTE: topK=%d differs from descriptor groundTruthK=%d%n",
                            config.topK, gtK);
                }
            }
        }

        // Final summary
        double totalWallSecs = (System.nanoTime() - benchmarkStartNs) / 1e9;
        printSummary(config, ingestionWallSecs, ingestionRows, ingestionThroughput, ingestionLatency,
                checkpointPostIngestSecs, indexWallSecs, checkpointPostIndexSecs,
                queryWallSecs, queriesRun, queryThroughput, queryLatency,
                recall, recallK, recallQueries, totalWallSecs);

        System.out.println("\nBenchmark complete.");
        System.exit(0);
    }

    /**
     * Prints a structured summary of the benchmark run.
     * Each phase is printed as a line of space-separated key=value pairs,
     * making it easy to parse programmatically (e.g. grep/awk) while remaining human-readable.
     */
    private static void printSummary(Config config,
                                     double ingestionWallSecs, long ingestionRows, double ingestionThroughput,
                                     MetricsCollector.Stats ingestionLatency,
                                     double checkpointPostIngestSecs,
                                     double indexWallSecs,
                                     double checkpointPostIndexSecs,
                                     double queryWallSecs, long queriesRun, double queryThroughput,
                                     MetricsCollector.Stats queryLatency,
                                     double recall, int recallK, int recallQueries,
                                     double totalWallSecs) {
        System.out.println();
        System.out.println("========================================");
        System.out.println("          BENCHMARK SUMMARY");
        System.out.println("========================================");
        System.out.printf("dataset=%s rows=%d similarity=%s%n",
                config.dataset, config.numRows, config.effectiveSimilarity());
        System.out.println("----------------------------------------");

        if (ingestionWallSecs >= 0 && ingestionLatency != null) {
            System.out.printf("phase=ingestion wall_time_s=%.1f rows=%d throughput_ops=%.0f "
                            + "threads=%d batch_size=%d "
                            + "latency_mean_ms=%.2f latency_p50_ms=%.2f latency_p95_ms=%.2f "
                            + "latency_p99_ms=%.2f latency_max_ms=%.2f%n",
                    ingestionWallSecs, ingestionRows, ingestionThroughput,
                    config.ingestThreads, config.batchSize,
                    ingestionLatency.meanNanos() / 1e6, ingestionLatency.p50Nanos() / 1e6,
                    ingestionLatency.p95Nanos() / 1e6, ingestionLatency.p99Nanos() / 1e6,
                    ingestionLatency.maxNanos() / 1e6);
        } else {
            System.out.println("phase=ingestion status=skipped");
        }

        if (checkpointPostIngestSecs >= 0) {
            System.out.printf("phase=checkpoint_post_ingest wall_time_s=%.1f%n", checkpointPostIngestSecs);
        }

        if (indexWallSecs >= 0) {
            System.out.printf("phase=index_creation wall_time_s=%.1f m=%d beam_width=%d%n",
                    indexWallSecs, config.indexM, config.indexBeamWidth);
        } else {
            System.out.println("phase=index_creation status=skipped");
        }

        if (checkpointPostIndexSecs >= 0) {
            System.out.printf("phase=checkpoint_post_index wall_time_s=%.1f%n", checkpointPostIndexSecs);
        }

        if (queryLatency != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("phase=query wall_time_s=%.1f queries=%d throughput_qps=%.0f "
                            + "threads=%d top_k=%d "
                            + "latency_mean_ms=%.2f latency_p50_ms=%.2f latency_p95_ms=%.2f "
                            + "latency_p99_ms=%.2f latency_max_ms=%.2f",
                    queryWallSecs, queriesRun, queryThroughput,
                    config.queryThreads, config.topK,
                    queryLatency.meanNanos() / 1e6, queryLatency.p50Nanos() / 1e6,
                    queryLatency.p95Nanos() / 1e6, queryLatency.p99Nanos() / 1e6,
                    queryLatency.maxNanos() / 1e6));
            if (recall >= 0) {
                sb.append(String.format(" recall@%d=%.4f recall_queries=%d", recallK, recall, recallQueries));
            }
            System.out.println(sb);
        }

        System.out.println("----------------------------------------");
        System.out.printf("total_wall_time_s=%.1f%n", totalWallSecs);
        System.out.println("========================================");
    }

    static <T> List<T> cycleVectors(List<T> vectors, int targetCount) {
        if (targetCount <= vectors.size()) {
            return vectors;
        }
        List<T> cycled = new ArrayList<>(targetCount);
        int originalSize = vectors.size();
        for (int i = 0; i < targetCount; i++) {
            cycled.add(vectors.get(i % originalSize));
        }
        return cycled;
    }

    private static double computeRecall(List<List<Integer>> results, List<int[]> groundTruth, int k) {
        int totalRelevant = 0;
        int totalFound = 0;
        int count = Math.min(results.size(), groundTruth.size());
        for (int i = 0; i < count; i++) {
            List<Integer> result = results.get(i);
            if (result == null) {
                continue;
            }
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
