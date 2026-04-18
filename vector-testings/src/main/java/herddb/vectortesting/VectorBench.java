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
import java.util.LinkedHashMap;
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
    private static double runWithProgress(BenchOutput out, String phase, String label, SqlTask task) throws Exception {
        if (!out.suppressesText()) {
            out.header(label);
        }
        out.phaseStart(phase);
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
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            out.progress(phase, elapsed, null, fields);
            worker.join(500);
        }

        double totalSecs = (System.nanoTime() - startNs) / 1e9;
        out.phaseDone(phase, totalSecs);

        if (err[0] != null) {
            throw err[0];
        }
        return totalSecs;
    }

    public static void main(String[] args) throws Exception {
        long benchmarkStartNs = System.nanoTime();
        Config config = Config.parse(args);
        BenchOutput out = BenchOutput.create(config);
        try {
            runBenchmark(config, out, benchmarkStartNs);
        } catch (Exception e) {
            // Top-level catch so NDJSON consumers get a structured error event before the JVM exits.
            out.error(e);
            throw e;
        }
    }

    private static void runBenchmark(Config config, BenchOutput out, long benchmarkStartNs) throws Exception {
        out.config(config);

        // Summary accumulators
        double ingestionWallSecs = -1, indexWallSecs = -1, queryWallSecs = -1;
        double checkpointPostIngestSecs = -1, checkpointPostIndexSecs = -1;
        double waitForIndexesSecs = -1;
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
                out.info("Auto-configured similarity from descriptor: " + desc.similarity);
            }
            if (config.numRows == 100_000 && desc.totalVectors > 0) {
                config.numRows = desc.totalVectors;
                out.info("Auto-configured rows from descriptor: " + desc.totalVectors);
            }
            if (!config.topKExplicit && desc.groundTruthK > 0) {
                config.topK = desc.groundTruthK;
                out.info("Auto-configured topK from descriptor groundTruthK: " + desc.groundTruthK);
            }
        }

        out.info("Loading query vectors...");
        loader.ensureQueryAndGroundTruth();
        List<float[]> queryVectors = loader.loadQueryVectors(config.queryCount);
        out.info("Loaded " + queryVectors.size() + " query vectors from dataset");

        // Cycle query vectors if requested count exceeds dataset size
        if (config.queryCount > queryVectors.size()) {
            int originalSize = queryVectors.size();
            queryVectors = cycleVectors(queryVectors, config.queryCount);
            out.info("Cycling " + originalSize + " query vectors to reach " + config.queryCount + " queries");
        }

        List<int[]> groundTruth = null;
        try {
            groundTruth = loader.loadGroundTruth(queryVectors.size());
            out.info("Loaded " + groundTruth.size() + " ground truth entries");
        } catch (java.io.IOException e) {
            out.info("Ground truth not available: " + e.getMessage());
        }

        int actualRows = config.numRows;

        // Phase 2: Drop table (if requested)
        if (config.dropTable) {
            out.info("Dropping table " + config.tableName + "...");
            try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + config.tableName);
            }
            out.info("Table dropped.");
        }

        // Phase 3: Schema creation
        out.info("Creating table " + config.tableName + "...");
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + config.tableName
                    + " (id int primary key, vec floata not null)");
        }
        out.info("Table ready.");

        // Phase 4a: Index creation before ingestion (if requested)
        if (config.indexBeforeIngest && !config.skipIndex) {
            String indexSql = "CREATE VECTOR INDEX vidx ON " + config.tableName + "(vec)"
                    + " WITH m=" + config.indexM
                    + " beamWidth=" + config.indexBeamWidth
                    + " similarity=" + config.effectiveSimilarity() + " fusedPQ=true";
            out.info("Executing (pre-ingest): " + indexSql);
            indexWallSecs = runWithProgress(out, "index_creation", "=== INDEX CREATION (pre-ingest) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(indexSql);
                }
            });
        }

        // Phase 4: Ingestion
        if (!config.skipIngest) {
            int toIngest = actualRows - config.resumeFrom;
            if (toIngest <= 0) {
                out.info("resumeFrom (" + config.resumeFrom + ") >= rows (" + actualRows + "), nothing to ingest.");
            } else {
            out.header("=== INGESTION PHASE ===");
            out.phaseStart("ingest");
            if (config.resumeFrom > 0) {
                out.info("Resuming from position " + config.resumeFrom + ", ingesting " + toIngest + " rows.");
            }
            MetricsCollector ingestMetrics = new MetricsCollector();
            AtomicReference<String> ingestStatus = new AtomicReference<>("");

            BlockingQueue<float[]> ingestQueue = new ArrayBlockingQueue<>(1000);
            AtomicBoolean producerDone = new AtomicBoolean(false);
            AtomicLong rowId = new AtomicLong(config.resumeFrom);
            AtomicLong commitsTotal = new AtomicLong(0);
            AtomicLong commitsRecovered = new AtomicLong(0);

            long ingestStart = System.nanoTime();

            ExecutorService ingestPool = Executors.newFixedThreadPool(config.ingestThreads);
            for (int t = 0; t < config.ingestThreads; t++) {
                ingestPool.submit(new IngestionWorker(config, ingestQueue, producerDone, rowId, ingestMetrics,
                        ingestStatus, ingestStart, commitsTotal, commitsRecovered));
            }

            // Progress display thread runs during the entire ingestion
            AtomicBoolean ingestDone = new AtomicBoolean(false);
            final int totalRowsTarget = config.numRows;
            Thread progressThread = new Thread(() -> {
                Runtime rt = Runtime.getRuntime();
                while (!ingestDone.get()) {
                    double elapsed = (System.nanoTime() - ingestStart) / 1e9;
                    long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                    long maxMb = rt.maxMemory() / (1024 * 1024);
                    long rowsIngested = rowId.get() - config.resumeFrom;
                    double opsPerSec = elapsed > 0 ? rowsIngested / elapsed : 0.0;
                    long remaining = Math.max(0L, totalRowsTarget - (config.resumeFrom + rowsIngested));
                    double etaSecs = opsPerSec > 0 ? remaining / opsPerSec : 0.0;

                    LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
                    fields.put("rows", rowsIngested);
                    fields.put("total", (long) totalRowsTarget);
                    fields.put("ops_per_sec", opsPerSec);
                    fields.put("eta_s", etaSecs);
                    fields.put("commits", commitsTotal.get());
                    fields.put("recovered_commits", commitsRecovered.get());
                    fields.put("heap_used_mb", usedMb);
                    fields.put("heap_max_mb", maxMb);

                    String spinnerLine = String.format("heap: %d/%d MB | %s", usedMb, maxMb, ingestStatus.get());

                    out.progress("ingest", elapsed, spinnerLine, fields);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            });
            progressThread.setDaemon(true);
            progressThread.start();

            // Optional status thread: every statusIntervalSeconds, query the server's
            // syslogstatus/systablestats/sysindexstatus tables and emit a [status] line.
            // Independent of the progress thread so slow server queries don't stall progress.
            Thread statusThread = null;
            if (config.statusIntervalSeconds > 0) {
                final long statusIntervalMs = (long) config.statusIntervalSeconds * 1000L;
                final MetricsCollector ingestMetricsForStatus = ingestMetrics;
                final long ingestStartForStatus = ingestStart;
                final AtomicLong commitsTotalForStatus = commitsTotal;
                final AtomicLong commitsRecoveredForStatus = commitsRecovered;
                statusThread = new Thread(() -> {
                    ServerStatusSampler sampler = new ServerStatusSampler(config);
                    long nextSample = System.currentTimeMillis() + statusIntervalMs;
                    while (!ingestDone.get()) {
                        try {
                            long now = System.currentTimeMillis();
                            if (now < nextSample) {
                                Thread.sleep(Math.min(500L, nextSample - now));
                                continue;
                            }
                            nextSample = now + statusIntervalMs;
                            LinkedHashMap<String, Object> fields = sampler.sample();
                            LinkedHashMap<String, Object> commits = new LinkedHashMap<>();
                            commits.put("total", commitsTotalForStatus.get());
                            commits.put("recovered", commitsRecoveredForStatus.get());
                            MetricsCollector.Stats s = ingestMetricsForStatus.computeStats();
                            commits.put("last_ms", round2(ingestMetricsForStatus.getLastNanos() / 1e6));
                            commits.put("avg_ms", round2(s.meanNanos() / 1e6));
                            commits.put("p50_ms", round2(s.p50Nanos() / 1e6));
                            commits.put("p99_ms", round2(s.p99Nanos() / 1e6));
                            commits.put("max_ms", round2(s.maxNanos() / 1e6));
                            fields.put("commits", commits);
                            double elapsed = (System.nanoTime() - ingestStartForStatus) / 1e9;
                            out.status("ingest", elapsed, fields);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }, "vector-bench-status");
                statusThread.setDaemon(true);
                statusThread.start();
            }

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
            if (statusThread != null) {
                statusThread.join();
            }
            double ingestSecs = (System.nanoTime() - ingestStart) / 1e9;
            out.phaseDone("ingest", ingestSecs);

            ingestionWallSecs = ingestSecs;
            ingestionRows = rowId.get() - config.resumeFrom;
            ingestionThroughput = ingestionRows / ingestSecs;
            ingestionLatency = ingestMetrics.computeStats();

            if (!out.suppressesText()) {
                System.out.printf("=== INGESTION RESULTS ===%n");
                System.out.printf("Rows: %d | Wall time: %.1fs | Throughput: %.0f ops/s%n",
                        ingestionRows, ingestSecs, ingestionThroughput);
                System.out.printf("Threads: %d | Batch size: %d | Max ops/s: %s%n", config.ingestThreads, config.batchSize,
                        config.ingestMaxOpsPerSecond > 0 ? config.ingestMaxOpsPerSecond : "unlimited");
                ingestionLatency.print("INGESTION LATENCY");
            }

            // Verify row count matches ingested records
            if (!config.skipVerify) {
                long expectedRows = rowId.get();
                long[] actualCount = {0};
                runWithProgress(out, "verification", "=== VERIFICATION (COUNT) ===", () -> {
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
                out.info(String.format("Verification OK: %d rows in table", actualCount[0]));
            }
            } // end toIngest > 0
        } else {
            out.info("Skipping ingestion phase.");
        }

        // Phase 4b: Checkpoint after ingestion
        if (config.checkpoint && !config.skipIngest) {
            out.info("Executing checkpoint with timeout " + config.checkpointTimeoutSeconds + "s ...");
            checkpointPostIngestSecs = runWithProgress(out, "checkpoint_post_ingest", "=== CHECKPOINT (post-ingest) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("EXECUTE CHECKPOINT 'herd', " + config.checkpointTimeoutSeconds);
                }
            });
        }

        // Phase 5: Index creation (post-ingest, unless already created before ingestion)
        if (!config.skipIndex && !config.indexBeforeIngest) {
            String indexSql = "CREATE VECTOR INDEX vidx ON " + config.tableName + "(vec)"
                    + " WITH m=" + config.indexM
                    + " beamWidth=" + config.indexBeamWidth
                    + " similarity=" + config.effectiveSimilarity() + " fusedPQ=true";
            out.info("Executing: " + indexSql);
            indexWallSecs = runWithProgress(out, "index_creation", "=== INDEX CREATION ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute(indexSql);
                }
            });
        } else if (config.skipIndex) {
            out.info("Skipping index creation.");
        }

        // Phase 5b: Checkpoint after index creation
        if (config.checkpoint && !config.skipIndex && !config.indexBeforeIngest) {
            out.info("Executing checkpoint with timeout " + config.checkpointTimeoutSeconds + "s ...");
            checkpointPostIndexSecs = runWithProgress(out, "checkpoint_post_index", "=== CHECKPOINT (post-index) ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("EXECUTE CHECKPOINT 'herd', " + config.checkpointTimeoutSeconds);
                }
            });
        }

        // Phase 5c: Wait for external tailers (indexing services) to catch up.
        // Without this barrier, ANN queries can miss recently inserted vectors because the
        // VectorIndexManager checkpoint no longer blocks on tailer catch-up.
        if (config.waitForIndexes) {
            out.info("Waiting for indexing services to catch up (timeout " + config.waitForIndexesTimeoutSeconds + "s)...");
            waitForIndexesSecs = runWithProgress(out, "wait_for_indexes", "=== WAIT FOR INDEXES ===", () -> {
                try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password);
                     Statement stmt = conn.createStatement()) {
                    stmt.execute("EXECUTE WAITFORINDEXES 'herd', " + config.waitForIndexesTimeoutSeconds);
                }
            });
        }

        // Phase 6: Queries
        out.header("=== QUERY PHASE ===");
        out.phaseStart("query");
        String queryTemplate = "SELECT id FROM " + config.tableName
                + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT " + config.topK;
        out.info("Query template: " + queryTemplate);
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
        while (!queryPool.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            double elapsed = (System.nanoTime() - queryStart) / 1e9;
            long queriesDone = queryMetrics.getCount();
            double qps = elapsed > 0 ? queriesDone / elapsed : 0.0;
            MetricsCollector.Stats intermediateStats = queryMetrics.computeStats();
            LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
            fields.put("queries_done", queriesDone);
            fields.put("total", (long) actualQueries);
            fields.put("qps", qps);
            fields.put("latency_mean_ms", round2(intermediateStats.meanNanos() / 1e6));
            fields.put("latency_p50_ms", round2(intermediateStats.p50Nanos() / 1e6));
            fields.put("latency_p95_ms", round2(intermediateStats.p95Nanos() / 1e6));
            fields.put("latency_p99_ms", round2(intermediateStats.p99Nanos() / 1e6));
            fields.put("latency_max_ms", round2(intermediateStats.maxNanos() / 1e6));
            out.progress("query", elapsed, queryStatus.get(), fields);
        }
        double querySecs = (System.nanoTime() - queryStart) / 1e9;
        out.phaseDone("query", querySecs);

        queryWallSecs = querySecs;
        queriesRun = queryMetrics.getCount();
        queryThroughput = queriesRun / querySecs;
        queryLatency = queryMetrics.computeStats();

        if (!out.suppressesText()) {
            System.out.printf("=== QUERY RESULTS ===%n");
            System.out.printf("Queries: %d | Wall time: %.1fs | Throughput: %.0f qps%n",
                    queriesRun, querySecs, queryThroughput);
            System.out.printf("Threads: %d | Top-K: %d%n", config.queryThreads, config.topK);
            queryLatency.print("QUERY LATENCY");
        }

        // Phase 7: Recall
        if (groundTruth != null && !groundTruth.isEmpty()) {
            // Only compute recall for queries that have ground truth (non-cycled portion)
            List<List<Integer>> recallResults = queryResults.subList(0, Math.min(queryResults.size(), groundTruth.size()));
            recall = computeRecall(recallResults, groundTruth, config.topK);
            recallQueries = recallResults.size();
            out.info(String.format("Recall@%d: %.4f (computed on %d queries with ground truth)",
                    config.topK, recall, recallQueries));
            if (config.dataset == DatasetLoader.DatasetPreset.CUSTOM && loader.getCustomDescriptor() != null) {
                int gtK = loader.getCustomDescriptor().groundTruthK;
                if (config.topK > gtK) {
                    out.info(String.format("WARNING: topK=%d exceeds ground truth K=%d from descriptor — "
                            + "recall may be unreliable (ground truth has fewer neighbors than requested)",
                            config.topK, gtK));
                } else if (config.topKExplicit && config.topK != gtK) {
                    out.info(String.format("NOTE: topK=%d differs from descriptor groundTruthK=%d",
                            config.topK, gtK));
                }
            }
        }

        // Final summary
        double totalWallSecs = (System.nanoTime() - benchmarkStartNs) / 1e9;
        emitSummary(out, config, ingestionWallSecs, ingestionRows, ingestionThroughput, ingestionLatency,
                checkpointPostIngestSecs, indexWallSecs, checkpointPostIndexSecs,
                waitForIndexesSecs,
                queryWallSecs, queriesRun, queryThroughput, queryLatency,
                recall, recallK, recallQueries, totalWallSecs);

        out.done();
        System.exit(0);
    }

    /**
     * Emits the structured benchmark summary through the output abstraction. In text modes
     * this produces the same {@code phase=...} human-readable lines as the legacy
     * {@code printSummary} did, so {@code write-report.sh} continues to parse them; in JSON
     * mode each phase becomes a {@code phase_end} NDJSON event.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private static void emitSummary(BenchOutput out, Config config,
                                    double ingestionWallSecs, long ingestionRows, double ingestionThroughput,
                                    MetricsCollector.Stats ingestionLatency,
                                    double checkpointPostIngestSecs,
                                    double indexWallSecs,
                                    double checkpointPostIndexSecs,
                                    double waitForIndexesSecs,
                                    double queryWallSecs, long queriesRun, double queryThroughput,
                                    MetricsCollector.Stats queryLatency,
                                    double recall, int recallK, int recallQueries,
                                    double totalWallSecs) {

        // phase=ingestion
        if (ingestionWallSecs >= 0 && ingestionLatency != null) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(ingestionWallSecs));
            f.put("rows", ingestionRows);
            f.put("throughput_ops", round0(ingestionThroughput));
            f.put("threads", config.ingestThreads);
            f.put("batch_size", config.batchSize);
            f.put("latency_mean_ms", round2(ingestionLatency.meanNanos() / 1e6));
            f.put("latency_p50_ms", round2(ingestionLatency.p50Nanos() / 1e6));
            f.put("latency_p95_ms", round2(ingestionLatency.p95Nanos() / 1e6));
            f.put("latency_p99_ms", round2(ingestionLatency.p99Nanos() / 1e6));
            f.put("latency_max_ms", round2(ingestionLatency.maxNanos() / 1e6));
            out.phaseEnd("ingestion", f);
        } else {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("status", "skipped");
            out.phaseEnd("ingestion", f);
        }

        if (checkpointPostIngestSecs >= 0) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(checkpointPostIngestSecs));
            out.phaseEnd("checkpoint_post_ingest", f);
        }

        if (indexWallSecs >= 0) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(indexWallSecs));
            f.put("m", config.indexM);
            f.put("beam_width", config.indexBeamWidth);
            out.phaseEnd("index_creation", f);
        } else {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("status", "skipped");
            out.phaseEnd("index_creation", f);
        }

        if (checkpointPostIndexSecs >= 0) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(checkpointPostIndexSecs));
            out.phaseEnd("checkpoint_post_index", f);
        }

        if (waitForIndexesSecs >= 0) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(waitForIndexesSecs));
            f.put("timeout_s", config.waitForIndexesTimeoutSeconds);
            out.phaseEnd("wait_for_indexes", f);
        }

        if (queryLatency != null) {
            LinkedHashMap<String, Object> f = new LinkedHashMap<>();
            f.put("wall_time_s", round1(queryWallSecs));
            f.put("queries", queriesRun);
            f.put("throughput_qps", round0(queryThroughput));
            f.put("threads", config.queryThreads);
            f.put("top_k", config.topK);
            f.put("latency_mean_ms", round2(queryLatency.meanNanos() / 1e6));
            f.put("latency_p50_ms", round2(queryLatency.p50Nanos() / 1e6));
            f.put("latency_p95_ms", round2(queryLatency.p95Nanos() / 1e6));
            f.put("latency_p99_ms", round2(queryLatency.p99Nanos() / 1e6));
            f.put("latency_max_ms", round2(queryLatency.maxNanos() / 1e6));
            if (recall >= 0) {
                f.put("recall@" + recallK, round4(recall));
                f.put("recall_queries", recallQueries);
            }
            out.phaseEnd("query", f);
        }

        LinkedHashMap<String, Object> summaryFields = new LinkedHashMap<>();
        summaryFields.put("dataset", config.dataset.name());
        summaryFields.put("rows", (long) config.numRows);
        summaryFields.put("similarity", config.effectiveSimilarity());
        summaryFields.put("total_wall_time_s", round1(totalWallSecs));
        out.summary(summaryFields);
    }

    private static double round0(double v) {
        return Math.round(v);
    }

    private static double round1(double v) {
        return Math.round(v * 10.0) / 10.0;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static double round4(double v) {
        return Math.round(v * 10000.0) / 10000.0;
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
