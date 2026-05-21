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

import com.google.common.util.concurrent.RateLimiter;
import herddb.codec.RecordSerializer;
import herddb.index.vector.VectorIndexManager;
import herddb.indexing.IndexingPushClient;
import herddb.indexing.proto.PushEntriesResponse;
import herddb.indexing.proto.SearchResponse;
import herddb.indexing.proto.SearchResult;
import herddb.log.LogEntry;
import herddb.log.LogEntryFactory;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The {@code --protocol grpc} ingestion path of VectorBench.
 *
 * <p>Instead of going through JDBC and a HerdDB server, this mode talks
 * straight to a single indexing service running with
 * {@code indexing.log.type=push}: it serializes HerdDB {@link LogEntry}
 * objects itself (CREATE_TABLE, CREATE_INDEX, BEGIN/INSERT/COMMIT) and pushes
 * them over the {@code PushEntries} gRPC RPC. No HerdDB server, BookKeeper or
 * commit log is involved.
 *
 * <p>It is ingestion + optional recall: a transaction wraps each batch of
 * inserts, the entries are serialized into pooled direct {@link ByteBuf}s to
 * keep the client's memory footprint low, and a single thread issues every
 * push so the indexing service sees a strictly increasing LSN stream.
 * Verification polls the index status over gRPC and checks the indexed
 * vector count; the query phase (when enabled) drives {@code Search} RPCs
 * against the same service and computes recall@K against the dataset's
 * ground truth.
 */
public final class GrpcBench {

    /** Name of the vector index this mode creates, populates and verifies. */
    private static final String INDEX_NAME = "vidx";

    /**
     * Maximum time to wait for the indexing service's tailer to apply every
     * pushed entry once {@link #ingest} has finished pushing.
     *
     * <p>The {@code PushEntries} RPC only guarantees that entries have been
     * <em>accepted into the bounded push buffer</em> by the time it returns —
     * the IS tailer thread applies them asynchronously. In push mode the IS
     * does not lag a real commit log, so once the last buffered entry has been
     * accepted the apply loop typically finishes within milliseconds. A short
     * fixed cap is enough to absorb that lag without pretending we need an
     * open-ended wait.
     *
     * <p>Deliberately not driven by {@code --wait-for-indexes-timeout}: that
     * flag is a JDBC-only concept (server-side {@code WAITFORINDEXES}) and is
     * meaningless for push mode, where there is no external tailer to wait
     * for. Promoting this to a CLI flag is intentionally avoided so the
     * "always up-to-date" contract of push mode stays visible.
     */
    private static final long VERIFY_CATCHUP_TIMEOUT_MS = 30_000L;

    /** Poll interval used while waiting for the tailer to apply the last buffered entries. */
    private static final long VERIFY_POLL_INTERVAL_MS = 200L;

    private GrpcBench() {
    }

    /**
     * Entry point for {@code --protocol grpc}. Loads the dataset, opens the
     * gRPC client and drives ingestion, optionally followed by verification
     * and a query / recall phase, then exits the JVM (mirroring the JDBC
     * path's terminal {@code System.exit(0)}).
     */
    static void run(Config config, BenchOutput out, long benchmarkStartNs, BenchRuntime runtime) throws Exception {
        out.header("=== gRPC PUSH MODE (indexing service " + config.grpcEndpoint + ") ===");
        if (config.resumeFromAuto) {
            out.info("WARNING: --resume-from auto is not supported with --protocol grpc; "
                    + "ingestion starts at row " + config.resumeFrom + ".");
        }
        out.info("Loading dataset...");
        DatasetLoader loader = new DatasetLoader(config.datasetDir, config.dataset, config.datasetUrl);
        loader.ensureDataset();

        long toIngest = Math.max(0L, config.numRows - config.resumeFrom);
        long pushed;
        double recall = -1.0;
        long recallQueries = 0;
        long queriesRun = 0;
        double queryWallSecs = -1.0;
        try (IndexingPushClient client = new IndexingPushClient(config.grpcEndpoint);
                DatasetLoader.VectorStream stream = loader.streamBaseVectors(config.resumeFrom, toIngest)) {
            pushed = ingest(client, config, out, stream.iterator(), config.resumeFrom, toIngest, runtime);

            if (!config.skipQuery) {
                QueryPhaseResult q = runQueryPhase(client, config, out, loader, runtime, pushed);
                if (q != null) {
                    recall = q.recall;
                    recallQueries = q.recallQueries;
                    queriesRun = q.queries;
                    queryWallSecs = q.wallSecs;
                }
            } else {
                out.info("Skipping query/recall phase (--skip-query).");
            }
        }
        double totalSecs = (System.nanoTime() - benchmarkStartNs) / 1e9;
        LinkedHashMap<String, Object> summary = new LinkedHashMap<>();
        summary.put("protocol", "grpc");
        summary.put("endpoint", config.grpcEndpoint);
        summary.put("dataset", config.dataset.name());
        summary.put("rows", pushed);
        if (queriesRun > 0) {
            summary.put("queries", queriesRun);
            summary.put("query_wall_s", Math.round(queryWallSecs * 10.0) / 10.0);
            summary.put("qps", queryWallSecs > 0
                    ? Math.round((queriesRun / queryWallSecs) * 10.0) / 10.0 : 0.0);
        }
        if (recall >= 0.0) {
            summary.put("recall_at_k", Math.round(recall * 10000.0) / 10000.0);
            summary.put("recall_queries", recallQueries);
            summary.put("top_k", config.topK);
        }
        summary.put("total_wall_time_s", Math.round(totalSecs * 10.0) / 10.0);
        out.summary(summary);
        // Final status: indicate we are done so the admin API stops reporting
        // an in-progress phase after run() has returned.
        if (runtime != null) {
            runtime.setStatusSupplier(() -> Collections.singletonMap("phase", "done"));
        }
        out.done();
        System.exit(0);
    }

    /**
     * Pushes the schema (CREATE_TABLE + CREATE_INDEX) and then streams
     * {@code totalRows} INSERTs from {@code vectors}, one transaction per
     * {@code config.batchSize} rows, and finally verifies the indexed vector
     * count over gRPC.
     *
     * <p>Package-private and free of {@code System.exit} so integration tests
     * can drive it directly with a synthetic vector source. The {@code runtime}
     * argument is optional ({@code null} permitted): when present, per-phase
     * status fields are published to the admin HTTP API as the bench moves
     * through {@code schema} → {@code ingest} → {@code verification}.
     *
     * @return the number of vector rows pushed
     */
    static long ingest(IndexingPushClient client, Config config, BenchOutput out,
                       Iterator<float[]> vectors, long firstRowId, long totalRows) throws Exception {
        return ingest(client, config, out, vectors, firstRowId, totalRows, null);
    }

    /**
     * Same as {@link #ingest(IndexingPushClient, Config, BenchOutput, Iterator, long, long)}
     * but accepts a {@link BenchRuntime} so the admin HTTP API can observe
     * each phase transition (issue #632).
     */
    static long ingest(IndexingPushClient client, Config config, BenchOutput out,
                       Iterator<float[]> vectors, long firstRowId, long totalRows,
                       BenchRuntime runtime) throws Exception {
        Table table = buildTable(config);
        Index index = buildIndex(config);

        // The client owns LSN assignment. A fresh ledger id per run keeps the
        // stream strictly after any watermark a reused indexing service may
        // already hold; offsets are monotonic within the run.
        long ledgerId = System.currentTimeMillis();
        long[] offset = {1L};

        out.phaseStart("schema");
        setSimpleStatus(runtime, "schema");
        out.info("Pushing CREATE TABLE " + config.tableName + " + CREATE VECTOR INDEX " + INDEX_NAME);
        PushEntriesResponse schemaResp = pushBatch(client, ledgerId, offset, Arrays.asList(
                LogEntryFactory.createTable(table, null),
                LogEntryFactory.createIndex(index, null)));
        if (schemaResp.getAcceptedCount() != 2) {
            throw new IllegalStateException("indexing service accepted "
                    + schemaResp.getAcceptedCount() + " of 2 schema entries");
        }
        // Baseline so verification works even against a non-fresh service.
        long baseline = client.getIndexStatus(TableSpace.DEFAULT, config.tableName, INDEX_NAME)
                .getVectorCount();
        out.phaseDone("schema", 0.0);

        out.header("=== INGESTION PHASE (gRPC push) ===");
        out.phaseStart("ingest");
        long ingestStart = System.nanoTime();
        // AtomicLongs so the status supplier (read concurrently from Jetty
        // threads in BenchRuntime) sees a coherent snapshot rather than a
        // half-updated long.
        AtomicLong pushedRows = new AtomicLong(0);
        AtomicLong pushCallsCounter = new AtomicLong(0);
        if (runtime != null) {
            final long ingestStartNs = ingestStart;
            runtime.setStatusSupplier(() -> {
                Runtime rt = Runtime.getRuntime();
                long rows = pushedRows.get();
                double elapsed = (System.nanoTime() - ingestStartNs) / 1e9;
                double opsPerSec = elapsed > 0 ? rows / elapsed : 0.0;
                LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                m.put("phase", "ingest");
                m.put("rows", rows);
                m.put("total", totalRows);
                m.put("ops_per_sec", opsPerSec);
                m.put("push_calls", pushCallsCounter.get());
                m.put("heap_used_mb", (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024));
                m.put("heap_max_mb", rt.maxMemory() / (1024 * 1024));
                return m;
            });
        }
        long rowId = firstRowId;
        long pushed = 0;
        long pushCalls = 0;
        long txId = 1;
        long lastProgressNs = ingestStart;

        while (vectors.hasNext()) {
            // One transaction per push batch: BEGIN + N INSERTs + COMMIT.
            List<LogEntry> batch = new ArrayList<>(config.batchSize + 2);
            batch.add(LogEntryFactory.beginTransaction(txId));
            int n = 0;
            while (n < config.batchSize && vectors.hasNext()) {
                float[] v = vectors.next();
                Record record = RecordSerializer.makeRecord(table, "id", rowId, "vec", v);
                batch.add(new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT,
                        txId, table.tableId, record.key, record.value));
                rowId++;
                n++;
            }
            batch.add(LogEntryFactory.commitTransaction(txId));
            txId++;

            PushEntriesResponse resp = pushBatch(client, ledgerId, offset, batch);
            pushed += n;
            pushCalls++;
            pushedRows.set(pushed);
            pushCallsCounter.set(pushCalls);
            if (resp.getAcceptedCount() != batch.size()) {
                throw new IllegalStateException("indexing service accepted "
                        + resp.getAcceptedCount() + " of " + batch.size() + " pushed entries");
            }

            long now = System.nanoTime();
            if (now - lastProgressNs > 1_000_000_000L || !vectors.hasNext()) {
                lastProgressNs = now;
                double elapsed = (now - ingestStart) / 1e9;
                double opsPerSec = elapsed > 0 ? pushed / elapsed : 0.0;
                LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
                fields.put("rows", pushed);
                fields.put("total", totalRows);
                fields.put("push_calls", pushCalls);
                fields.put("ops_per_sec", opsPerSec);
                out.progress("ingest", elapsed, String.format(
                        "pushed %d/%d rows | %.0f ops/s | %d push calls",
                        pushed, totalRows, opsPerSec, pushCalls), fields);
            }
        }

        double ingestSecs = (System.nanoTime() - ingestStart) / 1e9;
        out.phaseDone("ingest", ingestSecs);
        out.info(String.format("Pushed %d rows in %d transactions over %.1fs (%.0f ops/s)",
                pushed, pushCalls, ingestSecs, ingestSecs > 0 ? pushed / ingestSecs : 0.0));

        if (config.skipVerify) {
            out.info("Skipping vector-count verification (--skip-verify).");
        } else {
            verifyVectorCount(client, config, out, baseline + pushed, runtime);
        }
        return pushed;
    }

    /**
     * Polls {@code GetIndexStatus} until the indexed vector count reaches
     * {@code expected}. {@code PushEntries} only guarantees buffer-accept,
     * not apply, so a tiny lag is possible right after the last batch
     * returns — but in push mode the IS is "always up to date" and the
     * tailer drains in milliseconds, so we bound the wait at
     * {@link #VERIFY_CATCHUP_TIMEOUT_MS} (deliberately short, and explicitly
     * <em>not</em> driven by {@code --wait-for-indexes-timeout}, which is a
     * JDBC concept).
     */
    private static void verifyVectorCount(IndexingPushClient client, Config config,
                                          BenchOutput out, long expected,
                                          BenchRuntime runtime) throws InterruptedException {
        LongSupplier counter = () -> client.getIndexStatus(TableSpace.DEFAULT, config.tableName, INDEX_NAME)
                .getVectorCount();
        verifyVectorCount(counter, out, expected, runtime, VERIFY_CATCHUP_TIMEOUT_MS, VERIFY_POLL_INTERVAL_MS);
    }

    /**
     * Polling-loop core, factored out so unit tests can drive it with a
     * synthetic counter (no live gRPC server) and pin the timing contract:
     * the deadline is the {@code timeoutMs} value passed in, NOT
     * {@code config.waitForIndexesTimeoutSeconds}.
     *
     * <p>Package-private for {@link GrpcBenchTest}.
     */
    static void verifyVectorCount(LongSupplier currentCount, BenchOutput out, long expected,
                                  BenchRuntime runtime, long timeoutMs, long pollIntervalMs)
            throws InterruptedException {
        out.phaseStart("verification");
        long verifyStart = System.nanoTime();
        AtomicLong lastObserved = new AtomicLong(-1);
        if (runtime != null) {
            final long expectedFinal = expected;
            runtime.setStatusSupplier(() -> {
                LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                m.put("phase", "verification");
                m.put("vector_count", lastObserved.get());
                m.put("expected", expectedFinal);
                return m;
            });
        }
        long deadlineMs = System.currentTimeMillis() + timeoutMs;
        long last = -1;
        boolean firstAttempt = true;
        while (true) {
            last = currentCount.getAsLong();
            lastObserved.set(last);
            if (last >= expected) {
                out.phaseDone("verification", (System.nanoTime() - verifyStart) / 1e9);
                out.info("Verification OK: index '" + INDEX_NAME + "' reports " + last + " vectors");
                return;
            }
            if (firstAttempt) {
                // Only log a verification "progress" line when we actually had
                // to wait — the steady-state push-mode case is a single
                // sub-millisecond status call.
                double elapsed = (System.nanoTime() - verifyStart) / 1e9;
                LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
                fields.put("vector_count", last);
                fields.put("expected", expected);
                out.progress("verification", elapsed,
                        "indexed " + last + "/" + expected + " vectors — waiting for tailer to apply last batch",
                        fields);
                firstAttempt = false;
            }
            if (System.currentTimeMillis() >= deadlineMs) {
                break;
            }
            Thread.sleep(pollIntervalMs);
        }
        throw new IllegalStateException("index vector count reached only " + last
                + " of the expected " + expected + " within " + timeoutMs
                + " ms — the indexing-service tailer has not applied every pushed entry."
                + " In push mode this is expected to take milliseconds; a longer lag indicates"
                + " a stalled tailer (a long-running checkpoint/compaction or a bug)");
    }

    /**
     * Result of {@link #runQueryPhase}: aggregate counts and the computed recall.
     * Returned by the query phase so the bench summary can include the same
     * fields the JDBC path reports.
     */
    static final class QueryPhaseResult {
        final long queries;
        final double wallSecs;
        final double recall;
        final long recallQueries;

        QueryPhaseResult(long queries, double wallSecs, double recall, long recallQueries) {
            this.queries = queries;
            this.wallSecs = wallSecs;
            this.recall = recall;
            this.recallQueries = recallQueries;
        }
    }

    /**
     * Runs the query / recall phase over gRPC. Loads {@code config.queryCount}
     * query vectors and ground-truth records from {@code loader}, drives
     * {@code Search} RPCs in parallel across {@code config.queryThreads}
     * threads at {@code config.queryMaxOpsPerSecond} aggregate rate, then
     * computes recall@K against the dataset's ground truth. Returns
     * {@code null} if no query vectors are available (e.g. a CUSTOM dataset
     * with no query file), and the caller treats that as "phase skipped".
     */
    static QueryPhaseResult runQueryPhase(IndexingPushClient client, Config config, BenchOutput out,
                                          DatasetLoader loader, BenchRuntime runtime,
                                          long ingestedRows) throws Exception {
        List<float[]> queryVectors = loader.loadQueryVectors(config.queryCount);
        if (queryVectors == null || queryVectors.isEmpty()) {
            out.info("No query vectors available — skipping query/recall phase.");
            return null;
        }
        out.info("Loaded " + queryVectors.size() + " query vectors for the query phase");

        List<int[]> groundTruth;
        try {
            // Recall is only meaningful when ground truth matches the
            // baseline. Use the actual row count (resumeFrom + ingestedRows)
            // so CUSTOM datasets with prefix checkpoints pick the right file.
            long baseRowCount = config.resumeFrom + ingestedRows;
            groundTruth = loader.loadGroundTruth(queryVectors.size(), baseRowCount);
            out.info("Loaded " + groundTruth.size() + " ground-truth entries (top-K up to "
                    + (groundTruth.isEmpty() ? 0 : groundTruth.get(0).length) + ")");
        } catch (java.io.IOException e) {
            // Ground truth is optional — if the dataset has no file matching
            // the row count we still run the query phase (for QPS / latency)
            // but skip recall.
            out.info("Ground truth unavailable: " + e.getMessage() + " — running queries without recall.");
            groundTruth = null;
        }

        out.header("=== QUERY PHASE (gRPC search) ===");
        out.phaseStart("query");
        Table table = buildTable(config);
        int total = queryVectors.size();
        List<List<Integer>> results = new ArrayList<>(Collections.nCopies(total, null));
        MetricsCollector queryMetrics = new MetricsCollector();
        long queryStart = System.nanoTime();
        // Per-iteration re-read so an admin-issued query-rate change takes
        // effect on the next query, matching the JDBC QueryWorker behaviour.
        Supplier<RateLimiter> rateLimiterSupplier = runtime != null
                ? runtime::queryRateLimiter
                : () -> RateLimiter.create(config.queryMaxOpsPerSecond > 0
                        ? config.queryMaxOpsPerSecond : BenchRuntime.UNLIMITED_RATE);

        if (runtime != null) {
            final long queryStartNs = queryStart;
            runtime.setStatusSupplier(() -> {
                LinkedHashMap<String, Object> m = new LinkedHashMap<>();
                double elapsed = (System.nanoTime() - queryStartNs) / 1e9;
                long done = queryMetrics.getCount();
                double qps = elapsed > 0 ? done / elapsed : 0.0;
                m.put("phase", "query");
                m.put("queries_done", done);
                m.put("total", (long) total);
                m.put("qps", qps);
                m.put("top_k", config.topK);
                MetricsCollector.Stats s = queryMetrics.computeStats();
                LinkedHashMap<String, Object> latency = new LinkedHashMap<>();
                latency.put("mean_ms", s.meanNanos() / 1e6);
                latency.put("p50_ms", s.p50Nanos() / 1e6);
                latency.put("p95_ms", s.p95Nanos() / 1e6);
                latency.put("p99_ms", s.p99Nanos() / 1e6);
                latency.put("max_ms", s.maxNanos() / 1e6);
                m.put("latency", latency);
                return m;
            });
        }

        int threads = Math.max(1, config.queryThreads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>(threads);
            int chunk = total / threads;
            for (int t = 0; t < threads; t++) {
                final int startIdx = t * chunk;
                final int endIdx = (t == threads - 1) ? total : startIdx + chunk;
                futures.add(pool.submit(() ->
                        searchRange(client, config, table, queryVectors, startIdx, endIdx,
                                rateLimiterSupplier, queryMetrics, results)));
            }
            pool.shutdown();
            // Progress loop — same shape as VectorBench's JDBC query progress
            // line, just emitted from this thread since there is no JDBC pool.
            while (!pool.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                double elapsed = (System.nanoTime() - queryStart) / 1e9;
                long done = queryMetrics.getCount();
                double qps = elapsed > 0 ? done / elapsed : 0.0;
                LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
                fields.put("queries_done", done);
                fields.put("total", (long) total);
                fields.put("qps", qps);
                MetricsCollector.Stats stats = queryMetrics.computeStats();
                fields.put("latency_mean_ms", stats.meanNanos() / 1e6);
                fields.put("latency_p99_ms", stats.p99Nanos() / 1e6);
                out.progress("query", elapsed, String.format(
                        "queried %d/%d | %.0f qps | mean %.2f ms | p99 %.2f ms",
                        done, total, qps,
                        stats.meanNanos() / 1e6, stats.p99Nanos() / 1e6), fields);
            }
            for (Future<?> f : futures) {
                // Surface any worker exception (search RPC failure, deserialization bug).
                f.get();
            }
        } finally {
            if (!pool.isTerminated()) {
                pool.shutdownNow();
            }
        }
        double querySecs = (System.nanoTime() - queryStart) / 1e9;
        out.phaseDone("query", querySecs);

        long queriesRun = queryMetrics.getCount();
        double qps = querySecs > 0 ? queriesRun / querySecs : 0.0;
        out.info(String.format("Ran %d searches in %.1fs (%.0f qps), top-K=%d",
                queriesRun, querySecs, qps, config.topK));

        double recall = -1.0;
        long recallQueries = 0;
        if (groundTruth != null && !groundTruth.isEmpty()) {
            // Match the JDBC path's recall semantics: compare only the prefix
            // of result rows that have ground truth.
            List<List<Integer>> recallResults = results.subList(0, Math.min(results.size(), groundTruth.size()));
            recall = computeRecall(recallResults, groundTruth, config.topK);
            recallQueries = recallResults.size();
            out.info(String.format("Recall@%d: %.4f (computed on %d queries)",
                    config.topK, recall, recallQueries));
        } else {
            out.info("Recall@" + config.topK + ": not computed (no ground truth available)");
        }
        return new QueryPhaseResult(queriesRun, querySecs, recall, recallQueries);
    }

    /**
     * Runs the {@code Search} RPC for {@code queryVectors[startIdx..endIdx)},
     * deserializing each result's serialized primary key into the row's
     * integer id (single-column LONG PK — fits in int while the bench's row
     * count stays below 2^31). Populates {@code results[i]} with the list of
     * matched ids in rank order so the caller can compute recall.
     */
    private static void searchRange(IndexingPushClient client, Config config, Table table,
                                    List<float[]> queryVectors, int startIdx, int endIdx,
                                    Supplier<RateLimiter> rateLimiterSupplier,
                                    MetricsCollector metrics, List<List<Integer>> results) {
        for (int i = startIdx; i < endIdx; i++) {
            int k = config.topK;
            RateLimiter rl = rateLimiterSupplier.get();
            if (rl != null) {
                rl.acquire(1);
            }
            long start = System.nanoTime();
            SearchResponse response = client.search(TableSpace.DEFAULT, config.tableName,
                    INDEX_NAME, queryVectors.get(i), k);
            long elapsed = System.nanoTime() - start;
            metrics.record(elapsed);

            List<Integer> ids = new ArrayList<>(response.getResultsCount());
            for (SearchResult r : response.getResultsList()) {
                Bytes pk = Bytes.from_array(r.getPrimaryKey().toByteArray());
                Object idValue = RecordSerializer.deserializePrimaryKey(pk, table);
                // Single-column LONG PK: idValue is a Long. Recall arithmetic
                // works on int ids (ground truth is int[]); a bench with more
                // than 2^31 rows would need int64 recall comparisons too — far
                // beyond what fits in a single IS today.
                long id = ((Number) idValue).longValue();
                ids.add((int) id);
            }
            results.set(i, ids);
        }
    }

    /**
     * Recall@K: fraction of ground-truth-top-K ids that appear in the result
     * top-K. Identical semantics to {@code VectorBench.computeRecall} so the
     * gRPC and JDBC paths report the same metric.
     */
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

    private static void setSimpleStatus(BenchRuntime runtime, String phase) {
        if (runtime == null) {
            return;
        }
        runtime.setStatusSupplier(() -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("phase", phase);
            return m;
        });
    }

    /**
     * Pushes a batch, serializing each entry with
     * {@link LogEntry#serializeAsByteBuf()}.
     */
    private static PushEntriesResponse pushBatch(IndexingPushClient client, long ledgerId,
                                                 long[] offset, List<LogEntry> entries) {
        return pushBatch(client, ledgerId, offset, entries, LogEntry::serializeAsByteBuf);
    }

    /**
     * Serializes each entry into a pooled direct {@link ByteBuf} with
     * {@code serializer}, assigns it the next LSN, pushes the batch, and
     * releases <em>every</em> buffer once the (zero-copy) RPC has returned —
     * including when serializing a later entry throws mid-batch.
     *
     * <p>The serializer is a parameter so tests can exercise that buffer
     * lifecycle without a live indexing service.
     */
    static PushEntriesResponse pushBatch(IndexingPushClient client, long ledgerId, long[] offset,
                                         List<LogEntry> entries,
                                         Function<LogEntry, ByteBuf> serializer) {
        List<LogSequenceNumber> lsns = new ArrayList<>(entries.size());
        List<ByteBuf> bufs = new ArrayList<>(entries.size());
        try {
            for (LogEntry entry : entries) {
                lsns.add(new LogSequenceNumber(ledgerId, offset[0]++));
                bufs.add(serializer.apply(entry));
            }
            return client.pushEntries(lsns, bufs);
        } finally {
            for (ByteBuf buf : bufs) {
                buf.release();
            }
        }
    }

    private static Table buildTable(Config config) {
        return Table.builder()
                .name(config.tableName)
                .tablespace(TableSpace.DEFAULT)
                // LONG, not INTEGER: a bench may ingest more than 2^31 rows and
                // the primary key must stay unique.
                .column("id", ColumnTypes.LONG)
                .column("vec", ColumnTypes.FLOATARRAY)
                .primaryKey("id")
                .build();
    }

    private static Index buildIndex(Config config) {
        Index.Builder builder = Index.builder()
                .name(INDEX_NAME)
                .table(config.tableName)
                .tablespace(TableSpace.DEFAULT)
                .type(Index.TYPE_VECTOR)
                .column("vec", ColumnTypes.FLOATARRAY)
                .property(VectorIndexManager.PROP_M, String.valueOf(config.indexM))
                .property(VectorIndexManager.PROP_BEAM_WIDTH, String.valueOf(config.indexBeamWidth))
                .property(VectorIndexManager.PROP_SIMILARITY, config.effectiveSimilarity())
                .property(VectorIndexManager.PROP_FUSED_PQ, "true")
                .property(VectorIndexManager.PROP_NEIGHBOR_OVERFLOW,
                        String.valueOf(config.indexNeighborOverflow))
                .property(VectorIndexManager.PROP_ALPHA, String.valueOf(config.indexAlpha));
        if (config.indexNumShards > 1) {
            builder.property(VectorIndexManager.PROP_NUM_SHARDS, String.valueOf(config.indexNumShards));
        }
        return builder.build();
    }
}
