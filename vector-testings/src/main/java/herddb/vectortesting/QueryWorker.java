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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class QueryWorker implements Runnable {

    private final Config config;
    private final List<float[]> queryVectors;
    private final int startIdx;
    private final int endIdx;
    private final MetricsCollector metrics;
    /**
     * Per-query result storage used for recall computation. {@code null} when the
     * caller does not need recall (e.g. during-ingestion sampling rounds), in which
     * case results are discarded after timing is recorded.
     */
    private final List<List<Integer>> allResults;
    private final AtomicReference<String> statusLine;
    /**
     * Re-read per iteration so an admin-issued rate change (which swaps the
     * underlying {@link RateLimiter}) takes effect on the next query without
     * restarting the worker.
     */
    private final Supplier<RateLimiter> queryRateLimiter;
    /**
     * When {@code true}, a result set smaller than {@code topK} is logged but
     * does not abort the JVM. Use for during-ingestion sampling rounds where the
     * graph is only partially built and short results are expected. When
     * {@code false} (the default for post-ingest query phases), a short result
     * set is a hard error and the benchmark exits with non-zero status.
     */
    private final boolean tolerateShortResults;

    /**
     * Creates a worker for the normal post-ingest query phase.
     * Short result sets are treated as fatal errors ({@code System.exit(1)}).
     */
    public QueryWorker(Config config, List<float[]> queryVectors, int startIdx, int endIdx,
                       MetricsCollector metrics, List<List<Integer>> allResults,
                       AtomicReference<String> statusLine, Supplier<RateLimiter> queryRateLimiter) {
        this(config, queryVectors, startIdx, endIdx, metrics, allResults,
                statusLine, queryRateLimiter, false);
    }

    /**
     * Creates a worker with explicit control over short-result tolerance.
     * Pass {@code tolerateShortResults=true} for during-ingestion sampling so
     * partial result sets (graph not yet fully built) are accepted rather than
     * aborting the JVM.  Pass {@code allResults=null} when recall computation is
     * not needed (the caller discards results after metrics are recorded).
     */
    public QueryWorker(Config config, List<float[]> queryVectors, int startIdx, int endIdx,
                       MetricsCollector metrics, List<List<Integer>> allResults,
                       AtomicReference<String> statusLine, Supplier<RateLimiter> queryRateLimiter,
                       boolean tolerateShortResults) {
        this.config = config;
        this.queryVectors = queryVectors;
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.metrics = metrics;
        this.allResults = allResults;
        this.statusLine = statusLine;
        this.queryRateLimiter = queryRateLimiter;
        this.tolerateShortResults = tolerateShortResults;
    }

    @Override
    public void run() {
        try (Connection conn = DriverManager.getConnection(config.effectiveJdbcUrl(), config.username, config.password)) {
            PreparedStatement ps = null;
            int preparedK = -1;
            try {
                for (int i = startIdx; i < endIdx; i++) {
                    // Re-read topK per iteration so the admin API's
                    // /query/config/top-k override takes effect without a restart.
                    int k = config.topK;
                    if (ps == null || k != preparedK) {
                        if (ps != null) {
                            ps.close();
                        }
                        String sql = "SELECT id FROM " + config.tableName
                                + " ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC LIMIT " + k;
                        ps = conn.prepareStatement(sql);
                        preparedK = k;
                    }

                    RateLimiter currentLimiter = queryRateLimiter.get();
                    if (currentLimiter != null) {
                        currentLimiter.acquire(1);
                    }
                    long start = System.nanoTime();
                    ps.setObject(1, queryVectors.get(i));
                    List<Integer> ids = new ArrayList<>();
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            ids.add(rs.getInt(1));
                        }
                    }
                    if (ids.size() != k) {
                        if (tolerateShortResults) {
                            // During ingestion the HNSW graph is only partially built;
                            // short result sets are expected and should not abort the run.
                            System.err.println("[ingest_query] query " + i + " returned " + ids.size()
                                    + " results (expected " + k + ") — partial graph, continuing");
                        } else {
                            System.err.println("FATAL: query " + i + " returned " + ids.size()
                                    + " results, expected " + k);
                            System.exit(1);
                        }
                    }
                    long elapsed = System.nanoTime() - start;
                    metrics.record(elapsed);
                    // allResults is null when the caller does not need recall storage
                    // (e.g. during-ingestion sampling). Skip the set in that case.
                    if (allResults != null) {
                        allResults.set(i, ids);
                    }

                    // Status-line publication moved to VectorBench's query
                    // progress loop (issue #443): every worker calling
                    // computeStats() every 100 queries and overwriting the
                    // shared statusLine added redundant CPU on the hot
                    // path. The query progress loop already emits the same
                    // fields every 500 ms.
                }
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        } catch (SQLException e) {
            System.err.println("Query error in range [" + startIdx + ", " + endIdx + "): " + e.getMessage());
            e.printStackTrace();
        }
    }
}
