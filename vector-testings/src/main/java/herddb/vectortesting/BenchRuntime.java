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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

// Note: setting a new rate via setIngestMaxOps / setQueryMaxOps replaces the
// underlying RateLimiter with a fresh instance. This is deliberate — Guava's
// SmoothBursty uses pay-forward reservation semantics, so a large multi-permit
// acquire at a low rate reserves far into the future and would keep blocking
// subsequent callers even after the rate is raised. Swapping the reference
// means any *new* acquire() picks up the fresh limiter without that baggage,
// so an admin-issued rate raise unblocks queued workers on their next call.

/**
 * Shared mutable state exposed to the admin HTTP API.
 *
 * <p>All fields are populated or updated from the main {@link VectorBench} flow
 * and read concurrently by Jetty request handlers. Rate limiters and the
 * status supplier are thread-safe references; {@link Config} is treated as
 * monotonic (only int/long fields are written from the admin API and reads
 * see either the old or new value — acceptable for the handful of settings
 * exposed via HTTP).
 */
public class BenchRuntime {

    /**
     * Effectively-unlimited rate passed to Guava when the user sets
     * {@code 0} on one of the {@code *-max-ops} flags. One billion permits/s
     * is well above anything a single JVM can actually push through JDBC, so
     * {@code acquire()} becomes a no-op but the limiter is still there for
     * live override via the admin API.
     */
    public static final double UNLIMITED_RATE = 1_000_000_000.0;

    private final Config config;
    private final AtomicReference<RateLimiter> ingestRateLimiterRef;
    private final AtomicReference<RateLimiter> queryRateLimiterRef;
    private final AtomicReference<Supplier<Map<String, Object>>> statusSupplier =
            new AtomicReference<>(() -> Collections.singletonMap("phase", "idle"));

    public BenchRuntime(Config config) {
        this.config = config;
        this.ingestRateLimiterRef = new AtomicReference<>(RateLimiter.create(
                config.ingestMaxOpsPerSecond > 0 ? config.ingestMaxOpsPerSecond : UNLIMITED_RATE));
        this.queryRateLimiterRef = new AtomicReference<>(RateLimiter.create(
                config.queryMaxOpsPerSecond > 0 ? config.queryMaxOpsPerSecond : UNLIMITED_RATE));
    }

    public Config config() {
        return config;
    }

    public RateLimiter ingestRateLimiter() {
        return ingestRateLimiterRef.get();
    }

    public RateLimiter queryRateLimiter() {
        return queryRateLimiterRef.get();
    }

    /**
     * Update the ingest rate. {@code 0} means unlimited (maps to
     * {@link #UNLIMITED_RATE} on the underlying limiter). Creates a fresh
     * limiter so no prior pay-forward reservation from the old rate carries
     * over — raising the rate unblocks queued workers on their next call.
     */
    public void setIngestMaxOps(int opsPerSecond) {
        if (opsPerSecond < 0) {
            throw new IllegalArgumentException("ingest-max-ops must be >= 0, got " + opsPerSecond);
        }
        double rate = opsPerSecond > 0 ? opsPerSecond : UNLIMITED_RATE;
        ingestRateLimiterRef.set(RateLimiter.create(rate));
        config.ingestMaxOpsPerSecond = opsPerSecond;
    }

    public void setQueryMaxOps(int opsPerSecond) {
        if (opsPerSecond < 0) {
            throw new IllegalArgumentException("query-max-ops must be >= 0, got " + opsPerSecond);
        }
        double rate = opsPerSecond > 0 ? opsPerSecond : UNLIMITED_RATE;
        queryRateLimiterRef.set(RateLimiter.create(rate));
        config.queryMaxOpsPerSecond = opsPerSecond;
    }

    /**
     * Update {@link Config#topK}. Workers re-read the value between queries
     * and re-prepare their SQL statement on change.
     */
    public void setTopK(int topK) {
        if (topK <= 0) {
            throw new IllegalArgumentException("top-k must be > 0");
        }
        config.topK = topK;
    }

    public Supplier<Map<String, Object>> getStatusSupplier() {
        return statusSupplier.get();
    }

    public void setStatusSupplier(Supplier<Map<String, Object>> supplier) {
        statusSupplier.set(supplier);
    }
}
