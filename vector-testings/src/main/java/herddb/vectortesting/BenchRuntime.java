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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

// Note: setting a new rate via setIngestMaxOps / setQueryMaxOps replaces the
// underlying RateLimiter with a fresh instance (queries) or pushes the new
// rate into every per-thread limiter and unparks each parked worker (ingest).
// This is deliberate — Guava's SmoothBursty uses pay-forward reservation
// semantics, so a large multi-permit acquire at a low rate reserves far into
// the future and would keep blocking subsequent callers even after the rate
// is raised. Resetting the deadline / swapping the reference means any *new*
// acquire() picks up the fresh limiter without that baggage, so an
// admin-issued rate raise unblocks queued workers on their next call.

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

    /**
     * Maximum number of ingest threads that may be set via the admin API.
     * Prevents runaway thread creation from misconfigured POST requests.
     */
    public static final int MAX_INGEST_THREADS = 1024;

    private final Config config;
    /**
     * Per-thread rate limiters for ingestion (issue #402): the global rate is
     * split across N children to remove the inter-thread serialisation that a
     * shared Guava limiter caused.
     */
    private final IngestRateLimiterGroup ingestRateLimiterGroup;
    private final AtomicReference<RateLimiter> queryRateLimiterRef;
    private final AtomicReference<Supplier<Map<String, Object>>> statusSupplier =
            new AtomicReference<>(() -> Collections.singletonMap("phase", "idle"));

    /**
     * Number of ingest workers currently alive (incremented at the start of
     * each worker's {@code run()}, decremented in its {@code finally} block).
     * Used by {@link #setIngestThreads} to compute how many poison pills to
     * inject (reduction) or how many new workers to spawn (increase).
     */
    public final AtomicInteger activeIngestWorkers = new AtomicInteger(0);

    /**
     * The target ingest thread count as last requested via {@link #setIngestThreads}.
     * Initialised from {@link Config#ingestThreads}; may diverge if updated
     * before the ingest phase starts.
     */
    private volatile int targetIngestThreads;

    // Ingest context — set by VectorBench at the start of the ingest phase so
    // setIngestThreads can inject pills or spawn workers live. Cleared after
    // the ingest phase completes so stale references are not held.
    private volatile BlockingQueue<float[]> ingestQueue;
    private volatile ExecutorService ingestPool;
    private volatile Supplier<Runnable> ingestWorkerFactory;

    public BenchRuntime(Config config) {
        this.config = config;
        this.targetIngestThreads = config.ingestThreads;
        double initialRate = config.ingestMaxOpsPerSecond > 0
                ? config.ingestMaxOpsPerSecond
                : 0.0; // 0 = unlimited; the group maps to UNLIMITED_RATE
        // Allocate enough per-thread limiters for the configured worker count;
        // grown later if setIngestThreads adds workers at runtime.
        this.ingestRateLimiterGroup = new IngestRateLimiterGroup(
                Math.max(1, config.ingestThreads), initialRate);
        this.queryRateLimiterRef = new AtomicReference<>(RateLimiter.create(
                config.queryMaxOpsPerSecond > 0 ? config.queryMaxOpsPerSecond : UNLIMITED_RATE));
    }

    public Config config() {
        return config;
    }

    /**
     * Returns the per-thread ingest rate-limiter group. Workers acquire from
     * their own slot via {@link IngestRateLimiterGroup#acquire(int, int)}.
     */
    public IngestRateLimiterGroup ingestRateLimiterGroup() {
        return ingestRateLimiterGroup;
    }

    public RateLimiter queryRateLimiter() {
        return queryRateLimiterRef.get();
    }

    /**
     * Lock guarding the joint invariant
     * {@code batch-size <= effective transaction-size <= ingest-max-ops}.
     * {@link #setBatchSize}, {@link #setTransactionSize} and
     * {@link #setIngestMaxOps} all hold this lock for their entire
     * read-validate-write sequence so two concurrent admin POSTs cannot
     * each pass against the old value of the other field and then commit
     * writes that violate the invariant.
     */
    private final Object ingestionConfigLock = new Object();

    /**
     * Update the ingest rate. {@code 0} means unlimited (maps to
     * {@link #UNLIMITED_RATE} on the underlying limiter). Pushes the new
     * rate into every per-thread limiter and unparks all registered worker
     * threads, so a sleeping worker takes the new rate immediately rather
     * than after its old sleep elapses (issue #402).
     *
     * <p>Also enforces the safety invariant
     * {@code effectiveTransactionSize <= ingestMaxOps} when finite. The
     * config is left untouched on rejection.
     */
    public void setIngestMaxOps(int opsPerSecond) {
        if (opsPerSecond < 0) {
            throw new IllegalArgumentException("ingest-max-ops must be >= 0, got " + opsPerSecond);
        }
        synchronized (ingestionConfigLock) {
            if (opsPerSecond > 0) {
                int effectiveTxn = config.effectiveTransactionSize();
                if (effectiveTxn > opsPerSecond) {
                    throw new IllegalArgumentException(
                            "ingest-max-ops (" + opsPerSecond + ") must be >= effective transaction-size ("
                                    + effectiveTxn + "). Lower batch-size/transaction-size first.");
                }
            }
            double rate = opsPerSecond > 0 ? opsPerSecond : 0.0;
            ingestRateLimiterGroup.setEffectiveRate(rate);
            config.ingestMaxOpsPerSecond = opsPerSecond;
        }
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

    /**
     * Update {@link Config#batchSize}. Validates first, writes last:
     * <ul>
     *   <li>{@code newBatchSize >= 1}</li>
     *   <li>{@code newBatchSize <= currentTransactionSize} (when transactionSize is set)</li>
     *   <li>{@code currentEffectiveTransactionSize (post-update) <= ingestMaxOps}
     *       when {@code ingestMaxOps > 0}. Because lowering batch-size cannot raise
     *       the effective transaction size, this only matters when transactionSize
     *       is unset (so effective == batch); we still re-check defensively.</li>
     * </ul>
     * The config is left untouched on rejection.
     */
    public void setBatchSize(int newBatchSize) {
        if (newBatchSize < 1) {
            throw new IllegalArgumentException("batch-size must be >= 1, got " + newBatchSize);
        }
        synchronized (ingestionConfigLock) {
            int txn = config.transactionSize;
            if (txn > 0 && newBatchSize > txn) {
                throw new IllegalArgumentException(
                        "batch-size (" + newBatchSize + ") must be <= transaction-size ("
                                + txn + "). Raise transaction-size first or pick a smaller batch-size.");
            }
            // Effective transaction size after the change.
            int effectiveTxn = txn > 0 ? txn : newBatchSize;
            int rate = config.ingestMaxOpsPerSecond;
            if (rate > 0 && effectiveTxn > rate) {
                throw new IllegalArgumentException(
                        "effective transaction-size (" + effectiveTxn
                                + ") would exceed ingest-max-ops (" + rate
                                + "). Raise ingest-max-ops or lower transaction-size/batch-size.");
            }
            config.batchSize = newBatchSize;
        }
    }

    /**
     * Update {@link Config#transactionSize}. Validates first, writes last:
     * <ul>
     *   <li>{@code newTransactionSize >= 1}</li>
     *   <li>{@code newTransactionSize >= currentBatchSize}</li>
     *   <li>{@code newTransactionSize <= ingestMaxOps} when {@code ingestMaxOps > 0}</li>
     * </ul>
     * The config is left untouched on rejection.
     */
    public void setTransactionSize(int newTransactionSize) {
        if (newTransactionSize < 1) {
            throw new IllegalArgumentException(
                    "transaction-size must be >= 1, got " + newTransactionSize);
        }
        synchronized (ingestionConfigLock) {
            int batch = config.batchSize;
            if (newTransactionSize < batch) {
                throw new IllegalArgumentException(
                        "transaction-size (" + newTransactionSize + ") must be >= batch-size ("
                                + batch + "). Lower batch-size first or pick a larger transaction-size.");
            }
            int rate = config.ingestMaxOpsPerSecond;
            if (rate > 0 && newTransactionSize > rate) {
                throw new IllegalArgumentException(
                        "transaction-size (" + newTransactionSize
                                + ") must be <= ingest-max-ops (" + rate
                                + "). Raise ingest-max-ops first or lower transaction-size.");
            }
            config.transactionSize = newTransactionSize;
        }
    }

    public Supplier<Map<String, Object>> getStatusSupplier() {
        return statusSupplier.get();
    }

    public void setStatusSupplier(Supplier<Map<String, Object>> supplier) {
        statusSupplier.set(supplier);
    }

    /**
     * Registers the live ingest context so that {@link #setIngestThreads} can
     * inject poison pills or spawn new workers during the ingest phase.
     * Must be called by {@code VectorBench} immediately before submitting the
     * initial ingest workers and after all three parameters are ready.
     *
     * @param queue   the shared queue that ingest workers drain
     * @param pool    the executor service running the ingest workers
     * @param factory supplier that creates a new {@link IngestionWorker} runnable
     *                with all shared state already captured
     */
    public void setIngestContext(BlockingQueue<float[]> queue, ExecutorService pool,
                                 Supplier<Runnable> factory) {
        this.ingestQueue = queue;
        this.ingestPool = pool;
        this.ingestWorkerFactory = factory;
    }

    /**
     * Clears the ingest context after the ingest phase completes so that stale
     * references to the queue and pool are not retained beyond their useful life.
     */
    public void clearIngestContext() {
        this.ingestQueue = null;
        this.ingestPool = null;
        this.ingestWorkerFactory = null;
    }

    /**
     * Changes the number of live ingest threads.
     *
     * <p><b>Reducing</b> (newThreads &lt; activeIngestWorkers): injects
     * {@code (active - newThreads)} poison pills into the ingest queue. Each
     * pill causes one worker to exit after its current commit completes — no
     * batch is interrupted mid-flight. If the ingest context is not yet set
     * (called before the ingest phase), the pill injection is skipped and only
     * the config is updated.
     *
     * <p><b>Increasing</b> (newThreads &gt; activeIngestWorkers): submits
     * {@code (newThreads - active)} new worker tasks via the registered pool
     * and factory. New workers share the same queue and accumulators as the
     * initial workers. If the ingest context is not yet set, the spawn is
     * skipped and only the config is updated.
     *
     * <p>Also resizes the {@link IngestRateLimiterGroup} so the global ingest
     * rate is redistributed across the new worker count.
     *
     * @param newThreads target thread count; must be between 1 and
     *                   {@value #MAX_INGEST_THREADS} inclusive
     * @throws IllegalArgumentException if the value is out of range
     * @throws InterruptedException if a poison-pill {@code put} is interrupted
     */
    public void setIngestThreads(int newThreads) throws InterruptedException {
        if (newThreads <= 0) {
            throw new IllegalArgumentException("ingest-threads must be >= 1, got " + newThreads);
        }
        if (newThreads > MAX_INGEST_THREADS) {
            throw new IllegalArgumentException(
                    "ingest-threads must be <= " + MAX_INGEST_THREADS + ", got " + newThreads);
        }

        int active = activeIngestWorkers.get();
        BlockingQueue<float[]> queue = this.ingestQueue;
        ExecutorService pool = this.ingestPool;
        Supplier<Runnable> factory = this.ingestWorkerFactory;

        if (newThreads < active && queue != null) {
            // Inject poison pills to retire excess workers gracefully.
            int toRetire = active - newThreads;
            for (int i = 0; i < toRetire; i++) {
                queue.put(new float[0]);
            }
        } else if (newThreads > active && pool != null && factory != null) {
            // Spawn additional workers up to the target.
            int toSpawn = newThreads - active;
            for (int i = 0; i < toSpawn; i++) {
                pool.submit(factory.get());
            }
        }

        // Resize the per-thread limiter group so the per-worker rate share
        // (totalRate / newThreads) reflects the actual active worker count
        // — otherwise a 8 → 4 shrink would leave child rates at total/8 and
        // the aggregate throughput would be total/2. Growing appends new
        // children; shrinking drops the truncated tail (those workers have
        // already exited via the poison-pill path) and rebalances the
        // survivors to the new (larger) share.
        ingestRateLimiterGroup.resize(newThreads);

        config.ingestThreads = newThreads;
        targetIngestThreads = newThreads;
    }

    /**
     * Returns the current target ingest thread count (as last set via
     * {@link #setIngestThreads} or initialised from {@link Config}).
     */
    public int getTargetIngestThreads() {
        return targetIngestThreads;
    }

    /**
     * Changes the number of query threads stored in {@link Config}.
     *
     * <p>Note: the query phase pre-partitions work by thread index at startup,
     * so a live change during an ongoing query phase will not repartition
     * in-flight work. The new value is reflected immediately in
     * {@code GET /query/config} and takes full effect when the next benchmark
     * run starts.
     *
     * @param newThreads target thread count; must be >= 1
     * @throws IllegalArgumentException if the value is &lt; 1
     */
    public void setQueryThreads(int newThreads) {
        if (newThreads <= 0) {
            throw new IllegalArgumentException("query-threads must be >= 1, got " + newThreads);
        }
        config.queryThreads = newThreads;
    }
}
