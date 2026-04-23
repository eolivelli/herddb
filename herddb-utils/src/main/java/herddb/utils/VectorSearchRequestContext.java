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

package herddb.utils;

import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-scoped accumulator for per-request metrics on the vector-search hot
 * path. A {@code VectorSearch} gRPC handler initiates the search on one thread
 * (see {@code IndexingServiceImpl.search} → {@code PersistentVectorStore.searchInternal}),
 * and may fan out to worker threads when intra-query parallelism is enabled
 * (issue #245). A {@link ThreadLocal} holds the active context on whichever
 * thread is currently executing {@code readFileRange}; worker threads that
 * share the same request re-bind the same context via {@link #bind(VectorSearchRequestContext)}
 * so their reads are attributed to the originating request.
 *
 * <p>Usage pattern from the gRPC handler:
 * <pre>{@code
 * VectorSearchRequestContext.begin();
 * try {
 *     // ... perform the search; ensureBlockLoaded in the file client
 *     //     updates the current context via VectorSearchRequestContext.current()
 *     VectorSearchRequestContext ctx = VectorSearchRequestContext.current();
 *     // read ctx.getReadFileRangeCalls() etc. and feed them to histograms
 * } finally {
 *     VectorSearchRequestContext.end();
 * }
 * }</pre>
 *
 * <p>Usage pattern from a worker thread re-binding the parent's context:
 * <pre>{@code
 * VectorSearchRequestContext.bind(ctx);
 * try {
 *     // ... perform one segment's search
 * } finally {
 *     VectorSearchRequestContext.end();
 * }
 * }</pre>
 *
 * <p>This class lives in {@code herddb-utils} (rather than in
 * {@code herddb-remote-file-service} or {@code herddb-indexing-service})
 * because both the read hot path ({@code RemoteRandomAccessReader}) and the
 * vector-index core ({@code PersistentVectorStore}) need to reach it, and
 * only {@code herddb-utils} sits below them both in the dependency graph.
 *
 * <p>Counters are {@link LongAdder}s so concurrent worker threads can record
 * {@code readFileRange} events on the same context without contention.
 * {@link #current()} returns {@code null} outside an active request —
 * callers MUST null-check so that non-search read paths (e.g. data-page lazy
 * loads) don't trip an NPE.
 *
 * @author enrico.olivelli
 */
public final class VectorSearchRequestContext {

    private static final ThreadLocal<VectorSearchRequestContext> CURRENT = new ThreadLocal<>();

    private final LongAdder readFileRangeCalls = new LongAdder();
    private final LongAdder readFileRangeBytes = new LongAdder();
    private final LongAdder readFileRangeWaitNanos = new LongAdder();
    private final LongAdder cacheHits = new LongAdder();
    private final LongAdder cacheMisses = new LongAdder();

    private VectorSearchRequestContext() {
        // created via begin()
    }

    /**
     * Installs a fresh context in the current thread's {@link ThreadLocal} and
     * returns it. Overwrites any previously-active context (caller is expected
     * to have ended it first).
     */
    public static VectorSearchRequestContext begin() {
        VectorSearchRequestContext ctx = new VectorSearchRequestContext();
        CURRENT.set(ctx);
        return ctx;
    }

    /**
     * Binds an existing context to the current thread. Used by worker threads
     * participating in a parallel vector search so their {@code readFileRange}
     * calls accumulate into the initiator's context. Null-safe: binding
     * {@code null} is equivalent to {@link #end()}.
     */
    public static void bind(VectorSearchRequestContext ctx) {
        if (ctx == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(ctx);
        }
    }

    /**
     * Removes the current-thread context so that unrelated subsequent work on
     * this thread does not accidentally accumulate into it. Safe to call even
     * if no context is active.
     */
    public static void end() {
        CURRENT.remove();
    }

    /**
     * @return the active context for the current thread, or {@code null} when
     *         no search request is in progress.
     */
    public static VectorSearchRequestContext current() {
        return CURRENT.get();
    }

    /**
     * Records a single successful {@code readFileRange} round-trip. Called
     * from {@code RemoteRandomAccessReader.ensureBlockLoaded} regardless of
     * whether the bytes ultimately came from the network or from a local
     * cache — cache hits use {@link #recordCacheHit()} in addition to this.
     */
    public void recordReadFileRange(int bytes, long elapsedNanos) {
        readFileRangeCalls.increment();
        readFileRangeBytes.add(bytes);
        readFileRangeWaitNanos.add(elapsedNanos);
    }

    public void recordCacheHit() {
        cacheHits.increment();
    }

    public void recordCacheMiss() {
        cacheMisses.increment();
    }

    public long getReadFileRangeCalls() {
        return readFileRangeCalls.sum();
    }

    public long getReadFileRangeBytes() {
        return readFileRangeBytes.sum();
    }

    public long getReadFileRangeWaitNanos() {
        return readFileRangeWaitNanos.sum();
    }

    public long getCacheHits() {
        return cacheHits.sum();
    }

    public long getCacheMisses() {
        return cacheMisses.sum();
    }
}
