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

package herddb.remote;

/**
 * Thread-scoped accumulator for per-request metrics on the vector-search hot
 * path. A single {@code VectorSearch} gRPC handler thread executes the whole
 * search synchronously (see {@code IndexingServiceImpl.search} →
 * {@code PersistentVectorStore.searchInternal} → {@code GraphSearcher.search}),
 * so a plain {@link ThreadLocal} is sufficient to correlate
 * {@code readFileRange} calls back to the originating request.
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
 * <p>This class lives in {@code herddb-remote-file-service} (instead of the
 * indexing-service module) because {@code RemoteRandomAccessReader} needs to
 * update the context from the read hot path, and the indexing-service module
 * already depends on this module. Keeping the class here avoids a dependency
 * inversion.
 *
 * <p>Fields are plain {@code long}s (no atomics) because access is
 * single-threaded per request. {@link #current()} returns {@code null} outside
 * an active request — callers MUST null-check so that non-search read paths
 * (e.g. data-page lazy loads) don't trip an NPE.
 *
 * @author enrico.olivelli
 */
public final class VectorSearchRequestContext {

    private static final ThreadLocal<VectorSearchRequestContext> CURRENT = new ThreadLocal<>();

    private long readFileRangeCalls;
    private long readFileRangeBytes;
    private long readFileRangeWaitNanos;
    private long cacheHits;
    private long cacheMisses;

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
        readFileRangeCalls++;
        readFileRangeBytes += bytes;
        readFileRangeWaitNanos += elapsedNanos;
    }

    public void recordCacheHit() {
        cacheHits++;
    }

    public void recordCacheMiss() {
        cacheMisses++;
    }

    public long getReadFileRangeCalls() {
        return readFileRangeCalls;
    }

    public long getReadFileRangeBytes() {
        return readFileRangeBytes;
    }

    public long getReadFileRangeWaitNanos() {
        return readFileRangeWaitNanos;
    }

    public long getCacheHits() {
        return cacheHits;
    }

    public long getCacheMisses() {
        return cacheMisses;
    }
}
