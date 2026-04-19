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

package herddb.index.vector;

import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Vector index manager that delegates all operations to a remote IndexingService.
 * <p>
 * This is a thin client: DML operations are no-ops (the IndexingService consumes
 * them via CommitLog tailing), search is delegated via gRPC, and checkpoint
 * ensures the remote service has caught up before the commit log is truncated.
 *
 * @author enrico.olivelli
 */
public class VectorIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = Logger.getLogger(VectorIndexManager.class.getName());

    /* property keys for CREATE INDEX ... WITH */
    public static final String PROP_M = "m";
    public static final String PROP_BEAM_WIDTH = "beamWidth";
    public static final String PROP_NEIGHBOR_OVERFLOW = "neighborOverflow";
    public static final String PROP_ALPHA = "alpha";
    public static final String PROP_FUSED_PQ = "fusedPQ";
    public static final String PROP_SIMILARITY = "similarity";
    public static final String PROP_MAX_SEGMENT_SIZE = "maxSegmentSize";
    public static final String PROP_MAX_LIVE_GRAPH_SIZE = "maxLiveGraphSize";
    public static final String PROP_NUM_SHARDS = "numShards";

    /**
     * Resolved lazily at every call so that the owning DBManager can
     * swap the underlying {@link RemoteVectorIndexService} instance
     * (e.g. when the indexing-service client is restarted) without
     * having to tear down the table space and rebuild every
     * {@code VectorIndexManager}.
     */
    private final Supplier<RemoteVectorIndexService> remoteServiceSupplier;
    private final Counter queryRequests;
    private final Counter queryErrors;
    private final OpStatsLogger queryLatency;

    public VectorIndexManager(Index index,
                               AbstractTableManager tableManager,
                               CommitLog log,
                               DataStorageManager dataStorageManager,
                               String tableSpaceUUID,
                               long transaction,
                               int writeLockTimeout,
                               int readLockTimeout,
                               Supplier<RemoteVectorIndexService> remoteServiceSupplier,
                               StatsLogger statsLogger) {
        super(index, tableManager, dataStorageManager, tableSpaceUUID, log,
                transaction, writeLockTimeout, readLockTimeout);
        if (remoteServiceSupplier == null) {
            throw new IllegalArgumentException("remoteServiceSupplier is required");
        }
        this.remoteServiceSupplier = remoteServiceSupplier;
        StatsLogger vectorScope = statsLogger.scope("vector");
        this.queryRequests = vectorScope.getCounter("query_requests");
        this.queryErrors = vectorScope.getCounter("query_errors");
        this.queryLatency = vectorScope.getOpStatsLogger("query_latency");
    }

    private RemoteVectorIndexService remoteService() {
        RemoteVectorIndexService svc = remoteServiceSupplier.get();
        if (svc == null) {
            throw new IllegalStateException(
                    "RemoteVectorIndexService is required; embedded vector indexing mode is no longer supported");
        }
        return svc;
    }

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "start VECTOR index {0} in remote mode (IndexingService)",
                index.name);
        remoteService(); // fail fast if RemoteVectorIndexService is not configured
        return true;
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        // Rebuild handled by IndexingService via CommitLog replay
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        // Vector index state lives in the remote IndexingService, which persists its own
        // watermarks via its tailer. Checkpoint is a no-op here; commit-log retention is
        // enforced via getTailersMinLsn() and the client-triggered EXECUTE WAITFORINDEXES.
        return Collections.emptyList();
    }

    @Override
    public Optional<LogSequenceNumber> getTailersMinLsn() {
        try {
            return remoteService().getMinProcessedLsn(tableSpaceUUID);
        } catch (RuntimeException e) {
            // Broad catch: RemoteVectorIndexService is a plugin boundary (gRPC client);
            // any failure must not crash the checkpoint and must pin retention.
            LOGGER.log(Level.WARNING,
                    "getTailersMinLsn: remote service error, pinning retention at START_OF_TIME", e);
            return Optional.of(LogSequenceNumber.START_OF_TIME);
        }
    }

    @Override
    public boolean waitForTailersCatchUp(LogSequenceNumber targetLsn, long timeoutMs) throws InterruptedException {
        LOGGER.log(Level.INFO,
                "waitForTailersCatchUp index {0} on table {1} tablespace {2}, targetLSN={3}, timeout={4}ms",
                new Object[]{index.name, index.table, tableSpaceUUID, targetLsn, timeoutMs});
        long start = System.nanoTime();
        boolean caughtUp = remoteService().waitForCatchUp(tableSpaceUUID, targetLsn, timeoutMs);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        if (!caughtUp) {
            LOGGER.log(Level.WARNING,
                    "waitForTailersCatchUp index {0} on table {1} timed out after {2} ms",
                    new Object[]{index.name, index.table, elapsedMs});
        } else {
            LOGGER.log(Level.INFO,
                    "waitForTailersCatchUp index {0} on table {1} completed in {2} ms",
                    new Object[]{index.name, index.table, elapsedMs});
        }
        return caughtUp;
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpaceUUID, index.uuid, sequenceNumber);
    }

    @Override
    public void recordInserted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        // Indexing happens asynchronously via CommitLog tailing
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        // Indexing happens asynchronously via CommitLog tailing
    }

    @Override
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded)
            throws DataStorageManagerException {
        // Indexing happens asynchronously via CommitLog tailing
    }

    @Override
    public void truncate() throws DataStorageManagerException {
        // Truncation handled by IndexingService via CommitLog
    }

    @Override
    public boolean valueAlreadyMapped(Bytes key, Bytes primaryKey) throws DataStorageManagerException {
        return false; // vector index does not enforce uniqueness
    }

    /**
     * Performs an approximate nearest-neighbor search against the remote IndexingService.
     *
     * @param queryVector the query embedding
     * @param topK        maximum number of results to return
     * @return list of (primaryKey, score) pairs ordered best-first
     */
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK)
            throws StatementExecutionException {
        LOGGER.log(Level.INFO, "search index {0} on table {1} tablespace {2}, topK={3}, vectorDim={4}",
                new Object[]{index.name, index.table, tableSpaceUUID, topK, queryVector.length});
        long start = System.nanoTime();
        queryRequests.inc();
        try {
            List<Map.Entry<Bytes, Float>> results =
                    remoteService().search(tableSpaceUUID, index.table, index.name, queryVector, topK);
            long elapsedNanos = System.nanoTime() - start;
            queryLatency.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            LOGGER.log(Level.INFO, "search index {0} on table {1} completed in {2} ms, {3} results",
                    new Object[]{index.name, index.table, elapsedMs, results.size()});
            return results;
        } catch (Exception e) {
            long elapsedNanos = System.nanoTime() - start;
            queryLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
            queryErrors.inc();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            LOGGER.log(Level.SEVERE, "search index {0} on table {1} failed after {2} ms: {3}",
                    new Object[]{index.name, index.table, elapsedMs, e.getMessage()});
            throw new StatementExecutionException("remote vector index search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Opens an expanding iterator over the remote vector index, used by
     * {@code VectorANNScanOp} when a WHERE predicate (or stale-PK skipping)
     * may force fetching more than {@code targetK} hits to produce
     * {@code targetK} result rows.
     *
     * <p>The iterator calls {@link RemoteVectorIndexService#search} with an
     * initial budget of {@code max(minInitial, ceil(targetK * factor))} and,
     * if the caller drains that batch without stopping, doubles the budget
     * and calls search again, up to {@code maxExpansions} expansions.
     * Each call returns a superset (by rank) of the previous one — entries
     * already emitted are filtered out via a PK {@code seen} set. When the
     * service returns fewer rows than requested, the index is exhausted and
     * {@code hasNext()} goes {@code false}.
     */
    public SearchIterator searchStream(float[] queryVector, int targetK,
                                        float factor, int minInitial, int maxExpansions)
            throws StatementExecutionException {
        if (targetK <= 0) {
            throw new IllegalArgumentException("targetK must be positive, got " + targetK);
        }
        if (factor < 1.0f) {
            throw new IllegalArgumentException("factor must be >= 1.0, got " + factor);
        }
        if (maxExpansions < 0) {
            throw new IllegalArgumentException("maxExpansions must be >= 0, got " + maxExpansions);
        }
        int initialBudget = Math.max(minInitial, (int) Math.ceil((double) targetK * factor));
        if (initialBudget <= 0) {
            initialBudget = 1;
        }
        queryRequests.inc();
        return new ExpandingSearchIterator(remoteService(), tableSpaceUUID,
                index.table, index.name, queryVector, initialBudget, maxExpansions,
                queryLatency, queryErrors);
    }

    /**
     * Test-only factory: builds an {@link ExpandingSearchIterator} directly
     * against a {@link RemoteVectorIndexService} without needing a fully
     * constructed {@link VectorIndexManager}. Used by the unit tests to
     * exercise the expansion/dedupe logic in isolation. Not called from
     * production code.
     */
    public static SearchIterator newSearchIteratorForTest(RemoteVectorIndexService service,
                                                    String tablespace, String table, String indexName,
                                                    float[] queryVector, int targetK,
                                                    float factor, int minInitial, int maxExpansions,
                                                    OpStatsLogger queryLatency, Counter queryErrors) {
        if (targetK <= 0) {
            throw new IllegalArgumentException("targetK must be positive, got " + targetK);
        }
        if (factor < 1.0f) {
            throw new IllegalArgumentException("factor must be >= 1.0, got " + factor);
        }
        if (maxExpansions < 0) {
            throw new IllegalArgumentException("maxExpansions must be >= 0, got " + maxExpansions);
        }
        int initialBudget = Math.max(minInitial, (int) Math.ceil((double) targetK * factor));
        if (initialBudget <= 0) {
            initialBudget = 1;
        }
        return new ExpandingSearchIterator(service, tablespace, table, indexName,
                queryVector, initialBudget, maxExpansions, queryLatency, queryErrors);
    }

    /**
     * Pull-based iterator over vector search results. Implementations may
     * transparently issue additional RPCs to fetch more candidates. Callers
     * MUST call {@link #close()} when done.
     */
    public interface SearchIterator extends AutoCloseable {
        boolean hasNext() throws StatementExecutionException;

        Map.Entry<Bytes, Float> next() throws StatementExecutionException;

        @Override
        void close();
    }

    static final class ExpandingSearchIterator implements SearchIterator {

        private final RemoteVectorIndexService service;
        private final String tablespace;
        private final String table;
        private final String indexName;
        private final float[] queryVector;
        private final int maxExpansions;
        private final Set<Bytes> seen = new HashSet<>();
        private final Deque<Map.Entry<Bytes, Float>> buffer = new ArrayDeque<>();
        private final long startNanos = System.nanoTime();
        private final OpStatsLogger queryLatency;
        private final Counter queryErrors;

        private int budget;
        private int expansionsUsed;
        private boolean exhausted;
        private boolean closed;
        private int emittedTotal;
        private boolean firstBatchProcessed;

        ExpandingSearchIterator(RemoteVectorIndexService service,
                                 String tablespace, String table, String indexName,
                                 float[] queryVector, int initialBudget, int maxExpansions,
                                 OpStatsLogger queryLatency, Counter queryErrors) {
            this.service = service;
            this.tablespace = tablespace;
            this.table = table;
            this.indexName = indexName;
            this.queryVector = queryVector;
            this.budget = initialBudget;
            this.maxExpansions = maxExpansions;
            this.queryLatency = queryLatency;
            this.queryErrors = queryErrors;
        }

        @Override
        public boolean hasNext() throws StatementExecutionException {
            if (closed) {
                return false;
            }
            if (!buffer.isEmpty()) {
                return true;
            }
            while (buffer.isEmpty() && !exhausted && expansionsUsed <= maxExpansions) {
                fetchNextBatch();
            }
            return !buffer.isEmpty();
        }

        @Override
        public Map.Entry<Bytes, Float> next() throws StatementExecutionException {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            emittedTotal++;
            return buffer.pollFirst();
        }

        private void fetchNextBatch() throws StatementExecutionException {
            int requested = budget;
            long rpcStart = System.nanoTime();
            List<Map.Entry<Bytes, Float>> results;
            try {
                results = service.search(tablespace, table, indexName, queryVector, requested);
            } catch (RuntimeException e) {
                long elapsedNanos = System.nanoTime() - rpcStart;
                if (!firstBatchProcessed) {
                    if (queryLatency != null) {
                        queryLatency.registerFailedEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                    }
                    firstBatchProcessed = true;
                }
                if (queryErrors != null) {
                    queryErrors.inc();
                }
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
                LOGGER.log(Level.SEVERE,
                        "searchStream expansion {0} failed for index {1} after {2} ms: {3}",
                        new Object[]{expansionsUsed, indexName, elapsedMs, e.getMessage()});
                throw new StatementExecutionException(
                        "remote vector index search failed: " + e.getMessage(), e);
            }
            long elapsedNanos = System.nanoTime() - rpcStart;
            if (!firstBatchProcessed) {
                if (queryLatency != null) {
                    queryLatency.registerSuccessfulEvent(elapsedNanos, TimeUnit.NANOSECONDS);
                }
                firstBatchProcessed = true;
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            int returned = results.size();
            int newlyAdded = 0;
            List<Map.Entry<Bytes, Float>> newEntries = new ArrayList<>();
            for (Map.Entry<Bytes, Float> e : results) {
                if (seen.add(e.getKey())) {
                    newEntries.add(e);
                    newlyAdded++;
                }
            }
            buffer.addAll(newEntries);
            LOGGER.log(Level.FINE,
                    "searchStream expansion {0} index={1} budget={2} returned={3} new={4} in {5} ms",
                    new Object[]{expansionsUsed, indexName, requested, returned, newlyAdded, elapsedMs});

            // Exhaustion signal: the remote returned strictly fewer rows than
            // we asked for, so the underlying index has no more candidates.
            if (returned < requested) {
                exhausted = true;
            }
            expansionsUsed++;
            if (budget > Integer.MAX_VALUE / 2) {
                budget = Integer.MAX_VALUE;
            } else {
                budget *= 2;
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            buffer.clear();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            LOGGER.log(Level.FINE,
                    "searchStream close index={0} emitted={1} expansions={2} elapsed={3} ms exhausted={4}",
                    new Object[]{indexName, emittedTotal, expansionsUsed, elapsedMs, exhausted});
        }
    }

    /**
     * Returns status information from the remote IndexingService.
     */
    public RemoteVectorIndexService.IndexStatusInfo getRemoteIndexStatus() {
        return remoteService().getIndexStatus(tableSpaceUUID, index.table, index.name);
    }

    @Override
    protected Stream<Bytes> scanner(IndexOperation operation,
                                    StatementEvaluationContext context,
                                    TableContext tableContext) throws StatementExecutionException {
        throw new UnsupportedOperationException(
                "Vector index scan not yet supported by the SQL planner");
    }
}
