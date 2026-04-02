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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

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

    private final RemoteVectorIndexService remoteService;

    public VectorIndexManager(Index index,
                               AbstractTableManager tableManager,
                               CommitLog log,
                               DataStorageManager dataStorageManager,
                               String tableSpaceUUID,
                               long transaction,
                               int writeLockTimeout,
                               int readLockTimeout,
                               RemoteVectorIndexService remoteService) {
        super(index, tableManager, dataStorageManager, tableSpaceUUID, log,
                transaction, writeLockTimeout, readLockTimeout);
        if (remoteService == null) {
            throw new IllegalArgumentException(
                    "RemoteVectorIndexService is required; embedded vector indexing mode is no longer supported");
        }
        this.remoteService = remoteService;
    }

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "start VECTOR index {0} in remote mode (IndexingService)",
                index.name);
        return true;
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        // Rebuild handled by IndexingService via CommitLog replay
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        LOGGER.log(Level.INFO, "checkpoint index {0} on table {1} tablespace {2}, targetLSN={3}, pin={4}",
                new Object[]{index.name, index.table, tableSpaceUUID, sequenceNumber, pin});
        long start = System.nanoTime();
        try {
            remoteService.waitForCatchUp(tableSpaceUUID, sequenceNumber);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.INFO, "checkpoint index {0} on table {1} waitForCatchUp completed in {2} ms",
                    new Object[]{index.name, index.table, elapsedMs});
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.SEVERE, "checkpoint index {0} on table {1} interrupted after {2} ms",
                    new Object[]{index.name, index.table, elapsedMs});
            throw new DataStorageManagerException("interrupted while waiting for IndexingService catch-up", e);
        }
        return Collections.emptyList();
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
        try {
            List<Map.Entry<Bytes, Float>> results =
                    remoteService.search(tableSpaceUUID, index.table, index.name, queryVector, topK);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.INFO, "search index {0} on table {1} completed in {2} ms, {3} results",
                    new Object[]{index.name, index.table, elapsedMs, results.size()});
            return results;
        } catch (Exception e) {
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOGGER.log(Level.SEVERE, "search index {0} on table {1} failed after {2} ms: {3}",
                    new Object[]{index.name, index.table, elapsedMs, e.getMessage()});
            throw new StatementExecutionException("remote vector index search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns status information from the remote IndexingService.
     */
    public RemoteVectorIndexService.IndexStatusInfo getRemoteIndexStatus() {
        return remoteService.getIndexStatus(tableSpaceUUID, index.table, index.name);
    }

    @Override
    protected Stream<Bytes> scanner(IndexOperation operation,
                                    StatementEvaluationContext context,
                                    TableContext tableContext) throws StatementExecutionException {
        throw new UnsupportedOperationException(
                "Vector index scan not yet supported by the SQL planner");
    }
}
