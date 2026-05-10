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

import herddb.log.LogSequenceNumber;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SPI interface for remote vector index search.
 * <p>
 * The implementation lives in herddb-indexing-service (gRPC client)
 * and is wired into the Server/DBManager at startup.
 * <p>
 * This interface has no gRPC dependency, allowing herddb-core to remain
 * free of gRPC artifacts.
 *
 * @author enrico.olivelli
 */
public interface RemoteVectorIndexService extends AutoCloseable {

    /**
     * Searches the remote IndexingService for the nearest vectors.
     *
     * @param tablespace the tablespace name
     * @param table the table name
     * @param index the index name
     * @param vector the query vector
     * @param topK maximum number of results
     * @return list of (primaryKey, score) pairs sorted by score descending
     */
    List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                          float[] vector, int topK);

    /**
     * Returns status information for a vector index on the remote service.
     *
     * @param tablespace the tablespace name
     * @param table the table name
     * @param index the index name
     * @return status info
     */
    IndexStatusInfo getIndexStatus(String tablespace, String table, String index);

    /**
     * Waits until all IndexingService instances have processed the commit log
     * for the given tablespace up to (at least) the given LSN. This is called
     * during checkpoint to ensure that commit log files are not deleted before
     * all indexing services have consumed them.
     * <p>
     * If an instance is down, this method blocks until it comes back and catches up.
     * If an instance is behind, this method polls until it reaches the target LSN.
     *
     * @param tablespace the tablespace whose commit log is being checkpointed
     * @param sequenceNumber the checkpoint LSN that all instances must reach
     * @param timeoutMs maximum time to wait in milliseconds
     * @return true if all instances caught up, false if timeout expired
     * @throws InterruptedException if the waiting thread is interrupted
     */
    boolean waitForCatchUp(String tablespace, LogSequenceNumber sequenceNumber, long timeoutMs) throws InterruptedException;

    /**
     * Issue #509: eagerly notifies every IS instance to begin background
     * cleanup of the ZK segment registry and file-server data for the named
     * index. Called by {@link herddb.index.vector.VectorIndexManager#dropIndexData()}
     * immediately when the HerdDB server processes a DROP TABLE / DROP INDEX,
     * before the commit-log tailer has had a chance to reach the matching
     * {@code DROP_INDEX} log entry.
     *
     * <p>Best-effort: if an IS instance is unreachable (e.g. pod restart),
     * the failure is logged at WARNING and the method returns normally.
     * The commit-log tailer path remains the authoritative cleanup fallback —
     * the IS handles both the eager RPC and the tailer entry idempotently
     * (a second removal of a store key that is no longer tracked is a no-op).
     *
     * <p>The server does <em>not</em> block waiting for the actual file or
     * ZK node deletion to complete: the IS queues the cleanup in its
     * background {@code checkpointExecutor} and returns as soon as the
     * in-memory store reference has been removed from its tracking map.
     *
     * @param tablespace the HerdDB tablespace UUID
     * @param table      the table name
     * @param indexName  the index name
     */
    void dropIndex(String tablespace, String table, String indexName);

    /**
     * Returns the minimum LSN across all known IndexingService instances for
     * the given tablespace — the floor below which commit-log segments must
     * not be deleted while tailers are active.
     * <p>
     * Returns {@link Optional#empty()} when no instances are configured for
     * this tablespace (no retention constraint). Returns
     * {@link Optional#of(Object) Optional.of(LogSequenceNumber.START_OF_TIME)}
     * if any instance is unreachable, which forces maximum retention until
     * the instance comes back.
     *
     * @param tablespace the tablespace whose tailers should be queried
     * @return the retention floor, or {@link Optional#empty()} when no tailers exist
     */
    Optional<LogSequenceNumber> getMinProcessedLsn(String tablespace);

    /**
     * Status information for a remote vector index.
     *
     * <p>Issue #364: carries both the in-memory tailer position
     * ({@code tailerLsn*}, diagnostic only) and the durable recovery LSN
     * ({@code durableLsn*}, used by the server's commit-log retention floor
     * — never advance retention past this value, otherwise an IS restart
     * cannot replay the missing entries).
     *
     * <p>Issue #423: also carries the wall-clock timestamps of the LogEntry
     * at each LSN, so dashboards can report the time-lag of the IS as
     * {@code now - timestamp} (in milliseconds) without knowing the
     * commit-log layout.
     */
    class IndexStatusInfo {
        private final long vectorCount;
        private final int segmentCount;
        private final long tailerLsnLedger;
        private final long tailerLsnOffset;
        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the tailer LSN.
         * 0 = unknown. Issue #423.
         */
        private final long tailerLsnTimestamp;
        private final long durableLsnLedger;
        private final long durableLsnOffset;
        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the durable LSN.
         * 0 = unknown. Issue #423.
         */
        private final long durableLsnTimestamp;
        private final String status;
        /** Number of segments loaded so far during cold-start recovery (0 when not loading). */
        private final int loadingSegmentsDone;
        /** Total number of segments to load during cold-start recovery (0 when not loading). */
        private final int loadingSegmentsTotal;

        public IndexStatusInfo(long vectorCount, int segmentCount,
                               long tailerLsnLedger, long tailerLsnOffset,
                               long tailerLsnTimestamp,
                               long durableLsnLedger, long durableLsnOffset,
                               long durableLsnTimestamp,
                               String status,
                               int loadingSegmentsDone, int loadingSegmentsTotal) {
            this.vectorCount = vectorCount;
            this.segmentCount = segmentCount;
            this.tailerLsnLedger = tailerLsnLedger;
            this.tailerLsnOffset = tailerLsnOffset;
            this.tailerLsnTimestamp = tailerLsnTimestamp;
            this.durableLsnLedger = durableLsnLedger;
            this.durableLsnOffset = durableLsnOffset;
            this.durableLsnTimestamp = durableLsnTimestamp;
            this.status = status;
            this.loadingSegmentsDone = loadingSegmentsDone;
            this.loadingSegmentsTotal = loadingSegmentsTotal;
        }

        public long getVectorCount() {
            return vectorCount;
        }

        public int getSegmentCount() {
            return segmentCount;
        }

        public long getTailerLsnLedger() {
            return tailerLsnLedger;
        }

        public long getTailerLsnOffset() {
            return tailerLsnOffset;
        }

        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the tailer LSN.
         * Operators compute the tailer time-lag as
         * {@code now - getTailerLsnTimestamp()}. {@code 0} = unknown.
         * Issue #423.
         */
        public long getTailerLsnTimestamp() {
            return tailerLsnTimestamp;
        }

        public long getDurableLsnLedger() {
            return durableLsnLedger;
        }

        public long getDurableLsnOffset() {
            return durableLsnOffset;
        }

        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the durable LSN.
         * Operators compute the durable time-lag as
         * {@code now - getDurableLsnTimestamp()}. {@code 0} = unknown.
         * Issue #423.
         */
        public long getDurableLsnTimestamp() {
            return durableLsnTimestamp;
        }

        public String getStatus() {
            return status;
        }

        /**
         * Returns the number of segments that have finished loading during the current
         * cold-start recovery pass. Returns 0 when the store is not loading from status.
         */
        public int getLoadingSegmentsDone() {
            return loadingSegmentsDone;
        }

        /**
         * Returns the total number of on-disk segments that must be loaded during the
         * current cold-start recovery pass. Returns 0 when the store is not loading from status.
         */
        public int getLoadingSegmentsTotal() {
            return loadingSegmentsTotal;
        }
    }
}
