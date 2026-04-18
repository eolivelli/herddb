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
     */
    class IndexStatusInfo {
        private final long vectorCount;
        private final int segmentCount;
        private final long lastLsnLedger;
        private final long lastLsnOffset;
        private final String status;

        public IndexStatusInfo(long vectorCount, int segmentCount,
                               long lastLsnLedger, long lastLsnOffset, String status) {
            this.vectorCount = vectorCount;
            this.segmentCount = segmentCount;
            this.lastLsnLedger = lastLsnLedger;
            this.lastLsnOffset = lastLsnOffset;
            this.status = status;
        }

        public long getVectorCount() {
            return vectorCount;
        }

        public int getSegmentCount() {
            return segmentCount;
        }

        public long getLastLsnLedger() {
            return lastLsnLedger;
        }

        public long getLastLsnOffset() {
            return lastLsnOffset;
        }

        public String getStatus() {
            return status;
        }
    }
}
