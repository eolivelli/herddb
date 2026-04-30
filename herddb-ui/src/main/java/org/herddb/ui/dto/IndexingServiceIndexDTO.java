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

package org.herddb.ui.dto;

/**
 * Wire-shape for one vector index hosted by the indexing-service tier,
 * derived from {@code sysindexstatus}.
 */
public final class IndexingServiceIndexDTO {

    private String tablespace;
    private String tableName;
    private String indexName;
    private String indexType;
    private String indexUuid;
    private String status;
    private long vectorCount;
    private int segmentCount;
    // In-memory tailer position (diagnostic only) and durable recovery LSN
    // (used by server retention) — see IndexStatusInfo on
    // RemoteVectorIndexService for the contract (issue #364).
    private long tailerLsnLedger;
    private long tailerLsnOffset;
    private long durableLsnLedger;
    private long durableLsnOffset;
    /** Original JSON properties string from sysindexstatus (for debugging). */
    private String rawProperties;
    /** Error string if the indexing service was unreachable when sysindexstatus was sampled. */
    private String error;

    public IndexingServiceIndexDTO() {
    }

    public String getTablespace() {
        return tablespace;
    }

    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public String getIndexUuid() {
        return indexUuid;
    }

    public void setIndexUuid(String indexUuid) {
        this.indexUuid = indexUuid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getVectorCount() {
        return vectorCount;
    }

    public void setVectorCount(long vectorCount) {
        this.vectorCount = vectorCount;
    }

    public int getSegmentCount() {
        return segmentCount;
    }

    public void setSegmentCount(int segmentCount) {
        this.segmentCount = segmentCount;
    }

    public long getTailerLsnLedger() {
        return tailerLsnLedger;
    }

    public void setTailerLsnLedger(long tailerLsnLedger) {
        this.tailerLsnLedger = tailerLsnLedger;
    }

    public long getTailerLsnOffset() {
        return tailerLsnOffset;
    }

    public void setTailerLsnOffset(long tailerLsnOffset) {
        this.tailerLsnOffset = tailerLsnOffset;
    }

    public long getDurableLsnLedger() {
        return durableLsnLedger;
    }

    public void setDurableLsnLedger(long durableLsnLedger) {
        this.durableLsnLedger = durableLsnLedger;
    }

    public long getDurableLsnOffset() {
        return durableLsnOffset;
    }

    public void setDurableLsnOffset(long durableLsnOffset) {
        this.durableLsnOffset = durableLsnOffset;
    }

    public String getRawProperties() {
        return rawProperties;
    }

    public void setRawProperties(String rawProperties) {
        this.rawProperties = rawProperties;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
