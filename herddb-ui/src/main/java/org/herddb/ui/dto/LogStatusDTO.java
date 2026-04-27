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
 * Wire-shape returned by
 * {@code GET /api/v2/tablespaces/{name}/log-status}.
 *
 * <p>Mirrors the {@code syslogstatus} virtual table: current write-head LSN,
 * last completed checkpoint LSN/timestamp/duration, and the number of
 * BookKeeper ledgers that are dirty (i.e. not yet covered by a checkpoint).
 * All checkpoint fields are {@code null} when no checkpoint has been taken yet
 * (e.g. a freshly created tablespace or local/in-memory mode).
 */
public final class LogStatusDTO {

    private String tablespaceUuid;
    private String tablespace;
    private String nodeId;
    private Long ledger;
    private Long offset;
    private String status;
    private Long checkpointLedger;
    private Long checkpointOffset;
    private Long checkpointTimestamp;
    private Long checkpointDurationMs;
    private Integer dirtyLedgersCount;

    public LogStatusDTO() {
    }

    public String getTablespaceUuid() {
        return tablespaceUuid;
    }

    public void setTablespaceUuid(String tablespaceUuid) {
        this.tablespaceUuid = tablespaceUuid;
    }

    public String getTablespace() {
        return tablespace;
    }

    public void setTablespace(String tablespace) {
        this.tablespace = tablespace;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Long getLedger() {
        return ledger;
    }

    public void setLedger(Long ledger) {
        this.ledger = ledger;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getCheckpointLedger() {
        return checkpointLedger;
    }

    public void setCheckpointLedger(Long checkpointLedger) {
        this.checkpointLedger = checkpointLedger;
    }

    public Long getCheckpointOffset() {
        return checkpointOffset;
    }

    public void setCheckpointOffset(Long checkpointOffset) {
        this.checkpointOffset = checkpointOffset;
    }

    public Long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public void setCheckpointTimestamp(Long checkpointTimestamp) {
        this.checkpointTimestamp = checkpointTimestamp;
    }

    public Long getCheckpointDurationMs() {
        return checkpointDurationMs;
    }

    public void setCheckpointDurationMs(Long checkpointDurationMs) {
        this.checkpointDurationMs = checkpointDurationMs;
    }

    public Integer getDirtyLedgersCount() {
        return dirtyLedgersCount;
    }

    public void setDirtyLedgersCount(Integer dirtyLedgersCount) {
        this.dirtyLedgersCount = dirtyLedgersCount;
    }
}
