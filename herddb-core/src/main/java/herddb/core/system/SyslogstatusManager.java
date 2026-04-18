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

package herddb.core.system;

import herddb.codec.RecordSerializer;
import herddb.core.TableSpaceManager;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.Record;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.Transaction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Table Manager for the SYSLOGSTATUS virtual table.
 *
 * <p>Exposes the per-tablespace commit-log write head and the state of the
 * last completed checkpoint, so the gap between them (recovery exposure) is
 * directly readable with a single {@code SELECT}.
 *
 * @author enrico.olivelli
 */
public class SyslogstatusManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("syslogstatus")
            .column("tablespace_uuid", ColumnTypes.STRING)
            .column("nodeid", ColumnTypes.STRING)
            .column("tablespace_name", ColumnTypes.STRING)
            .column("ledger", ColumnTypes.LONG)
            .column("offset", ColumnTypes.LONG)
            .column("status", ColumnTypes.STRING)
            .column("checkpoint_ledger", ColumnTypes.LONG)
            .column("checkpoint_offset", ColumnTypes.LONG)
            .column("checkpoint_timestamp", ColumnTypes.TIMESTAMP)
            .column("checkpoint_duration_ms", ColumnTypes.LONG)
            .column("dirty_ledgers_count", ColumnTypes.INTEGER)
            .primaryKey("tablespace_uuid", false)
            .primaryKey("nodeid", false)
            .build();

    public SyslogstatusManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList(Transaction transaction) throws StatementExecutionException {
        boolean isVirtual = tableSpaceManager.isVirtual();
        boolean isLeader = tableSpaceManager.isLeader();
        LogSequenceNumber logSequenceNumber = isVirtual ? null : tableSpaceManager.getLog().getLastSequenceNumber();
        LogSequenceNumber checkpointLsn = isVirtual ? null : tableSpaceManager.getLastCheckpointSequenceNumber();
        long checkpointTimestamp = isVirtual ? 0L : tableSpaceManager.getLastCheckpointTimestamp();
        long checkpointDurationMs = isVirtual ? 0L : tableSpaceManager.getLastCheckpointDurationMs();

        Long ledger = isVirtual ? 0L : logSequenceNumber.ledgerId;
        Long offset = isVirtual ? 0L : logSequenceNumber.offset;
        Long cpLedger = checkpointLsn != null ? checkpointLsn.ledgerId : null;
        Long cpOffset = checkpointLsn != null ? checkpointLsn.offset : null;
        Timestamp cpTs = checkpointTimestamp > 0 ? new Timestamp(checkpointTimestamp) : null;
        Long cpDuration = checkpointTimestamp > 0 ? checkpointDurationMs : null;
        Integer dirtyLedgers = (!isVirtual && checkpointLsn != null)
                ? (int) Math.max(0L, logSequenceNumber.ledgerId - checkpointLsn.ledgerId)
                : null;

        List<Record> result = new ArrayList<>();
        result.add(RecordSerializer.makeRecord(
                table,
                "tablespace_uuid", tableSpaceManager.getTableSpaceUUID(),
                "nodeid", tableSpaceManager.getDbmanager().getNodeId(),
                "tablespace_name", tableSpaceManager.getTableSpaceName(),
                "ledger", ledger,
                "offset", offset,
                "status", isVirtual ? "virtual" : isLeader ? "leader" : "follower",
                "checkpoint_ledger", cpLedger,
                "checkpoint_offset", cpOffset,
                "checkpoint_timestamp", cpTs,
                "checkpoint_duration_ms", cpDuration,
                "dirty_ledgers_count", dirtyLedgers
        ));
        return result;

    }

}
