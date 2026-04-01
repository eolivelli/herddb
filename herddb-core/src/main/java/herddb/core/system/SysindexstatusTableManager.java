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
import herddb.core.AbstractIndexManager;
import herddb.core.TableSpaceManager;
import herddb.index.brin.BRINIndexManager;
import herddb.index.vector.RemoteVectorIndexService;
import herddb.index.vector.VectorIndexManager;
import herddb.model.ColumnTypes;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table Manager for the SYSINDEXSTATUS virtual table.
 * Exposes runtime status and configuration for each index.
 */
public class SysindexstatusTableManager extends AbstractSystemTableManager {

    private static final Table TABLE = Table
            .builder()
            .name("sysindexstatus")
            .column("tablespace", ColumnTypes.STRING)
            .column("table_name", ColumnTypes.STRING)
            .column("index_name", ColumnTypes.STRING)
            .column("index_type", ColumnTypes.STRING)
            .column("index_uuid", ColumnTypes.STRING)
            .column("properties", ColumnTypes.STRING)
            .primaryKey("table_name", false)
            .primaryKey("index_name", false)
            .build();

    public SysindexstatusTableManager(TableSpaceManager parent) {
        super(parent, TABLE);
    }

    @Override
    protected Iterable<Record> buildVirtualRecordList(Transaction transaction) {
        List<Table> tables = tableSpaceManager.getAllVisibleTables(transaction);
        List<Record> result = new ArrayList<>();
        for (Table t : tables) {
            Map<String, AbstractIndexManager> indexesOnTable = tableSpaceManager.getIndexesOnTable(t.name);
            if (indexesOnTable == null) {
                continue;
            }
            for (AbstractIndexManager indexManager : indexesOnTable.values()) {
                Index index = indexManager.getIndex();
                String properties = buildProperties(indexManager);
                result.add(RecordSerializer.makeRecord(table,
                        "tablespace", t.tablespace,
                        "table_name", t.name,
                        "index_name", index.name,
                        "index_type", index.type,
                        "index_uuid", index.uuid,
                        "properties", properties
                ));
            }
        }
        return result;
    }

    private static String buildProperties(AbstractIndexManager indexManager) {
        Map<String, Object> props = new LinkedHashMap<>();
        if (indexManager instanceof VectorIndexManager) {
            VectorIndexManager vim = (VectorIndexManager) indexManager;
            try {
                RemoteVectorIndexService.IndexStatusInfo status = vim.getRemoteIndexStatus();
                props.put("vectorCount", status.getVectorCount());
                props.put("segmentCount", status.getSegmentCount());
                props.put("lastLsnLedger", status.getLastLsnLedger());
                props.put("lastLsnOffset", status.getLastLsnOffset());
                props.put("status", status.getStatus());
            } catch (Exception e) {
                props.put("error", e.getMessage());
            }
        } else if (indexManager instanceof BRINIndexManager) {
            BRINIndexManager brin = (BRINIndexManager) indexManager;
            props.put("numBlocks", brin.getNumBlocks());
        }
        return toJson(props);
    }

    static String toJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(entry.getKey()).append('"').append(':');
            Object value = entry.getValue();
            if (value instanceof String) {
                sb.append('"').append(value).append('"');
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else if (value instanceof Float) {
                float f = (Float) value;
                if (f == (long) f) {
                    sb.append(String.valueOf((long) f));
                } else {
                    sb.append(f);
                }
            } else {
                sb.append(value);
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
