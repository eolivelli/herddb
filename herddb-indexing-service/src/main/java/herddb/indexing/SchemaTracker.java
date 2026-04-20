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

package herddb.indexing;

import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.model.Index;
import herddb.model.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tracks table and index schemas from commit log entries.
 *
 * @author enrico.olivelli
 */
public class SchemaTracker {

    private static final Logger LOGGER = Logger.getLogger(SchemaTracker.class.getName());

    private final Map<String, Table> tables = new HashMap<>();
    private final Map<String, Index> indexes = new HashMap<>();

    /**
     * Per-table memoized list of vector indexes. Populated lazily on first
     * {@link #getVectorIndexesForTable(String)} lookup and invalidated from
     * {@link #applyEntry(LogEntry)} on any schema change that could affect
     * the result. The existing {@code indexes} map remains the source of
     * truth so tests that mutate it via reflection still work — a reflection
     * mutation just means the cached entry is stale until the next schema
     * event or an explicit {@link #invalidateVectorIndexCache()} call.
     *
     * <p>Threading: writes happen only on the tailer thread after
     * {@code awaitPendingWork()} has drained the striped apply workers, so
     * the {@link ConcurrentHashMap} is sufficient for safe publication to
     * reader threads. The returned lists are immutable snapshots.
     */
    private final ConcurrentHashMap<String, List<Index>> vectorIndexesByTable = new ConcurrentHashMap<>();

    /**
     * Applies a log entry to update the tracked schema state.
     */
    public void applyEntry(LogEntry entry) {
        switch (entry.type) {
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                tables.put(table.name, table);
                vectorIndexesByTable.remove(table.name);
                LOGGER.log(Level.INFO, "CREATE_TABLE: {0}", table.name);
                break;
            }
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                tables.put(table.name, table);
                vectorIndexesByTable.remove(table.name);
                LOGGER.log(Level.INFO, "ALTER_TABLE: {0}", table.name);
                break;
            }
            case LogEntryType.DROP_TABLE: {
                String tableName = entry.tableName;
                tables.remove(tableName);
                vectorIndexesByTable.remove(tableName);
                LOGGER.log(Level.INFO, "DROP_TABLE: {0}", tableName);
                break;
            }
            case LogEntryType.CREATE_INDEX: {
                Index index = Index.deserialize(entry.value.to_array());
                indexes.put(index.name, index);
                if (Index.TYPE_VECTOR.equals(index.type)) {
                    vectorIndexesByTable.remove(index.table);
                }
                LOGGER.log(Level.INFO, "CREATE_INDEX: {0} type={1}", new Object[]{index.name, index.type});
                break;
            }
            case LogEntryType.DROP_INDEX: {
                // The index name is serialized in the entry value
                String indexName = new String(entry.value.to_array(), java.nio.charset.StandardCharsets.UTF_8);
                Index removed = indexes.remove(indexName);
                if (removed != null && Index.TYPE_VECTOR.equals(removed.type)) {
                    vectorIndexesByTable.remove(removed.table);
                }
                LOGGER.log(Level.INFO, "DROP_INDEX: {0}", indexName);
                break;
            }
            default:
                // Not a schema-related entry, ignore
                break;
        }
    }

    /**
     * Returns the table definition for the given table name, or null if not tracked.
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * Returns the index definition for the given index name, or null if not tracked.
     */
    public Index getIndex(String indexName) {
        return indexes.get(indexName);
    }

    /**
     * Returns all vector indexes associated with the given table. The result
     * is an immutable snapshot cached per table; tables with no vector index
     * produce an empty list with zero per-call allocations on the DML hot
     * path.
     */
    public Collection<Index> getVectorIndexesForTable(String tableName) {
        return vectorIndexesByTable.computeIfAbsent(tableName, this::computeVectorIndexesForTable);
    }

    private List<Index> computeVectorIndexesForTable(String tableName) {
        List<Index> result = null;
        for (Index idx : indexes.values()) {
            if (Index.TYPE_VECTOR.equals(idx.type) && tableName.equals(idx.table)) {
                if (result == null) {
                    result = new ArrayList<>(2);
                }
                result.add(idx);
            }
        }
        return result == null ? Collections.emptyList() : List.copyOf(result);
    }

    /**
     * Drops the cached per-table vector-index lookup. Intended for tests that
     * mutate the underlying {@code indexes} map via reflection and need the
     * cache to re-read from the source of truth.
     */
    void invalidateVectorIndexCache() {
        vectorIndexesByTable.clear();
    }

    /**
     * Returns a snapshot of every tracked index. Used by the indexing-admin
     * diagnostic CLI to enumerate indexes without knowing the table name.
     */
    public Collection<Index> getAllIndexes() {
        return new ArrayList<>(indexes.values());
    }
}
