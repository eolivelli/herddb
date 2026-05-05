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

package herddb.log;

import herddb.model.Index;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.utils.Bytes;

/**
 * Factory for entries
 *
 * @author enrico.olivelli
 */
public class LogEntryFactory {

    public static LogEntry createTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_TABLE, transaction != null ? transaction.transactionId : 0, table.tableId, null, Bytes.from_array(payload));
    }

    public static LogEntry alterTable(Table table, Transaction transaction) {
        byte[] payload = table.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.ALTER_TABLE, transaction != null ? transaction.transactionId : 0, table.tableId, null, Bytes.from_array(payload));
    }

    public static LogEntry dropTable(Table table, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DROP_TABLE, transaction != null ? transaction.transactionId : 0, table.tableId, null, null);
    }

    /**
     * Convenience overload for tests and external tooling that build a
     * {@code DROP_TABLE} entry without a {@link Table} object handy. Uses
     * {@code tableId = 0}: the matching {@code CREATE_TABLE} in the same
     * synthetic stream is expected to have been built the same way (e.g.
     * via {@link Table.Builder} without an explicit {@code tableId}). Real
     * leader-issued DROP_TABLE entries must use
     * {@link #dropTable(Table, Transaction)} with the live table.
     *
     * @deprecated Prefer {@link #dropTable(Table, Transaction)} so the
     *             entry carries a real {@code tableId} resolvable via the
     *             tablespace's id index. Retained for compatibility with
     *             existing test code that does not have a {@link Table}.
     */
    @Deprecated
    public static LogEntry dropTable(String tableName, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DROP_TABLE, transaction != null ? transaction.transactionId : 0, 0, null, null);
    }

    public static LogEntry dropIndex(String indexName, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DROP_INDEX, transaction != null ? transaction.transactionId : 0, 0, null, Bytes.from_string(indexName));
    }

    public static LogEntry beginTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.BEGINTRANSACTION, transactionId, 0, null, null);
    }

    public static LogEntry dataConsistency(Table table, Bytes value) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.TABLE_CONSISTENCY_CHECK, 0, table.tableId, null, value);
    }

    public static LogEntry commitTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.COMMITTRANSACTION, transactionId, 0, null, null);
    }

    public static LogEntry rollbackTransaction(long transactionId) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.ROLLBACKTRANSACTION, transactionId, 0, null, null);
    }

    public static LogEntry insert(Table table, Bytes key, Bytes value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.INSERT, transaction != null ? transaction.transactionId : 0, table.tableId, key, value);
    }

    public static LogEntry update(Table table, Bytes key, Bytes value, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.UPDATE, transaction != null ? transaction.transactionId : 0, table.tableId, key, value);
    }

    public static LogEntry delete(Table table, Bytes key, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.DELETE, transaction != null ? transaction.transactionId : 0, table.tableId, key, null);
    }

    public static LogEntry createIndex(Index index, Transaction transaction) {
        // The table this index belongs to is already encoded inside the
        // serialized {@link Index} payload (see {@link Index#table}), so the
        // CREATE_INDEX entry is written with {@code tableId = 0}: the apply
        // path resolves the parent table from {@code index.table} at replay
        // time. This keeps the entry unconditionally small (1-byte vint) and
        // avoids forcing every test callsite to plumb a {@link Table} object.
        byte[] payload = index.serialize();
        return new LogEntry(System.currentTimeMillis(), LogEntryType.CREATE_INDEX, transaction != null ? transaction.transactionId : 0, 0, null, Bytes.from_array(payload));
    }

    public static LogEntry truncate(Table table, Transaction transaction) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.TRUNCATE_TABLE,
                transaction != null ? transaction.transactionId : 0, table.tableId, null, null);
    }

    public static LogEntry noop() {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.NOOP,
                -1, 0, null, null);
    }

    public static LogEntry indexingServiceRebalance(IndexingServiceRebalanceDescriptor descriptor) {
        return new LogEntry(System.currentTimeMillis(), LogEntryType.INDEXING_SERVICE_REBALANCE,
                0, 0, null, Bytes.from_array(descriptor.serialize()));
    }

}
