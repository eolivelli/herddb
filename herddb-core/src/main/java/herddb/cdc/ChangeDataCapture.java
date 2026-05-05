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
package herddb.cdc;


import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT;
import static herddb.client.ClientConfiguration.PROPERTY_ZOOKEEPER_SESSIONTIMEOUT;
import herddb.client.ClientConfiguration;
import herddb.cluster.BookkeeperCommitLog;
import herddb.cluster.BookkeeperCommitLogManager;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.DataAccessorForFullRecord;
import herddb.log.CommitLog;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Record;
import herddb.model.Table;
import herddb.server.ServerConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * This utility provides a way to Change Data Capture with HerdDB.
 */
public class ChangeDataCapture implements AutoCloseable {

    /**
     * Type of Mutation
     */
    public enum MutationType {
        INSERT,
        UPDATE,
        DELETE,
        CREATE_TABLE,
        DROP_TABLE,
        ALTER_TABLE
    }

    /**
     * Details about a Mutation
     */
    public static class Mutation {
        private final Table table;
        private final MutationType mutationType;
        private final DataAccessorForFullRecord record;
        private final LogSequenceNumber logSequenceNumber;
        private final long timestamp;

        public Mutation(Table table, MutationType mutationType,
                        DataAccessorForFullRecord record, LogSequenceNumber logSequenceNumber,
                        long timestamp) {
            this.table = table;
            this.mutationType = mutationType;
            this.record = record;
            this.logSequenceNumber = logSequenceNumber;
            this.timestamp = timestamp;
        }

        public Table getTable() {
            return table;
        }

        public MutationType getMutationType() {
            return mutationType;
        }

        public DataAccessorForFullRecord getRecord() {
            return record;
        }

        public LogSequenceNumber getLogSequenceNumber() {
            return logSequenceNumber;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Mutation{"
                    + "table=" + table
                    + ", mutationType=" + mutationType
                    + ", record=" + record
                    + ", logSequenceNumber=" + logSequenceNumber
                    + ", timestamp=" + timestamp
                    + '}';
        }
    }

    /**
     * Implement this interface in order to receive the flow of Mutations
     */
    public interface MutationListener {
        void accept(Mutation mutation);
    }

    public interface TableSchemaHistoryStorage {
        /**
         * Stores a schema change for a table
         * @param lsn the lsn at which the change happened
         * @param table the schema
         */
        void storeSchema(LogSequenceNumber lsn, Table table);

        /**
         * Return the schema at the given log sequence number
         * @param lsn
         * @return the schema
         */
        Table fetchSchema(LogSequenceNumber lsn, String tableName);

        /**
         * Optional id → name resolver used by the CDC to translate a
         * commit-log {@code tableId} (issue #408 — entries no longer carry
         * the table name) back into a name when the in-memory id → name
         * cache is empty (e.g. CDC restarts at an LSN past the relevant
         * {@code CREATE_TABLE} entry).
         * <p>
         * The default implementation returns {@code null}, preserving
         * source / binary compatibility with existing implementations:
         * callers that hit the {@code null} fall back to whatever name
         * they have observed on the in-flight commit-log stream. Storage
         * implementations that index stored schemas by id can override
         * this to expose the mapping across CDC restarts.
         *
         * @param tableId per-tablespace integer id from a {@link LogEntry}
         * @return the table name if known, or {@code null}
         */
        default String resolveTableName(int tableId) {
            return null;
        }
    }

    private final ClientConfiguration configuration;
    private final MutationListener listener;
    private final TableSchemaHistoryStorage tableSchemaHistoryStorage;
    private LogSequenceNumber lastPosition;
    private final String tableSpaceUUID;
    private volatile boolean closed = false;
    private volatile boolean running = false;

    private ZookeeperMetadataStorageManager zookeeperMetadataStorageManager;
    private BookkeeperCommitLogManager manager;
    private Map<Long, TransactionHolder> transactions = new HashMap<>();

    private static class TransactionHolder {
        private List<Mutation> mutations = new ArrayList<>();
        // Issue #408: keyed by Table#tableId — the in-flight transaction's
        // schema overrides for CREATE/ALTER/DROP TABLE entries that have not
        // yet been COMMITted, so DML inside the same transaction can resolve
        // its target without consulting the persistent schema history.
        // The public TableSchemaHistoryStorage API is name-keyed; this map
        // is the CDC's private translation layer.
        private Map<Integer, Table> tablesDefinitions = new HashMap<>();
    }

    /**
     * Issue #408: the WAL no longer encodes the table name on each entry.
     * The CDC translates {@code entry.tableId → tableName} via this map (kept
     * up to date from {@code CREATE_TABLE} / {@code ALTER_TABLE} entries seen
     * in the stream) before invoking the user-provided
     * {@link TableSchemaHistoryStorage#fetchSchema(LogSequenceNumber, String)},
     * preserving the historical name-keyed public API.
     */
    private final Map<Integer, String> tableIdToName = new HashMap<>();

    public ChangeDataCapture(String tableSpaceUUID, ClientConfiguration configuration, MutationListener listener, LogSequenceNumber startingPosition, TableSchemaHistoryStorage tableSchemaHistoryStorage) {
        this.configuration = configuration;
        this.listener = listener;
        this.lastPosition = startingPosition;
        this.tableSpaceUUID = tableSpaceUUID;
        this.tableSchemaHistoryStorage = tableSchemaHistoryStorage;
    }

    /**
     * Bootstrap the procedure.
     * @throws Exception
     */
    public void start() throws Exception {
        zookeeperMetadataStorageManager = buildMetadataStorageManager(configuration);
        manager = new BookkeeperCommitLogManager(zookeeperMetadataStorageManager, new ServerConfiguration(), NullStatsLogger.INSTANCE);
        manager.start();
    }

    /**
     * Execute one run
     * @return the last sequence number, to be used to configure CDC for the next execution
     * @throws Exception
     */
    public LogSequenceNumber run() throws Exception {
        if (zookeeperMetadataStorageManager == null) {
            throw new IllegalStateException("not started");
        }

        try (BookkeeperCommitLog cdc = manager.createCommitLog(tableSpaceUUID, tableSpaceUUID, "cdc");) {
            running = true;
            CommitLog.FollowerContext context = cdc.startFollowing(lastPosition);
            cdc.followTheLeader(lastPosition, new CommitLog.EntryAcceptor() {
                @Override
                public boolean accept(LogSequenceNumber lsn, LogEntry entry) throws Exception {
                    applyEntry(entry, lsn);
                    lastPosition = lsn;
                    return !closed;
                }
            }, context);
            return lastPosition;
        } finally {
            running = false;
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        long _start = System.currentTimeMillis();

        while (running
                && (System.currentTimeMillis() - _start < 10_000)) {
            Thread.sleep(100);
        }
        if (manager != null) {
            manager.close();
        }
        if (zookeeperMetadataStorageManager != null) {
            zookeeperMetadataStorageManager.close();
        }
    }

    private void fire(Mutation mutation, long transactionId) {
        if (transactionId > 0) {
            TransactionHolder transaction = transactions.get(transactionId);
            transaction.mutations.add(mutation);
        } else {
            listener.accept(mutation);
        }
    }

    private Table lookupTable(LogSequenceNumber lsn, LogEntry entry) {
        int tableId = entry.tableId;
        if (entry.transactionId > 0) {
            TransactionHolder transaction = transactions.get(entry.transactionId);
            Table table = transaction.tablesDefinitions.get(tableId);
            if (table != null) {
                return table;
            }
        }
        // Translate id → name and delegate to the user-provided history
        // storage (name-keyed public API). The map is populated from
        // CREATE_TABLE / ALTER_TABLE entries observed in the stream;
        // when the CDC starts past a CREATE_TABLE (e.g. resuming from a
        // mid-log LSN after a restart) the cache is empty for that id —
        // we then ask the storage's optional resolveTableName(id) hook
        // before giving up.
        String tableName = tableIdToName.get(tableId);
        if (tableName == null) {
            tableName = tableSchemaHistoryStorage.resolveTableName(tableId);
            if (tableName == null) {
                // Surface the gap loudly so operators can see that the
                // CDC cannot translate this id (no matching CREATE_TABLE
                // observed since startup, and the storage's optional
                // hook has no record either). The Mutation will be
                // delivered with table == null, mirroring the historical
                // behaviour for an unknown name.
                LOG.log(java.util.logging.Level.WARNING,
                        "CDC could not resolve tableId={0} at lsn={1} — mutation will carry a null Table",
                        new Object[]{tableId, lsn});
                return null;
            }
            tableIdToName.put(tableId, tableName);
        }
        return tableSchemaHistoryStorage.fetchSchema(lsn, tableName);
    }

    private static final java.util.logging.Logger LOG =
            java.util.logging.Logger.getLogger(ChangeDataCapture.class.getName());

    private void applyEntry(LogEntry entry, LogSequenceNumber lsn) throws Exception {
        switch (entry.type) {
            case LogEntryType.NOOP:
            case LogEntryType.CREATE_INDEX:
            case LogEntryType.DROP_INDEX:
                break;
            case LogEntryType.DROP_TABLE: {
                Table table = lookupTable(lsn, entry);
                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    // set null to mark the table as DROPPED
                    transaction.tablesDefinitions.put(entry.tableId, null);
                }

                fire(new Mutation(table, MutationType.DROP_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.CREATE_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                // Track the id → name mapping so future DML entries (which
                // only carry the integer id) can resolve back to the
                // user-provided name-keyed schema-history storage.
                tableIdToName.put(table.tableId, table.name);
                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    transaction.tablesDefinitions.put(entry.tableId, table);
                } else {
                    tableSchemaHistoryStorage.storeSchema(lsn, table);
                }
                fire(new Mutation(table, MutationType.CREATE_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.ALTER_TABLE: {
                Table table = Table.deserialize(entry.value.to_array());
                // ALTER may rename the table (same tableId, new name); refresh
                // the local id → name map so subsequent DML resolves to the
                // current name. The Table's id is preserved by
                // Table#applyAlterTable on the leader.
                tableIdToName.put(table.tableId, table.name);
                if (entry.transactionId > 0) {
                    TransactionHolder transaction = transactions.get(entry.transactionId);
                    transaction.tablesDefinitions.put(entry.tableId, table);
                } else {
                    tableSchemaHistoryStorage.storeSchema(lsn, table);
                }
                fire(new Mutation(table, MutationType.ALTER_TABLE, null, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.INSERT: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.INSERT, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.DELETE: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.DELETE, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.UPDATE: {
                Table table = lookupTable(lsn, entry);
                DataAccessorForFullRecord record = new DataAccessorForFullRecord(table, new Record(entry.key, entry.value));
                fire(new Mutation(table, MutationType.UPDATE, record, lsn, entry.timestamp), entry.transactionId);
            }
            break;
            case LogEntryType.BEGINTRANSACTION: {
                transactions.put(entry.transactionId, new TransactionHolder());
            }
            break;
            case LogEntryType.COMMITTRANSACTION: {
                TransactionHolder transaction = transactions.remove(entry.transactionId);
                transaction.tablesDefinitions.forEach((tableId, tableDef) -> {
                    if (tableDef == null) { // DROP TABLE

                    } else { // CREATE/ALTER
                        tableSchemaHistoryStorage.storeSchema(lsn, tableDef);
                    }
                });
                for (Mutation mutation : transaction.mutations) {
                    listener.accept(mutation);
                }
            }
            break;
            case LogEntryType.ROLLBACKTRANSACTION: {
                TransactionHolder rolled = transactions.remove(entry.transactionId);
                // Issue #408 review: scrub the per-id mapping for any
                // CREATE_TABLE / ALTER_TABLE that this transaction
                // stamped — its id never committed and must not leak
                // into the in-memory id → name cache. Without this,
                // a BEGIN; CREATE TABLE; ROLLBACK sequence keeps a
                // stale (rolledBackId → name) entry forever, growing
                // the cache linearly in the number of rolled-back
                // CREATEs and leaving the CDC's view of the id space
                // out of sync with the leader.
                if (rolled != null) {
                    // Remove via the boxed key — the cache itself is
                    // Integer-keyed, so deboxing here would only force
                    // a re-box at the call site (caught by SpotBugs
                    // BX_UNBOXING_IMMEDIATELY_REBOXED).
                    for (Integer id : rolled.tablesDefinitions.keySet()) {
                        if (id != null) {
                            tableIdToName.remove(id);
                        }
                    }
                }
                break;
            }
            default:
                // discard unknown entry types
                break;
        }
    }

    private static ZookeeperMetadataStorageManager buildMetadataStorageManager(ClientConfiguration configuration)
            throws MetadataStorageManagerException {
        String zkAddress = configuration.getString(PROPERTY_ZOOKEEPER_ADDRESS, PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
        String zkPath = configuration.getString(PROPERTY_ZOOKEEPER_PATH, PROPERTY_ZOOKEEPER_PATH_DEFAULT);
        int sessionTimeout = configuration.getInt(PROPERTY_ZOOKEEPER_SESSIONTIMEOUT, 60000);
        ZookeeperMetadataStorageManager zk = new ZookeeperMetadataStorageManager(zkAddress, sessionTimeout, zkPath);
        zk.start(false);
        return zk;
    }
}
