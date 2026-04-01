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

import herddb.codec.DataAccessorForFullRecord;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.utils.Bytes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Core engine for the IndexingService.
 * <p>
 * Tails the CommitLog, buffers transactions, tracks schemas,
 * and drives VectorIndexManager instances for each vector index.
 *
 * @author enrico.olivelli
 */
public class IndexingServiceEngine implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(IndexingServiceEngine.class.getName());

    private static final long WATERMARK_SAVE_INTERVAL_ENTRIES = 1000;

    private final Path logDirectory;
    private final Path dataDirectory;
    private final Properties config;

    private WatermarkStore watermarkStore;
    private SchemaTracker schemaTracker;
    private TransactionBuffer transactionBuffer;
    private CommitLogTailer tailer;
    private Thread tailerThread;

    private volatile LogSequenceNumber lastProcessedLsn;
    private long entriesSinceLastWatermarkSave;

    private volatile StatsLogger statsLogger;

    /**
     * In-memory vector stores keyed by "table.index".
     * Each store holds all vectors for one vector index.
     */
    private final ConcurrentHashMap<String, InMemoryVectorStore> vectorStores = new ConcurrentHashMap<>();

    private static String storeKey(String table, String index) {
        return table + "." + index;
    }

    public IndexingServiceEngine(Path logDirectory, Path dataDirectory, Properties config) {
        this.logDirectory = logDirectory;
        this.dataDirectory = dataDirectory;
        this.config = config;
    }

    public void start() throws Exception {
        LOGGER.info("IndexingServiceEngine starting, logDir=" + logDirectory + ", dataDir=" + dataDirectory);

        // Initialize components
        watermarkStore = new WatermarkStore(dataDirectory);
        LogSequenceNumber watermark = watermarkStore.load();
        lastProcessedLsn = watermark;
        entriesSinceLastWatermarkSave = 0;

        schemaTracker = new SchemaTracker();
        transactionBuffer = new TransactionBuffer();

        // Create and start the tailer
        tailer = new CommitLogTailer(logDirectory, watermark, this::processEntry);
        tailerThread = new Thread(tailer, "indexing-service-tailer");
        tailerThread.setDaemon(true);
        tailerThread.start();

        LOGGER.info("IndexingServiceEngine started, watermark=" + watermark);
    }

    /**
     * Entry consumer callback invoked by the CommitLogTailer.
     */
    private void processEntry(LogSequenceNumber lsn, LogEntry entry) {
        try {
            LOGGER.log(Level.FINEST, "Processing entry at LSN {0}, type={1}, txId={2}",
                    new Object[]{lsn, entry.type, entry.transactionId});
            long txId = entry.transactionId;

            switch (entry.type) {
                case LogEntryType.BEGINTRANSACTION:
                    transactionBuffer.beginTransaction(txId);
                    break;

                case LogEntryType.COMMITTRANSACTION:
                    // Apply all buffered entries for this transaction
                    List<TransactionBuffer.BufferedLogEntry> buffered = transactionBuffer.commitTransaction(txId);
                    for (TransactionBuffer.BufferedLogEntry be : buffered) {
                        applyEntry(be.getLsn(), be.getEntry());
                    }
                    break;

                case LogEntryType.ROLLBACKTRANSACTION:
                    transactionBuffer.rollbackTransaction(txId);
                    break;

                default:
                    if (txId != 0) {
                        // Transactional entry: buffer it
                        transactionBuffer.addEntry(txId, lsn, entry);
                    } else {
                        // Non-transactional entry: apply immediately
                        applyEntry(lsn, entry);
                    }
                    break;
            }

            lastProcessedLsn = lsn;
            entriesSinceLastWatermarkSave++;

            // Periodically save watermark
            if (entriesSinceLastWatermarkSave >= WATERMARK_SAVE_INTERVAL_ENTRIES) {
                saveWatermark();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing entry at LSN " + lsn + ": " + entry, e);
        }
    }

    /**
     * Applies a single (committed or non-transactional) entry.
     */
    private void applyEntry(LogSequenceNumber lsn, LogEntry entry) {
        switch (entry.type) {
            // DDL operations: update schema tracker
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
                schemaTracker.applyEntry(entry);
                break;

            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE:
                schemaTracker.applyEntry(entry);
                // Remove all vector stores for this table
                String droppedTable = entry.tableName;
                vectorStores.entrySet().removeIf(e -> e.getKey().startsWith(droppedTable + "."));
                break;

            case LogEntryType.CREATE_INDEX:
                schemaTracker.applyEntry(entry);
                createVectorStoreIfNeeded(entry);
                break;

            case LogEntryType.DROP_INDEX: {
                String indexName = new String(entry.value.to_array(), java.nio.charset.StandardCharsets.UTF_8);
                // Remove vector store before updating schema (we need the index info)
                Index idx = schemaTracker.getIndex(indexName);
                if (idx != null && Index.TYPE_VECTOR.equals(idx.type)) {
                    vectorStores.remove(storeKey(idx.table, idx.name));
                    LOGGER.log(Level.INFO, "Removed vector store for index {0}", indexName);
                }
                schemaTracker.applyEntry(entry);
                break;
            }

            // DML operations: apply to vector indexes
            case LogEntryType.INSERT:
                applyInsert(entry);
                break;
            case LogEntryType.UPDATE:
                applyUpdate(entry);
                break;
            case LogEntryType.DELETE:
                applyDelete(entry);
                break;

            default:
                // NOOP, TABLE_CONSISTENCY_CHECK, etc. -- ignore
                break;
        }
    }

    private void createVectorStoreIfNeeded(LogEntry entry) {
        Index index = Index.deserialize(entry.value.to_array());
        if (!Index.TYPE_VECTOR.equals(index.type)) {
            return;
        }
        String key = storeKey(index.table, index.name);
        // The vector column is the first (and only) column of the vector index
        String vectorColumnName = index.columnNames[0];
        vectorStores.put(key, new InMemoryVectorStore(vectorColumnName));
        LOGGER.log(Level.INFO, "Created vector store for index {0} on column {1}",
                new Object[]{index.name, vectorColumnName});
    }

    private void applyInsert(LogEntry entry) {
        String tableName = entry.tableName;
        Collection<Index> vectorIndexes = schemaTracker.getVectorIndexesForTable(tableName);
        if (vectorIndexes.isEmpty()) {
            return;
        }
        Table table = schemaTracker.getTable(tableName);
        if (table == null) {
            LOGGER.log(Level.WARNING, "INSERT on unknown table {0}, skipping vector indexing", tableName);
            return;
        }
        Record record = new Record(entry.key, entry.value);
        DataAccessorForFullRecord accessor = new DataAccessorForFullRecord(table, record);
        for (Index idx : vectorIndexes) {
            InMemoryVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store == null) {
                continue;
            }
            float[] vector = extractVector(accessor, store.vectorColumnName);
            if (vector != null) {
                store.addVector(entry.key, vector);
            }
        }
    }

    private void applyUpdate(LogEntry entry) {
        String tableName = entry.tableName;
        Collection<Index> vectorIndexes = schemaTracker.getVectorIndexesForTable(tableName);
        if (vectorIndexes.isEmpty()) {
            return;
        }
        Table table = schemaTracker.getTable(tableName);
        if (table == null) {
            return;
        }
        Record record = new Record(entry.key, entry.value);
        DataAccessorForFullRecord accessor = new DataAccessorForFullRecord(table, record);
        for (Index idx : vectorIndexes) {
            InMemoryVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store == null) {
                continue;
            }
            // Remove old entry, add new one
            store.removeVector(entry.key);
            float[] vector = extractVector(accessor, store.vectorColumnName);
            if (vector != null) {
                store.addVector(entry.key, vector);
            }
        }
    }

    private void applyDelete(LogEntry entry) {
        String tableName = entry.tableName;
        Collection<Index> vectorIndexes = schemaTracker.getVectorIndexesForTable(tableName);
        if (vectorIndexes.isEmpty()) {
            return;
        }
        for (Index idx : vectorIndexes) {
            InMemoryVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store != null) {
                store.removeVector(entry.key);
            }
        }
    }

    private static float[] extractVector(DataAccessorForFullRecord accessor, String columnName) {
        Object value = accessor.get(columnName);
        if (value instanceof float[]) {
            return (float[]) value;
        }
        return null;
    }

    private void saveWatermark() {
        try {
            watermarkStore.save(lastProcessedLsn);
            entriesSinceLastWatermarkSave = 0;
            LOGGER.log(Level.INFO, "Saved watermark at {0}", lastProcessedLsn);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to save watermark", e);
        }
    }

    public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                  float[] vector, int limit) {
        LOGGER.log(Level.INFO, "engine search: tablespace={0}, table={1}, index={2}, limit={3}, vectorDim={4}, lastLSN={5}",
                new Object[]{tablespace, table, index, limit, vector.length, lastProcessedLsn});
        InMemoryVectorStore store = vectorStores.get(storeKey(table, index));
        if (store == null) {
            LOGGER.log(Level.WARNING, "No vector store found for {0}.{1}", new Object[]{table, index});
            return Collections.emptyList();
        }
        return store.search(vector, limit);
    }

    public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
        InMemoryVectorStore store = vectorStores.get(storeKey(table, index));
        long vectorCount = store != null ? store.size() : 0;
        return new IndexStatusInfo(vectorCount, 1,
                lastProcessedLsn != null ? lastProcessedLsn.ledgerId : -1,
                lastProcessedLsn != null ? lastProcessedLsn.offset : -1,
                "tailing");
    }

    public void setStatsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
    }

    /**
     * Registers per-index gauges for a vector index managed by this engine.
     * Called when a VectorIndexManager instance is created for a vector index.
     * The gauges report real data once the VectorIndexManager is wired in.
     */
    void registerIndexMetrics(String tablespace, String table, String indexName) {
        StatsLogger sl = this.statsLogger;
        if (sl == null) {
            return;
        }
        StatsLogger indexStats = sl
                .scope("tablespace_" + tablespace)
                .scope("table_" + table)
                .scope("vidx_" + indexName);

        // TODO: wire these gauges to actual VectorIndexManager instances
        indexStats.registerGauge("node_count", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("live_node_count", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("ondisk_node_count", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("segment_count", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("dimension", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("estimated_size_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() { return 0L; }
            @Override public Long getSample() { return 0L; }
        });
        indexStats.registerGauge("live_vectors_memory_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() { return 0L; }
            @Override public Long getSample() { return 0L; }
        });
        indexStats.registerGauge("live_shard_count", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
        indexStats.registerGauge("dirty", new Gauge<Integer>() {
            @Override public Integer getDefaultValue() { return 0; }
            @Override public Integer getSample() { return 0; }
        });
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("IndexingServiceEngine closing");

        // Stop the tailer
        if (tailer != null) {
            tailer.close();
        }
        if (tailerThread != null) {
            tailerThread.interrupt();
            try {
                tailerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Save final watermark
        if (lastProcessedLsn != null && watermarkStore != null) {
            saveWatermark();
        }

        // Clear all in-memory vector stores
        vectorStores.clear();

        LOGGER.info("IndexingServiceEngine closed");
    }

    /**
     * Status information for a single vector index.
     */
    public static class IndexStatusInfo {
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
