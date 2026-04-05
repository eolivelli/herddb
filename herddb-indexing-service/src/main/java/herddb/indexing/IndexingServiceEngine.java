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

import herddb.cluster.BookKeeperCommitLogTailer;
import herddb.cluster.ZookeeperMetadataStorageManager;
import herddb.codec.DataAccessorForFullRecord;
import herddb.core.MemoryManager;
import herddb.file.FileMetadataStorageManager;
import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.PersistentVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.index.vector.VectorMemoryBudget;
import herddb.log.CommitLogTailing;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.metadata.MetadataStorageManager;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.utils.Bytes;
import herddb.utils.XXHash64Utils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
public class IndexingServiceEngine implements AutoCloseable, VectorMemoryBudget {

    private static final Logger LOGGER = Logger.getLogger(IndexingServiceEngine.class.getName());

    private static final long WATERMARK_SAVE_INTERVAL_ENTRIES = 1000;

    private final Path logDirectory;
    private final Path dataDirectory;
    private final IndexingServerConfiguration config;

    private final int instanceId;
    private final int numInstances;

    private WatermarkStore watermarkStore;
    private SchemaTracker schemaTracker;
    private TransactionBuffer transactionBuffer;
    private CommitLogTailing tailer;
    private Thread tailerThread;

    private volatile LogSequenceNumber lastProcessedLsn;
    private long entriesSinceLastWatermarkSave;

    private volatile StatsLogger statsLogger;

    private MetadataStorageManager metadataStorageManager;
    private boolean ownsMetadataStorageManager;

    private MemoryManager memoryManager;
    private DataStorageManager dataStorageManager;
    private long maxVectorMemoryBytes = Long.MAX_VALUE;
    private volatile String tableSpaceUUID;

    private ExecutorService[] applyWorkers;
    private int applyParallelism;
    private volatile Throwable asyncError;

    /**
     * In-memory vector stores keyed by "table.index".
     * Each store holds all vectors for one vector index.
     */
    private final ConcurrentHashMap<String, AbstractVectorStore> vectorStores = new ConcurrentHashMap<>();

    private VectorStoreFactory vectorStoreFactory = (indexName, tableName, vectorColumnName, dataDir, indexProperties) ->
            new InMemoryVectorStore(vectorColumnName,
                    InMemoryVectorStore.parseSimilarityType(
                            indexProperties != null ? indexProperties.get(VectorIndexManager.PROP_SIMILARITY) : null));

    private static String storeKey(String table, String index) {
        return table + "." + index;
    }

    public IndexingServiceEngine(Path logDirectory, Path dataDirectory, IndexingServerConfiguration config) {
        this.logDirectory = logDirectory;
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.instanceId = config.getInt(IndexingServerConfiguration.PROPERTY_INSTANCE_ID,
                IndexingServerConfiguration.PROPERTY_INSTANCE_ID_DEFAULT);
        this.numInstances = config.getInt(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                IndexingServerConfiguration.PROPERTY_NUM_INSTANCES_DEFAULT);
    }

    private MetadataStorageManager buildMetadataStorageManager() {
        String mode = config.getString(IndexingServerConfiguration.PROPERTY_MODE,
                IndexingServerConfiguration.PROPERTY_MODE_DEFAULT);
        switch (mode) {
            case ServerConfiguration.PROPERTY_MODE_STANDALONE: {
                Path metadataDirectory = java.nio.file.Paths.get(
                        config.getString(IndexingServerConfiguration.PROPERTY_METADATA_DIR,
                                IndexingServerConfiguration.PROPERTY_METADATA_DIR_DEFAULT)).toAbsolutePath();
                LOGGER.log(Level.INFO, "Indexing service cluster metadata directory: {0}", metadataDirectory);
                return new FileMetadataStorageManager(metadataDirectory);
            }
            case ServerConfiguration.PROPERTY_MODE_CLUSTER:
            case ServerConfiguration.PROPERTY_MODE_DISKLESSCLUSTER: {
                String zkAddress = config.getString(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS,
                        IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
                int zkSessionTimeout = config.getInt(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                        IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
                String zkPath = config.getString(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH,
                        IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT);
                return new ZookeeperMetadataStorageManager(zkAddress, zkSessionTimeout, zkPath);
            }
            default:
                throw new IllegalArgumentException("Unknown server.mode: " + mode);
        }
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public void setVectorStoreFactory(VectorStoreFactory factory) {
        this.vectorStoreFactory = factory;
    }

    public void setMemoryManager(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        LOGGER.log(Level.INFO, "MemoryManager set: maxDataUsedMemory={0} MB, maxLogicalPageSize={1}",
                new Object[]{memoryManager.getMaxDataUsedMemory() / (1024 * 1024),
                             memoryManager.getMaxLogicalPageSize()});
    }

    public void setMaxVectorMemoryBytes(long maxVectorMemoryBytes) {
        this.maxVectorMemoryBytes = maxVectorMemoryBytes;
        LOGGER.log(Level.INFO, "MaxVectorMemoryBytes set: {0} MB",
                new Object[]{maxVectorMemoryBytes / (1024 * 1024)});
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    public void setDataStorageManager(DataStorageManager dataStorageManager) {
        this.dataStorageManager = dataStorageManager;
        LOGGER.log(Level.INFO, "DataStorageManager set: {0}", dataStorageManager.getClass().getName());
    }

    public DataStorageManager getDataStorageManager() {
        return dataStorageManager;
    }

    public void setMetadataStorageManager(MetadataStorageManager metadataStorageManager) {
        this.metadataStorageManager = metadataStorageManager;
    }

    public MetadataStorageManager getMetadataStorageManager() {
        return metadataStorageManager;
    }

    // -------------------------------------------------------------------------
    // VectorMemoryBudget implementation
    // -------------------------------------------------------------------------

    @Override
    public long totalEstimatedMemoryUsageBytes() {
        long total = 0;
        for (AbstractVectorStore store : vectorStores.values()) {
            total += store.estimatedMemoryUsageBytes();
        }
        return total;
    }

    @Override
    public long maxMemoryBytes() {
        return maxVectorMemoryBytes;
    }

    public void start() throws Exception {
        LOGGER.info("IndexingServiceEngine starting, logDir=" + logDirectory + ", dataDir=" + dataDirectory);

        // Start the data storage manager if configured
        if (dataStorageManager != null) {
            dataStorageManager.start();
            LOGGER.info("DataStorageManager started");
        }

        // Configure VectorStoreFactory based on storage type
        String storageType = config.getString(IndexingServerConfiguration.PROPERTY_STORAGE_TYPE,
                IndexingServerConfiguration.PROPERTY_STORAGE_TYPE_DEFAULT);
        if ("file".equals(storageType) && dataStorageManager != null && memoryManager != null) {
            LOGGER.info("Configuring PersistentVectorStore factory (storage type: file)");
            final DataStorageManager dsm = dataStorageManager;
            final MemoryManager mm = memoryManager;
            final Path tmpDir = dataDirectory;
            final int m = config.getInt(IndexingServerConfiguration.PROPERTY_VECTOR_M,
                    IndexingServerConfiguration.PROPERTY_VECTOR_M_DEFAULT);
            final int beamWidth = config.getInt(IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH,
                    IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH_DEFAULT);
            final float neighborOverflow = (float) config.getDouble(
                    IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW,
                    IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW_DEFAULT);
            final float alpha = (float) config.getDouble(IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA,
                    IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA_DEFAULT);
            final boolean fusedPQ = config.getBoolean(IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ,
                    IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ_DEFAULT);
            final long maxSegmentSize = config.getLong(IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE,
                    IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE_DEFAULT);
            final int maxLiveGraphSize = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE,
                    IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_GRAPH_SIZE_DEFAULT);
            final long compactionInterval = config.getLong(
                    IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL,
                    IndexingServerConfiguration.PROPERTY_COMPACTION_INTERVAL_DEFAULT);

            final long vectorMemLimit = maxVectorMemoryBytes;
            final VectorMemoryBudget budget = this;
            vectorStoreFactory = (indexName, tableName, vectorColumnName, dataDir, indexProperties) -> {
                var similarityFunction = PersistentVectorStore.parseSimilarityFunction(
                        indexProperties != null ? indexProperties.get(VectorIndexManager.PROP_SIMILARITY) : null);
                PersistentVectorStore store = new PersistentVectorStore(
                        indexName, tableName, tableSpaceUUID, vectorColumnName,
                        tmpDir, dsm, mm,
                        m, beamWidth, neighborOverflow, alpha,
                        fusedPQ, maxSegmentSize, maxLiveGraphSize,
                        compactionInterval,
                        similarityFunction, vectorMemLimit, budget);
                try {
                    store.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start PersistentVectorStore " + indexName, e);
                }
                return store;
            };

        } else {
            LOGGER.info("Using InMemoryVectorStore factory (storage type: " + storageType + ")");
        }

        // Initialize components
        watermarkStore = new WatermarkStore(dataDirectory);
        LogSequenceNumber watermark = watermarkStore.load();
        lastProcessedLsn = watermark;
        entriesSinceLastWatermarkSave = 0;

        schemaTracker = new SchemaTracker();
        transactionBuffer = new TransactionBuffer();

        // Initialize striped DML apply workers
        int configuredParallelism = config.getInt(
                IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM,
                IndexingServerConfiguration.PROPERTY_APPLY_PARALLELISM_DEFAULT);
        applyParallelism = configuredParallelism > 0
                ? configuredParallelism
                : Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        int queueCapacity = config.getInt(
                IndexingServerConfiguration.PROPERTY_APPLY_QUEUE_CAPACITY,
                IndexingServerConfiguration.PROPERTY_APPLY_QUEUE_CAPACITY_DEFAULT);
        applyWorkers = new ExecutorService[applyParallelism];
        for (int i = 0; i < applyParallelism; i++) {
            final int idx = i;
            applyWorkers[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(queueCapacity),
                    r -> {
                        Thread t = new Thread(r, "indexing-apply-worker-" + idx);
                        t.setDaemon(true);
                        return t;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }
        LOGGER.log(Level.INFO, "DML apply workers started, parallelism={0}, queueCapacity={1}",
                new Object[]{applyParallelism, queueCapacity});

        // Validate instance identity
        if (numInstances < 1) {
            throw new IllegalArgumentException("numInstances must be >= 1, got " + numInstances);
        }
        if (instanceId < 0 || instanceId >= numInstances) {
            throw new IllegalArgumentException(
                    "instanceId must be in [0, " + (numInstances - 1) + "], got " + instanceId);
        }
        LOGGER.log(Level.INFO, "Instance identity: instanceId={0}, numInstances={1}",
                new Object[]{instanceId, numInstances});

        // Boot MetadataStorageManager if not injected
        if (metadataStorageManager == null) {
            metadataStorageManager = buildMetadataStorageManager();
            ownsMetadataStorageManager = true;
            metadataStorageManager.start();
            LOGGER.log(Level.INFO, "MetadataStorageManager started: {0}",
                    metadataStorageManager.getClass().getName());
        }

        // Resolve tablespace name to UUID, polling until available or interrupted
        String tablespaceName = config.getString(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME_DEFAULT);
        long pollIntervalMs = config.getLong(IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS_DEFAULT);
        TableSpace tableSpace = null;
        while (true) {
            tableSpace = metadataStorageManager.describeTableSpace(tablespaceName);
            if (tableSpace != null) {
                break;
            }
            LOGGER.log(Level.INFO, "Tablespace ''{0}'' not yet available, retrying in {1}ms...",
                    new Object[]{tablespaceName, pollIntervalMs});
            Thread.sleep(pollIntervalMs);
        }
        this.tableSpaceUUID = tableSpace.uuid;
        LOGGER.log(Level.INFO, "Resolved tablespace name ''{0}'' to UUID ''{1}''",
                new Object[]{tablespaceName, tableSpaceUUID});

        // Create and start the tailer
        String logType = config.getString(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                IndexingServerConfiguration.PROPERTY_LOG_TYPE_DEFAULT);
        if ("bookkeeper".equals(logType)) {
            String zkAddress = config.getString(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS,
                    IndexingServerConfiguration.PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT);
            int zkSessionTimeout = config.getInt(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT,
                    IndexingServerConfiguration.PROPERTY_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT);
            String zkPath = config.getString(IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH,
                    IndexingServerConfiguration.PROPERTY_ZOOKEEPER_PATH_DEFAULT);
            String bkLedgersPath = config.getString(IndexingServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH,
                    IndexingServerConfiguration.PROPERTY_BOOKKEEPER_LEDGERS_PATH_DEFAULT);
            LOGGER.log(Level.INFO, "Creating BookKeeperCommitLogTailer, zk={0}, tsUUID={1}",
                    new Object[]{zkAddress, tableSpaceUUID});
            tailer = new BookKeeperCommitLogTailer(zkAddress, zkSessionTimeout, zkPath,
                    bkLedgersPath, tableSpaceUUID, watermark, this::processEntry);
        } else {
            tailer = new FileCommitLogTailer(logDirectory, tableSpaceUUID, watermark, this::processEntry);
        }
        tailerThread = new Thread(tailer, "indexing-service-tailer");
        tailerThread.setDaemon(true);
        tailerThread.start();

        LOGGER.info("IndexingServiceEngine started, watermark=" + watermark);
    }

    /**
     * Entry consumer callback invoked by the commit log tailer.
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
                    applyBufferedEntries(buffered);
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
                        applySingleEntry(lsn, entry);
                    }
                    break;
            }

            lastProcessedLsn = lsn;
            entriesSinceLastWatermarkSave++;

            // Periodically save watermark
            if (entriesSinceLastWatermarkSave >= WATERMARK_SAVE_INTERVAL_ENTRIES) {
                awaitPendingWork();
                saveWatermark();
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error processing entry at LSN " + lsn + ": " + entry, e);
        }
    }

    private void applyBufferedEntries(List<TransactionBuffer.BufferedLogEntry> entries) {
        for (TransactionBuffer.BufferedLogEntry be : entries) {
            applySingleEntry(be.getLsn(), be.getEntry());
        }
    }

    private void applySingleEntry(LogSequenceNumber lsn, LogEntry entry) {
        if (isDdlType(entry.type)) {
            // DDL must be synchronous: drain all pending DML first
            awaitPendingWork();
            applyEntry(lsn, entry);
        } else if (isDmlType(entry.type)) {
            checkAsyncError();
            submitDmlAsync(lsn, entry);
        } else {
            applyEntry(lsn, entry);
        }
    }

    private void submitDmlAsync(LogSequenceNumber lsn, LogEntry entry) {
        int stripe = Math.floorMod(entry.key.hashCode(), applyParallelism);
        applyWorkers[stripe].execute(() -> {
            try {
                applyEntry(lsn, entry);
            } catch (Throwable t) {
                asyncError = t;
                LOGGER.log(Level.SEVERE, "Async DML apply failed at LSN " + lsn, t);
            }
        });
    }

    private void awaitPendingWork() {
        if (applyWorkers == null) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(applyParallelism);
        for (ExecutorService worker : applyWorkers) {
            worker.execute(latch::countDown);
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for pending DML work", e);
        }
        checkAsyncError();
    }

    private void checkAsyncError() {
        Throwable err = asyncError;
        if (err != null) {
            asyncError = null;
            throw new RuntimeException("Async DML apply failed", err);
        }
    }

    private static boolean isDmlType(short type) {
        return type == LogEntryType.INSERT
                || type == LogEntryType.UPDATE
                || type == LogEntryType.DELETE;
    }

    private static boolean isDdlType(short type) {
        return type == LogEntryType.CREATE_TABLE
                || type == LogEntryType.ALTER_TABLE
                || type == LogEntryType.DROP_TABLE
                || type == LogEntryType.TRUNCATE_TABLE
                || type == LogEntryType.CREATE_INDEX
                || type == LogEntryType.DROP_INDEX;
    }

    /**
     * Routes entry through the async pipeline (used by tests).
     * DDL entries are applied synchronously after draining pending work.
     * DML entries are submitted to the striped apply workers.
     */
    // package-private for testing
    void applySingleEntryForTest(LogSequenceNumber lsn, LogEntry entry) {
        applySingleEntry(lsn, entry);
    }

    /**
     * Drains all pending async DML work (used by tests).
     */
    // package-private for testing
    void awaitPendingWorkForTest() {
        awaitPendingWork();
    }

    /**
     * Applies a single (committed or non-transactional) entry.
     */
    // package-private for testing
    void applyEntry(LogSequenceNumber lsn, LogEntry entry) {
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
        AbstractVectorStore store = vectorStoreFactory.create(index.name, index.table, vectorColumnName, dataDirectory, index.properties);
        vectorStores.put(key, store);
        registerIndexMetrics(index.tablespace, index.table, index.name, store);
        LOGGER.log(Level.INFO, "Created vector store for index {0} on column {1} with properties {2}",
                new Object[]{index.name, vectorColumnName, index.properties});
    }

    /**
     * Determines whether an INSERT for the given primary key should be accepted by this instance
     * based on the shard assignment.
     */
    boolean isInsertAcceptedLocally(Bytes key, int numShards) {
        if (numInstances <= 1) {
            return true;
        }
        if (numShards <= 1) {
            return true;
        }
        long hash = XXHash64Utils.hash(key.getBuffer(), key.getOffset(), key.getLength());
        int shardId = Math.floorMod((int) hash, numShards);
        return shardId % numInstances == instanceId;
    }

    private int getNumShardsForTable(Collection<Index> vectorIndexes) {
        for (Index idx : vectorIndexes) {
            String val = idx.properties.get(VectorIndexManager.PROP_NUM_SHARDS);
            if (val != null) {
                try {
                    return Integer.parseInt(val);
                } catch (NumberFormatException e) {
                    LOGGER.log(Level.WARNING, "Invalid numShards value: {0}", val);
                }
            }
        }
        return 1;
    }

    private void applyInsert(LogEntry entry) {
        String tableName = entry.tableName;
        Collection<Index> vectorIndexes = schemaTracker.getVectorIndexesForTable(tableName);
        if (vectorIndexes.isEmpty()) {
            return;
        }
        // Shard filtering: skip INSERT if this instance does not own the shard
        int numShards = getNumShardsForTable(vectorIndexes);
        if (!isInsertAcceptedLocally(entry.key, numShards)) {
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
            AbstractVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store == null) {
                continue;
            }
            float[] vector = extractVector(accessor, store.getVectorColumnName());
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
            AbstractVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store == null) {
                continue;
            }
            // Remove old entry, add new one
            store.removeVector(entry.key);
            float[] vector = extractVector(accessor, store.getVectorColumnName());
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
            AbstractVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
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
            LOGGER.log(Level.FINE, "Saved watermark at {0}", lastProcessedLsn);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to save watermark", e);
        }
    }

    public List<Map.Entry<Bytes, Float>> search(String tablespace, String table, String index,
                                                  float[] vector, int limit) {
        AbstractVectorStore store = vectorStores.get(storeKey(table, index));
        if (store == null) {
            LOGGER.log(Level.WARNING, "No vector store found for {0}.{1}", new Object[]{table, index});
            return Collections.emptyList();
        }
        return store.search(vector, limit);
    }

    public IndexStatusInfo getIndexStatus(String tablespace, String table, String index) {
        AbstractVectorStore store = vectorStores.get(storeKey(table, index));
        long vectorCount = store != null ? store.size() : 0;
        int segmentCount = 1;
        if (store instanceof PersistentVectorStore) {
            segmentCount = ((PersistentVectorStore) store).getSegmentCount();
        }
        return new IndexStatusInfo(vectorCount, segmentCount,
                lastProcessedLsn != null ? lastProcessedLsn.ledgerId : -1,
                lastProcessedLsn != null ? lastProcessedLsn.offset : -1,
                "tailing");
    }

    public void setStatsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        registerTailerMetrics();
    }

    private void registerTailerMetrics() {
        StatsLogger sl = this.statsLogger;
        if (sl == null) {
            return;
        }
        StatsLogger tailerStats = sl.scope("tailer");

        tailerStats.registerGauge("watermark_ledger_id", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return -1L;
            }
            @Override
            public Long getSample() {
                LogSequenceNumber lsn = lastProcessedLsn;
                return lsn != null ? lsn.ledgerId : -1L;
            }
        });
        tailerStats.registerGauge("watermark_offset", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return -1L;
            }
            @Override
            public Long getSample() {
                LogSequenceNumber lsn = lastProcessedLsn;
                return lsn != null ? lsn.offset : -1L;
            }
        });
        tailerStats.registerGauge("entries_processed", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                CommitLogTailing t = tailer;
                return t != null ? t.getEntriesProcessed() : 0L;
            }
        });
        tailerStats.registerGauge("running", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                CommitLogTailing t = tailer;
                return t != null && t.isRunning() ? 1 : 0;
            }
        });

        StatsLogger applyStats = sl.scope("apply");
        applyStats.registerGauge("queue_size", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                int total = 0;
                for (ExecutorService w : applyWorkers) {
                    total += ((ThreadPoolExecutor) w).getQueue().size();
                }
                return total;
            }
        });
        applyStats.registerGauge("queue_capacity", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                if (applyWorkers.length == 0) {
                    return 0;
                }
                BlockingQueue<?> q = ((ThreadPoolExecutor) applyWorkers[0]).getQueue();
                return q.size() + q.remainingCapacity();
            }
        });
    }

    /**
     * Registers per-index gauges for a vector index managed by this engine.
     * Called when a vector store is created for a vector index.
     */
    void registerIndexMetrics(String tablespace, String table, String indexName, AbstractVectorStore store) {
        StatsLogger sl = this.statsLogger;
        if (sl == null) {
            return;
        }
        StatsLogger indexStats = sl
                .scope("tablespace_" + tablespace)
                .scope("table_" + table)
                .scope("vidx_" + indexName);

        indexStats.registerGauge("node_count", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return store.size();
            }
        });
        indexStats.registerGauge("estimated_size_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return store.estimatedMemoryUsageBytes();
            }
        });

        if (store instanceof PersistentVectorStore) {
            PersistentVectorStore pvs = (PersistentVectorStore) store;

            indexStats.registerGauge("live_node_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getLiveNodeCount();
                }
            });
            indexStats.registerGauge("ondisk_node_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getOnDiskNodeCount();
                }
            });
            indexStats.registerGauge("segment_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getSegmentCount();
                }
            });
            indexStats.registerGauge("dimension", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getDimension();
                }
            });
            indexStats.registerGauge("live_vectors_memory_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLiveVectorsMemoryBytes();
                }
            });
            indexStats.registerGauge("live_shard_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getLiveShardCount();
                }
            });
            indexStats.registerGauge("dirty", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.isDirty() ? 1 : 0;
                }
            });
            indexStats.registerGauge("checkpoint_active", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.isCheckpointActive() ? 1 : 0;
                }
            });
            indexStats.registerGauge("checkpoint_count", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalCheckpointCount();
                }
            });
            indexStats.registerGauge("checkpoint_fusedpq_count", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalFusedPQCheckpointCount();
                }
            });
            indexStats.registerGauge("checkpoint_simple_count", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalSimpleCheckpointCount();
                }
            });
            indexStats.registerGauge("checkpoint_duration_ms", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLastCheckpointDurationMs();
                }
            });
            indexStats.registerGauge("checkpoint_phase_b_duration_ms", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLastCheckpointPhaseBDurationMs();
                }
            });
            indexStats.registerGauge("checkpoint_vectors_processed", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLastCheckpointVectorsProcessed();
                }
            });
            indexStats.registerGauge("backpressure_active", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.isBackpressureActive() ? 1 : 0;
                }
            });
            indexStats.registerGauge("backpressure_count", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalBackpressureCount();
                }
            });
            indexStats.registerGauge("backpressure_time_ms", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalBackpressureTimeMs();
                }
            });
            indexStats.registerGauge("max_vector_memory_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    long v = pvs.getMaxVectorMemoryBytes();
                    return v == Long.MAX_VALUE ? 0L : v;
                }
            });
            indexStats.registerGauge("frozen_shard_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getFrozenShardCount();
                }
            });
            indexStats.registerGauge("live_vector_cap_during_checkpoint", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getLiveVectorCapDuringCheckpoint();
                }
            });
        }
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

        // Drain and shut down apply workers
        if (applyWorkers != null) {
            try {
                awaitPendingWork();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error draining apply workers during shutdown", e);
            }
            for (ExecutorService worker : applyWorkers) {
                worker.shutdown();
            }
            for (ExecutorService worker : applyWorkers) {
                try {
                    if (!worker.awaitTermination(5, TimeUnit.SECONDS)) {
                        worker.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    worker.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            applyWorkers = null;
            LOGGER.info("DML apply workers shut down");
        }

        // Save final watermark
        if (lastProcessedLsn != null && watermarkStore != null) {
            saveWatermark();
        }

        // Close and clear all vector stores
        for (AbstractVectorStore store : vectorStores.values()) {
            try {
                store.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing vector store", e);
            }
        }
        vectorStores.clear();

        // Close the data storage manager if configured
        if (dataStorageManager != null) {
            try {
                dataStorageManager.close();
                LOGGER.info("DataStorageManager closed");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing DataStorageManager", e);
            }
        }

        // Close the metadata storage manager if we own it
        if (ownsMetadataStorageManager && metadataStorageManager != null) {
            try {
                metadataStorageManager.close();
                LOGGER.info("MetadataStorageManager closed");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing MetadataStorageManager", e);
            }
        }

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
