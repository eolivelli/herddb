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
import herddb.index.vector.ReadOnlyVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.index.vector.VectorMemoryBudget;
import herddb.log.CommitLogTailing;
import herddb.log.IndexingServiceRebalanceDescriptor;
import herddb.log.LogEntry;
import herddb.log.LogEntryType;
import herddb.log.LogSequenceNumber;
import herddb.metadata.IndexingServiceCheckpointState;
import herddb.metadata.MetadataStorageManager;
import herddb.metadata.MetadataStorageManagerException;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.TableSpace;
import herddb.remote.RemoteFileDataStorageManager;
import herddb.remote.SegmentBlockCache;
import herddb.server.ServerConfiguration;
import herddb.storage.DataStorageManager;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.XXHash64Utils;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
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

    /**
     * Index property key used to persist the storage-level UUID of the vector
     * store inside the {@link WatermarkSnapshot} schema. The UUID is obtained
     * via {@link AbstractVectorStore#getStoreUUID()} and is non-null only for
     * stores that persist data across restarts (e.g. {@link PersistentVectorStore}).
     *
     * <p>On restart, the engine reads this UUID and passes it to the vector store
     * factory so the new store can locate the same S3 / local checkpoint path,
     * enabling recovery without full DML log replay (issue #368).
     *
     * <p>The leading underscore marks this as an internal IS property; it is never
     * set by the user and must not conflict with user-visible {@code VectorIndexManager}
     * property keys.
     */
    static final String PROP_IS_STORE_UUID = "_is.store.uuid";

    /**
     * Minimum number of entries processed between tailer-driven checkpoint
     * attempts. Each trigger drains pending DML, calls {@code checkpoint()}
     * on every persistent vector store and — only if all checkpoints complete
     * successfully — writes the watermark.
     *
     * <p>This is a <em>backstop</em>: the primary checkpoint driver is the
     * per-store background compaction loop
     * ({@code indexing.compaction.interval}, default 60 s). A low interval
     * here caused the FusedPQ Phase B on the tailer thread to starve BK
     * tailing during catch-up (issue #90), so the default is intentionally
     * large. Configurable via
     * {@link IndexingServerConfiguration#PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES}.
     */
    private long watermarkCheckpointIntervalEntries;

    private final Path logDirectory;
    private final Path dataDirectory;
    private final IndexingServerConfiguration config;

    private final int instanceId;
    /**
     * Bootstrap value of {@link #currentNumInstances} read from the JVM
     * property {@code indexing.cluster.numInstances} at engine construction.
     * Routing decisions read {@link #currentNumInstances}, which a REBALANCE
     * log entry can update at runtime.
     */
    private final int bootstrapNumInstances;
    /**
     * Effective number of indexing-service primary instances used for routing
     * decisions on every INSERT/UPDATE/DELETE applied by this engine. Mutable
     * because a {@code INDEXING_SERVICE_REBALANCE} log entry updates it on
     * the fly: existing data on the old owners stays where it is, but every
     * subsequent write routes by the new value, so a freshly-added pod
     * starts owning a share of NEW writes against EVERY existing vector
     * index without any data movement.
     */
    private volatile int currentNumInstances;

    private WatermarkStore watermarkStore;
    private SchemaTracker schemaTracker;
    private TransactionBuffer transactionBuffer;
    private CommitLogTailing tailer;
    private Thread tailerThread;

    private volatile LogSequenceNumber lastProcessedLsn;
    /**
     * The LSN of the most recent checkpoint whose watermark has been
     * successfully persisted via {@link WatermarkStore#save(WatermarkSnapshot)}.
     * After a restart, the engine resumes from this LSN — so the server-side
     * commit-log retention floor MUST pin against this value (and not the
     * in-memory {@link #lastProcessedLsn}) to avoid dropping ledgers the IS
     * would still need on recovery (issue #364).
     *
     * <p>Initialized at {@link #start()} from the loaded
     * {@link WatermarkSnapshot#lsn}. Advanced strictly inside
     * {@link #checkpointAndSaveWatermark()}, immediately after a successful
     * {@code watermarkStore.save(...)} call. Never moves backwards.
     */
    private volatile LogSequenceNumber lastDurableLsn = LogSequenceNumber.START_OF_TIME;
    private long entriesSinceLastCheckpoint;

    /**
     * Most recent {@link IndexingServiceRebalanceDescriptor} observed by the
     * tailer. Drives two effects: (a) updates {@link #currentNumInstances}
     * for routing decisions on every subsequent INSERT/UPDATE/DELETE, so a
     * scale-up immediately spreads new writes against EVERY existing index
     * across the new owner set without moving any historical data; and
     * (b) supplies the schema snapshot used by the JOINING-fallback boot
     * path when the BookKeeper history has been trimmed.
     *
     * <p>Lower-or-equal epochs are treated as no-ops (idempotent on log
     * replay).
     */
    private volatile IndexingServiceRebalanceDescriptor lastObservedRebalance;
    private final java.util.concurrent.atomic.AtomicLong observedRebalanceEpoch =
            new java.util.concurrent.atomic.AtomicLong(Long.MIN_VALUE);

    /**
     * Lifecycle state of an indexing-service primary.
     *
     * <ul>
     *   <li>{@link #ACTIVE} (default): the engine has loaded its schema —
     *       either by hydrating from local/remote storage or by bootstrapping
     *       from a {@code REBALANCE} entry — and is processing every
     *       commit-log entry normally.</li>
     *   <li>{@link #JOINING}: the engine has no schema yet and drops every
     *       commit-log entry except {@code INDEXING_SERVICE_REBALANCE}. The
     *       first REBALANCE installs the schema and transitions the engine
     *       to {@link #ACTIVE}.</li>
     * </ul>
     */
    public enum EngineStatus { ACTIVE, JOINING }

    private volatile EngineStatus engineStatus = EngineStatus.ACTIVE;

    // Shadow-replica state (only meaningful when config.isShadow() == true).
    /** true once this shadow has completed its first successful reload. */
    private volatile boolean shadowReady;
    /** Primary's latest advertised LSN read from ZK, or null if never observed. */
    private volatile LogSequenceNumber primaryAdvertisedLsn;
    /** Loaded LSN of the shadow's current on-disk view. */
    private volatile LogSequenceNumber shadowLoadedLsn;
    /** Wall-clock of the most recent successful reload. */
    private volatile long shadowLastReloadTimestampMs;
    /** Count of successful reloads (including the initial one). */
    private final java.util.concurrent.atomic.AtomicLong shadowReloadCount =
            new java.util.concurrent.atomic.AtomicLong(0);
    /** Single-thread executor that serialises reloads (ZK watcher hands off). */
    private ExecutorService shadowReloadExecutor;

    private volatile StatsLogger statsLogger;

    private MetadataStorageManager metadataStorageManager;
    private boolean ownsMetadataStorageManager;

    private MemoryManager memoryManager;
    private DataStorageManager dataStorageManager;
    private long maxVectorMemoryBytes = Long.MAX_VALUE;
    private volatile String tableSpaceUUID;

    /**
     * Shared multipart-block cache used by {@link RemoteRandomAccessReader} on
     * the vector-search hot path. Created in {@link #start()} from
     * {@link IndexingServerConfiguration#PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_MAX_BYTES}
     * and installed on the {@link RemoteFileDataStorageManager} when the DSM
     * is a remote one. Replaces the page-keyed {@code SharedSegmentPageCache}
     * that was removed when page-based persistence was dropped.
     */
    private volatile SegmentBlockCache segmentBlockCache;

    /**
     * Bytes to read sequentially from the start of each segment's graph file
     * after Phase C, before saving the watermark (issue #322).
     * Read from {@link IndexingServerConfiguration#PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}
     * at {@link #start()}, with the JVM system property
     * {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES}
     * as the fallback default when the properties file key is absent.
     * A value of {@code 0} disables warmup.
     */
    private long warmupBytesPerSegment;

    private ExecutorService[] applyWorkers;
    private int applyParallelism;
    private volatile Throwable asyncError;

    /**
     * Single-thread executor that runs {@link #checkpointAndSaveWatermark()}
     * off the tailer thread (issue #213). The tailer must never block on a
     * Phase B Future.get(); instead it hands the checkpoint work to this
     * executor via {@link #triggerCheckpointAsync()} and keeps dispatching
     * entries to the apply-worker pool.
     */
    private ExecutorService checkpointExecutor;

    /**
     * Tracks the most recently submitted tailer-driven checkpoint so that
     * {@link #triggerCheckpointAsync()} can coalesce new triggers while one
     * is still running, and {@link #forceCheckpointAndSaveWatermark()} and
     * {@link #close()} can await it.
     */
    private volatile Future<?> inflightCheckpoint;

    /**
     * Pending {@code DROP_INDEX} / {@code DROP_TABLE} cleanup tasks submitted to
     * {@link #checkpointExecutor} so they are serialised after any in-flight
     * {@link #checkpointAndSaveWatermark()}. Tracked here so that tests
     * (and {@link #close()}) can wait for the storage cleanup to finish before
     * asserting that the index data has been removed.
     *
     * <p>Submitting through {@code checkpointExecutor} avoids closing a vector
     * store while a concurrent checkpoint cycle still holds a reference to it
     * (issue #383).
     */
    private final java.util.List<Future<?>> pendingDropTasks =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /**
     * Wall-clock time at which {@link #start()} finished, used by the
     * admin CLI to compute engine uptime.
     */
    private volatile long startTimeMillis;

    /**
     * Human-readable identifier for this engine instance, populated by
     * {@link IndexingServer} once the gRPC endpoint is bound. Nullable.
     */
    private volatile String instanceIdLabel;

    /**
     * Hook invoked after the tablespace UUID has been resolved but before the
     * watermark is loaded and the tailer is started. Used by
     * {@link IndexingServer} to hydrate the local metadata cache from S3 and
     * install an {@code S3WatermarkStore} that addresses its S3 object by
     * tablespace UUID.
     */
    private java.util.function.Consumer<String> afterTableSpaceResolved;

    /**
     * In-memory vector stores keyed by "table.index".
     * Each store holds all vectors for one vector index.
     */
    private final ConcurrentHashMap<String, AbstractVectorStore> vectorStores = new ConcurrentHashMap<>();

    /**
     * Tracks the logical {@link herddb.model.Index#uuid} that was used to create
     * each vector store (keyed by the same {@link #storeKey} as {@link #vectorStores}).
     * Used in {@link #createVectorStoreIfNeeded} to distinguish a true duplicate
     * CREATE_INDEX (same UUID → skip) from a rename/recreate (different UUID → warn).
     */
    private final ConcurrentHashMap<String, String> vectorStoreIndexUuids = new ConcurrentHashMap<>();

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
        this.bootstrapNumInstances = config.getInt(IndexingServerConfiguration.PROPERTY_NUM_INSTANCES,
                IndexingServerConfiguration.PROPERTY_NUM_INSTANCES_DEFAULT);
        this.currentNumInstances = this.bootstrapNumInstances;
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
            case ServerConfiguration.PROPERTY_MODE_CLUSTER: {
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

    public IndexingServerConfiguration getConfig() {
        return config;
    }

    /**
     * Injects the {@link WatermarkStore}. Must be called before {@link #start()}.
     * If not set, a {@link LocalWatermarkStore} backed by {@code dataDirectory}
     * is used.
     */
    public void setWatermarkStore(WatermarkStore watermarkStore) {
        this.watermarkStore = watermarkStore;
    }

    /**
     * Registers a hook that runs after the tablespace UUID is resolved but
     * before the commit-log tailer is started. The tablespace UUID is passed
     * as an argument.
     */
    public void setAfterTableSpaceResolved(java.util.function.Consumer<String> hook) {
        this.afterTableSpaceResolved = hook;
    }

    /**
     * Exposes the checkpoint+watermark save path for tests and the cluster
     * E2E test that needs to force a checkpoint at a specific point in time.
     *
     * <p>Issue #213: tailer-driven checkpoints now run on a background
     * executor. To preserve the post-condition callers rely on — "after this
     * returns, a fresh checkpoint has completed and, if successful, the
     * watermark has been persisted" — we first await any in-flight async
     * checkpoint, then run a synchronous one on the caller thread.
     */
    public void forceCheckpointAndSaveWatermark() {
        Future<?> inflight = this.inflightCheckpoint;
        if (inflight != null) {
            try {
                inflight.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted awaiting in-flight checkpoint in forceCheckpointAndSaveWatermark");
                return;
            } catch (ExecutionException e) {
                // The async task already logged the failure; we still run a
                // fresh sync checkpoint below so the caller's post-condition
                // is satisfied.
                LOGGER.log(Level.FINE, "In-flight async checkpoint ended with failure", e.getCause());
            }
        }
        checkpointAndSaveWatermark();
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
        if (("file".equals(storageType) || "remote".equals(storageType))
                && dataStorageManager != null && memoryManager != null) {
            LOGGER.log(Level.INFO,
                    "Configuring PersistentVectorStore factory (storage type: {0})",
                    storageType);
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
            final long vectorCompactionIntervalMs = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_INTERVAL_MS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_INTERVAL_MS_DEFAULT);
            final long vectorCompactionMinBytes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MIN_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MIN_BYTES_DEFAULT);
            final long vectorCompactionMaxBytes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_BYTES_DEFAULT);
            final long vectorCompactionRetentionMs = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_RETENTION_MS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_RETENTION_MS_DEFAULT);
            final int vectorCompactionMaxCount = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_COUNT,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_COUNT_DEFAULT);
            final boolean vectorCompactionTieredEnabled = config.getBoolean(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED_DEFAULT);
            final int vectorCompactionBackpressureSegments = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS_DEFAULT);
            final long vectorCompactionBackpressureMaxWaitMs = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_MAX_WAIT_MS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_MAX_WAIT_MS_DEFAULT);
            LOGGER.log(Level.INFO,
                    "vector index compaction: tieredEnabled={0}, backpressureSegments={1}, "
                            + "backpressureMaxWaitMs={2}",
                    new Object[]{vectorCompactionTieredEnabled, vectorCompactionBackpressureSegments,
                            vectorCompactionBackpressureMaxWaitMs});
            final long maxLiveBytesPerCheckpoint = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_BYTES_PER_CHECKPOINT,
                    IndexingServerConfiguration.PROPERTY_VECTOR_MAX_LIVE_BYTES_PER_CHECKPOINT_DEFAULT);
            LOGGER.log(Level.INFO, "vector index maxLiveBytesPerCheckpoint: {0} bytes",
                    maxLiveBytesPerCheckpoint);
            long segmentPageCacheMaxBytes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_MAX_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_MAX_BYTES_DEFAULT);
            // A value of 0 (default) means "auto-size": budget the cache at 1/4 of
            // Netty's max direct memory because the cache stores pooled direct
            // ByteBufs (see SegmentBlockCache). Falls back to JVM max heap when
            // PlatformDependent.maxDirectMemory() is unavailable (returns -1).
            if (segmentPageCacheMaxBytes == 0) {
                long maxDirect = PlatformDependent.maxDirectMemory();
                long source = maxDirect > 0 ? maxDirect : Runtime.getRuntime().maxMemory();
                String sourceLabel = maxDirect > 0 ? "Netty maxDirectMemory" : "JVM maxMemory (fallback)";
                segmentPageCacheMaxBytes = source / 4;
                LOGGER.log(Level.INFO,
                        "vector index segmentPageCacheMaxBytes auto-sized to {0} bytes "
                                + "({1} MB) = 1/4 of {2} ({3} bytes)",
                        new Object[]{
                                segmentPageCacheMaxBytes,
                                segmentPageCacheMaxBytes / (1024 * 1024),
                                sourceLabel,
                                source
                        });
            }
            this.segmentBlockCache = segmentPageCacheMaxBytes > 0
                    ? new SegmentBlockCache(segmentPageCacheMaxBytes)
                    : SegmentBlockCache.disabled();
            LOGGER.log(Level.INFO,
                    "vector index segmentPageCacheMaxBytes: {0} (active={1})",
                    new Object[]{segmentPageCacheMaxBytes, segmentBlockCache.isActive()});
            // Install the cache + stats logger on the remote DSM so that every
            // multipartIndexReaderSupplier it builds routes reads through it.
            // Stats logger may still be null at this point (set later by
            // IndexingServer.start()); fall back to NullStatsLogger so the DSM
            // setter can enforce non-null.
            if (dataStorageManager instanceof RemoteFileDataStorageManager) {
                StatsLogger readerStats = this.statsLogger != null
                        ? this.statsLogger
                        : org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE;
                ((RemoteFileDataStorageManager) dataStorageManager)
                        .setSegmentBlockCache(segmentBlockCache, readerStats);
            }
            registerSegmentBlockCacheMetrics(segmentBlockCache);
            if (dataStorageManager instanceof RemoteFileDataStorageManager && this.statsLogger != null) {
                ((RemoteFileDataStorageManager) dataStorageManager).getClient()
                        .registerMetrics(this.statsLogger.scope("remote_file_client"));
            }

            // Resolve the post-Phase-C cache warmup byte budget (issue #322).
            // Priority: properties-file key > JVM system property > hard-coded default.
            long syspropWarmupBytes = Long.getLong(
                    IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES_DEFAULT);
            this.warmupBytesPerSegment = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                    syspropWarmupBytes);
            LOGGER.log(Level.INFO,
                    "vector index segmentCacheWarmupBytes: {0} ({1})",
                    new Object[]{warmupBytesPerSegment,
                            warmupBytesPerSegment > 0 ? "enabled" : "disabled"});

            final long vectorMemLimit = maxVectorMemoryBytes;
            final VectorMemoryBudget budget = this;
            final long finalSegmentPageCacheMaxBytes = segmentPageCacheMaxBytes;
            int configuredSearchParallelism = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEARCH_PARALLELISM,
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEARCH_PARALLELISM_DEFAULT);
            final int resolvedSearchParallelism = configuredSearchParallelism > 0
                    ? configuredSearchParallelism
                    : Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
            LOGGER.log(Level.INFO, "vector search parallelism: {0}", resolvedSearchParallelism);
            vectorStoreFactory = (indexName, tableName, vectorColumnName, dataDir, indexProperties) -> {
                var similarityFunction = PersistentVectorStore.parseSimilarityFunction(
                        indexProperties != null ? indexProperties.get(VectorIndexManager.PROP_SIMILARITY) : null);
                // If the watermark snapshot embedded a store UUID (issue #368), reuse it so
                // that PersistentVectorStore.start() can locate the existing S3 checkpoint
                // via getIndexStatus() and avoid a full DML replay on restart.
                String savedUUID = indexProperties != null ? indexProperties.get(PROP_IS_STORE_UUID) : null;
                String autoIndexUUID = (savedUUID != null && !savedUUID.isEmpty())
                        ? savedUUID
                        : indexName + "_" + tableName + "_" + System.nanoTime();
                PersistentVectorStore store = new PersistentVectorStore(
                        indexName, tableName, tableSpaceUUID, vectorColumnName,
                        autoIndexUUID, tmpDir, dsm, mm,
                        m, beamWidth, neighborOverflow, alpha,
                        fusedPQ, maxSegmentSize, maxLiveGraphSize,
                        compactionInterval,
                        similarityFunction, vectorMemLimit, budget, maxLiveBytesPerCheckpoint,
                        finalSegmentPageCacheMaxBytes, resolvedSearchParallelism);
                store.configureCompaction(
                        vectorCompactionIntervalMs,
                        vectorCompactionMinBytes,
                        vectorCompactionMaxBytes,
                        /*minCount*/ 4,
                        vectorCompactionMaxCount,
                        vectorCompactionRetentionMs);
                store.setTieredCompactionEnabled(vectorCompactionTieredEnabled);
                store.setCompactionBackpressureThreshold(vectorCompactionBackpressureSegments);
                store.setCompactionBackpressureMaxWaitMs(vectorCompactionBackpressureMaxWaitMs);
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

        // Initialize components (watermark store is loaded later, after the
        // tablespace UUID is resolved, because a remote-backed watermark store
        // addresses its S3 object by tablespace UUID).
        entriesSinceLastCheckpoint = 0;
        watermarkCheckpointIntervalEntries = Math.max(1L, config.getLong(
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES,
                IndexingServerConfiguration.PROPERTY_WATERMARK_CHECKPOINT_INTERVAL_ENTRIES_DEFAULT));
        LOGGER.log(Level.INFO, "watermark checkpoint interval: {0} entries",
                watermarkCheckpointIntervalEntries);

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

        // Single-thread executor that runs tailer-driven checkpoints off the
        // tailer thread (issue #213). Must be started before the tailer so
        // triggerCheckpointAsync() can submit tasks as soon as the tailer
        // starts processing entries.
        checkpointExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "indexing-checkpoint");
            t.setDaemon(true);
            return t;
        });

        // Validate instance identity. The bootstrap numInstances is a lower
        // bound on the engine's identity range; the running value may grow
        // when a REBALANCE entry arrives, so we accept any instanceId
        // strictly less than the bootstrap value here. Pods with an
        // instanceId outside that range are intended to come up with
        // bootstrapFromRebalance=true and will be activated by the next
        // REBALANCE entry.
        if (bootstrapNumInstances < 1) {
            throw new IllegalArgumentException("numInstances must be >= 1, got " + bootstrapNumInstances);
        }
        if (instanceId < 0) {
            throw new IllegalArgumentException("instanceId must be >= 0, got " + instanceId);
        }
        if (!config.isBootstrapFromRebalance() && instanceId >= bootstrapNumInstances) {
            throw new IllegalArgumentException(
                    "instanceId must be in [0, " + (bootstrapNumInstances - 1) + "], got " + instanceId
                            + "; for a fresh joining replica set "
                            + IndexingServerConfiguration.PROPERTY_BOOTSTRAP_FROM_REBALANCE + "=true");
        }
        LOGGER.log(Level.INFO, "Instance identity: instanceId={0}, bootstrapNumInstances={1}",
                new Object[]{instanceId, bootstrapNumInstances});

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
        long tablespaceWaitTimeoutMs = config.getLong(
                IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_TIMEOUT_MS_DEFAULT);
        long deadline = System.currentTimeMillis() + tablespaceWaitTimeoutMs;
        LOGGER.log(Level.INFO, "Waiting up to {0}ms for tablespace ''{1}'' to become available...",
                new Object[]{tablespaceWaitTimeoutMs, tablespaceName});
        TableSpace tableSpace = null;
        while (true) {
            tableSpace = metadataStorageManager.describeTableSpace(tablespaceName);
            if (tableSpace != null) {
                break;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new RuntimeException("Timed out after " + tablespaceWaitTimeoutMs + "ms waiting for tablespace '"
                        + tablespaceName + "' to become available");
            }
            LOGGER.log(Level.INFO, "Tablespace ''{0}'' not yet available, retrying in {1}ms...",
                    new Object[]{tablespaceName, pollIntervalMs});
            Thread.sleep(pollIntervalMs);
        }
        this.tableSpaceUUID = tableSpace.uuid;
        LOGGER.log(Level.INFO, "Resolved tablespace name ''{0}'' to UUID ''{1}''",
                new Object[]{tablespaceName, tableSpaceUUID});

        // Allow external components (IndexingServer) to bootstrap state from
        // remote storage now that we know the tablespace UUID — this is where
        // the SharedCheckpointMetadataManager hydrates {remoteMetaDir} from S3
        // and where an S3WatermarkStore gets installed.
        if (afterTableSpaceResolved != null) {
            try {
                afterTableSpaceResolved.accept(tableSpaceUUID);
            } catch (Exception e) {
                throw new IOException("tablespace-resolved hook failed", e);
            }
        }

        // Shadow replicas skip the entire commit-log tailer + watermark path.
        // They discover their indexes by reading the primary's definitions
        // from the shared storage and serve queries from the on-disk segments
        // via ReadOnlyVectorStore. Segment freshness is driven by
        // reload-on-ZK-notify against the primary's advertised LSN (step 5).
        if (config.isShadow()) {
            startAsShadow();
            return;
        }

        // Load the watermark snapshot now that the watermark store has been
        // configured for this tablespace UUID. The snapshot bundles the
        // last-applied LSN AND the engine's effective numInstances at the
        // time of the matching checkpoint — so a freshly-restarted engine
        // re-acquires the correct routing value even if the BookKeeper
        // ledger that carried the most recent INDEXING_SERVICE_REBALANCE
        // entry has been trimmed in the meantime.
        if (watermarkStore == null) {
            watermarkStore = new LocalWatermarkStore(dataDirectory);
        }
        // A corrupt or unreadable watermark is fatal: silently falling back to
        // START_OF_TIME would mask corruption and could trigger full BK log replay
        // against ledgers that have already been trimmed (issue #368).  The stores
        // return WatermarkSnapshot.START_OF_TIME themselves when the file/object is
        // simply absent, so an IOException here always means something is wrong.
        WatermarkSnapshot snapshot = watermarkStore.load();
        LogSequenceNumber watermark = snapshot.lsn;
        lastProcessedLsn = watermark;
        // The loaded watermark IS the durable recovery LSN: by construction
        // checkpointAndSaveWatermark only ever publishes an LSN once every
        // store has finished its checkpoint Phase C, so resuming from this
        // value is safe even if the JVM was killed mid-checkpoint after the
        // save. See lastDurableLsn JavaDoc (issue #364).
        lastDurableLsn = watermark;
        if (snapshot.numInstances > 0) {
            int previous = currentNumInstances;
            currentNumInstances = snapshot.numInstances;
            LOGGER.log(Level.INFO,
                    "Loaded watermark snapshot: {0}; currentNumInstances {1} -> {2}",
                    new Object[]{snapshot, previous, currentNumInstances});
        } else {
            LOGGER.log(Level.INFO,
                    "Loaded watermark snapshot: {0}; keeping bootstrap currentNumInstances={1}",
                    new Object[]{snapshot, currentNumInstances});
        }

        // JOINING fallback: a freshly added pod whose history was trimmed
        // (or which has no local state at all) bootstraps schema from the
        // next REBALANCE entry rather than replaying CREATE_TABLE /
        // CREATE_INDEX from START_OF_TIME. Until that REBALANCE arrives,
        // every other entry is dropped.
        if (config.isBootstrapFromRebalance()) {
            engineStatus = EngineStatus.JOINING;
            LOGGER.log(Level.INFO,
                    "Engine starting in JOINING state ({0}=true); will wait for next REBALANCE",
                    IndexingServerConfiguration.PROPERTY_BOOTSTRAP_FROM_REBALANCE);
        }

        // Determine the tailer start position.
        //
        // When the watermark snapshot carries a schema (issue #368), the engine
        // hydrates its SchemaTracker and vector stores from the snapshot BEFORE
        // the tailer starts:
        //
        //   • SchemaTracker is pre-populated so DML entries can be routed to
        //     the correct vector store even when the early BookKeeper ledgers
        //     that carried the original CREATE_TABLE / CREATE_INDEX DDL entries
        //     have been trimmed by the server's retention policy.
        //
        //   • Each vector store is recreated with the UUID that was embedded in
        //     the snapshot (PROP_IS_STORE_UUID), so PersistentVectorStore.start()
        //     finds the matching S3 checkpoint via getIndexStatus() and loads the
        //     durably-persisted segments.  The tailer then starts from the
        //     watermark LSN and replays only the NEW entries that arrived after
        //     that checkpoint — avoiding a potentially enormous replay of already-
        //     persisted DML.
        //
        //   • If the log also contains the CREATE_INDEX entry for an index whose
        //     store was already created from the snapshot, createVectorStoreIfNeeded
        //     detects the duplicate and skips re-creation (preventing a resource leak).
        //
        // When no schema is available (fresh pod, START_OF_TIME watermark), the
        // tailer starts from the beginning of the log to rebuild everything.
        LogSequenceNumber tailerStart;
        if (!config.isBootstrapFromRebalance() && snapshot.hasSchema()) {
            installSchemaFromSnapshot(snapshot);
            tailerStart = watermark;
            LOGGER.log(Level.INFO,
                    "Schema recovered from watermark snapshot ({0} tables, {1} vector indexes); "
                            + "tailer will start from watermark {2}",
                    new Object[]{snapshot.tables.size(), snapshot.vectorIndexes.size(), watermark});
        } else {
            tailerStart = LogSequenceNumber.START_OF_TIME;
            LOGGER.log(Level.INFO,
                    "No schema in watermark snapshot; tailer will start from START_OF_TIME "
                            + "to replay DDL and DML entries");
        }

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
            // Pass bookkeeper.*/bookie.* settings through to the tailer's BK
            // client; the tailer filters on those prefixes (issue #180).
            java.util.Properties bkClientProps = config.asProperties();
            tailer = new BookKeeperCommitLogTailer(zkAddress, zkSessionTimeout, zkPath,
                    bkLedgersPath, tableSpaceUUID, tailerStart, this::processEntry, bkClientProps);
        } else {
            tailer = new FileCommitLogTailer(logDirectory, tableSpaceUUID, tailerStart, this::processEntry);
        }
        tailerThread = new Thread(tailer, "indexing-service-tailer");
        tailerThread.setDaemon(true);
        tailerThread.start();

        this.startTimeMillis = System.currentTimeMillis();

        // Primaries advertise their initial state so shadows booting before
        // the next checkpoint can still see a valid state znode.
        publishInitialCheckpointState();

        LOGGER.log(Level.INFO,
                "IndexingServiceEngine started, watermark={0}, tailerStart={1}",
                new Object[]{watermark, tailerStart});
    }

    /**
     * Injects the human-readable identifier for this engine instance (usually
     * {@code host:port} once gRPC has bound to a port). Used by the admin CLI.
     */
    public void setInstanceIdLabel(String instanceIdLabel) {
        this.instanceIdLabel = instanceIdLabel;
    }

    public String getInstanceIdLabel() {
        return instanceIdLabel;
    }

    public String getTableSpaceUUID() {
        return tableSpaceUUID;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public int getApplyParallelism() {
        return applyParallelism;
    }

    public int getLoadedIndexCount() {
        return vectorStores.size();
    }

    public int getApplyQueueSize() {
        ExecutorService[] workers = this.applyWorkers;
        if (workers == null) {
            return 0;
        }
        int total = 0;
        for (ExecutorService w : workers) {
            if (w instanceof ThreadPoolExecutor) {
                total += ((ThreadPoolExecutor) w).getQueue().size();
            }
        }
        return total;
    }

    public int getApplyQueueCapacity() {
        ExecutorService[] workers = this.applyWorkers;
        if (workers == null || workers.length == 0 || !(workers[0] instanceof ThreadPoolExecutor)) {
            return 0;
        }
        BlockingQueue<?> q = ((ThreadPoolExecutor) workers[0]).getQueue();
        int perStripeCapacity = q.size() + q.remainingCapacity();
        return perStripeCapacity * workers.length;
    }

    public long getTailerEntriesProcessed() {
        CommitLogTailing t = tailer;
        return t != null ? t.getEntriesProcessed() : 0L;
    }

    public boolean isTailerRunning() {
        CommitLogTailing t = tailer;
        return t != null && t.isRunning();
    }

    public LogSequenceNumber getLastProcessedLsn() {
        return lastProcessedLsn;
    }

    /**
     * Returns the LSN of the most recent checkpoint whose watermark has been
     * successfully persisted to remote storage. After a restart, the engine
     * resumes from this value. Used by {@code GetIndexStatus} so the server
     * can pin commit-log retention against the IS's recovery floor (issue
     * #364), not its volatile in-memory tailer position.
     */
    public LogSequenceNumber getLastDurableLsn() {
        return lastDurableLsn;
    }

    /**
     * Test-only: drive an entry through the same path the live tailer uses,
     * including the transaction buffer (so BEGINTRANSACTION /
     * COMMITTRANSACTION / ROLLBACKTRANSACTION work end-to-end without
     * having to spin up a real commit log).
     */
    // package-private for testing
    void processEntryForTest(LogSequenceNumber lsn, LogEntry entry) {
        processEntry(lsn, entry);
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
            entriesSinceLastCheckpoint++;

            // Periodically force a checkpoint and then persist the watermark.
            // The checkpoint runs on a dedicated thread (issue #213) so the
            // tailer never blocks on Phase B Future.get() and the apply-worker
            // pool keeps receiving entries throughout the checkpoint window.
            // The watermark is saved ONLY after all stores successfully
            // checkpoint — never at any other time. See
            // {@link WatermarkStore} for the save contract.
            if (entriesSinceLastCheckpoint >= watermarkCheckpointIntervalEntries) {
                triggerCheckpointAsync();
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

    /**
     * Called from the tailer thread when the watermark-checkpoint-interval
     * threshold is reached. Hands the checkpoint off to
     * {@link #checkpointExecutor} so the tailer returns immediately and keeps
     * dispatching entries to the apply-worker pool while Phase B runs.
     *
     * <p>If a previous tailer-driven checkpoint is still running, the trigger
     * is coalesced: {@code entriesSinceLastCheckpoint} is NOT reset, so the
     * next call to {@code processEntry} will try again and fire as soon as
     * the in-flight checkpoint completes. The {@code checkpointLock.tryLock}
     * inside {@code PersistentVectorStore.doCheckpoint} already makes
     * overlapping cycles a safe no-op, so at-most-one submission is purely a
     * throughput optimisation.
     */
    private void triggerCheckpointAsync() {
        Future<?> inflight = this.inflightCheckpoint;
        if (inflight != null && !inflight.isDone()) {
            LOGGER.log(Level.FINE,
                    "Checkpoint trigger coalesced: previous tailer-driven checkpoint still running");
            return;
        }
        if (checkpointExecutor == null || checkpointExecutor.isShutdown()) {
            // Engine is closing or not fully started; skip silently.
            return;
        }
        entriesSinceLastCheckpoint = 0;
        try {
            this.inflightCheckpoint = checkpointExecutor.submit(this::checkpointAndSaveWatermark);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Submitted during shutdown race — acceptable; the next tailer
            // trigger (if the engine survives) will retry.
            LOGGER.log(Level.FINE, "Checkpoint trigger rejected (executor shutting down)");
        }
    }

    private void checkAsyncError() {
        Throwable err = asyncError;
        if (err != null) {
            asyncError = null;
            throw new RuntimeException("Async DML apply failed", err);
        }
    }

    /**
     * Submits the post-DROP cleanup of a vector store ({@code store.close()}
     * + {@link DataStorageManager#dropIndex} for the store's storage UUID)
     * to the {@link #checkpointExecutor} so it serialises after any in-flight
     * checkpoint cycle. Closing a store synchronously from the tailer thread
     * could race with a concurrent {@link #checkpointAndSaveWatermark()} that
     * still holds a reference to it (issue #383).
     *
     * <p>Best-effort: a failure to drop the on-storage data is logged at
     * WARNING but never propagates — re-running DROP on a future restart is
     * idempotent because the engine no longer tracks the store.
     *
     * <p>Skipped silently when the executor is null (engine never reached
     * {@link #start()}, e.g. tests that only build but don't start an engine)
     * or already shut down (engine is closing); in both cases the close +
     * dropIndex run synchronously on the calling thread so the data is still
     * released.
     */
    private void submitVectorStoreDeletion(String storeKeyForLog, AbstractVectorStore store) {
        if (store == null) {
            return;
        }
        Runnable task = () -> {
            String storeUUID = null;
            try {
                storeUUID = store.getStoreUUID();
            } catch (RuntimeException e) {
                // Reading the UUID is a trivial accessor; if it throws there
                // is nothing useful we can do beyond logging — proceed to
                // close and skip the storage drop.
                LOGGER.log(Level.WARNING,
                        "DROP cleanup for " + storeKeyForLog
                                + ": failed to read store UUID, skipping storage cleanup",
                        e);
            }
            try {
                store.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "DROP cleanup for " + storeKeyForLog
                                + ": failed to close vector store; resources may leak",
                        e);
            }
            if (storeUUID != null && dataStorageManager != null && tableSpaceUUID != null) {
                try {
                    dataStorageManager.dropIndex(tableSpaceUUID, storeUUID);
                    LOGGER.log(Level.INFO,
                            "DROP cleanup for {0}: removed on-storage data for store UUID {1}",
                            new Object[]{storeKeyForLog, storeUUID});
                } catch (herddb.storage.DataStorageManagerException e) {
                    // Non-fatal: leaves orphan data behind but the engine no
                    // longer references it. A future call to dropIndex against
                    // the same UUID is idempotent so a retry on next restart
                    // can clean up any residual state.
                    LOGGER.log(Level.WARNING,
                            "DROP cleanup for " + storeKeyForLog
                                    + ": failed to drop on-storage data for UUID " + storeUUID,
                            e);
                }
            }
        };

        ExecutorService exec = checkpointExecutor;
        if (exec == null || exec.isShutdown()) {
            // Engine never started its checkpoint executor (test path) or
            // is closing — run inline so the data is still released.
            task.run();
            return;
        }
        try {
            Future<?> f = exec.submit(task);
            pendingDropTasks.add(f);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Executor shut down between the isShutdown() probe and submit()
            // — fall back to inline execution.
            task.run();
        }
    }

    /**
     * Test hook: blocks until every {@code DROP_INDEX} / {@code DROP_TABLE}
     * cleanup task submitted via {@link #submitVectorStoreDeletion} has
     * completed. Used by tests that assert on the on-storage state right
     * after applying a DROP entry.
     *
     * <p>Public (rather than package-private) because the full-cluster
     * integration tests live in {@code herddb-services} and need to wait
     * for DROP cleanup to settle before asserting on the file-server
     * disk layout.
     */
    public void awaitPendingDeletionsForTest() {
        java.util.List<Future<?>> snapshot;
        synchronized (pendingDropTasks) {
            snapshot = new ArrayList<>(pendingDropTasks);
        }
        for (Future<?> f : snapshot) {
            try {
                f.get(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted awaiting DROP cleanup", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("DROP cleanup task failed", e.getCause());
            } catch (java.util.concurrent.TimeoutException e) {
                throw new RuntimeException("Timed out awaiting DROP cleanup", e);
            }
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
     * Invokes {@link #triggerCheckpointAsync()} (used by tests that want to
     * exercise the async checkpoint path without driving the full
     * {@code processEntry} code path).
     */
    // package-private for testing
    void triggerCheckpointAsyncForTest() {
        triggerCheckpointAsync();
    }

    /**
     * Returns the currently tracked in-flight tailer-driven checkpoint
     * {@link Future}, or {@code null} if none is tracked (used by tests to
     * verify coalescing and shutdown semantics).
     */
    // package-private for testing
    Future<?> getInflightCheckpointFutureForTest() {
        return inflightCheckpoint;
    }

    /**
     * Sets the "last processed LSN" that
     * {@link #checkpointAndSaveWatermark()} will capture and persist. Used
     * by tests that drive the engine via
     * {@link #applySingleEntryForTest(LogSequenceNumber, herddb.log.LogEntry)}
     * instead of the real tailer, because that path bypasses
     * {@code processEntry()} where {@code lastProcessedLsn} would normally
     * advance.
     */
    // package-private for testing
    void setLastProcessedLsnForTest(LogSequenceNumber lsn) {
        this.lastProcessedLsn = lsn;
    }

    /**
     * Applies a single (committed or non-transactional) entry.
     */
    // package-private for testing
    void applyEntry(LogSequenceNumber lsn, LogEntry entry) {
        // JOINING state: drop everything except the REBALANCE entry that
        // bootstraps schema. See Step 7 of the scale-up plan: a freshly
        // added pod that cannot replay history (BK ledgers trimmed) sits in
        // JOINING until a REBALANCE arrives, at which point it acquires
        // schema and transitions to ACTIVE.
        if (engineStatus == EngineStatus.JOINING
                && entry.type != LogEntryType.INDEXING_SERVICE_REBALANCE) {
            return;
        }
        switch (entry.type) {
            // DDL operations: update schema tracker
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
                schemaTracker.applyEntry(entry);
                break;

            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE: {
                schemaTracker.applyEntry(entry);
                // Remove all vector stores for this table AND release their
                // remote/local persistent state.  Without the dropIndex()
                // call below, every per-segment graph + map file would
                // linger on the file server / S3 forever, causing the
                // bucket to grow without bound under a CREATE/DROP
                // workload (issue #383).
                String droppedTable = entry.tableName;
                String droppedTablePrefix = droppedTable + ".";
                java.util.List<Map.Entry<String, AbstractVectorStore>> toClose =
                        new ArrayList<>();
                vectorStores.entrySet().removeIf(e -> {
                    if (e.getKey().startsWith(droppedTablePrefix)) {
                        toClose.add(e);
                        return true;
                    }
                    return false;
                });
                // Keep vectorStoreIndexUuids in sync so a future CREATE_INDEX with the same
                // (table, name) key does not mis-fire the "different UUID" guard and silently
                // refuse to create the new store (issue #368 review).
                vectorStoreIndexUuids.entrySet().removeIf(e -> e.getKey().startsWith(droppedTablePrefix));
                for (Map.Entry<String, AbstractVectorStore> dropped : toClose) {
                    submitVectorStoreDeletion(dropped.getKey(), dropped.getValue());
                }
                break;
            }

            case LogEntryType.CREATE_INDEX:
                schemaTracker.applyEntry(entry);
                createVectorStoreIfNeeded(entry);
                break;

            case LogEntryType.DROP_INDEX: {
                String indexName = new String(entry.value.to_array(), java.nio.charset.StandardCharsets.UTF_8);
                // Remove vector store before updating schema (we need the index info)
                Index idx = schemaTracker.getIndex(indexName);
                if (idx != null && Index.TYPE_VECTOR.equals(idx.type)) {
                    String k = storeKey(idx.table, idx.name);
                    AbstractVectorStore removed = vectorStores.remove(k);
                    vectorStoreIndexUuids.remove(k);
                    if (removed != null) {
                        // Close the store and drop its on-storage data
                        // (graph/map segments + IndexStatus markers).
                        // Without this, every per-segment graph + map file
                        // would linger on the file server / S3 forever
                        // (issue #383).
                        submitVectorStoreDeletion(k, removed);
                    }
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

            case LogEntryType.INDEXING_SERVICE_REBALANCE:
                handleRebalanceEntry(entry);
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
        if (vectorStores.containsKey(key)) {
            // Store already exists — compare the logical Index UUID to distinguish
            // an idempotent replay (same UUID → benign) from a genuine schema
            // divergence (different UUID → we already have a store, log a warning
            // but skip because we cannot safely close-and-replace without knowing
            // which store holds the authoritative data).
            String existingUuid = vectorStoreIndexUuids.get(key);
            if (existingUuid != null && !existingUuid.equals(index.uuid)) {
                LOGGER.log(Level.WARNING,
                        "CREATE_INDEX for {0} carries uuid={1} but existing store was created "
                                + "for uuid={2}; skipping re-creation (snapshot store takes precedence)",
                        new Object[]{key, index.uuid, existingUuid});
            } else {
                // Same UUID (or UUID unknown): benign duplicate from snapshot replay.
                LOGGER.log(Level.FINE,
                        "Vector store for {0} already exists (uuid={1}); skipping re-creation",
                        new Object[]{key, existingUuid});
            }
            return;
        }
        // The vector column is the first (and only) column of the vector index
        String vectorColumnName = index.columnNames[0];
        AbstractVectorStore store = vectorStoreFactory.create(index.name, index.table, vectorColumnName, dataDirectory, index.properties);
        vectorStores.put(key, store);
        vectorStoreIndexUuids.put(key, index.uuid);
        registerIndexMetrics(index.tablespace, index.table, index.name, store);
        LOGGER.log(Level.INFO, "Created vector store for index {0} on column {1} with properties {2}",
                new Object[]{index.name, vectorColumnName, index.properties});
    }

    /**
     * Per-key ownership decision for a given vector index using this engine's
     * current number of instances.
     *
     * <p>{@code numShards} is a per-index property (set at CREATE INDEX time)
     * because it controls hash-bucket granularity within the index — it does
     * not move data, just decides how many distinct buckets exist.
     *
     * <p>{@code numInstances} is engine-wide and mutable: the
     * {@link #handleRebalanceEntry} path updates it on the fly when a
     * {@code INDEXING_SERVICE_REBALANCE} log entry is observed. Existing
     * data on the previous owners stays where it is, but every subsequent
     * write routes by the new value — so a freshly-added pod starts owning
     * a share of NEW writes against EVERY existing vector index, without
     * any data migration.
     */
    boolean isAcceptedLocally(Bytes key, Index index) {
        int n = currentNumInstances;
        int indexNumShards = getNumShardsForIndex(index);
        if (n <= 1 || indexNumShards <= 1) {
            return true;
        }
        long hash = XXHash64Utils.hash(key.getBuffer(), key.getOffset(), key.getLength());
        int shardId = Math.floorMod((int) hash, indexNumShards);
        return shardId % n == instanceId;
    }

    private int getNumShardsForIndex(Index index) {
        String val = index.properties.get(VectorIndexManager.PROP_NUM_SHARDS);
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Invalid {0} on index {1}: {2}",
                        new Object[]{VectorIndexManager.PROP_NUM_SHARDS, index.name, val});
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
        Table table = schemaTracker.getTable(tableName);
        if (table == null) {
            LOGGER.log(Level.WARNING, "INSERT on unknown table {0}, skipping vector indexing", tableName);
            return;
        }
        Record record = new Record(entry.key, entry.value);
        DataAccessorForFullRecord accessor = new DataAccessorForFullRecord(table, record);
        for (Index idx : vectorIndexes) {
            // Per-index ownership: indexes on the same table may have different
            // numInstances (e.g. a pre-rebalance index with N=2 next to a
            // post-rebalance index with N=4); each makes its own decision.
            if (!isAcceptedLocally(entry.key, idx)) {
                continue;
            }
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
            // Broadcast the remove: a rebalance may have left stale copies of
            // this key on previous owners that no longer own it under the
            // current numInstances; we have to clean them up wherever they
            // sit. The add side is filtered: only the current owner under
            // the new mapping installs the new vector — otherwise we'd
            // phantom-add the key onto every replica.
            store.removeVector(entry.key);
            if (!isAcceptedLocally(entry.key, idx)) {
                continue;
            }
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
        // Broadcast: every replica calls removeVector unconditionally.
        // After a rebalance, the same primary key may sit on TWO replicas at
        // once — on its original owner under the old numInstances (the
        // historical write) AND on its new owner under the new numInstances
        // (a subsequent re-INSERT or UPDATE). A filtered DELETE would only
        // hit one of them and leak the other; broadcast guarantees the key
        // disappears from everywhere. removeVector is a no-op on replicas
        // that never had the key, so the cost is a single map lookup.
        for (Index idx : vectorIndexes) {
            AbstractVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store != null) {
                store.removeVector(entry.key);
            }
        }
    }

    /**
     * Processes an {@link LogEntryType#INDEXING_SERVICE_REBALANCE} entry.
     * Updates {@link #currentNumInstances} from the descriptor so every
     * subsequent routing decision uses the new value (existing data on
     * previous owners stays put — search fans out across all replicas
     * regardless), and bumps {@link #observedRebalanceEpoch}.
     *
     * <p>Lower-or-equal epochs are silently ignored to keep log replay
     * idempotent. The JOINING fallback path (Step 7) overrides this with
     * additional bootstrap behavior.
     */
    void handleRebalanceEntry(LogEntry entry) {
        if (entry.value == null) {
            LOGGER.log(Level.WARNING, "INDEXING_SERVICE_REBALANCE entry with null payload, ignoring");
            return;
        }
        IndexingServiceRebalanceDescriptor descriptor;
        try {
            descriptor = IndexingServiceRebalanceDescriptor.deserialize(entry.value.to_array());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "INDEXING_SERVICE_REBALANCE entry with malformed payload: " + e.getMessage(), e);
            return;
        }
        long previous = observedRebalanceEpoch.get();
        boolean joining = engineStatus == EngineStatus.JOINING;
        // A JOINING engine MUST process the first REBALANCE it sees regardless
        // of epoch ordering — otherwise it has no way to acquire schema if the
        // EXECUTE rebalance happened before the pod started. ACTIVE engines
        // skip lower-or-equal epochs to keep replay idempotent.
        if (!joining && descriptor.epoch <= previous) {
            LOGGER.log(Level.FINE,
                    "Ignoring REBALANCE epoch {0} (already observed {1})",
                    new Object[]{descriptor.epoch, previous});
            return;
        }
        if (joining) {
            installSchemaFromDescriptor(descriptor);
            engineStatus = EngineStatus.ACTIVE;
            LOGGER.log(Level.INFO,
                    "JOINING -> ACTIVE on REBALANCE epoch={0}: installed {1} tables, {2} vector indexes",
                    new Object[]{descriptor.epoch,
                            descriptor.tables.size(), descriptor.vectorIndexes.size()});
        }
        // Update the engine's effective numInstances. This is the load-bearing
        // change of the REBALANCE entry: from this LSN onward every routing
        // decision uses the new value. Existing data on previous owners stays
        // where it is (search fans out across all replicas anyway), but new
        // writes are spread across the new owner set — including freshly
        // added pods, for EVERY existing vector index.
        int previousN = currentNumInstances;
        currentNumInstances = descriptor.defaultNumInstances;
        if (previousN != currentNumInstances) {
            LOGGER.log(Level.INFO,
                    "currentNumInstances {0} -> {1} (REBALANCE epoch={2})",
                    new Object[]{previousN, currentNumInstances, descriptor.epoch});
        }
        // Always advance the high-water mark to the OBSERVED value, even if
        // the JOINING bootstrap accepted an older epoch — subsequent REBALANCE
        // entries with strictly higher epochs will then be processed normally.
        if (descriptor.epoch > previous) {
            observedRebalanceEpoch.set(descriptor.epoch);
        }
        lastObservedRebalance = descriptor;
        LOGGER.log(Level.INFO,
                "Observed REBALANCE epoch={0} defaultNumInstances={1} tables={2} vectorIndexes={3}",
                new Object[]{descriptor.epoch, descriptor.defaultNumInstances,
                        descriptor.tables.size(), descriptor.vectorIndexes.size()});
    }

    /** Test- and diagnostics-only accessor for the engine's current effective numInstances. */
    public int getCurrentNumInstances() {
        return currentNumInstances;
    }

    /**
     * Synthesises {@code CREATE_TABLE}/{@code CREATE_INDEX} log entries from
     * the descriptor and feeds them to the {@link SchemaTracker}, then
     * creates a vector store for every vector index in the snapshot. Used
     * by the JOINING fallback to acquire schema without replaying the
     * historical commit-log entries.
     */
    private void installSchemaFromDescriptor(IndexingServiceRebalanceDescriptor descriptor) {
        for (Table t : descriptor.tables) {
            byte[] blob = t.serialize();
            schemaTracker.applyEntry(new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_TABLE, 0L, t.name, null,
                    herddb.utils.Bytes.from_array(blob)));
        }
        for (Index ix : descriptor.vectorIndexes) {
            byte[] blob = ix.serialize();
            LogEntry synth = new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_INDEX, 0L, ix.table, null,
                    herddb.utils.Bytes.from_array(blob));
            schemaTracker.applyEntry(synth);
            createVectorStoreIfNeeded(synth);
        }
    }

    /**
     * Hydrates the {@link SchemaTracker} and creates vector stores from the
     * schema bundled in a {@link WatermarkSnapshot}. Called during
     * {@link #start()} when the watermark carries a schema snapshot (issue
     * #368), allowing the tailer to start from the watermark LSN instead of
     * {@code START_OF_TIME}.
     *
     * <p>This mirrors {@link #installSchemaFromDescriptor} but sources schema
     * from the watermark rather than a REBALANCE log entry.
     */
    private void installSchemaFromSnapshot(WatermarkSnapshot snapshot) {
        for (Table t : snapshot.tables) {
            byte[] blob = t.serialize();
            schemaTracker.applyEntry(new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_TABLE, 0L, t.name, null,
                    herddb.utils.Bytes.from_array(blob)));
        }
        for (Index ix : snapshot.vectorIndexes) {
            byte[] blob = ix.serialize();
            LogEntry synth = new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_INDEX, 0L, ix.table, null,
                    herddb.utils.Bytes.from_array(blob));
            schemaTracker.applyEntry(synth);
            createVectorStoreIfNeeded(synth);
        }
    }

    /** Test- and diagnostics-only accessor for the engine lifecycle state. */
    public EngineStatus getEngineStatus() {
        return engineStatus;
    }

    /** Forces this engine into {@link EngineStatus#JOINING}; for tests only. */
    void forceJoiningForTest() {
        this.engineStatus = EngineStatus.JOINING;
    }

    /**
     * Most recently observed REBALANCE descriptor or {@code null} if none yet.
     * Test- and diagnostics-only accessor.
     */
    public IndexingServiceRebalanceDescriptor getLastObservedRebalance() {
        return lastObservedRebalance;
    }

    /**
     * Highest REBALANCE epoch observed so far, or {@link Long#MIN_VALUE} if
     * no REBALANCE entry has been processed.
     */
    public long getObservedRebalanceEpoch() {
        return observedRebalanceEpoch.get();
    }

    private static float[] extractVector(DataAccessorForFullRecord accessor, String columnName) {
        Object value = accessor.get(columnName);
        if (value instanceof float[]) {
            return (float[]) value;
        }
        return null;
    }

    /**
     * Drains pending DML work, calls {@code checkpoint()} on every persistent
     * vector store, then — if ALL checkpoints completed successfully — writes
     * the watermark. This is the only path that persists the watermark.
     *
     * <p>Ordering matters: the watermark LSN must correspond to state that
     * has been fully durably persisted (vector pages + IndexStatus on S3).
     * If any checkpoint fails we do not advance the watermark, so restart
     * will replay from the previous watermark — correct because the apply
     * path is idempotent.
     */
    private void checkpointAndSaveWatermark() {
        try {
            // Drain async DML so that lastProcessedLsn is actually applied
            // into the in-memory store state before checkpoint captures it.
            awaitPendingWork();
            LogSequenceNumber checkpointLsn = lastProcessedLsn;
            entriesSinceLastCheckpoint = 0;

            boolean allCheckpointsDurable = true;
            for (Map.Entry<String, AbstractVectorStore> entry : vectorStores.entrySet()) {
                AbstractVectorStore store = entry.getValue();
                if (store instanceof PersistentVectorStore) {
                    try {
                        boolean durable = ((PersistentVectorStore) store).checkpoint();
                        if (!durable) {
                            // Either another checkpoint is in progress
                            // (tryLock-skip) or the min-live-vectors gate
                            // deferred this cycle. In both cases the live
                            // shard may contain vectors not yet on disk, so
                            // advancing the watermark would violate the
                            // "watermark <= durable-state LSN" invariant.
                            // Retry on the next tailer trigger — with the
                            // raised watermark interval the retry cost is
                            // negligible.
                            LOGGER.log(Level.INFO,
                                    "watermark NOT advanced: checkpoint on store {0} was deferred "
                                            + "(concurrent cycle or min-live-vectors gate); "
                                            + "will retry on next trigger",
                                    entry.getKey());
                            allCheckpointsDurable = false;
                            break;
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING,
                                "checkpoint failed, watermark will NOT be advanced: " + e.getMessage(), e);
                        allCheckpointsDurable = false;
                        break;
                    }
                }
            }
            if (!allCheckpointsDurable) {
                return;
            }
            // Pre-warm the SegmentBlockCache before unblocking WAITFORINDEXES
            // (issue #322): read the entry-point neighbourhood of each segment
            // so the first query batch finds hot cache blocks rather than
            // issuing cold gRPC streaming reads against the file server.
            // Warmup is best-effort — failures are logged and never abort the
            // watermark save.  A warmupBytesPerSegment of 0 disables this.
            if (warmupBytesPerSegment > 0) {
                for (AbstractVectorStore store : vectorStores.values()) {
                    if (store instanceof PersistentVectorStore) {
                        ((PersistentVectorStore) store).warmUpBlockCache(warmupBytesPerSegment);
                    }
                }
            }
            // Only now — all stores have durably persisted state covering
            // checkpointLsn — is it safe to publish the watermark snapshot.
            // We capture the engine's current numInstances together with the
            // LSN so a future restart re-acquires the right routing value
            // even if the BookKeeper history that carried the most recent
            // INDEXING_SERVICE_REBALANCE entry has been trimmed by then.
            //
            // We also capture a schema snapshot (table and vector-index
            // definitions from the SchemaTracker). This lets a restarting
            // engine hydrate its SchemaTracker from the watermark and start
            // the tailer from the watermark LSN rather than START_OF_TIME,
            // which is critical when early DDL ledgers have been trimmed from
            // BookKeeper and the CREATE_TABLE / CREATE_INDEX entries are no
            // longer replayable (issue #368).
            List<Table> schemaTables = new ArrayList<>(schemaTracker.getAllTables());
            List<Index> schemaVectorIndexes = new ArrayList<>();
            for (Index idx : schemaTracker.getAllIndexes()) {
                if (Index.TYPE_VECTOR.equals(idx.type)) {
                    // Embed the store UUID in the index properties so a restarting
                    // engine can reconstruct the persistent vector store against the
                    // same S3 checkpoint path and avoid full DML log replay (issue #368).
                    // Uses AbstractVectorStore.getStoreUUID() so this works for any
                    // store implementation that supports UUID-based checkpoint recovery,
                    // not just PersistentVectorStore (and is also testable without it).
                    AbstractVectorStore store = vectorStores.get(storeKey(idx.table, idx.name));
                    String storeUUID = store != null ? store.getStoreUUID() : null;
                    // Always rebuild the index with a fresh UUID value: either the
                    // current store's UUID (non-null → embed it), or no UUID at all
                    // (null → drop any stale UUID that may have been loaded from a
                    // previous snapshot via installSchemaFromSnapshot).
                    boolean hasPropIsStoreUUID = idx.properties.containsKey(PROP_IS_STORE_UUID);
                    if (storeUUID != null || hasPropIsStoreUUID) {
                        // Rebuild the index, updating or removing PROP_IS_STORE_UUID.
                        Index.Builder b = Index.builder()
                                .uuid(idx.uuid)
                                .name(idx.name)
                                .table(idx.table)
                                .tablespace(idx.tablespace)
                                .type(idx.type)
                                .column(idx.columnNames[0], idx.columns[0].type);
                        if (storeUUID != null) {
                            b.property(PROP_IS_STORE_UUID, storeUUID);
                        }
                        // Preserve all existing user-visible properties (similarity, numShards, etc.).
                        for (Map.Entry<String, String> e : idx.properties.entrySet()) {
                            if (!PROP_IS_STORE_UUID.equals(e.getKey())) {
                                b.property(e.getKey(), e.getValue());
                            }
                        }
                        idx = b.build();
                    }
                    schemaVectorIndexes.add(idx);
                }
            }
            WatermarkSnapshot snapshotToSave =
                    new WatermarkSnapshot(checkpointLsn, currentNumInstances,
                            schemaTables, schemaVectorIndexes);
            try {
                watermarkStore.save(snapshotToSave);
                // Only after the watermark has been successfully persisted to
                // remote storage is the engine guaranteed to recover from
                // checkpointLsn on restart — this is the LSN the server's
                // retention floor must pin against (issue #364).
                lastDurableLsn = checkpointLsn;
                LOGGER.log(Level.FINE, "Saved watermark snapshot {0}", snapshotToSave);
            } catch (IOException e) {
                // Watermark save failed: leave lastDurableLsn unchanged.
                // lastProcessedLsn keeps advancing in the in-memory tailer,
                // but the recovery floor stays anchored at the previous
                // durable LSN — exactly the invariant the server relies on.
                LOGGER.log(Level.WARNING, "Failed to save watermark", e);
            }
            // Advertise the new durable LSN to ZK so shadow replicas can reload.
            // Primaries only; shadows never reach this code path (tailer is
            // not started for them).
            publishCheckpointStateBestEffort(checkpointLsn);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "checkpointAndSaveWatermark failed", e);
        }
    }

    /**
     * Publishes the engine's current durable LSN + aggregate segment count to
     * ZooKeeper under {@code /herddb/indexingServices/state/{instanceId}} so
     * that shadow replicas of this primary can reload their on-disk view.
     *
     * <p>Best-effort: a failure to write to ZK never fails the checkpoint — the
     * watermark has already been saved, so the primary is consistent; the
     * shadow will simply observe the next successful publish.
     */
    private void publishCheckpointStateBestEffort(LogSequenceNumber lsn) {
        if (metadataStorageManager == null || lsn == null || config.isShadow()) {
            return;
        }
        try {
            int segmentCount = 0;
            for (AbstractVectorStore store : vectorStores.values()) {
                if (store instanceof PersistentVectorStore) {
                    segmentCount += ((PersistentVectorStore) store).getSegmentCount();
                }
            }
            metadataStorageManager.publishIndexingServiceCheckpointState(
                    new IndexingServiceCheckpointState(
                            instanceId,
                            lsn.ledgerId,
                            lsn.offset,
                            segmentCount,
                            System.currentTimeMillis()));
            LOGGER.log(Level.FINE,
                    "Published indexing-service checkpoint state: instance={0}, lsn={1}, segments={2}",
                    new Object[]{instanceId, lsn, segmentCount});
        } catch (MetadataStorageManagerException e) {
            LOGGER.log(Level.WARNING,
                    "Failed to publish indexing-service checkpoint state for instance " + instanceId, e);
        }
    }

    /**
     * Publishes the initial checkpoint state on engine startup so that shadow
     * replicas booting before the first post-start checkpoint can still find
     * a valid state entry for this primary.
     */
    void publishInitialCheckpointState() {
        if (config.isShadow()) {
            return;
        }
        LogSequenceNumber lsn = lastProcessedLsn != null ? lastProcessedLsn : LogSequenceNumber.START_OF_TIME;
        publishCheckpointStateBestEffort(lsn);
    }

    // -------------------------------------------------------------------------
    // Shadow-replica boot path
    // -------------------------------------------------------------------------

    /**
     * Shadow-mode counterpart of the tailer/watermark startup path.
     *
     * <p>Discovers the primary's tables and indexes from the shared storage
     * (no commit-log replay), creates a {@link ReadOnlyVectorStore} for each
     * vector index, performs an initial reload, and installs a ZK watch on
     * the primary's advertised state. Every watcher fire enqueues a reload
     * onto {@link #shadowReloadExecutor}; reloads are serialised.
     */
    private void startAsShadow() throws IOException {
        final int shadowOf = config.getInt(IndexingServerConfiguration.PROPERTY_SHADOW_OF,
                IndexingServerConfiguration.PROPERTY_SHADOW_OF_UNSET);
        LOGGER.log(Level.INFO,
                "IndexingServiceEngine starting in shadow mode (shadowOf={0}); "
                        + "skipping commit-log tailer and watermark store",
                shadowOf);
        this.lastProcessedLsn = LogSequenceNumber.START_OF_TIME;
        this.startTimeMillis = System.currentTimeMillis();

        shadowReloadExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "indexing-shadow-reload");
            t.setDaemon(true);
            return t;
        });

        // Discover vector indexes directly from the DSM — no SchemaTracker
        // needed because shadows never process DML. Other index kinds
        // (non-vector) are ignored: the indexing service handles only vector
        // indexes. Tables do not need to be tracked either; the store key
        // (table, index) is what matters for Search RPCs.
        try {
            List<Index> indexes = dataStorageManager.loadIndexes(LogSequenceNumber.START_OF_TIME, tableSpaceUUID);
            for (Index idx : indexes) {
                if (!Index.TYPE_VECTOR.equals(idx.type)) {
                    continue;
                }
                createShadowVectorStore(idx);
            }
        } catch (Exception e) {
            throw new IOException("Shadow boot failed while discovering schema from storage", e);
        }

        // Perform an initial reload so the shadow has fresh state before
        // exposing the gRPC endpoints.
        IndexingServiceCheckpointState primaryState = null;
        try {
            if (metadataStorageManager != null) {
                primaryState = metadataStorageManager.getIndexingServiceCheckpointState(shadowOf);
            }
        } catch (MetadataStorageManagerException e) {
            LOGGER.log(Level.WARNING, "Failed to read primary's advertised checkpoint state at boot", e);
        }
        if (primaryState != null) {
            this.primaryAdvertisedLsn = primaryState.toLogSequenceNumber();
        }
        boolean initialReloadOk = doShadowReload();
        this.shadowReady = initialReloadOk;

        // Install the ZK watch — every update enqueues a reload.
        if (metadataStorageManager != null) {
            try {
                metadataStorageManager.watchIndexingServiceCheckpointState(shadowOf, state -> {
                    this.primaryAdvertisedLsn = state.toLogSequenceNumber();
                    enqueueShadowReload();
                });
            } catch (MetadataStorageManagerException e) {
                LOGGER.log(Level.WARNING,
                        "Shadow could not install watch on primary's state; reloads will not fire", e);
            }
        }
    }

    /**
     * Creates a {@link ReadOnlyVectorStore} for the given vector index and
     * puts it in {@link #vectorStores}. Uses the index's stable UUID so the
     * shadow reads the primary's on-disk segments at exactly the same storage
     * path.
     */
    private void createShadowVectorStore(Index idx) {
        final String vectorColumnName = idx.columnNames[0];
        final int m = config.getInt(IndexingServerConfiguration.PROPERTY_VECTOR_M,
                IndexingServerConfiguration.PROPERTY_VECTOR_M_DEFAULT);
        final int beamWidth = config.getInt(IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH,
                IndexingServerConfiguration.PROPERTY_VECTOR_BEAM_WIDTH_DEFAULT);
        final float neighborOverflow = (float) config.getDouble(
                IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW,
                IndexingServerConfiguration.PROPERTY_VECTOR_NEIGHBOR_OVERFLOW_DEFAULT);
        final float alpha = (float) config.getDouble(
                IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA,
                IndexingServerConfiguration.PROPERTY_VECTOR_ALPHA_DEFAULT);
        final boolean fusedPQ = config.getBoolean(IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ,
                IndexingServerConfiguration.PROPERTY_VECTOR_FUSED_PQ_DEFAULT);
        final long maxSegmentSize = config.getLong(IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE,
                IndexingServerConfiguration.PROPERTY_VECTOR_MAX_SEGMENT_SIZE_DEFAULT);
        var similarityFunction = PersistentVectorStore.parseSimilarityFunction(
                idx.properties != null ? idx.properties.get(VectorIndexManager.PROP_SIMILARITY) : null);
        ReadOnlyVectorStore store = new ReadOnlyVectorStore(
                idx.name, idx.table, tableSpaceUUID, vectorColumnName,
                idx.uuid, dataDirectory, dataStorageManager, memoryManager,
                m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, 0, similarityFunction);
        try {
            store.start();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "Shadow failed to start ReadOnlyVectorStore for index " + idx.name, e);
            try {
                store.close();
            } catch (Exception ignore) {
                // best-effort
            }
            return;
        }
        vectorStores.put(storeKey(idx.table, idx.name), store);
        registerIndexMetrics(idx.tablespace, idx.table, idx.name, store);
        LOGGER.log(Level.INFO, "Shadow created ReadOnlyVectorStore for index {0}.{1} (uuid={2})",
                new Object[]{idx.table, idx.name, idx.uuid});
    }

    /**
     * Runs a reload of every {@link ReadOnlyVectorStore} managed by this
     * engine. Returns true iff every store reloaded (or there were no stores
     * at all and we still consider the shadow ready to serve empty results).
     */
    private boolean doShadowReload() {
        boolean allOk = true;
        LogSequenceNumber maxLsn = null;
        for (AbstractVectorStore s : vectorStores.values()) {
            if (!(s instanceof ReadOnlyVectorStore)) {
                continue;
            }
            ReadOnlyVectorStore ro = (ReadOnlyVectorStore) s;
            try {
                IndexStatus status = dataStorageManager.getIndexStatus(
                        tableSpaceUUID, ro.unwrap().getIndexUuid(), LogSequenceNumber.START_OF_TIME);
                if (status != null && status.indexData != null && status.indexData.length > 0) {
                    ro.reloadFromStatus(status);
                    LogSequenceNumber l = ro.getLoadedLsn();
                    if (l != null && (maxLsn == null || l.after(maxLsn))) {
                        maxLsn = l;
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Shadow reload failed for index " + ro, e);
                allOk = false;
            }
        }
        if (maxLsn != null) {
            this.shadowLoadedLsn = maxLsn;
        }
        if (allOk) {
            this.shadowLastReloadTimestampMs = System.currentTimeMillis();
            this.shadowReloadCount.incrementAndGet();
            this.shadowReady = true;
        }
        return allOk;
    }

    private void enqueueShadowReload() {
        ExecutorService exec = shadowReloadExecutor;
        if (exec == null) {
            return;
        }
        try {
            exec.submit(this::doShadowReload);
        } catch (java.util.concurrent.RejectedExecutionException ignore) {
            // executor shut down — no more reloads needed.
        }
    }

    // -------------------------------------------------------------------------
    // Shadow accessors (used by IndexingServiceImpl's GetShadowStatus /
    // WaitForCheckpoint RPC implementations landing in step 8).
    // -------------------------------------------------------------------------

    public boolean isShadowReady() {
        return shadowReady;
    }

    /** True iff this engine was configured as a shadow replica (role=shadow). */
    public boolean isConfiguredAsShadow() {
        return config.isShadow();
    }

    /**
     * Returns the {@link IndexingServerConfiguration#PROPERTY_SHADOW_OF} this
     * engine was configured with, or {@code -1} if it is a primary (or the
     * setting is unset, which for a primary is normal).
     */
    public int getShadowOfOrMinusOne() {
        return config.getInt(IndexingServerConfiguration.PROPERTY_SHADOW_OF,
                IndexingServerConfiguration.PROPERTY_SHADOW_OF_UNSET);
    }

    public LogSequenceNumber getShadowLoadedLsn() {
        return shadowLoadedLsn;
    }

    public LogSequenceNumber getPrimaryAdvertisedLsn() {
        return primaryAdvertisedLsn;
    }

    public long getShadowLastReloadTimestampMs() {
        return shadowLastReloadTimestampMs;
    }

    public long getShadowReloadCount() {
        return shadowReloadCount.get();
    }

    /**
     * Minimum {@code IndexStatus.generation} currently loaded across
     * every vector store this engine holds. Used by the retention
     * protocol: shadow instances expose this value via
     * {@code GetShadowStatus}; primaries aggregate the min across all
     * shadows to gate physical deletion of compacted-out segment files.
     *
     * <p>Returns 0 when no vector store is loaded yet — matches the
     * startup default and prevents premature deletion.
     */
    public long getMinAppliedIndexStatusGeneration() {
        long min = Long.MAX_VALUE;
        boolean any = false;
        for (AbstractVectorStore store : vectorStores.values()) {
            if (store instanceof PersistentVectorStore) {
                long g = ((PersistentVectorStore) store).getCurrentIndexStatusGeneration();
                if (g < min) {
                    min = g;
                }
                any = true;
            }
        }
        return any ? min : 0L;
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
        // Snapshot both LSNs together so the response is internally consistent.
        // Server-side retention pins on durable_lsn_*; tailer_lsn_* is exposed
        // for diagnostics only (issue #364).
        LogSequenceNumber tailerSnap = lastProcessedLsn;
        LogSequenceNumber durableSnap = lastDurableLsn;
        return new IndexStatusInfo(vectorCount, segmentCount,
                tailerSnap != null ? tailerSnap.ledgerId : -1,
                tailerSnap != null ? tailerSnap.offset : -1,
                durableSnap != null ? durableSnap.ledgerId : -1,
                durableSnap != null ? durableSnap.offset : -1,
                "tailing");
    }

    /**
     * Test-only helper: registers a pre-built vector store under the given
     * index, so diagnostic RPCs can be exercised without replaying a commit
     * log. Visible for tests within the same package.
     */
    // package-private for testing
    void registerIndexForTest(Index index, AbstractVectorStore store) {
        if (schemaTracker == null) {
            schemaTracker = new SchemaTracker();
        }
        // Simulate what applyEntry would do for a CREATE_INDEX record.
        java.util.Map<String, Index> tracked =
                getSchemaTrackerIndexes();
        tracked.put(index.name, index);
        // Reflection write bypasses applyEntry() so the per-table vector
        // index cache does not get invalidated — drop it explicitly.
        schemaTracker.invalidateVectorIndexCache();
        vectorStores.put(storeKey(index.table, index.name), store);
    }

    /**
     * Reflection-free accessor used only by
     * {@link #registerIndexForTest(Index, AbstractVectorStore)}. Exposes the
     * private {@code indexes} map on {@link SchemaTracker} so tests can seed
     * it. We avoid a public setter on {@code SchemaTracker} because the
     * production code only mutates it via log entries.
     */
    @SuppressWarnings("unchecked")
    private java.util.Map<String, Index> getSchemaTrackerIndexes() {
        try {
            java.lang.reflect.Field f = SchemaTracker.class.getDeclaredField("indexes");
            f.setAccessible(true);
            return (java.util.Map<String, Index>) f.get(schemaTracker);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to access SchemaTracker.indexes", e);
        }
    }

    /**
     * Enumerates every vector index loaded by this engine instance. Used by
     * the indexing-admin CLI.
     */
    public List<IndexDescriptor> listIndexes() {
        List<IndexDescriptor> out = new ArrayList<>(vectorStores.size());
        if (schemaTracker == null) {
            return out;
        }
        for (Index idx : schemaTracker.getAllIndexes()) {
            if (!Index.TYPE_VECTOR.equals(idx.type)) {
                continue;
            }
            AbstractVectorStore store = vectorStores.get(storeKey(idx.table, idx.name));
            long vectorCount = store != null ? store.size() : 0;
            String status = store != null ? "tailing" : "missing";
            int segmentCount = (store instanceof PersistentVectorStore)
                    ? ((PersistentVectorStore) store).getSegmentCount()
                    : (store != null ? 1 : 0);
            out.add(new IndexDescriptor(idx.tablespace, idx.table, idx.name, vectorCount, status, segmentCount));
        }
        return out;
    }

    /**
     * Returns extended diagnostic information for a single vector index.
     * Returns {@code null} if the index is not loaded on this instance.
     */
    public IndexDetails describeIndex(String tablespace, String table, String index) {
        AbstractVectorStore store = vectorStores.get(storeKey(table, index));
        if (store == null) {
            return null;
        }
        IndexDetails d = new IndexDetails();
        d.tablespace = tablespace;
        d.table = table;
        d.index = index;
        d.vectorCount = store.size();
        d.status = "tailing";
        d.estimatedMemoryBytes = store.estimatedMemoryUsageBytes();
        d.liveVectorsMemoryBytes = store.estimatedMemoryUsageBytes();
        d.storeClass = store.getClass().getSimpleName();
        d.persistent = store instanceof PersistentVectorStore;
        if (store instanceof PersistentVectorStore) {
            PersistentVectorStore pvs = (PersistentVectorStore) store;
            d.dimension = pvs.getDimension();
            d.similarity = pvs.getSimilarityFunction();
            d.liveNodeCount = pvs.getLiveNodeCount();
            d.onDiskNodeCount = pvs.getOnDiskNodeCount();
            d.segmentCount = pvs.getSegmentCount();
            d.liveShardCount = pvs.getLiveShardCount();
            d.liveVectorsMemoryBytes = pvs.getLiveVectorsMemoryBytes();
            d.onDiskSizeBytes = pvs.getEstimatedSizeBytes();
            d.dirty = pvs.isDirty();
            d.fusedPQEnabled = pvs.isFusedPQEnabled();
            d.m = pvs.getM();
            d.beamWidth = pvs.getBeamWidth();
            d.compactionPhase = pvs.getCompactionPhase();
            d.compactionProgress = pvs.getCompactionProgressPercent();
            d.compactionNodesDone = pvs.getCompactionNodesDone();
            d.compactionNodesTotal = pvs.getCompactionNodesTotal();
            d.uploadBytesDone = pvs.getUploadBytesDone();
            d.uploadBytesTotal = pvs.getUploadBytesTotal();
            d.nextNodeId = pvs.getNextNodeId();
            if (!"idle".equals(d.compactionPhase)) {
                d.status = d.compactionPhase;
            }
        } else if (store instanceof InMemoryVectorStore) {
            d.similarity = ((InMemoryVectorStore) store).getSimilarityType().name();
            d.segmentCount = 1;
            d.liveNodeCount = store.size();
            d.liveShardCount = 1;
        }
        LogSequenceNumber tailerSnap = lastProcessedLsn;
        if (tailerSnap != null) {
            d.tailerLsnLedger = tailerSnap.ledgerId;
            d.tailerLsnOffset = tailerSnap.offset;
        } else {
            d.tailerLsnLedger = -1L;
            d.tailerLsnOffset = -1L;
        }
        LogSequenceNumber durableSnap = lastDurableLsn;
        if (durableSnap != null) {
            d.durableLsnLedger = durableSnap.ledgerId;
            d.durableLsnOffset = durableSnap.offset;
        } else {
            d.durableLsnLedger = -1L;
            d.durableLsnOffset = -1L;
        }
        return d;
    }

    /**
     * Streams every primary key held by the given vector index through the
     * supplied visitor. Returns the total number of PKs visited. The visitor
     * may return {@code false} to stop the walk early (e.g. the server handler
     * uses this to implement a chunked-response limit).
     *
     * @param limit maximum number of PKs to visit, or {@code <= 0} for no cap
     */
    public long streamPrimaryKeys(String tablespace, String table, String index,
                                   boolean includeOnDisk, long limit,
                                   Predicate<Bytes> visitor) {
        AbstractVectorStore store = vectorStores.get(storeKey(table, index));
        if (store == null) {
            return 0;
        }
        long[] count = new long[]{0L};
        store.forEachPrimaryKey(includeOnDisk, pk -> {
            count[0]++;
            if (limit > 0 && count[0] > limit) {
                return false;
            }
            return visitor.test(pk);
        });
        if (limit > 0 && count[0] > limit) {
            count[0] = limit;
        }
        return count[0];
    }

    /**
     * Sum of estimated memory used by every loaded vector store.
     */
    public long getTotalEstimatedMemoryBytes() {
        long total = 0;
        for (AbstractVectorStore store : vectorStores.values()) {
            total += store.estimatedMemoryUsageBytes();
        }
        return total;
    }

    public void setStatsLogger(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;
        registerTailerMetrics();
        registerShadowMetrics();
        // Netty direct-memory counters (issue #246) so the unified JVM
        // dashboard can show pool-arena growth for the IS alongside the
        // main server and the remote file service. The gauges carry
        // their own netty_ prefix — pass the root logger, not a scope.
        if (statsLogger != null) {
            herddb.core.stats.NettyMemoryMetrics.register(statsLogger);
        }
    }

    /**
     * Registers shadow-specific Prometheus gauges. Called only when the
     * engine has been configured as a shadow ({@link #isConfiguredAsShadow()});
     * for primaries it's a no-op.
     */
    private void registerShadowMetrics() {
        StatsLogger sl = this.statsLogger;
        if (sl == null || !config.isShadow()) {
            return;
        }
        StatsLogger shadow = sl.scope("shadow");
        shadow.registerGauge("ready", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return shadowReady ? 1 : 0;
            }
        });
        shadow.registerGauge("reload_count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return shadowReloadCount.get();
            }
        });
        shadow.registerGauge("loaded_ledger_id", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return -1L;
            }
            @Override
            public Long getSample() {
                LogSequenceNumber l = shadowLoadedLsn;
                return l != null ? l.ledgerId : -1L;
            }
        });
        shadow.registerGauge("loaded_offset", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return -1L;
            }
            @Override
            public Long getSample() {
                LogSequenceNumber l = shadowLoadedLsn;
                return l != null ? l.offset : -1L;
            }
        });
        shadow.registerGauge("last_reload_timestamp_ms", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return shadowLastReloadTimestampMs;
            }
        });
        // lag_entries: primary_advertised_offset - loaded_offset when same
        // ledger, else -1 to signal "unknown across ledgers".
        shadow.registerGauge("lag_entries", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return -1L;
            }
            @Override
            public Long getSample() {
                LogSequenceNumber adv = primaryAdvertisedLsn;
                LogSequenceNumber loaded = shadowLoadedLsn;
                if (adv == null || loaded == null) {
                    return -1L;
                }
                if (adv.ledgerId != loaded.ledgerId) {
                    return -1L;
                }
                return Math.max(0L, adv.offset - loaded.offset);
            }
        });
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
        indexStats.registerGauge("live_vectors_estimated_memory_bytes", new Gauge<Long>() {
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
            // Global monotonic node-id counter (issue #256). Exposed as a
            // gauge so dashboards can track the burn rate and alert long
            // before the long space is exhausted.
            indexStats.registerGauge("next_node_id", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getNextNodeId();
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
            indexStats.registerGauge("segment_count_backpressure_active", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.isSegmentCountBackpressureActive() ? 1 : 0;
                }
            });
            indexStats.registerGauge("segment_count_backpressure_total", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getSegmentCountBackpressureTotal();
                }
            });
            indexStats.registerGauge("segment_count_backpressure_time_ms", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getSegmentCountBackpressureTimeMs();
                }
            });
            indexStats.registerGauge("segment_count_backpressure_timeouts", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getSegmentCountBackpressureTimeouts();
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
            // P3.7 metrics — checkpoint throughput, segment count, disk usage,
            // rollback counters.
            indexStats.registerGauge("sealed_segment_count", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getSealedSegmentCount();
                }
            });
            indexStats.registerGauge("phase_b_bytes_written", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLastPhaseBBytesWritten();
                }
            });
            // Compaction progress gauges (issue #80). compaction_phase is not
            // a scalar, so we expose two 0/1 "active" gauges and let clients
            // derive the phase from whichever is non-zero.
            indexStats.registerGauge("compaction_nodes_done", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getCompactionNodesDone();
                }
            });
            indexStats.registerGauge("compaction_nodes_total", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getCompactionNodesTotal();
                }
            });
            indexStats.registerGauge("compaction_progress_pct", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    int v = pvs.getCompactionProgressPercent();
                    return v < 0 ? 0 : v;
                }
            });
            indexStats.registerGauge("upload_bytes_done", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getUploadBytesDone();
                }
            });
            indexStats.registerGauge("upload_bytes_total", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getUploadBytesTotal();
                }
            });
            indexStats.registerGauge("writing_graph_active", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getWritingGraphActiveCount() > 0 ? 1 : 0;
                }
            });
            indexStats.registerGauge("uploading_segment_active", new Gauge<Integer>() {
                @Override
                public Integer getDefaultValue() {
                    return 0;
                }
                @Override
                public Integer getSample() {
                    return pvs.getUploadingActiveCount() > 0 ? 1 : 0;
                }
            });
            indexStats.registerGauge("phase_b_vectors_per_second", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return (long) pvs.getLastPhaseBVectorsPerSecond();
                }
            });
            indexStats.registerGauge("checkpoint_consecutive_failures", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getConsecutiveCheckpointFailures();
                }
            });
            indexStats.registerGauge("checkpoint_total_failures", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalCheckpointFailures();
                }
            });
            indexStats.registerGauge("rolled_back_pages_total", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTotalRolledBackPages();
                }
            });
            indexStats.registerGauge("rolled_back_pages_last", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getLastRolledBackPages();
                }
            });
            indexStats.registerGauge("tmp_dir_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getTmpDirBytes();
                }
            });
            indexStats.registerGauge("free_disk_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    long v = pvs.getFreeDiskBytes();
                    return v < 0 ? 0L : v;
                }
            });
            indexStats.registerGauge("ondisk_estimated_size_bytes", new Gauge<Long>() {
                @Override
                public Long getDefaultValue() {
                    return 0L;
                }
                @Override
                public Long getSample() {
                    return pvs.getEstimatedSizeBytes();
                }
            });
            registerVectorIndexCompactionMetrics(indexStats, pvs);
            pvs.setSegmentSizeStats(indexStats.getOpStatsLogger("segment_size_bytes"));
        }
    }

    /**
     * Registers all graph-merge compaction and retention-reaper metrics
     * for one {@link PersistentVectorStore}. Named to match the plan in
     * VECTOR.md so the Grafana dashboard panels line up one-to-one.
     */
    private void registerVectorIndexCompactionMetrics(StatsLogger indexStats,
                                                      PersistentVectorStore pvs) {
        registerLongCounter(indexStats, "compaction_runs_total",
                pvs::getCompactionRunsTotal);
        registerLongCounter(indexStats, "compaction_successes_total",
                pvs::getCompactionSuccessesTotal);
        registerLongCounter(indexStats, "compaction_failures_read_io_total",
                pvs::getCompactionFailuresReadIoTotal);
        registerLongCounter(indexStats, "compaction_failures_write_io_total",
                pvs::getCompactionFailuresWriteIoTotal);
        registerLongCounter(indexStats, "compaction_failures_metadata_io_total",
                pvs::getCompactionFailuresMetadataIoTotal);
        registerLongCounter(indexStats, "compaction_failures_corruption_total",
                pvs::getCompactionFailuresCorruptionTotal);
        registerLongCounter(indexStats, "compaction_failures_disk_full_total",
                pvs::getCompactionFailuresDiskFullTotal);
        registerLongCounter(indexStats, "compaction_failures_aborted_input_gone_total",
                pvs::getCompactionFailuresAbortedInputGoneTotal);
        registerLongCounter(indexStats, "compaction_live_pk_filtered_total",
                pvs::getCompactionLivePkFilteredTotal);
        registerLongCounter(indexStats, "pending_deletes_reaped_total",
                pvs::getPendingDeletesReapedTotal);
        registerLongCounter(indexStats, "pending_deletes_reap_failures_total",
                pvs::getPendingDeletesReapFailuresTotal);
        registerLongGauge(indexStats, "compaction_last_duration_ms",
                pvs::getCompactionLastDurationMs);
        registerLongGauge(indexStats, "compaction_last_bytes_read",
                pvs::getCompactionLastBytesRead);
        registerLongGauge(indexStats, "compaction_last_bytes_written",
                pvs::getCompactionLastBytesWritten);
        registerLongGauge(indexStats, "compaction_last_input_segments",
                pvs::getCompactionLastInputSegments);
        registerLongGauge(indexStats, "compaction_last_output_segments",
                pvs::getCompactionLastOutputSegments);
        registerLongGauge(indexStats, "compaction_consecutive_failures",
                pvs::getCompactionConsecutiveFailures);
        registerIntGauge(indexStats, "compaction_active",
                pvs::getCompactionActive);
        registerIntGauge(indexStats, "pending_deletes_count",
                () -> pvs.getPendingDeletesSnapshot().size());
        registerLongGauge(indexStats, "pending_deletes_bytes", () -> {
            // Approximate: pendingDeletes record paths, not byte sizes.
            // Report 0 for now; the reaper already emits an accurate
            // "reaped bytes" metric via pending_deletes_reaped_total
            // paired with compaction_last_bytes_read.
            return 0L;
        });
        registerLongGauge(indexStats, "pending_deletes_oldest_age_seconds", () -> {
            long now = System.currentTimeMillis();
            long oldest = Long.MAX_VALUE;
            boolean any = false;
            for (PersistentVectorStore.PendingDelete pd : pvs.getPendingDeletesSnapshot()) {
                long age = now - (pd.deadlineMs - 0);  // pd.deadlineMs = createdAt+retention
                if (age < oldest) {
                    oldest = age;
                    any = true;
                }
            }
            return any ? Math.max(0L, oldest / 1000L) : 0L;
        });
        registerLongGauge(indexStats, "applied_index_status_generation",
                pvs::getCurrentIndexStatusGeneration);
    }

    private static void registerLongCounter(StatsLogger scope, String name,
                                            java.util.function.LongSupplier supplier) {
        scope.registerGauge(name, new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return supplier.getAsLong();
            }
        });
    }

    private static void registerLongGauge(StatsLogger scope, String name,
                                          java.util.function.LongSupplier supplier) {
        registerLongCounter(scope, name, supplier);
    }

    private static void registerIntGauge(StatsLogger scope, String name,
                                         java.util.function.IntSupplier supplier) {
        scope.registerGauge(name, new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return supplier.getAsInt();
            }
        });
    }

    /**
     * Registers Prometheus gauges + derived counters for the shared
     * {@link SegmentBlockCache}. Called once from {@link #start()} after the
     * cache is created. All metrics are read lazily via {@link Gauge#getSample}
     * so they stay in sync without any bookkeeping on the hot path.
     */
    private void registerSegmentBlockCacheMetrics(SegmentBlockCache cache) {
        StatsLogger local = this.statsLogger;
        if (local == null) {
            return;
        }
        StatsLogger scope = local.scope("indexing").scope("segment_block_cache");
        scope.registerGauge("hits", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.hitCount();
            }
        });
        scope.registerGauge("misses", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.missCount();
            }
        });
        scope.registerGauge("evictions", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.evictionCount();
            }
        });
        scope.registerGauge("load_success", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.loadSuccessCount();
            }
        });
        scope.registerGauge("load_failure", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.loadFailureCount();
            }
        });
        scope.registerGauge("load_time_nanos_total", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.totalLoadTimeNanos();
            }
        });
        scope.registerGauge("size_entries", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.estimatedSize();
            }
        });
        scope.registerGauge("size_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.weightedSize();
            }
        });
        scope.registerGauge("max_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.maxBytes();
            }
        });
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("IndexingServiceEngine closing");

        // Shadow-only: stop the single-thread reload executor.
        if (shadowReloadExecutor != null) {
            shadowReloadExecutor.shutdownNow();
            try {
                shadowReloadExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            shadowReloadExecutor = null;
        }

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

        // Shut down the checkpoint executor BEFORE the apply workers so that
        // any in-flight Phase B can complete and save the watermark. Doing
        // this after the apply workers are shut down would cause the in-flight
        // checkpoint's awaitPendingWork() barrier-submit to fail with
        // RejectedExecutionException.
        if (checkpointExecutor != null) {
            checkpointExecutor.shutdown();
            try {
                if (!checkpointExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOGGER.log(Level.WARNING,
                            "In-flight checkpoint did not complete within 30s of shutdown; forcing shutdownNow");
                    checkpointExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                checkpointExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            checkpointExecutor = null;
            inflightCheckpoint = null;
            // Pending DROP cleanup tasks have either completed (executor
            // drained on shutdown) or were cancelled by shutdownNow(); either
            // way, drop our references so the futures (and the captured
            // AbstractVectorStore instances) are eligible for GC.
            pendingDropTasks.clear();
            LOGGER.info("Checkpoint executor shut down");
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

        // Shutdown: do NOT save the watermark as a side effect. The watermark
        // is only persisted after a successful checkpoint via
        // checkpointAndSaveWatermark(). Saving at shutdown would publish an
        // LSN that is not guaranteed to be covered by checkpointed state on
        // S3 — on a wiped-disk restart the service would claim progress it
        // cannot actually replay. Replay from the last checkpoint watermark
        // on next boot is safe (apply is idempotent).

        // Close and clear all vector stores
        for (AbstractVectorStore store : vectorStores.values()) {
            try {
                store.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error closing vector store", e);
            }
        }
        vectorStores.clear();
        vectorStoreIndexUuids.clear();

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
     *
     * <p>Carries two LSNs with very different semantics (issue #364):
     * <ul>
     *   <li>{@code tailerLsn*} — the in-memory tailer position; updated on
     *       every applied entry; useful for diagnostic visibility but UNSAFE
     *       as a recovery floor.</li>
     *   <li>{@code durableLsn*} — the LSN of the most recent checkpoint whose
     *       watermark has been persisted to remote storage; the LSN the
     *       engine will resume from on restart. The server's commit-log
     *       retention floor MUST be pinned against this value.</li>
     * </ul>
     */
    public static class IndexStatusInfo {
        private final long vectorCount;
        private final int segmentCount;
        private final long tailerLsnLedger;
        private final long tailerLsnOffset;
        private final long durableLsnLedger;
        private final long durableLsnOffset;
        private final String status;

        public IndexStatusInfo(long vectorCount, int segmentCount,
                               long tailerLsnLedger, long tailerLsnOffset,
                               long durableLsnLedger, long durableLsnOffset,
                               String status) {
            this.vectorCount = vectorCount;
            this.segmentCount = segmentCount;
            this.tailerLsnLedger = tailerLsnLedger;
            this.tailerLsnOffset = tailerLsnOffset;
            this.durableLsnLedger = durableLsnLedger;
            this.durableLsnOffset = durableLsnOffset;
            this.status = status;
        }

        public long getVectorCount() {
            return vectorCount;
        }

        public int getSegmentCount() {
            return segmentCount;
        }

        public long getTailerLsnLedger() {
            return tailerLsnLedger;
        }

        public long getTailerLsnOffset() {
            return tailerLsnOffset;
        }

        public long getDurableLsnLedger() {
            return durableLsnLedger;
        }

        public long getDurableLsnOffset() {
            return durableLsnOffset;
        }

        public String getStatus() {
            return status;
        }
    }

    /**
     * Lightweight per-index row for {@link #listIndexes()}.
     */
    public static final class IndexDescriptor {
        private final String tablespace;
        private final String table;
        private final String index;
        private final long vectorCount;
        private final String status;
        private final int segmentCount;

        public IndexDescriptor(String tablespace, String table, String index,
                               long vectorCount, String status, int segmentCount) {
            this.tablespace = tablespace;
            this.table = table;
            this.index = index;
            this.vectorCount = vectorCount;
            this.status = status;
            this.segmentCount = segmentCount;
        }

        public String getTablespace() {
            return tablespace;
        }

        public String getTable() {
            return table;
        }

        public String getIndex() {
            return index;
        }

        public long getVectorCount() {
            return vectorCount;
        }

        public String getStatus() {
            return status;
        }

        public int getSegmentCount() {
            return segmentCount;
        }
    }

    /**
     * Extended per-index diagnostic record returned by
     * {@link #describeIndex(String, String, String)}. Fields are mutable so
     * {@code describeIndex} can assemble the record lazily.
     */
    public static final class IndexDetails {
        public String tablespace;
        public String table;
        public String index;
        public long vectorCount;
        public String status;
        public int dimension;
        public String similarity;
        public long liveNodeCount;
        public long onDiskNodeCount;
        public int segmentCount;
        public int liveShardCount;
        public long estimatedMemoryBytes;
        public long liveVectorsMemoryBytes;
        public long onDiskSizeBytes;
        public boolean dirty;
        // In-memory tailer position; diagnostic only. See IndexStatusInfo.
        public long tailerLsnLedger;
        public long tailerLsnOffset;
        // Durable recovery LSN; the LSN the engine resumes from on restart.
        public long durableLsnLedger;
        public long durableLsnOffset;
        public boolean fusedPQEnabled;
        public int m;
        public int beamWidth;
        public boolean persistent;
        public String storeClass;
        // Compaction progress (issue #80). compactionPhase is "idle" when
        // no Phase-B activity is in flight; progress is -1 when idle.
        public String compactionPhase = "idle";
        public int compactionProgress = -1;
        public long compactionNodesDone;
        public long compactionNodesTotal;
        public long uploadBytesDone;
        public long uploadBytesTotal;
        // Global monotonic node-id counter (issue #256). Widened to long end-to-end;
        // surfaced so clients and dashboards can observe the burn rate.
        public long nextNodeId;
    }
}
