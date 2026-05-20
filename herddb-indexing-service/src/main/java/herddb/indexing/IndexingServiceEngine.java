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
import herddb.indexing.vector.AbstractVectorStore;
import herddb.indexing.vector.PersistentVectorStore;
import herddb.indexing.vector.ReadOnlyVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.indexing.vector.VectorMemoryBudget;
import herddb.indexing.segment.SegmentRegistryClient;
import herddb.indexing.segment.SegmentRegistryException;
import herddb.indexing.segment.SegmentRegistryPublisher;
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
import io.netty.util.concurrent.FastThreadLocalThread;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
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

    /**
     * Non-null only when {@code indexing.log.type=push}: the testing-only
     * tailer fed by the {@code PushEntries} gRPC RPC. Held with a typed
     * reference (in addition to {@link #tailer}) so {@link IndexingServiceImpl}
     * can enqueue client-pushed entries into its bounded buffer.
     */
    private volatile PushCommitLogTailer pushTailer;

    private volatile LogSequenceNumber lastProcessedLsn;
    /**
     * Wall-clock timestamp (epoch ms) of the {@link LogEntry} at
     * {@link #lastProcessedLsn} — the freshness of the in-memory tailer.
     * Updated atomically-with-best-effort next to {@link #lastProcessedLsn}
     * inside {@link #processEntry(LogSequenceNumber, LogEntry)}. {@code 0}
     * means "no entries processed yet". Issue #423: surfaced via
     * {@code GetIndexStatus.tailer_lsn_timestamp}.
     *
     * <p><b>Caveat:</b> {@link LogEntry#timestamp} is a wall-clock value
     * stamped by whichever cluster writer produced the entry. Different
     * writers may have skewed clocks, so this field can <em>regress</em>
     * between adjacent entries when the tailer crosses a writer boundary.
     * Use it as an indicative freshness signal for dashboards
     * ({@code now - lastProcessedEntryTimestamp ≈ "how far behind real
     * time the IS is"}), not as a monotonic clock.
     */
    private volatile long lastProcessedEntryTimestamp;
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
    /**
     * Wall-clock timestamp (epoch ms) of the {@link LogEntry} at
     * {@link #lastDurableLsn} — the freshness of the durable recovery state.
     * Initialized at {@link #start()} from
     * {@link WatermarkSnapshot#lastEntryTimestamp}. Advanced strictly inside
     * {@link #checkpointAndSaveWatermark()}, immediately after a successful
     * {@code watermarkStore.save(...)} call. Issue #423: surfaced via
     * {@code GetIndexStatus.durable_lsn_timestamp}.
     */
    private volatile long lastDurableEntryTimestamp;
    private long entriesSinceLastCheckpoint;

    /**
     * Per-operation-type counters for the commit-log tailer (issue #459).
     * Together with the existing {@code tailer.entries_processed} gauge, these
     * let dashboards and supervision agents distinguish "the tailer is busy
     * applying real vector writes" from "the tailer is mostly skipping
     * non-vector traffic" or "the tailer is catching up on DDL". All counters
     * are monotonically increasing since JVM start; rate is derived by
     * Prometheus / consumers.
     *
     * <p>Counters are written from a single thread (the tailer thread) inside
     * {@link #processEntry(LogSequenceNumber, LogEntry)} but read from
     * arbitrary threads via the Prometheus gauge samples and gRPC handlers.
     * {@link LongAdder} gives lock-free striped writes and a consistent
     * (eventually-consistent) read.
     *
     * <p>Classification:
     * <ul>
     *   <li>INSERT/UPDATE/DELETE → bumps the matching per-op counter and
     *       {@link #tailerEntriesAccepted} (intent to mutate the HNSW graph,
     *       even if the entry's table has no vector index — the per-index
     *       skip happens deeper in {@code applyInsert} / {@code applyUpdate} /
     *       {@code applyDelete}).</li>
     *   <li>DDL (CREATE_TABLE / ALTER_TABLE / DROP_TABLE / TRUNCATE_TABLE /
     *       CREATE_INDEX / DROP_INDEX) → bumps {@link #tailerDdl} and
     *       {@link #tailerEntriesSkipped} (no graph mutation; just schema).</li>
     *   <li>Everything else (NOOP, REBALANCE, transactional control entries
     *       BEGIN/COMMIT/ROLLBACK that are themselves not graph mutations)
     *       → bumps {@link #tailerEntriesSkipped}. The buffered entries that a
     *       COMMITTRANSACTION ultimately replays are themselves DML and were
     *       already classified as accepted on their original arrival.</li>
     * </ul>
     *
     * <p>Issue #463: {@link #tailerEntriesShardFiltered} is bumped from
     * {@link #applyInsert(LogEntry)}, NOT from {@code classifyForMetrics} —
     * the shard-filter decision is per-key + per-index and only knowable
     * after schema lookup. It counts INSERT entries this replica did NOT
     * apply because every vector index for the table rejected the key
     * via {@link #isAcceptedLocally(Bytes, Index)}. UPDATE entries are
     * not tracked here because their broadcast remove still mutates state
     * on every replica, regardless of shard ownership.
     */
    private final LongAdder tailerEntriesAccepted = new LongAdder();
    private final LongAdder tailerEntriesSkipped = new LongAdder();
    private final LongAdder tailerEntriesShardFiltered = new LongAdder();
    private final LongAdder tailerInserts = new LongAdder();
    private final LongAdder tailerUpdates = new LongAdder();
    private final LongAdder tailerDeletes = new LongAdder();
    private final LongAdder tailerDdl = new LongAdder();

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
    /**
     * Lifecycle states used by {@link #processEntry} and
     * {@link #applyEntry} to decide whether a commit-log entry should
     * be applied at all.
     *
     * <ul>
     *   <li>{@link #ACTIVE}: normal — entries are applied; the watermark
     *       advances on every successful apply.</li>
     *   <li>{@link #JOINING}: the engine has no schema yet and drops every
     *       commit-log entry except {@code INDEXING_SERVICE_REBALANCE}.
     *       The first REBALANCE installs the schema and transitions the
     *       engine to {@link #ACTIVE}.</li>
     *   <li>{@link #FAILED}: a fatal apply-time error occurred (issue #471 —
     *       a {@code rebuild=true} CREATE VECTOR INDEX whose IS-side
     *       back-fill threw). The engine refuses to advance the tailer
     *       past the failed entry: every subsequent
     *       {@link #processEntry} call early-returns without
     *       advancing {@code lastProcessedLsn} or
     *       {@code entriesSinceLastCheckpoint}, so the watermark cannot
     *       be persisted past the failed entry. On engine restart, the
     *       failed entry is replayed from the still-stale watermark and
     *       the rebuild re-runs from scratch. This avoids the
     *       silent-data-loss path where a successful follow-up entry
     *       would otherwise advance the watermark past a partially-
     *       back-filled vector store, making the partial state
     *       permanent.</li>
     * </ul>
     */
    public enum EngineStatus { ACTIVE, JOINING, FAILED }

    private volatile EngineStatus engineStatus = EngineStatus.ACTIVE;

    // Shadow-replica state (only meaningful when config.isShadow() == true).
    /** true once this shadow has completed its first successful reload. */
    private volatile boolean shadowReady;
    /** Primary's latest advertised LSN read from ZK, or null if never observed. */
    private volatile LogSequenceNumber primaryAdvertisedLsn;
    /** Loaded LSN of the shadow's current on-disk view. */
    private volatile LogSequenceNumber shadowLoadedLsn;
    /**
     * Wall-clock timestamp (epoch ms) of the LogEntry at {@link #shadowLoadedLsn}
     * — the freshness of the data this shadow can serve. Picked up from the
     * primary's advertised
     * {@link herddb.metadata.IndexingServiceCheckpointState#getLastEntryTimestampMillis()}
     * on every reload. {@code 0} means "unknown" (primary has not published
     * a checkpoint yet). Issue #423: surfaced via
     * {@code GetShadowStatus.loaded_entry_timestamp_ms}.
     */
    private volatile long shadowLoadedEntryTimestamp;
    /** Wall-clock of the most recent successful reload. */
    private volatile long shadowLastReloadTimestampMs;
    /** Count of successful reloads (including the initial one). */
    private final java.util.concurrent.atomic.AtomicLong shadowReloadCount =
            new java.util.concurrent.atomic.AtomicLong(0);
    /** Single-thread executor that serialises reloads (ZK watcher hands off). */
    private ExecutorService shadowReloadExecutor;

    private volatile StatsLogger statsLogger;

    /**
     * Single shared {@link herddb.indexing.segment.SegmentAssignmentMetrics}
     * for the engine — subscribes to every {@code SegmentAssignmentWatcher}
     * created by this IS instance so the gauges + counters reflect the
     * union of segments owned across all indexes. Prometheus exposition
     * happens through {@link #registerSegmentAssignmentMetrics}.
     */
    private final herddb.indexing.segment.SegmentAssignmentMetrics segmentAssignmentMetrics =
            new herddb.indexing.segment.SegmentAssignmentMetrics();

    /**
     * One {@link herddb.indexing.segment.SegmentAssignmentWatcher} per vector store,
     * keyed by {@link #storeKey}. Each watcher watches the ZK segment-registry subtree
     * for that store's index and calls
     * {@link herddb.indexing.vector.AbstractVectorStore#adoptExternalSegment} /
     * {@link herddb.indexing.vector.AbstractVectorStore#dropSegmentByUuid} when the
     * optimizer produces or deprecates segments (issue #514).
     *
     * <p>Populated by the {@code vectorStoreFactory} lambda and closed in
     * {@link #close()}, before the vector stores themselves are closed.
     */
    private final java.util.concurrent.ConcurrentHashMap<String, herddb.indexing.segment.SegmentAssignmentWatcher>
            segmentWatchers = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Issue #471 — engine-wide counters and timings for the
     * {@code rebuild=true} back-fill pass driven by
     * {@link VectorIndexRebuilder}. Registered with the engine's
     * {@link StatsLogger} during {@link #start()} so the {@code
     * rebuild.records_scanned}, {@code rebuild.records_indexed},
     * {@code rebuild.last_start_time_ms}, and {@code
     * rebuild.last_end_time_ms} gauges show up in Prometheus.
     */
    private final VectorIndexRebuildMetrics rebuildMetrics = new VectorIndexRebuildMetrics();

    private MetadataStorageManager metadataStorageManager;
    private boolean ownsMetadataStorageManager;

    private MemoryManager memoryManager;
    private DataStorageManager dataStorageManager;
    private long maxVectorMemoryBytes = Long.MAX_VALUE;
    private volatile String tableSpaceUUID;

    /**
     * Issue #491 — segmented-v2 ZK segment registry handle, populated in
     * {@link #start()} when {@link IndexingServerConfiguration#PROPERTY_INDEX_OPTIMIZER_ENABLED}
     * is {@code true} AND the metadata storage manager is a
     * {@link ZookeeperMetadataStorageManager} (i.e. cluster mode). When set,
     * the production {@code vectorStoreFactory} attaches a
     * {@link SegmentRegistryPublisher} to every freshly-created
     * {@link PersistentVectorStore} so that each successful checkpoint
     * publishes its sealed segments as ACTIVE znodes — making them visible
     * to the external index-optimizer service.
     *
     * <p>Stays {@code null} when the property is absent / false (legacy
     * single-IS mode) OR when the metadata storage manager is non-ZK
     * (standalone mode + property accidentally set — surfaced as a WARNING
     * log so operators notice the misconfiguration). The factory branch
     * checks for null and silently falls back to legacy behaviour in both
     * cases.
     *
     * <p>{@code volatile} because reads happen on the tailer thread (when it
     * invokes the factory on a CREATE_INDEX) while the field is published
     * from the engine-bootstrap thread inside {@link #start()}.
     */
    private volatile SegmentRegistryClient segmentRegistry;

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

    /**
     * Whether the post-Phase-C warmup runs on a dedicated executor
     * ({@code true}) or inline on {@link #checkpointExecutor} ({@code false}).
     * Resolved at {@link #start()} from
     * {@link IndexingServerConfiguration#PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC}
     * with the JVM system property
     * {@link IndexingServerConfiguration#SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC}
     * as fallback (issue #472).
     */
    private boolean warmupAsync;

    /**
     * Single-thread executor that runs the post-Phase-C BFS warmup off the
     * {@link #checkpointExecutor} thread (issue #472). The warmup reads the
     * entry-point neighbourhood of every segment via the same
     * {@link herddb.remote.SegmentBlockCache} that search queries use; running
     * it inline on the checkpoint thread blocks both the next checkpoint and
     * the watermark snapshot publication for the duration of the BFS (~3 s
     * for 21 × 33 MiB segments in the gist1m bench profile of issue #472).
     *
     * <p>Owned by the engine — created in {@link #start()} when warmup is
     * enabled in async mode and shut down in {@link #close()}. {@code null}
     * when warmup is disabled or running in synchronous mode.
     */
    private ExecutorService warmupExecutor;

    /**
     * Tracks the most recently submitted warmup task. Used to coalesce
     * concurrent submits (skip a new submit if the previous warmup is still
     * running) and to let {@link #forceCheckpointAndSaveWatermark()} await
     * completion so test post-conditions still hold (issue #472).
     */
    private final AtomicReference<Future<?>> lastWarmupFuture = new AtomicReference<>();

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

    /**
     * Derives a stable 32-hex-char tablespace UUID from the tablespace name.
     * Used by push mode when no HerdDB server has registered the tablespace in
     * the metadata store: a name-based UUID is identical across restarts and
     * across sibling push-mode instances, so they all resolve the same storage
     * namespace without any coordination.
     */
    private static String deterministicTableSpaceUuid(String tablespaceName) {
        return java.util.UUID.nameUUIDFromBytes(
                        tablespaceName.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toString().replace("-", "");
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
        // The synchronous checkpoint may have queued an async warmup
        // (issue #472). Wait for it to complete so that the caller's
        // post-condition — "after this returns, the cache has been warmed
        // and bytes have been read through the storage manager" — still
        // holds for tests written before async warmup existed. Production
        // code does not call forceCheckpointAndSaveWatermark; it goes
        // through triggerCheckpointAsync instead, which is unaffected.
        Future<?> warmup = lastWarmupFuture.get();
        if (warmup != null) {
            try {
                warmup.get(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Interrupted awaiting warmup in forceCheckpointAndSaveWatermark");
            } catch (ExecutionException e) {
                LOGGER.log(Level.FINE, "Async warmup ended with failure", e.getCause());
            } catch (java.util.concurrent.TimeoutException e) {
                LOGGER.log(Level.WARNING,
                        "Async warmup did not complete within 60 s in forceCheckpointAndSaveWatermark");
            }
        }
    }

    public void setVectorStoreFactory(VectorStoreFactory factory) {
        this.vectorStoreFactory = factory;
    }

    /**
     * Issue #491: visible-for-tests accessor for the engine's currently-installed
     * vector store factory. After {@link #start()} this is the production factory
     * built around the configured storage type (or the in-memory fallback). Tests
     * use it to invoke the production lambda directly and assert the per-store
     * wiring (publisher attached, external compaction enabled, etc.) without
     * having to spin up a real BookKeeper commit-log tailer.
     */
    VectorStoreFactory getVectorStoreFactory() {
        return vectorStoreFactory;
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
            // Micro-segment fast-path threshold (issue #570).
            final long vectorCompactionMicroSegmentMaxNodes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MICROSEGMENT_MAX_NODES,
                    IndexingServerConfiguration
                            .PROPERTY_VECTOR_INDEX_COMPACTION_MICROSEGMENT_MAX_NODES_DEFAULT);
            // Hard cap on input segments per compaction cycle (issue #587).
            final int vectorCompactionMaxInputs = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_INPUTS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_INPUTS_DEFAULT);
            // Hard cap on total bytes of source graph files per compaction cycle (issue #602).
            final long vectorCompactionMaxInputBytes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_INPUT_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_MAX_INPUT_BYTES_DEFAULT);
            final boolean vectorCompactionTieredEnabled = config.getBoolean(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_TIERED_ENABLED_DEFAULT);
            final int vectorCompactionBackpressureSegments = config.getInt(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_SEGMENTS_DEFAULT);
            final long vectorCompactionBackpressureMaxWaitMs = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_MAX_WAIT_MS,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_BACKPRESSURE_MAX_WAIT_MS_DEFAULT);
            // Range validation for the kick fraction (must be in (0, 1)) is
            // enforced inside {@link PersistentVectorStore#setLocalCompactionKickFraction};
            // we let the setter throw IllegalArgumentException at start time
            // rather than duplicating the check here.
            final double vectorCompactionLocalKickFraction = config.getDouble(
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_KICK_FRACTION,
                    IndexingServerConfiguration.PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_KICK_FRACTION_DEFAULT);
            final boolean vectorCompactionLocalEnabledWithOptimizer = config.getBoolean(
                    IndexingServerConfiguration
                            .PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_ENABLED_WITH_OPTIMIZER,
                    IndexingServerConfiguration
                            .PROPERTY_VECTOR_INDEX_COMPACTION_LOCAL_ENABLED_WITH_OPTIMIZER_DEFAULT);
            // Streaming compaction (issue #485): config key takes precedence over
            // the herddb.vectorindex.streamingCompactionEnabled system property
            // at IS startup. The flag is process-wide because the optimizer-pod
            // path (RemoteSegmentGraphMerger) consults the same static.
            final boolean vectorCompactionStreamingEnabled = config.getBoolean(
                    IndexingServerConfiguration
                            .PROPERTY_VECTOR_INDEX_COMPACTION_STREAMING_ENABLED,
                    IndexingServerConfiguration
                            .PROPERTY_VECTOR_INDEX_COMPACTION_STREAMING_ENABLED_DEFAULT);
            herddb.indexing.vector.PersistentVectorStore.setStreamingCompactionEnabled(
                    vectorCompactionStreamingEnabled);
            LOGGER.log(Level.INFO,
                    "vector index compaction: tieredEnabled={0}, backpressureSegments={1}, "
                            + "backpressureMaxWaitMs={2}, localKickFraction={3},"
                            + " localEnabledWithOptimizer={4}, streamingEnabled={5},"
                            + " microSegmentMaxNodes={6}, maxInputs={7},"
                            + " maxInputBytes={8}",
                    new Object[]{vectorCompactionTieredEnabled, vectorCompactionBackpressureSegments,
                            vectorCompactionBackpressureMaxWaitMs,
                            vectorCompactionLocalKickFraction,
                            vectorCompactionLocalEnabledWithOptimizer,
                            vectorCompactionStreamingEnabled,
                            vectorCompactionMicroSegmentMaxNodes,
                            vectorCompactionMaxInputs,
                            vectorCompactionMaxInputBytes});
            // Async IO pipeline for FusedPQ search (issue #547).
            // Config key takes precedence over the system property.
            final boolean vectorSearchAsyncPipelineEnabled = config.getBoolean(
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEARCH_ASYNC_PIPELINE_ENABLED,
                    Boolean.getBoolean(
                            IndexingServerConfiguration.SYSPROP_VECTOR_SEARCH_ASYNC_PIPELINE_ENABLED));
            herddb.indexing.vector.PersistentVectorStore.setSearchAsyncPipelineEnabled(
                    vectorSearchAsyncPipelineEnabled);
            LOGGER.log(Level.INFO, "vector search async IO pipeline (issue #547): enabled={0}",
                    vectorSearchAsyncPipelineEnabled);
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
            // Frontier (pinned) region budget (issue #578).
            // Default 0 → auto-size as 10% of the main cache budget.
            long frontierMaxBytes = config.getLong(
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_FRONTIER_MAX_BYTES,
                    IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_PAGE_CACHE_FRONTIER_MAX_BYTES_DEFAULT);
            if (frontierMaxBytes == 0 && segmentPageCacheMaxBytes > 0) {
                frontierMaxBytes = segmentPageCacheMaxBytes / 10;
            }
            if (frontierMaxBytes < 0) {
                frontierMaxBytes = 0; // explicit disable
            }
            this.segmentBlockCache = segmentPageCacheMaxBytes > 0
                    ? new SegmentBlockCache(segmentPageCacheMaxBytes, frontierMaxBytes)
                    : SegmentBlockCache.disabled();
            LOGGER.log(Level.INFO,
                    "vector index segmentPageCacheMaxBytes: {0} (active={1}), "
                            + "frontierMaxBytes: {2} (active={3})",
                    new Object[]{segmentPageCacheMaxBytes, segmentBlockCache.isActive(),
                            frontierMaxBytes, segmentBlockCache.isFrontierCacheActive()});
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

            final long vectorMemLimit = maxVectorMemoryBytes;
            final VectorMemoryBudget budget = this;
            final long finalSegmentPageCacheMaxBytes = segmentPageCacheMaxBytes;
            final long finalFrontierMaxBytes = frontierMaxBytes;
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
                        /*minCount*/ PersistentVectorStore.DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT,
                        vectorCompactionMaxCount,
                        vectorCompactionRetentionMs);
                store.setTieredCompactionEnabled(vectorCompactionTieredEnabled);
                store.setCompactionMicroSegmentMaxNodes(vectorCompactionMicroSegmentMaxNodes);
                store.setCompactionMaxInputs(vectorCompactionMaxInputs);
                // Issue #602: per-cycle download budget cap.
                store.setCompactionMaxInputBytes(vectorCompactionMaxInputBytes);
                store.setCompactionBackpressureThreshold(vectorCompactionBackpressureSegments);
                store.setCompactionBackpressureMaxWaitMs(vectorCompactionBackpressureMaxWaitMs);
                store.setLocalCompactionKickFraction(vectorCompactionLocalKickFraction);
                store.setLocalCompactionEnabledWithOptimizer(
                        vectorCompactionLocalEnabledWithOptimizer);
                // Issue #569: hand the warmup byte budget to the store so it can
                // warm each new segment's block cache AT CREATION TIME (in the
                // checkpoint Phase C-prep and compaction merge paths), before the
                // segment becomes searchable. The post-checkpoint warm-all below
                // (submitWarmupAsyncOrInline) then degenerates to an idempotent
                // no-op in steady state, breaking the warmup→checkpoint→warmup
                // death spiral. `warmupBytesPerSegment` is resolved earlier in
                // start(); the factory lambda runs later (tailer-driven), so the
                // field is always populated by the time this executes.
                store.setWarmupBytesPerSegment(this.warmupBytesPerSegment);
                // Issue #578: set the frontier-pin BFS budget:
                //   -1  = mirror warmupBytesPerSegment (the default sentinel in
                //         PersistentVectorStore); used when the frontier region is
                //         active so the pin BFS covers the same number of nodes as
                //         the main warmup BFS.
                //    0  = disable pin BFS entirely; used when no frontier budget
                //         has been allocated (finalFrontierMaxBytes <= 0).
                store.setPinBytesPerSegment(finalFrontierMaxBytes > 0 ? -1L : 0L);
                // Issue #491: when the external index-optimizer is enabled cluster-wide
                // (indexing.optimizer.enabled=true) AND the metadata storage manager is
                // ZK-backed, attach a SegmentRegistryPublisher BEFORE start() so that:
                //   - the reconcile sweep at start() heals any partial-publish state
                //     left by a previous crash;
                //   - every subsequent successful checkpoint stages PROVISIONAL znodes
                //     and commits them to ACTIVE — making the segments visible to the
                //     IndexOptimizerEngine;
                //   - the IS-local compaction loop flips into pressure-driven mode
                //     (kickFraction × backpressure cap) instead of being the sole
                //     driver, leaving steady-state compaction to the optimizer.
                // Both setSegmentPublisher and setExternalCompactionEnabled MUST be
                // called before start() — see their Javadoc. Reading the engine fields
                // at lambda-invocation time is safe because the tailer (the only caller
                // of the factory) starts AFTER tableSpaceUUID is resolved and AFTER
                // segmentRegistry is set in start().
                SegmentRegistryClient registrySnapshot = this.segmentRegistry;
                if (registrySnapshot != null && tableSpaceUUID != null) {
                    SegmentRegistryPublisher publisher = new SegmentRegistryPublisher(
                            registrySnapshot, tableSpaceUUID, tableName,
                            autoIndexUUID, indexName, instanceId);
                    store.setSegmentPublisher(publisher);
                    store.setExternalCompactionEnabled(true);
                    LOGGER.log(Level.INFO,
                            "PersistentVectorStore {0} (table={1}, indexUuid={2}) wired with"
                                    + " SegmentRegistryPublisher; external compaction enabled",
                            new Object[]{indexName, tableName, autoIndexUUID});
                }
                try {
                    store.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start PersistentVectorStore " + indexName, e);
                }

                // Issue #514: arm a SegmentAssignmentWatcher so this store
                // automatically adopts optimizer-merged segments and drops
                // deprecated inputs without requiring a restart.
                // The watcher is created AFTER store.start() so the store is
                // fully initialised (dimension known, reconcile done) before
                // the initial scan fires.
                SegmentRegistryClient watcherRegistry = this.segmentRegistry;
                if (watcherRegistry != null && tableSpaceUUID != null) {
                    final herddb.indexing.vector.AbstractVectorStore finalStore = store;
                    final String finalIndexUUID = autoIndexUUID;
                    final String finalIndexName = indexName;
                    herddb.indexing.segment.SegmentAssignmentWatcher watcher =
                            new herddb.indexing.segment.SegmentAssignmentWatcher(
                                    watcherRegistry, instanceId,
                                    new herddb.indexing.segment.SegmentAssignmentListener() {
                                        @Override
                                        public void onSegmentAssigned(
                                                herddb.indexing.segment.VersionedSegmentMetadata vsm) {
                                            segmentAssignmentMetrics.onSegmentAssigned(vsm);
                                            herddb.indexing.segment.SegmentMetadata m = vsm.metadata();
                                            if (m.getMapFileSize() <= 0L) {
                                                // Segment znode was written without mapFileSize
                                                // (pre-issue-#484 format). Skip adoption — the
                                                // segment cannot be read without a valid map size.
                                                LOGGER.log(Level.WARNING,
                                                        "Skipping adoption of segment "
                                                                + m.getSegmentUuid()
                                                                + " (mapFileSize="
                                                                + m.getMapFileSize()
                                                                + " <= 0): znode predates issue #484 fix");
                                                return;
                                            }
                                            if (m.getSizeBytes() <= m.getMapFileSize()) {
                                                // Corrupted or legacy znode: sizeBytes must be
                                                // strictly larger than mapFileSize because
                                                // sizeBytes = graphFileSize + mapFileSize and
                                                // graphFileSize must be > 0. A zero or negative
                                                // graphFileSize would corrupt the multipart read.
                                                LOGGER.log(Level.WARNING,
                                                        "Skipping adoption of segment "
                                                                + m.getSegmentUuid()
                                                                + " (sizeBytes=" + m.getSizeBytes()
                                                                + " <= mapFileSize=" + m.getMapFileSize()
                                                                + "): znode has invalid or zero graphFileSize");
                                                return;
                                            }
                                            if (m.getGraphPath() == null
                                                    || m.getGraphPath().isEmpty()
                                                    || m.getMapPath() == null
                                                    || m.getMapPath().isEmpty()) {
                                                // Defensive guard: a znode without a graph or map
                                                // path (e.g. a partial publish interrupted mid-write)
                                                // would reach adoptExternalSegment and trigger an
                                                // IllegalStateException in loadFusedPQSegment.
                                                // Skip adoption and surface a WARNING instead.
                                                LOGGER.log(Level.WARNING,
                                                        "Skipping adoption of segment "
                                                                + m.getSegmentUuid()
                                                                + ": znode has null or empty "
                                                                + "graphPath/mapPath");
                                                return;
                                            }
                                            boolean adoptedOk = false;
                                            try {
                                                finalStore.adoptExternalSegment(
                                                        m.getSegmentUuid(),
                                                        m.getSegmentId(),
                                                        m.getGraphPath(),
                                                        m.getSizeBytes() - m.getMapFileSize(),
                                                        m.getMapPath(),
                                                        m.getMapFileSize(),
                                                        m.getGeneration());
                                                adoptedOk = true;
                                            } catch (java.io.IOException
                                                    | herddb.storage.DataStorageManagerException e) {
                                                LOGGER.log(Level.WARNING,
                                                        "Failed to adopt external segment "
                                                                + m.getSegmentUuid()
                                                                + " into store " + finalIndexUUID, e);
                                            }
                                            // Issue #555: signal that this IS pod has loaded the
                                            // segment (or has it loaded already). The optimizer
                                            // waits for one of these ephemeral znodes from each
                                            // interested pod before committing the atomic swap.
                                            // Idempotent on NodeExists. Skipped when adoption
                                            // failed because asserting "I have this segment"
                                            // would be a lie.
                                            if (adoptedOk) {
                                                String serviceId = getInstanceIdLabel();
                                                if (serviceId == null || serviceId.isEmpty()) {
                                                    LOGGER.log(Level.WARNING,
                                                            "issue #555: cannot create swap-ack znode "
                                                                    + "for segment {0}: instanceIdLabel "
                                                                    + "is not set",
                                                            m.getSegmentUuid());
                                                } else {
                                                    try {
                                                        watcherRegistry.createSwapAckNode(
                                                                m.getSegmentUuid(), serviceId);
                                                    } catch (herddb.indexing.segment.SegmentRegistryException ackFailed) {
                                                        LOGGER.log(Level.WARNING,
                                                                "issue #555: failed to write swap-ack "
                                                                        + "for segment {0} from {1}: {2}",
                                                                new Object[]{
                                                                        m.getSegmentUuid(),
                                                                        serviceId,
                                                                        ackFailed.getMessage()});
                                                    }
                                                }
                                            }
                                        }

                                        @Override
                                        public void onSegmentReleased(
                                                herddb.indexing.segment.SegmentMetadata previous) {
                                            if (previous == null) {
                                                // Defence-in-depth: scanIndex now always passes
                                                // the cached metadata for znode-gone events, but
                                                // guard here in case a future code path differs.
                                                LOGGER.log(Level.WARNING,
                                                        "onSegmentReleased called with null metadata"
                                                                + " for store " + finalIndexUUID
                                                                + "; cannot drop by UUID");
                                                return;
                                            }
                                            finalStore.dropSegmentByUuid(previous.getSegmentUuid());
                                            segmentAssignmentMetrics.onSegmentReleased(previous);
                                        }

                                        @Override
                                        public void onPendingAssignment(
                                                herddb.indexing.segment.VersionedSegmentMetadata vsm) {
                                            segmentAssignmentMetrics.onPendingAssignment(vsm);
                                        }
                                    });
                    String watcherKey = storeKey(tableName, indexName);
                    try {
                        watcher.watchIndex(tableSpaceUUID, autoIndexUUID);
                        // Startup reconcile: drop any adopted (external-storage-key)
                        // segments that are not present in ZK. This handles the case
                        // where the IS was down while the optimizer expired and deleted
                        // a segment whose UUID is no longer in the registry. Without
                        // the reconcile, the orphaned segment stays in the store
                        // indefinitely and will fail at search time if the optimizer
                        // has also deleted its multipart files.
                        finalStore.reconcileAdoptedSegments(
                                watcher.snapshotKnownSegments().keySet());
                        segmentWatchers.put(watcherKey, watcher);
                        LOGGER.log(Level.INFO,
                                "SegmentAssignmentWatcher armed for store {0} (indexUuid={1})",
                                new Object[]{watcherKey, finalIndexUUID});
                    } catch (Exception e) {
                        // Broad catch is intentional: both SegmentRegistryException (from
                        // watchIndex) and unchecked RuntimeException (e.g. from
                        // reconcileAdoptedSegments → dropSegmentByUuid → BLink close)
                        // must close the watcher so its background refresh executor is
                        // stopped and it does not leak outside segmentWatchers.
                        LOGGER.log(Level.WARNING,
                                "Failed to arm SegmentAssignmentWatcher for store " + finalIndexName
                                        + " (indexUuid=" + finalIndexUUID
                                        + "); external segment adoption disabled for this store", e);
                        watcher.close();
                    }
                }
                return store;
            };

        } else {
            LOGGER.info("Using InMemoryVectorStore factory (storage type: " + storageType + ")");
        }

        // Resolve the post-Phase-C cache warmup byte budget (issue #322) and
        // the async/sync warmup mode (issue #472), outside the storage-type
        // if-branch so tests that inject a custom factory producing a
        // PersistentVectorStore (with storage_type=memory) honour the same
        // configuration as the production remote-file path. The production
        // in-memory mode is unaffected because InMemoryVectorStore is
        // filtered out by the `instanceof PersistentVectorStore` check in
        // submitWarmupAsyncOrInline().
        // Priority: properties-file key > JVM system property > hard-coded default.
        long syspropWarmupBytes = Long.getLong(
                IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES_DEFAULT);
        this.warmupBytesPerSegment = config.getLong(
                IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_BYTES,
                syspropWarmupBytes);
        String syspropWarmupAsync = System.getProperty(
                IndexingServerConfiguration.SYSPROP_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC);
        boolean defaultWarmupAsync = syspropWarmupAsync != null
                ? Boolean.parseBoolean(syspropWarmupAsync)
                : IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC_DEFAULT;
        this.warmupAsync = config.getBoolean(
                IndexingServerConfiguration.PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC,
                defaultWarmupAsync);
        LOGGER.log(Level.INFO,
                "vector index segmentCacheWarmupBytes: {0} ({1}, mode={2})",
                new Object[]{warmupBytesPerSegment,
                        warmupBytesPerSegment > 0 ? "enabled" : "disabled",
                        warmupAsync ? "async" : "sync"});

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
                        FastThreadLocalThread t = new FastThreadLocalThread(r, "indexing-apply-worker-" + idx);
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
            FastThreadLocalThread t = new FastThreadLocalThread(r, "indexing-checkpoint");
            t.setDaemon(true);
            return t;
        });

        // Single-thread executor that runs the post-Phase-C BFS warmup off the
        // checkpointExecutor thread (issue #472). Created only when warmup is
        // enabled AND in async mode, so a sync-mode engine pays no executor
        // cost. The warmup thread is daemon so it never blocks JVM exit; on
        // close() it is shut down with a bounded awaitTermination, then
        // shutdownNow.
        if (warmupBytesPerSegment > 0 && warmupAsync) {
            warmupExecutor = Executors.newSingleThreadExecutor(r -> {
                FastThreadLocalThread t = new FastThreadLocalThread(r, "indexing-warmup");
                t.setDaemon(true);
                return t;
            });
        }

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

        // Issue #491: wire the segmented-v2 ZK registry when the external
        // index-optimizer is enabled. Done here (after metadataStorageManager
        // is started, before the vector-store factory ever runs) so that
        // every store created by the production factory observes a non-null
        // registry handle and attaches a SegmentRegistryPublisher. Without
        // this wiring the optimizer pod observes zero indexes because no
        // checkpoint ever publishes its segments to ZK.
        boolean indexOptimizerEnabled = config.getBoolean(
                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED,
                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED_DEFAULT);
        if (indexOptimizerEnabled) {
            if (metadataStorageManager instanceof ZookeeperMetadataStorageManager) {
                ZookeeperMetadataStorageManager zkMeta =
                        (ZookeeperMetadataStorageManager) metadataStorageManager;
                SegmentRegistryClient registry = new SegmentRegistryClient(
                        zkMeta::getZooKeeper, zkMeta.getBasePath());
                try {
                    registry.ensureRoot();
                } catch (SegmentRegistryException e) {
                    // Registry root creation is part of cluster bootstrap; if it
                    // fails the optimizer cannot work either way. Surface as a
                    // checked failure rather than silently dropping the wiring.
                    throw new IOException(
                            "Failed to ensure segment registry root in ZK at "
                                    + zkMeta.getBasePath()
                                    + SegmentRegistryClient.REGISTRY_SUBPATH, e);
                }
                this.segmentRegistry = registry;
                LOGGER.log(Level.INFO,
                        "Segment registry wired at {0}; vector index segments will be"
                                + " published to ZK and the IS-local compaction loop will run"
                                + " in pressure-driven fallback mode (indexing.optimizer.enabled=true)",
                        registry.getRegistryRootPath());
            } else {
                // Standalone mode (FileMetadataStorageManager) plus
                // optimizer.enabled=true is a misconfiguration: the optimizer
                // pod requires ZooKeeper. Log loud and continue without a
                // registry — production deployments will notice the warning
                // in the IS startup logs and either remove the property or
                // switch to cluster mode.
                LOGGER.log(Level.WARNING,
                        "{0}=true but metadata storage manager is {1} (not ZK-backed);"
                                + " the segment registry cannot be wired and the external"
                                + " optimizer will see zero indexes. Either set server.mode=cluster"
                                + " or unset {0}.",
                        new Object[]{
                                IndexingServerConfiguration.PROPERTY_INDEX_OPTIMIZER_ENABLED,
                                metadataStorageManager.getClass().getName()});
            }
        }

        // Resolve the tablespace name to a UUID (the engine's storage namespace).
        String tablespaceName = config.getString(IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_NAME_DEFAULT);
        String configuredTableSpaceUuid = config.getString(
                IndexingServerConfiguration.PROPERTY_TABLESPACE_UUID,
                IndexingServerConfiguration.PROPERTY_TABLESPACE_UUID_DEFAULT);
        if (!configuredTableSpaceUuid.isEmpty()) {
            // Explicit override — also lets push-mode siblings and the index
            // optimizer be pinned to one storage namespace from config.
            this.tableSpaceUUID = configuredTableSpaceUuid;
            LOGGER.log(Level.INFO,
                    "Using explicitly configured tablespace UUID ''{0}'' for tablespace ''{1}''",
                    new Object[]{tableSpaceUUID, tablespaceName});
        } else if (isPushModeConfigured()) {
            // Push mode (testing only): there may be no HerdDB server to
            // register the tablespace in the metadata store, so do NOT block
            // for up to 30 minutes. Try a single lookup; if the tablespace is
            // absent, derive a deterministic UUID from its name so restarts
            // and sibling instances resolve the same storage namespace.
            TableSpace tableSpace = metadataStorageManager.describeTableSpace(tablespaceName);
            if (tableSpace != null) {
                this.tableSpaceUUID = tableSpace.uuid;
                LOGGER.log(Level.INFO,
                        "Push mode: resolved tablespace ''{0}'' to UUID ''{1}'' from metadata",
                        new Object[]{tablespaceName, tableSpaceUUID});
            } else {
                this.tableSpaceUUID = deterministicTableSpaceUuid(tablespaceName);
                LOGGER.log(Level.INFO,
                        "Push mode: tablespace ''{0}'' is not registered in the metadata store; "
                                + "using UUID ''{1}'' derived from the tablespace name",
                        new Object[]{tablespaceName, tableSpaceUUID});
            }
        } else {
            // file / bookkeeper mode: a HerdDB server owns the tablespace.
            // Poll until it becomes available or the timeout expires.
            long pollIntervalMs = config.getLong(
                    IndexingServerConfiguration.PROPERTY_TABLESPACE_WAIT_POLL_INTERVAL_MS,
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
                    throw new RuntimeException("Timed out after " + tablespaceWaitTimeoutMs
                            + "ms waiting for tablespace '" + tablespaceName + "' to become available");
                }
                LOGGER.log(Level.INFO, "Tablespace ''{0}'' not yet available, retrying in {1}ms...",
                        new Object[]{tablespaceName, pollIntervalMs});
                Thread.sleep(pollIntervalMs);
            }
            this.tableSpaceUUID = tableSpace.uuid;
            LOGGER.log(Level.INFO, "Resolved tablespace name ''{0}'' to UUID ''{1}''",
                    new Object[]{tablespaceName, tableSpaceUUID});
        }

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
        // Re-hydrate the durable freshness timestamp from the snapshot so
        // dashboards can compute "durable_lag_ms" immediately after a
        // restart, without waiting for the next successful checkpoint.
        // Stays 0 ("unknown") for START_OF_TIME. Issue #423.
        lastDurableEntryTimestamp = snapshot.lastEntryTimestamp;
        // The tailer freshness clock starts at the durable freshness — the
        // engine will replay entries from `watermark` onward, but until the
        // first replay tick advances `lastProcessedLsn` we report the same
        // freshness as the durable state (rather than 0).
        lastProcessedEntryTimestamp = snapshot.lastEntryTimestamp;
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
        } else if (IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH.equals(logType)) {
            // Testing-only push mode: entries arrive over the PushEntries gRPC
            // RPC instead of a file/BookKeeper log, so no HerdDB server and no
            // materialised commit log are required. Recovery is otherwise
            // unchanged — checkpointed segments and the schema snapshot are
            // reloaded above, tailerStart is the durable watermark, and the
            // push tailer simply resumes from there (skipping any re-pushed
            // stale entries).
            int pushBufferCapacity = config.getInt(
                    IndexingServerConfiguration.PROPERTY_LOG_PUSH_BUFFER_CAPACITY,
                    IndexingServerConfiguration.PROPERTY_LOG_PUSH_BUFFER_CAPACITY_DEFAULT);
            LOGGER.log(Level.INFO,
                    "Creating PushCommitLogTailer (testing-only push mode), "
                            + "bufferCapacity={0}, tailerStart={1}",
                    new Object[]{pushBufferCapacity, tailerStart});
            PushCommitLogTailer push = new PushCommitLogTailer(
                    pushBufferCapacity, tailerStart, this::processEntry);
            this.pushTailer = push;
            tailer = push;
        } else {
            tailer = new FileCommitLogTailer(logDirectory, tableSpaceUUID, tailerStart, this::processEntry);
        }
        tailerThread = new FastThreadLocalThread(tailer, "indexing-service-tailer");
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

    /**
     * Returns the push-mode tailer when the engine is running with
     * {@code indexing.log.type=push}, or {@code null} otherwise. Used by the
     * {@code PushEntries} gRPC handler to enqueue client-pushed entries into
     * the bounded buffer.
     */
    public PushCommitLogTailer getPushTailer() {
        return pushTailer;
    }

    /**
     * Returns {@code true} when this engine is configured for push mode
     * ({@code indexing.log.type=push}), regardless of whether {@link #start()}
     * has completed. Lets the {@code PushEntries} gRPC handler distinguish
     * "still starting" (retryable) from "not a push-mode service" (permanent):
     * the gRPC server binds before {@link #start()} assigns {@link #pushTailer},
     * so {@link #getPushTailer()} can be {@code null} during a normal boot.
     */
    public boolean isPushModeConfigured() {
        return IndexingServerConfiguration.PROPERTY_LOG_TYPE_PUSH.equals(
                config.getString(IndexingServerConfiguration.PROPERTY_LOG_TYPE,
                        IndexingServerConfiguration.PROPERTY_LOG_TYPE_DEFAULT));
    }

    /**
     * Issue #491: returns the segmented-v2 ZK registry handle wired during
     * {@link #start()}, or {@code null} when {@link IndexingServerConfiguration#PROPERTY_INDEX_OPTIMIZER_ENABLED}
     * was unset / false at startup OR when the metadata storage manager was
     * non-ZK at startup. Visible for tests; production code does not need to
     * access the registry directly (the production factory does).
     */
    SegmentRegistryClient getSegmentRegistry() {
        return segmentRegistry;
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

    /**
     * Number of read batches the underlying tailer has completed since this
     * engine started (issue #459). One batch is one poll/follow cycle that
     * processed at least one entry. Returns {@code 0} when the tailer is not
     * yet started.
     */
    public long getTailerBatchesProcessed() {
        CommitLogTailing t = tailer;
        return t != null ? t.getBatchesProcessed() : 0L;
    }

    /** Issue #459: entries the tailer counted as "accepted" (would mutate the HNSW graph). */
    public long getTailerEntriesAccepted() {
        return tailerEntriesAccepted.sum();
    }

    /** Issue #459: entries the tailer counted as "skipped" (DDL, NOOP, REBALANCE, transactional control). */
    public long getTailerEntriesSkipped() {
        return tailerEntriesSkipped.sum();
    }

    /**
     * Issue #463: INSERT entries this replica did not apply because every
     * vector index defined on the entry's table rejected the key via the
     * shard filter. Operators verify the filter is doing its job by
     * watching this counter rise — for a balanced {@code numShards} ≥
     * {@code numInstances} configuration it should track roughly
     * {@code (numInstances - 1) / numInstances} of {@link #getTailerInserts()}.
     *
     * <p>The counter excludes:
     * <ul>
     *   <li>INSERTs whose table has no vector index (different reason —
     *       not a shard-filter rejection);</li>
     *   <li>UPDATE / DELETE entries (UPDATE always does a broadcast remove
     *       on every replica, DELETE is broadcast unconditionally — neither
     *       is "filtered out" the same way an INSERT can be).</li>
     * </ul>
     */
    public long getTailerEntriesShardFiltered() {
        return tailerEntriesShardFiltered.sum();
    }

    /** Issue #459: INSERT entries the tailer has classified. */
    public long getTailerInserts() {
        return tailerInserts.sum();
    }

    /** Issue #459: UPDATE entries the tailer has classified. */
    public long getTailerUpdates() {
        return tailerUpdates.sum();
    }

    /** Issue #459: DELETE entries the tailer has classified. */
    public long getTailerDeletes() {
        return tailerDeletes.sum();
    }

    /** Issue #459: DDL entries the tailer has classified. */
    public long getTailerDdl() {
        return tailerDdl.sum();
    }

    public boolean isTailerRunning() {
        CommitLogTailing t = tailer;
        return t != null && t.isRunning();
    }

    public LogSequenceNumber getLastProcessedLsn() {
        return lastProcessedLsn;
    }

    /**
     * Wall-clock timestamp (epoch ms) of the LogEntry at
     * {@link #getLastProcessedLsn()}. {@code 0} means "unknown" (no entries
     * processed yet on a fresh engine). Used by {@code GetIndexStatus} so
     * dashboards can compute {@code tailer_lag_ms = now - timestamp}
     * (issue #423).
     */
    public long getLastProcessedEntryTimestamp() {
        return lastProcessedEntryTimestamp;
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
     * Wall-clock timestamp (epoch ms) of the LogEntry at
     * {@link #getLastDurableLsn()}. {@code 0} means "unknown" (no successful
     * checkpoint yet). Used by {@code GetIndexStatus} so dashboards can
     * compute {@code durable_lag_ms = now - timestamp} (issue #423).
     */
    public long getLastDurableEntryTimestamp() {
        return lastDurableEntryTimestamp;
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
        if (engineStatus == EngineStatus.FAILED) {
            // Issue #471: the engine has been put into FAILED state by an
            // unrecoverable apply-time error (typically a rebuild=true
            // CREATE VECTOR INDEX whose IS-side back-fill threw). Do
            // NOT advance lastProcessedLsn or entriesSinceLastCheckpoint
            // — the watermark must stay anchored at the position before
            // the failed entry so the rebuild is replayed from scratch
            // on engine restart. We log at FINE not SEVERE: the SEVERE
            // log was already emitted at the moment of the failure;
            // repeating it on every subsequent entry would flood the
            // log without adding information.
            LOGGER.log(Level.FINE,
                    "engine FAILED — dropping entry at LSN {0}, type={1} (replay on restart)",
                    new Object[]{lsn, entry.type});
            return;
        }
        try {
            LOGGER.log(Level.FINEST, "Processing entry at LSN {0}, type={1}, txId={2}",
                    new Object[]{lsn, entry.type, entry.transactionId});
            // Classify per-operation type for the issue-#459 metrics.
            // Done unconditionally on every entry the tailer hands us, before
            // the BEGIN/COMMIT/ROLLBACK fan-out below — buffered DML entries
            // are counted at original arrival time, not at COMMIT replay time.
            classifyForMetrics(entry);
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
            // Track the LogEntry wall-clock so diagnostic tooling can report
            // "tailer_lag_ms = now - lastProcessedEntryTimestamp" (issue #423).
            // Note: read separately from lastProcessedLsn under no lock — a
            // sub-microsecond race between the two volatile writes is
            // acceptable for a diagnostic measured in seconds.
            if (entry.timestamp > 0L) {
                lastProcessedEntryTimestamp = entry.timestamp;
            }
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

    /**
     * Replays the buffered entries of a transaction whose
     * {@code COMMITTRANSACTION} we just observed.
     *
     * <p>Issue #459: each buffered entry was already classified by
     * {@link #classifyForMetrics(LogEntry)} when it first arrived from the
     * tailer (back when {@code processEntry()} placed it in the
     * {@link TransactionBuffer}); replaying through {@code applySingleEntry}
     * here MUST NOT re-classify, otherwise every transactional INSERT/UPDATE/
     * DELETE would silently double-count.
     */
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
            // Segmented-v2 registry sweep: when a publisher is wired, drop
            // every registry znode for this index BEFORE we delete the
            // multipart files. Doing it in this order means a crash between
            // the two steps leaves orphan multipart files (the optimizer's
            // reaper or a future restart sweep can catch them) rather than
            // orphan registry znodes that point at files no longer on disk
            // (which would cause other IS instances to observe phantom
            // segments and attempt failed ownership transfers).
            if (store instanceof PersistentVectorStore) {
                try {
                    ((PersistentVectorStore) store).dropAllRegistryEntries();
                } catch (RuntimeException e) {
                    // dropAllRegistryEntries is itself best-effort and never
                    // throws back to here, but defend in depth — a registry
                    // sweep failure must not block the multipart cleanup.
                    LOGGER.log(Level.WARNING,
                            "DROP cleanup for " + storeKeyForLog
                                    + ": registry sweep failed (orphan znodes will be reaped"
                                    + " by next reconcile or optimizer pass)",
                            e);
                }
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
            // Prune already-completed entries and add the new one under
            // the same lock used by awaitPendingDeletionsForTest, so a
            // test waiting on the snapshot never observes a partially-
            // pruned state. Pruning prevents unbounded growth under
            // workloads that DROP many indexes — each retained Future
            // would otherwise pin its captured AbstractVectorStore
            // (issue #383 review).
            synchronized (pendingDropTasks) {
                pendingDropTasks.removeIf(Future::isDone);
                pendingDropTasks.add(f);
            }
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Executor shut down between the isShutdown() probe and submit()
            // — fall back to inline execution.
            task.run();
        }
    }

    /**
     * Test hook: drains the shadow reload executor so any reload tasks
     * already enqueued have completed before the caller observes
     * {@link #getShadowReloadCount()} or {@link #getShadowLoadedLsn()}.
     * Submitted as a no-op task that {@code get()}s after every queued
     * reload finishes, because the executor is single-threaded.
     *
     * <p>No-op when not running as a shadow.
     */
    public void awaitShadowReloadsForTest() {
        ExecutorService exec = shadowReloadExecutor;
        if (exec == null) {
            return;
        }
        try {
            exec.submit(() -> {
            }).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted awaiting shadow reload", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Shadow reload barrier task failed", e.getCause());
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuntimeException("Timed out awaiting shadow reload barrier", e);
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

    /**
     * Issue #509: eager DROP trigger called by the IS gRPC server when the
     * HerdDB server issues a {@code DropIndex} RPC on DROP TABLE / DROP INDEX,
     * without waiting for the commit-log tailer to process the matching
     * {@code DROP_INDEX} log entry.
     *
     * <p>Removes the vector store from the in-memory {@link #vectorStores} and
     * {@link #vectorStoreIndexUuids} maps using the caller-supplied
     * {@code (table, indexName)} key — no {@link SchemaTracker} access is
     * needed, which keeps this method safe to call from the gRPC server thread
     * (the tailer owns {@code schemaTracker}; the two maps are
     * {@link java.util.concurrent.ConcurrentHashMap}s and are safe to mutate
     * from any thread).
     *
     * <p>If a store was found, it is handed to
     * {@link #submitVectorStoreDeletion} which:
     * <ol>
     *   <li>Calls {@code store.close()} to release in-memory resources.</li>
     *   <li>Calls {@code store.dropAllRegistryEntries()} to sweep the ZK
     *       segment-registry entries (fast — a handful of znode deletes even
     *       for a 20B-vector index).</li>
     *   <li>Calls {@code dataStorageManager.dropIndex()} to delete the
     *       on-disk / MinIO segment files (may be slow; runs in the
     *       background {@code checkpointExecutor} so the RPC returns
     *       before file deletion completes).</li>
     * </ol>
     *
     * <p>Idempotent: if the store is not tracked (already removed by a
     * previous eager call or by the tailer's own DROP_INDEX path), this
     * is a harmless no-op.
     *
     * @param table     the table name as used in {@link #storeKey}
     * @param indexName the index name as used in {@link #storeKey}
     */
    public void dropIndexImmediate(String table, String indexName, String requestedUuid) {
        String k = storeKey(table, indexName);
        // Atomically inspect and, if appropriate, remove the store from
        // vectorStores.  The UUID gate prevents data loss on DROP+CREATE cycles:
        // if the IS tailer has already processed both the DROP_INDEX and a
        // subsequent CREATE_INDEX for the same (table, indexName), the
        // currently-tracked store has a different UUID and must not be removed.
        //
        // We do NOT remove from vectorStoreIndexUuids here: the tailer's own
        // DROP_INDEX path will do that when it catches up, avoiding a secondary
        // race where a concurrent CREATE_INDEX could put a new UUID in the map
        // between our compute() and a separate vectorStoreIndexUuids.remove().
        //
        // An empty/null requestedUuid skips the UUID gate — safe fallback for
        // IS clients built before this field was added (rolling upgrades).
        AbstractVectorStore[] toDelete = {null};
        vectorStores.compute(k, (key, currentStore) -> {
            if (currentStore == null) {
                // Already gone — tailer cleaned it up or it was never tracked.
                return null;
            }
            if (requestedUuid != null && !requestedUuid.isEmpty()) {
                String currentUuid = vectorStoreIndexUuids.get(key);
                if (currentUuid == null) {
                    // UUID not yet written — createVectorStoreIfNeeded writes UUID
                    // before the store, so if a UUID is absent here the store entry
                    // is stale from before the first UUID gate was introduced, or
                    // we are in an unexpected state. Refuse to delete: conservatively
                    // keep the store and let the tailer clean up (positive-match policy).
                    LOGGER.log(Level.INFO,
                            "dropIndexImmediate: key {0} has no tracked UUID "
                                    + "(requested uuid={1}); refusing to remove — "
                                    + "tailer will handle cleanup",
                            new Object[]{key, requestedUuid});
                    return currentStore; // keep
                }
                if (!currentUuid.equals(requestedUuid)) {
                    // UUID mismatch: the IS has already processed both the DROP and
                    // a subsequent CREATE_INDEX — the new store must not be removed.
                    LOGGER.log(Level.INFO,
                            "dropIndexImmediate: key {0} already replaced by uuid={1} "
                                    + "(requested uuid={2}); no-op "
                                    + "(DROP+CREATE race resolved correctly)",
                            new Object[]{key, currentUuid, requestedUuid});
                    return currentStore; // keep the new store
                }
            }
            toDelete[0] = currentStore;
            return null; // remove atomically
        });

        if (toDelete[0] != null) {
            LOGGER.log(Level.INFO,
                    "dropIndexImmediate: submitting eager deletion for store key {0} "
                            + "(triggered by HerdDB server DropIndex RPC, issue #509)",
                    k);
            submitVectorStoreDeletion(k, toDelete[0]);
        } else {
            LOGGER.log(Level.FINE,
                    "dropIndexImmediate: store key {0} not removed (already dropped, "
                            + "never seen, or UUID mismatch — tailer will handle it); no-op",
                    k);
        }
    }

    /**
     * Outcome of {@link #deleteSegment(String, String, String, boolean, boolean)}.
     * Mirrors the wire fields of {@code DeleteSegmentResponse} so the gRPC
     * handler is a thin translation layer.
     */
    public static final class DeleteSegmentResult {
        public final String segment;
        public final boolean removed;
        public final long vectorsLost;
        public final boolean graphFilePresent;
        public final boolean storagePurged;

        public DeleteSegmentResult(String segment, boolean removed, long vectorsLost,
                                   boolean graphFilePresent, boolean storagePurged) {
            this.segment = segment;
            this.removed = removed;
            this.vectorsLost = vectorsLost;
            this.graphFilePresent = graphFilePresent;
            this.storagePurged = storagePurged;
        }
    }

    /**
     * Thrown by {@link #deleteSegment} when the request must be rejected
     * without mutating state: the index or store is not loaded, the
     * segment is not registered, or the segment's graph file is still
     * present in remote storage and {@code force == false}.
     */
    public static final class DeleteSegmentException extends RuntimeException {
        public DeleteSegmentException(String message) {
            super(message);
        }
    }

    /**
     * Issue #617: operator remediation tool. Removes a single segment from
     * a {@link PersistentVectorStore}'s in-memory metadata, with optional
     * purging of the segment's multipart files in the underlying
     * {@link DataStorageManager}.
     *
     * <p>Refuses the deletion when the segment's graph file IS reachable
     * in remote storage and {@code force == false} — the most likely
     * explanation for a reachable graph file is that the operator is
     * targeting the wrong segment. The {@code --force} flag (and an
     * extra confirmation in the CLI) lets the operator override.
     *
     * <p>On a successful in-memory removal, this method re-publishes the
     * current {@link IndexingServiceCheckpointState} so shadow replicas
     * observe the new (smaller) segment count on their next reload. The
     * re-publish is best-effort — if it fails the next regular checkpoint
     * will still carry the updated segment list, so shadows converge.
     *
     * @throws DeleteSegmentException when the request cannot be satisfied
     *                                without mutating state
     */
    public DeleteSegmentResult deleteSegment(String table, String indexName, String segmentStorageKey,
                                              boolean purgeStorage, boolean force) {
        if (table == null || table.isEmpty()) {
            throw new DeleteSegmentException("table is required");
        }
        if (indexName == null || indexName.isEmpty()) {
            throw new DeleteSegmentException("index is required");
        }
        if (segmentStorageKey == null || segmentStorageKey.isEmpty()) {
            throw new DeleteSegmentException("segment is required");
        }
        AbstractVectorStore store = vectorStores.get(storeKey(table, indexName));
        if (store == null) {
            throw new DeleteSegmentException("index " + table + "." + indexName + " is not loaded");
        }
        if (!(store instanceof PersistentVectorStore)) {
            // pr-reviewer follow-up #6: a ReadOnlyVectorStore means we are
            // running as a shadow replica (or have loaded a snapshot in
            // read-only mode); the IS-level gate at IndexingServiceImpl
            // .deleteSegment normally short-circuits these RPCs before they
            // reach the engine, but we keep belt-and-braces here in case a
            // future caller bypasses the gRPC layer.
            if (store instanceof herddb.indexing.vector.ReadOnlyVectorStore) {
                throw new DeleteSegmentException(
                        "index " + table + "." + indexName
                                + ": this instance is a shadow replica — target the primary"
                                + " indexing service");
            }
            throw new DeleteSegmentException(
                    "index " + table + "." + indexName + " is non-persistent ("
                            + store.getClass().getSimpleName() + "); has no on-disk segments");
        }
        PersistentVectorStore pvs = (PersistentVectorStore) store;

        // Presence check (informational + safety gate).
        java.util.List<String> keys = pvs.getSegmentStorageKeysSnapshot();
        if (!keys.contains(segmentStorageKey)) {
            throw new DeleteSegmentException(
                    "segment " + segmentStorageKey + " is not registered in index "
                            + table + "." + indexName + "; currently loaded segments: " + keys);
        }

        // MinIO HEAD-equivalent — best effort. We deliberately read the
        // tablespace UUID from the engine rather than from the request because
        // the IS is single-tablespace per instance.
        boolean graphPresent = false;
        if (dataStorageManager != null) {
            graphPresent = dataStorageManager.multipartIndexFileExists(
                    tableSpaceUUID, segmentStorageKey, "graph");
        }
        if (graphPresent && !force) {
            throw new DeleteSegmentException(
                    "refusing to delete segment " + segmentStorageKey
                            + ": graph file IS reachable in remote storage. "
                            + "Re-run with force=true if you are sure this is the right segment "
                            + "(see issue #617).");
        }

        // Audit-level log BEFORE the mutation so a crash mid-delete leaves a
        // forensic trace in the IS log.
        LOGGER.log(Level.SEVERE,
                "deleteSegment: operator-initiated removal of segment {0} from {1}.{2}"
                        + " (graph_file_present={3}, force={4}, purge_storage={5})"
                        + " — issue #617",
                new Object[]{segmentStorageKey, table, indexName,
                    graphPresent, force, purgeStorage});

        AbstractVectorStore.SegmentDropResult drop = pvs.dropSegmentByStorageKey(segmentStorageKey);
        if (!drop.removed) {
            // Race: a concurrent compaction swap removed the segment between
            // our snapshot and the drop. Treat as a no-op — the operator's
            // intent (segment gone) has been satisfied, but we cannot compute
            // the vectors_lost count because the segment handle is no longer
            // accessible. Surface -1L per the proto contract so operators can
            // distinguish "removed 0 vectors" from "did not remove anything
            // and cannot tell what would have been lost"
            // (pr-reviewer follow-up #5).
            LOGGER.log(Level.WARNING,
                    "deleteSegment: segment {0} disappeared between snapshot and drop"
                            + " (concurrent compaction swap?); reporting no-op with"
                            + " vectors_lost=-1 (race path)",
                    segmentStorageKey);
            return new DeleteSegmentResult(segmentStorageKey, false, -1L, graphPresent, false);
        }

        boolean storagePurged = false;
        if (purgeStorage && dataStorageManager != null) {
            // Best-effort: failures are logged but do not undo the in-memory
            // removal. The operator can re-run with purge_storage=true to
            // retry the file deletion if needed (the underlying call is
            // idempotent).
            try {
                dataStorageManager.deleteMultipartIndexFile(tableSpaceUUID, segmentStorageKey, "graph");
                dataStorageManager.deleteMultipartIndexFile(tableSpaceUUID, segmentStorageKey, "map");
                storagePurged = true;
            } catch (herddb.storage.DataStorageManagerException e) {
                LOGGER.log(Level.WARNING,
                        "deleteSegment: in-memory removal of " + segmentStorageKey
                                + " succeeded but multipart file purge failed; "
                                + "operator may need to clean up storage manually",
                        e);
            }
        }

        // Trigger a checkpoint so the new (smaller) segment list is
        // serialised to the on-disk IndexStatus AND the corresponding
        // IndexingServiceCheckpointState is republished to ZK. Without
        // the checkpoint a shadow reload would still see the deleted
        // segment in the IndexStatus and fail with "multipart file not
        // found" when it tries to mmap the purged map file. The
        // dropSegmentByStorageKey path marks the store dirty so the
        // checkpoint will actually serialise.
        //
        // forceCheckpointAndSaveWatermark also calls
        // publishCheckpointStateBestEffort internally, so shadows are
        // notified as part of the same write.
        try {
            forceCheckpointAndSaveWatermark();
        } catch (RuntimeException e) {
            // Checkpoint failure must not undo the in-memory removal —
            // a subsequent regular checkpoint (or another delete-segment
            // call) will eventually serialise the reduced segment list.
            // Logged at WARNING so operators can correlate with the
            // SEVERE audit line emitted above.
            LOGGER.log(Level.WARNING,
                    "deleteSegment: post-delete checkpoint failed; shadows may"
                            + " not observe the new segment count until the"
                            + " next regular checkpoint",
                    e);
        }

        return new DeleteSegmentResult(
                segmentStorageKey, true, drop.vectorsLost, graphPresent, storagePurged);
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
     * Bumps the issue-#459 per-operation-type counters based on the entry's
     * {@link LogEntry#type}. Always invoked from
     * {@link #processEntry(LogSequenceNumber, LogEntry)} on the tailer thread,
     * before the BEGIN/COMMIT/ROLLBACK fan-out, so each commit-log entry is
     * classified exactly once at original arrival time.
     */
    private void classifyForMetrics(LogEntry entry) {
        switch (entry.type) {
            case LogEntryType.INSERT:
                tailerInserts.increment();
                tailerEntriesAccepted.increment();
                break;
            case LogEntryType.UPDATE:
                tailerUpdates.increment();
                tailerEntriesAccepted.increment();
                break;
            case LogEntryType.DELETE:
                tailerDeletes.increment();
                tailerEntriesAccepted.increment();
                break;
            case LogEntryType.CREATE_TABLE:
            case LogEntryType.ALTER_TABLE:
            case LogEntryType.DROP_TABLE:
            case LogEntryType.TRUNCATE_TABLE:
            case LogEntryType.CREATE_INDEX:
            case LogEntryType.DROP_INDEX:
                tailerDdl.increment();
                tailerEntriesSkipped.increment();
                break;
            default:
                // NOOP, TABLE_CONSISTENCY_CHECK, INDEXING_SERVICE_REBALANCE,
                // BEGIN/COMMIT/ROLLBACKTRANSACTION — none of these mutate the
                // HNSW graph directly.
                tailerEntriesSkipped.increment();
                break;
        }
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
     * Returns the most recently submitted async warmup {@link Future}, or
     * {@code null} if no warmup has been submitted yet OR warmup was last run
     * inline in sync mode (used by tests to verify async dispatch and
     * coalescing — see {@code BlockCacheWarmupAsyncTest}).
     */
    // package-private for testing
    Future<?> getLastWarmupFutureForTest() {
        return lastWarmupFuture.get();
    }

    /**
     * Awaits the most recently submitted async warmup {@link Future}, if any.
     * No-op when warmup is disabled, ran inline in sync mode, or has already
     * completed.  Same {@link ExecutionException} policy as
     * {@link #forceCheckpointAndSaveWatermark()} on the in-flight
     * checkpoint — log at FINE and return, since the warmup task itself
     * already logs per-store failures at WARNING in
     * {@link #runWarmupTask}.
     *
     * @param timeoutMs maximum time to wait, in milliseconds
     * @throws java.util.concurrent.TimeoutException if the warmup does not
     *         complete within {@code timeoutMs}
     */
    // package-private for testing
    void awaitPendingWarmupForTest(long timeoutMs)
            throws InterruptedException, java.util.concurrent.TimeoutException {
        Future<?> f = lastWarmupFuture.get();
        if (f == null) {
            return;
        }
        try {
            f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            // Consistent with forceCheckpointAndSaveWatermark's handling of
            // the in-flight checkpoint Future: the warmup task already
            // logged per-store failures at WARNING, so we just note the
            // root cause at FINE and let the test continue. Tests that
            // need to inspect failure state should use
            // getLastWarmupFutureForTest() and call get() directly.
            LOGGER.log(Level.FINE, "Async warmup ended with failure", e.getCause());
        }
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
     * Test-only sibling of {@link #setLastProcessedLsnForTest(LogSequenceNumber)}.
     * Sets the wall-clock timestamp the engine will treat as "the LogEntry at
     * the tailer position", used by tests that drive the engine via
     * {@link #applyEntry(LogSequenceNumber, LogEntry)} (which bypasses
     * {@code processEntry()}). Issue #423.
     */
    // package-private for testing
    void setLastProcessedEntryTimestampForTest(long timestampMillis) {
        this.lastProcessedEntryTimestamp = timestampMillis;
    }

    /**
     * Test-only snapshot of the live {@code segmentWatchers} map (issue #514).
     * Lets tests assert that DROP_INDEX / DROP_TABLE / TRUNCATE_TABLE correctly
     * close and remove their corresponding {@link herddb.indexing.segment.SegmentAssignmentWatcher}
     * entries so no background refresh executor leaks.
     *
     * @return an unmodifiable view of the current watcher map; keys are
     *         {@link #storeKey} strings, values are (possibly already-closed)
     *         watcher instances
     */
    // package-private for testing
    java.util.Map<String, herddb.indexing.segment.SegmentAssignmentWatcher>
            snapshotSegmentWatchersForTest() {
        return java.util.Collections.unmodifiableMap(new java.util.HashMap<>(segmentWatchers));
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

            case LogEntryType.DROP_TABLE: {
                // Issue #408: resolve the dropped table's name BEFORE
                // applying the schema-tracker mutation — the tracker drops
                // the id → name mapping as part of applying DROP_TABLE.
                String droppedTable = schemaTracker.getTableNameById(entry.tableId);
                schemaTracker.applyEntry(entry);
                if (droppedTable == null) {
                    // No locally tracked table for this id — nothing to clean up.
                    break;
                }
                // Remove all vector stores for this table AND release their
                // remote/local persistent state.  Without the dropIndex()
                // call below, every per-segment graph + map file would
                // linger on the file server / S3 forever, causing the
                // bucket to grow without bound under a CREATE/DROP
                // workload (issue #383).
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
                // Close SegmentAssignmentWatchers for all dropped stores so their
                // background refresh executors are stopped and ZK watcher callbacks
                // can no longer fire after the stores are gone (issue #514).
                segmentWatchers.entrySet().removeIf(e -> {
                    if (e.getKey().startsWith(droppedTablePrefix)) {
                        e.getValue().close();
                        return true;
                    }
                    return false;
                });
                for (Map.Entry<String, AbstractVectorStore> dropped : toClose) {
                    submitVectorStoreDeletion(dropped.getKey(), dropped.getValue());
                }
                break;
            }

            case LogEntryType.TRUNCATE_TABLE: {
                // TRUNCATE_TABLE keeps the table + every vector index
                // *registered* in SchemaTracker (TRUNCATE has no
                // SchemaTracker case) — the table and its indexes are
                // expected to keep accepting INSERT/UPDATE/DELETE after
                // the truncate. Release the in-memory live shards and
                // wipe the on-storage data, then re-create a fresh
                // PersistentVectorStore against a NEW storage UUID so
                // subsequent DML lands cleanly. Erasing the on-storage
                // data without re-creating the store would silently
                // drop every later INSERT for that index (issue #383
                // review).
                // Issue #408: TRUNCATE_TABLE entries carry only the integer
                // tableId; resolve the table name via SchemaTracker.
                String truncatedTable = schemaTracker.getTableNameById(entry.tableId);
                if (truncatedTable == null) {
                    // The tracker has not seen a CREATE_TABLE for this id yet
                    // (e.g. cold start replay before the matching schema
                    // entry); nothing to truncate locally.
                    break;
                }
                String truncatedTablePrefix = truncatedTable + ".";
                java.util.List<Index> toRefresh = new ArrayList<>();
                for (Index idx : schemaTracker.getAllIndexes()) {
                    if (Index.TYPE_VECTOR.equals(idx.type)
                            && truncatedTable.equals(idx.table)) {
                        toRefresh.add(idx);
                    }
                }
                java.util.List<Map.Entry<String, AbstractVectorStore>> toClose =
                        new ArrayList<>();
                vectorStores.entrySet().removeIf(e -> {
                    if (e.getKey().startsWith(truncatedTablePrefix)) {
                        toClose.add(e);
                        return true;
                    }
                    return false;
                });
                vectorStoreIndexUuids.entrySet().removeIf(
                        e -> e.getKey().startsWith(truncatedTablePrefix));
                // Close SegmentAssignmentWatchers for the truncated stores.
                // New watchers are created below when re-creating each store.
                segmentWatchers.entrySet().removeIf(e -> {
                    if (e.getKey().startsWith(truncatedTablePrefix)) {
                        e.getValue().close();
                        return true;
                    }
                    return false;
                });
                for (Map.Entry<String, AbstractVectorStore> dropped : toClose) {
                    submitVectorStoreDeletion(dropped.getKey(), dropped.getValue());
                }
                // Re-create each vector store with a fresh storage UUID.
                // Critical: strip any pre-existing PROP_IS_STORE_UUID
                // before serialising, otherwise the factory's
                // savedUUID branch (start() line ~620) reuses the OLD
                // store UUID — which the async submitVectorStoreDeletion
                // task above is about to wipe. That race silently
                // deletes the freshly-truncated index's writes (issue
                // #383 review). Stripping the property forces the
                // factory's fresh-nanoTime branch.
                for (Index idx : toRefresh) {
                    Index rebuilt = rebuildIndexWithoutStoreUuid(idx);
                    LogEntry synth = new LogEntry(System.currentTimeMillis(),
                            LogEntryType.CREATE_INDEX, 0L, 0, null,
                            herddb.utils.Bytes.from_array(rebuilt.serialize()));
                    createVectorStoreIfNeeded(synth);
                }
                break;
            }

            case LogEntryType.CREATE_INDEX:
                schemaTracker.applyEntry(entry);
                createVectorStoreIfNeeded(entry);
                // Issue #471: only the LIVE tailer path drives the
                // back-fill. The synthetic CREATE_INDEX entries built
                // by installSchemaFromSnapshot / installSchemaFromDescriptor
                // call createVectorStoreIfNeeded directly and never
                // re-enter applyEntry, so they cannot accidentally
                // re-fire the rebuild on snapshot replay.
                triggerRebuildIfNeeded(entry);
                break;

            case LogEntryType.DROP_INDEX: {
                String indexName = new String(entry.value.to_array(), java.nio.charset.StandardCharsets.UTF_8);
                // Remove vector store before updating schema (we need the index info)
                Index idx = schemaTracker.getIndex(indexName);
                if (idx != null && Index.TYPE_VECTOR.equals(idx.type)) {
                    String k = storeKey(idx.table, idx.name);
                    AbstractVectorStore removed = vectorStores.remove(k);
                    vectorStoreIndexUuids.remove(k);
                    // Close the SegmentAssignmentWatcher so its background refresh
                    // executor is stopped and no further ZK watcher callbacks fire
                    // for a store that no longer exists (issue #514).
                    herddb.indexing.segment.SegmentAssignmentWatcher removedWatcher =
                            segmentWatchers.remove(k);
                    if (removedWatcher != null) {
                        removedWatcher.close();
                    }
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
        // IMPORTANT: put the UUID into vectorStoreIndexUuids BEFORE putting the
        // store into vectorStores.  dropIndexImmediate() uses a compute() on
        // vectorStores: if it sees currentStore != null it reads vectorStoreIndexUuids
        // to decide whether to remove the store.  By writing the UUID first we
        // guarantee that by the time the store is visible in vectorStores, its
        // UUID is already visible in vectorStoreIndexUuids, making the gate
        // race-free (issue #509 TOCTOU fix).
        vectorStoreIndexUuids.put(key, index.uuid);
        vectorStores.put(key, store);
        registerIndexMetrics(index.tablespace, index.table, index.name, store);
        LOGGER.log(Level.INFO, "Created vector store for index {0} on column {1} with properties {2}",
                new Object[]{index.name, vectorColumnName, index.properties});
    }

    /**
     * Issue #471 — invoked from the live {@code applyEntry} CREATE_INDEX
     * branch to back-fill a freshly-created vector index from the
     * pinned table checkpoint, when the server-side
     * {@code TableSpaceManager.createIndex} flow has marked the index
     * with {@code rebuild=true}.
     *
     * <p>Synchronous on the tailer thread by design: the engine's
     * {@code lastProcessedLsn} cannot advance past the
     * {@code CREATE_INDEX} entry until {@code applyEntry} returns, so
     * a crash mid-rebuild keeps the watermark at LSN &lt;
     * {@code CREATE_INDEX} and the entry is replayed on restart. The
     * rebuilder uses only the public
     * {@link AbstractVectorStore#addVector} path, so all three of
     * {@code PersistentVectorStore}'s back-pressure layers apply
     * unchanged.
     *
     * <p>Snapshot-replay paths
     * ({@link #installSchemaFromSnapshot}, {@link #installSchemaFromDescriptor})
     * call {@link #createVectorStoreIfNeeded} directly and never
     * re-enter {@code applyEntry}, so this method cannot accidentally
     * re-fire the rebuild after a successful checkpoint has advanced
     * the watermark past the CREATE_INDEX entry.
     */
    private void triggerRebuildIfNeeded(LogEntry entry) {
        // Issue #471: any failure inside this method (the pre-flight
        // IllegalStateException paths AND any Throwable from the
        // rebuilder) MUST escalate to EngineStatus.FAILED before
        // bubbling up. processEntry's outer catch is `catch
        // (Exception e)`, which silently absorbs RuntimeException
        // and lets the next successful tailer entry advance
        // lastProcessedLsn past the failed CREATE_INDEX — the silent
        // data-loss path. The finally block below sets FAILED before
        // any throwable propagates: for Exception paths processEntry's
        // catch then logs and absorbs them; for Error paths
        // (e.g. OutOfMemoryError from jvector graph allocation on a
        // 20 B-row scan) the tailer thread dies after FAILED is
        // already published, so on restart the watermark is still
        // pre-CREATE_INDEX and the rebuild replays.
        boolean rebuildSucceeded = false;
        Index index = null;
        try {
            index = Index.deserialize(entry.value.to_array());
            if (!Index.TYPE_VECTOR.equals(index.type)) {
                rebuildSucceeded = true;
                return;
            }
            String rebuildFlag = index.properties.get(VectorIndexManager.PROP_REBUILD);
            if (!"true".equals(rebuildFlag)) {
                rebuildSucceeded = true;
                return;
            }
            Table table = schemaTracker.getTable(index.table);
            if (table == null) {
                // We just applied the CREATE_INDEX entry to the tracker
                // above; the table MUST be there. Surface this loudly —
                // it is a real correctness bug if it ever fires, not a
                // recoverable race.
                throw new IllegalStateException(
                        "rebuild requested for index " + index.tablespace + "." + index.table
                                + "." + index.name + " but the table is not tracked");
            }
            AbstractVectorStore store = vectorStores.get(storeKey(index.table, index.name));
            if (store == null) {
                // createVectorStoreIfNeeded just ran — same surfacing.
                throw new IllegalStateException(
                        "rebuild requested for index " + index.tablespace + "." + index.table
                                + "." + index.name + " but no vector store was created");
            }
            if (dataStorageManager == null) {
                // Engine boots without a DSM in some test scenarios; the
                // rebuild scan needs one. Surface the misconfiguration.
                throw new IllegalStateException(
                        "rebuild requested but the engine has no DataStorageManager");
            }
            if (tableSpaceUUID == null) {
                // Defensive: should never happen because the tablespace
                // is resolved during start() before any commit-log entry
                // is delivered. But if a test path ever drives applyEntry
                // before start(), surface the misuse explicitly rather
                // than letting the DSM see a null tablespace.
                throw new IllegalStateException(
                        "rebuild requested before tablespace UUID was resolved");
            }
            // Effectively-final capture for the lambda below.
            final Index indexForLambda = index;
            VectorIndexRebuilder rebuilder = new VectorIndexRebuilder(
                    dataStorageManager, tableSpaceUUID, table, index, store,
                    key -> isAcceptedLocally(key, indexForLambda),
                    rebuildMetrics);
            rebuilder.run();
            rebuildSucceeded = true;
        } finally {
            if (!rebuildSucceeded) {
                // Set FAILED before any throwable propagates so the
                // very next processEntry call early-returns on
                // FAILED state. Use the Index data we have (it may
                // be null if Index.deserialize itself threw — that
                // is a payload-corruption scenario, log without
                // identity).
                LOGGER.log(Level.SEVERE,
                        "rebuild failed for {0}; engine entering FAILED state — "
                                + "tailer will refuse to advance past this entry until restart",
                        index != null
                                ? (index.tablespace + "." + index.table + "." + index.name)
                                : "<undeserializable index>");
                engineStatus = EngineStatus.FAILED;
            }
        }
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
        // Issue #408: DML entries carry only the integer tableId; resolve
        // the name once via SchemaTracker for the rest of this hot-path
        // method (no second lookup per index).
        String tableName = schemaTracker.getTableNameById(entry.tableId);
        if (tableName == null) {
            return;
        }
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
        // Issue #463: track whether AT LEAST one vector index for this table
        // accepted the key locally. If every applicable index rejects it via
        // the shard filter, the entry contributes nothing to local state and
        // we bump tailerEntriesShardFiltered so operators can confirm the
        // filter is firing in production. Note this is strictly a "rejected
        // by every index" counter — INSERTs on tables with no vector index
        // hit the early return above and are NOT counted here (different
        // reason: no vector indexing applies).
        boolean anyIndexAcceptedKey = false;
        for (Index idx : vectorIndexes) {
            // Per-index ownership: indexes on the same table may have different
            // numInstances (e.g. a pre-rebalance index with N=2 next to a
            // post-rebalance index with N=4); each makes its own decision.
            if (!isAcceptedLocally(entry.key, idx)) {
                continue;
            }
            anyIndexAcceptedKey = true;
            AbstractVectorStore store = vectorStores.get(storeKey(tableName, idx.name));
            if (store == null) {
                continue;
            }
            float[] vector = extractVector(accessor, store.getVectorColumnName());
            if (vector != null) {
                store.addVector(entry.key, vector);
            }
        }
        if (!anyIndexAcceptedKey) {
            tailerEntriesShardFiltered.increment();
        }
    }

    private void applyUpdate(LogEntry entry) {
        String tableName = schemaTracker.getTableNameById(entry.tableId);
        if (tableName == null) {
            return;
        }
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
        String tableName = schemaTracker.getTableNameById(entry.tableId);
        if (tableName == null) {
            return;
        }
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
            // Issue #408: synthetic CREATE_TABLE — pass the table's own
            // tableId so SchemaTracker registers the id → name mapping that
            // later DML / DROP_TABLE / TRUNCATE_TABLE entries rely on.
            schemaTracker.applyEntry(new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_TABLE, 0L, t.tableId, null,
                    herddb.utils.Bytes.from_array(blob)));
        }
        for (Index ix : descriptor.vectorIndexes) {
            byte[] blob = ix.serialize();
            LogEntry synth = new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_INDEX, 0L, 0, null,
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
                    LogEntryType.CREATE_TABLE, 0L, t.tableId, null,
                    herddb.utils.Bytes.from_array(blob)));
        }
        for (Index ix : snapshot.vectorIndexes) {
            byte[] blob = ix.serialize();
            LogEntry synth = new LogEntry(System.currentTimeMillis(),
                    LogEntryType.CREATE_INDEX, 0L, 0, null,
                    herddb.utils.Bytes.from_array(blob));
            schemaTracker.applyEntry(synth);
            createVectorStoreIfNeeded(synth);
        }
    }

    /**
     * Returns a copy of {@code idx} with {@link #PROP_IS_STORE_UUID}
     * stripped while every other property is preserved. Used by the
     * TRUNCATE_TABLE handler before re-creating the vector store, to
     * force {@link #createVectorStoreIfNeeded} (and the engine's
     * factory) down the fresh-nanoTime UUID branch — without this the
     * snapshot-restored {@code Index} still carries the OLD store UUID
     * and the freshly-truncated store would race the in-flight DROP
     * cleanup of that same UUID, silently wiping its writes (issue
     * #383 review).
     */
    private static Index rebuildIndexWithoutStoreUuid(Index idx) {
        if (!idx.properties.containsKey(PROP_IS_STORE_UUID)) {
            return idx;
        }
        Index.Builder b = Index.builder()
                .uuid(idx.uuid)
                .name(idx.name)
                .table(idx.table)
                .tablespace(idx.tablespace)
                .type(idx.type)
                .column(idx.columnNames[0], idx.columns[0].type);
        for (Map.Entry<String, String> e : idx.properties.entrySet()) {
            if (!PROP_IS_STORE_UUID.equals(e.getKey())) {
                b.property(e.getKey(), e.getValue());
            }
        }
        return b.build();
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
            // Capture the LogEntry timestamp at the same instant as the LSN
            // so the watermark we publish carries an internally-consistent
            // (LSN, freshness) pair (issue #423). The two volatile reads are
            // not strictly atomic, but in the worst case we observe the
            // (LSN_n, timestamp_{n-1}) pair from two adjacent entries — fine
            // for a freshness diagnostic measured in seconds.
            long checkpointEntryTimestamp = lastProcessedEntryTimestamp;
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
            // watermark save. A warmupBytesPerSegment of 0 disables this.
            //
            // In async mode (default since issue #472) the warmup runs on
            // {@link #warmupExecutor} and the watermark snapshot is published
            // immediately below. In sync mode the warmup runs inline here,
            // preserving the original issue #322 behaviour.
            submitWarmupAsyncOrInline();
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
                            checkpointEntryTimestamp,
                            schemaTables, schemaVectorIndexes);
            try {
                watermarkStore.save(snapshotToSave);
                // Only after the watermark has been successfully persisted to
                // remote storage is the engine guaranteed to recover from
                // checkpointLsn on restart — this is the LSN the server's
                // retention floor must pin against (issue #364).
                lastDurableLsn = checkpointLsn;
                lastDurableEntryTimestamp = checkpointEntryTimestamp;
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
            publishCheckpointStateBestEffort(checkpointLsn, checkpointEntryTimestamp);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "checkpointAndSaveWatermark failed", e);
        }
    }

    /**
     * Runs the post-Phase-C BFS warmup (issue #322) either on a dedicated
     * executor (async, default since issue #472) or inline on the calling
     * thread (sync, opt-in via
     * {@link IndexingServerConfiguration#PROPERTY_VECTOR_SEGMENT_CACHE_WARMUP_ASYNC}).
     *
     * <p>In async mode this method snapshots the current persistent vector
     * stores, submits a single task to {@link #warmupExecutor}, stores the
     * resulting Future in {@link #lastWarmupFuture} via
     * {@link AtomicReference#compareAndSet} (so a future refactor that
     * introduces concurrent callers does not silently leak a Future), and
     * returns immediately.
     *
     * <p>Concurrent submits are coalesced: if a previous warmup is still
     * running, the new submit is dropped (logged at FINE) and the next
     * checkpoint trigger will queue a fresh warmup with the then-current
     * segment list.  This prevents unbounded executor queueing while keeping
     * the system converging to "warm against the latest segment set".
     *
     * <p><b>Quiet-system corner case:</b> if checkpoint <i>N+1</i> is
     * coalesced away because warmup <i>N</i> is still running, the segments
     * produced by checkpoint <i>N+1</i> are warmed only when checkpoint
     * <i>N+2</i> fires. If no further checkpoint fires (e.g. ingestion
     * stops, replicas catch up), those segments stay cold until the first
     * organic ANN search hits them. This is acceptable because warmup is a
     * best-effort latency optimization; the cache will be populated lazily
     * by query traffic, exactly as it would have been without issue #322.
     *
     * <p><b>Threading contract:</b> in production this method is reachable
     * only from {@link #checkpointAndSaveWatermark()}, which itself runs
     * exclusively on the single-thread {@link #checkpointExecutor}. The
     * {@link #lastWarmupFuture} CAS is therefore expected to succeed every
     * time; the loop below logs+returns on a CAS miss as a defensive guard
     * against a future refactor that introduces a concurrent caller.
     *
     * <p>In sync mode the warmup runs inline on the caller (typically the
     * {@code indexing-checkpoint} thread), preserving the original issue #322
     * behaviour where the watermark snapshot is held back until the cache is
     * primed.
     *
     * <p>Per-store {@link RuntimeException}s are caught and logged at WARNING
     * inside {@link #runWarmupTask} so a single misbehaving store cannot
     * abort the warmup of the others.  (The store-level
     * {@link PersistentVectorStore#warmUpBlockCache} already swallows
     * {@link IOException} itself.)
     */
    private void submitWarmupAsyncOrInline() {
        if (warmupBytesPerSegment <= 0) {
            return;
        }
        // Note: `warmupExecutor.isShutdown()` is racy with concurrent
        // close() — the executor can transition between this read and the
        // submit() below. The race is intentionally accepted: the
        // RejectedExecutionException catch handles the late-loser case.
        if (warmupAsync && warmupExecutor != null && !warmupExecutor.isShutdown()) {
            // Snapshot the persistent stores at submit time so the executor
            // task does not race with concurrent map mutations (createIndex /
            // dropIndex). The snapshot is a defensive copy of references —
            // if a store is closed concurrently, the per-store catch below
            // logs and continues.
            final List<PersistentVectorStore> snapshot = new ArrayList<>();
            for (AbstractVectorStore store : vectorStores.values()) {
                if (store instanceof PersistentVectorStore) {
                    snapshot.add((PersistentVectorStore) store);
                }
            }
            if (snapshot.isEmpty()) {
                return;
            }
            // Coalesce: if a previous warmup is still in progress, drop this
            // submit. The next checkpoint will trigger a fresh warmup against
            // the then-current segment list.
            Future<?> prev = lastWarmupFuture.get();
            if (prev != null && !prev.isDone()) {
                LOGGER.log(Level.FINE,
                        "Skipping warmup submit: previous warmup still in progress");
                return;
            }
            final long bytesPerSegment = this.warmupBytesPerSegment;
            try {
                Future<?> f = warmupExecutor.submit(() -> runWarmupTask(snapshot, bytesPerSegment));
                // CAS rather than plain set: today the threading contract
                // (single-thread checkpointExecutor) guarantees that the
                // observed `prev` is still current here, but a concurrent
                // caller introduced by a future refactor would otherwise
                // silently leak a Future. On CAS failure cancel `f` to
                // avoid leaking the executor task and log at FINE.
                if (!lastWarmupFuture.compareAndSet(prev, f)) {
                    f.cancel(false);
                    LOGGER.log(Level.FINE,
                            "Warmup submit raced with another caller; cancelling new task");
                }
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // Executor was shut down concurrently with this submit —
                // accept the race and skip the warmup.
                LOGGER.log(Level.FINE,
                        "Warmup executor rejected task (likely concurrent shutdown)");
            }
        } else {
            // Sync mode (or async disabled at runtime): run inline.
            for (AbstractVectorStore store : vectorStores.values()) {
                if (store instanceof PersistentVectorStore) {
                    ((PersistentVectorStore) store).warmUpBlockCache(warmupBytesPerSegment);
                }
            }
        }
    }

    /**
     * Test-only hook executed at the very start of the async warmup task,
     * before any segment is touched. {@code null} (the default) means no-op.
     * Tests use this to pause the warmup deterministically (e.g. {@code
     * latch.await()}) so they can observe the in-flight state without
     * interfering with the unrelated Phase A / B / C reads of the checkpoint.
     */
    private volatile Runnable warmupPauseHookForTest = null;

    /**
     * Installs (or clears) the test-only warmup pause hook. Package-private:
     * production code never calls this. The hook fires on the
     * {@code indexing-warmup} thread before the per-store BFS loop starts.
     */
    // package-private for testing
    void setWarmupPauseHookForTest(Runnable hook) {
        this.warmupPauseHookForTest = hook;
    }

    /**
     * Body of the async warmup task: iterates the snapshot of persistent
     * stores and calls {@link PersistentVectorStore#warmUpBlockCache} on each.
     * Per-store {@link RuntimeException}s are caught so a single store's
     * failure cannot prevent the others from warming.
     */
    private void runWarmupTask(List<PersistentVectorStore> snapshot, long bytesPerSegment) {
        Runnable hook = warmupPauseHookForTest;
        if (hook != null) {
            try {
                hook.run();
            } catch (RuntimeException e) {
                // Test hook misbehaved — log and continue. Production never
                // installs a hook so this catch is purely defensive against
                // a misuse in tests.
                LOGGER.log(Level.WARNING, "warmupPauseHookForTest threw; continuing", e);
            }
        }
        for (PersistentVectorStore store : snapshot) {
            try {
                store.warmUpBlockCache(bytesPerSegment);
            } catch (RuntimeException e) {
                // Catch RuntimeException narrowly: warmUpBlockCache itself
                // already handles IOException internally; the only way
                // RuntimeException propagates here is from a store closed
                // concurrently or another unexpected programming error.
                // Log and continue with the next store so a single bad store
                // does not break warmup for the others.
                LOGGER.log(Level.WARNING,
                        "Async warmUpBlockCache failed for one store; continuing with others", e);
            }
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
    private void publishCheckpointStateBestEffort(LogSequenceNumber lsn, long entryTimestampMillis) {
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
                            System.currentTimeMillis(),
                            entryTimestampMillis));
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
        // At engine boot we only know the freshness from the loaded watermark
        // (or 0 on a fresh install). Issue #423.
        publishCheckpointStateBestEffort(lsn, lastDurableEntryTimestamp);
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
            FastThreadLocalThread t = new FastThreadLocalThread(r, "indexing-shadow-reload");
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
            // Pre-seed the shadow freshness clock from the primary's
            // advertised state so the very first GetShadowStatus call
            // after boot already carries a meaningful timestamp, even if
            // doShadowReload() is racing with us (issue #423).
            this.shadowLoadedEntryTimestamp = primaryState.getLastEntryTimestampMillis();
        }
        boolean initialReloadOk = doShadowReload();
        this.shadowReady = initialReloadOk;

        // Install the ZK watch — every update enqueues a reload.
        if (metadataStorageManager != null) {
            try {
                metadataStorageManager.watchIndexingServiceCheckpointState(shadowOf, state -> {
                    this.primaryAdvertisedLsn = state.toLogSequenceNumber();
                    // Issue #423: do NOT update shadowLoadedEntryTimestamp
                    // here. The proto contract on
                    // GetShadowStatus.loaded_entry_timestamp_ms says it is
                    // the timestamp of the LogEntry at loaded_ledger_id /
                    // loaded_offset. Advancing the timestamp BEFORE
                    // doShadowReload() actually replays the new on-disk
                    // state into each ReadOnlyVectorStore would expose an
                    // inconsistent (LSN_old, ts_new) pair while the reload
                    // executor is still running. The post-reload write
                    // inside doShadowReload() is the only place the
                    // freshness is published.
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
                // Per-store failure isolation: a single ReadOnlyVectorStore
                // failing its reload (corrupt segment file, transient I/O)
                // must not abort the whole reload pass for the other stores
                // this shadow holds. The failed store keeps serving its
                // previously-loaded view; allOk=false makes the pass
                // non-final so shadowLastReloadTimestampMs is not advanced.
                LOGGER.log(Level.WARNING, "Shadow reload failed for index " + ro, e);
                allOk = false;
            }
        }
        if (maxLsn != null) {
            this.shadowLoadedLsn = maxLsn;
            // Capture the primary's advertised LogEntry timestamp at the
            // moment of the reload so GetShadowStatus can report the
            // freshness of the data this shadow can serve (issue #423).
            //
            // Caveats:
            //   * shadowLoadedLsn is set BEFORE this read so the visible
            //     pair is at worst (LSN_n, timestamp_{n+1}) — never
            //     (LSN_n, timestamp_{n-1}). The watch-callback path
            //     deliberately does NOT touch shadowLoadedEntryTimestamp,
            //     leaving this method as the single writer.
            //   * IndexStatus on disk currently always carries
            //     sequenceNumber=START_OF_TIME (PersistentVectorStore does
            //     not yet stamp it with the real checkpoint LSN), so we do
            //     NOT condition the timestamp write on
            //     maxLsn.equals(primary.toLogSequenceNumber()) — that
            //     check would never pass and shadow freshness reporting
            //     would be permanently broken. Instead we trust that the
            //     primary's published state is internally consistent
            //     (LSN + timestamp written together in
            //     publishCheckpointStateBestEffort).
            try {
                IndexingServiceCheckpointState primary =
                        metadataStorageManager != null
                                ? metadataStorageManager.getIndexingServiceCheckpointState(
                                        getShadowOfOrMinusOne())
                                : null;
                if (primary != null) {
                    this.shadowLoadedEntryTimestamp = primary.getLastEntryTimestampMillis();
                }
            } catch (MetadataStorageManagerException e) {
                LOGGER.log(Level.FINE,
                        "Shadow could not refresh primary's lastEntryTimestamp after reload",
                        e);
            }
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

    /**
     * Wall-clock timestamp (epoch ms) of the LogEntry at
     * {@link #getShadowLoadedLsn()} — the freshness of the data this shadow
     * can serve. Picked up from the primary's advertised
     * {@link IndexingServiceCheckpointState#getLastEntryTimestampMillis()} on
     * every reload. {@code 0} means "unknown" (primary has not published a
     * checkpoint yet). Issue #423.
     */
    public long getShadowLoadedEntryTimestamp() {
        return shadowLoadedEntryTimestamp;
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
     * Test-only accessor: returns the segment count of the loaded vector
     * store for {@code table.indexName} regardless of whether it is a
     * {@link PersistentVectorStore} (primary) or a
     * {@link herddb.indexing.vector.ReadOnlyVectorStore} (shadow).
     * Returns {@code -1} when the store is not loaded.
     *
     * <p>Added in pr-reviewer follow-up #4 for issue #617 so the
     * {@code ShadowDeleteSegmentE2ETest.lateBootShadowObservesPostDeleteState}
     * case can assert that a shadow booted AFTER a primary-side delete
     * loads the smaller (post-delete) segment count, without having to
     * unwrap the vector store map directly.
     */
    public int getSegmentCountForTest(String table, String indexName) {
        AbstractVectorStore store = vectorStores.get(storeKey(table, indexName));
        if (store == null) {
            return -1;
        }
        if (store instanceof PersistentVectorStore) {
            return ((PersistentVectorStore) store).getSegmentCount();
        }
        if (store instanceof herddb.indexing.vector.ReadOnlyVectorStore) {
            return ((herddb.indexing.vector.ReadOnlyVectorStore) store).getSegmentCount();
        }
        return -1;
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
        int loadingSegmentsDone = 0;
        int loadingSegmentsTotal = 0;
        String status = "tailing";
        if (store instanceof PersistentVectorStore) {
            PersistentVectorStore pvs = (PersistentVectorStore) store;
            segmentCount = pvs.getSegmentCount();
            if (pvs.isLoadingFromStatus()) {
                loadingSegmentsDone = pvs.getLoadingSegmentsDone();
                loadingSegmentsTotal = pvs.getLoadingSegmentsTotal();
                status = "loading";
            }
        }
        // Snapshot both LSNs and the matching freshness timestamps together.
        // The pair is approximately consistent: in the worst case a reader
        // can observe (LSN_n, timestamp_{n+1}) — i.e. the timestamp is one
        // entry newer than the LSN — because the writes in processEntry()
        // are not atomic across the two volatile fields. Acceptable for a
        // diagnostic measured in seconds; locking would impose hot-path
        // cost for no operational benefit.
        // Server-side retention pins on durable_lsn_*; tailer_lsn_* is
        // exposed for diagnostics only (issue #364). Timestamps are
        // diagnostic-only (issue #423).
        LogSequenceNumber tailerSnap = lastProcessedLsn;
        long tailerTsSnap = lastProcessedEntryTimestamp;
        LogSequenceNumber durableSnap = lastDurableLsn;
        long durableTsSnap = lastDurableEntryTimestamp;
        return new IndexStatusInfo(vectorCount, segmentCount,
                tailerSnap != null ? tailerSnap.ledgerId : -1,
                tailerSnap != null ? tailerSnap.offset : -1,
                tailerTsSnap,
                durableSnap != null ? durableSnap.ledgerId : -1,
                durableSnap != null ? durableSnap.offset : -1,
                durableTsSnap,
                status,
                loadingSegmentsDone, loadingSegmentsTotal);
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
            int segmentCount = 0;
            int loadingDone = 0;
            int loadingTotal = 0;
            if (store instanceof PersistentVectorStore) {
                PersistentVectorStore pvs = (PersistentVectorStore) store;
                segmentCount = pvs.getSegmentCount();
                if (pvs.isLoadingFromStatus()) {
                    loadingDone = pvs.getLoadingSegmentsDone();
                    loadingTotal = pvs.getLoadingSegmentsTotal();
                    status = "loading";
                }
            } else if (store != null) {
                segmentCount = 1;
            }
            out.add(new IndexDescriptor(idx.tablespace, idx.table, idx.name, vectorCount, status,
                    segmentCount, loadingDone, loadingTotal));
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
        // Surface the per-index `numShards` configuration alongside the live
        // counters so operators can correlate sharding decisions with what the
        // index was actually created with (issue #451). The schema tracker is
        // the source of truth for the Index.properties map; if the index has
        // been dropped between the vectorStores lookup and here we just leave
        // d.numShards at its default (1).
        Index indexDef = schemaTracker.getIndex(index);
        if (indexDef != null) {
            d.numShards = getNumShardsForIndex(indexDef);
        }
        if (store instanceof PersistentVectorStore) {
            PersistentVectorStore pvs = (PersistentVectorStore) store;
            d.dimension = pvs.getDimension();
            d.similarity = pvs.getSimilarityFunction();
            d.liveNodeCount = pvs.getLiveNodeCount();
            d.onDiskNodeCount = pvs.getOnDiskNodeCount();
            d.segmentCount = pvs.getSegmentCount();
            d.liveShardCount = pvs.getLiveShardCount();
            d.liveVectorsMemoryBytes = pvs.getLiveVectorsMemoryBytes();
            d.ondiskSegmentMemoryBytes = pvs.getOnDiskSegmentsEstimatedMemoryBytes();
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
        // Capture timestamps in the same order as the LSN reads above, so the
        // (LSN, timestamp) pairs come from the same volatile-write window.
        // Issue #423.
        d.tailerLsnTimestamp = lastProcessedEntryTimestamp;
        LogSequenceNumber durableSnap = lastDurableLsn;
        if (durableSnap != null) {
            d.durableLsnLedger = durableSnap.ledgerId;
            d.durableLsnOffset = durableSnap.offset;
        } else {
            d.durableLsnLedger = -1L;
            d.durableLsnOffset = -1L;
        }
        d.durableLsnTimestamp = lastDurableEntryTimestamp;
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
        registerSegmentAssignmentMetrics();
        // Issue #471: register the rebuild gauges
        // (rebuild.records_scanned, rebuild.records_indexed,
        // rebuild.last_start_time_ms, rebuild.last_end_time_ms) so
        // operators can see rebuild progress + status without scraping
        // logs. The class is a no-op when statsLogger == null.
        rebuildMetrics.register(statsLogger);
        // Netty direct-memory counters (issue #246) so the unified JVM
        // dashboard can show pool-arena growth for the IS alongside the
        // main server and the remote file service. The gauges carry
        // their own netty_ prefix — pass the root logger, not a scope.
        if (statsLogger != null) {
            herddb.core.stats.NettyMemoryMetrics.register(statsLogger);
        }
    }

    /**
     * Returns the engine's shared {@link herddb.indexing.segment.SegmentAssignmentMetrics}
     * observer. Visible so future code that constructs a
     * {@link herddb.indexing.segment.SegmentAssignmentWatcher} can chain
     * this observer onto its listener (the production integration path is
     * not wired yet — see field doc).
     */
    public herddb.indexing.segment.SegmentAssignmentMetrics getSegmentAssignmentMetrics() {
        return segmentAssignmentMetrics;
    }

    /**
     * Issue #471 — read-only accessor for the engine-wide rebuild
     * counters (records scanned, records indexed, last start/end
     * timestamps). Used by the rebuild test suite to assert that a
     * triggered rebuild actually visited records and inserted the
     * expected subset.
     */
    public VectorIndexRebuildMetrics getRebuildMetricsForTest() {
        return rebuildMetrics;
    }

    /**
     * Registers Prometheus gauges + counters for the segmented-v2
     * ownership-watcher activity (Grafana panel: "Segmented-v2 ownership"
     * on the indexing-service dashboard).
     */
    private void registerSegmentAssignmentMetrics() {
        StatsLogger sl = this.statsLogger;
        if (sl == null) {
            return;
        }
        StatsLogger ownership = sl.scope("segments_ownership");
        ownership.registerGauge("owned", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return segmentAssignmentMetrics.getOwnedSegmentsCount();
            }
        });
        ownership.registerGauge("loads_total", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return segmentAssignmentMetrics.getSegmentLoadsTotal();
            }
        });
        ownership.registerGauge("releases_total", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return segmentAssignmentMetrics.getSegmentReleasesTotal();
            }
        });
        ownership.registerGauge("pending_assignments_observed_total", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return segmentAssignmentMetrics.getPendingAssignmentsObservedTotal();
            }
        });
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
        // Issue #459: per-operation-type counters. Each one is exposed as a
        // monotonically increasing gauge — Prometheus / consumers compute
        // rates by differencing successive snapshots. Names line up with the
        // Prometheus metric names (tailer_<scope-leaf>) on which the
        // indexing-service Grafana dashboard panels are wired.
        tailerStats.registerGauge("entries_accepted", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerEntriesAccepted.sum();
            }
        });
        tailerStats.registerGauge("entries_skipped", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerEntriesSkipped.sum();
            }
        });
        // Issue #463: INSERT entries this replica did not apply because the
        // shard filter rejected the key on every vector index defined on the
        // entry's table. Operators verify cross-replica sharding is actually
        // happening by watching this rise to ~(N-1)/N of `tailer_inserts`.
        tailerStats.registerGauge("entries_shard_filtered", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerEntriesShardFiltered.sum();
            }
        });
        tailerStats.registerGauge("inserts", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerInserts.sum();
            }
        });
        tailerStats.registerGauge("updates", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerUpdates.sum();
            }
        });
        tailerStats.registerGauge("deletes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerDeletes.sum();
            }
        });
        tailerStats.registerGauge("ddl", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return tailerDdl.sum();
            }
        });
        tailerStats.registerGauge("batches", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                CommitLogTailing t = tailer;
                return t != null ? t.getBatchesProcessed() : 0L;
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

        // --- Frontier (pinned) region metrics ---
        // Same shape as the main cache metrics so the Grafana dashboard can
        // use the same panel templates with a "frontier" label filter.
        // All return 0 when the frontier region is disabled.
        StatsLogger frontier = scope.scope("frontier");
        frontier.registerGauge("hits", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierHitCount();
            }
        });
        frontier.registerGauge("evictions", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierEvictionCount();
            }
        });
        frontier.registerGauge("load_success", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierLoadSuccessCount();
            }
        });
        frontier.registerGauge("load_failure", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierLoadFailureCount();
            }
        });
        frontier.registerGauge("load_time_nanos_total", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierTotalLoadTimeNanos();
            }
        });
        frontier.registerGauge("size_entries", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierEstimatedSize();
            }
        });
        frontier.registerGauge("size_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.frontierWeightedSize();
            }
        });
        frontier.registerGauge("max_bytes", new Gauge<Long>() {
            @Override public Long getDefaultValue() {
                return 0L;
            }

            @Override public Long getSample() {
                return cache.maxFrontierBytes();
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

        // Shut down the warmup executor AFTER the checkpoint executor
        // (issue #472) so the final synchronous checkpoint above could still
        // queue a warmup. The warmup is best-effort and never holds invariants
        // — we wait briefly for an in-flight warmup to finish, then force
        // shutdownNow. The thread is daemon so it never blocks JVM exit even
        // if shutdownNow is interrupted.
        if (warmupExecutor != null) {
            warmupExecutor.shutdown();
            try {
                if (!warmupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.log(Level.FINE,
                            "In-flight warmup did not complete within 5s of shutdown; forcing shutdownNow");
                    warmupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                warmupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            warmupExecutor = null;
            // If a BFS read inside the warmup task is in a non-interruptible
            // state (e.g. blocked in a Channel that ignores Thread.interrupt),
            // the daemon thread may outlive close(). That is safe: the thread
            // is daemon (cannot block JVM exit), the snapshot list it captured
            // is dropped on the next GC, and the warmup is best-effort.
            lastWarmupFuture.set(null);
            LOGGER.info("Warmup executor shut down");
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

        // Close segment-assignment watchers BEFORE vector stores so no adoption
        // events fire against a half-closed store (issue #514).
        for (herddb.indexing.segment.SegmentAssignmentWatcher w : segmentWatchers.values()) {
            try {
                w.close();
            } catch (RuntimeException e) {
                // close() is void and handles its own InterruptedException internally;
                // this catch is a safety net for unexpected runtime failures that must
                // not prevent the remaining shutdown steps from running.
                LOGGER.log(Level.WARNING, "Error closing segment watcher during shutdown", e);
            }
        }
        segmentWatchers.clear();

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
        private final long tailerLsnTimestamp;
        private final long durableLsnLedger;
        private final long durableLsnOffset;
        private final long durableLsnTimestamp;
        private final String status;
        private final int loadingSegmentsDone;
        private final int loadingSegmentsTotal;

        public IndexStatusInfo(long vectorCount, int segmentCount,
                               long tailerLsnLedger, long tailerLsnOffset,
                               long tailerLsnTimestamp,
                               long durableLsnLedger, long durableLsnOffset,
                               long durableLsnTimestamp,
                               String status,
                               int loadingSegmentsDone, int loadingSegmentsTotal) {
            this.vectorCount = vectorCount;
            this.segmentCount = segmentCount;
            this.tailerLsnLedger = tailerLsnLedger;
            this.tailerLsnOffset = tailerLsnOffset;
            this.tailerLsnTimestamp = tailerLsnTimestamp;
            this.durableLsnLedger = durableLsnLedger;
            this.durableLsnOffset = durableLsnOffset;
            this.durableLsnTimestamp = durableLsnTimestamp;
            this.status = status;
            this.loadingSegmentsDone = loadingSegmentsDone;
            this.loadingSegmentsTotal = loadingSegmentsTotal;
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

        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the tailer LSN.
         * {@code 0} means "unknown" (no entries processed yet). Issue #423.
         */
        public long getTailerLsnTimestamp() {
            return tailerLsnTimestamp;
        }

        public long getDurableLsnLedger() {
            return durableLsnLedger;
        }

        public long getDurableLsnOffset() {
            return durableLsnOffset;
        }

        /**
         * Wall-clock timestamp (epoch ms) of the LogEntry at the durable
         * watermark LSN. {@code 0} means "unknown" (no successful checkpoint
         * yet). Issue #423.
         */
        public long getDurableLsnTimestamp() {
            return durableLsnTimestamp;
        }

        public String getStatus() {
            return status;
        }

        /** Segments loaded so far during recovery; 0 when not loading. */
        public int getLoadingSegmentsDone() {
            return loadingSegmentsDone;
        }

        /** Total segments to load during recovery; 0 when not loading. */
        public int getLoadingSegmentsTotal() {
            return loadingSegmentsTotal;
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
        private final int loadingSegmentsDone;
        private final int loadingSegmentsTotal;

        public IndexDescriptor(String tablespace, String table, String index,
                               long vectorCount, String status, int segmentCount,
                               int loadingSegmentsDone, int loadingSegmentsTotal) {
            this.tablespace = tablespace;
            this.table = table;
            this.index = index;
            this.vectorCount = vectorCount;
            this.status = status;
            this.segmentCount = segmentCount;
            this.loadingSegmentsDone = loadingSegmentsDone;
            this.loadingSegmentsTotal = loadingSegmentsTotal;
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

        /** Segments loaded so far during recovery; 0 when not loading. */
        public int getLoadingSegmentsDone() {
            return loadingSegmentsDone;
        }

        /** Total segments to load during recovery; 0 when not loading. */
        public int getLoadingSegmentsTotal() {
            return loadingSegmentsTotal;
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
        // Issue #563: in-memory footprint of the on-disk VectorSegment objects
        // (pkData/pkOffsets/pkLengths arrays + BLink pk-to-ordinal trees).
        // Reported separately from liveVectorsMemoryBytes so operators can tell
        // genuine live-vector memory apart from on-disk segment overhead.
        public long ondiskSegmentMemoryBytes;
        public long onDiskSizeBytes;
        public boolean dirty;
        // In-memory tailer position; diagnostic only. See IndexStatusInfo.
        public long tailerLsnLedger;
        public long tailerLsnOffset;
        // Wall-clock (epoch ms) of the LogEntry at the tailer LSN; 0=unknown.
        // Issue #423.
        public long tailerLsnTimestamp;
        // Durable recovery LSN; the LSN the engine resumes from on restart.
        public long durableLsnLedger;
        public long durableLsnOffset;
        // Wall-clock (epoch ms) of the LogEntry at the durable LSN; 0=unknown.
        // Issue #423.
        public long durableLsnTimestamp;
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
        // Per-index `numShards` from the CREATE VECTOR INDEX WITH clause.
        // Controls within-instance HNSW graph-bucket granularity; defaults to
        // 1 when the property is absent. Issue #451.
        public int numShards = 1;
    }
}
