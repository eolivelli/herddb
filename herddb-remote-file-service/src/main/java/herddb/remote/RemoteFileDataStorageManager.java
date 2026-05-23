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

package herddb.remote;

import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.file.FileDataStorageManager;
import herddb.file.FileRecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.index.KeyToPageIndexFactory;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.remote.storage.ObjectStorage;
import herddb.server.ServerConfiguration;
import herddb.storage.DataPageDoesNotExistException;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.ByteBufCursor;
import herddb.utils.ByteBufDataOutput;
import herddb.utils.XXHash64Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * DataStorageManager that stores page data on remote RemoteFileService instances
 * and keeps metadata locally (checkpoint files, table/index metadata, transactions).
 *
 * @author enrico.olivelli
 */
public class RemoteFileDataStorageManager extends DataStorageManager
        implements herddb.server.RemoteFileStorageManager {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileDataStorageManager.class.getName());

    private final FileDataStorageManager localMetadataManager;
    private final RemoteFileServiceClient client;
    private final Path tmpDir;
    private final int swapThreshold;

    /**
     * Maximum number of concurrent block uploads per {@link #writePage} call
     * when splitting a page into multipart blocks. Configured via
     * {@link ServerConfiguration#PROPERTY_REMOTE_FILE_BLOCK_PARALLELISM}.
     */
    private final int blockUploadParallelism;

    /**
     * When set, checkpoint metadata (TableStatus, IndexStatus, table/index definitions,
     * checkpoint LSN) is also published to remote storage so that shared-storage read
     * replicas can consume it.
     */
    private volatile SharedCheckpointMetadataManager sharedCheckpointMetadataManager;

    /**
     * When non-null, used by {@link #downloadMultipartIndexFile} to download segment
     * map files directly from object storage (bypassing the file-server).
     * This eliminates the serial wire round-trips that make cold-start recovery very
     * slow when there are thousands of segments (issue #381).
     *
     * <p>Set via {@link #setDirectObjectStorage(ObjectStorage)} after construction,
     * typically by the indexing-service bootstrap when
     * {@code indexing.s3.direct.enabled=true}.
     */
    private volatile ObjectStorage directObjectStorage;

    /**
     * Issue #638: when {@code true}, {@link #writeMultipartIndexFile} uploads
     * segment files <em>directly</em> to object storage as a single S3 object
     * via the S3 Multipart Upload API (driven by {@code S3TransferManager}),
     * bypassing the gRPC file-server hop. Symmetric to the
     * {@link #directObjectStorage} read-side flag.
     *
     * <p>Set via {@link #enableDirectUpload(long)} at startup only. Calling
     * {@link #enableDirectUpload(long)} while uploads are in flight is unsafe.
     */
    private volatile boolean directUploadEnabled;

    /**
     * Issue #638: semaphore that bounds the total bytes currently being
     * uploaded via the direct-S3 path, independent of the gRPC client's
     * {@code inflightWriteBytes} budget. Permits are acquired before the
     * Transfer Manager upload starts and released when its completion
     * future fires (success or failure). When direct upload is disabled
     * the semaphore is {@code null} and the cap field is {@code 0}.
     *
     * <p>The dedicated budget keeps direct compaction writes from
     * starving (or being starved by) the gRPC write plane, and lets
     * operators tune the direct cap independently via
     * {@code indexing.remote.file.client.max.inflight.direct.write.bytes}.
     */
    private volatile Semaphore directInflightUploadBytes;

    /**
     * Issue #638: total capacity of {@link #directInflightUploadBytes},
     * recorded so warning logs and the {@code availableDirectInflightUploadBytes()}
     * gauge can report the configured limit.
     */
    private volatile long maxDirectInflightUploadBytes;

    /**
     * Issue #638: per-permit cap for {@link #directInflightUploadBytes}.
     * Mirrors the deadlock-prevention pattern in
     * {@code RemoteFileServiceClient.acquireInflightWriteBytes} — a single
     * upload that exceeds the semaphore's total capacity must not block
     * forever; instead we acquire up to {@code directUploadPermits}.
     */
    private volatile int directUploadPermits;

    /**
     * Issue #638: probe-result cache for the {@code .bulk} layout presence
     * check. Avoids issuing an S3 {@code HEAD} for every random-access
     * read on the same logical multipart file. Populated on first probe;
     * invalidated whenever the logical file is written or deleted.
     */
    private final ConcurrentHashMap<String, Boolean> bulkLayoutCache = new ConcurrentHashMap<>();

    /**
     * Issue #638: local cache of bulk multipart files that have been
     * downloaded for {@link #multipartIndexReaderSupplier} access. Bulk
     * files are stored as a single S3 object (no per-block layout) so we
     * materialise them to local disk once and reuse a memory-mapped
     * reader for subsequent supplier invocations. Keyed by logical path.
     * Cleared on {@link #deleteMultipartIndexFile} and {@link #close()}.
     */
    private final ConcurrentHashMap<String, Path> bulkLocalCache = new ConcurrentHashMap<>();

    /**
     * Issue #638: suffix appended to the multipart logical path when the
     * file is stored in the bulk layout (single S3 object). Chosen to be
     * unambiguously distinct from the legacy per-block suffix
     * {@code .multipart/{N}} so that both layouts can coexist for the same
     * logical file (legacy + new writes) without collision in the bucket.
     */
    static final String BULK_LAYOUT_SUFFIX = ".bulk";

    /**
     * Configures a direct object-storage client for segment map-file downloads
     * during recovery. When set, {@link #supportsDirectMultipartDownload()} returns
     * {@code true} and {@link #downloadMultipartIndexFile} reads block objects directly
     * from S3 instead of routing through the file server.
     *
     * <p>Ownership of {@code storage} is transferred to this manager:
     * it will be closed by {@link #close()} together with all other resources.
     *
     * @param storage an open, ready-to-use {@link ObjectStorage} instance
     */
    public void setDirectObjectStorage(ObjectStorage storage) {
        this.directObjectStorage = storage;
    }

    /**
     * Issue #638: turns on the direct-S3 upload path for
     * {@link #writeMultipartIndexFile}. Must be called <em>after</em>
     * {@link #setDirectObjectStorage} — uploads dispatch through the same
     * {@link ObjectStorage} instance used for direct reads.
     *
     * <p>{@code maxInflightBytes} sets the cap on the per-DSM in-flight
     * direct-upload semaphore. The cap is independent of the gRPC client's
     * {@code remote.file.client.max.inflight.write.bytes}: tune them
     * separately via {@code indexing.remote.file.client.max.inflight.direct.write.bytes}.
     *
     * <p>Idempotent: calling repeatedly with the same value is a no-op; calling
     * with a different value resizes the semaphore by drain-and-replace (only
     * safe during startup, before any direct upload has been dispatched).
     */
    public void enableDirectUpload(long maxInflightBytes) {
        if (maxInflightBytes <= 0L) {
            throw new IllegalArgumentException(
                    "maxInflightBytes must be > 0, got " + maxInflightBytes);
        }
        // Idempotency: if already enabled with the same cap, skip re-initialisation
        // to avoid discarding an existing semaphore that has outstanding permits.
        // Re-invocation with a different cap is only safe at startup (before any
        // upload has been dispatched).
        if (directUploadEnabled && maxInflightBytes == this.maxDirectInflightUploadBytes) {
            return;
        }
        // Cap to Integer.MAX_VALUE because Semaphore counts permits as int.
        int permits = (int) Math.min(maxInflightBytes, Integer.MAX_VALUE);
        this.directInflightUploadBytes = new Semaphore(permits);
        this.maxDirectInflightUploadBytes = maxInflightBytes;
        this.directUploadPermits = permits;
        this.directUploadEnabled = true;
        LOGGER.log(Level.INFO,
                "direct multipart upload enabled (issue #638): maxInflightBytes={0}",
                new Object[]{maxInflightBytes});
    }

    /**
     * Issue #638: turns off the direct-S3 upload path. Used by tests and by
     * the promotable wrapper when demoting to a read-only role. Outstanding
     * direct uploads must be drained before calling this.
     */
    public void disableDirectUpload() {
        this.directUploadEnabled = false;
        this.directInflightUploadBytes = null;
        this.maxDirectInflightUploadBytes = 0L;
        this.directUploadPermits = 0;
    }

    @Override
    public boolean supportsDirectMultipartUpload() {
        return directUploadEnabled && directObjectStorage != null;
    }

    /**
     * Issue #638: returns the number of bytes still available in the
     * direct-upload inflight semaphore, or 0 when direct upload is
     * disabled. Used by tests and as a future gauge source.
     */
    public long availableDirectInflightUploadBytes() {
        Semaphore s = this.directInflightUploadBytes;
        return s == null ? 0L : s.availablePermits();
    }

    /**
     * Issue #638: returns the configured maximum in-flight bytes for direct
     * uploads, or 0 when direct upload is disabled.
     */
    public long maxDirectInflightUploadBytes() {
        return maxDirectInflightUploadBytes;
    }

    /**
     * Tracks the set of active data page IDs as of the last successful tableCheckpoint per
     * "{tableSpace}/{uuid}" key. Used to compute the stale-page diff without a full remote listFiles.
     * Populated on the first checkpoint after boot (via listFiles fallback); subsequent checkpoints
     * use the diff path.  Entries are evicted when a table or tablespace is dropped.
     */
    private final ConcurrentHashMap<String, Set<Long>> lastCheckpointedDataPages = new ConcurrentHashMap<>();

    /**
     * Same as {@link #lastCheckpointedDataPages} but for index pages.
     */
    private final ConcurrentHashMap<String, Set<Long>> lastCheckpointedIndexPages = new ConcurrentHashMap<>();

    /**
     * Deferred page deletions keyed by "{tableSpace}/{uuid}". Each entry records pages
     * that became stale at a specific checkpoint LSN and are waiting until it is safe to
     * actually delete them from remote storage. Populated only when retention is enabled.
     */
    private final ConcurrentHashMap<String, List<PendingDeletion>> pendingDataDeletions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<PendingDeletion>> pendingIndexDeletions = new ConcurrentHashMap<>();

    /**
     * When retention is enabled, page deletions are deferred according to the following rules:
     * <ul>
     *   <li>wait at least {@link #minRetentionMillis} after a page became stale (safety grace)</li>
     *   <li>delete when the min replica LSN (from {@link #minReplicaLsnSupplier}) has advanced
     *       past the page's stale-LSN</li>
     *   <li>force-delete after {@link #maxRetentionMillis} even if replicas are behind (safety cap)</li>
     * </ul>
     */
    private volatile boolean retentionEnabled = false;
    private volatile Function<String, LogSequenceNumber> minReplicaLsnSupplier = ts -> null;
    private volatile long minRetentionMillis = 0L;
    private volatile long maxRetentionMillis = Long.MAX_VALUE;

    private final LazyValueCache lazyValueCache;

    /**
     * Shared multipart-block cache used by {@link RemoteRandomAccessReader}
     * on the vector-index search path. Starts as
     * {@link SegmentBlockCache#disabled()} (pass-through, no caching) and is
     * replaced by the indexing-service engine at startup via
     * {@link #setSegmentBlockCache}. Never {@code null}.
     */
    private volatile SegmentBlockCache segmentBlockCache = SegmentBlockCache.disabled();

    /**
     * Stats logger forwarded to every {@link RemoteRandomAccessReader}
     * created by {@link #multipartIndexReaderSupplier}. Starts as
     * {@link NullStatsLogger#INSTANCE} and is replaced by the indexing-service
     * engine alongside the block cache. Never {@code null}.
     */
    private volatile StatsLogger readerStatsLogger = NullStatsLogger.INSTANCE;

    /**
     * Whether to compute XXHash64 and write it as a page footer on the write path.
     * When {@code false}, {@code 0L} (NO_HASH_PRESENT) is stored instead, saving
     * significant CPU during Phase C checkpoints.
     */
    private final boolean pageHashWritesEnabled;

    /**
     * Whether to verify the page footer hash on the read path.
     * When {@code false}, footer verification is skipped entirely.
     */
    private final boolean pageHashChecksEnabled;

    /**
     * Maximum number of paths sent in a single {@code DeleteFiles} RPC during
     * {@link #cleanupAfterTableBoot}. Configured via
     * {@link ServerConfiguration#PROPERTY_REMOTE_FILE_CLEANUP_BATCH_SIZE}.
     */
    private final int cleanupBatchSize;

    /**
     * Counter incremented once per {@code cleanupAfterTableBoot} batch RPC.
     * Always non-null; defaults to a {@link NullStatsLogger}-backed no-op
     * counter when no stats logger is wired in.
     */
    private final Counter cleanupBatchesCounter;

    /**
     * Counter of stale pages actually deleted by {@code cleanupAfterTableBoot}.
     * Always non-null.
     */
    private final Counter cleanupDeletionsCounter;

    /**
     * Latency (microseconds) of each batch RPC issued by
     * {@code cleanupAfterTableBoot}. Always non-null.
     */
    private final OpStatsLogger cleanupBatchLatency;

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client) {
        this(localMetadataDir, tmpDir, swapThreshold, client, new LazyValueCache(0L),
                ServerConfiguration.PROPERTY_REMOTE_FILE_BLOCK_PARALLELISM_DEFAULT,
                ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_REMOTE_FILE_CLEANUP_BATCH_SIZE_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client, LazyValueCache lazyValueCache) {
        this(localMetadataDir, tmpDir, swapThreshold, client, lazyValueCache,
                ServerConfiguration.PROPERTY_REMOTE_FILE_BLOCK_PARALLELISM_DEFAULT,
                ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_REMOTE_FILE_CLEANUP_BATCH_SIZE_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client, LazyValueCache lazyValueCache,
            int blockUploadParallelism) {
        this(localMetadataDir, tmpDir, swapThreshold, client, lazyValueCache,
                blockUploadParallelism,
                ServerConfiguration.PROPERTY_HASH_WRITES_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_HASH_CHECKS_ENABLED_DEFAULT,
                ServerConfiguration.PROPERTY_REMOTE_FILE_CLEANUP_BATCH_SIZE_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client, LazyValueCache lazyValueCache,
            int blockUploadParallelism,
            boolean pageHashWritesEnabled, boolean pageHashChecksEnabled) {
        this(localMetadataDir, tmpDir, swapThreshold, client, lazyValueCache,
                blockUploadParallelism, pageHashWritesEnabled, pageHashChecksEnabled,
                ServerConfiguration.PROPERTY_REMOTE_FILE_CLEANUP_BATCH_SIZE_DEFAULT,
                NullStatsLogger.INSTANCE);
    }

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client, LazyValueCache lazyValueCache,
            int blockUploadParallelism,
            boolean pageHashWritesEnabled, boolean pageHashChecksEnabled,
            int cleanupBatchSize, StatsLogger statsLogger) {
        this.tmpDir = tmpDir;
        this.swapThreshold = swapThreshold;
        this.client = client;
        this.lazyValueCache = lazyValueCache == null ? new LazyValueCache(0L) : lazyValueCache;
        this.blockUploadParallelism = Math.max(1, blockUploadParallelism);
        this.pageHashWritesEnabled = pageHashWritesEnabled;
        this.pageHashChecksEnabled = pageHashChecksEnabled;
        this.cleanupBatchSize = Math.max(1, cleanupBatchSize);
        StatsLogger scope = (statsLogger == null ? NullStatsLogger.INSTANCE : statsLogger).scope("cleanup");
        this.cleanupBatchesCounter = scope.getCounter("batches");
        this.cleanupDeletionsCounter = scope.getCounter("deletions");
        this.cleanupBatchLatency = scope.getOpStatsLogger("batch_latency");
        this.localMetadataManager = new FileDataStorageManager(
                localMetadataDir, tmpDir, swapThreshold,
                false, false, false, false, false,
                new NullStatsLogger());
    }

    /** Value cache used by lazy page loads. */
    LazyValueCache getLazyValueCache() {
        return lazyValueCache;
    }

    /**
     * Client used for all remote I/O. Exposed so callers can read client-side
     * diagnostics such as the in-flight read-permit gauge (issue #246)
     * without having to thread yet another dependency through the engine.
     */
    public RemoteFileServiceClient getClient() {
        return client;
    }

    /**
     * Installs the shared {@link SegmentBlockCache} and stats logger to be
     * used by every {@link RemoteRandomAccessReader} returned from
     * {@link #multipartIndexReaderSupplier}. Intended to be called once at
     * startup, before any vector-index segment is loaded. Pass
     * {@link SegmentBlockCache#disabled()} and/or
     * {@link NullStatsLogger#INSTANCE} to disable either feature without
     * reintroducing null checks on the hot path.
     */
    public void setSegmentBlockCache(SegmentBlockCache cache, StatsLogger statsLogger) {
        this.segmentBlockCache = java.util.Objects.requireNonNull(cache,
                "cache (use SegmentBlockCache.disabled() to disable)");
        this.readerStatsLogger = java.util.Objects.requireNonNull(statsLogger,
                "statsLogger (use NullStatsLogger.INSTANCE to disable)");
        LOGGER.log(Level.INFO,
                "SegmentBlockCache installed: active={0}, maxBytes={1}",
                new Object[]{cache.isActive(), cache.maxBytes()});
    }

    /** Visible for indexing-service gauges. Never {@code null}. */
    public SegmentBlockCache getSegmentBlockCache() {
        return segmentBlockCache;
    }

    /**
     * Enables publication of checkpoint metadata to remote storage for shared-storage read replicas.
     */
    @Override
    public void setSharedCheckpointMetadataManager(herddb.server.SharedCheckpointMetadata manager) {
        this.sharedCheckpointMetadataManager = (SharedCheckpointMetadataManager) manager;
    }

    /**
     * Enables deferred page deletion so that shared-storage read replicas can safely consume
     * pages from old checkpoints. See the class-level documentation for the retention model.
     *
     * @param minReplicaLsnSupplier given a tableSpace UUID, returns the minimum checkpoint LSN
     *        across all currently-registered replicas, or {@code null} if no replicas are tracked
     * @param minRetentionMillis minimum grace period before a page can be deleted, even if all
     *        replicas have advanced past its stale-LSN
     * @param maxRetentionMillis maximum time a page can be retained; after this, it is force-deleted
     *        even if some replicas are still behind (they will need to re-bootstrap)
     */
    @Override
    public void setRetentionPolicy(
            Function<String, LogSequenceNumber> minReplicaLsnSupplier,
            long minRetentionMillis,
            long maxRetentionMillis) {
        this.minReplicaLsnSupplier = minReplicaLsnSupplier != null ? minReplicaLsnSupplier : ts -> null;
        this.minRetentionMillis = Math.max(0, minRetentionMillis);
        this.maxRetentionMillis = maxRetentionMillis <= 0 ? Long.MAX_VALUE : maxRetentionMillis;
        this.retentionEnabled = true;
        LOGGER.log(Level.INFO,
                "Deferred page deletion enabled: minRetention={0}ms, maxRetention={1}ms",
                new Object[]{this.minRetentionMillis, this.maxRetentionMillis});
    }

    // visible for testing
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // visible for testing
    int pendingDataDeletionCount(String tableSpace, String uuid) {
        List<PendingDeletion> pending = pendingDataDeletions.get(tableSpace + "/" + uuid);
        return pending == null ? 0 : pending.size();
    }

    // visible for testing
    int pendingIndexDeletionCount(String tableSpace, String uuid) {
        List<PendingDeletion> pending = pendingIndexDeletions.get(tableSpace + "/" + uuid);
        return pending == null ? 0 : pending.size();
    }

    private static final class PendingDeletion {
        final LogSequenceNumber staleAt;
        final long scheduledAtMillis;
        final String remotePath;
        final String description;
        final String tableSpace;
        final String uuid;

        PendingDeletion(LogSequenceNumber staleAt, long scheduledAtMillis, String remotePath,
                        String description, String tableSpace, String uuid) {
            this.staleAt = staleAt;
            this.scheduledAtMillis = scheduledAtMillis;
            this.remotePath = remotePath;
            this.description = description;
            this.tableSpace = tableSpace;
            this.uuid = uuid;
        }
    }

    /**
     * Evaluates pending deletions for a table/index and returns the subset that is safe to
     * execute now. Safe deletions are removed from the pending list.
     */
    private List<PostCheckpointAction> promotePendingDeletions(
            ConcurrentHashMap<String, List<PendingDeletion>> store, String key, String tableSpace) {
        List<PendingDeletion> pending = store.get(key);
        if (pending == null || pending.isEmpty()) {
            return Collections.emptyList();
        }
        LogSequenceNumber minReplicaLsn = null;
        try {
            minReplicaLsn = minReplicaLsnSupplier.apply(tableSpace);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to query min replica LSN for " + tableSpace
                    + "; retaining pages conservatively", e);
        }
        long now = currentTimeMillis();
        List<PostCheckpointAction> toRun = new ArrayList<>();
        synchronized (pending) {
            Iterator<PendingDeletion> it = pending.iterator();
            while (it.hasNext()) {
                PendingDeletion pd = it.next();
                long ageMs = now - pd.scheduledAtMillis;
                boolean minGracePassed = ageMs >= minRetentionMillis;
                // A replica at LSN L does not reference pages that became stale at LSN pd.staleAt
                // when L >= pd.staleAt. after() is strictly-after, so we check !pd.staleAt.after(L).
                boolean replicasAdvanced = minReplicaLsn != null
                        && !pd.staleAt.after(minReplicaLsn);
                boolean forceByMaxAge = ageMs >= maxRetentionMillis;
                boolean safe = (minGracePassed && replicasAdvanced) || forceByMaxAge;
                if (safe) {
                    toRun.add(new RemoteDeletePageAction(pd.tableSpace, pd.uuid, pd.description,
                            pd.remotePath, client));
                    it.remove();
                    if (forceByMaxAge && !replicasAdvanced) {
                        LOGGER.log(Level.WARNING,
                                "Force-deleting page {0} after {1}ms (min replica LSN: {2}, stale at: {3})",
                                new Object[]{pd.remotePath, ageMs, minReplicaLsn, pd.staleAt});
                    }
                }
            }
        }
        return toRun;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws DataStorageManagerException {
        localMetadataManager.start();
    }

    @Override
    public void close() throws DataStorageManagerException {
        // Issue #411: drain the value cache first so every cache-owned refcount
        // on a direct ByteBuf is released back to the pool. Doing this before
        // any other cleanup guarantees that a partial close (one of the steps
        // below throws) still returns the bulk of the direct memory to the
        // allocator. close() is idempotent, so calling it on an already-closed
        // cache is a no-op.
        lazyValueCache.close();
        localMetadataManager.close();
        // Issue #638: best-effort cleanup of local bulk cache files. These
        // live under tmpDir/bulk-cache/ but we delete each one explicitly so
        // tests that snapshot tmpDir get a clean state and any leftover bulk
        // file from a previous run is gone before the JVM exits.
        for (Path localCache : bulkLocalCache.values()) {
            try {
                Files.deleteIfExists(localCache);
            } catch (IOException ioe) {
                LOGGER.log(Level.FINE,
                        "could not delete local bulk cache file {0} during close: {1}",
                        new Object[]{localCache, ioe.getMessage()});
            }
        }
        bulkLocalCache.clear();
        bulkLayoutCache.clear();
        // Close the direct-S3 client if one was wired in (issue #381).
        // S3AsyncClient + CRT HTTP-client threads are native resources; closing
        // here prevents leaks on pod shutdown and in tests that restart the IS.
        ObjectStorage direct = this.directObjectStorage;
        if (direct != null) {
            try {
                direct.close();
            } catch (Exception e) {
                // ObjectStorage.close() is declared throws Exception by the AutoCloseable
                // interface; concrete implementations (S3ObjectStorage, LocalObjectStorage)
                // only throw unchecked exceptions, but the compiler requires catching the
                // declared checked Exception. Swallowing is correct here: this is best-effort
                // cleanup on shutdown and we must not prevent the rest of close() from running.
                LOGGER.log(Level.WARNING,
                        "error closing direct S3 ObjectStorage on RemoteFileDataStorageManager.close()", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Remote page paths
    // -------------------------------------------------------------------------

    private static String remoteDataPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/data/" + pageId + ".page";
    }

    private static String remoteIndexPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/index/" + pageId + ".page";
    }

    /** System property to override the multipart block size (bytes). Default: 4 MB. */
    public static final String MULTIPART_BLOCK_SIZE_PROPERTY = "herddb.remote.multipart.blockSize";
    private static final int MULTIPART_BLOCK_SIZE =
            Integer.getInteger(MULTIPART_BLOCK_SIZE_PROPERTY, 4 * 1024 * 1024);

    /**
     * System property to override the read-buffer size used by
     * {@link RemoteRandomAccessReader} when serving vector-index searches over
     * remote multipart graph files. Default: 16384 bytes (16 KiB). See
     * issue #104 — this buffer is intentionally decoupled from
     * {@link #MULTIPART_BLOCK_SIZE} so that HNSW graph traversals do not fetch
     * multi-MiB windows per miss.
     *
     * <p>The 16 KiB default is sized to absorb a single jvector logical read in
     * one wire round-trip. The dominant per-node read during search is the full-
     * resolution vector fetched by {@code OnDiskGraphIndex.getVectorInto}
     * for re-ranking, which reads {@code dimension * 4} bytes in a single
     * {@code readFloatVector} call — 3840 bytes for GIST1M (dim=960), 6144 bytes
     * for 1536-dim embeddings. A 4 KiB buffer would split those reads across
     * two wire round-trips whenever the position is unaligned; 16 KiB keeps a
     * raw vector up to ~4096 dimensions in a single fetch while still being
     * 256× smaller than the 4 MiB write block and an exact divisor of it.
     */
    public static final String READ_BUFFER_SIZE_PROPERTY = "herddb.vector.remote.read.bufferSize";
    static final int READ_BUFFER_SIZE =
            Integer.getInteger(READ_BUFFER_SIZE_PROPERTY, 16 * 1024);

    private static String remoteMultipartPath(String tableSpace, String uuid, String fileType) {
        return tableSpace + "/" + uuid + "/multipart/" + fileType;
    }

    private static String remoteDataPrefix(String tableSpace, String uuid) {
        return tableSpace + "/" + uuid + "/data/";
    }

    private static String remoteIndexPrefix(String tableSpace, String uuid) {
        return tableSpace + "/" + uuid + "/index/";
    }

    private static String remoteTablespacePrefix(String tableSpace) {
        return tableSpace + "/";
    }

    private static long pageIdFromRemotePath(String path) {
        int slash = path.lastIndexOf('/');
        String filename = path.substring(slash + 1);
        if (filename.endsWith(".page")) {
            try {
                return Long.parseLong(filename.substring(0, filename.length() - ".page".length()));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    // -------------------------------------------------------------------------
    // Page serialization (matches FileDataStorageManager format)
    // -------------------------------------------------------------------------

    private ByteBuf serializeIndexPage(DataWriter writer) throws IOException {
        // Use a direct ByteBuf so that Bytes.writeTo(ByteBuf) for off-heap-backed
        // keys (IndexKeySlab slabs) performs a direct-to-direct copy with no heap
        // byte[] allocation per key (issue #497).
        // Pre-size using the writer's estimate (+16 for outer version/flags VLongs and hash).
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(writer.sizeEstimate() + 16);
        try {
            herddb.utils.ByteBufUtils.writeVLong(buf, 1); // outer version
            herddb.utils.ByteBufUtils.writeVLong(buf, 0); // outer flags
            writer.write(new ByteBufDataOutput(buf));
            int payloadLen = buf.writerIndex();
            long hash;
            if (pageHashWritesEnabled) {
                // One heap allocation per page (not per key): copy the payload
                // bytes to a temporary array for XXHash64 computation.
                // This is far cheaper than the per-key to_array() it replaces.
                byte[] tmp = new byte[payloadLen];
                buf.getBytes(0, tmp, 0, payloadLen);
                hash = XXHash64Utils.hash(tmp, 0, payloadLen);
            } else {
                hash = 0L; // NO_HASH_PRESENT
            }
            buf.writeLong(hash); // footer
        } catch (IOException e) {
            buf.release();
            throw e;
        }
        return buf;
    }

    private static <X> X deserializeIndexPage(byte[] data, DataReader<X> reader)
            throws IOException, DataStorageManagerException {
        try (ByteBufCursor dataIn = ByteBufCursor.wrap(data)) {
            long version = dataIn.readVLong();
            long flags = dataIn.readVLong();
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted remote index page");
            }
            return reader.read(dataIn);
        }
    }

    // -------------------------------------------------------------------------
    // Remote page operations
    // -------------------------------------------------------------------------

    @Override
    public List<Record> readPage(String tableSpace, String uuid, Long pageId)
            throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        int blockSize = client.getBlockSize();
        byte[] headerBytes = client.readFileRange(path, 0L,
                LazyDataPageFormat.FIXED_HEADER_SIZE, blockSize);
        if (headerBytes == null || headerBytes.length < LazyDataPageFormat.FIXED_HEADER_SIZE) {
            throw new DataPageDoesNotExistException(
                    "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        LazyDataPageFormat.FixedHeader h;
        ByteBuf headerBuf = io.netty.buffer.Unpooled.wrappedBuffer(headerBytes);
        try {
            h = LazyDataPageFormat.readHeader(headerBuf);
        } finally {
            headerBuf.release();
        }
        long totalSize = h.totalSize();
        if (totalSize > (long) Integer.MAX_VALUE) {
            throw new DataStorageManagerException(
                    "remote page too big to read eagerly: " + path + " totalSize=" + totalSize);
        }
        byte[] full = client.readFileRange(path, 0L, (int) totalSize, blockSize);
        if (full == null || full.length < totalSize) {
            throw new DataStorageManagerException(
                    "short read for remote page " + path + ": expected " + totalSize
                            + " got " + (full == null ? 0 : full.length));
        }
        ByteBuf buf = io.netty.buffer.Unpooled.wrappedBuffer(full);
        try {
            return LazyDataPageFormat.readAllRecords(buf, pageHashChecksEnabled);
        } finally {
            buf.release();
        }
    }

    @Override
    public void writePage(String tableSpace, String uuid, long pageId,
            Collection<Record> newPage) throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        ByteBuf buf = LazyDataPageFormat.write(newPage, pageHashWritesEnabled);
        try {
            writeAsMultipart(path, buf);
        } finally {
            buf.release();
        }
        // Any cached values under this (tableSpace/uuid/pageId) now refer to
        // stale bytes — drop them so a subsequent lazy read sees the new page.
        lazyValueCache.invalidateForPage(tableSpace, uuid, pageId);
    }

    /**
     * Writes {@code buf} as a multipart object at {@code path} so that
     * subsequent {@link RemoteFileServiceClient#readFileRange} calls can
     * satisfy byte-range reads. Blocks larger than a single server block
     * are split into consecutive blocks.
     * <p>
     * For multi-block payloads, block uploads are dispatched concurrently
     * via {@link RemoteFileServiceClient#writeFileBlockAsync}, bounded to
     * {@link #blockUploadParallelism} in-flight at any time. Each slice is
     * a view over the caller-owned parent {@code buf}; the parent must stay
     * live until this method returns, which is guaranteed because every
     * submitted future is joined before we return (success or failure).
     * Writes to the same {@code path} target different {@code blockIndex}
     * keys and can legally complete out of order.
     */
    private void writeAsMultipart(String path, ByteBuf buf) {
        final int blockSize = client.getBlockSize();
        final int total = buf.readableBytes();
        if (total == 0) {
            // empty content: still create block 0 so readers see a valid file
            client.writeFileBlock(path, 0L, io.netty.buffer.Unpooled.EMPTY_BUFFER);
            return;
        }
        if (total <= blockSize) {
            // single-block fast path — avoid CompletableFuture / semaphore overhead
            client.writeFileBlock(path, 0L, buf.slice(buf.readerIndex(), total));
            return;
        }
        final int maxInFlight = Math.max(1, blockUploadParallelism);
        final Semaphore permits = new Semaphore(maxInFlight);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        long blockIndex = 0L;
        int pos = buf.readerIndex();
        int remaining = total;
        RuntimeException submissionFailure = null;
        try {
            while (remaining > 0) {
                permits.acquire();
                int chunk = Math.min(remaining, blockSize);
                ByteBuf slice = buf.slice(pos, chunk);
                CompletableFuture<Void> future;
                try {
                    future = client.writeFileBlockAsync(path, blockIndex, slice);
                } catch (RuntimeException ex) {
                    // Synchronous failure while submitting the stub call; release the permit,
                    // stop submitting, and fall through to drain the in-flight futures so
                    // the parent ByteBuf is safe to release after we return.
                    permits.release();
                    submissionFailure = ex;
                    break;
                }
                futures.add(future.whenComplete((r, t) -> permits.release()));
                pos += chunk;
                remaining -= chunk;
                blockIndex++;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Drain whatever futures we did submit; the parent ByteBuf belongs to the caller.
            joinAllSilently(futures);
            throw new RuntimeException("Interrupted during parallel block upload of " + path, ex);
        }
        CompletionException joinFailure = null;
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (CompletionException ex) {
            joinFailure = ex;
        }
        if (submissionFailure != null) {
            throw submissionFailure;
        }
        if (joinFailure != null) {
            Throwable cause = joinFailure.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw joinFailure;
        }
    }

    /**
     * Join every future unconditionally so the caller can safely release the
     * parent ByteBuf even when some blocks failed. Individual failures are
     * swallowed here — the outer {@code allOf(...).join()} call in the
     * happy-path surfaces the root cause.
     */
    private static void joinAllSilently(List<CompletableFuture<Void>> futures) {
        for (CompletableFuture<Void> f : futures) {
            try {
                f.join();
            } catch (CompletionException ignored) {
                // another future already carries the original error; we just need the buf pinned
                // until every in-flight wire round-trip is done, success or failure.
            }
        }
    }

    // -------------------------------------------------------------------------
    // Lazy read helpers (range-read v2 pages)
    // -------------------------------------------------------------------------

    /**
     * Reads the 22-byte fixed header of a v2 page via a byte-range read and
     * returns the parsed counts/sizes. Throws
     * {@link DataPageDoesNotExistException} if the remote file is absent.
     */
    LazyDataPageFormat.FixedHeader readPageHeader(String tableSpace, String uuid, long pageId)
            throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        byte[] headerBytes;
        try {
            headerBytes = client.readFileRange(path, 0L,
                    LazyDataPageFormat.FIXED_HEADER_SIZE, client.getBlockSize());
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                throw new DataPageDoesNotExistException(
                        "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
            }
            throw new DataStorageManagerException("Error reading remote page header: " + path, e);
        }
        if (headerBytes == null || headerBytes.length < LazyDataPageFormat.FIXED_HEADER_SIZE) {
            throw new DataPageDoesNotExistException(
                    "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        ByteBuf tmp = io.netty.buffer.Unpooled.wrappedBuffer(headerBytes);
        try {
            return LazyDataPageFormat.readHeader(tmp);
        } finally {
            tmp.release();
        }
    }

    /**
     * Reads the index section of a v2 page via a byte-range read and returns
     * the per-record metadata (key + value offset + value length).
     */
    List<LazyDataPageFormat.RecordMetadata> readPageIndex(String tableSpace, String uuid,
            long pageId, LazyDataPageFormat.FixedHeader h) throws DataStorageManagerException {
        if (h.indexSize == 0) {
            return Collections.emptyList();
        }
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        byte[] indexBytes;
        try {
            indexBytes = client.readFileRange(path,
                    LazyDataPageFormat.FIXED_HEADER_SIZE, h.indexSize, client.getBlockSize());
        } catch (RuntimeException e) {
            throw new DataStorageManagerException("Error reading remote page index: " + path, e);
        }
        if (indexBytes == null || indexBytes.length < h.indexSize) {
            throw new DataStorageManagerException("Short read for remote page index: " + path);
        }
        ByteBuf tmp = io.netty.buffer.Unpooled.wrappedBuffer(indexBytes);
        try {
            // Pass valueSize so corrupted index entries are rejected at parse
            // time on the lazy-load path (issue #416), surfacing a single
            // DataStorageManagerException instead of a downstream
            // NegativeArraySizeException / IndexOutOfBoundsException.
            return LazyDataPageFormat.readIndex(tmp, h.numRecords, h.valueSize);
        } finally {
            tmp.release();
        }
    }

    /**
     * Fetches a single record value from a v2 page, consulting the value
     * cache first and issuing a byte-range read against remote storage on
     * miss.
     *
     * <p><b>Issue #411 — off-heap return</b>: returns a direct
     * {@link ByteBuf} retained slice from the
     * {@link LazyValueCache}'s pool. The caller owns one refcount and
     * <b>must</b> release it (typically by handing the slice to
     * {@link herddb.utils.Bytes#fromOffHeap(ByteBuf)} whose
     * own lifecycle returns the refcount to the pool on
     * {@link herddb.utils.Bytes#release()} or on lazy materialisation).
     *
     * <p>For zero-length values the returned buffer is the empty
     * {@link io.netty.buffer.Unpooled#EMPTY_BUFFER}; releasing it is a
     * no-op so callers do not need to special-case empty values.
     */
    ByteBuf readPageValue(String tableSpace, String uuid, long pageId,
            LazyDataPageFormat.FixedHeader h, long valueOffset, int valueLength)
            throws DataStorageManagerException {
        if (valueLength == 0) {
            return io.netty.buffer.Unpooled.EMPTY_BUFFER;
        }
        LazyValueCache.ValueKey key = new LazyValueCache.ValueKey(
                tableSpace, uuid, pageId, valueOffset);
        try {
            return lazyValueCache.getOrFetch(key, () -> {
                long absolute = LazyDataPageFormat.absoluteValueOffset(h, valueOffset);
                String path = remoteDataPagePath(tableSpace, uuid, pageId);
                byte[] bytes = client.readFileRange(path, absolute, valueLength, client.getBlockSize());
                if (bytes == null || bytes.length < valueLength) {
                    throw new IllegalStateException("Short read for value at "
                            + path + "[" + absolute + "+" + valueLength + "]");
                }
                return bytes;
            });
        } catch (RuntimeException e) {
            // Broad catch: the loader delegates to the remote client, which can raise
            // any unchecked exception on a wire failure. Wrapping as
            // DataStorageManagerException keeps the method contract and lets
            // LazyDataPage.get() route the error into a HerdDBInternalException
            // that TableManager's defensive catches unwind (issue #181).
            throw new DataStorageManagerException(e.getMessage(), e);
        }
    }

    @Override
    public boolean supportsLazyPageLoad() {
        return true;
    }

    @Override
    public herddb.core.DataPage loadLazyDataPage(String tableSpace, String uuid, Long pageId,
            herddb.core.TableManager owner, long maxSize) throws DataStorageManagerException {
        LazyDataPageFormat.FixedHeader h = readPageHeader(tableSpace, uuid, pageId);
        List<LazyDataPageFormat.RecordMetadata> metadata = readPageIndex(tableSpace, uuid, pageId, h);
        return LazyDataPage.build(owner, pageId, maxSize,
                this, tableSpace, uuid, h, metadata);
    }

    private static boolean isNotFound(RuntimeException e) {
        // readFileRangeAsync surfaces server-side errors via TYPE_ERROR PDUs
        // wrapped in IOException; surface common "not found" conditions as
        // DataPageDoesNotExistException so callers can distinguish from a
        // protocol/network-level failure.
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("NOT_FOUND") || msg.contains("does not exist"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String uuid, Long pageId, DataReader<X> reader)
            throws DataStorageManagerException {
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        byte[] data = client.readFile(path);
        if (data == null) {
            throw new DataStorageManagerException(
                    "No such remote index page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        try {
            return deserializeIndexPage(data, reader);
        } catch (IOException e) {
            throw new DataStorageManagerException("Error reading remote index page: " + path, e);
        }
    }

    @Override
    public void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer) {
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        try {
            ByteBuf buf = serializeIndexPage(writer);
            try {
                client.writeFile(path, buf);
            } finally {
                buf.release();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing remote index page: " + path, e);
        }
    }

    @Override
    public CompletableFuture<Void> writeIndexPageAsync(String tableSpace, String uuid, long pageId,
            DataWriter writer) {
        // Serialize synchronously on the caller thread (so the DataWriter state is fully
        // consumed before we return), then dispatch the network write asynchronously.
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        final ByteBuf buf;
        try {
            buf = serializeIndexPage(writer);
        } catch (IOException e) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new RuntimeException("Error serializing remote index page: " + path, e));
            return failed;
        }
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.writeFileAsync(path, buf).whenComplete((bytesWritten, err) -> {
                try {
                    if (err != null) {
                        result.completeExceptionally(err);
                    } else {
                        result.complete(null);
                    }
                } finally {
                    buf.release();
                }
            });
        } catch (RuntimeException ex) {
            // Synchronous failure building the stub: release the buf now and fail the future.
            buf.release();
            result.completeExceptionally(ex);
        }
        return result;
    }

    @Override
    public void deleteIndexPage(String tableSpace, String uuid, long pageId)
            throws DataStorageManagerException {
        // Used by PersistentVectorStore's Phase-B rollback path to reclaim
        // pages that were written but never made it into a durable
        // IndexStatus checkpoint. Without this override we fall through to
        // the no-op base implementation and leak S3 objects until the next
        // successful indexCheckpoint sweep — which may never come if the
        // failure cause (e.g. remote storage unreachable) keeps recurring.
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        try {
            client.deleteFile(path);
        } catch (RuntimeException ignored) {
            // Idempotent: deleting a page that was never written must not
            // throw. Log-worthy but not fatal; the caller already has the
            // original failure in hand.
            LOGGER.log(Level.FINE,
                    "deleteIndexPage: non-fatal error deleting {0}: {1}",
                    new Object[]{path, ignored.getMessage()});
        }
    }

    // -------------------------------------------------------------------------
    // Multipart large-file support (FusedPQ graphs, map data, etc.)
    // -------------------------------------------------------------------------

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          Path tempFile, LongConsumer progress)
            throws IOException, DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        // Any previously-cached probe state for this logical file is stale the
        // moment we begin a write; invalidate proactively so a concurrent
        // probe observes the fresh layout once the upload completes.
        bulkLayoutCache.remove(logicalPath);
        // Issue #638: when the direct-S3 upload path is wired, dispatch the
        // upload as a single S3 object via S3TransferManager.uploadFile()
        // (real S3 Multipart Upload, parts pipelined by the CRT HTTP client).
        // Falls back to the gRPC per-block path when direct upload is off.
        if (supportsDirectMultipartUpload()) {
            return writeMultipartIndexFileDirect(logicalPath, tempFile, progress);
        }
        int blockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        long totalBytes;
        try (InputStream raw = Files.newInputStream(tempFile);
             InputStream in = progress != null
                     ? new CountingInputStream(raw, progress)
                     : raw) {
            totalBytes = client.writeMultipartFile(logicalPath, in, blockSize);
        }
        LOGGER.log(Level.INFO,
                "writeMultipartIndexFile: {0} written {1} bytes in blocks of {2}",
                new Object[]{logicalPath, totalBytes, blockSize});
        return logicalPath;
    }

    /**
     * Issue #638: direct-S3 upload path. Reserves up to
     * {@code min(fileSize, directUploadPermits)} permits from the in-flight
     * upload semaphore — a file larger than the cap holds the full cap for
     * the duration of the upload, which serialises large uploads behind one
     * another. Invokes
     * {@link ObjectStorage#uploadFile(String, Path, LongConsumer)} against
     * the single bulk-layout key {@code logicalPath + ".bulk"}, and releases
     * all permits when the future completes regardless of outcome.
     *
     * <p>On any failure the partial S3 object is deleted best-effort so we
     * never leave an orphaned half-written {@code .bulk} key behind. The
     * permit-release-on-failure pattern mirrors issue #575 from the gRPC
     * path: a single idempotent releaser is registered with
     * {@link CompletableFuture#whenComplete} so a thrown exception does not
     * stall the semaphore for the network timeout.
     *
     * @return the same logical path the gRPC variant returns — opaque to
     *         callers
     */
    private String writeMultipartIndexFileDirect(String logicalPath, Path tempFile,
                                                 LongConsumer progress) throws IOException {
        final ObjectStorage storage = this.directObjectStorage;
        if (storage == null) {
            // supportsDirectMultipartUpload() guarantees storage != null at the
            // call site, but a concurrent disableDirectUpload() could race
            // between the check and here; surface a clean fallback to gRPC by
            // throwing an UnsupportedOperationException, which the test layer
            // can also assert on.
            throw new UnsupportedOperationException(
                    "Direct S3 not configured on this RemoteFileDataStorageManager");
        }
        final String bulkPath = logicalPath + BULK_LAYOUT_SUFFIX;
        final long fileSize = Files.size(tempFile);
        // Reserve permits before launching the upload. A zero-byte file
        // skips the reservation entirely (no inflight bytes to bound).
        final Runnable release = fileSize > 0L
                ? reserveDirectInflightUploadBytes(fileSize)
                : () -> { };
        final long startNanos = System.nanoTime();
        try {
            long uploaded = storage.uploadFile(bulkPath, tempFile, progress)
                    .whenComplete((bytes, err) -> release.run())
                    .get();
            LOGGER.log(Level.INFO,
                    "writeMultipartIndexFile(direct): {0} uploaded {1} bytes (bulk layout) in {2} ms",
                    new Object[]{bulkPath, uploaded,
                            TimeUnit.NANOSECONDS.toMillis(
                                    System.nanoTime() - startNanos)});
            // Mark the bulk layout as present so subsequent probes (existence
            // check, reader supplier, download path) take the bulk branch
            // without re-issuing a HEAD round-trip.
            bulkLayoutCache.put(logicalPath, Boolean.TRUE);
            return logicalPath;
        } catch (InterruptedException e) {
            // Permit already released by whenComplete; we still need to
            // attempt the orphan cleanup since the storage future may have
            // partially completed on the wire.
            Thread.currentThread().interrupt();
            bestEffortDeleteBulkOrphan(storage, bulkPath);
            throw new IOException(
                    "Interrupted while uploading multipart " + bulkPath, e);
        } catch (java.util.concurrent.ExecutionException e) {
            // Permit already released by whenComplete (idempotent). Clean up
            // any partial object so retries are not blocked by an orphan.
            bestEffortDeleteBulkOrphan(storage, bulkPath);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("direct multipart upload failed for " + bulkPath, cause);
        }
    }

    /**
     * Issue #638: best-effort cleanup of a partial {@code .bulk} S3 object
     * after a failed direct upload. Errors during the cleanup itself are
     * logged at {@code WARNING} but never propagated — the caller has
     * already decided to fail the write and a stuck orphan only widens the
     * blast radius into the next retry. The S3 Multipart Upload itself is
     * aborted internally by the CRT client on upload failure (no orphan
     * parts), so this delete only targets the rare case where the upload
     * completed enough to materialise the final object before some other
     * step (e.g. cancellation) failed.
     */
    private static void bestEffortDeleteBulkOrphan(ObjectStorage storage, String bulkPath) {
        try {
            storage.delete(bulkPath).get(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING,
                    "interrupted during best-effort cleanup of orphan bulk object {0}",
                    new Object[]{bulkPath});
        } catch (java.util.concurrent.ExecutionException
                | java.util.concurrent.TimeoutException
                | RuntimeException cleanupErr) {
            // RuntimeException catch is required because some S3 backends surface
            // delete errors as unchecked AWS SDK exceptions (e.g. SdkServiceException
            // subclasses). This is best-effort cleanup; swallowing here is correct
            // because the storage-level orphan is no worse than the original write
            // failure that triggered it.
            LOGGER.log(Level.WARNING,
                    "best-effort cleanup of orphan bulk object {0} failed: {1}",
                    new Object[]{bulkPath, cleanupErr.toString()});
        }
    }

    /**
     * Issue #638: acquires permits from {@link #directInflightUploadBytes}
     * for a direct upload of {@code bytes}. Returns an idempotent releaser
     * runnable. Mirrors the deadlock-prevention pattern from
     * {@code RemoteFileServiceClient.acquireInflightWriteBytes}: when the
     * payload exceeds the semaphore's total capacity we acquire up to the
     * cap (smaller concurrent uploads can interleave between chunks).
     */
    private Runnable reserveDirectInflightUploadBytes(long bytes) throws IOException {
        Semaphore s = this.directInflightUploadBytes;
        if (s == null) {
            // disableDirectUpload() raced with the upload — return a no-op
            // releaser; callers are still responsible for handling the
            // upcoming UnsupportedOperationException from uploadFile.
            return () -> { };
        }
        int toAcquire = (int) Math.min(bytes, this.directUploadPermits);
        if (bytes > this.directUploadPermits) {
            LOGGER.log(Level.WARNING,
                    "direct-upload payload ({0} bytes) exceeds inflight cap ({1} bytes);"
                            + " will hold up to {1} permits at a time during this upload"
                            + " — consider raising the configured limit",
                    new Object[]{bytes, (long) this.directUploadPermits});
        }
        if (!s.tryAcquire(toAcquire)) {
            LOGGER.log(Level.WARNING,
                    "direct-upload inflight reservation blocked "
                            + "(requested={0} bytes, available={1}/{2}); waiting",
                    new Object[]{toAcquire, s.availablePermits(),
                            this.maxDirectInflightUploadBytes});
            long startNanos = System.nanoTime();
            // Bounded wait (10 min). A stuck TM future (CRT bug, network partition)
            // would otherwise stall every subsequent direct upload silently forever.
            // 10 min is generous enough for any realistic multipart upload.
            try {
                boolean acquired = s.tryAcquire(toAcquire, 10L,
                        TimeUnit.MINUTES);
                if (!acquired) {
                    throw new IOException(
                            "direct-upload inflight semaphore timed out after 10 minutes "
                                    + "(requested=" + toAcquire + " bytes, available="
                                    + s.availablePermits() + "/"
                                    + this.maxDirectInflightUploadBytes
                                    + "). Consider raising "
                                    + "indexing.remote.file.client.max.inflight.direct.write.bytes.");
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while waiting for direct-upload inflight semaphore", ie);
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos);
            if (elapsedMs >= 50L) {
                LOGGER.log(Level.WARNING,
                        "direct-upload inflight reservation unblocked after {0} ms"
                                + " (requested={1} bytes)",
                        new Object[]{elapsedMs, toAcquire});
            }
        }
        AtomicBoolean released =
                new AtomicBoolean(false);
        final int finalAcquired = toAcquire;
        return () -> {
            if (released.compareAndSet(false, true)) {
                s.release(finalAcquired);
            }
        };
    }

    /**
     * Wraps an InputStream and reports the number of bytes read since the last
     * report via a {@link LongConsumer}. Used by Phase-B multipart uploads so
     * that the PersistentVectorStore can expose mid-flight upload progress.
     */
    private static final class CountingInputStream extends FilterInputStream {
        private final LongConsumer progress;

        CountingInputStream(InputStream in, LongConsumer progress) {
            super(in);
            this.progress = progress;
        }

        @Override
        public int read() throws IOException {
            int r = super.read();
            if (r >= 0) {
                progress.accept(1L);
            }
            return r;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int r = super.read(b, off, len);
            if (r > 0) {
                progress.accept(r);
            }
            return r;
        }
    }

    @Override
    public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
            String tableSpace, String uuid, String fileType, long fileSize)
            throws DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        // Issue #638: bulk-layout files are stored as a single S3 object at
        // {logicalPath}.bulk. Random-access reads against the bulk object
        // happen via a memory-mapped reader over a local cache file that we
        // materialise on first call; subsequent suppliers reuse the same
        // cache file. Per-block files (legacy layout) keep the existing
        // remote-random-access path.
        // Issue #645: use the strict probe so a transient HEAD failure (5xx,
        // network blip) does not silently fall through to the gRPC reader
        // for a file that was actually written via direct-S3. The gRPC
        // file-server has no record of direct-uploaded blocks, so falling
        // back to it would always surface as "Block not found" and crash
        // the IS on restart.
        boolean bulk;
        try {
            bulk = isBulkLayoutOrThrow(logicalPath);
        } catch (IOException ioe) {
            throw new DataStorageManagerException(
                    "bulk-layout probe failed for " + logicalPath
                            + "; refusing to fall back to gRPC because direct-S3"
                            + " is wired (issue #645)", ioe);
        }
        if (bulk) {
            try {
                Path localFile = ensureBulkLocalCacheFile(logicalPath, fileSize);
                return new io.github.jbellis.jvector.disk.MappedChunkReader.Supplier(localFile);
            } catch (IOException ioe) {
                throw new DataStorageManagerException(
                        "Failed to materialise bulk multipart file " + logicalPath, ioe);
            }
        }
        int writeBlockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        return new RemoteRandomAccessReader.Supplier(
                client, logicalPath, fileSize, writeBlockSize, READ_BUFFER_SIZE,
                readerStatsLogger, segmentBlockCache);
    }

    /**
     * Issue #638/#645: lenient probe — returns {@code true} when the
     * logical multipart file is stored in the bulk layout (single S3 object
     * at {@code {logicalPath}.bulk}). Honours the in-memory probe cache so
     * repeated calls for the same logical path issue at most one S3
     * {@code HEAD}. Returns {@code false} for legacy per-block files,
     * when the direct-S3 client is not wired, and — best-effort — when
     * the HEAD probe fails transiently.
     *
     * <p>This is the boolean-only variant used by
     * {@link #multipartIndexFileExists} (whose API contract is "best-effort
     * presence check"). The read paths
     * ({@link #multipartIndexReaderSupplier} and
     * {@link #downloadMultipartIndexFile}) MUST use
     * {@link #isBulkLayoutOrThrow(String)} instead so a transient probe
     * failure surfaces loudly instead of silently misrouting through the
     * gRPC file-server, which is the failure mode that crash-looped the
     * indexing service in issue #645.
     */
    private boolean isBulkLayout(String logicalPath) {
        try {
            return isBulkLayoutOrThrow(logicalPath);
        } catch (IOException probeErr) {
            // Lenient-probe path: keep the historical contract of
            // multipartIndexFileExists ("best-effort, never throws") by
            // mapping a transient probe failure to a conservative {@code
            // false} answer. A {@code false} from this method on the
            // exists-check path is OK because the caller follows up with a
            // legacy per-block probe; a {@code false} on a read path would
            // be unsafe, which is why read paths now call
            // {@link #isBulkLayoutOrThrow} directly.
            LOGGER.log(Level.FINE,
                    "bulk-layout probe failed for {0} (best-effort, returning false): {1}",
                    new Object[]{logicalPath, probeErr.toString()});
            return false;
        }
    }

    /**
     * Issue #645: strict probe — returns {@code true} when the logical
     * multipart file is stored in the bulk layout, {@code false} when it
     * is definitively absent (HEAD 404 / {@code NOT_FOUND}), and throws
     * {@link IOException} when the existence could not be determined
     * (transient network error, HTTP 5xx, SDK error, timeout, interrupted).
     *
     * <p>Honours the in-memory probe cache so repeated calls for the same
     * logical path issue at most one S3 {@code HEAD}. Only positive
     * results are cached; transient probe failures are <em>not</em>
     * cached (cf. the issue-#638 review note on {@link #isBulkLayout}).
     *
     * <p>The contract on the underlying
     * {@link ObjectStorage#existsObject(String)} call is tri-state as of
     * issue #645: {@code true} / {@code false} (definitive 404) /
     * exceptional. This method translates those three terminal states
     * directly: {@code true} → {@code true}, {@code false} → {@code false},
     * exceptional → {@code throw IOException}. Read-path callers MUST
     * surface that throw instead of falling back to gRPC, because the
     * gRPC file-server has no record of direct-S3-uploaded blocks and
     * the fallback path would always surface as {@code Block not found}.
     */
    private boolean isBulkLayoutOrThrow(String logicalPath) throws IOException {
        ObjectStorage storage = this.directObjectStorage;
        if (storage == null) {
            // No direct-S3 wiring at all: this installation only uses the
            // gRPC file-server. The probe is structurally {@code false}.
            return false;
        }
        Boolean cached = bulkLayoutCache.get(logicalPath);
        if (cached != null) {
            return cached;
        }
        boolean present;
        try {
            present = Boolean.TRUE.equals(
                    storage.existsObject(logicalPath + BULK_LAYOUT_SUFFIX).get(
                            15L, TimeUnit.SECONDS));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    "interrupted while probing bulk layout for " + logicalPath, ie);
        } catch (java.util.concurrent.TimeoutException te) {
            // A HEAD that does not return within 15 s is a transient
            // condition. Don't cache; let the caller decide whether to
            // retry or fail the read.
            throw new IOException(
                    "bulk-layout HEAD probe timed out for " + logicalPath, te);
        } catch (java.util.concurrent.ExecutionException ee) {
            // existsObject completed exceptionally — per the tri-state
            // contract this means "could not determine" (5xx, network,
            // generic SDK error). NoSuchKey / 404 collapse to {@code
            // false} inside existsObject itself and never reach here.
            Throwable cause = ee.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(
                    "bulk-layout HEAD probe failed for " + logicalPath, cause);
        }
        // Only cache positive results. Caching false permanently would pin
        // a logical path as "not bulk" for the entire JVM lifetime, causing
        // read failures in two scenarios:
        //   (1) A writer removes the cache entry (before upload) and a
        //       concurrent probe fires a HEAD that returns false (upload still
        //       in flight). If we cached that false, a subsequent put(TRUE) from
        //       the writer would race against our stale put(false), and whichever
        //       wins last pins the wrong state.
        //   (2) A legacy path later rewritten in bulk format (by another pod or
        //       after restart) would remain permanently invisible until JVM restart.
        // The cost is one extra HEAD per cold read on non-bulk paths — acceptable
        // given that probes are cheap and uploads are not.
        if (present) {
            bulkLayoutCache.put(logicalPath, Boolean.TRUE);
        }
        return present;
    }

    /**
     * Issue #638: returns a local file path containing the full byte content
     * of the bulk-layout multipart file at {@code logicalPath}. The file is
     * downloaded once (the first time this method is called for a given
     * logical path) into the manager's tmp directory; subsequent callers
     * reuse the same path so the same {@code MappedChunkReader.Supplier} can
     * memory-map it. The cache entry is removed by
     * {@link #deleteMultipartIndexFile} and the local file is deleted on
     * {@link #close()}.
     *
     * <p>{@code expectedSize} is informational — used in log lines but not
     * for validation. The actual file size is whatever {@code S3TransferManager.downloadFile}
     * materialised.
     */
    private Path ensureBulkLocalCacheFile(String logicalPath, long expectedSize)
            throws IOException {
        // Fast path: already cached.
        Path cached = bulkLocalCache.get(logicalPath);
        if (cached != null) {
            return cached;
        }
        ObjectStorage storage = this.directObjectStorage;
        if (storage == null) {
            throw new IOException(
                    "Direct S3 not configured — cannot materialise bulk file " + logicalPath);
        }
        // Place the cache file under the manager's tmp directory so it
        // travels with the rest of the IS local state on restart cleanup.
        Path cacheDir = tmpDir.resolve("bulk-cache");
        Files.createDirectories(cacheDir);
        String safeName = logicalPath.replace('/', '_').replace('\\', '_');
        // Download to a UNIQUE temp file per call to prevent concurrent callers
        // for the same logical path from writing to the same destination path
        // simultaneously (which would interleave bytes and corrupt the output).
        // putIfAbsent below elects exactly one caller's download as the canonical
        // cache entry; all losers delete their copies.
        Path tmpFile = cacheDir.resolve(
                safeName + "." + UUID.randomUUID() + BULK_LAYOUT_SUFFIX);
        try {
            storage.downloadFileBulk(logicalPath + BULK_LAYOUT_SUFFIX, tmpFile).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Clean up partial file so a retry starts from scratch.
            Files.deleteIfExists(tmpFile);
            throw new IOException(
                    "interrupted while materialising bulk file " + logicalPath, ie);
        } catch (java.util.concurrent.ExecutionException ee) {
            Files.deleteIfExists(tmpFile);
            Throwable cause = ee.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(
                    "failed to materialise bulk file " + logicalPath, cause);
        }
        // Race: another caller may have finished downloading for the same
        // logical path concurrently. putIfAbsent atomically elects the winner.
        // The loser deletes its temp file (identical content, just redundant).
        Path existing = bulkLocalCache.putIfAbsent(logicalPath, tmpFile);
        if (existing != null) {
            // We lost the race — delete our temp download and use the winner's.
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException cleanupErr) {
                LOGGER.log(Level.FINE,
                        "could not clean up duplicate bulk cache download {0}: {1}",
                        new Object[]{tmpFile, cleanupErr.getMessage()});
            }
            return existing;
        }
        LOGGER.log(Level.INFO,
                "materialised bulk multipart file {0} (expectedSize={1}) to local cache {2}",
                new Object[]{logicalPath, expectedSize, tmpFile});
        return tmpFile;
    }

    /**
     * Issue #617 + pr-reviewer follow-up #2: backend-specific override of the
     * {@code multipartIndexFileExists} probe. The default
     * {@link DataStorageManager#multipartIndexFileExists} implementation
     * builds a {@link RemoteRandomAccessReader} with a fictitious
     * {@code fileSize=1L} and then calls {@code readInt()} on it — that
     * combination triggers an infinite loop in
     * {@link RemoteRandomAccessReader#readFully(byte[])} because the loop's
     * {@code toCopy} becomes zero once {@code position} matches the fake
     * {@code totalSize}. We bypass the reader entirely and probe block-0
     * directly via the file-server client's {@code readFileRange} API,
     * which returns {@code null} when the underlying block is absent.
     *
     * <p>4 bytes is the same probe length the base contract documents (the
     * default impl does {@code readInt()}), so the contract gap that
     * {@code --force} covers — a block-0 of ≥4 bytes is enough to make the
     * probe return {@code true} even on a truncated upload — is preserved.
     */
    @Override
    public boolean multipartIndexFileExists(String tableSpace, String uuid, String fileType) {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        // Issue #638: probe the bulk layout first via S3 HEAD (with in-memory
        // cache). A bulk file's existence is authoritative — we wrote it
        // ourselves via the direct upload path. If absent, fall through to
        // the legacy per-block existence probe.
        if (isBulkLayout(logicalPath)) {
            return true;
        }
        int blockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        try {
            byte[] head = client.readFileRange(logicalPath, 0L, 4, blockSize);
            // RemoteFileServiceClient.readFileRange returns null for missing
            // blocks (see RemoteFileServiceTest.testWriteFileBlockAndReadFileRange).
            // A non-null result with ≥ 1 byte means block-0 is reachable —
            // matches the base contract's "best-effort presence check".
            return head != null && head.length > 0;
        } catch (RuntimeException e) {
            // Some backends wrap NoSuchKey-style errors in unchecked
            // exceptions; treat any failure during the probe as "missing"
            // per the contract.
            LOGGER.log(Level.FINE,
                    "multipartIndexFileExists: probe failed for {0}: {1}",
                    new Object[]{logicalPath, e.getMessage()});
            return false;
        }
    }

    @Override
    public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
            throws DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        // Issue #638: delete both the legacy per-block layout AND the bulk
        // layout (single .bulk object). Either may be present — for files
        // written via the direct path only .bulk exists; for files written
        // via gRPC only per-block exists; both deletes are idempotent so a
        // missing layout silently succeeds.
        try {
            client.deleteFile(logicalPath);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING,
                    "deleteMultipartIndexFile: non-fatal error deleting {0}: {1}",
                    new Object[]{logicalPath, e.getMessage()});
        }
        ObjectStorage storage = this.directObjectStorage;
        if (storage != null) {
            try {
                storage.delete(logicalPath + BULK_LAYOUT_SUFFIX)
                        .get(15L, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING,
                        "deleteMultipartIndexFile: interrupted while deleting bulk variant of {0}",
                        new Object[]{logicalPath});
            } catch (java.util.concurrent.ExecutionException
                    | java.util.concurrent.TimeoutException
                    | RuntimeException bulkErr) {
                // RuntimeException covers SDK-wrapped 404s and other transient
                // failures. Bulk delete is best-effort: if it fails the next
                // optimizer sweep will catch the orphan, exactly like the
                // legacy per-block path. Swallow with a log.
                LOGGER.log(Level.WARNING,
                        "deleteMultipartIndexFile: non-fatal error deleting bulk variant"
                                + " {0}{1}: {2}",
                        new Object[]{logicalPath, BULK_LAYOUT_SUFFIX, bulkErr.toString()});
            }
        }
        // Drop probe-cache + local-cache state so a future segment rewritten
        // under the same logical path does not observe stale layout info or
        // serve stale bytes from a leftover local cache file.
        bulkLayoutCache.remove(logicalPath);
        Path localCache = bulkLocalCache.remove(logicalPath);
        if (localCache != null) {
            try {
                Files.deleteIfExists(localCache);
            } catch (IOException ioe) {
                LOGGER.log(Level.FINE,
                        "could not delete local bulk cache file {0}: {1}",
                        new Object[]{localCache, ioe.getMessage()});
            }
        }
        // Drop any cached blocks for this path so a future segment rewritten
        // under the same logical path does not serve stale bytes.
        segmentBlockCache.invalidatePath(logicalPath);
    }

    @Override
    public boolean supportsDirectMultipartDownload() {
        return directObjectStorage != null;
    }

    /**
     * Downloads a multipart segment file directly from object storage to a local file,
     * bypassing the file-server. Blocks are fetched sequentially and written to
     * {@code destFile} without materialising each block in a Netty {@link ByteBuf}:
     * block 0 creates (or replaces) the destination file; each subsequent block is
     * appended. Storage backends that override
     * {@link ObjectStorage#downloadToFile(String, java.nio.file.Path, boolean)}
     * (e.g. {@link herddb.remote.storage.S3ObjectStorage} via
     * {@code AsyncResponseTransformer.toFile()}) avoid any intermediate heap or
     * direct-memory copy entirely.
     *
     * <p>When {@code fileSize == 0} the loop runs zero iterations and the destination
     * file is left untouched. Callers that require the file to exist (e.g. created
     * as empty) should use {@code Files.createFile(destFile)} before calling this
     * method. This matches the contract documented in
     * {@link herddb.storage.DataStorageManager#downloadMultipartIndexFile}.
     *
     * <p>Only callable when {@link #supportsDirectMultipartDownload()} is {@code true}.
     */
    @Override
    public void downloadMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                           long fileSize, java.nio.file.Path destFile)
            throws IOException, DataStorageManagerException {
        ObjectStorage storage = this.directObjectStorage;
        if (storage == null) {
            throw new UnsupportedOperationException(
                    "Direct S3 not configured on this RemoteFileDataStorageManager");
        }
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        // Issue #638: bulk layout fast path — a single .bulk object is
        // downloaded in one shot via S3TransferManager.downloadFile (real S3
        // Multipart Download, parallel parts pipelined by CRT). Falls back to
        // the legacy per-block sequential download when the bulk variant is
        // absent.
        // Issue #645: use the strict probe — a transient HEAD failure must
        // NOT silently fall through to the legacy per-block path, because
        // for a file written via direct-S3 the per-block layout is empty in
        // S3 and the download would always fail. Throwing here surfaces a
        // clear diagnostic instead of crash-looping the IS with confusing
        // "Block not found" errors.
        if (isBulkLayoutOrThrow(logicalPath)) {
            try {
                storage.downloadFileBulk(logicalPath + BULK_LAYOUT_SUFFIX, destFile).get();
                LOGGER.log(Level.FINE,
                        "downloadMultipartIndexFile(bulk): {0} -> {1} fileSize={2}",
                        new Object[]{logicalPath, destFile, fileSize});
                return;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "interrupted while downloading bulk multipart " + logicalPath, ie);
            } catch (java.util.concurrent.ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException(
                        "bulk multipart download failed for " + logicalPath, cause);
            }
        }
        // Block size must match what was used at write time.
        int writeBlockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        int numBlocks = (int) Math.ceil((double) fileSize / writeBlockSize);

        LOGGER.log(Level.FINE,
                "downloadMultipartIndexFile: {0} fileSize={1} writeBlockSize={2} numBlocks={3}",
                new Object[]{logicalPath, fileSize, writeBlockSize, numBlocks});

        for (int i = 0; i < numBlocks; i++) {
            String blockPath = logicalPath + ObjectStorage.MULTIPART_SUFFIX + "/" + i;
            // Block 0: create / replace the destination file.
            // Block 1+: append so the blocks form a single contiguous file.
            boolean append = i > 0;
            try {
                storage.downloadToFile(blockPath, destFile, append).get();
            } catch (java.util.concurrent.ExecutionException e) {
                // Unwrap IOException so the caller sees the root I/O error directly
                // rather than a double-nested IOException(cause=IOException).
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException(
                        "Failed to download block " + i + " of " + logicalPath, cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(
                        "Interrupted while downloading block " + i + " of " + logicalPath, e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Full table scan
    // -------------------------------------------------------------------------

    @Override
    public void fullTableScan(String tableSpace, String uuid, FullTableScanConsumer consumer)
            throws DataStorageManagerException {
        TableStatus status = getLatestTableStatus(tableSpace, uuid);
        doFullTableScan(tableSpace, uuid, status, consumer);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, LogSequenceNumber sequenceNumber,
            FullTableScanConsumer consumer) throws DataStorageManagerException {
        TableStatus status = getTableStatus(tableSpace, uuid, sequenceNumber);
        doFullTableScan(tableSpace, uuid, status, consumer);
    }

    private void doFullTableScan(String tableSpace, String uuid, TableStatus status,
            FullTableScanConsumer consumer) throws DataStorageManagerException {
        consumer.acceptTableStatus(status);
        List<Long> activePages = new ArrayList<>(status.activePages.keySet());
        activePages.sort(null);
        for (long pageId : activePages) {
            List<Record> records = readPage(tableSpace, uuid, pageId);
            consumer.acceptPage(pageId, records);
        }
        consumer.endTable();
    }

    // -------------------------------------------------------------------------
    // Checkpoint — local metadata + remote page cleanup PostCheckpointActions
    // -------------------------------------------------------------------------

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid,
            TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
        // Delegate local metadata file writing + old metadata file cleanup to localMetadataManager
        List<PostCheckpointAction> result = new ArrayList<>(
                localMetadataManager.tableCheckpoint(tableSpace, uuid, tableStatus, pin));

        // Add remote page deletion actions for stale pages
        final Map<Long, Integer> pins = pinTableAndGetPages(tableSpace, uuid, tableStatus, pin);
        long maxPageId = tableStatus.activePages.keySet().stream()
                .max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        Set<Long> currentActivePages = tableStatus.activePages.keySet();
        String key = tableSpace + "/" + uuid;

        Set<Long> previousActivePages = lastCheckpointedDataPages.get(key);
        List<long[]> newlyStale = new ArrayList<>(); // [pageId] or [-1] when only path is known
        List<String> newlyStalePaths = new ArrayList<>();
        if (previousActivePages != null) {
            // Fast path: diff against the previous checkpoint — no remote listing needed
            for (Long pageId : previousActivePages) {
                if (!pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remoteDataPagePath(tableSpace, uuid, pageId));
                }
            }
        } else {
            // First checkpoint after boot: enumerate all remote files to find orphans
            LOGGER.log(Level.INFO, "tableCheckpoint {0}/{1}: using full remote listing (first checkpoint after boot)",
                    new Object[]{tableSpace, uuid});
            List<String> remotePages = client.listFiles(remoteDataPrefix(tableSpace, uuid));
            for (String remotePath : remotePages) {
                long pageId = pageIdFromRemotePath(remotePath);
                if (pageId > 0
                        && !pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remotePath);
                }
            }
        }
        lastCheckpointedDataPages.put(key, new HashSet<>(currentActivePages));

        // Emit data-page deletion actions: either deferred (with retention) or immediate
        if (retentionEnabled) {
            long now = currentTimeMillis();
            List<PendingDeletion> bucket = pendingDataDeletions.computeIfAbsent(key,
                    k -> Collections.synchronizedList(new ArrayList<>()));
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                bucket.add(new PendingDeletion(tableStatus.sequenceNumber, now, newlyStalePaths.get(i),
                        "delete remote page " + pageId, tableSpace, uuid));
            }
            result.addAll(promotePendingDeletions(pendingDataDeletions, key, tableSpace));
        } else {
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote page " + pageId, newlyStalePaths.get(i), client));
            }
        }

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTableStatus(tableSpace, uuid, tableStatus);
        }

        return result;
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
            IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
        List<PostCheckpointAction> result = new ArrayList<>(
                localMetadataManager.indexCheckpoint(tableSpace, uuid, indexStatus, pin));

        final Map<Long, Integer> pins = pinIndexAndGetPages(tableSpace, uuid, indexStatus, pin);
        long maxPageId = indexStatus.activePages.stream()
                .max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);
        Set<Long> currentActivePages = indexStatus.activePages;
        String key = tableSpace + "/" + uuid;

        Set<Long> previousActivePages = lastCheckpointedIndexPages.get(key);
        List<long[]> newlyStale = new ArrayList<>();
        List<String> newlyStalePaths = new ArrayList<>();
        if (previousActivePages != null) {
            // Fast path: diff against the previous checkpoint — no remote listing needed
            for (Long pageId : previousActivePages) {
                if (!pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remoteIndexPagePath(tableSpace, uuid, pageId));
                }
            }
        } else {
            // First checkpoint after boot: enumerate all remote files to find orphans
            LOGGER.log(Level.INFO, "indexCheckpoint {0}/{1}: using full remote listing (first checkpoint after boot)",
                    new Object[]{tableSpace, uuid});
            List<String> remotePages = client.listFiles(remoteIndexPrefix(tableSpace, uuid));
            for (String remotePath : remotePages) {
                long pageId = pageIdFromRemotePath(remotePath);
                if (pageId > 0
                        && !pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remotePath);
                }
            }
        }
        lastCheckpointedIndexPages.put(key, new HashSet<>(currentActivePages));

        // Emit index-page deletion actions: either deferred (with retention) or immediate
        if (retentionEnabled) {
            long now = currentTimeMillis();
            List<PendingDeletion> bucket = pendingIndexDeletions.computeIfAbsent(key,
                    k -> Collections.synchronizedList(new ArrayList<>()));
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                bucket.add(new PendingDeletion(indexStatus.sequenceNumber, now, newlyStalePaths.get(i),
                        "delete remote index page " + pageId, tableSpace, uuid));
            }
            result.addAll(promotePendingDeletions(pendingIndexDeletions, key, tableSpace));
        } else {
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote index page " + pageId, newlyStalePaths.get(i), client));
            }
        }

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeIndexStatus(tableSpace, uuid, indexStatus);
        }

        return result;
    }

    private static class RemoteDeletePageAction extends PostCheckpointAction {
        private final String remotePath;
        private final RemoteFileServiceClient client;

        RemoteDeletePageAction(String tableSpace, String tableName, String description,
                String remotePath, RemoteFileServiceClient client) {
            super(tableSpace, tableName, description);
            this.remotePath = remotePath;
            this.client = client;
        }

        @Override
        public void run() {
            LOGGER.log(Level.FINE, description);
            client.deleteFile(remotePath);
        }
    }

    // -------------------------------------------------------------------------
    // Table/index structure operations
    // -------------------------------------------------------------------------

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        localMetadataManager.initTablespace(tableSpace);
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.initTable(tableSpace, uuid);
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.initIndex(tableSpace, uuid);
    }

    @Override
    public void dropTable(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.dropTable(tableSpace, uuid);
        client.deleteByPrefix(remoteDataPrefix(tableSpace, uuid));
        String key = tableSpace + "/" + uuid;
        lastCheckpointedDataPages.remove(key);
        pendingDataDeletions.remove(key);
        lazyValueCache.invalidateForTable(tableSpace, uuid);
    }

    @Override
    public void dropIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.dropIndex(tableSpace, uuid);
        deleteAllRemoteArtefactsForIndex(tableSpace, uuid);
        String key = tableSpace + "/" + uuid;
        lastCheckpointedIndexPages.remove(key);
        pendingIndexDeletions.remove(key);
    }

    @Override
    public void truncateIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.truncateIndex(tableSpace, uuid);
        deleteAllRemoteArtefactsForIndex(tableSpace, uuid);
        String key = tableSpace + "/" + uuid;
        lastCheckpointedIndexPages.remove(key);
        pendingIndexDeletions.remove(key);
    }

    /**
     * Deletes every remote-storage object that belongs to a logical
     * vector index identified by {@code (tableSpace, uuid)}. This must
     * cover three distinct path families that the various writers use
     * for the same logical index — without all three, segments / index
     * status markers leak forever after a DROP (issue #383):
     * <ul>
     *   <li>{@code {tableSpace}/{uuid}/...} — index pages and the parent
     *       index dir written by {@code writeIndexPage} /
     *       {@code indexCheckpoint};</li>
     *   <li>{@code {tableSpace}/{uuid}_*} — per-segment multipart files
     *       (graph, map) and any per-checkpoint temp BLink storages.
     *       {@link herddb.index.vector.PersistentVectorStore} derives
     *       fresh storage UUIDs of the form {@code {parentUuid}_seg{N}}
     *       and {@code {parentUuid}_tmp_pkset_*} from the parent index
     *       UUID, so a prefix match limited to {@code {uuid}/} would
     *       leave them orphaned;</li>
     *   <li>{@code {tableSpace}/_metadata/{uuid}.*} — the per-LSN
     *       {@code .indexstatus} markers written by
     *       {@link SharedCheckpointMetadataManager} for shared-storage
     *       read replicas.</li>
     * </ul>
     */
    private void deleteAllRemoteArtefactsForIndex(String tableSpace, String uuid) {
        // {tableSpace}/{uuid}/ already covers {tableSpace}/{uuid}/index/
        // (the legacy remoteIndexPrefix), so deleting both would issue a
        // redundant network round-trip on every DROP/TRUNCATE.
        client.deleteByPrefix(tableSpace + "/" + uuid + "/");
        client.deleteByPrefix(tableSpace + "/" + uuid + "_");
        client.deleteByPrefix(tableSpace + "/_metadata/" + uuid + ".");
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        localMetadataManager.eraseTablespaceData(tableSpace);
        client.deleteByPrefix(remoteTablespacePrefix(tableSpace));
        String prefix = tableSpace + "/";
        lastCheckpointedDataPages.keySet().removeIf(k -> k.startsWith(prefix));
        lastCheckpointedIndexPages.keySet().removeIf(k -> k.startsWith(prefix));
        pendingDataDeletions.keySet().removeIf(k -> k.startsWith(prefix));
        pendingIndexDeletions.keySet().removeIf(k -> k.startsWith(prefix));
        lazyValueCache.invalidateForTablespace(tableSpace);
        segmentBlockCache.invalidatePrefix(prefix);
    }

    @Override
    public void cleanupAfterTableBoot(String tableSpace, String uuid, Set<Long> activePagesAtBoot)
            throws DataStorageManagerException {
        // Build the stale-page list from the remote listing.
        long listStartNanos = System.nanoTime();
        List<String> remotePages = client.listFiles(remoteDataPrefix(tableSpace, uuid));
        List<String> stalePaths = new ArrayList<>();
        for (String remotePath : remotePages) {
            long pageId = pageIdFromRemotePath(remotePath);
            if (pageId > 0 && !activePagesAtBoot.contains(pageId)) {
                stalePaths.add(remotePath);
            }
        }
        long listElapsedMs = (System.nanoTime() - listStartNanos) / 1_000_000L;
        if (stalePaths.isEmpty()) {
            LOGGER.log(Level.INFO,
                    "cleanupAfterTableBoot[{0}/{1}]: nothing to delete "
                            + "(scanned {2} remote pages, active={3}, listMs={4})",
                    new Object[]{tableSpace, uuid, remotePages.size(), activePagesAtBoot.size(),
                            listElapsedMs});
            return;
        }
        int totalToDelete = stalePaths.size();
        int batchSize = cleanupBatchSize;
        int totalBatches = (totalToDelete + batchSize - 1) / batchSize;
        LOGGER.log(Level.INFO,
                "cleanupAfterTableBoot[{0}/{1}]: deleting {2} stale remote pages "
                        + "in {3} batches of up to {4} (active={5}, listMs={6})",
                new Object[]{tableSpace, uuid, totalToDelete, totalBatches, batchSize,
                        activePagesAtBoot.size(), listElapsedMs});
        long cumulativeNanos = 0L;
        long lastProgressLogNanos = System.nanoTime();
        int totalDeleted = 0;
        for (int offset = 0, batchIdx = 1; offset < totalToDelete; offset += batchSize, batchIdx++) {
            int end = Math.min(offset + batchSize, totalToDelete);
            List<String> batch = stalePaths.subList(offset, end);
            long batchStartNanos = System.nanoTime();
            int deleted;
            try {
                deleted = client.deleteFiles(batch);
            } catch (RuntimeException ex) {
                long batchElapsedMicros = (System.nanoTime() - batchStartNanos) / 1_000L;
                cleanupBatchLatency.registerFailedEvent(batchElapsedMicros,
                        TimeUnit.MICROSECONDS);
                LOGGER.log(Level.WARNING,
                        "cleanupAfterTableBoot[" + tableSpace + "/" + uuid + "]: batch "
                                + batchIdx + "/" + totalBatches + " of size " + batch.size()
                                + " failed; aborting cleanup. " + totalDeleted + "/" + totalToDelete
                                + " deleted so far (cumulativeMs="
                                + (cumulativeNanos / 1_000_000L) + ")",
                        ex);
                throw ex;
            }
            long batchElapsedNanos = System.nanoTime() - batchStartNanos;
            cumulativeNanos += batchElapsedNanos;
            cleanupBatchesCounter.inc();
            cleanupDeletionsCounter.addCount(deleted);
            cleanupBatchLatency.registerSuccessfulEvent(batchElapsedNanos / 1_000L,
                    TimeUnit.MICROSECONDS);
            totalDeleted += deleted;
            // Rate-limit per-batch progress lines: always log the first batch, the
            // final batch, every 100 batches, and at most once every 30 seconds.
            // For small cleanups (e.g. a few batches) this prints every batch.
            boolean isFirst = batchIdx == 1;
            boolean isLast = batchIdx == totalBatches;
            boolean every100 = (batchIdx % 100) == 0;
            long sinceLastLogMs = (System.nanoTime() - lastProgressLogNanos) / 1_000_000L;
            boolean every30s = sinceLastLogMs >= 30_000L;
            if (isFirst || isLast || every100 || every30s) {
                LOGGER.log(Level.INFO,
                        "cleanupAfterTableBoot[{0}/{1}]: batch {2}/{3} size={4} "
                                + "deleted={5} totalDeleted={6}/{7} "
                                + "batchMs={8} cumulativeMs={9}",
                        new Object[]{tableSpace, uuid, batchIdx, totalBatches, batch.size(),
                                deleted, totalDeleted, totalToDelete,
                                batchElapsedNanos / 1_000_000L,
                                cumulativeNanos / 1_000_000L});
                lastProgressLogNanos = System.nanoTime();
            }
        }
        LOGGER.log(Level.INFO,
                "cleanupAfterTableBoot[{0}/{1}]: complete. "
                        + "deleted={2}/{3} batches={4} batchSize={5} "
                        + "totalMs={6} avgBatchMs={7}",
                new Object[]{tableSpace, uuid, totalDeleted, totalToDelete, totalBatches,
                        batchSize, cumulativeNanos / 1_000_000L,
                        totalBatches > 0 ? (cumulativeNanos / 1_000_000L) / totalBatches : 0L});
    }

    // -------------------------------------------------------------------------
    // Metadata delegation to localMetadataManager
    // -------------------------------------------------------------------------

    @Override
    public int getActualNumberOfPages(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return localMetadataManager.getActualNumberOfPages(tableSpace, uuid);
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return localMetadataManager.getLatestTableStatus(tableSpace, uuid);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String uuid,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        // Issue #471: read replicas (e.g. the IndexingService rebuilder)
        // do not write metadata locally — the herddb leader writes
        // TableStatus to ITS local metadata directory and publishes to
        // shared remote storage via SharedCheckpointMetadataManager.
        // Fall back to shared storage when the local file is missing
        // so a fresh read replica can scan a table at a checkpoint LSN
        // it never wrote itself. The fallback is opt-in: a leader
        // without a SharedCheckpointMetadataManager configured (or a
        // standalone test setup) keeps the local-only behavior, so
        // existing call sites are unchanged.
        try {
            return localMetadataManager.getTableStatus(tableSpace, uuid, sequenceNumber);
        } catch (DataStorageManagerException localErr) {
            SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
            if (shared == null) {
                throw localErr;
            }
            TableStatus sharedStatus;
            try {
                sharedStatus = shared.readTableStatus(tableSpace, uuid, sequenceNumber);
            } catch (DataStorageManagerException sharedErr) {
                // Shared also failed — surface the LOCAL error (more
                // recognizable for operators) but suppress the
                // shared error so it shows up in the logs.
                localErr.addSuppressed(sharedErr);
                throw localErr;
            }
            // SharedCheckpointMetadataManager.readTableStatus returns
            // a "new table" sentinel (empty activePages, lsn=
            // START_OF_TIME) when the path does not exist on the
            // shared backend. That sentinel is NOT what the caller
            // asked for — re-throw the original local error so the
            // caller can react to a genuinely-missing checkpoint.
            if (sharedStatus == null
                    || sharedStatus.activePages == null
                    || (sharedStatus.activePages.isEmpty()
                            && LogSequenceNumber.START_OF_TIME.equals(sharedStatus.sequenceNumber))) {
                throw localErr;
            }
            return sharedStatus;
        }
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String uuid,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return localMetadataManager.getIndexStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.loadTables(sequenceNumber, tableSpace);
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.loadIndexes(sequenceNumber, tableSpace);
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace,
            Consumer<Transaction> consumer) throws DataStorageManagerException {
        localMetadataManager.loadTransactions(sequenceNumber, tableSpace, consumer);
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(String tableSpace,
            LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist,
            boolean prepareActions) throws DataStorageManagerException {
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTableDefinitions(tableSpace, sequenceNumber, tables);
            shared.writeIndexDefinitions(tableSpace, sequenceNumber, indexlist);
        }

        return result;
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeCheckpointSequenceNumber(tableSpace, sequenceNumber);

        // Publish to shared storage for read replicas — this is written LAST,
        // acting as the atomic commit marker for the checkpoint metadata
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeCheckpointLsn(tableSpace, sequenceNumber);
        }

        return result;
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace,
            LogSequenceNumber sequenceNumber, Collection<Transaction> transactions)
            throws DataStorageManagerException {
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTransactions(tableSpace, sequenceNumber, transactions);
        }

        return result;
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.getLastcheckpointSequenceNumber(tableSpace);
    }

    // -------------------------------------------------------------------------
    // Index and record set factory
    // -------------------------------------------------------------------------

    @Override
    public KeyToPageIndex createKeyToPageMap(String tableSpace, String uuid,
            MemoryManager memoryManager) throws DataStorageManagerException {
        return KeyToPageIndexFactory.create(tableSpace, uuid, memoryManager, this);
    }

    @Override
    public void releaseKeyToPageMap(String tableSpace, String uuid, KeyToPageIndex index) {
        if (index != null) {
            index.close();
        }
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return new FileRecordSetFactory(tmpDir, swapThreshold);
    }
}
