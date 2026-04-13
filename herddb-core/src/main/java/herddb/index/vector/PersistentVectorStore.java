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

package herddb.index.vector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.MemoryManager;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkIndexDataStorage;
import herddb.index.blink.BytesLongSizeEvaluator;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import herddb.utils.VisibleByteArrayOutputStream;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.OpStatsLogger;

/**
 * Persistent vector store backed by jvector (OnHeapGraphIndex / HNSW-style) with
 * on-disk persistence via {@link DataStorageManager}, {@link BLink} for PK mapping,
 * and {@link MemoryManager} for bounded memory.
 *
 * <p>When {@code fusedPQ} is enabled (default), checkpoints use jvector's
 * {@link OnDiskGraphIndex} format with FusedPQ + InlineVectors features for
 * faster approximate scoring at search time. On load, a hybrid approach is
 * used: the loaded on-disk graph is searched with FusedPQ scoring, and new
 * inserts since the last checkpoint are searched in-memory. Results are merged.
 *
 * <p>This class is a standalone persistent vector store that can be used by the
 * indexing service. It manages its own background compaction thread.
 *
 * @author enrico.olivelli
 */
@SuppressWarnings({"deprecation"})
public class PersistentVectorStore extends AbstractVectorStore {

    private static final Logger LOGGER = Logger.getLogger(PersistentVectorStore.class.getName());

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    /* jvector graph hyper-parameter defaults */
    static final int DEFAULT_M = 16;
    static final int DEFAULT_BEAM_WIDTH = 100;
    static final float DEFAULT_NEIGHBOR_OVERFLOW = 1.2f;
    static final float DEFAULT_ALPHA = 1.4f;
    static final boolean ADD_HIERARCHY = false;
    static final boolean REFINE_FINAL_GRAPH = false;

    /** Minimum dimension for which FusedPQ is enabled (PQ requires dim >= M_subspaces). */
    static final int MIN_DIM_FOR_FUSED_PQ = 8;

    /** Minimum number of vectors required for FusedPQ (jvector FusedPQ requires exactly 256 PQ clusters). */
    static final int MIN_VECTORS_FOR_FUSED_PQ = 256;

    /** Maximum memory (in bytes) for live vectors during checkpoint back-pressure. */
    private static final long MAX_LIVE_BYTES_DURING_CHECKPOINT =
            Long.getLong("herddb.vectorindex.maxLiveBytesDuringCheckpoint", 4L * 1024 * 1024 * 1024);

    /**
     * Hard cap on how many live vectors may accumulate during a single
     * checkpoint's Phase B. This bounds the worst-case Phase B duration by
     * limiting the size of the pool that Phase B has to graph-build. When
     * unset (or set to 0), the cap is governed solely by the memory-budget
     * derivation (see {@link #computeLiveVectorCapDuringCheckpoint}).
     *
     * <p>Set via system property
     * {@code herddb.vectorindex.maxLiveVectorsPerCheckpoint}.
     */
    public static final int MAX_LIVE_VECTORS_PER_CHECKPOINT =
            Math.max(0, Integer.getInteger(
                    "herddb.vectorindex.maxLiveVectorsPerCheckpoint", 0));

    /**
     * How many Phase B segment builds may run concurrently. Each parallel
     * segment build allocates ~{@code segSize × dim × 4} bytes for the live
     * vector copy plus PQ codebooks, so the default is deliberately low;
     * override via system property for installations with plenty of heap.
     */
    public static final int PHASE_B_SEGMENT_PARALLELISM =
            Math.max(1, Integer.getInteger(
                    "herddb.vectorindex.phaseBSegmentParallelism", 2));

    /**
     * When the number of sealed segments exceeds this threshold, each Phase A
     * demotes the smallest sealed segments back to the mergeable pool so the
     * next Phase B can compact them into a smaller number of larger segments.
     * Set to {@link Integer#MAX_VALUE} to disable merging.
     */
    public static final int SEGMENT_MERGE_THRESHOLD =
            Math.max(2, Integer.getInteger(
                    "herddb.vectorindex.segmentMergeThreshold", 32));

    /**
     * How many of the smallest sealed segments to demote on each trigger.
     * Must be at least 2 for the merge to produce a net reduction.
     */
    public static final int SEGMENT_MERGE_BATCH =
            Math.max(2, Integer.getInteger(
                    "herddb.vectorindex.segmentMergeBatch", 4));

    /** Dedicated ForkJoinPool for checkpoint graph building. */
    private static final ForkJoinPool CHECKPOINT_POOL = new ForkJoinPool(
            Math.max(1, Runtime.getRuntime().availableProcessors() / 2),
            pool -> {
                ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                t.setDaemon(true);
                t.setName("persistent-vector-store-checkpoint-" + t.getPoolIndex());
                return t;
            },
            null, false);

    /** Maximum bytes per persisted page chunk (1 MB). */
    static final int CHUNK_SIZE = 1_048_576;

    /** Page-type tag written at the start of each graph-data page. */
    static final int TYPE_VECTOR_GRAPHCHUNK = 12;
    /** Page-type tag written at the start of each pk/vector-map page. */
    static final int TYPE_VECTOR_MAPCHUNK = 13;

    /** Metadata version for the simple OnHeapGraphIndex format. */
    private static final int METADATA_VERSION_SIMPLE = 1;
    /** Metadata version for the FusedPQ OnDiskGraphIndex format. */
    private static final int METADATA_VERSION_FUSEDPQ = 2;
    /** Metadata version for multi-segment format. */
    private static final int METADATA_VERSION_MULTI_SEGMENT = 3;

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    private final String indexName;
    private final String tableName;
    private final String tableSpaceUUID;
    private final String indexUUID;
    private final Path tmpDirectory;
    private final DataStorageManager dataStorageManager;
    private final MemoryManager memoryManager;

    /* instance hyper-parameters */
    private final int m;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final boolean fusedPQ;
    private final VectorSimilarityFunction similarityFunction;
    private final long maxSegmentSize;
    private final int maxLiveGraphSize;
    private final long compactionIntervalMs;
    private final long maxVectorMemoryBytes;
    private final VectorMemoryBudget memoryBudget;

    /** Optional stats logger for recording per-segment size distribution. */
    private volatile OpStatsLogger segmentSizeStats;

    // -------------------------------------------------------------------------
    // In-memory state -- LIVE inserts (new since last checkpoint)
    // -------------------------------------------------------------------------

    /** All live graph shards. The LAST element is the active (unsealed) shard. */
    private volatile List<LiveGraphShard> liveShards = new ArrayList<>();

    /** Monotonically increasing node-ID counter. */
    private final AtomicInteger nextNodeId = new AtomicInteger(0);

    /** Global lock-free vector storage shared by all live shards. */
    private VectorStorage vectorStorage = new VectorStorage(0);

    /** Page-ID counter. */
    private final AtomicLong newPageId = new AtomicLong(1);

    /** Tracks whether the index has been modified since the last successful checkpoint. */
    private final AtomicBoolean dirty = new AtomicBoolean(true);

    private volatile int dimension = 0;

    // -------------------------------------------------------------------------
    // Frozen state -- snapshot captured in Phase A of checkpoint
    // -------------------------------------------------------------------------

    /** Frozen shards from Phase A. */
    private volatile List<LiveGraphShard> frozenShards;

    /** PKs deleted during Phase B. */
    private volatile Set<Bytes> pendingCheckpointDeletes;

    /** Max live vectors allowed during Phase B before back-pressure kicks in. */
    private volatile int liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;

    /** Signaled when Phase C completes. */
    private volatile CountDownLatch checkpointPhaseComplete;

    // -------------------------------------------------------------------------
    // On-disk state -- multiple segments
    // -------------------------------------------------------------------------

    /** On-disk segments. */
    private volatile List<VectorSegment> segments = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Counter for assigning unique segment IDs. */
    private final AtomicInteger nextSegmentId = new AtomicInteger(0);

    /** Protects state swaps during checkpoint. */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /** Prevents concurrent three-phase checkpoints from interleaving and losing data. */
    private final ReentrantLock checkpointLock = new ReentrantLock();

    // -------------------------------------------------------------------------
    // Background compaction thread
    // -------------------------------------------------------------------------

    private volatile Thread compactionThread;
    private volatile boolean running;

    // -------------------------------------------------------------------------
    // Checkpoint statistics (observable by external metrics)
    // -------------------------------------------------------------------------

    private final AtomicLong lastCheckpointDurationMs = new AtomicLong(0);
    private final AtomicLong lastCheckpointPhaseBDurationMs = new AtomicLong(0);
    private final AtomicLong totalCheckpointCount = new AtomicLong(0);
    private final AtomicLong totalFusedPQCheckpointCount = new AtomicLong(0);
    private final AtomicLong totalSimpleCheckpointCount = new AtomicLong(0);
    private final AtomicLong lastCheckpointVectorsProcessed = new AtomicLong(0);
    /** Approximate bytes written by the last completed Phase B. */
    private final AtomicLong lastPhaseBBytesWritten = new AtomicLong(0);
    /** Pages reclaimed by the most recent failure recovery. */
    private final AtomicLong lastRolledBackPages = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Memory back-pressure statistics
    // -------------------------------------------------------------------------

    private final AtomicLong totalBackpressureCount = new AtomicLong(0);
    private final AtomicLong totalBackpressureTimeMs = new AtomicLong(0);
    private volatile int backpressureActive;
    private final Object memoryPressureMonitor = new Object();
    private final Object compactionWakeUp = new Object();
    private boolean compactionWakeUpPending = false;

    // -------------------------------------------------------------------------
    // Provisional page tracking
    // -------------------------------------------------------------------------

    /**
     * Collects pageIds allocated by the current Phase B attempt. When Phase B
     * aborts before {@code persistIndexStatusMultiSegment} records a new
     * checkpoint marker, these pages are unreferenced on disk and would only be
     * reclaimed by the next successful {@code indexCheckpoint} sweep — which
     * may never come if the disk is full. The recovery path drains this list
     * and calls {@link DataStorageManager#deleteIndexPage} for each entry.
     *
     * <p>Field (rather than a local) so tests and background threads can
     * observe the leakage window.
     */
    private volatile List<Long> provisionalPageIds;

    /** Metric: provisional pages rolled back by the last failure recovery. */
    private final AtomicLong totalRolledBackPages = new AtomicLong(0);
    /** Metric: how many Phase B attempts have failed since the last success. */
    private final AtomicLong consecutiveCheckpointFailures = new AtomicLong(0);
    /** Metric: total checkpoint failures over the lifetime of the store. */
    private final AtomicLong totalCheckpointFailures = new AtomicLong(0);

    public long getTotalRolledBackPages() {
        return totalRolledBackPages.get();
    }

    public long getConsecutiveCheckpointFailures() {
        return consecutiveCheckpointFailures.get();
    }

    public long getTotalCheckpointFailures() {
        return totalCheckpointFailures.get();
    }

    // -------------------------------------------------------------------------
    // Test hook
    // -------------------------------------------------------------------------

    private volatile Runnable checkpointPhaseBHook;

    /** Sets a hook that runs during Phase B of checkpoint. For testing. */
    public void setCheckpointPhaseBHook(Runnable hook) {
        this.checkpointPhaseBHook = hook;
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE);
    }

    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, Long.MAX_VALUE);
    }

    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction,
                                 long maxVectorMemoryBytes) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, maxVectorMemoryBytes, null);
    }

    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction,
                                 long maxVectorMemoryBytes,
                                 VectorMemoryBudget memoryBudget) {
        super(vectorColumnName);
        this.indexName = indexName;
        this.tableName = tableName;
        this.tableSpaceUUID = tableSpaceUUID;
        this.indexUUID = indexName + "_" + tableName + "_" + System.nanoTime();
        this.tmpDirectory = tmpDirectory;
        this.dataStorageManager = dataStorageManager;
        this.memoryManager = memoryManager;
        this.m = m;
        this.beamWidth = beamWidth;
        this.neighborOverflow = neighborOverflow;
        this.alpha = alpha;
        this.fusedPQ = fusedPQ;
        this.similarityFunction = similarityFunction;
        this.maxSegmentSize = maxSegmentSize;
        this.maxLiveGraphSize = maxLiveGraphSize;
        this.compactionIntervalMs = compactionIntervalMs;
        this.maxVectorMemoryBytes = maxVectorMemoryBytes;
        this.memoryBudget = memoryBudget;
    }

    /**
     * Constructor that accepts an explicit indexUUID, useful for recovery testing
     * where the same UUID must be used across store instances.
     */
    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, String indexUUID, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, indexUUID, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE);
    }

    /**
     * Constructor that accepts an explicit indexUUID and similarity function.
     */
    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, String indexUUID, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, indexUUID, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, Long.MAX_VALUE);
    }

    /**
     * Constructor that accepts an explicit indexUUID, similarity function, and memory limit.
     */
    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, String indexUUID, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction,
                                 long maxVectorMemoryBytes) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, indexUUID, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, maxVectorMemoryBytes, null);
    }

    /**
     * Constructor that accepts an explicit indexUUID, similarity function, memory limit,
     * and global memory budget.
     */
    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, String indexUUID, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction,
                                 long maxVectorMemoryBytes,
                                 VectorMemoryBudget memoryBudget) {
        super(vectorColumnName);
        this.indexName = indexName;
        this.tableName = tableName;
        this.tableSpaceUUID = tableSpaceUUID;
        this.indexUUID = indexUUID;
        this.tmpDirectory = tmpDirectory;
        this.dataStorageManager = dataStorageManager;
        this.memoryManager = memoryManager;
        this.m = m;
        this.beamWidth = beamWidth;
        this.neighborOverflow = neighborOverflow;
        this.alpha = alpha;
        this.fusedPQ = fusedPQ;
        this.similarityFunction = similarityFunction;
        this.maxSegmentSize = maxSegmentSize;
        this.maxLiveGraphSize = maxLiveGraphSize;
        this.compactionIntervalMs = compactionIntervalMs;
        this.maxVectorMemoryBytes = maxVectorMemoryBytes;
        this.memoryBudget = memoryBudget;
    }

    // -------------------------------------------------------------------------
    // Similarity function parsing
    // -------------------------------------------------------------------------

    /**
     * Parses a similarity string (from index properties) into a {@link VectorSimilarityFunction}.
     * Accepted values: "cosine", "euclidean", "dot". Case-insensitive.
     * Returns {@link VectorSimilarityFunction#COSINE} for null or unrecognized values.
     */
    public static VectorSimilarityFunction parseSimilarityFunction(String similarity) {
        if (similarity == null) {
            return VectorSimilarityFunction.COSINE;
        }
        switch (similarity.toLowerCase()) {
            case "euclidean":
                return VectorSimilarityFunction.EUCLIDEAN;
            case "dot":
                return VectorSimilarityFunction.DOT_PRODUCT;
            case "cosine":
            default:
                return VectorSimilarityFunction.COSINE;
        }
    }

    // -------------------------------------------------------------------------
    // LiveGraphShard inner class
    // -------------------------------------------------------------------------

    /**
     * Encapsulates the state of a single live in-memory graph shard.
     * The active shard (last in the list) accepts new inserts; sealed shards are read-only.
     */
    static class LiveGraphShard {
        final ConcurrentHashMap<Bytes, Integer> pkToNode;
        final ConcurrentHashMap<Integer, Bytes> nodeToPk;
        final RandomAccessVectorValues mravv;
        final GraphIndexBuilder builder;
        final AtomicInteger vectorCount = new AtomicInteger(0);
        /**
         * Global nodeId of the first node added to this shard's builder.
         * The builder uses local nodeIds {@code [0, vectorCount)}.
         * To convert: {@code localNodeId = globalNodeId - startNodeId}.
         */
        final int startNodeId;

        LiveGraphShard(ConcurrentHashMap<Bytes, Integer> pkToNode,
                       ConcurrentHashMap<Integer, Bytes> nodeToPk,
                       RandomAccessVectorValues mravv,
                       GraphIndexBuilder builder,
                       int startNodeId) {
            this.pkToNode = pkToNode;
            this.nodeToPk = nodeToPk;
            this.mravv = mravv;
            this.builder = builder;
            this.startNodeId = startNodeId;
        }
    }

    /** Holds the result of writing a single segment during checkpoint. */
    private static class SegmentWriteResult {
        final int segmentId;
        final List<Long> graphPageIds;
        final List<Long> mapPageIds;
        final long estimatedSizeBytes;
        // Multipart mode (non-null when the storage manager supports multipart writes)
        final String graphFilePath;
        final long graphFileSize;
        final String mapFilePath;
        final long mapFileSize;

        /** Page-based constructor. */
        SegmentWriteResult(int segmentId, List<Long> graphPageIds, List<Long> mapPageIds,
                           long estimatedSizeBytes) {
            this.segmentId = segmentId;
            this.graphPageIds = graphPageIds;
            this.mapPageIds = mapPageIds;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.graphFilePath = null;
            this.graphFileSize = 0;
            this.mapFilePath = null;
            this.mapFileSize = 0;
        }

        /** Multipart constructor. */
        SegmentWriteResult(int segmentId, String graphFilePath, long graphFileSize,
                           String mapFilePath, long mapFileSize, long estimatedSizeBytes) {
            this.segmentId = segmentId;
            this.graphPageIds = Collections.emptyList();
            this.mapPageIds = Collections.emptyList();
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.graphFilePath = graphFilePath;
            this.graphFileSize = graphFileSize;
            this.mapFilePath = mapFilePath;
            this.mapFileSize = mapFileSize;
        }

        boolean isMultipart() {
            return graphFilePath != null;
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the persistent vector store. Initialises storage and loads existing
     * segments from DataStorageManager if present.
     */
    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "starting PersistentVectorStore {0} uuid {1}",
                new Object[]{indexName, indexUUID});

        dataStorageManager.initIndex(tableSpaceUUID, indexUUID);
        vectorStorage = new VectorStorage(computeEffectiveMaxLiveGraphSize());

        // Try to load existing state
        try {
            IndexStatus status = dataStorageManager.getIndexStatus(
                    tableSpaceUUID, indexUUID, LogSequenceNumber.START_OF_TIME);
            if (status != null && status.indexData != null && status.indexData.length > 0) {
                loadFromStatus(status);
            }
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.INFO,
                    "no existing state for PersistentVectorStore {0}, starting empty: {1}",
                    new Object[]{indexName, e.getMessage()});
        }

        // Start background compaction thread
        running = true;
        compactionThread = new Thread(this::compactionLoop,
                "persistent-vector-store-compaction-" + indexName);
        compactionThread.setDaemon(true);
        compactionThread.start();

        LOGGER.log(Level.INFO, "PersistentVectorStore {0} started", indexName);
    }

    /** Upper bound on the back-off applied after repeated checkpoint failures (30 min). */
    static final long MAX_BACKOFF_MS =
            Long.getLong("herddb.vectorindex.maxCheckpointBackoffMs", 30L * 60 * 1000);

    /**
     * Computes the extra wait between checkpoint attempts after {@code failures}
     * consecutive failures. Exponential: compactionIntervalMs * 2^(failures - 1),
     * capped at {@link #MAX_BACKOFF_MS}. Returns 0 on the first attempt.
     *
     * <p>Package-private in spirit, but exposed as public so that tests in
     * the {@code herddb-indexing-service} module can verify the policy.
     */
    public static long computeBackoffMs(long baseIntervalMs, long failures, long maxBackoffMs) {
        if (failures <= 0) {
            return 0L;
        }
        // Use a 60 s floor for the base so that configurations with
        // compactionIntervalMs == 0 still back off sensibly.
        long effectiveBase = Math.max(baseIntervalMs, 60_000L);
        long shift = Math.min(failures - 1, 20); // prevent overflow
        long backoff = effectiveBase << shift;
        if (backoff < 0 || backoff > maxBackoffMs) {
            return maxBackoffMs;
        }
        return backoff;
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private void compactionLoop() {
        while (running) {
            try {
                long baseSleepMs = shouldTriggerMemoryPressureCheckpoint()
                        ? Math.min(compactionIntervalMs, 1000)
                        : compactionIntervalMs;
                long backoff = computeBackoffMs(compactionIntervalMs,
                        consecutiveCheckpointFailures.get(), MAX_BACKOFF_MS);
                long sleepMs = saturatedAdd(baseSleepMs, backoff);
                if (backoff > 0) {
                    LOGGER.log(Level.WARNING,
                            "vector store {0}: backing off checkpoint by {1} ms after {2} consecutive failures",
                            new Object[]{indexName, backoff, consecutiveCheckpointFailures.get()});
                }
                synchronized (compactionWakeUp) {
                    if (!compactionWakeUpPending) {
                        compactionWakeUp.wait(sleepMs);
                    }
                    compactionWakeUpPending = false;
                }
            } catch (InterruptedException e) {
                if (!running) {
                    return;
                }
                // Interrupted by shutdown; clear the flag and proceed.
                Thread.interrupted();
            }
            if (!running) {
                return;
            }
            if (dirty.get() || shouldTriggerMemoryPressureCheckpoint()) {
                try {
                    checkpoint();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE,
                            "compaction failed for PersistentVectorStore " + indexName, e);
                }
                synchronized (memoryPressureMonitor) {
                    memoryPressureMonitor.notifyAll();
                }
            }
        }
    }

    private static long saturatedAdd(long a, long b) {
        long r = a + b;
        if (((a ^ r) & (b ^ r)) < 0) {
            return Long.MAX_VALUE;
        }
        return r;
    }

    private boolean shouldTriggerMemoryPressureCheckpoint() {
        // Check global budget first (covers all stores sharing the same heap)
        if (memoryBudget != null) {
            boolean trigger = memoryBudget.isAboveThreshold(0.7);
            if (trigger) {
                LOGGER.log(Level.INFO,
                        "vector store {0} memory pressure (global): {1} bytes exceeds 70% of global limit {2} bytes, triggering early checkpoint",
                        new Object[]{indexName, memoryBudget.totalEstimatedMemoryUsageBytes(), memoryBudget.maxMemoryBytes()});
            }
            return trigger;
        }
        // Fallback to per-store check
        if (maxVectorMemoryBytes == Long.MAX_VALUE) {
            return false;
        }
        long usage = estimatedMemoryUsageBytes();
        boolean trigger = usage > (long) (maxVectorMemoryBytes * 0.7);
        if (trigger) {
            LOGGER.log(Level.INFO,
                    "vector store {0} memory pressure: {1} bytes exceeds 70% of limit {2} bytes, triggering early checkpoint",
                    new Object[]{indexName, usage, maxVectorMemoryBytes});
        }
        return trigger;
    }

    private void waitForMemoryPressureRelief() {
        long startMs = System.currentTimeMillis();
        backpressureActive = 1;
        totalBackpressureCount.incrementAndGet();
        if (memoryBudget != null) {
            LOGGER.log(Level.WARNING,
                    "vector store {0} memory back-pressure (global): estimated {1} bytes exceeds global limit {2} bytes, blocking addVector",
                    new Object[]{indexName, memoryBudget.totalEstimatedMemoryUsageBytes(), memoryBudget.maxMemoryBytes()});
        } else {
            long usage = estimatedMemoryUsageBytes();
            LOGGER.log(Level.WARNING,
                    "vector store {0} memory back-pressure: estimated {1} bytes exceeds limit {2} bytes, blocking addVector",
                    new Object[]{indexName, usage, maxVectorMemoryBytes});
        }

        // Wake up the compaction thread to trigger an immediate checkpoint
        synchronized (compactionWakeUp) {
            compactionWakeUpPending = true;
            compactionWakeUp.notifyAll();
        }

        synchronized (memoryPressureMonitor) {
            while (running && isMemoryOverLimit()) {
                try {
                    memoryPressureMonitor.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        backpressureActive = 0;
        long elapsedMs = System.currentTimeMillis() - startMs;
        totalBackpressureTimeMs.addAndGet(elapsedMs);
        LOGGER.log(Level.INFO,
                "vector store {0} memory back-pressure released after {1} ms (waited for checkpoint)",
                new Object[]{indexName, elapsedMs});
    }

    private boolean isMemoryOverLimit() {
        if (memoryBudget != null) {
            return memoryBudget.isMemoryPressureActive();
        }
        return maxVectorMemoryBytes != Long.MAX_VALUE
                && estimatedMemoryUsageBytes() > maxVectorMemoryBytes;
    }

    @Override
    public void close() throws Exception {
        running = false;
        Thread ct = compactionThread;
        if (ct != null) {
            ct.interrupt();
            ct.join(10000);
        }

        for (LiveGraphShard shard : liveShards) {
            if (shard.builder != null) {
                try {
                    shard.builder.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING,
                            "error closing vector index builder for " + indexName, e);
                }
            }
        }
        List<LiveGraphShard> frozen = this.frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                if (shard.builder != null) {
                    try {
                        shard.builder.close();
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING,
                                "error closing frozen vector index builder for " + indexName, e);
                    }
                }
            }
            this.frozenShards = null;
        }
        this.pendingCheckpointDeletes = null;
        this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
        CountDownLatch latch = this.checkpointPhaseComplete;
        if (latch != null) {
            latch.countDown();
            this.checkpointPhaseComplete = null;
        }
        for (VectorSegment seg : segments) {
            seg.close();
        }
        segments = new java.util.concurrent.CopyOnWriteArrayList<>();

        LOGGER.log(Level.INFO, "PersistentVectorStore {0} closed", indexName);
    }

    // -------------------------------------------------------------------------
    // DML operations
    // -------------------------------------------------------------------------

    /**
     * Adds a vector with the given primary key.
     */
    @Override
    public void addVector(Bytes pk, float[] vector) {
        if (vector == null || vector.length == 0) {
            return;
        }

        // Back-pressure: if checkpoint Phase B is active and live cap exceeded,
        // wait for Phase C to complete before proceeding.
        CountDownLatch latch = checkpointPhaseComplete;
        if (latch != null && totalLiveSize() >= liveVectorCapDuringCheckpoint) {
            LOGGER.log(Level.FINE,
                    "vector store {0} back-pressure: live size {1} reached cap {2}, waiting",
                    new Object[]{indexName, totalLiveSize(), liveVectorCapDuringCheckpoint});
            waitForCheckpointToComplete(latch);
        }

        // Memory limit back-pressure: block if vector memory exceeds budget
        if (memoryBudget != null) {
            if (memoryBudget.isMemoryPressureActive()) {
                waitForMemoryPressureRelief();
            }
        } else if (maxVectorMemoryBytes != Long.MAX_VALUE
                && estimatedMemoryUsageBytes() > maxVectorMemoryBytes) {
            waitForMemoryPressureRelief();
        }

        stateLock.readLock().lock();
        try {
            if (dimension == 0) {
                initBuilderForDimension(vector.length);
            }
            if (vector.length != dimension) {
                LOGGER.log(Level.WARNING,
                        "vector dimension mismatch on insert: expected {0} but got {1}, skipping",
                        new Object[]{dimension, vector.length});
                return;
            }
            VectorFloat<?> vec = VTS.createFloatVector(vector);

            List<LiveGraphShard> shards = this.liveShards;
            LiveGraphShard active = shards.get(shards.size() - 1);

            // Check if rotation is needed BEFORE allocating the nodeId so that
            // the new shard's startNodeId (= nextNodeId.get() at creation time)
            // matches the first nodeId assigned to it, keeping localId >= 0.
            if (active.nodeToPk.size() >= computeEffectiveMaxLiveGraphSize()) {
                active = rotateLiveShard();
            }

            int nodeId = nextNodeId.getAndIncrement();
            vectorStorage.set(nodeId, vec);
            active.vectorCount.incrementAndGet();
            active.pkToNode.put(pk, nodeId);
            active.nodeToPk.put(nodeId, pk);
            active.builder.addGraphNode(nodeId - active.startNodeId, vec);
            dirty.set(true);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Removes the vector with the given primary key.
     */
    @Override
    public void removeVector(Bytes pk) {
        stateLock.readLock().lock();
        try {
            // Check on-disk segments first
            for (VectorSegment seg : segments) {
                if (seg.deletePk(pk)) {
                    dirty.set(true);
                    break;
                }
            }
            // Track delete for Phase B awareness
            Set<Bytes> pending = pendingCheckpointDeletes;
            if (pending != null) {
                pending.add(pk);
            }
            // Check all live shards
            for (LiveGraphShard shard : liveShards) {
                Integer nodeId = shard.pkToNode.remove(pk);
                if (nodeId != null) {
                    shard.nodeToPk.remove(nodeId);
                    dirty.set(true);
                    if (shard.builder != null) {
                        shard.builder.markNodeDeleted(nodeId - shard.startNodeId);
                    } else {
                        vectorStorage.remove(nodeId);
                        shard.vectorCount.decrementAndGet();
                    }
                    break;
                }
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    // Search
    // -------------------------------------------------------------------------

    /**
     * Performs an approximate nearest-neighbor search against the vector store.
     *
     * @param queryVector the query embedding
     * @param topK        maximum number of results to return
     * @return list of (primaryKey, score) pairs ordered best-first
     */
    @Override
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK) {
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>();
        VectorFloat<?> qv = VTS.createFloatVector(queryVector);

        // Overquery each source to improve recall when merging across segments.
        // Each source returns more candidates; the final merge picks the true topK.
        int perSourceK = topK * VectorSegment.OVERQUERY_FACTOR;

        // Search all on-disk segments
        List<VectorSegment> currentSegments = this.segments;
        for (VectorSegment seg : currentSegments) {
            seg.search(qv, perSourceK, similarityFunction, results);
        }

        // Search all live in-memory shards
        for (LiveGraphShard shard : liveShards) {
            if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                int k = Math.min(perSourceK, shard.nodeToPk.size());
                ImmutableGraphIndex graph = shard.builder.getGraph();
                SearchResult result = GraphSearcher.search(
                        qv, k, shard.mravv, similarityFunction, graph, Bits.ALL);
                for (SearchResult.NodeScore ns : result.getNodes()) {
                    Bytes pk = shard.nodeToPk.get(ns.node + shard.startNodeId);
                    if (pk != null) {
                        results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                    }
                }
            }
        }

        // Search frozen shards (during Phase B of checkpoint)
        List<LiveGraphShard> frozen = frozenShards;
        if (frozen != null) {
            Set<Bytes> pending = pendingCheckpointDeletes;
            for (LiveGraphShard shard : frozen) {
                if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                    int k = Math.min(perSourceK, shard.nodeToPk.size());
                    try {
                        ImmutableGraphIndex graph = shard.builder.getGraph();
                        SearchResult result = GraphSearcher.search(
                                qv, k, shard.mravv, similarityFunction, graph, Bits.ALL);
                        for (SearchResult.NodeScore ns : result.getNodes()) {
                            Bytes pk = shard.nodeToPk.get(ns.node + shard.startNodeId);
                            if (pk != null && (pending == null || !pending.contains(pk))) {
                                results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING,
                                "error searching frozen shard for " + indexName, e);
                    }
                }
            }
        }

        // Merge and sort by score descending, take top-K
        results.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
        return results.size() <= topK ? results : results.subList(0, topK);
    }

    // -------------------------------------------------------------------------
    // Size and memory
    // -------------------------------------------------------------------------

    /**
     * Returns total vector count from live shards + on-disk segments.
     */
    @Override
    public int size() {
        int frozenCount = 0;
        List<LiveGraphShard> frozen = frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                frozenCount += shard.nodeToPk.size();
            }
        }
        return totalLiveSize() + frozenCount + (int) onDiskNodeToPkSize();
    }

    /**
     * Returns estimated memory usage in bytes.
     *
     * <p>Accounts for:
     * <ul>
     *   <li>Raw float vectors in VectorStorage</li>
     *   <li>HNSW graph structure (Neighbors, int[] node arrays, float[] score arrays,
     *       CompletionTracker) — via JVector's own {@code ramBytesUsed()}</li>
     *   <li>pkToNode + nodeToPk ConcurrentHashMap entries (~100 bytes per entry × 2)</li>
     *   <li>Bytes PK objects (~50 bytes average)</li>
     * </ul>
     */
    @Override
    public long estimatedMemoryUsageBytes() {
        long total = 0;
        for (LiveGraphShard shard : liveShards) {
            total += shardMemoryBytes(shard);
        }
        List<LiveGraphShard> frozen = frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                total += shardMemoryBytes(shard);
            }
        }
        return total;
    }

    private long shardMemoryBytes(LiveGraphShard shard) {
        long count = shard.vectorCount.get();
        long bytes = count * (long) dimension * Float.BYTES;
        bytes += count * 200L; // pkToNode + nodeToPk ConcurrentHashMap entries (~100B × 2)
        bytes += count * 50L;  // Bytes PK objects (average)
        if (shard.builder != null) {
            bytes += ((OnHeapGraphIndex) shard.builder.getGraph()).ramBytesUsed();
        }
        return bytes;
    }

    // -------------------------------------------------------------------------
    // Checkpoint
    // -------------------------------------------------------------------------

    /**
     * Performs a checkpoint, persisting live state to disk.
     * Uses three-phase checkpoint for FusedPQ format or simple format for small indexes.
     */
    public void checkpoint() throws DataStorageManagerException {
        try {
            doCheckpoint();
        } catch (IOException e) {
            throw new DataStorageManagerException(e);
        }
    }

    private void doCheckpoint() throws IOException, DataStorageManagerException {
        if (!checkpointLock.tryLock()) {
            LOGGER.log(Level.INFO, "checkpoint {0}: skipped (another checkpoint in progress)", indexName);
            return;
        }
        try {
            doCheckpointUnderLock();
        } finally {
            checkpointLock.unlock();
        }
    }

    private void doCheckpointUnderLock() throws IOException, DataStorageManagerException {
        long checkpointStartMs = System.currentTimeMillis();
        LogSequenceNumber sequenceNumber = LogSequenceNumber.START_OF_TIME;

        stateLock.writeLock().lock();
        try {
            boolean anySegmentDirty = segments.stream().anyMatch(s -> s.dirty);
            if (!dirty.get() && !anySegmentDirty) {
                LOGGER.log(Level.FINE, "checkpoint {0}: skipped (no changes)", indexName);
                return;
            }

            int totalLiveVectors = totalLiveSize();
            boolean hasLiveNodes = totalLiveVectors > 0;
            boolean hasOnDiskNodes = onDiskNodeToPkSize() > 0;

            if (!hasLiveNodes && !hasOnDiskNodes && liveShards.isEmpty() && segments.isEmpty()) {
                IndexStatus emptyStatus = new IndexStatus(
                        indexName, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, emptyStatus, false);
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint {0}: empty", indexName);
                return;
            }

            if (dimension == 0) {
                IndexStatus emptyStatus = new IndexStatus(
                        indexName, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, emptyStatus, false);
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint {0}: empty dimension", indexName);
                return;
            }

            int totalActiveVectors = (int) onDiskNodeToPkSize() + totalLiveVectors;

            if (totalActiveVectors == 0 && !segments.isEmpty()) {
                for (VectorSegment seg : segments) {
                    seg.close();
                    dropSegmentBLinkStorage(seg);
                }
                segments = new java.util.concurrent.CopyOnWriteArrayList<>();
                nextSegmentId.set(0);
                IndexStatus emptyStatus = new IndexStatus(
                        indexName, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, emptyStatus, false);
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint {0}: all vectors deleted, saving empty", indexName);
                return;
            }

            boolean useFusedPQ = fusedPQ
                    && dimension >= MIN_DIM_FOR_FUSED_PQ
                    && totalActiveVectors >= MIN_VECTORS_FOR_FUSED_PQ;

            if (!useFusedPQ) {
                doCheckpointSimpleUnderLock(sequenceNumber);
                totalSimpleCheckpointCount.incrementAndGet();
                totalCheckpointCount.incrementAndGet();
                lastCheckpointVectorsProcessed.set(totalActiveVectors);
                lastCheckpointDurationMs.set(System.currentTimeMillis() - checkpointStartMs);
                return;
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        // FusedPQ path: use three-phase checkpoint
        doCheckpointFusedPQThreePhase(sequenceNumber);
        totalFusedPQCheckpointCount.incrementAndGet();
        totalCheckpointCount.incrementAndGet();
        lastCheckpointDurationMs.set(System.currentTimeMillis() - checkpointStartMs);
    }

    /**
     * Simple format checkpoint -- runs entirely under write lock.
     * Must be called with stateLock.writeLock() held.
     */
    private void doCheckpointSimpleUnderLock(LogSequenceNumber sequenceNumber)
            throws IOException, DataStorageManagerException {
        // Collect all vectors from all live shards + on-disk segments
        ConcurrentHashMap<Integer, VectorFloat<?>> allVectors = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Bytes> allNodeToPk = new ConcurrentHashMap<>();
        int seqId = 0;
        for (LiveGraphShard shard : liveShards) {
            for (Map.Entry<Integer, Bytes> e : shard.nodeToPk.entrySet()) {
                VectorFloat<?> vec = vectorStorage.get(e.getKey());
                if (vec != null) {
                    allVectors.put(seqId, vec);
                    allNodeToPk.put(seqId, e.getValue());
                    seqId++;
                }
            }
        }
        if (!segments.isEmpty()) {
            for (VectorSegment seg : segments) {
                if (seg.onDiskGraph != null) {
                    try (OnDiskGraphIndex.View view = seg.onDiskGraph.getView();
                         Stream<Map.Entry<Bytes, Bytes>> scanStream = seg.scanNodeToPk()) {
                        List<Map.Entry<Bytes, Bytes>> entries = scanStream.collect(Collectors.toList());
                        for (Map.Entry<Bytes, Bytes> e : entries) {
                            int ordinal = VectorSegment.ordinalToBytes(0).equals(e.getKey()) ? 0 : bytesToOrdinal(e.getKey());
                            VectorFloat<?> vec = view.getVector(ordinal);
                            allVectors.put(seqId, vec);
                            allNodeToPk.put(seqId, e.getValue());
                            seqId++;
                        }
                    }
                }
                seg.close();
                dropSegmentBLinkStorage(seg);
            }
            segments = new java.util.concurrent.CopyOnWriteArrayList<>();
            nextSegmentId.set(0);
        }

        // Close all existing shard builders
        for (LiveGraphShard shard : liveShards) {
            if (shard.builder != null) {
                try {
                    shard.builder.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        // Release old nodeId slots
        for (LiveGraphShard shard : liveShards) {
            for (Integer oldNodeId : shard.nodeToPk.keySet()) {
                vectorStorage.remove(oldNodeId);
            }
        }

        // Rebuild a single shard with all data using remapped sequential IDs (0..seqId-1).
        // Use startNodeId=0 so local IDs in the builder match the remapped global IDs.
        nextNodeId.set(seqId);
        LiveGraphShard newShard = createEmptyLiveShard(dimension, beamWidth, neighborOverflow, alpha, 0);
        for (Map.Entry<Integer, VectorFloat<?>> e : allVectors.entrySet()) {
            vectorStorage.set(e.getKey(), e.getValue());
            newShard.vectorCount.incrementAndGet();
        }
        for (Map.Entry<Integer, Bytes> e : allNodeToPk.entrySet()) {
            newShard.nodeToPk.put(e.getKey(), e.getValue());
            newShard.pkToNode.put(e.getValue(), e.getKey());
        }
        for (Map.Entry<Integer, VectorFloat<?>> e : allVectors.entrySet()) {
            newShard.builder.addGraphNode(e.getKey(), e.getValue());
        }
        newShard.builder.cleanup();
        this.liveShards = new ArrayList<>(Collections.singletonList(newShard));

        List<Long> graphPageIds;
        Path graphTmpFile = Files.createTempFile(tmpDirectory, "herddb-vector-graph-", ".tmp");
        try {
            try (java.io.DataOutputStream graphDos = new java.io.DataOutputStream(
                    new BufferedOutputStream(new FileOutputStream(graphTmpFile.toFile()), CHUNK_SIZE))) {
                ((OnHeapGraphIndex) newShard.builder.getGraph()).save(graphDos);
            }
            graphPageIds = writeChunks(graphTmpFile, TYPE_VECTOR_GRAPHCHUNK);
        } finally {
            Files.deleteIfExists(graphTmpFile);
        }

        List<Long> mapPageIds;
        Path mapTmpFile = serializeMapDataToFile(vectorStorage, newShard.nodeToPk);
        try {
            mapPageIds = writeChunks(mapTmpFile, TYPE_VECTOR_MAPCHUNK);
        } finally {
            Files.deleteIfExists(mapTmpFile);
        }

        int totalNodes = newShard.nodeToPk.size();
        persistIndexStatusSimple(graphPageIds, mapPageIds, totalNodes, false, sequenceNumber);
        dirty.set(false);
        LOGGER.log(Level.INFO,
                "checkpoint {0}: {1} nodes (simple), {2} graph pages, {3} map pages",
                new Object[]{indexName, totalNodes, graphPageIds.size(), mapPageIds.size()});
    }

    /**
     * Three-phase FusedPQ checkpoint.
     */
    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private void doCheckpointFusedPQThreePhase(LogSequenceNumber sequenceNumber)
            throws IOException, DataStorageManagerException {

        // Phase A: snapshot + swap (brief write lock)
        List<LiveGraphShard> snapshotShards;
        List<VectorSegment> sealedSegments;
        List<VectorSegment> mergeableSegments;
        int snapshotDimension;

        stateLock.writeLock().lock();
        try {
            snapshotDimension = dimension;
            snapshotShards = this.liveShards;

            sealedSegments = new ArrayList<>();
            mergeableSegments = new ArrayList<>();
            for (VectorSegment seg : segments) {
                if (seg.isSealed(maxSegmentSize)) {
                    sealedSegments.add(seg);
                } else {
                    mergeableSegments.add(seg);
                }
            }

            // Segment-merge trigger: when sealed count is above the threshold,
            // demote the smallest sealed segments back to the mergeable pool so
            // the upcoming Phase B compacts them into larger segments.
            demoteSmallestSealedSegments(sealedSegments, mergeableSegments);

            this.frozenShards = snapshotShards;
            this.pendingCheckpointDeletes = ConcurrentHashMap.newKeySet();
            this.checkpointPhaseComplete = new CountDownLatch(1);
            int totalSnapshotSize = 0;
            for (LiveGraphShard shard : snapshotShards) {
                totalSnapshotSize += shard.nodeToPk.size();
            }
            long effectiveBudget = maxVectorMemoryBytes != Long.MAX_VALUE ? maxVectorMemoryBytes
                    : (memoryBudget != null ? memoryBudget.maxMemoryBytes() : Long.MAX_VALUE);
            this.liveVectorCapDuringCheckpoint = computeLiveVectorCapDuringCheckpoint(
                    totalSnapshotSize, snapshotDimension, m, neighborOverflow,
                    effectiveBudget, computeEffectiveMaxLiveGraphSize());

            initEmptyLiveShards(snapshotDimension, beamWidth, neighborOverflow, alpha);
            dirty.set(false);

            LOGGER.log(Level.INFO,
                    "checkpoint {0} Phase A: snapshotted {1} live shards ({2} vectors, dim={3}) + {4} on-disk vectors, "
                            + "{5} sealed + {6} mergeable segments",
                    new Object[]{indexName, snapshotShards.size(), totalSnapshotSize, snapshotDimension,
                            onDiskNodeToPkSize(), sealedSegments.size(), mergeableSegments.size()});
            LOGGER.log(Level.INFO,
                    "checkpoint {0} Phase A: liveVectorCapDuringCheckpoint={1}"
                            + " (frozenVectors={2}, dim={3}, budget={4})",
                    new Object[]{indexName, liveVectorCapDuringCheckpoint,
                            totalSnapshotSize, snapshotDimension, effectiveBudget});
        } finally {
            stateLock.writeLock().unlock();
        }

        // Phase B: build graphs, write to disk (NO lock)
        // Install a provisional-page tracker BEFORE any writes so the failure path
        // can reclaim partially-written pages instead of leaking them until the
        // next (possibly never-arriving) successful checkpoint.
        this.provisionalPageIds = Collections.synchronizedList(new ArrayList<>());
        List<SegmentWriteResult> newSegmentResults;
        try {
            Runnable hook = checkpointPhaseBHook;
            if (hook != null) {
                hook.run();
            }

            newSegmentResults = doCheckpointFusedPQPhaseB(
                    snapshotShards, snapshotDimension, sealedSegments, mergeableSegments, sequenceNumber);
        } catch (IOException | RuntimeException e) {
            LOGGER.log(Level.SEVERE, "checkpoint " + indexName + ": Phase B exception", e);
            rollbackProvisionalPages();
            consecutiveCheckpointFailures.incrementAndGet();
            totalCheckpointFailures.incrementAndGet();
            recoverFromPhaseBFailure(snapshotShards);
            throw e;
        }

        // Phase C-prep: pre-load new segments
        List<VectorSegment> preloadedSegments = new ArrayList<>();
        try {
            if (newSegmentResults != null) {
                for (SegmentWriteResult swr : newSegmentResults) {
                    VectorSegment seg = new VectorSegment(swr.segmentId);
                    seg.estimatedSizeBytes = swr.estimatedSizeBytes;
                    seg.graphPageIds = swr.graphPageIds;
                    seg.mapPageIds = swr.mapPageIds;
                    seg.graphFilePath = swr.graphFilePath;
                    seg.graphFileSize = swr.graphFileSize;
                    seg.mapFilePath = swr.mapFilePath;
                    seg.mapFileSize = swr.mapFileSize;
                    Path reloadMapFile;
                    if (swr.isMultipart()) {
                        reloadMapFile = readMultipartMapDataToTempFile(seg);
                    } else {
                        reloadMapFile = readChunksToTempFile(
                                swr.mapPageIds.stream().mapToLong(Long::longValue).toArray(),
                                TYPE_VECTOR_MAPCHUNK);
                    }
                    loadFusedPQSegment(seg, reloadMapFile, snapshotDimension, nextNodeId.get());
                    preloadedSegments.add(seg);
                }
            }
        } catch (IOException | RuntimeException e) {
            for (VectorSegment seg : preloadedSegments) {
                seg.close();
                dropSegmentBLinkStorage(seg);
            }
            // Phase B had already persisted an IndexStatus containing these new
            // segments, so the pages are now referenced on disk. We still call
            // rollbackProvisionalPages as a best-effort to delete them directly;
            // any that fail to delete will be reconciled by the next successful
            // indexCheckpoint sweep.
            rollbackProvisionalPages();
            consecutiveCheckpointFailures.incrementAndGet();
            totalCheckpointFailures.incrementAndGet();
            recoverFromPhaseBFailure(snapshotShards);
            throw e;
        }

        // Phase B + prep succeeded: the pages belong to the new persisted state.
        // Clear the tracker without deleting anything, and reset the failure count.
        this.provisionalPageIds = null;
        consecutiveCheckpointFailures.set(0);

        // Phase C: swap + cleanup (brief write lock)
        stateLock.writeLock().lock();
        try {
            for (LiveGraphShard shard : snapshotShards) {
                if (shard.builder != null) {
                    try {
                        shard.builder.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }

            if (newSegmentResults != null) {
                for (VectorSegment seg : mergeableSegments) {
                    seg.close();
                    dropSegmentBLinkStorage(seg);
                }

                List<VectorSegment> newSegments = new java.util.concurrent.CopyOnWriteArrayList<>();
                for (VectorSegment sealed : sealedSegments) {
                    sealed.dirty = false;
                    newSegments.add(sealed);
                }
                newSegments.addAll(preloadedSegments);

                Set<Bytes> pending = this.pendingCheckpointDeletes;
                if (pending != null) {
                    for (Bytes pk : pending) {
                        for (VectorSegment seg : newSegments) {
                            if (seg.deletePk(pk)) {
                                break;
                            }
                        }
                    }
                }

                this.segments = newSegments;

                int maxOrd = -1;
                for (VectorSegment seg : newSegments) {
                    if (seg.maxOrdinal > maxOrd) {
                        maxOrd = seg.maxOrdinal;
                    }
                }
                this.nextNodeId.set(Math.max(maxOrd + 1, nextNodeId.get()));

                int totalNodes = (int) onDiskNodeToPkSize() + totalLiveSize();
                LOGGER.log(Level.INFO,
                        "checkpoint {0} Phase C: {1} nodes across {2} segments (FusedPQ), "
                                + "{3} new live inserts during checkpoint",
                        new Object[]{indexName, totalNodes, newSegments.size(), totalLiveSize()});
            } else {
                int totalNodes = totalLiveSize() + (int) onDiskNodeToPkSize();
                LOGGER.log(Level.INFO,
                        "checkpoint {0} Phase C: {1} nodes (simple fallback)",
                        new Object[]{indexName, totalNodes});
            }

            // Release vectorStorage slots for checkpointed nodeIds
            if (snapshotShards != null) {
                for (LiveGraphShard shard : snapshotShards) {
                    for (Integer nodeId : shard.nodeToPk.keySet()) {
                        vectorStorage.remove(nodeId);
                    }
                }
            }

            // Shrink the backing array if most slots were freed
            vectorStorage.compact(nextNodeId.get());

            this.frozenShards = null;
            this.pendingCheckpointDeletes = null;
            this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
            dirty.set(totalLiveSize() > 0);

            recordSegmentSizeDistribution();
        } finally {
            CountDownLatch latch = this.checkpointPhaseComplete;
            this.checkpointPhaseComplete = null;
            stateLock.writeLock().unlock();
            if (latch != null) {
                latch.countDown();
            }
            synchronized (memoryPressureMonitor) {
                memoryPressureMonitor.notifyAll();
            }
        }
    }

    /**
     * Phase B for FusedPQ checkpoint. Runs without any lock.
     */
    private List<SegmentWriteResult> doCheckpointFusedPQPhaseB(
            List<LiveGraphShard> snapshotShards,
            int snapshotDimension,
            List<VectorSegment> sealedSegments,
            List<VectorSegment> mergeableSegments,
            LogSequenceNumber sequenceNumber)
            throws IOException, DataStorageManagerException {

        for (LiveGraphShard shard : snapshotShards) {
            if (shard.builder != null) {
                shard.builder.cleanup();
            }
        }

        // Collect all vectors from snapshot shards + mergeable segments
        List<VectorFloat<?>> poolVectorsList = new ArrayList<>();
        List<Bytes> poolPkList = new ArrayList<>();

        for (LiveGraphShard shard : snapshotShards) {
            for (Map.Entry<Integer, Bytes> e : shard.nodeToPk.entrySet()) {
                VectorFloat<?> vec = vectorStorage.get(e.getKey());
                if (vec != null) {
                    poolVectorsList.add(vec);
                    poolPkList.add(e.getValue());
                }
            }
        }

        for (VectorSegment seg : mergeableSegments) {
            if (seg.onDiskGraph != null) {
                try (OnDiskGraphIndex.View view = seg.onDiskGraph.getView();
                     Stream<Map.Entry<Bytes, Bytes>> scanStream = seg.scanNodeToPk()) {
                    scanStream.forEach(e -> {
                        int ordinal = bytesToOrdinal(e.getKey());
                        VectorFloat<?> vec = view.getVector(ordinal);
                        poolVectorsList.add(vec);
                        poolPkList.add(e.getValue());
                    });
                }
            }
        }

        // If pool too small for FusedPQ, unseal smallest sealed segments
        while (poolVectorsList.size() < MIN_VECTORS_FOR_FUSED_PQ && !sealedSegments.isEmpty()) {
            VectorSegment smallest = sealedSegments.stream()
                    .min((a, b) -> Long.compare(a.estimatedSizeBytes, b.estimatedSizeBytes))
                    .get();
            sealedSegments.remove(smallest);
            mergeableSegments.add(smallest);
            if (smallest.onDiskGraph != null) {
                try (OnDiskGraphIndex.View view = smallest.onDiskGraph.getView();
                     Stream<Map.Entry<Bytes, Bytes>> scanStream = smallest.scanNodeToPk()) {
                    scanStream.forEach(e -> {
                        int ordinal = bytesToOrdinal(e.getKey());
                        VectorFloat<?> vec = view.getVector(ordinal);
                        poolVectorsList.add(vec);
                        poolPkList.add(e.getValue());
                    });
                }
            }
        }

        // If pool still too small, fall back to simple path
        if (poolVectorsList.size() < MIN_VECTORS_FOR_FUSED_PQ && !poolVectorsList.isEmpty()) {
            for (VectorSegment seg : mergeableSegments) {
                seg.close();
                dropSegmentBLinkStorage(seg);
            }
            for (VectorSegment seg : sealedSegments) {
                seg.close();
                dropSegmentBLinkStorage(seg);
            }
            stateLock.writeLock().lock();
            try {
                segments = new java.util.concurrent.CopyOnWriteArrayList<>();
                nextSegmentId.set(0);
            } finally {
                stateLock.writeLock().unlock();
            }

            // Build maps from pool for simple checkpoint
            VectorStorage poolStorage = new VectorStorage(poolVectorsList.size());
            ConcurrentHashMap<Integer, Bytes> poolNodeToPk = new ConcurrentHashMap<>();
            for (int i = 0; i < poolVectorsList.size(); i++) {
                poolStorage.set(i, poolVectorsList.get(i));
                poolNodeToPk.put(i, poolPkList.get(i));
            }
            VectorStorageRandomAccessVectorValues poolMravv =
                    new VectorStorageRandomAccessVectorValues(poolStorage, snapshotDimension);
            BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(poolMravv, similarityFunction);
            GraphIndexBuilder poolBuilder = new GraphIndexBuilder(
                    bsp, snapshotDimension, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
            for (int i = 0; i < poolVectorsList.size(); i++) {
                poolBuilder.addGraphNode(i, poolVectorsList.get(i));
            }
            poolBuilder.cleanup();

            List<Long> graphPageIds;
            Path graphTmpFile = Files.createTempFile(tmpDirectory, "herddb-vector-graph-", ".tmp");
            try {
                try (java.io.DataOutputStream graphDos = new java.io.DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(graphTmpFile.toFile()), CHUNK_SIZE))) {
                    ((OnHeapGraphIndex) poolBuilder.getGraph()).save(graphDos);
                }
                graphPageIds = writeChunks(graphTmpFile, TYPE_VECTOR_GRAPHCHUNK);
            } finally {
                Files.deleteIfExists(graphTmpFile);
            }
            List<Long> mapPageIds;
            Path mapTmpFile = serializeMapDataToFile(poolStorage, poolNodeToPk);
            try {
                mapPageIds = writeChunks(mapTmpFile, TYPE_VECTOR_MAPCHUNK);
            } finally {
                Files.deleteIfExists(mapTmpFile);
            }
            try {
                poolBuilder.close();
            } catch (IOException e) {
                // ignore
            }
            persistIndexStatusSimple(graphPageIds, mapPageIds, poolNodeToPk.size(), false, sequenceNumber);
            lastCheckpointVectorsProcessed.set(poolNodeToPk.size());
            LOGGER.log(Level.INFO,
                    "checkpoint {0}: {1} nodes (simple fallback from FusedPQ)",
                    new Object[]{indexName, poolNodeToPk.size()});
            return null;
        }

        // Estimate avgBytesPerVector
        long avgBytesPerVector = (long) snapshotDimension * Float.BYTES * 2;
        for (VectorSegment seg : mergeableSegments) {
            long segSize = seg.size();
            if (segSize > 0 && seg.estimatedSizeBytes > 0) {
                avgBytesPerVector = seg.estimatedSizeBytes / segSize;
                break;
            }
        }
        if (avgBytesPerVector == (long) snapshotDimension * Float.BYTES * 2) {
            for (VectorSegment seg : sealedSegments) {
                long segSize = seg.size();
                if (segSize > 0 && seg.estimatedSizeBytes > 0) {
                    avgBytesPerVector = seg.estimatedSizeBytes / segSize;
                    break;
                }
            }
        }

        int maxNodesPerSegment = (int) Math.max(MIN_VECTORS_FOR_FUSED_PQ,
                maxSegmentSize / Math.max(1, avgBytesPerVector));

        int totalSegments = (int) Math.ceil((double) poolVectorsList.size() / maxNodesPerSegment);
        long phaseBStartMs = System.currentTimeMillis();
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: writing {1} vectors across ~{2} segments (parallelism={3})",
                new Object[]{indexName, poolVectorsList.size(), totalSegments,
                        PHASE_B_SEGMENT_PARALLELISM});

        // Slice the pool into segment-sized chunks up-front, so each task is
        // self-contained and we can parallelise independent segment builds.
        // Each slice carries its assigned segmentId so the allocation order is
        // deterministic and observable.
        List<SegmentSlice> slices = new ArrayList<>();
        int start = 0;
        int segmentIndex = 0;
        int totalPoolSize = poolVectorsList.size();
        while (start < totalPoolSize) {
            int end = Math.min(start + maxNodesPerSegment, totalPoolSize);
            if (end < totalPoolSize
                    && (totalPoolSize - end) < MIN_VECTORS_FOR_FUSED_PQ) {
                end = totalPoolSize;
            }
            segmentIndex++;
            int segId = nextSegmentId.getAndIncrement();
            slices.add(new SegmentSlice(segId, segmentIndex, start, end));
            start = end;
        }

        List<SegmentWriteResult> newSegmentResults =
                buildSegmentsInParallel(slices, poolVectorsList, poolPkList,
                        snapshotDimension, totalSegments);

        long phaseBElapsedMs = System.currentTimeMillis() - phaseBStartMs;
        lastCheckpointPhaseBDurationMs.set(phaseBElapsedMs);
        lastCheckpointVectorsProcessed.set(totalPoolSize);
        long bytesWritten = 0L;
        for (SegmentWriteResult r : newSegmentResults) {
            if (r.isMultipart()) {
                bytesWritten += r.graphFileSize + r.mapFileSize;
            } else {
                bytesWritten += (long) r.graphPageIds.size() * CHUNK_SIZE;
                bytesWritten += (long) r.mapPageIds.size() * CHUNK_SIZE;
            }
        }
        lastPhaseBBytesWritten.set(bytesWritten);
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: completed in {1} ms ({2} segments, {3} total vectors, {4} bytes)",
                new Object[]{indexName, phaseBElapsedMs, newSegmentResults.size(),
                        totalPoolSize, bytesWritten});

        // Order the results by segmentId so that the persisted IndexStatus and
        // the in-memory segment list are both deterministic.
        newSegmentResults.sort(java.util.Comparator.comparingInt(r -> r.segmentId));
        persistIndexStatusMultiSegment(sealedSegments, newSegmentResults, sequenceNumber);

        return newSegmentResults;
    }

    /**
     * Moves the smallest sealed segments into the mergeable list when the
     * number of sealed segments exceeds {@link #SEGMENT_MERGE_THRESHOLD}. This
     * is the knob that keeps on-disk segment count bounded: without it,
     * checkpoint only grows the sealed set monotonically (as observed in the
     * 1B-vector run that motivated this fix).
     *
     * <p>Package-private for testing.
     */
    static void chooseSegmentsToDemote(
            List<VectorSegment> sealedSegments,
            List<VectorSegment> demotions,
            int threshold, int batch) {
        demotions.clear();
        if (sealedSegments.size() <= threshold) {
            return;
        }
        // Pick the `batch` smallest sealed segments.
        List<VectorSegment> sorted = new ArrayList<>(sealedSegments);
        sorted.sort(java.util.Comparator.comparingLong(s -> s.estimatedSizeBytes));
        int pick = Math.min(batch, sorted.size());
        for (int i = 0; i < pick; i++) {
            demotions.add(sorted.get(i));
        }
    }

    private void demoteSmallestSealedSegments(
            List<VectorSegment> sealedSegments, List<VectorSegment> mergeableSegments) {
        if (sealedSegments.size() <= SEGMENT_MERGE_THRESHOLD) {
            return;
        }
        List<VectorSegment> demotions = new ArrayList<>();
        chooseSegmentsToDemote(sealedSegments, demotions,
                SEGMENT_MERGE_THRESHOLD, SEGMENT_MERGE_BATCH);
        if (demotions.isEmpty()) {
            return;
        }
        sealedSegments.removeAll(demotions);
        mergeableSegments.addAll(demotions);
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase A: demoted {1} sealed segments (smallest first) for merging "
                        + "(sealed={2}, threshold={3})",
                new Object[]{indexName, demotions.size(), sealedSegments.size(),
                        SEGMENT_MERGE_THRESHOLD});
    }

    /** Slice descriptor: one FusedPQ segment to build in Phase B. */
    private static final class SegmentSlice {
        final int segmentId;
        final int oneBasedIndex;
        final int fromInclusive;
        final int toExclusive;

        SegmentSlice(int segmentId, int oneBasedIndex, int fromInclusive, int toExclusive) {
            this.segmentId = segmentId;
            this.oneBasedIndex = oneBasedIndex;
            this.fromInclusive = fromInclusive;
            this.toExclusive = toExclusive;
        }

        int size() {
            return toExclusive - fromInclusive;
        }
    }

    /**
     * Builds all slices in parallel, bounded by {@link #PHASE_B_SEGMENT_PARALLELISM}.
     * If any build fails, all remaining tasks are cancelled and the first failure
     * is rethrown. Successful segment-local allocations are still tracked in
     * {@link #provisionalPageIds} so the outer rollback path can reclaim them.
     */
    private List<SegmentWriteResult> buildSegmentsInParallel(
            List<SegmentSlice> slices,
            List<VectorFloat<?>> poolVectorsList,
            List<Bytes> poolPkList,
            int snapshotDimension,
            int totalSegments) throws IOException, DataStorageManagerException {

        int parallelism = Math.min(PHASE_B_SEGMENT_PARALLELISM, Math.max(1, slices.size()));
        if (parallelism == 1) {
            // Fast path, no extra threads.
            List<SegmentWriteResult> results = new ArrayList<>(slices.size());
            for (SegmentSlice s : slices) {
                results.add(buildOneSegment(s, poolVectorsList, poolPkList,
                        snapshotDimension, totalSegments));
                // Release references immediately in the serial path.
                releaseSliceReferences(s, poolVectorsList, poolPkList);
            }
            return results;
        }

        java.util.concurrent.ExecutorService executor =
                java.util.concurrent.Executors.newFixedThreadPool(parallelism, r -> {
                    Thread t = new Thread(r, "persistent-vector-store-phaseB-" + indexName);
                    t.setDaemon(true);
                    return t;
                });
        try {
            List<java.util.concurrent.Future<SegmentWriteResult>> futures = new ArrayList<>(slices.size());
            for (SegmentSlice s : slices) {
                futures.add(executor.submit(() -> buildOneSegment(s,
                        poolVectorsList, poolPkList, snapshotDimension, totalSegments)));
            }
            List<SegmentWriteResult> results = new ArrayList<>(slices.size());
            Throwable firstFailure = null;
            for (int i = 0; i < futures.size(); i++) {
                java.util.concurrent.Future<SegmentWriteResult> f = futures.get(i);
                try {
                    SegmentWriteResult r = f.get();
                    results.add(r);
                    // Release slice references once the segment is durable.
                    releaseSliceReferences(slices.get(i), poolVectorsList, poolPkList);
                } catch (java.util.concurrent.ExecutionException ee) {
                    if (firstFailure == null) {
                        firstFailure = ee.getCause() != null ? ee.getCause() : ee;
                    }
                    for (int j = i + 1; j < futures.size(); j++) {
                        futures.get(j).cancel(true);
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    if (firstFailure == null) {
                        firstFailure = ie;
                    }
                }
            }
            if (firstFailure != null) {
                if (firstFailure instanceof IOException) {
                    throw (IOException) firstFailure;
                }
                if (firstFailure instanceof DataStorageManagerException) {
                    throw (DataStorageManagerException) firstFailure;
                }
                if (firstFailure instanceof RuntimeException) {
                    throw (RuntimeException) firstFailure;
                }
                throw new IOException("Phase B parallel build failed", firstFailure);
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    private SegmentWriteResult buildOneSegment(
            SegmentSlice s,
            List<VectorFloat<?>> poolVectorsList,
            List<Bytes> poolPkList,
            int snapshotDimension,
            int totalSegments) throws IOException, DataStorageManagerException {

        ConcurrentHashMap<Integer, VectorFloat<?>> partVectors = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Bytes> partNodeToPk = new ConcurrentHashMap<>();
        VectorStorage partStorage = new VectorStorage(s.size());
        for (int i = s.fromInclusive; i < s.toExclusive; i++) {
            int seqId = i - s.fromInclusive;
            partVectors.put(seqId, poolVectorsList.get(i));
            partStorage.set(seqId, poolVectorsList.get(i));
            partNodeToPk.put(seqId, poolPkList.get(i));
        }

        long segStartMs = System.currentTimeMillis();
        SegmentWriteResult result = writeOneSegmentData(
                s, partVectors, partNodeToPk, partStorage, snapshotDimension);

        long segElapsedMs = System.currentTimeMillis() - segStartMs;
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: segment {1}/{2} ({3} nodes) written in {4} ms ({5})",
                new Object[]{indexName, s.oneBasedIndex, totalSegments, s.size(), segElapsedMs,
                        result.isMultipart() ? "multipart" : "page-based"});

        return result;
    }

    private static void releaseSliceReferences(
            SegmentSlice s, List<VectorFloat<?>> poolVectorsList, List<Bytes> poolPkList) {
        for (int i = s.fromInclusive; i < s.toExclusive; i++) {
            poolVectorsList.set(i, null);
            poolPkList.set(i, null);
        }
    }

    /**
     * Deletes any pageIds tracked in {@link #provisionalPageIds}. Called when
     * Phase B or Phase C-prep aborts before the new state becomes durable so
     * that the partially-written pages do not linger on disk until the next
     * successful {@code indexCheckpoint} sweep (which may never arrive if the
     * disk is full).
     *
     * <p>The method always clears the tracker, whether or not any deletes
     * succeeded. Delete failures are logged but not rethrown — the failure has
     * already been signalled via the original Phase B exception.
     */
    private void rollbackProvisionalPages() {
        List<Long> toRollback = this.provisionalPageIds;
        this.provisionalPageIds = null;
        if (toRollback == null || toRollback.isEmpty()) {
            return;
        }
        List<Long> snapshot;
        synchronized (toRollback) {
            snapshot = new ArrayList<>(toRollback);
        }
        int rolled = 0;
        int failed = 0;
        for (long pageId : snapshot) {
            try {
                dataStorageManager.deleteIndexPage(tableSpaceUUID, indexUUID, pageId);
                rolled++;
            } catch (DataStorageManagerException e) {
                failed++;
                LOGGER.log(Level.WARNING,
                        "checkpoint " + indexName + ": failed to delete provisional pageId " + pageId, e);
            }
        }
        totalRolledBackPages.addAndGet(rolled);
        lastRolledBackPages.set(rolled);
        LOGGER.log(Level.WARNING,
                "checkpoint {0}: rolled back {1} provisional pages ({2} delete failures)",
                new Object[]{indexName, rolled, failed});
    }

    /**
     * Recovers from a Phase B failure by merging frozen state back into live state.
     */
    private void recoverFromPhaseBFailure(List<LiveGraphShard> snapshotShards) {
        stateLock.writeLock().lock();
        try {
            LOGGER.log(Level.WARNING,
                    "checkpoint {0}: Phase B failed, restoring frozen state", indexName);

            List<LiveGraphShard> currentShards = this.liveShards;
            LiveGraphShard lastSnapshot = snapshotShards.get(snapshotShards.size() - 1);

            for (LiveGraphShard currentShard : currentShards) {
                for (Map.Entry<Integer, Bytes> e : currentShard.nodeToPk.entrySet()) {
                    int nodeId = e.getKey();
                    VectorFloat<?> vec = vectorStorage.get(nodeId);
                    Bytes pk = e.getValue();
                    if (vec != null) {
                        lastSnapshot.pkToNode.put(pk, nodeId);
                        lastSnapshot.nodeToPk.put(nodeId, pk);
                        lastSnapshot.vectorCount.incrementAndGet();
                        if (lastSnapshot.builder != null) {
                            try {
                                lastSnapshot.builder.addGraphNode(nodeId - lastSnapshot.startNodeId, vec);
                            } catch (Exception ex) {
                                LOGGER.log(Level.WARNING, "Failed to re-add node during recovery", ex);
                            }
                        }
                    }
                }
                if (currentShard.builder != null) {
                    try {
                        currentShard.builder.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }

            this.liveShards = new ArrayList<>(snapshotShards);
            this.frozenShards = null;
            this.pendingCheckpointDeletes = null;
            this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
            dirty.set(true);
        } finally {
            CountDownLatch latch = this.checkpointPhaseComplete;
            this.checkpointPhaseComplete = null;
            stateLock.writeLock().unlock();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load from status
    // -------------------------------------------------------------------------

    private void loadFromStatus(IndexStatus status) throws IOException, DataStorageManagerException {
        ByteBuffer metaBuf = ByteBuffer.wrap(status.indexData);

        int version = metaBuf.getInt();
        if (version != METADATA_VERSION_MULTI_SEGMENT) {
            LOGGER.log(Level.SEVERE,
                    "unsupported vector index metadata version {0} for {1} (only v{2} is supported),"
                            + " starting empty — old experimental formats have been removed",
                    new Object[]{version, indexName, METADATA_VERSION_MULTI_SEGMENT});
            return;
        }

        int dim = metaBuf.getInt();
        int savedM = metaBuf.getInt();
        int savedBeamWidth = metaBuf.getInt();
        float savedNeighborOverflow = metaBuf.getFloat();
        float savedAlpha = metaBuf.getFloat();
        /* boolean savedAddHierarchy = */ metaBuf.get();
        /* boolean savedFusedPQ = */ metaBuf.get();

        int savedNextNodeId = metaBuf.getInt();

        this.dimension = dim;
        newPageId.set(status.newPageId);

        loadMultiSegmentFormat(metaBuf, dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
    }

    private void loadMultiSegmentFormat(ByteBuffer metaBuf, int dim, int savedNextNodeId,
                                         int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException, DataStorageManagerException {
        java.io.DataInputStream dis = new java.io.DataInputStream(
                new java.io.ByteArrayInputStream(metaBuf.array(), metaBuf.position(),
                        metaBuf.remaining()));
        int numSegments;
        try {
            numSegments = dis.readInt();
        } catch (IOException e) {
            throw new DataStorageManagerException("Failed to read segment count", e);
        }

        if (dim == 0 || numSegments == 0) {
            LOGGER.log(Level.INFO, "vector store {0} is empty (multi-segment), no load needed", indexName);
            return;
        }

        int maxSegId = -1;
        for (int s = 0; s < numSegments; s++) {
            int segId;
            long estimatedSize;
            boolean multipart;
            try {
                segId = dis.readInt();
                estimatedSize = dis.readLong();
                multipart = dis.readByte() == 1;
            } catch (IOException e) {
                throw new DataStorageManagerException("Failed to read segment header", e);
            }

            VectorSegment seg = new VectorSegment(segId);
            seg.estimatedSizeBytes = estimatedSize;

            if (multipart) {
                try {
                    String graphFilePath = dis.readUTF();
                    long graphFileSize = dis.readLong();
                    String mapFilePath = dis.readUTF();
                    long mapFileSize = dis.readLong();
                    seg.graphFilePath = graphFilePath;
                    seg.graphFileSize = graphFileSize;
                    seg.mapFilePath = mapFilePath.isEmpty() ? null : mapFilePath;
                    seg.mapFileSize = mapFileSize;
                } catch (IOException e) {
                    throw new DataStorageManagerException("Failed to read multipart segment metadata", e);
                }
                // For multipart, we need to read map data via the multipart reader
                Path mapFile = readMultipartMapDataToTempFile(seg);
                loadFusedPQSegment(seg, mapFile, dim, savedNextNodeId);
            } else {
                try {
                    int numGraphChunks = dis.readInt();
                    long[] graphChunkPageIds = new long[numGraphChunks];
                    for (int i = 0; i < numGraphChunks; i++) {
                        graphChunkPageIds[i] = dis.readLong();
                    }
                    int numMapChunks = dis.readInt();
                    long[] mapChunkPageIds = new long[numMapChunks];
                    for (int i = 0; i < numMapChunks; i++) {
                        mapChunkPageIds[i] = dis.readLong();
                    }
                    seg.graphPageIds = toLongList(graphChunkPageIds);
                    seg.mapPageIds = toLongList(mapChunkPageIds);
                    Path mapFile = readChunksToTempFile(mapChunkPageIds, TYPE_VECTOR_MAPCHUNK);
                    loadFusedPQSegment(seg, mapFile, dim, savedNextNodeId);
                } catch (IOException e) {
                    throw new DataStorageManagerException("Failed to read page-based segment metadata", e);
                }
            }
            segments.add(seg);
            if (segId > maxSegId) {
                maxSegId = segId;
            }
        }
        nextSegmentId.set(maxSegId + 1);

        int maxOrd = -1;
        for (VectorSegment seg : segments) {
            if (seg.maxOrdinal > maxOrd) {
                maxOrd = seg.maxOrdinal;
            }
        }
        this.nextNodeId.set(maxOrd + 1);

        initEmptyLiveShards(dim, savedBeamWidth, savedNeighborOverflow, savedAlpha);

        LOGGER.log(Level.INFO,
                "loaded vector store {0} (multi-segment): {1} segments, dimension {2}",
                new Object[]{indexName, numSegments, dim});
    }

    /**
     * Loads a single FusedPQ segment. The map data is read from a one-shot
     * temp file (linear scan, then deleted). The graph is <em>not</em>
     * materialised on disk: instead {@link seg#graphPageIds} is plumbed
     * through a {@link PageStoreReader.Supplier} that reads graph bytes on
     * demand from the page store, with a small bounded LRU cache.
     *
     * <p>Either {@code seg.graphFilePath} (multipart mode) or
     * {@code seg.graphPageIds} (page-based mode) must be populated
     * before this method is called.
     */
    private void loadFusedPQSegment(VectorSegment seg, Path mapFile, int dim, int savedNextNodeId)
            throws IOException, DataStorageManagerException {
        if (seg.graphFilePath == null && (seg.graphPageIds == null || seg.graphPageIds.isEmpty())) {
            throw new IllegalStateException(
                    "loadFusedPQSegment requires either seg.graphFilePath (multipart) "
                    + "or seg.graphPageIds (page-based) to be populated");
        }
        createSegmentBLinks(seg);

        int entryCount;
        int maxOrdinal = -1;

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()), CHUNK_SIZE))) {
            entryCount = dis.readInt();
            java.io.ByteArrayOutputStream pkBuf = new java.io.ByteArrayOutputStream(entryCount * 8);
            int[] tempOrdinals = new int[entryCount];
            int[] tempPkLengths = new int[entryCount];
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                int pkLen = dis.readInt();
                byte[] pkBytes = new byte[pkLen];
                dis.readFully(pkBytes);
                int floatCount = dis.readInt();
                skipFully(dis, (long) floatCount * Float.BYTES);

                tempOrdinals[i] = ordinal;
                tempPkLengths[i] = pkLen;
                pkBuf.write(pkBytes);

                Bytes pk = Bytes.from_array(pkBytes);
                seg.onDiskPkToNode.insert(pk, (long) ordinal);
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            byte[] allPkData = pkBuf.toByteArray();

            // Build compact cache arrays
            if (maxOrdinal >= 0) {
                int cacheSize = maxOrdinal + 1;
                int[] offsets = new int[cacheSize];
                int[] lengths = new int[cacheSize];
                java.util.Arrays.fill(offsets, -1);
                int pos = 0;
                for (int i = 0; i < entryCount; i++) {
                    offsets[tempOrdinals[i]] = pos;
                    lengths[tempOrdinals[i]] = tempPkLengths[i];
                    pos += tempPkLengths[i];
                }
                seg.pkData = allPkData;
                seg.pkOffsets = offsets;
                seg.pkLengths = lengths;
            } else {
                seg.pkData = new byte[0];
                seg.pkOffsets = new int[0];
                seg.pkLengths = new int[0];
            }
            seg.liveCount.set(entryCount);
            seg.maxOrdinal = maxOrdinal;
        }

        Files.deleteIfExists(mapFile);

        ReaderSupplier readerSupplier;
        if (seg.graphFilePath != null) {
            // Multipart mode: use remote reader via storage manager
            readerSupplier = dataStorageManager.multipartIndexReaderSupplier(
                    tableSpaceUUID,
                    indexUUID + "_seg" + seg.segmentId,
                    "graph",
                    seg.graphFileSize);
            if (readerSupplier == null) {
                throw new DataStorageManagerException(
                        "Storage manager does not support multipart reads for segment " + seg.segmentId);
            }
        } else {
            long[] graphPageIds = new long[seg.graphPageIds.size()];
            for (int i = 0; i < graphPageIds.length; i++) {
                graphPageIds[i] = seg.graphPageIds.get(i);
            }
            readerSupplier = new PageStoreReader.Supplier(
                    dataStorageManager, tableSpaceUUID, indexUUID,
                    graphPageIds, TYPE_VECTOR_GRAPHCHUNK,
                    PageStoreReader.DEFAULT_LRU_PAGES);
        }
        seg.onDiskGraph = OnDiskGraphIndex.load(readerSupplier);
        seg.onDiskReaderSupplier = readerSupplier;
        // onDiskGraphFile intentionally left null: no resident copy on disk.

        LOGGER.log(Level.INFO,
                "loaded vector segment {0} for store {1}: {2} nodes",
                new Object[]{seg.segmentId, indexName, seg.size()});
    }

    // -------------------------------------------------------------------------
    // FusedPQ graph building
    // -------------------------------------------------------------------------

    /**
     * Writes the graph and map data for one segment. Uses multipart writes when the storage
     * manager supports them (e.g. RemoteFileDataStorageManager in S3 mode), otherwise falls
     * back to the existing page-based approach.
     */
    private SegmentWriteResult writeOneSegmentData(
            SegmentSlice s,
            ConcurrentHashMap<Integer, VectorFloat<?>> partVectors,
            ConcurrentHashMap<Integer, Bytes> partNodeToPk,
            VectorStorage partStorage,
            int snapshotDimension)
            throws IOException, DataStorageManagerException {

        // Try multipart write via storage manager
        Path graphTempFile = writeFusedPQGraphToTempFile(partVectors, partNodeToPk, snapshotDimension);
        try {
            String graphFilePath = dataStorageManager.writeMultipartIndexFile(
                    tableSpaceUUID,
                    indexUUID + "_seg" + s.segmentId,
                    "graph",
                    graphTempFile);
            if (graphFilePath != null) {
                long graphFileSize = Files.size(graphTempFile);
                // Map data
                Path mapTempFile = writeFusedPQMapDataToTempFile(
                        new VectorStorageRandomAccessVectorValues(partStorage, snapshotDimension),
                        partNodeToPk);
                try {
                    String mapFilePath = dataStorageManager.writeMultipartIndexFile(
                            tableSpaceUUID,
                            indexUUID + "_seg" + s.segmentId,
                            "map",
                            mapTempFile);
                    long mapFileSize = Files.size(mapTempFile);
                    long estimatedSize = graphFileSize + mapFileSize;
                    return new SegmentWriteResult(s.segmentId,
                            graphFilePath, graphFileSize,
                            mapFilePath, mapFileSize,
                            estimatedSize);
                } finally {
                    Files.deleteIfExists(mapTempFile);
                }
            }
        } finally {
            Files.deleteIfExists(graphTempFile);
        }

        // Fallback: page-based writes
        List<Long> graphPageIds = writeFusedPQGraph(partVectors, partNodeToPk, snapshotDimension);
        List<Long> mapPageIds = writeFusedPQMapData(
                new VectorStorageRandomAccessVectorValues(partStorage, snapshotDimension), partNodeToPk);
        long estimatedSize = (long) (graphPageIds.size() + mapPageIds.size()) * CHUNK_SIZE;
        return new SegmentWriteResult(s.segmentId, graphPageIds, mapPageIds, estimatedSize);
    }

    /**
     * Writes the merged graph as FusedPQ on-disk format.
     */
    private List<Long> writeFusedPQGraph(ConcurrentHashMap<Integer, VectorFloat<?>> allVectors,
                                          ConcurrentHashMap<Integer, Bytes> allNodeToPk,
                                          int dim) throws IOException, DataStorageManagerException {
        if (allNodeToPk.isEmpty()) {
            return writeChunks(new byte[0], TYPE_VECTOR_GRAPHCHUNK);
        }

        int totalVectors = allNodeToPk.size();

        long graphStartMs = System.currentTimeMillis();
        LOGGER.log(Level.INFO,
                "writeFusedPQGraph {0}: building graph for {1} vectors (dim={2}) using {3} threads",
                new Object[]{indexName, totalVectors, dim, CHECKPOINT_POOL.getParallelism()});

        VectorStorage allStorage = new VectorStorage(allVectors.size());
        allVectors.forEach(allStorage::set);
        VectorStorageRandomAccessVectorValues allMravv =
                new VectorStorageRandomAccessVectorValues(allStorage, dim, allVectors.size());
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(allMravv, similarityFunction);
        GraphIndexBuilder mergedBuilder = new GraphIndexBuilder(
                bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH,
                PhysicalCoreExecutor.pool(), CHECKPOINT_POOL);

        int progressInterval = Math.max(1000, totalVectors / 10);
        AtomicInteger nodesAdded = new AtomicInteger(0);
        // Submit graph-building work to the checkpoint pool.
        java.util.concurrent.ForkJoinTask<?> graphTask = CHECKPOINT_POOL.submit(() ->
                allVectors.entrySet().parallelStream()
                    .filter(e -> allNodeToPk.containsKey(e.getKey()))
                    .forEach(e -> {
                        mergedBuilder.addGraphNode(e.getKey(), e.getValue());
                        int count = nodesAdded.incrementAndGet();
                        if (count % progressInterval == 0) {
                            LOGGER.log(Level.INFO,
                                    "writeFusedPQGraph {0}: added {1}/{2} nodes ({3}%)",
                                    new Object[]{indexName, count, totalVectors,
                                            (int) (100.0 * count / totalVectors)});
                        }
                    })
        );
        try {
            graphTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("writeFusedPQGraph interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException("writeFusedPQGraph failed", cause);
        }
        mergedBuilder.cleanup();
        OnHeapGraphIndex mergedGraph = (OnHeapGraphIndex) mergedBuilder.getGraph();
        long graphElapsedMs = System.currentTimeMillis() - graphStartMs;
        LOGGER.log(Level.INFO,
                "writeFusedPQGraph {0}: graph built in {1} ms ({2} nodes)",
                new Object[]{indexName, graphElapsedMs, totalVectors});

        // Compute PQ
        long pqStartMs = System.currentTimeMillis();
        int pqSubspaces = Math.max(1, dim / 4);
        ProductQuantization pq = ProductQuantization.compute(allMravv, pqSubspaces, 256, true);
        PQVectors pqv = pq.encodeAll(allMravv, PhysicalCoreExecutor.pool());
        long pqElapsedMs = System.currentTimeMillis() - pqStartMs;
        LOGGER.log(Level.INFO,
                "writeFusedPQGraph {0}: PQ computed in {1} ms",
                new Object[]{indexName, pqElapsedMs});

        // Write to temp file
        long writeStartMs = System.currentTimeMillis();
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-vector-", ".idx");
        try {
            try (OnDiskGraphIndexWriter writer = new OnDiskGraphIndexWriter.Builder(mergedGraph, tempFile)
                    .with(new FusedPQ(mergedGraph.maxDegree(), pq))
                    .with(new InlineVectors(dim))
                    .build()) {
                ImmutableGraphIndex.View view = mergedGraph.getView();
                EnumMap<FeatureId, IntFunction<io.github.jbellis.jvector.graph.disk.feature.Feature.State>> suppliers =
                        new EnumMap<>(FeatureId.class);
                suppliers.put(FeatureId.FUSED_PQ,
                        ordinal -> new FusedPQ.State(view, pqv, ordinal));
                suppliers.put(FeatureId.INLINE_VECTORS,
                        ordinal -> new InlineVectors.State(allMravv.getVector(ordinal)));
                writer.write(suppliers);
            }
            List<Long> pages = writeChunks(tempFile, TYPE_VECTOR_GRAPHCHUNK);
            long writeElapsedMs = System.currentTimeMillis() - writeStartMs;
            LOGGER.log(Level.INFO,
                    "writeFusedPQGraph {0}: disk write completed in {1} ms ({2} pages)",
                    new Object[]{indexName, writeElapsedMs, pages.size()});
            return pages;
        } finally {
            Files.deleteIfExists(tempFile);
            try {
                mergedBuilder.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Writes map data for FusedPQ format.
     */
    private List<Long> writeFusedPQMapData(RandomAccessVectorValues allVectors,
                                            ConcurrentHashMap<Integer, Bytes> allNodeToPk)
            throws IOException, DataStorageManagerException {
        List<Integer> sortedNodeIds = new ArrayList<>(allNodeToPk.keySet());
        java.util.Collections.sort(sortedNodeIds);
        Map<Integer, Integer> oldToNew = new java.util.HashMap<>();
        for (int i = 0; i < sortedNodeIds.size(); i++) {
            oldToNew.put(sortedNodeIds.get(i), i);
        }

        Path mapTmpFile = Files.createTempFile(tmpDirectory, "herddb-vector-map-", ".tmp");
        try {
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(mapTmpFile.toFile()), CHUNK_SIZE)) {
                int entryCount = sortedNodeIds.size();
                writeInt(bos, entryCount);

                for (int oldId : sortedNodeIds) {
                    int newOrdinal = oldToNew.get(oldId);
                    Bytes pk = allNodeToPk.get(oldId);
                    byte[] pkBytes = pk.to_array();
                    VectorFloat<?> vec = allVectors.getVector(oldId);

                    writeInt(bos, newOrdinal);
                    writeInt(bos, pkBytes.length);
                    bos.write(pkBytes);
                    int floatCount = vec.length();
                    writeInt(bos, floatCount);
                    for (int j = 0; j < floatCount; j++) {
                        int bits = Float.floatToIntBits(vec.get(j));
                        writeInt(bos, bits);
                    }
                }
            }
            return writeChunks(mapTmpFile, TYPE_VECTOR_MAPCHUNK);
        } finally {
            Files.deleteIfExists(mapTmpFile);
        }
    }

    /**
     * Builds the FusedPQ graph and writes it to a new temp file.
     * The caller is responsible for deleting the returned file.
     */
    private Path writeFusedPQGraphToTempFile(
            ConcurrentHashMap<Integer, VectorFloat<?>> allVectors,
            ConcurrentHashMap<Integer, Bytes> allNodeToPk,
            int dim) throws IOException, DataStorageManagerException {
        if (allNodeToPk.isEmpty()) {
            Path empty = Files.createTempFile(tmpDirectory, "herddb-vector-empty-", ".idx");
            return empty;
        }
        int totalVectors = allNodeToPk.size();
        VectorStorage allStorage = new VectorStorage(allVectors.size());
        allVectors.forEach(allStorage::set);
        VectorStorageRandomAccessVectorValues allMravv =
                new VectorStorageRandomAccessVectorValues(allStorage, dim, allVectors.size());
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(allMravv, similarityFunction);
        GraphIndexBuilder mergedBuilder = new GraphIndexBuilder(
                bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH,
                PhysicalCoreExecutor.pool(), CHECKPOINT_POOL);

        int progressInterval = Math.max(1000, totalVectors / 10);
        AtomicInteger nodesAdded = new AtomicInteger(0);
        java.util.concurrent.ForkJoinTask<?> graphTask = CHECKPOINT_POOL.submit(() ->
                allVectors.entrySet().parallelStream()
                    .filter(e -> allNodeToPk.containsKey(e.getKey()))
                    .forEach(e -> {
                        mergedBuilder.addGraphNode(e.getKey(), e.getValue());
                        int count = nodesAdded.incrementAndGet();
                        if (count % progressInterval == 0) {
                            LOGGER.log(Level.INFO,
                                    "writeFusedPQGraphToTempFile {0}: added {1}/{2} nodes ({3}%)",
                                    new Object[]{indexName, count, totalVectors,
                                            (int) (100.0 * count / totalVectors)});
                        }
                    })
        );
        try {
            graphTask.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("writeFusedPQGraphToTempFile interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("writeFusedPQGraphToTempFile failed", cause);
        }
        mergedBuilder.cleanup();
        OnHeapGraphIndex mergedGraph = (OnHeapGraphIndex) mergedBuilder.getGraph();

        int pqSubspaces = Math.max(1, dim / 4);
        ProductQuantization pq = ProductQuantization.compute(allMravv, pqSubspaces, 256, true);
        PQVectors pqv = pq.encodeAll(allMravv, PhysicalCoreExecutor.pool());

        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-vector-", ".idx");
        boolean success = false;
        try {
            try (OnDiskGraphIndexWriter writer = new OnDiskGraphIndexWriter.Builder(mergedGraph, tempFile)
                    .with(new FusedPQ(mergedGraph.maxDegree(), pq))
                    .with(new InlineVectors(dim))
                    .build()) {
                ImmutableGraphIndex.View view = mergedGraph.getView();
                EnumMap<FeatureId, IntFunction<io.github.jbellis.jvector.graph.disk.feature.Feature.State>> suppliers =
                        new EnumMap<>(FeatureId.class);
                suppliers.put(FeatureId.FUSED_PQ, ordinal -> new FusedPQ.State(view, pqv, ordinal));
                suppliers.put(FeatureId.INLINE_VECTORS,
                        ordinal -> new InlineVectors.State(allMravv.getVector(ordinal)));
                writer.write(suppliers);
            }
            success = true;
            return tempFile;
        } finally {
            if (!success) {
                Files.deleteIfExists(tempFile);
            }
            try {
                mergedBuilder.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Builds the map data and writes it to a new temp file.
     * The caller is responsible for deleting the returned file.
     */
    private Path writeFusedPQMapDataToTempFile(
            RandomAccessVectorValues allVectors,
            ConcurrentHashMap<Integer, Bytes> allNodeToPk) throws IOException {
        List<Integer> sortedNodeIds = new ArrayList<>(allNodeToPk.keySet());
        java.util.Collections.sort(sortedNodeIds);
        Map<Integer, Integer> oldToNew = new java.util.HashMap<>();
        for (int i = 0; i < sortedNodeIds.size(); i++) {
            oldToNew.put(sortedNodeIds.get(i), i);
        }
        Path mapTmpFile = Files.createTempFile(tmpDirectory, "herddb-vector-map-", ".tmp");
        boolean success = false;
        try {
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(mapTmpFile.toFile()), CHUNK_SIZE)) {
                int entryCount = sortedNodeIds.size();
                writeInt(bos, entryCount);
                for (int oldId : sortedNodeIds) {
                    int newOrdinal = oldToNew.get(oldId);
                    Bytes pk = allNodeToPk.get(oldId);
                    byte[] pkBytes = pk.to_array();
                    VectorFloat<?> vec = allVectors.getVector(oldId);
                    writeInt(bos, newOrdinal);
                    writeInt(bos, pkBytes.length);
                    bos.write(pkBytes);
                    int floatCount = vec.length();
                    writeInt(bos, floatCount);
                    for (int j = 0; j < floatCount; j++) {
                        int bits = Float.floatToIntBits(vec.get(j));
                        writeInt(bos, bits);
                    }
                }
            }
            success = true;
            return mapTmpFile;
        } finally {
            if (!success) {
                Files.deleteIfExists(mapTmpFile);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Chunk I/O methods
    // -------------------------------------------------------------------------

    /**
     * Reads index chunk pages into a temporary file and returns the path.
     */
    private Path readChunksToTempFile(long[] pageIds, int expectedChunkType)
            throws IOException, DataStorageManagerException {
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-vector-", ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, CHUNK_SIZE)) {
            for (long pageId : pageIds) {
                byte[] chunkData = dataStorageManager.readIndexPage(
                        tableSpaceUUID, indexUUID, pageId,
                        in -> {
                            int type = in.readVInt();
                            if (type != expectedChunkType) {
                                throw new IOException(
                                        "page " + pageId + ": expected type "
                                                + expectedChunkType + " but got " + type);
                            }
                            int len = in.readVInt();
                            byte[] data = new byte[len];
                            in.readArray(len, data);
                            return data;
                        });
                bos.write(chunkData);
            }
        }
        return tempFile;
    }

    /**
     * Downloads the map data for a multipart segment into a local temp file.
     * Uses the storage manager's multipart reader supplier to read the map file.
     * The caller is responsible for deleting the returned file.
     */
    private Path readMultipartMapDataToTempFile(VectorSegment seg)
            throws IOException, DataStorageManagerException {
        if (seg.mapFilePath == null || seg.mapFileSize == 0) {
            // Empty or missing map — return an empty temp file
            return Files.createTempFile(tmpDirectory, "herddb-vector-map-empty-", ".tmp");
        }
        io.github.jbellis.jvector.disk.ReaderSupplier supplier =
                dataStorageManager.multipartIndexReaderSupplier(
                        tableSpaceUUID,
                        indexUUID + "_seg" + seg.segmentId,
                        "map",
                        seg.mapFileSize);
        if (supplier == null) {
            throw new DataStorageManagerException(
                    "Storage manager does not support multipart reads for map of segment " + seg.segmentId);
        }
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-vector-map-", ".tmp");
        boolean success = false;
        try (io.github.jbellis.jvector.disk.RandomAccessReader reader = supplier.get();
             FileOutputStream fos = new FileOutputStream(tempFile.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, CHUNK_SIZE)) {
            reader.seek(0);
            // Read in chunks to avoid large allocations
            byte[] buf = new byte[CHUNK_SIZE];
            long remaining = seg.mapFileSize;
            while (remaining > 0) {
                int toRead = (int) Math.min(buf.length, remaining);
                byte[] readBuf = toRead == buf.length ? buf : new byte[toRead];
                reader.readFully(readBuf);
                bos.write(readBuf, 0, toRead);
                remaining -= toRead;
            }
            success = true;
            return tempFile;
        } finally {
            if (!success) {
                Files.deleteIfExists(tempFile);
            }
        }
    }

    /**
     * Streams a file into CHUNK_SIZE pieces and writes each as an index page.
     * <p>Every allocated pageId is recorded in {@link #provisionalPageIds} (if non-null)
     * <em>before</em> the corresponding {@code writeIndexPage} call, so that a failure
     * mid-write still leaves a rollback record for the caller to reclaim the page.
     */
    private List<Long> writeChunks(Path file, int chunkType) throws DataStorageManagerException, IOException {
        List<Long> pageIds = new ArrayList<>();
        long fileSize = Files.size(file);
        if (fileSize == 0) {
            long pageId = newPageId.getAndIncrement();
            trackProvisionalPageId(pageId);
            dataStorageManager.writeIndexPage(tableSpaceUUID, indexUUID, pageId, out -> {
                out.writeVInt(chunkType);
                out.writeVInt(0);
            });
            pageIds.add(pageId);
            return pageIds;
        }
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file.toFile()), CHUNK_SIZE)) {
            byte[] buf = new byte[CHUNK_SIZE];
            int bytesRead;
            while ((bytesRead = bis.read(buf, 0, CHUNK_SIZE)) > 0) {
                final int len = bytesRead;
                long pageId = newPageId.getAndIncrement();
                trackProvisionalPageId(pageId);
                dataStorageManager.writeIndexPage(tableSpaceUUID, indexUUID, pageId, out -> {
                    out.writeVInt(chunkType);
                    out.writeVInt(len);
                    out.write(buf, 0, len);
                });
                pageIds.add(pageId);
            }
        }
        return pageIds;
    }

    /**
     * Records a pageId in the in-flight provisional list (if one is installed).
     * Called by all page-writing helpers in Phase B so that a mid-write abort can
     * reclaim the partially-written pages.
     */
    private void trackProvisionalPageId(long pageId) {
        List<Long> tracker = this.provisionalPageIds;
        if (tracker != null) {
            synchronized (tracker) {
                tracker.add(pageId);
            }
        }
    }

    /**
     * Splits data into CHUNK_SIZE pieces and writes each as an index page.
     */
    private List<Long> writeChunks(byte[] data, int chunkType) throws DataStorageManagerException {
        List<Long> pageIds = new ArrayList<>();
        if (data.length == 0) {
            long pageId = newPageId.getAndIncrement();
            trackProvisionalPageId(pageId);
            dataStorageManager.writeIndexPage(tableSpaceUUID, indexUUID, pageId, out -> {
                out.writeVInt(chunkType);
                out.writeVInt(0);
            });
            pageIds.add(pageId);
            return pageIds;
        }
        for (int offset = 0; offset < data.length; offset += CHUNK_SIZE) {
            final int off = offset;
            final int len = Math.min(CHUNK_SIZE, data.length - offset);
            long pageId = newPageId.getAndIncrement();
            trackProvisionalPageId(pageId);
            dataStorageManager.writeIndexPage(tableSpaceUUID, indexUUID, pageId, out -> {
                out.writeVInt(chunkType);
                out.writeVInt(len);
                out.write(data, off, len);
            });
            pageIds.add(pageId);
        }
        return pageIds;
    }

    // -------------------------------------------------------------------------
    // Metadata persistence
    // -------------------------------------------------------------------------

    private void persistIndexStatusSimple(
            List<Long> graphPageIds, List<Long> mapPageIds,
            int totalNodes, boolean useFusedPQ,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        int metaSize = useFusedPQ
                ? 30 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8
                : 29 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8;

        ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
        metaBuf.putInt(useFusedPQ ? METADATA_VERSION_FUSEDPQ : METADATA_VERSION_SIMPLE);
        metaBuf.putInt(dimension);
        metaBuf.putInt(m);
        metaBuf.putInt(beamWidth);
        metaBuf.putFloat(neighborOverflow);
        metaBuf.putFloat(alpha);
        metaBuf.put((byte) (ADD_HIERARCHY ? 1 : 0));
        if (useFusedPQ) {
            metaBuf.put((byte) 1);
        }
        metaBuf.putInt(nextNodeId.get());
        metaBuf.putInt(graphPageIds.size());
        for (long id : graphPageIds) {
            metaBuf.putLong(id);
        }
        metaBuf.putInt(mapPageIds.size());
        for (long id : mapPageIds) {
            metaBuf.putLong(id);
        }

        Set<Long> activePages = new HashSet<>();
        activePages.addAll(graphPageIds);
        activePages.addAll(mapPageIds);

        IndexStatus indexStatus = new IndexStatus(
                indexName, sequenceNumber,
                newPageId.get(), activePages, metaBuf.array());

        dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, indexStatus, false);
    }

    private void persistIndexStatusMultiSegment(
            List<VectorSegment> sealedSegments, List<SegmentWriteResult> newSegmentResults,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

        int totalSegments = sealedSegments.size() + newSegmentResults.size();
        Set<Long> activePages = new HashSet<>();

        VisibleByteArrayOutputStream baos = new VisibleByteArrayOutputStream(256);
        try (java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
            dos.writeInt(METADATA_VERSION_MULTI_SEGMENT);
            dos.writeInt(dimension);
            dos.writeInt(m);
            dos.writeInt(beamWidth);
            dos.writeFloat(neighborOverflow);
            dos.writeFloat(alpha);
            dos.writeByte(ADD_HIERARCHY ? 1 : 0);
            dos.writeByte(1); // fusedPQ
            dos.writeInt(nextNodeId.get());
            dos.writeInt(totalSegments);

            for (VectorSegment seg : sealedSegments) {
                writeSegmentMeta(dos, activePages,
                        seg.segmentId, seg.estimatedSizeBytes,
                        seg.graphPageIds, seg.mapPageIds,
                        seg.graphFilePath, seg.graphFileSize,
                        seg.mapFilePath, seg.mapFileSize);
            }
            for (SegmentWriteResult swr : newSegmentResults) {
                writeSegmentMeta(dos, activePages,
                        swr.segmentId, swr.estimatedSizeBytes,
                        swr.graphPageIds, swr.mapPageIds,
                        swr.graphFilePath, swr.graphFileSize,
                        swr.mapFilePath, swr.mapFileSize);
            }
        } catch (IOException e) {
            throw new DataStorageManagerException("Failed to serialize index metadata", e);
        }

        IndexStatus indexStatus = new IndexStatus(
                indexName, sequenceNumber,
                newPageId.get(), activePages, baos.toByteArray());

        dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, indexStatus, false);
    }

    private static void writeSegmentMeta(
            java.io.DataOutputStream dos, Set<Long> activePages,
            int segmentId, long estimatedSizeBytes,
            List<Long> graphPageIds, List<Long> mapPageIds,
            String graphFilePath, long graphFileSize,
            String mapFilePath, long mapFileSize) throws IOException {
        dos.writeInt(segmentId);
        dos.writeLong(estimatedSizeBytes);
        boolean multipart = graphFilePath != null;
        dos.writeByte(multipart ? 1 : 0);
        if (multipart) {
            dos.writeUTF(graphFilePath);
            dos.writeLong(graphFileSize);
            dos.writeUTF(mapFilePath != null ? mapFilePath : "");
            dos.writeLong(mapFileSize);
        } else {
            dos.writeInt(graphPageIds.size());
            for (long id : graphPageIds) {
                dos.writeLong(id);
                activePages.add(id);
            }
            dos.writeInt(mapPageIds.size());
            for (long id : mapPageIds) {
                dos.writeLong(id);
                activePages.add(id);
            }
        }
    }

    // -------------------------------------------------------------------------
    // BLink helpers for on-disk ordinal <-> PK maps
    // -------------------------------------------------------------------------

    static Bytes ordinalToBytes(int ordinal) {
        byte[] buf = new byte[4];
        buf[0] = (byte) (ordinal >>> 24);
        buf[1] = (byte) (ordinal >>> 16);
        buf[2] = (byte) (ordinal >>> 8);
        buf[3] = (byte) ordinal;
        return Bytes.from_array(buf);
    }

    static int bytesToOrdinal(Bytes b) {
        byte[] d = b.to_array();
        return ((d[0] & 0xFF) << 24) | ((d[1] & 0xFF) << 16) | ((d[2] & 0xFF) << 8) | (d[3] & 0xFF);
    }

    private void dropSegmentBLinkStorage(VectorSegment seg) {
        String pkToNodeName = indexUUID + "_seg" + seg.segmentId + "_pktonode";
        try {
            dataStorageManager.dropIndex(tableSpaceUUID, pkToNodeName);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.WARNING, "Failed to drop BLink storage for segment " + seg.segmentId
                    + " of vector store " + indexName, e);
        }
    }

    private void createSegmentBLinks(VectorSegment seg) {
        long pageSize = memoryManager.getMaxLogicalPageSize();
        String pkToNodeName = indexUUID + "_seg" + seg.segmentId + "_pktonode";
        try {
            dataStorageManager.initIndex(tableSpaceUUID, pkToNodeName);
        } catch (DataStorageManagerException e) {
            throw new RuntimeException("Failed to init BLink storage for vector store " + indexName
                    + " segment " + seg.segmentId, e);
        }
        seg.onDiskPkToNode = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesLongStorage(pkToNodeName));
    }

    // -------------------------------------------------------------------------
    // BLink data storage implementations
    // -------------------------------------------------------------------------

    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;
    private static final byte BLINK_INNER_NODE_PAGE = 1;
    private static final byte BLINK_LEAF_NODE_PAGE = 2;

    /**
     * BLink storage for {@code BLink<Bytes, Long>} (pkToNode map).
     */
    private final class BytesLongStorage implements BLinkIndexDataStorage<Bytes, Long> {
        private final String storeName;

        BytesLongStorage(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void loadNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, BLINK_INNER_NODE_PAGE, data);
        }

        @Override
        public void loadLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, BLINK_LEAF_NODE_PAGE, data);
        }

        private void loadPage(long pageId, byte type, Map<Bytes, Long> map) throws IOException {
            dataStorageManager.readIndexPage(tableSpaceUUID, storeName, pageId, in -> {
                long version = in.readVLong();
                long flags = in.readVLong();
                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted BLink page " + pageId);
                }
                byte rtype = in.readByte();
                if (rtype != type) {
                    throw new IOException("Wrong page type " + rtype + " expected " + type);
                }
                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {
                    switch (block) {
                        case NODE_PAGE_KEY_VALUE_BLOCK:
                            map.put(in.readBytes(), in.readVLong());
                            break;
                        case NODE_PAGE_INF_BLOCK:
                            map.put(Bytes.POSITIVE_INFINITY, in.readVLong());
                            break;
                        default:
                            throw new IOException("Wrong block type " + block);
                    }
                }
                return map;
            });
        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            return writePage(NEW_PAGE, data, BLINK_INNER_NODE_PAGE);
        }

        @Override
        public long createLeafPage(Map<Bytes, Long> data) throws IOException {
            return writePage(NEW_PAGE, data, BLINK_LEAF_NODE_PAGE);
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            writePage(pageId, data, BLINK_INNER_NODE_PAGE);
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            writePage(pageId, data, BLINK_LEAF_NODE_PAGE);
        }

        private long writePage(long pageId, Map<Bytes, Long> data, byte type) throws IOException {
            if (pageId == NEW_PAGE) {
                pageId = newPageId.getAndIncrement();
            }
            dataStorageManager.writeIndexPage(tableSpaceUUID, storeName, pageId, out -> {
                out.writeVLong(1);
                out.writeVLong(0);
                out.writeByte(type);
                data.forEach((x, y) -> {
                    try {
                        if (x == Bytes.POSITIVE_INFINITY) {
                            out.writeByte(NODE_PAGE_INF_BLOCK);
                            out.writeVLong(y);
                        } else {
                            out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                            out.writeArray(x.to_array());
                            out.writeVLong(y);
                        }
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
                out.writeByte(NODE_PAGE_END_BLOCK);
            });
            return pageId;
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the effective maximum live graph size per shard.
     */
    int computeEffectiveMaxLiveGraphSize() {
        if (maxLiveGraphSize > 0) {
            return maxLiveGraphSize;
        }
        double factor = Math.sqrt((double) m * beamWidth / 1600.0);
        int computed = (int) (50_000 / Math.max(factor, 0.5));
        return Math.max(10_000, Math.min(100_000, computed));
    }

    /**
     * Computes the maximum number of live vectors allowed during checkpoint Phase B.
     *
     * <p>When a memory budget is configured ({@code effectiveBudget != Long.MAX_VALUE}),
     * the cap is derived from the remaining headroom after accounting for the frozen shards
     * that are being written during Phase B.  This prevents workers from accumulating so many
     * new live shards that combined heap (frozen + live) exceeds the JVM limit before Phase C
     * can release the frozen data.
     *
     * <p>When no budget is configured the method falls back to the static
     * {@link #MAX_LIVE_BYTES_DURING_CHECKPOINT} system-property limit.
     *
     * @param frozenVectorCount total vector count across all frozen shards (Phase A snapshot)
     * @param dim               vector dimension
     * @param m                 HNSW M parameter (max connections per node)
     * @param neighborOverflow  HNSW neighbor overflow factor
     * @param effectiveBudget   per-store or global budget in bytes; {@code Long.MAX_VALUE} if unconfigured
     * @param minShardSize      minimum floor — result of {@link #computeEffectiveMaxLiveGraphSize()}
     * @return cap to assign to {@link #liveVectorCapDuringCheckpoint}
     */
    static int computeLiveVectorCapDuringCheckpoint(
            int frozenVectorCount, int dim, int m, float neighborOverflow,
            long effectiveBudget, int minShardSize) {
        return computeLiveVectorCapDuringCheckpoint(
                frozenVectorCount, dim, m, neighborOverflow,
                effectiveBudget, minShardSize, MAX_LIVE_VECTORS_PER_CHECKPOINT);
    }

    /**
     * Variant with an explicit absolute cap {@code maxLiveVectorsPerCheckpoint}.
     * When non-zero, the returned cap is the minimum of the memory-derived
     * cap and this absolute cap. Floored at {@code minShardSize} so that a
     * checkpoint can always make at least one shard worth of progress.
     *
     * <p>Package-private for unit tests.
     */
    static int computeLiveVectorCapDuringCheckpoint(
            int frozenVectorCount, int dim, int m, float neighborOverflow,
            long effectiveBudget, int minShardSize, int maxLiveVectorsPerCheckpoint) {
        long estimatedBytesPerVector = estimatedBytesPerVector(dim, m, neighborOverflow);
        int baseCap;
        if (effectiveBudget == Long.MAX_VALUE) {
            baseCap = (int) Math.min(Integer.MAX_VALUE,
                    MAX_LIVE_BYTES_DURING_CHECKPOINT / Math.max(1L, estimatedBytesPerVector));
        } else {
            long frozenEstimated = (long) frozenVectorCount * estimatedBytesPerVector;
            long headroom = Math.max(0L, effectiveBudget - frozenEstimated);
            baseCap = (int) Math.min(Integer.MAX_VALUE, headroom / estimatedBytesPerVector);
            baseCap = Math.max(baseCap, minShardSize);
        }
        if (maxLiveVectorsPerCheckpoint > 0 && maxLiveVectorsPerCheckpoint < baseCap) {
            return Math.max(maxLiveVectorsPerCheckpoint, minShardSize);
        }
        return baseCap;
    }

    /**
     * Estimated heap bytes consumed per live vector, using the same accounting as
     * {@link #shardMemoryBytes(LiveGraphShard)}.
     */
    static long estimatedBytesPerVector(int dim, int m, float neighborOverflow) {
        // int nodeArrayLength = maxOverflowDegree + 1 = (int)(m * neighborOverflow) + 1
        int nodeArrayLen = (int) (m * neighborOverflow) + 1;
        // NodeArray.ramBytesUsed(nodeArrayLen):
        //   OH(16) + size_field(4) + ref+ah nodes(24) + ref+ah scores(24) + nodeArrayLen*(4+4)
        // Neighbors adds: nodeId(4) + diverseBefore(4)
        // Plus REF_BYTES(8) for the DenseIntMap slot
        long graphBytesPerNode = 8L + 16L + 4L + 24L + 24L + (long) nodeArrayLen * 8L + 8L;
        return (long) dim * Float.BYTES   // raw vector
                + 250L                   // pkToNode + nodeToPk + Bytes PK
                + graphBytesPerNode;     // HNSW graph overhead per node (layer 0)
    }

    private LiveGraphShard createEmptyLiveShard(int dim, int bw, float no, float a) {
        return createEmptyLiveShard(dim, bw, no, a, nextNodeId.get());
    }

    /**
     * Creates an empty live shard with an explicit {@code startNodeId}.
     * Use this when the global nodeId space has already been remapped (e.g., simple checkpoint rebuild).
     */
    private LiveGraphShard createEmptyLiveShard(int dim, int bw, float no, float a, int startNodeId) {
        ConcurrentHashMap<Bytes, Integer> p2n = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, Bytes> n2p = new ConcurrentHashMap<>();
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(vectorStorage, dim, -1, startNodeId);
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction);
        GraphIndexBuilder b = new GraphIndexBuilder(
                bsp, dim, m, bw, no, a, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
        return new LiveGraphShard(p2n, n2p, ravv, b, startNodeId);
    }

    /**
     * Rotates the live graph shard if the active shard has reached the max size.
     */
    private synchronized LiveGraphShard rotateLiveShard() {
        List<LiveGraphShard> shards = this.liveShards;
        LiveGraphShard active = shards.get(shards.size() - 1);
        if (active.nodeToPk.size() < computeEffectiveMaxLiveGraphSize()) {
            return active;
        }

        LiveGraphShard newShard = createEmptyLiveShard(dimension, beamWidth, neighborOverflow, alpha);
        List<LiveGraphShard> newList = new ArrayList<>(shards);
        newList.add(newShard);
        this.liveShards = newList;

        LOGGER.log(Level.INFO,
                "vector store {0}: rotated live graph shard, now {1} shards ({2} vectors in sealed shard)",
                new Object[]{indexName, newList.size(), active.nodeToPk.size()});

        return newShard;
    }

    private void initEmptyLiveShards(int dim, int bw, float no, float a) {
        LiveGraphShard shard = createEmptyLiveShard(dim, bw, no, a);
        this.liveShards = new ArrayList<>(Collections.singletonList(shard));
    }

    private synchronized void initBuilderForDimension(int dim) {
        if (this.dimension == 0) {
            LiveGraphShard shard = createEmptyLiveShard(dim, beamWidth, neighborOverflow, alpha);
            this.liveShards = new ArrayList<>(Collections.singletonList(shard));
            this.dimension = dim;
        }
    }

    /** Returns the total number of live vectors across all shards. */
    private int totalLiveSize() {
        int total = 0;
        for (LiveGraphShard shard : liveShards) {
            total += shard.nodeToPk.size();
        }
        return total;
    }

    private long onDiskNodeToPkSize() {
        long total = 0;
        for (VectorSegment seg : segments) {
            total += seg.size();
        }
        return total;
    }

    private static void waitForCheckpointToComplete(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException("interrupted waiting for checkpoint", e);
        }
    }

    private static List<Long> toLongList(long[] arr) {
        List<Long> list = new ArrayList<>(arr.length);
        for (long v : arr) {
            list.add(v);
        }
        return list;
    }

    private static void skipFully(DataInputStream dis, long n) throws IOException {
        while (n > 0) {
            int skipped = dis.skipBytes((int) Math.min(n, Integer.MAX_VALUE));
            if (skipped <= 0) {
                dis.readByte();
                n--;
            } else {
                n -= skipped;
            }
        }
    }

    private static void writeInt(OutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    /**
     * Serialises the pk/vector map into a temp file.
     */
    private Path serializeMapDataToFile(VectorStorage storage,
                                        ConcurrentHashMap<Integer, Bytes> nodeToKey) throws IOException {
        Path tmpFile = Files.createTempFile(tmpDirectory, "herddb-vector-map-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(tmpFile.toFile()), CHUNK_SIZE)) {
            List<Map.Entry<Integer, Bytes>> entries = new ArrayList<>(nodeToKey.entrySet());
            int entryCount = entries.size();
            writeInt(bos, entryCount);

            for (Map.Entry<Integer, Bytes> e : entries) {
                int nodeId = e.getKey();
                byte[] pkBytes = e.getValue().to_array();
                VectorFloat<?> vec = storage.get(nodeId);
                if (vec == null) {
                    continue;
                }
                int floatCount = vec.length();
                writeInt(bos, nodeId);
                writeInt(bos, pkBytes.length);
                bos.write(pkBytes);
                writeInt(bos, floatCount);
                for (int j = 0; j < floatCount; j++) {
                    int bits = Float.floatToIntBits(vec.get(j));
                    writeInt(bos, bits);
                }
            }
        }
        return tmpFile;
    }

    private void resetState() {
        for (LiveGraphShard shard : liveShards) {
            if (shard.builder != null) {
                try {
                    shard.builder.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        List<LiveGraphShard> frozen = this.frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                if (shard.builder != null) {
                    try {
                        shard.builder.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
        frozenShards = null;
        pendingCheckpointDeletes = null;
        liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
        CountDownLatch latch = this.checkpointPhaseComplete;
        if (latch != null) {
            latch.countDown();
            this.checkpointPhaseComplete = null;
        }
        for (VectorSegment seg : segments) {
            seg.close();
        }
        segments = new java.util.concurrent.CopyOnWriteArrayList<>();
        liveShards = new ArrayList<>();
        nextNodeId.set(0);
        nextSegmentId.set(0);
        dimension = 0;
        vectorStorage = new VectorStorage(computeEffectiveMaxLiveGraphSize());
    }

    // -------------------------------------------------------------------------
    // Accessors (for tests and monitoring)
    // -------------------------------------------------------------------------

    public String getIndexName() {
        return indexName;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public String getVectorColumnName() {
        return vectorColumnName;
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isFusedPQEnabled() {
        return fusedPQ;
    }

    public int getLiveNodeCount() {
        int frozenCount = 0;
        List<LiveGraphShard> frozen = frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                frozenCount += shard.nodeToPk.size();
            }
        }
        return totalLiveSize() + frozenCount;
    }

    public int getOnDiskNodeCount() {
        return (int) onDiskNodeToPkSize();
    }

    /**
     * Visits every primary key currently stored in this vector store.
     * Walks live shards (and frozen shards, if a checkpoint is running)
     * first, then, if {@code includeOnDisk} is true, walks on-disk segments
     * via {@link VectorSegment#scanNodeToPk()}.
     *
     * <p>The visitor returns {@code false} to stop the traversal early.
     *
     * <p>PKs that only exist in sealed on-disk segments may collide with
     * live PKs when the live graph still holds a newer copy of the same
     * record; callers that need deduplication must track seen PKs
     * themselves.
     */
    @Override
    public void forEachPrimaryKey(boolean includeOnDisk, Predicate<Bytes> visitor) {
        for (LiveGraphShard shard : liveShards) {
            for (Bytes pk : shard.nodeToPk.values()) {
                if (!visitor.test(pk)) {
                    return;
                }
            }
        }
        List<LiveGraphShard> frozen = this.frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                for (Bytes pk : shard.nodeToPk.values()) {
                    if (!visitor.test(pk)) {
                        return;
                    }
                }
            }
        }
        if (!includeOnDisk) {
            return;
        }
        for (VectorSegment seg : segments) {
            try (Stream<Map.Entry<Bytes, Bytes>> stream = seg.scanNodeToPk()) {
                java.util.Iterator<Map.Entry<Bytes, Bytes>> it = stream.iterator();
                while (it.hasNext()) {
                    Bytes pk = it.next().getValue();
                    if (!visitor.test(pk)) {
                        return;
                    }
                }
            }
        }
    }

    public int getSegmentCount() {
        return segments.size();
    }

    public long getMaxSegmentSize() {
        return maxSegmentSize;
    }

    public long getEstimatedSizeBytes() {
        long total = 0;
        for (VectorSegment seg : segments) {
            total += seg.estimatedSizeBytes;
        }
        return total;
    }

    public void setSegmentSizeStats(OpStatsLogger segmentSizeStats) {
        this.segmentSizeStats = segmentSizeStats;
    }

    private void recordSegmentSizeDistribution() {
        OpStatsLogger stats = this.segmentSizeStats;
        if (stats == null) {
            return;
        }
        for (VectorSegment seg : segments) {
            stats.registerSuccessfulValue(seg.estimatedSizeBytes);
        }
    }

    public boolean isDirty() {
        return dirty.get();
    }

    public boolean isCheckpointActive() {
        return frozenShards != null;
    }

    public int getLiveShardCount() {
        return liveShards.size();
    }

    public int getMaxLiveGraphSize() {
        return maxLiveGraphSize;
    }

    public int getEffectiveMaxLiveGraphSize() {
        return computeEffectiveMaxLiveGraphSize();
    }

    public int getM() {
        return m;
    }

    public int getBeamWidth() {
        return beamWidth;
    }

    public float getNeighborOverflow() {
        return neighborOverflow;
    }

    public float getAlpha() {
        return alpha;
    }

    public String getSimilarityFunction() {
        return similarityFunction.name();
    }

    public long getLastCheckpointDurationMs() {
        return lastCheckpointDurationMs.get();
    }

    public long getLastCheckpointPhaseBDurationMs() {
        return lastCheckpointPhaseBDurationMs.get();
    }

    public long getTotalCheckpointCount() {
        return totalCheckpointCount.get();
    }

    public long getTotalFusedPQCheckpointCount() {
        return totalFusedPQCheckpointCount.get();
    }

    public long getTotalSimpleCheckpointCount() {
        return totalSimpleCheckpointCount.get();
    }

    public long getLastCheckpointVectorsProcessed() {
        return lastCheckpointVectorsProcessed.get();
    }

    public long getLiveVectorsMemoryBytes() {
        return estimatedMemoryUsageBytes();
    }

    public long getTotalBackpressureCount() {
        return totalBackpressureCount.get();
    }

    public long getTotalBackpressureTimeMs() {
        return totalBackpressureTimeMs.get();
    }

    public boolean isBackpressureActive() {
        return backpressureActive != 0;
    }

    public long getMaxVectorMemoryBytes() {
        return maxVectorMemoryBytes;
    }

    public int getFrozenShardCount() {
        List<LiveGraphShard> frozen = frozenShards;
        return frozen != null ? frozen.size() : 0;
    }

    public int getLiveVectorCapDuringCheckpoint() {
        return liveVectorCapDuringCheckpoint;
    }

    // -------------------------------------------------------------------------
    // P3.7 metrics — segments, Phase B throughput, disk usage
    // -------------------------------------------------------------------------

    /** Current count of sealed + mergeable segments. */
    public int getSealedSegmentCount() {
        return segments.size();
    }

    /**
     * Vectors-per-second throughput achieved by the last completed Phase B
     * segment-build pass. 0 if Phase B has not run yet.
     */
    public double getLastPhaseBVectorsPerSecond() {
        long durMs = lastCheckpointPhaseBDurationMs.get();
        long vectors = lastCheckpointVectorsProcessed.get();
        if (durMs <= 0 || vectors <= 0) {
            return 0d;
        }
        return vectors * 1000.0 / durMs;
    }

    /**
     * Approximate bytes written by the last Phase B (graph + map segments).
     * 0 if no Phase B has completed yet.
     */
    public long getLastPhaseBBytesWritten() {
        return lastPhaseBBytesWritten.get();
    }

    /** Number of pages discarded by the most recent failure recovery. */
    public long getLastRolledBackPages() {
        return lastRolledBackPages.get();
    }

    /**
     * Free bytes reported by the tmp directory's filesystem. Returns
     * {@code -1} if the path is not available.
     */
    public long getFreeDiskBytes() {
        try {
            if (tmpDirectory == null) {
                return -1L;
            }
            java.io.File f = tmpDirectory.toFile();
            return f.getUsableSpace();
        } catch (SecurityException ignored) {
            return -1L;
        }
    }

    /**
     * Total bytes occupied by files whose name starts with
     * {@code herddb-vector-} in {@link #tmpDirectory}. Intended as an
     * observability metric for the P1.4 goal: the number should stay near
     * zero at rest (only transient map tmp files during checkpoint).
     */
    public long getTmpDirBytes() {
        try {
            if (tmpDirectory == null || !java.nio.file.Files.isDirectory(tmpDirectory)) {
                return 0L;
            }
            long[] acc = {0L};
            try (java.util.stream.Stream<java.nio.file.Path> s =
                    java.nio.file.Files.list(tmpDirectory)) {
                s.filter(p -> p.getFileName().toString().startsWith("herddb-vector-"))
                        .forEach(p -> {
                            try {
                                acc[0] += java.nio.file.Files.size(p);
                            } catch (java.io.IOException ignored) {
                                // skip
                            }
                        });
            }
            return acc[0];
        } catch (java.io.IOException ignored) {
            return 0L;
        }
    }
}
