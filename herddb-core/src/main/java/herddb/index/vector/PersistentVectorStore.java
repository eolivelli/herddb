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
import herddb.utils.VectorSearchRequestContext;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
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
     * Minimum number of live vectors that must have accumulated in the live
     * shard before a non-bootstrap Phase A is allowed to run. Below this
     * threshold, {@code doCheckpointUnderLock} defers the cycle (subject to
     * {@link #maxCheckpointDeferralMs}) so the tailer has time to drain a
     * larger batch of entries. Memory-pressure checkpoints and segment-merge
     * checkpoints bypass the gate; the very first checkpoint on an empty
     * index also bypasses it so the index never sits entirely in memory.
     *
     * <p><b>Default 50 000.</b> Tuned for the 1M-vector gist1m catch-up
     * workload in issue #90: during catch-up the tailer drains past 50 000
     * vectors within seconds, so the gate adds zero latency on the hot
     * path. Small-workload unit tests that rely on multiple back-to-back
     * checkpoints override this to 0 in {@code @Before}. Set to 0 globally
     * to restore pre-fix behaviour.
     *
     * <p>Initialized from system property
     * {@code herddb.vectorindex.minLiveVectorsForCheckpoint}. Non-final to
     * let unit tests override after class load; production code should only
     * read, never write.
     */
    public static volatile int minLiveVectorsForCheckpoint =
            Math.max(0, Integer.getInteger(
                    "herddb.vectorindex.minLiveVectorsForCheckpoint", 50_000));

    /**
     * Maximum time the {@link #minLiveVectorsForCheckpoint} gate may defer
     * a pending checkpoint. Once {@code now - lastSuccessfulCheckpointMs}
     * exceeds this bound, the gate unconditionally releases and Phase A runs
     * even with a partial live shard. Guarantees bounded flush latency when
     * ingest has stopped mid-shard (issue #90).
     *
     * <p>Initialized from system property
     * {@code herddb.vectorindex.maxCheckpointDeferralMs}. Default: 60 s.
     * Non-final to let unit tests override after class load; production
     * code should only read, never write.
     */
    public static volatile long maxCheckpointDeferralMs =
            Math.max(0L, Long.getLong(
                    "herddb.vectorindex.maxCheckpointDeferralMs", 60_000L));

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

    /**
     * How many segments may be written between PQ codebook re-trainings for the
     * same index. After the first training, the codebook is reused for up to
     * {@code pqCodebookRetrainingInterval - 1} further segment writes. Set to
     * {@code 0} to disable caching and always retrain (original behaviour).
     *
     * <p>Default: 100. Suitable for stationary-distribution workloads (bigann,
     * sift, gist). Reduce for highly non-stationary datasets. Configurable via
     * system property {@code herddb.vectorindex.pqCodebookRetrainingInterval}.
     */
    public static volatile int pqCodebookRetrainingInterval =
            Math.max(0, Integer.getInteger(
                    "herddb.vectorindex.pqCodebookRetrainingInterval", 100));

    /**
     * Parallelism for the JVM-wide checkpoint pool. Configurable via the
     * {@code herddb.vectorindex.checkpointThreads} system property. Default
     * is {@code max(1, availableProcessors() / 2)}.
     */
    private static final int CHECKPOINT_POOL_SIZE = Math.max(1,
            Integer.getInteger("herddb.vectorindex.checkpointThreads",
                    Math.max(1, Runtime.getRuntime().availableProcessors() / 2)));

    /** Dedicated ForkJoinPool for checkpoint graph building. */
    private static final ForkJoinPool CHECKPOINT_POOL = createCheckpointPool();

    private static ForkJoinPool createCheckpointPool() {
        int defaultSize = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        LOGGER.log(Level.INFO,
                "PersistentVectorStore checkpoint pool: parallelism={0} "
                        + "(system property herddb.vectorindex.checkpointThreads, "
                        + "default max(1, availableProcessors()/2)={1})",
                new Object[]{CHECKPOINT_POOL_SIZE, defaultSize});
        return new ForkJoinPool(
                CHECKPOINT_POOL_SIZE,
                pool -> {
                    ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    t.setDaemon(true);
                    t.setName("persistent-vector-store-checkpoint-" + t.getPoolIndex());
                    return t;
                },
                null, false);
    }

    /** Buffer size for in-memory / on-disk staging of index artefacts (1 MB). */
    static final int CHUNK_SIZE = 1_048_576;

    /**
     * Multi-segment multipart metadata format. Carries per-segment
     * {@code generation}, a per-IndexStatus monotonic generation
     * counter, and a {@code pendingDeletes} list driving the
     * graph-merge compaction retention protocol.
     */
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
    private final long maxLiveBytesPerCheckpoint;

    /** Optional stats logger for recording per-segment size distribution. */
    private volatile OpStatsLogger segmentSizeStats;

    // -------------------------------------------------------------------------
    // In-memory state -- LIVE inserts (new since last checkpoint)
    // -------------------------------------------------------------------------

    /** All live graph shards. The LAST element is the active (unsealed) shard. */
    private volatile List<LiveGraphShard> liveShards = new ArrayList<>();

    /**
     * Monotonically increasing global node-id counter. Widened to {@code long}
     * so it cannot silently wrap after {@code 2^31} ids are burned by ingest +
     * compaction reservations (issue #256). Each {@link LiveGraphShard} owns
     * its own per-shard {@link VectorStorage} keyed by a local ordinal, so
     * the global id here never reaches a {@code VectorStorage} backing array.
     */
    private final AtomicLong nextNodeId = new AtomicLong(0);

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

    /**
     * Live shards deferred from Phase A because the byte cap was reached;
     * prepended to {@link #liveShards} at Phase C or restored on Phase B failure.
     */
    private volatile List<LiveGraphShard> deferredShards;

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

    /**
     * Monotonically increasing IndexStatus generation. Each successful
     * call to {@link #persistIndexStatusMultiSegment} bumps this counter
     * and stamps every newly-produced segment with the new value.
     * Loaded from the latest persisted metadata (max segment generation)
     * at startup so generations remain monotonic across restarts.
     */
    private final AtomicLong currentIndexStatusGeneration = new AtomicLong(0);

    /**
     * Files queued for physical deletion by the compaction retention
     * protocol. Persisted in the IndexStatus so the decision survives
     * restarts. The reaper removes entries once both the wall-clock
     * deadline has passed AND all known shadow replicas have acked a
     * generation &gt; {@code sinceGeneration}.
     */
    private final java.util.concurrent.CopyOnWriteArrayList<PendingDelete> pendingDeletes =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * PKs deleted while a compaction is in flight. Installed by the
     * compaction loop before it starts reading input segments; every
     * {@link #removeVector} call appends to this set so the swap step
     * can replay the deletes against the freshly-built merged output
     * before it becomes visible.
     */
    private volatile Set<Bytes> pendingCompactionDeletes;

    // -------------------------------------------------------------------------
    // Compaction state
    // -------------------------------------------------------------------------

    /** Serialises compaction cycles. One run at a time. */
    private final ReentrantLock compactionLock = new ReentrantLock();

    /** Background thread driving {@link #runCompactionCycle(long)}. */
    private volatile Thread vectorIndexCompactionThread;
    private final Object vectorIndexCompactionWakeup = new Object();
    private volatile boolean vectorIndexCompactionWakeupPending;

    /** Compaction policy knobs — defaults match IndexingServerConfiguration. */
    private volatile long vectorIndexCompactionIntervalMs = 5L * 60_000L;
    private volatile long vectorIndexCompactionMinBytes = 256L * 1024 * 1024;
    private volatile long vectorIndexCompactionMaxBytes = 1024L * 1024 * 1024;
    private volatile int vectorIndexCompactionMinCount = 4;
    private volatile long vectorIndexCompactionRetentionMs = 10L * 60_000L;

    // Compaction metrics
    final AtomicLong compactionRunsTotal = new AtomicLong();
    final AtomicLong compactionSuccessesTotal = new AtomicLong();
    final AtomicLong compactionFailuresReadIoTotal = new AtomicLong();
    final AtomicLong compactionFailuresWriteIoTotal = new AtomicLong();
    final AtomicLong compactionFailuresMetadataIoTotal = new AtomicLong();
    final AtomicLong compactionFailuresCorruptionTotal = new AtomicLong();
    final AtomicLong compactionFailuresDiskFullTotal = new AtomicLong();
    final AtomicLong compactionFailuresAbortedInputGoneTotal = new AtomicLong();
    final AtomicLong compactionLastDurationMs = new AtomicLong();
    final AtomicLong compactionLastBytesRead = new AtomicLong();
    final AtomicLong compactionLastBytesWritten = new AtomicLong();
    final AtomicLong compactionLastInputSegments = new AtomicLong();
    final AtomicLong compactionLastOutputSegments = new AtomicLong();
    final AtomicLong compactionLivePkFilteredTotal = new AtomicLong();
    final AtomicInteger compactionActive = new AtomicInteger();
    final AtomicLong compactionConsecutiveFailures = new AtomicLong();
    final AtomicLong pendingDeletesReapedTotal = new AtomicLong();
    final AtomicLong pendingDeletesReapFailuresTotal = new AtomicLong();

    // -------------------------------------------------------------------------
    // PQ codebook cache (issue #281)
    // -------------------------------------------------------------------------

    /**
     * Most recently trained PQ codebook for this index. {@code null} until the
     * first FusedPQ-eligible segment is written. Protected by {@code volatile};
     * a benign race at the retraining boundary may produce at most one extra
     * training, which does not affect correctness.
     */
    private volatile ProductQuantization cachedPQ;

    /**
     * Number of FusedPQ segments written since the last PQ codebook training.
     * Compared against {@link #pqCodebookRetrainingInterval} in
     * {@link #getOrTrainPQ}.
     */
    private final AtomicInteger pqSegmentsSinceTraining = new AtomicInteger(0);

    /**
     * Total number of PQ codebook trainings performed for this index.
     * Package-private to allow direct access from same-package tests.
     */
    final AtomicInteger pqTrainingsTotal = new AtomicInteger(0);

    /** Protects state swaps during checkpoint. */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /** Prevents concurrent three-phase checkpoints from interleaving and losing data. */
    private final ReentrantLock checkpointLock = new ReentrantLock();

    // -------------------------------------------------------------------------
    // Parallel search (issue #245)
    // -------------------------------------------------------------------------

    /**
     * Degree of intra-query parallelism for {@link #searchInternal}. When
     * {@code >= 2}, each on-disk segment and each in-memory shard is searched
     * on a worker thread from {@link #searchExecutor}; results are merged on
     * the calling thread. When {@code <= 1}, the entire search runs on the
     * caller (preserves the original serial behaviour).
     */
    private final int searchParallelism;

    /**
     * Pool used to fan out segment/shard searches. {@code null} iff
     * {@link #searchParallelism} {@code <= 1}.
     */
    private final ExecutorService searchExecutor;

    /** Monotonically increasing counter for naming search worker threads. */
    private static final AtomicInteger SEARCH_WORKER_SEQ = new AtomicInteger();

    // -------------------------------------------------------------------------
    // Background compaction thread
    // -------------------------------------------------------------------------

    private volatile Thread compactionThread;
    private volatile boolean running;

    /**
     * When true, this store is operating as a read-only "shadow" view over the
     * remote storage: it loads state on start() (same as a primary) but never
     * starts the compaction thread, rejects {@link #addVector}/{@link #removeVector}
     * and treats {@link #checkpoint()} as a no-op. The shadow reloads its
     * segment list via {@link #reloadFromStatus(IndexStatus)}.
     *
     * <p>Set via {@link #setReadOnly(boolean)} before {@link #start()}.
     */
    private volatile boolean readOnly = false;

    /**
     * LogSequenceNumber of the {@link IndexStatus} this store most recently
     * loaded — either at start() or from a shadow's reloadFromStatus call.
     * {@code null} until the first successful load.
     */
    private volatile LogSequenceNumber loadedLsn;

    // -------------------------------------------------------------------------
    // Checkpoint statistics (observable by external metrics)
    // -------------------------------------------------------------------------

    private final AtomicLong lastCheckpointDurationMs = new AtomicLong(0);
    private final AtomicLong lastCheckpointPhaseBDurationMs = new AtomicLong(0);
    private final AtomicLong totalCheckpointCount = new AtomicLong(0);
    private final AtomicLong totalFusedPQCheckpointCount = new AtomicLong(0);
    /**
     * Number of times {@link #doCheckpointUnderLock} skipped a cycle because
     * the min-live-vectors gate tripped and the max-deferral bound had not
     * yet elapsed. Incremented only on the deferral path — not on the
     * "nothing dirty" early return, which represents a durable state.
     */
    private final AtomicLong totalCheckpointsDeferred = new AtomicLong(0);
    private final AtomicLong lastCheckpointVectorsProcessed = new AtomicLong(0);
    /** Approximate bytes written by the last completed Phase B. */
    private final AtomicLong lastPhaseBBytesWritten = new AtomicLong(0);
    /** Pages reclaimed by the most recent failure recovery. */
    private final AtomicLong lastRolledBackPages = new AtomicLong(0);
    /**
     * Wall-clock time of the most recent checkpoint that left the store in a
     * fully durable state (successful Phase C, or an early return that
     * reflects already-durable state). Used by the min-live-vectors gate to
     * bound how long a partial live shard may be deferred.
     */
    private volatile long lastSuccessfulCheckpointMs = System.currentTimeMillis();

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

    /**
     * Multipart files written during Phase B whose containing checkpoint has
     * not yet become durable. Each entry is {@code {tableSpace, uuid, fileType}}
     * and the mirror of {@link #provisionalPageIds} for the multipart API.
     * If Phase B or Phase C-prep aborts, {@link #rollbackProvisionalArtefacts()}
     * calls {@link DataStorageManager#deleteMultipartIndexFile} for each entry
     * so partially-written artefacts do not linger on disk.
     */
    private volatile List<String[]> provisionalMultipartFiles;

    /** Metric: provisional pages rolled back by the last failure recovery. */
    private final AtomicLong totalRolledBackPages = new AtomicLong(0);
    /** Metric: how many Phase B attempts have failed since the last success. */
    private final AtomicLong consecutiveCheckpointFailures = new AtomicLong(0);
    /** Metric: total checkpoint failures over the lifetime of the store. */
    private final AtomicLong totalCheckpointFailures = new AtomicLong(0);

    // Compaction progress counters (issue #80).
    //
    // The supervisor loop (indexing-admin + Grafana) needs to see Phase-B
    // progress without grepping logs. Counters are reset at the start of
    // each Phase B and left populated after it finishes so a post-hoc
    // describe-index still shows the final totals of the last compaction.
    private final AtomicInteger writingGraphActive = new AtomicInteger();
    private final AtomicInteger uploadingActive = new AtomicInteger();
    private final AtomicLong compactionNodesDone = new AtomicLong();
    private final AtomicLong compactionNodesTotal = new AtomicLong();
    private final AtomicLong uploadBytesDone = new AtomicLong();
    private final AtomicLong uploadBytesTotal = new AtomicLong();

    // Deferred shard metrics (issue #107).
    //
    // Track shard deferral due to Phase A byte-cap logic, for visibility into
    // when the checkpoint memory budget is constraining shards across cycles.
    private final AtomicInteger deferralEvents = new AtomicInteger();
    private final AtomicLong currentDeferredVectors = new AtomicLong();
    private final AtomicLong totalDeferredVectors = new AtomicLong();

    /** Current IndexStatus generation; 0 if no checkpoint has ever been persisted. */
    public long getCurrentIndexStatusGeneration() {
        return currentIndexStatusGeneration.get();
    }

    // Metric accessors (used by IndexingServiceEngine's per-store
    // Prometheus gauge registration).
    public long getCompactionRunsTotal() {
        return compactionRunsTotal.get();
    }

    public long getCompactionSuccessesTotal() {
        return compactionSuccessesTotal.get();
    }

    public long getCompactionFailuresReadIoTotal() {
        return compactionFailuresReadIoTotal.get();
    }

    public long getCompactionFailuresWriteIoTotal() {
        return compactionFailuresWriteIoTotal.get();
    }

    public long getCompactionFailuresMetadataIoTotal() {
        return compactionFailuresMetadataIoTotal.get();
    }

    public long getCompactionFailuresCorruptionTotal() {
        return compactionFailuresCorruptionTotal.get();
    }

    public long getCompactionFailuresDiskFullTotal() {
        return compactionFailuresDiskFullTotal.get();
    }

    public long getCompactionFailuresAbortedInputGoneTotal() {
        return compactionFailuresAbortedInputGoneTotal.get();
    }

    public long getCompactionLivePkFilteredTotal() {
        return compactionLivePkFilteredTotal.get();
    }

    public long getCompactionLastDurationMs() {
        return compactionLastDurationMs.get();
    }

    public long getCompactionLastBytesRead() {
        return compactionLastBytesRead.get();
    }

    public long getCompactionLastBytesWritten() {
        return compactionLastBytesWritten.get();
    }

    public long getCompactionLastInputSegments() {
        return compactionLastInputSegments.get();
    }

    public long getCompactionLastOutputSegments() {
        return compactionLastOutputSegments.get();
    }

    public long getCompactionConsecutiveFailures() {
        return compactionConsecutiveFailures.get();
    }

    public int getCompactionActive() {
        return compactionActive.get();
    }

    public long getPendingDeletesReapedTotal() {
        return pendingDeletesReapedTotal.get();
    }

    public long getPendingDeletesReapFailuresTotal() {
        return pendingDeletesReapFailuresTotal.get();
    }

    /** Total PQ codebook trainings for this index (see {@link #pqCodebookRetrainingInterval}). */
    public int getPqTrainingsTotal() {
        return pqTrainingsTotal.get();
    }

    /** Snapshot of the pendingDeletes list (defensive copy). */
    public List<PendingDelete> getPendingDeletesSnapshot() {
        return new ArrayList<>(pendingDeletes);
    }

    /**
     * Encodes the identity of a multipart segment file into the opaque
     * {@code filePath} stored in a {@link PendingDelete}. The tableSpace
     * is always this store's {@code tableSpaceUUID}, so we only need to
     * encode the segment uuid and file type.
     */
    static String encodeMultipartPath(String segUuid, String fileType) {
        return segUuid + ":" + fileType;
    }

    /**
     * Queues the two multipart files backing a segment (graph + map)
     * for retention-aware deletion. Called by the compaction swap step
     * for every input segment that the merged output replaces.
     */
    void queueSegmentPendingDelete(VectorSegment seg, long retentionMs) {
        long deadlineMs = System.currentTimeMillis() + retentionMs;
        long sinceGen = currentIndexStatusGeneration.get();
        String segUuid = indexUUID + "_seg" + seg.segmentId;
        pendingDeletes.add(new PendingDelete(
                encodeMultipartPath(segUuid, "graph"), deadlineMs, sinceGen));
        if (seg.mapFilePath != null) {
            pendingDeletes.add(new PendingDelete(
                    encodeMultipartPath(segUuid, "map"), deadlineMs, sinceGen));
        }
    }

    /**
     * Runs one pass of the retention reaper. Moves every
     * {@link PendingDelete} whose deadline has passed AND whose
     * {@code sinceGeneration} is &le; the supplied shadow-acked
     * generation to the physical-delete stage, then drops it from the
     * in-memory {@code pendingDeletes} list.
     *
     * <p>Callers that have no shadow replicas should pass
     * {@link Long#MAX_VALUE} so the shadow gate never blocks reclaim —
     * retention then depends solely on the wall-clock deadline.
     *
     * @return number of files successfully deleted in this pass.
     */
    public int reapExpiredPendingDeletes(long minShadowAckedGeneration) {
        long nowMs = System.currentTimeMillis();
        VectorIndexCompactor.Partition partition = VectorIndexCompactor.partitionReapable(
                pendingDeletes, nowMs, minShadowAckedGeneration);
        if (partition.reapable.isEmpty()) {
            return 0;
        }
        int deleted = 0;
        for (PendingDelete pd : partition.reapable) {
            int sep = pd.filePath.lastIndexOf(':');
            if (sep <= 0 || sep >= pd.filePath.length() - 1) {
                LOGGER.log(Level.WARNING,
                        "reaper {0}: malformed pendingDelete entry {1}, dropping",
                        new Object[]{indexName, pd.filePath});
                continue;
            }
            String segUuid = pd.filePath.substring(0, sep);
            String fileType = pd.filePath.substring(sep + 1);
            try {
                dataStorageManager.deleteMultipartIndexFile(tableSpaceUUID, segUuid, fileType);
                deleted++;
                pendingDeletesReapedTotal.incrementAndGet();
            } catch (DataStorageManagerException e) {
                LOGGER.log(Level.WARNING,
                        "reaper " + indexName + ": failed to delete " + segUuid + "/" + fileType,
                        e);
                pendingDeletesReapFailuresTotal.incrementAndGet();
                // Leave the entry in pendingDeletes so a later reap cycle retries.
                partition.retained.add(pd);
            }
        }
        // Swap the pendingDeletes list atomically: only retained entries survive.
        pendingDeletes.clear();
        pendingDeletes.addAll(partition.retained);
        return deleted;
    }

    public long getTotalRolledBackPages() {
        return totalRolledBackPages.get();
    }

    public long getConsecutiveCheckpointFailures() {
        return consecutiveCheckpointFailures.get();
    }

    public long getTotalCheckpointFailures() {
        return totalCheckpointFailures.get();
    }

    /**
     * Returns the current compaction phase: {@code "idle"},
     * {@code "writing-graph"}, or {@code "uploading-segment"}. Priority is
     * upload &gt; graph-write, so when segment writes overlap (Phase B
     * parallelism) the most advanced phase wins.
     */
    public String getCompactionPhase() {
        if (uploadingActive.get() > 0) {
            return "uploading-segment";
        }
        if (writingGraphActive.get() > 0) {
            return "writing-graph";
        }
        return "idle";
    }

    public int getWritingGraphActiveCount() {
        return writingGraphActive.get();
    }

    public int getUploadingActiveCount() {
        return uploadingActive.get();
    }

    public long getCompactionNodesDone() {
        return compactionNodesDone.get();
    }

    public long getCompactionNodesTotal() {
        return compactionNodesTotal.get();
    }

    public long getUploadBytesDone() {
        return uploadBytesDone.get();
    }

    public long getUploadBytesTotal() {
        return uploadBytesTotal.get();
    }

    /**
     * Returns the progress percentage of whichever phase is currently
     * active, or {@code -1} when idle. During {@code uploading-segment} it
     * reflects bytes-based progress; during {@code writing-graph} it
     * reflects node-count progress.
     */
    public int getCompactionProgressPercent() {
        if (uploadingActive.get() > 0) {
            long total = uploadBytesTotal.get();
            long done = uploadBytesDone.get();
            return total > 0 ? (int) Math.min(100L, (100L * done) / total) : 0;
        }
        if (writingGraphActive.get() > 0) {
            long total = compactionNodesTotal.get();
            long done = compactionNodesDone.get();
            return total > 0 ? (int) Math.min(100L, (100L * done) / total) : 0;
        }
        return -1;
    }

    public int getDeferralEvents() {
        return deferralEvents.get();
    }

    public long getCurrentDeferredVectors() {
        return currentDeferredVectors.get();
    }

    public long getTotalDeferredVectors() {
        return totalDeferredVectors.get();
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
        this(indexName, tableName, tableSpaceUUID, vectorColumnName,
                indexName + "_" + tableName + "_" + System.nanoTime(), tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE, null, 0, 0);
    }

    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName,
                indexName + "_" + tableName + "_" + System.nanoTime(), tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, Long.MAX_VALUE, null, 0, 0);
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
        this(indexName, tableName, tableSpaceUUID, vectorColumnName,
                indexName + "_" + tableName + "_" + System.nanoTime(), tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, maxVectorMemoryBytes, null, 0, 0);
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
                                 VectorMemoryBudget memoryBudget,
                                 long maxLiveBytesPerCheckpoint) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName,
                indexName + "_" + tableName + "_" + System.nanoTime(),
                tmpDirectory, dataStorageManager, memoryManager, m, beamWidth,
                neighborOverflow, alpha, fusedPQ, maxSegmentSize, maxLiveGraphSize,
                compactionIntervalMs, similarityFunction, maxVectorMemoryBytes,
                memoryBudget, maxLiveBytesPerCheckpoint, 0);
    }

    /**
     * Constructor that accepts all parameters including segment page cache max bytes,
     * for use by the IndexingServiceEngine factory.
     */
    public PersistentVectorStore(String indexName, String tableName, String tableSpaceUUID,
                                 String vectorColumnName, Path tmpDirectory,
                                 DataStorageManager dataStorageManager,
                                 MemoryManager memoryManager,
                                 int m, int beamWidth, float neighborOverflow, float alpha,
                                 boolean fusedPQ, long maxSegmentSize, int maxLiveGraphSize,
                                 long compactionIntervalMs,
                                 VectorSimilarityFunction similarityFunction,
                                 long maxVectorMemoryBytes,
                                 VectorMemoryBudget memoryBudget,
                                 long maxLiveBytesPerCheckpoint,
                                 long segmentPageCacheMaxBytes) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName,
                indexName + "_" + tableName + "_" + System.nanoTime(),
                tmpDirectory, dataStorageManager, memoryManager, m, beamWidth,
                neighborOverflow, alpha, fusedPQ, maxSegmentSize, maxLiveGraphSize,
                compactionIntervalMs, similarityFunction, maxVectorMemoryBytes,
                memoryBudget, maxLiveBytesPerCheckpoint, segmentPageCacheMaxBytes);
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
                VectorSimilarityFunction.COSINE, Long.MAX_VALUE, null, 0, 0);
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
                similarityFunction, Long.MAX_VALUE, null, 0, 0);
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
                similarityFunction, maxVectorMemoryBytes, null, 0, 0);
    }

    /**
     * Constructor that accepts an explicit indexUUID, similarity function, memory limit,
     * global memory budget, live-shard snapshot byte cap, and segment page cache max bytes.
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
                                 VectorMemoryBudget memoryBudget,
                                 long maxLiveBytesPerCheckpoint,
                                 long segmentPageCacheMaxBytes) {
        this(indexName, tableName, tableSpaceUUID, vectorColumnName, indexUUID, tmpDirectory,
                dataStorageManager, memoryManager, m, beamWidth, neighborOverflow, alpha,
                fusedPQ, maxSegmentSize, maxLiveGraphSize, compactionIntervalMs,
                similarityFunction, maxVectorMemoryBytes, memoryBudget,
                maxLiveBytesPerCheckpoint, segmentPageCacheMaxBytes, 1);
    }

    /**
     * Constructor that additionally accepts a degree of intra-query parallelism
     * for vector search (issue #245). When {@code searchParallelism >= 2}, the
     * store owns a fixed-size executor that fans out segment and shard
     * searches across worker threads; the executor is shut down in
     * {@link #close()}. Values {@code <= 1} keep the original serial
     * behaviour and do not allocate any pool.
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
                                 VectorMemoryBudget memoryBudget,
                                 long maxLiveBytesPerCheckpoint,
                                 long segmentPageCacheMaxBytes,
                                 int searchParallelism) {
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
        this.maxLiveBytesPerCheckpoint = maxLiveBytesPerCheckpoint > 0 ? maxLiveBytesPerCheckpoint : 10L * 1024 * 1024 * 1024;
        // segmentPageCacheMaxBytes is now honoured by the multipart-aware
        // SegmentBlockCache, which lives on the RemoteFileDataStorageManager
        // rather than inside this store — it sits under the jvector reader
        // (RemoteRandomAccessReader) on the vector-search hot path. The
        // constructor still validates the value for backward compatibility.
        if (segmentPageCacheMaxBytes < 0) {
            throw new IllegalArgumentException("segmentPageCacheMaxBytes must be >= 0");
        }
        this.searchParallelism = Math.max(1, searchParallelism);
        if (this.searchParallelism >= 2) {
            final int parallelism = this.searchParallelism;
            final String idx = indexName;
            this.searchExecutor = Executors.newFixedThreadPool(parallelism, r -> {
                Thread t = new Thread(r, "vector-search-" + idx + "-"
                        + SEARCH_WORKER_SEQ.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
        } else {
            this.searchExecutor = null;
        }
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
        /**
         * Primary-key to <em>local ordinal</em>. Keys are bounded by
         * {@code computeEffectiveMaxLiveGraphSize()} so they always fit in
         * an {@code int}.
         */
        final ConcurrentHashMap<Bytes, Integer> pkToNode;
        /**
         * Local ordinal to primary-key. Counterpart of {@link #pkToNode}.
         */
        final ConcurrentHashMap<Integer, Bytes> nodeToPk;
        final RandomAccessVectorValues mravv;
        final GraphIndexBuilder builder;
        final AtomicInteger vectorCount = new AtomicInteger(0);
        /**
         * Per-shard lock-free vector storage, indexed by <em>local ordinal</em>
         * {@code [0, vectorCount)}. Owned by this shard; dropped when the
         * shard is checkpointed out (Phase C) so its int-indexed backing
         * array never grows past the configured shard cap even as the
         * global {@code nextNodeId} counter progresses (issue #256).
         */
        final VectorStorage vectorStorage;
        /**
         * Global nodeId of the first node added to this shard (i.e.
         * {@code PersistentVectorStore.nextNodeId.get()} at the moment
         * the shard was created). Retained for observability and for the
         * local-ordinal computation {@code (int)(globalNodeId - startNodeId)}
         * at insert time.
         */
        final long startNodeId;

        LiveGraphShard(ConcurrentHashMap<Bytes, Integer> pkToNode,
                       ConcurrentHashMap<Integer, Bytes> nodeToPk,
                       RandomAccessVectorValues mravv,
                       GraphIndexBuilder builder,
                       VectorStorage vectorStorage,
                       long startNodeId) {
            this.pkToNode = pkToNode;
            this.nodeToPk = nodeToPk;
            this.mravv = mravv;
            this.builder = builder;
            this.vectorStorage = vectorStorage;
            this.startNodeId = startNodeId;
        }
    }

    /** Holds the result of writing a single segment's multipart files during checkpoint. */
    static class SegmentWriteResult {
        final int segmentId;
        final String graphFilePath;
        final long graphFileSize;
        final String mapFilePath;
        final long mapFileSize;
        final long estimatedSizeBytes;

        SegmentWriteResult(int segmentId, String graphFilePath, long graphFileSize,
                           String mapFilePath, long mapFileSize, long estimatedSizeBytes) {
            this.segmentId = segmentId;
            this.graphFilePath = graphFilePath;
            this.graphFileSize = graphFileSize;
            this.mapFilePath = mapFilePath;
            this.mapFileSize = mapFileSize;
            this.estimatedSizeBytes = estimatedSizeBytes;
        }
    }

    /**
     * A segment or map file queued for physical deletion. Tracked in the
     * IndexStatus so the retention protocol survives restarts
     * and reaper decisions are replayable.
     *
     * <p>Deletion becomes eligible when {@code System.currentTimeMillis() >=
     * deadlineMs} AND {@code sinceGeneration <=
     * min(shadowAckedGeneration)} (or no shadows are known).
     */
    public static final class PendingDelete {
        public final String filePath;
        public final long deadlineMs;
        public final long sinceGeneration;

        public PendingDelete(String filePath, long deadlineMs, long sinceGeneration) {
            this.filePath = filePath;
            this.deadlineMs = deadlineMs;
            this.sinceGeneration = sinceGeneration;
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
        LOGGER.log(Level.INFO, "starting PersistentVectorStore {0} uuid {1} (readOnly={2})",
                new Object[]{indexName, indexUUID, readOnly});

        if (!dataStorageManager.supportsVectorIndexes()) {
            throw new DataStorageManagerException(
                    "Vector indexes are not supported on "
                            + dataStorageManager.getClass().getSimpleName()
                            + ". Use a file- or remote-file-based DataStorageManager.");
        }

        dataStorageManager.initIndex(tableSpaceUUID, indexUUID);

        // Try to load existing state
        try {
            IndexStatus status = dataStorageManager.getIndexStatus(
                    tableSpaceUUID, indexUUID, LogSequenceNumber.START_OF_TIME);
            if (status != null && status.indexData != null && status.indexData.length > 0) {
                loadFromStatus(status);
                this.loadedLsn = status.sequenceNumber;
            }
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.INFO,
                    "no existing state for PersistentVectorStore {0}, starting empty: {1}",
                    new Object[]{indexName, e.getMessage()});
        }

        if (readOnly) {
            // Shadow replicas never run compaction or write back to storage —
            // their segment list is updated by reloadFromStatus calls triggered
            // from the shadow engine when the primary advertises a new LSN.
            LOGGER.log(Level.INFO,
                    "PersistentVectorStore {0} started in read-only mode (no compaction thread)",
                    indexName);
            return;
        }

        // Start background checkpoint-driver thread
        running = true;
        compactionThread = new Thread(this::compactionLoop,
                "persistent-vector-store-compaction-" + indexName);
        compactionThread.setDaemon(true);
        compactionThread.start();

        // Start background graph-merge compaction thread (separate cadence,
        // separate responsibilities from the checkpoint driver).
        vectorIndexCompactionThread = new Thread(this::vectorIndexCompactionLoop,
                "persistent-vector-store-vidxcompaction-" + indexName);
        vectorIndexCompactionThread.setDaemon(true);
        vectorIndexCompactionThread.start();

        LOGGER.log(Level.INFO, "PersistentVectorStore {0} started", indexName);
    }

    /**
     * Applies the compaction policy knobs from configuration. Must be
     * called before {@link #start()} for the values to influence the
     * first compaction cycle, but can also be called at any time to
     * re-tune a running store.
     */
    public void configureCompaction(long intervalMs, long minBytes, long maxBytes,
                                    int minCount, long retentionMs) {
        this.vectorIndexCompactionIntervalMs = intervalMs;
        this.vectorIndexCompactionMinBytes = minBytes;
        this.vectorIndexCompactionMaxBytes = maxBytes;
        this.vectorIndexCompactionMinCount = minCount;
        this.vectorIndexCompactionRetentionMs = retentionMs;
    }

    /** Wakes the compaction thread. Called by tests and the retention reaper. */
    public void wakeVectorIndexCompaction() {
        synchronized (vectorIndexCompactionWakeup) {
            vectorIndexCompactionWakeupPending = true;
            vectorIndexCompactionWakeup.notifyAll();
        }
    }

    /**
     * Toggles read-only mode. Must be called before {@link #start()}. See the
     * {@link #readOnly} field doc for semantics.
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /** Exposes the internal indexUUID (used by shadows to re-read IndexStatus). */
    public String getIndexUuid() {
        return indexUUID;
    }

    /**
     * LogSequenceNumber of the last {@link IndexStatus} applied to this store,
     * or {@code null} if none has been applied. For primaries this is set once
     * at start() and is not updated (primaries advance state through the live
     * shard / checkpoint pipeline, not through reload). For shadows it advances
     * on every successful reloadFromStatus.
     */
    public LogSequenceNumber getLoadedLsn() {
        return loadedLsn;
    }

    /**
     * Shadow-only: re-load the on-disk segment list from the given
     * {@link IndexStatus} snapshot. Closes segments that no longer appear in
     * the new status, opens new ones, and updates {@link #getLoadedLsn()}.
     *
     * <p>Blocks queries only for the segment-list swap at the end: new
     * segments are populated outside the write lock (remote reads happen
     * without blocking searches), and only the pointer swap is performed
     * under the lock. Must not be called on a primary.
     */
    public synchronized void reloadFromStatus(IndexStatus newStatus)
            throws IOException, DataStorageManagerException {
        if (!readOnly) {
            throw new IllegalStateException(
                    "reloadFromStatus is only supported on read-only PersistentVectorStore");
        }
        if (newStatus == null || newStatus.indexData == null || newStatus.indexData.length == 0) {
            return;
        }
        // For simplicity in this first implementation, close all existing
        // segments and reload from scratch. A later optimisation (issue tracked
        // in the shadow-replicas design plan) can diff the segment list and
        // preserve unchanged segments to avoid re-opening readers.
        stateLock.writeLock().lock();
        try {
            List<VectorSegment> old = segments;
            segments = new java.util.concurrent.CopyOnWriteArrayList<>();
            for (VectorSegment seg : old) {
                try {
                    seg.close();
                } catch (Exception e) {
                    LOGGER.log(Level.FINE, "ignoring segment close failure during reload", e);
                }
            }
            nextSegmentId.set(0);
            nextNodeId.set(0);
            loadFromStatus(newStatus);
            this.loadedLsn = newStatus.sequenceNumber;
        } finally {
            stateLock.writeLock().unlock();
        }
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

    /**
     * Dedicated driver for graph-merge compaction (issue — vector-index
     * compaction). Runs on its own cadence, independent of the
     * checkpoint driver above: we do NOT want a tight checkpoint loop
     * to also trigger a heavyweight segment rewrite every time.
     */
    private void vectorIndexCompactionLoop() {
        while (running) {
            long sleepMs = vectorIndexCompactionIntervalMs;
            long failures = compactionConsecutiveFailures.get();
            if (failures > 0) {
                long backoff = computeBackoffMs(vectorIndexCompactionIntervalMs,
                        failures, MAX_BACKOFF_MS);
                sleepMs = saturatedAdd(sleepMs, backoff);
            }
            try {
                synchronized (vectorIndexCompactionWakeup) {
                    if (!vectorIndexCompactionWakeupPending) {
                        vectorIndexCompactionWakeup.wait(sleepMs);
                    }
                    vectorIndexCompactionWakeupPending = false;
                }
            } catch (InterruptedException e) {
                if (!running) {
                    return;
                }
                Thread.interrupted();
            }
            if (!running) {
                return;
            }
            try {
                runCompactionCycle();
            } catch (InterruptedException e) {
                if (!running) {
                    return;
                }
            } catch (RuntimeException e) {
                // Safety net: the cycle itself logs specific failure
                // reasons, but we must not let the thread die.
                LOGGER.log(Level.SEVERE,
                        "vector store " + indexName + ": unexpected compaction failure", e);
                compactionConsecutiveFailures.incrementAndGet();
            }
        }
    }

    /**
     * Runs at most one compaction cycle. Silently skips when the lock
     * is busy (tests may invoke synchronously) or when the policy says
     * no candidates are large enough / numerous enough.
     */
    public void runCompactionCycle() throws InterruptedException {
        if (!compactionLock.tryLock()) {
            return; // a cycle is already running
        }
        try {
            long cycleStart = System.currentTimeMillis();
            compactionRunsTotal.incrementAndGet();

            List<VectorSegment> snapshot = new ArrayList<>(segments);
            List<VectorSegment> candidates = VectorIndexCompactor.chooseSegmentsToMerge(
                    snapshot,
                    vectorIndexCompactionMinCount,
                    vectorIndexCompactionMinBytes,
                    vectorIndexCompactionMaxBytes);
            if (candidates.isEmpty()) {
                return;
            }

            LOGGER.log(Level.INFO,
                    "vector store {0}: starting graph-merge compaction ({1} candidate segments)",
                    new Object[]{indexName, candidates.size()});

            compactionActive.set(1);
            this.pendingCompactionDeletes =
                    java.util.concurrent.ConcurrentHashMap.newKeySet();
            Set<Bytes> liveShardPkSnapshot = new java.util.HashSet<>();
            for (LiveGraphShard shard : liveShards) {
                liveShardPkSnapshot.addAll(shard.pkToNode.keySet());
            }
            Map<Bytes, Integer> authority = VectorIndexCompactor.buildAuthorityMap(
                    candidates, snapshot, liveShardPkSnapshot);

            long bytesRead = 0;
            long vectorsWritten = 0;
            long vectorsFiltered = 0;
            VectorIndexCompactor.RebuildResult rebuild = null;
            try {
                rebuild = VectorIndexCompactor.rebuildSegment(this, candidates, authority);
                if (rebuild == null) {
                    // Nothing survived the filter — everything in these inputs
                    // is tombstoned or superseded. Skip the rebuild; just
                    // swap the inputs out and queue them for retention.
                    atomicSwapCompactionResult(candidates, null, 0L);
                    compactionSuccessesTotal.incrementAndGet();
                    compactionConsecutiveFailures.set(0);
                    long emptyCycleMs = System.currentTimeMillis() - cycleStart;
                    compactionLastDurationMs.set(emptyCycleMs);
                    compactionLastBytesRead.set(candidates.stream()
                            .mapToLong(s -> s.estimatedSizeBytes).sum());
                    compactionLastBytesWritten.set(0);
                    compactionLastInputSegments.set(candidates.size());
                    compactionLastOutputSegments.set(0);
                    compactionLivePkFilteredTotal.addAndGet(countDeadPks(candidates, authority));
                    LOGGER.log(Level.INFO,
                            "vector store {0}: empty-result compaction in {1} ms — "
                                    + "swapped out {2} fully-obsolete segments",
                            new Object[]{indexName, emptyCycleMs, candidates.size()});
                    return;
                }

                bytesRead = candidates.stream().mapToLong(s -> s.estimatedSizeBytes).sum();
                vectorsWritten = rebuild.vectorCount;
                vectorsFiltered = rebuild.filteredCount;

                // Apply deletes that arrived during the rebuild.
                Set<Bytes> lateDeletes = this.pendingCompactionDeletes;
                if (lateDeletes != null) {
                    for (Bytes pk : lateDeletes) {
                        rebuild.mergedSegment.deletePk(pk);
                    }
                }

                atomicSwapCompactionResult(candidates, rebuild.mergedSegment,
                        rebuild.bytesWritten);

                compactionSuccessesTotal.incrementAndGet();
                compactionConsecutiveFailures.set(0);
                long durationMs = System.currentTimeMillis() - cycleStart;
                compactionLastDurationMs.set(durationMs);
                compactionLastBytesRead.set(bytesRead);
                compactionLastBytesWritten.set(rebuild.bytesWritten);
                compactionLastInputSegments.set(candidates.size());
                compactionLastOutputSegments.set(1);
                compactionLivePkFilteredTotal.addAndGet(vectorsFiltered);

                LOGGER.log(Level.INFO,
                        "vector store {0}: compaction complete in {1} ms — "
                                + "{2} inputs ({3} bytes) -> 1 output ({4} bytes, "
                                + "{5} vectors kept, {6} filtered)",
                        new Object[]{indexName, durationMs, candidates.size(), bytesRead,
                                rebuild.bytesWritten, vectorsWritten, vectorsFiltered});
            } catch (VectorIndexCompactor.CompactionException e) {
                recordCompactionFailure(e.reason);
                LOGGER.log(Level.WARNING,
                        "vector store " + indexName + ": compaction failed ("
                                + e.reason + ")", e);
                // Clean up orphaned output files if any were written.
                if (rebuild != null && rebuild.orphanPaths != null) {
                    long now = System.currentTimeMillis();
                    long sinceGen = currentIndexStatusGeneration.get();
                    for (String[] orphan : rebuild.orphanPaths) {
                        pendingDeletes.add(new PendingDelete(
                                encodeMultipartPath(orphan[0], orphan[1]),
                                now, sinceGen));
                    }
                }
            } catch (IOException e) {
                recordCompactionFailure(VectorIndexCompactor.FailureReason.WRITE_IO);
                LOGGER.log(Level.WARNING,
                        "vector store " + indexName + ": compaction I/O failure", e);
            } catch (DataStorageManagerException e) {
                recordCompactionFailure(VectorIndexCompactor.FailureReason.METADATA_IO);
                LOGGER.log(Level.WARNING,
                        "vector store " + indexName + ": compaction metadata failure", e);
            }
        } finally {
            compactionActive.set(0);
            this.pendingCompactionDeletes = null;
            compactionLock.unlock();
        }
    }

    private static long countDeadPks(List<VectorSegment> candidates, Map<Bytes, Integer> authority) {
        long total = 0;
        for (VectorSegment seg : candidates) {
            int[] offsets = seg.pkOffsets;
            int[] lengths = seg.pkLengths;
            byte[] data = seg.pkData;
            if (offsets == null) {
                continue;
            }
            for (int ord = 0; ord < offsets.length; ord++) {
                if (offsets[ord] < 0) {
                    total++;
                    continue;
                }
                Bytes pk = Bytes.from_array(data, offsets[ord], lengths[ord]);
                Integer owner = authority.get(pk);
                if (owner == null || owner != seg.segmentId) {
                    total++;
                }
            }
        }
        return total;
    }

    private void recordCompactionFailure(VectorIndexCompactor.FailureReason reason) {
        compactionConsecutiveFailures.incrementAndGet();
        switch (reason) {
            case READ_IO:
                compactionFailuresReadIoTotal.incrementAndGet();
                break;
            case WRITE_IO:
                compactionFailuresWriteIoTotal.incrementAndGet();
                break;
            case METADATA_IO:
                compactionFailuresMetadataIoTotal.incrementAndGet();
                break;
            case CORRUPTION:
                compactionFailuresCorruptionTotal.incrementAndGet();
                break;
            case DISK_FULL:
                compactionFailuresDiskFullTotal.incrementAndGet();
                break;
            case ABORTED_INPUT_GONE:
                compactionFailuresAbortedInputGoneTotal.incrementAndGet();
                break;
            default:
                break;
        }
    }

    /**
     * Atomic segment-list swap + IndexStatus publish for a completed
     * compaction run. Validates that every input is still present in
     * {@code segments} — if a concurrent checkpoint has moved an input
     * under us, aborts with {@code ABORTED_INPUT_GONE} instead of
     * silently dropping data.
     */
    private void atomicSwapCompactionResult(List<VectorSegment> inputs,
                                            VectorSegment mergedOutput,
                                            long bytesWritten)
            throws VectorIndexCompactor.CompactionException, DataStorageManagerException {
        checkpointLock.lock();
        try {
            stateLock.writeLock().lock();
            try {
                // Validate every input is still in the segment list.
                List<VectorSegment> current = segments;
                Set<Integer> currentIds = new java.util.HashSet<>();
                for (VectorSegment s : current) {
                    currentIds.add(s.segmentId);
                }
                for (VectorSegment in : inputs) {
                    if (!currentIds.contains(in.segmentId)) {
                        throw new VectorIndexCompactor.CompactionException(
                                VectorIndexCompactor.FailureReason.ABORTED_INPUT_GONE,
                                "input segment " + in.segmentId + " disappeared from segment list");
                    }
                }

                // Build new segment list.
                List<VectorSegment> newSegments =
                        new java.util.concurrent.CopyOnWriteArrayList<>();
                Set<Integer> inputIds = new java.util.HashSet<>();
                for (VectorSegment in : inputs) {
                    inputIds.add(in.segmentId);
                }
                for (VectorSegment s : current) {
                    if (!inputIds.contains(s.segmentId)) {
                        newSegments.add(s);
                    }
                }
                if (mergedOutput != null) {
                    // Fresh generation will be assigned by
                    // persistIndexStatusMultiSegment; we set it now so
                    // the in-memory state matches what we persist.
                    mergedOutput.generation = currentIndexStatusGeneration.get() + 1;
                    newSegments.add(mergedOutput);
                }

                // Queue inputs for retention-aware deletion.
                for (VectorSegment in : inputs) {
                    queueSegmentPendingDelete(in, vectorIndexCompactionRetentionMs);
                }

                // Publish the new IndexStatus. Compaction reuses
                // persistIndexStatusMultiSegment by passing sealed=all
                // kept segments and newSegmentResults=empty (the merged
                // output is already a VectorSegment, not a
                // SegmentWriteResult).
                List<VectorSegment> allSealedForPersist = new ArrayList<>(newSegments);
                persistIndexStatusMultiSegment(
                        allSealedForPersist,
                        java.util.Collections.emptyList(),
                        java.util.Collections.emptyList(),
                        LogSequenceNumber.START_OF_TIME);

                this.segments = newSegments;
                dirty.set(dirty.get() || totalLiveSize() > 0);
            } finally {
                stateLock.writeLock().unlock();
            }
            // Close the inputs OUTSIDE the write lock: the searchers that
            // held a reference dropped it when we swapped `segments`.
            for (VectorSegment in : inputs) {
                try {
                    in.close();
                } catch (RuntimeException e) {
                    // Narrow catch would be ideal but seg.close() can
                    // surface BLink close failures and we must not let
                    // the swap fail afterwards.
                    LOGGER.log(Level.FINE,
                            "vector store " + indexName
                                    + ": ignoring close failure for compacted input segment",
                            e);
                }
            }
        } finally {
            checkpointLock.unlock();
        }
    }

    // Package-private accessors for VectorIndexCompactor
    int newSegmentId() {
        return nextSegmentId.getAndIncrement();
    }

    long allocateCompactionNodeIds(int count) {
        // The bump of nextNodeId must be atomic with a rotation of the active
        // live shard. Without rotation, the next addVector call would allocate
        // nodeId = nextNodeId (post-bump) but compute
        // local = nodeId - active.startNodeId on the OLD shard, opening a
        // `count`-wide gap in the shard's local ordinal space. Phase B would
        // then iterate the shard's graph and call pqv.get(ordinal) with an
        // ordinal far beyond the PQ training-set size, throwing
        // IndexOutOfBoundsException (issue #255).
        //
        // We hold stateLock.writeLock() to exclude addVector (which holds the
        // read lock). This method is called from VectorIndexCompactor.rebuildSegment
        // which does NOT hold checkpointLock or stateLock, so the lock order is
        // deadlock-free.
        stateLock.writeLock().lock();
        try {
            long start = nextNodeId.getAndAdd(count);
            List<LiveGraphShard> shards = this.liveShards;
            if (dimension != 0 && shards != null && !shards.isEmpty()) {
                LiveGraphShard activeShard = shards.get(shards.size() - 1);
                int sealedSize = activeShard.nodeToPk.size();
                // Rotate whenever the bump would leave the active shard's
                // startNodeId stale — this covers both the non-empty case
                // (PR #257 / issue #255) AND the empty case (an active
                // shard that was created before this bump would otherwise
                // see the next ingest compute
                //   local = postBumpNextNodeId - oldStartNodeId
                // which is exactly the bump-wide gap we are trying to avoid).
                // createEmptyLiveShard reads nextNodeId.get() AFTER the bump
                // so the replacement shard's startNodeId equals the post-bump
                // value and future ingest produces local = 0, 1, 2, ...
                // contiguously.
                if (sealedSize > 0 || activeShard.startNodeId != nextNodeId.get()) {
                    LiveGraphShard fresh = createEmptyLiveShard(
                            dimension, beamWidth, neighborOverflow, alpha,
                            nextNodeId.get());
                    List<LiveGraphShard> newList;
                    if (sealedSize > 0) {
                        // Preserve the non-empty shard as sealed and append
                        // the fresh one — matches the issue-#255 behaviour.
                        newList = new ArrayList<>(shards);
                        newList.add(fresh);
                    } else {
                        // Replace the stale empty shard so we do not leak
                        // empty shards across every compaction reservation.
                        newList = new ArrayList<>(shards.subList(0, shards.size() - 1));
                        newList.add(fresh);
                    }
                    synchronized (this) {
                        this.liveShards = newList;
                    }
                    LOGGER.log(Level.INFO,
                            "vector store {0}: rotated live graph shard before "
                                    + "compaction nodeId reservation, now {1} shards "
                                    + "({2} vectors in sealed shard, reserved range "
                                    + "[{3},{4}))",
                            new Object[]{indexName, newList.size(), sealedSize,
                                    start, start + count});
                }
            }
            return start;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    int compactionDimension() {
        return dimension;
    }

    VectorSimilarityFunction compactionSimilarity() {
        return similarityFunction;
    }

    SegmentWriteResult writeSyntheticShard(LiveGraphShard syntheticShard,
                                           int segmentId,
                                           int dim) throws IOException, DataStorageManagerException {
        return writeShardAsFusedPQSegment(syntheticShard, segmentId, dim);
    }

    VectorSegment preloadCompactedSegment(SegmentWriteResult swr) throws IOException, DataStorageManagerException {
        VectorSegment seg = new VectorSegment(swr.segmentId);
        seg.estimatedSizeBytes = swr.estimatedSizeBytes;
        seg.graphFilePath = swr.graphFilePath;
        seg.graphFileSize = swr.graphFileSize;
        seg.mapFilePath = swr.mapFilePath;
        seg.mapFileSize = swr.mapFileSize;
        Path mapFile = readMultipartMapDataToTempFile(seg);
        loadFusedPQSegment(seg, mapFile, dimension, nextNodeId.get());
        return seg;
    }

    String indexUUID() {
        return indexUUID;
    }

    int graphBuilderM() {
        return m;
    }

    int graphBuilderBeamWidth() {
        return beamWidth;
    }

    float graphBuilderNeighborOverflow() {
        return neighborOverflow;
    }

    float graphBuilderAlpha() {
        return alpha;
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
        Thread vct = vectorIndexCompactionThread;
        if (vct != null) {
            synchronized (vectorIndexCompactionWakeup) {
                vectorIndexCompactionWakeupPending = true;
                vectorIndexCompactionWakeup.notifyAll();
            }
            vct.interrupt();
            vct.join(10000);
            vectorIndexCompactionThread = null;
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
            this.deferredShards = null;
            currentDeferredVectors.set(0);
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

        if (searchExecutor != null) {
            searchExecutor.shutdown();
            try {
                if (!searchExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    searchExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                searchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

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
        if (readOnly) {
            throw new UnsupportedOperationException(
                    "addVector is not supported on a read-only PersistentVectorStore "
                            + indexName + " (shadow replica)");
        }
        if (vector == null || vector.length == 0) {
            return;
        }
        addVectorInternal(pk, vector.length, VTS.createFloatVector(vector));
    }

    /**
     * Zero-copy variant of {@link #addVector(Bytes, float[])}: wraps the caller-owned
     * buffer as a {@code VectorFloat<?>} view via {@code wrapFloatVector} rather than
     * materializing a {@code float[]}. The buffer is not retained past this call.
     */
    @Override
    public void addVector(Bytes pk, ByteBuffer vector) {
        if (readOnly) {
            throw new UnsupportedOperationException(
                    "addVector is not supported on a read-only PersistentVectorStore "
                            + indexName + " (shadow replica)");
        }
        if (vector == null || vector.remaining() == 0) {
            return;
        }
        int dim = vector.remaining() / Float.BYTES;
        addVectorInternal(pk, dim, VTS.wrapFloatVector(vector));
    }

    private void addVectorInternal(Bytes pk, int dim, VectorFloat<?> vec) {
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

        // Phase 1 — structural checks: decide whether init or rotation is needed
        // using cheap volatile reads BEFORE taking any lock.  Both fields are
        // volatile so the reads are safe without a lock; we double-check under
        // the write lock inside initBuilderForDimension / rotateLiveShard.
        // Building the candidate shard (expensive jvector GraphIndexBuilder
        // initialisation) also happens outside any lock so that concurrent ingest
        // threads do not block one another during shard construction (issue #282).
        if (dimension == 0) {
            // First-ever insert: build the candidate outside any lock, publish
            // under write lock. Only one shard is created even under contention
            // because of the dimension==0 double-check inside the write lock.
            LiveGraphShard candidate = createEmptyLiveShard(dim, beamWidth, neighborOverflow, alpha);
            initBuilderForDimension(dim, candidate);
        }

        List<LiveGraphShard> snapShards = this.liveShards;
        if (!snapShards.isEmpty()) {
            LiveGraphShard snapActive = snapShards.get(snapShards.size() - 1);
            if (snapActive.nodeToPk.size() >= computeEffectiveMaxLiveGraphSize()) {
                // Rotation needed: build the candidate outside any lock, then
                // publish under the write lock.  Under burst concurrency up to K
                // threads may build a candidate; K-1 are discarded by the
                // double-check inside rotateLiveShard (issue #282).
                LiveGraphShard candidate = createEmptyLiveShard(
                        dimension, beamWidth, neighborOverflow, alpha);
                rotateLiveShard(candidate);
            }
        }

        // Phase 2 — insert under the read lock.  stateLock.writeLock() is
        // exclusive with readLock so once we hold the read lock, no checkpoint
        // or rotation can swap liveShards out from under us.
        //
        // We loop at most twice: on the first attempt the active shard is
        // normally non-full (Phase 1 handled it).  In the rare case that
        // another thread filled the freshly-rotated shard between our Phase-1
        // check and acquiring the read lock we rotate once more and retry.
        // The loop terminates because every iteration either inserts (done) or
        // triggers exactly one more rotation (continue).
        while (true) {
            stateLock.readLock().lock();
            try {
                if (dim != dimension) {
                    LOGGER.log(Level.WARNING,
                            "vector dimension mismatch on insert: expected {0} but got {1}, skipping",
                            new Object[]{dimension, dim});
                    return;
                }

                List<LiveGraphShard> shards = this.liveShards;
                LiveGraphShard active = shards.get(shards.size() - 1);

                if (active.nodeToPk.size() < computeEffectiveMaxLiveGraphSize()) {
                    long globalNodeId = nextNodeId.getAndIncrement();
                    // Per-shard span is bounded by computeEffectiveMaxLiveGraphSize(),
                    // so the cast always fits. Math.toIntExact turns a violated
                    // invariant into a loud ArithmeticException instead of silently
                    // wrapping (issue #256).
                    int localOrd = Math.toIntExact(globalNodeId - active.startNodeId);
                    active.vectorStorage.set(localOrd, vec);
                    active.vectorCount.incrementAndGet();
                    active.pkToNode.put(pk, localOrd);
                    active.nodeToPk.put(localOrd, pk);
                    active.builder.addGraphNode(localOrd, vec);
                    dirty.set(true);
                    return; // inserted successfully — done
                }
                // Active shard is still full (another thread filled it between
                // Phase-1 and now). Fall through to rotate outside the read
                // lock, then loop back.
            } finally {
                stateLock.readLock().unlock();
            }
            // Release the read lock before acquiring the write lock inside
            // rotateLiveShard (ReentrantReadWriteLock does not support
            // read-to-write upgrade).
            LiveGraphShard candidate = createEmptyLiveShard(
                    dimension, beamWidth, neighborOverflow, alpha);
            rotateLiveShard(candidate);
        }
    }

    /**
     * Removes the vector with the given primary key.
     */
    @Override
    public void removeVector(Bytes pk) {
        if (readOnly) {
            throw new UnsupportedOperationException(
                    "removeVector is not supported on a read-only PersistentVectorStore "
                            + indexName + " (shadow replica)");
        }
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
            // Track delete for in-flight compaction: if we are rebuilding
            // segments right now, the merged output must see this delete
            // before it is published.
            Set<Bytes> pendingCompact = pendingCompactionDeletes;
            if (pendingCompact != null) {
                pendingCompact.add(pk);
            }
            // Check all live shards
            for (LiveGraphShard shard : liveShards) {
                Integer localOrd = shard.pkToNode.remove(pk);
                if (localOrd != null) {
                    shard.nodeToPk.remove(localOrd);
                    dirty.set(true);
                    if (shard.builder != null) {
                        shard.builder.markNodeDeleted(localOrd);
                    } else {
                        shard.vectorStorage.remove(localOrd);
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
        return searchInternal(VTS.createFloatVector(queryVector), topK);
    }

    /**
     * Zero-copy variant of {@link #search(float[], int)}: interprets the caller-owned
     * buffer's remaining bytes as the query vector without materializing a {@code float[]}.
     * The buffer is not retained past this call.
     */
    @Override
    public List<Map.Entry<Bytes, Float>> search(ByteBuffer queryVector, int topK) {
        return searchInternal(VTS.wrapFloatVector(queryVector), topK);
    }

    private List<Map.Entry<Bytes, Float>> searchInternal(VectorFloat<?> qv, int topK) {
        // Overquery each source to improve recall when merging across segments.
        // Each source returns more candidates; the final merge picks the true topK.
        final int perSourceK = topK * VectorSegment.OVERQUERY_FACTOR;

        // Capture the request-scoped context (if any) so worker threads can
        // attribute their readFileRange events back to the originating request.
        final VectorSearchRequestContext ctx = VectorSearchRequestContext.current();

        List<Callable<List<Map.Entry<Bytes, Float>>>> tasks = new ArrayList<>();

        // Phase 1: on-disk segments.
        final List<VectorSegment> currentSegments = this.segments;
        for (final VectorSegment seg : currentSegments) {
            tasks.add(wrapInContext(ctx, () -> {
                List<Map.Entry<Bytes, Float>> local = new ArrayList<>(perSourceK);
                seg.search(qv, perSourceK, similarityFunction, local);
                return local;
            }));
        }

        // Phase 2: live in-memory shards (no pending-deletes filtering).
        for (final LiveGraphShard shard : liveShards) {
            if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                tasks.add(wrapInContext(ctx, () -> searchLiveShard(shard, qv, perSourceK, null)));
            }
        }

        List<List<Map.Entry<Bytes, Float>>> partials;

        // Phase 3: frozen + deferred shards. Both are cleaned up by Phase C
        // under the write lock, so we must hold the read lock to prevent
        // Phase C from dropping the shards (and their per-shard VectorStorage
        // references) while GraphSearcher is still accessing them via
        // shard.mravv. Without this lock, a race would let the shard become
        // unreachable while jvector's scorer is calling getVector() on its
        // storage, leading to NullPointerException (issue #129). The lock
        // is held while invokeSearchTasks blocks on task completion: worker
        // threads can join as additional readers because ReentrantReadWriteLock
        // allows multiple concurrent read holders.
        stateLock.readLock().lock();
        try {
            final Set<Bytes> pending = pendingCheckpointDeletes;
            List<LiveGraphShard> frozen = frozenShards;
            if (frozen != null) {
                for (final LiveGraphShard shard : frozen) {
                    if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                        tasks.add(wrapInContext(ctx,
                                () -> searchLiveShard(shard, qv, perSourceK, pending)));
                    }
                }
            }
            List<LiveGraphShard> deferred = deferredShards;
            if (deferred != null) {
                for (final LiveGraphShard shard : deferred) {
                    if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                        tasks.add(wrapInContext(ctx,
                                () -> searchLiveShard(shard, qv, perSourceK, pending)));
                    }
                }
            }

            partials = invokeSearchTasks(tasks);
        } finally {
            stateLock.readLock().unlock();
        }

        // Serial merge (intentional — see issue #245). Sort all partial
        // results by score descending and take top-K.
        List<Map.Entry<Bytes, Float>> merged = new ArrayList<>();
        for (List<Map.Entry<Bytes, Float>> p : partials) {
            merged.addAll(p);
        }
        merged.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));
        return merged.size() <= topK ? merged : merged.subList(0, topK);
    }

    /**
     * Searches a single live graph shard and returns its partial results.
     * Shared by the live, frozen, and deferred phases of
     * {@link #searchInternal}; {@code pendingDeletes} is {@code null} for the
     * live-shard phase and the store's pending-deletes set when scanning
     * frozen/deferred shards during a checkpoint.
     */
    private List<Map.Entry<Bytes, Float>> searchLiveShard(LiveGraphShard shard, VectorFloat<?> qv,
                                                          int perSourceK, Set<Bytes> pendingDeletes) {
        int k = Math.min(perSourceK, shard.nodeToPk.size());
        ImmutableGraphIndex graph = shard.builder.getGraph();
        SearchResult result = GraphSearcher.search(
                qv, k, shard.mravv, similarityFunction, graph, Bits.ALL);
        List<Map.Entry<Bytes, Float>> out = new ArrayList<>(result.getNodes().length);
        for (SearchResult.NodeScore ns : result.getNodes()) {
            Bytes pk = shard.nodeToPk.get(ns.node);
            if (pk != null && (pendingDeletes == null || !pendingDeletes.contains(pk))) {
                out.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
            }
        }
        return out;
    }

    /**
     * Wraps a search task so it binds the initiator's
     * {@link VectorSearchRequestContext} on the worker thread for the
     * duration of one call, then clears it. Null-safe when no context is
     * active on the calling thread.
     */
    private Callable<List<Map.Entry<Bytes, Float>>> wrapInContext(
            VectorSearchRequestContext ctx,
            Callable<List<Map.Entry<Bytes, Float>>> inner) {
        if (ctx == null) {
            return inner;
        }
        return () -> {
            VectorSearchRequestContext.bind(ctx);
            try {
                return inner.call();
            } finally {
                VectorSearchRequestContext.end();
            }
        };
    }

    /**
     * Runs all search tasks, returning their results in the order they were
     * submitted. Runs inline on the caller thread when no executor is
     * configured, when only one task was produced, or when running on a
     * worker thread that would otherwise try to reenter its own pool. Any
     * checked/unchecked exception from a task is surfaced as a
     * {@link RuntimeException} — matching the existing serial behaviour
     * where segment search errors bubble up as runtime exceptions.
     */
    private List<List<Map.Entry<Bytes, Float>>> invokeSearchTasks(
            List<Callable<List<Map.Entry<Bytes, Float>>>> tasks) {
        List<List<Map.Entry<Bytes, Float>>> out = new ArrayList<>(tasks.size());
        if (searchExecutor == null || tasks.size() <= 1) {
            for (Callable<List<Map.Entry<Bytes, Float>>> task : tasks) {
                try {
                    out.add(task.call());
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("vector search task failed", e);
                }
            }
            return out;
        }
        List<Future<List<Map.Entry<Bytes, Float>>>> futures;
        try {
            futures = searchExecutor.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("vector search interrupted", e);
        }
        for (Future<List<Map.Entry<Bytes, Float>>> f : futures) {
            try {
                out.add(f.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("vector search interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException("vector search task failed", cause);
            }
        }
        return out;
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
        int deferredCount = 0;
        List<LiveGraphShard> deferred = deferredShards;
        if (deferred != null) {
            for (LiveGraphShard shard : deferred) {
                deferredCount += shard.nodeToPk.size();
            }
        }
        return totalLiveSize() + frozenCount + deferredCount + (int) onDiskNodeToPkSize();
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
        List<LiveGraphShard> deferred = deferredShards;
        if (deferred != null) {
            for (LiveGraphShard shard : deferred) {
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
     *
     * @return {@code true} if, at return time, the on-disk state fully covers
     *         every vector this store has observed so far (i.e. the watermark
     *         may safely advance past any LSN already applied to the live
     *         shard); {@code false} if the caller did no work AND the live
     *         shard may contain un-persisted vectors. A {@code false} return
     *         happens only on the two deferral paths: another checkpoint was
     *         already in progress (tryLock-skip), or the min-live-vectors
     *         gate deferred this cycle. In both cases {@code dirty} stays set
     *         and the caller MUST NOT advance the watermark based on this
     *         call — retry on the next trigger.
     */
    public boolean checkpoint() throws DataStorageManagerException {
        if (readOnly) {
            // A read-only shadow never has dirty live state; treat checkpoint
            // as an immediate no-op success so callers (including the engine's
            // checkpointAndSaveWatermark loop — not actually invoked on
            // shadows) can remain oblivious.
            return true;
        }
        try {
            return doCheckpoint();
        } catch (IOException e) {
            throw new DataStorageManagerException(e);
        }
    }

    private boolean doCheckpoint() throws IOException, DataStorageManagerException {
        if (!checkpointLock.tryLock()) {
            LOGGER.log(Level.INFO, "checkpoint {0}: skipped (another checkpoint in progress)", indexName);
            return false;
        }
        try {
            return doCheckpointUnderLock();
        } finally {
            checkpointLock.unlock();
        }
    }

    private boolean doCheckpointUnderLock() throws IOException, DataStorageManagerException {
        long checkpointStartMs = System.currentTimeMillis();
        LogSequenceNumber sequenceNumber = LogSequenceNumber.START_OF_TIME;

        stateLock.writeLock().lock();
        try {
            boolean anySegmentDirty = segments.stream().anyMatch(s -> s.dirty);
            if (!dirty.get() && !anySegmentDirty) {
                LOGGER.log(Level.FINE, "checkpoint {0}: skipped (no changes)", indexName);
                lastSuccessfulCheckpointMs = System.currentTimeMillis();
                return true;
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
                lastSuccessfulCheckpointMs = System.currentTimeMillis();
                return true;
            }

            if (dimension == 0) {
                IndexStatus emptyStatus = new IndexStatus(
                        indexName, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, emptyStatus, false);
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint {0}: empty dimension", indexName);
                lastSuccessfulCheckpointMs = System.currentTimeMillis();
                return true;
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
                lastSuccessfulCheckpointMs = System.currentTimeMillis();
                return true;
            }

            // Min-live-vectors deferral gate (issue #90).
            //
            // During catch-up the live shard blows past the threshold in
            // seconds and this gate never trips. It only matters when the
            // compaction loop is about to run a Phase A on a small partial
            // shard — then we defer the cycle so the tailer can accumulate
            // more vectors before we pay the Phase B cost. The deferral
            // is bounded by MAX_CHECKPOINT_DEFERRAL_MS so a partial shard
            // left behind by stopped ingest is guaranteed to flush.
            int minLiveGate = minLiveVectorsForCheckpoint;
            long deferralBoundMs = maxCheckpointDeferralMs;
            if (minLiveGate > 0
                    && totalLiveVectors < minLiveGate
                    && !anySegmentDirty
                    && !segments.isEmpty()
                    && !shouldTriggerMemoryPressureCheckpoint()) {
                long elapsed = System.currentTimeMillis() - lastSuccessfulCheckpointMs;
                if (elapsed < deferralBoundMs) {
                    LOGGER.log(Level.FINE,
                            "checkpoint {0}: deferred ({1} live vectors < {2} threshold, "
                                    + "{3} ms since last success < {4} ms deferral bound)",
                            new Object[]{indexName, totalLiveVectors, minLiveGate,
                                    elapsed, deferralBoundMs});
                    totalCheckpointsDeferred.incrementAndGet();
                    return false;
                }
                LOGGER.log(Level.INFO,
                        "checkpoint {0}: deferral bound reached ({1} ms >= {2} ms), "
                                + "running Phase A with {3} live vectors",
                        new Object[]{indexName, elapsed, deferralBoundMs, totalLiveVectors});
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        // Three-phase FusedPQ checkpoint. Small shards fall back internally to
        // a non-FusedPQ InlineVectors graph still written via the multipart
        // API — there is no longer a separate page-based simple path.
        doCheckpointFusedPQThreePhase(sequenceNumber);
        totalFusedPQCheckpointCount.incrementAndGet();
        totalCheckpointCount.incrementAndGet();
        lastCheckpointDurationMs.set(System.currentTimeMillis() - checkpointStartMs);
        lastSuccessfulCheckpointMs = System.currentTimeMillis();
        return true;
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

            // Phase A: byte-cap split — if snapshot would exceed the budget,
            // take only the oldest shards that fit and defer the rest.
            List<LiveGraphShard> allShards = this.liveShards;
            long byteCap = maxLiveBytesPerCheckpoint;
            long bytesPerVec = (byteCap > 0 && snapshotDimension > 0)
                    ? estimatedBytesPerVector(snapshotDimension, m, neighborOverflow) : 0L;
            int capIndex = allShards.size();   // default: take all
            if (bytesPerVec > 0) {
                long accumulated = 0;
                for (int i = 0; i < allShards.size(); i++) {
                    long shardVecs = allShards.get(i).nodeToPk.size();
                    // Always include at least one shard to guarantee progress.
                    if (i > 0 && (accumulated + shardVecs) * bytesPerVec > byteCap) {
                        capIndex = i;
                        break;
                    }
                    accumulated += shardVecs;
                }
            }
            if (capIndex < allShards.size()) {
                snapshotShards = new ArrayList<>(allShards.subList(0, capIndex));
                this.deferredShards = new ArrayList<>(allShards.subList(capIndex, allShards.size()));
                long snapshotVectors = snapshotShards.stream().mapToInt(s -> s.nodeToPk.size()).sum();
                long deferredVectors = this.deferredShards.stream().mapToInt(s -> s.nodeToPk.size()).sum();
                deferralEvents.incrementAndGet();
                currentDeferredVectors.set(deferredVectors);
                totalDeferredVectors.addAndGet(deferredVectors);
                LOGGER.log(Level.WARNING,
                        "checkpoint {0} Phase A: byte cap {1} reached; snapshotting {2} shards "
                                + "({3} vectors, ~{4} MB), deferring {5} shards ({6} vectors) to next cycle",
                        new Object[]{indexName, byteCap, capIndex, snapshotVectors,
                                snapshotVectors * bytesPerVec / (1024 * 1024),
                                allShards.size() - capIndex, deferredVectors});
            } else {
                snapshotShards = allShards;
                this.deferredShards = null;
                // No deferral needed; all shards fit within the byte cap
                currentDeferredVectors.set(0);
            }

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
            // Include deferred shards in the frozen count so Phase B back-pressure
            // is appropriately tight.
            int deferredSize = 0;
            List<LiveGraphShard> def = this.deferredShards;
            if (def != null) {
                for (LiveGraphShard s : def) {
                    deferredSize += s.nodeToPk.size();
                }
            }
            long effectiveBudget = maxVectorMemoryBytes != Long.MAX_VALUE ? maxVectorMemoryBytes
                    : (memoryBudget != null ? memoryBudget.maxMemoryBytes() : Long.MAX_VALUE);
            this.liveVectorCapDuringCheckpoint = computeLiveVectorCapDuringCheckpoint(
                    totalSnapshotSize + deferredSize, snapshotDimension, m, neighborOverflow,
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
        // Install provisional-artefact trackers BEFORE any writes so the failure
        // path can reclaim partially-written pages / multipart files instead of
        // leaking them until the next (possibly never-arriving) successful
        // checkpoint.
        this.provisionalPageIds = Collections.synchronizedList(new ArrayList<>());
        this.provisionalMultipartFiles = Collections.synchronizedList(new ArrayList<>());
        // Reset compaction progress counters so a describe-index sampled
        // mid-Phase-B reflects this checkpoint's totals, not the previous one.
        compactionNodesDone.set(0);
        compactionNodesTotal.set(0);
        uploadBytesDone.set(0);
        uploadBytesTotal.set(0);
        List<SegmentWriteResult> newSegmentResults;
        try {
            newSegmentResults = doCheckpointFusedPQPhaseB(
                    snapshotShards, snapshotDimension, sealedSegments, mergeableSegments, sequenceNumber);

            // Hook fires AFTER the heavy Phase B work but still while holding
            // the checkpoint lock, so the concurrent-tryLock-skip test can
            // park here without having to race slow PQ training on the
            // release path.
            Runnable hook = checkpointPhaseBHook;
            if (hook != null) {
                hook.run();
            }
        } catch (IOException | RuntimeException e) {
            LOGGER.log(Level.SEVERE, "checkpoint " + indexName + ": Phase B exception", e);
            rollbackProvisionalArtefacts();
            consecutiveCheckpointFailures.incrementAndGet();
            totalCheckpointFailures.incrementAndGet();
            recoverFromPhaseBFailure(snapshotShards);
            throw e;
        }

        // Phase C-prep: pre-load new segments
        List<VectorSegment> preloadedSegments = new ArrayList<>();
        try {
            if (newSegmentResults != null) {
                // persistIndexStatusMultiSegment has just bumped
                // currentIndexStatusGeneration and stamped the new segment
                // metadata with the fresh value. Read it here and apply
                // it to the in-memory VectorSegments so subsequent segment
                // authority comparisons match the persisted state.
                long freshGeneration = currentIndexStatusGeneration.get();
                for (SegmentWriteResult swr : newSegmentResults) {
                    VectorSegment seg = new VectorSegment(swr.segmentId);
                    seg.estimatedSizeBytes = swr.estimatedSizeBytes;
                    seg.graphFilePath = swr.graphFilePath;
                    seg.graphFileSize = swr.graphFileSize;
                    seg.mapFilePath = swr.mapFilePath;
                    seg.mapFileSize = swr.mapFileSize;
                    seg.generation = freshGeneration;
                    Path reloadMapFile = readMultipartMapDataToTempFile(seg);
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
            // segments, so the artefacts are now referenced on disk. We still
            // call rollbackProvisionalArtefacts as a best-effort to delete them
            // directly; any that fail to delete will be reconciled by the next
            // successful indexCheckpoint sweep.
            rollbackProvisionalArtefacts();
            consecutiveCheckpointFailures.incrementAndGet();
            totalCheckpointFailures.incrementAndGet();
            recoverFromPhaseBFailure(snapshotShards);
            throw e;
        }

        // Phase B + prep succeeded: the artefacts belong to the new persisted
        // state. Clear the trackers without deleting anything, and reset the
        // failure count.
        this.provisionalPageIds = null;
        this.provisionalMultipartFiles = null;
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

            List<VectorSegment> newSegments = new java.util.concurrent.CopyOnWriteArrayList<>();
            // Preserve existing sealed segments
            for (VectorSegment sealed : sealedSegments) {
                sealed.dirty = false;
                newSegments.add(sealed);
            }
            // Preserve existing mergeable segments (old compaction targets)
            for (VectorSegment mergeable : mergeableSegments) {
                mergeable.dirty = false;
                newSegments.add(mergeable);
            }
            // Add newly written segments from this checkpoint
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
            this.nextNodeId.set(Math.max((long) maxOrd + 1L, nextNodeId.get()));

            int totalNodes = (int) onDiskNodeToPkSize() + totalLiveSize();
            LOGGER.log(Level.INFO,
                    "checkpoint {0} Phase C: {1} nodes across {2} segments (FusedPQ), "
                            + "{3} new live inserts during checkpoint",
                    new Object[]{indexName, totalNodes, newSegments.size(), totalLiveSize()});

            // No vectorStorage cleanup needed: each shard owns its own per-shard
            // VectorStorage (issue #256). Dropping the reference to the old shards
            // via the `this.frozenShards = null` assignment below allows the GC
            // to reclaim the backing arrays without any per-slot bookkeeping.

            // Rejoin deferred shards before clearing so the next checkpoint cycle processes them.
            List<LiveGraphShard> deferred = this.deferredShards;
            if (deferred != null && !deferred.isEmpty()) {
                List<LiveGraphShard> merged = new ArrayList<>(deferred);
                merged.addAll(this.liveShards);
                this.liveShards = merged;
            }

            this.frozenShards = null;
            this.deferredShards = null;
            currentDeferredVectors.set(0);  // Deferred shards have been restored to live set
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

        long phaseBStartMs = System.currentTimeMillis();

        // Cleanup all shard builders first (finalizes HNSW diversity/refine)
        for (LiveGraphShard shard : snapshotShards) {
            if (shard.builder != null) {
                shard.builder.cleanup();
            }
        }

        // NEW: Per-shard FusedPQ write (no pooling!)
        // Each shard's already-built OnHeapGraphIndex is serialized directly.
        // This avoids the mega-pool and graph rebuild that caused the OOM.
        // Write shards in parallel for efficiency and proper error propagation.
        List<SegmentWriteResult> newSegmentResults = new ArrayList<>();
        int totalShardVectors = 0;

        // Build list of per-shard write tasks
        List<ShardWriteTask> shardTasks = new ArrayList<>();
        for (LiveGraphShard shard : snapshotShards) {
            int segId = nextSegmentId.getAndIncrement();
            shardTasks.add(new ShardWriteTask(shard, segId, snapshotDimension));
        }

        // Execute shard writes in parallel with proper error handling
        if (!shardTasks.isEmpty()) {
            int parallelism = Math.min(PHASE_B_SEGMENT_PARALLELISM, shardTasks.size());
            if (parallelism == 1) {
                // Fast path: serial execution
                for (ShardWriteTask task : shardTasks) {
                    SegmentWriteResult result = writeShardAsFusedPQSegment(
                            task.shard, task.segmentId, snapshotDimension);
                    if (result != null) {
                        newSegmentResults.add(result);
                        totalShardVectors += task.shard.nodeToPk.size();
                    }
                }
            } else {
                // Parallel execution with proper error handling
                java.util.concurrent.ExecutorService executor =
                        java.util.concurrent.Executors.newFixedThreadPool(parallelism, r -> {
                            Thread t = new Thread(r, "persistent-vector-store-phaseB-shard-" + indexName);
                            t.setDaemon(true);
                            return t;
                        });
                try {
                    List<java.util.concurrent.Future<SegmentWriteResult>> futures = new ArrayList<>();
                    for (ShardWriteTask task : shardTasks) {
                        // Capture task values locally to avoid lambda capture issues
                        LiveGraphShard shard = task.shard;
                        int segId = task.segmentId;
                        futures.add(executor.submit(() -> writeShardAsFusedPQSegment(
                                shard, segId, snapshotDimension)));
                    }

                    final int[] accumulatedShardVectors = {0};
                    Throwable firstFailure = awaitAllOrFirstFailure(futures, (i, r) -> {
                        if (r != null) {
                            newSegmentResults.add(r);
                            accumulatedShardVectors[0] += shardTasks.get(i).shard.nodeToPk.size();
                        }
                    });
                    totalShardVectors += accumulatedShardVectors[0];

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
                        throw new IOException("Phase B parallel shard write failed", firstFailure);
                    }
                } finally {
                    executor.shutdownNow();
                }
            }
        }

        // If no new segments were written (all shards empty or too small), just
        // persist the unchanged segment list and keep going — the data lives in
        // the liveShards and will be retried on the next checkpoint cycle.

        // Log per-shard write summary
        long phaseBElapsedMs = System.currentTimeMillis() - phaseBStartMs;
        lastCheckpointPhaseBDurationMs.set(phaseBElapsedMs);
        lastCheckpointVectorsProcessed.set(totalShardVectors);
        long bytesWritten = 0L;
        for (SegmentWriteResult r : newSegmentResults) {
            bytesWritten += r.graphFileSize + r.mapFileSize;
        }
        lastPhaseBBytesWritten.set(bytesWritten);
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: completed in {1} ms ({2} shard segments, {3} total vectors, {4} bytes)",
                new Object[]{indexName, phaseBElapsedMs, newSegmentResults.size(),
                        totalShardVectors, bytesWritten});

        // Order the results by segmentId so that the persisted IndexStatus and
        // the in-memory segment list are both deterministic.
        newSegmentResults.sort(java.util.Comparator.comparingInt(r -> r.segmentId));
        persistIndexStatusMultiSegment(sealedSegments, mergeableSegments, newSegmentResults, sequenceNumber);

        return newSegmentResults;
    }

    /**
     * Callback for {@link #awaitAllOrFirstFailure}: invoked once per
     * successfully-completed future with its index and the produced value.
     * Package-private so unit tests in the same package can exercise the
     * reduction helper directly.
     */
    @FunctionalInterface
    interface IndexedConsumer<T> {
        void accept(int index, T value);
    }

    /**
     * Reduces a list of {@link java.util.concurrent.Future}s: waits for each
     * in order, invokes {@code onSuccess} on values, cancels the remaining
     * futures as soon as one throws {@link java.util.concurrent.ExecutionException},
     * and returns the first real failure (or {@code null} if all succeeded).
     *
     * <p>A later future observed as cancelled (because this helper cancelled
     * it after an earlier failure) does not overwrite the recorded
     * {@code firstFailure}: the {@link java.util.concurrent.CancellationException}
     * is kept only as a last-resort fallback so that the real root cause
     * survives (issue #234). Same treatment for {@link InterruptedException}.
     *
     * <p>Package-private for testing.
     */
    static <T> Throwable awaitAllOrFirstFailure(
            List<java.util.concurrent.Future<T>> futures,
            IndexedConsumer<T> onSuccess) {
        Throwable firstFailure = null;
        for (int i = 0; i < futures.size(); i++) {
            try {
                T value = futures.get(i).get();
                onSuccess.accept(i, value);
            } catch (java.util.concurrent.ExecutionException ee) {
                if (firstFailure == null) {
                    firstFailure = ee.getCause() != null ? ee.getCause() : ee;
                }
                for (int j = i + 1; j < futures.size(); j++) {
                    futures.get(j).cancel(true);
                }
            } catch (java.util.concurrent.CancellationException ce) {
                if (firstFailure == null) {
                    firstFailure = ce;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                if (firstFailure == null) {
                    firstFailure = ie;
                }
            }
        }
        return firstFailure;
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

    /** Task descriptor: one shard to write in Phase B (per-shard FusedPQ approach). */
    private static final class ShardWriteTask {
        final LiveGraphShard shard;
        final int segmentId;
        final int snapshotDimension;

        ShardWriteTask(LiveGraphShard shard, int segmentId, int snapshotDimension) {
            this.shard = shard;
            this.segmentId = segmentId;
            this.snapshotDimension = snapshotDimension;
        }
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
     * Zero-copy view over a single shard's per-shard {@link VectorStorage}.
     * Maps ordinal i (0-based within shard) directly to {@code storage.get(i)}.
     * Implements RandomAccessVectorValues for use with PQ training and OnDiskGraphIndexWriter.
     */
    private static final class VectorStorageShardView implements io.github.jbellis.jvector.graph.RandomAccessVectorValues {
        private final VectorStorage storage;
        private final int size;
        private final int dimension;

        VectorStorageShardView(VectorStorage storage, int size, int dimension) {
            this.storage = storage;
            this.size = size;
            this.dimension = dimension;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public VectorFloat<?> getVector(int i) {
            return storage.get(i);
        }

        @Override
        public boolean isValueShared() {
            return true;
        }

        @Override
        public VectorStorageShardView copy() {
            return this;
        }
    }

    /**
     * Returns the shard's {@code nodeToPk} map — already keyed by the
     * shard-relative local ordinal after the issue-#256 refactor.
     */
    private ConcurrentHashMap<Integer, Bytes> buildOrdinalToPk(LiveGraphShard shard) {
        return shard.nodeToPk;
    }

    /**
     * Returns a PQ codebook to use for encoding a FusedPQ segment.
     *
     * <p>When {@link #pqCodebookRetrainingInterval} is {@code > 0} and a
     * compatible codebook is cached (same dimension, fewer than
     * {@code pqCodebookRetrainingInterval} segments written since the last
     * training), the cached codebook is returned immediately.
     *
     * <p>When retraining is needed and a cached codebook with a matching dimension
     * exists, {@link ProductQuantization#refine(RandomAccessVectorValues)} is used
     * instead of a full {@link ProductQuantization#compute} call.  {@code refine}
     * warm-starts from the existing centroids and runs a single Lloyd's iteration,
     * which is sufficient when the vector distribution changes slowly between
     * retraining intervals (typical for streaming ingestion workloads).  This
     * reduces K-Means iteration count from {@link ProductQuantization#K_MEANS_ITERATIONS}
     * (default 6) to 1, cutting per-retraining CPU and allocation cost by ~6×.
     *
     * <p>When no cached codebook exists (first training or dimension change), a full
     * K-Means run with K-Means++ initialisation is used.
     *
     * <p>Concurrent calls from parallel Phase B shard writers are safe: a benign
     * race at the retraining boundary may trigger one extra training, which does
     * not affect correctness.
     *
     * @param ravv        vectors to train on (only consulted when training is required)
     * @param pqSubspaces number of PQ subspaces ({@code M})
     * @return the PQ codebook to use; always non-null
     */
    private ProductQuantization getOrTrainPQ(RandomAccessVectorValues ravv, int pqSubspaces) {
        int interval = pqCodebookRetrainingInterval;
        if (interval > 0) {
            ProductQuantization existing = this.cachedPQ;
            if (existing != null
                    && existing.getOriginalDimension() == ravv.dimension()
                    && pqSegmentsSinceTraining.get() < interval) {
                pqSegmentsSinceTraining.incrementAndGet();
                LOGGER.log(Level.FINE,
                        "checkpoint {0}: reusing cached PQ codebook "
                                + "(segments since training: {1}/{2})",
                        new Object[]{indexName, pqSegmentsSinceTraining.get(), interval});
                return existing;
            }
        }
        ProductQuantization existing = this.cachedPQ;
        ProductQuantization pq;
        if (existing != null && existing.getOriginalDimension() == ravv.dimension()) {
            // Warm-start from the cached codebook: 1 Lloyd's iteration instead of
            // K_MEANS_ITERATIONS full iterations with K-Means++ initialisation.
            // Distribution drift over pqCodebookRetrainingInterval segments is small,
            // so one refinement step keeps the codebook accurate at ~1/6 of the cost.
            LOGGER.log(Level.INFO,
                    "checkpoint {0}: refining PQ codebook "
                            + "(training #{1}, segments since last: {2})",
                    new Object[]{indexName, pqTrainingsTotal.get() + 1,
                            pqSegmentsSinceTraining.get()});
            pq = existing.refine(ravv);
        } else {
            // No cached codebook (first training or dimension change): full K-Means.
            LOGGER.log(Level.INFO,
                    "checkpoint {0}: training PQ codebook from scratch "
                            + "(training #{1}, segments since last: {2})",
                    new Object[]{indexName, pqTrainingsTotal.get() + 1,
                            pqSegmentsSinceTraining.get()});
            pq = ProductQuantization.compute(ravv, pqSubspaces, 256, true);
        }
        pqTrainingsTotal.incrementAndGet();
        this.cachedPQ = pq;
        pqSegmentsSinceTraining.set(1);
        return pq;
    }

    /**
     * Serializes a single live shard's already-built OnHeapGraphIndex to FusedPQ format.
     * The shard's builder must already have been cleaned up (shard.builder.cleanup() called).
     *
     * Ordinals in the shard graph are 0-based (nodeId - shard.startNodeId), so
     * OnDiskGraphIndexWriter can consume the graph directly without remapping.
     *
     * @return SegmentWriteResult for this shard's segment, or null if fallback to simple format
     */
    private SegmentWriteResult writeShardAsFusedPQSegment(
            LiveGraphShard shard,
            int segmentId,
            int snapshotDimension) throws IOException, DataStorageManagerException {

        int shardSize = shard.nodeToPk.size();
        if (shardSize == 0) {
            // Empty shard, skip it
            return null;
        }

        // Update compaction metrics for this shard
        compactionNodesTotal.addAndGet(shardSize);

        writingGraphActive.incrementAndGet();
        try {
            // Create a zero-copy view over this shard's per-shard VectorStorage.
            VectorStorageShardView shardView = new VectorStorageShardView(
                    shard.vectorStorage, shardSize, snapshotDimension);

            // Get the shard's already-built OnHeapGraphIndex (no rebuild!)
            OnHeapGraphIndex shardGraph = (OnHeapGraphIndex) shard.builder.getGraph();

            // Determine whether FusedPQ is used for this shard.
            // Small shards (< MIN_VECTORS_FOR_FUSED_PQ) do not write the FusedPQ
            // feature, so PQ training is skipped entirely for them (it was
            // computed but unused in the original code — issue #281).
            int pqSubspaces = Math.max(1, snapshotDimension / 4);
            boolean useFusedPQForShard = shardSize >= MIN_VECTORS_FOR_FUSED_PQ;
            // For FusedPQ-eligible shards, reuse the cached codebook when possible
            // instead of re-running K-Means from scratch (issue #281).
            ProductQuantization pq = useFusedPQForShard
                    ? getOrTrainPQ(shardView, pqSubspaces) : null;
            PQVectors pqv = (pq != null) ? pq.encodeAll(shardView, PhysicalCoreExecutor.pool()) : null;

            // Write graph + features to temp file, streaming via suppliers
            Path tempFile = Files.createTempFile(
                    tmpDirectory, "herddb-vector-shard-", ".idx");
            boolean success = false;
            try {
                OnDiskGraphIndexWriter.Builder builder = new OnDiskGraphIndexWriter.Builder(
                        shardGraph, tempFile);
                if (useFusedPQForShard) {
                    builder.with(new FusedPQ(shardGraph.maxDegree(), pq));
                }
                try (OnDiskGraphIndexWriter writer = builder
                        .with(new InlineVectors(snapshotDimension))
                        .build()) {
                    ImmutableGraphIndex.View view = shardGraph.getView();
                    EnumMap<FeatureId, IntFunction<io.github.jbellis.jvector.graph.disk.feature.Feature.State>> suppliers =
                            new EnumMap<>(FeatureId.class);
                    if (useFusedPQForShard) {
                        suppliers.put(FeatureId.FUSED_PQ,
                                ordinal -> new FusedPQ.State(view, pqv, ordinal));
                    }
                    suppliers.put(FeatureId.INLINE_VECTORS,
                            ordinal -> new InlineVectors.State(shardView.getVector(ordinal)));
                    writer.write(suppliers);
                }
                success = true;

                // Upload graph file
                long graphSize = Files.size(tempFile);
                uploadBytesTotal.addAndGet(graphSize);
                uploadingActive.incrementAndGet();
                String graphFilePath;
                String segUuid = indexUUID + "_seg" + segmentId;
                try {
                    graphFilePath = dataStorageManager.writeMultipartIndexFile(
                            tableSpaceUUID,
                            segUuid, "graph",
                            tempFile, uploadBytesDone::addAndGet);
                } finally {
                    uploadingActive.decrementAndGet();
                }
                trackProvisionalMultipartFile(segUuid, "graph");

                ConcurrentHashMap<Integer, Bytes> ordinalToPk = buildOrdinalToPk(shard);
                Path mapFile = writeFusedPQMapDataToTempFile(shardView, ordinalToPk);
                try {
                    long mapSize = Files.size(mapFile);
                    uploadBytesTotal.addAndGet(mapSize);
                    uploadingActive.incrementAndGet();
                    String mapFilePath;
                    try {
                        mapFilePath = dataStorageManager.writeMultipartIndexFile(
                                tableSpaceUUID,
                                segUuid, "map",
                                mapFile, uploadBytesDone::addAndGet);
                    } finally {
                        uploadingActive.decrementAndGet();
                    }
                    trackProvisionalMultipartFile(segUuid, "map");
                    compactionNodesDone.addAndGet(shardSize);
                    return new SegmentWriteResult(segmentId,
                            graphFilePath, graphSize,
                            mapFilePath, mapSize,
                            graphSize + mapSize);
                } finally {
                    Files.deleteIfExists(mapFile);
                }
            } finally {
                // Always delete temp file - it's been uploaded/written to persistent storage
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "error deleting temp file for " + indexName, e);
                }
                // Intentionally do NOT close shard.builder here. The builder is
                // still referenced by `frozenShards` and is being iterated by
                // concurrent searches (searchInternal reads frozen shards while
                // Phase B runs). Closing it now would race with those searches
                // and crash with NPE/use-after-free on the jvector graph view
                // (issue #235). Phase C closes every snapshotShard builder
                // atomically under stateLock.writeLock(), which is the safe
                // point because no search holds the read lock at that moment.
            }
        } finally {
            writingGraphActive.decrementAndGet();
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
            Throwable firstFailure = awaitAllOrFirstFailure(futures, (i, r) -> {
                results.add(r);
                // Release slice references once the segment is durable.
                releaseSliceReferences(slices.get(i), poolVectorsList, poolPkList);
            });
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
                "checkpoint {0} Phase B: segment {1}/{2} ({3} nodes) written in {4} ms",
                new Object[]{indexName, s.oneBasedIndex, totalSegments, s.size(), segElapsedMs});

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
     * Deletes any artefacts tracked in {@link #provisionalPageIds} and
     * {@link #provisionalMultipartFiles}. Called when Phase B or Phase C-prep
     * aborts before the new state becomes durable so that the partially-written
     * pages / multipart files do not linger on disk until the next successful
     * {@code indexCheckpoint} sweep (which may never arrive if the disk is full).
     *
     * <p>The method always clears both trackers, whether or not any deletes
     * succeeded. Delete failures are logged but not rethrown — the failure has
     * already been signalled via the original Phase B exception.
     *
     * <p>The {@link #totalRolledBackPages} / {@link #lastRolledBackPages} metrics
     * count both page-based and multipart-file rollbacks as a single "rolled-back
     * artefact" unit.
     */
    private void rollbackProvisionalArtefacts() {
        List<Long> pageTracker = this.provisionalPageIds;
        List<String[]> multipartTracker = this.provisionalMultipartFiles;
        this.provisionalPageIds = null;
        this.provisionalMultipartFiles = null;

        int rolled = 0;
        int failed = 0;
        if (pageTracker != null && !pageTracker.isEmpty()) {
            List<Long> snapshot;
            synchronized (pageTracker) {
                snapshot = new ArrayList<>(pageTracker);
            }
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
        }
        if (multipartTracker != null && !multipartTracker.isEmpty()) {
            List<String[]> snapshot;
            synchronized (multipartTracker) {
                snapshot = new ArrayList<>(multipartTracker);
            }
            for (String[] entry : snapshot) {
                try {
                    dataStorageManager.deleteMultipartIndexFile(entry[0], entry[1], entry[2]);
                    rolled++;
                } catch (DataStorageManagerException e) {
                    failed++;
                    LOGGER.log(Level.WARNING,
                            "checkpoint " + indexName + ": failed to delete provisional multipart file "
                                    + entry[0] + "/" + entry[1] + "/" + entry[2], e);
                } catch (RuntimeException e) {
                    // UnsupportedOperationException from read-replica / BK backends etc.
                    // We should never reach here with the current backends, but be
                    // defensive: a broken delete must not crash the rollback path.
                    failed++;
                    LOGGER.log(Level.WARNING,
                            "checkpoint " + indexName + ": failed to delete provisional multipart file "
                                    + entry[0] + "/" + entry[1] + "/" + entry[2], e);
                }
            }
        }
        totalRolledBackPages.addAndGet(rolled);
        lastRolledBackPages.set(rolled);
        LOGGER.log(Level.WARNING,
                "checkpoint {0}: rolled back {1} provisional artefacts ({2} delete failures)",
                new Object[]{indexName, rolled, failed});
    }

    /**
     * Records a successfully-written multipart file in the in-flight provisional
     * tracker (if one is installed). Called by the Phase B segment writers so
     * that a mid-Phase-B abort can reclaim the partially-written artefacts.
     */
    private void trackProvisionalMultipartFile(String uuidWithSeg, String fileType) {
        List<String[]> tracker = this.provisionalMultipartFiles;
        if (tracker != null) {
            tracker.add(new String[]{tableSpaceUUID, uuidWithSeg, fileType});
        }
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

            // After the issue-#256 refactor each shard owns its own per-shard
            // VectorStorage keyed by a LOCAL ordinal, so we can preserve
            // Phase B inserts simply by concatenating {snapshotShards,
            // currentShards} into the new liveShards list — no cross-shard
            // replay, no globalNodeId → local-ordinal remapping, and no risk
            // of overflowing lastSnapshot's cap by piling vectors into it.
            // `lastSnapshot` reference is retained only for the log below.
            for (LiveGraphShard currentShard : currentShards) {
                // jvector builders for currentShards remain open (initEmptyLiveShards
                // at Phase A entry leaves the fresh shards' builders live),
                // so nothing to close here. snapshotShards' builders are also
                // still open — writeShardAsFusedPQSegment deliberately does
                // not close them during Phase B (see its finally block).
                if (currentShard.nodeToPk.isEmpty() && currentShard.builder != null) {
                    try {
                        currentShard.builder.close();
                    } catch (IOException e) {
                        // ignore: empty-shard cleanup only
                    }
                }
            }
            // Tolerate an unused lastSnapshot reference when logging is disabled.
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE,
                        "checkpoint {0}: Phase B recovery concatenating {1} snapshot + {2} current shards (last snapshot startNodeId={3})",
                        new Object[]{indexName, snapshotShards.size(), currentShards.size(),
                                lastSnapshot.startNodeId});
            }

            List<LiveGraphShard> rebuilt = new ArrayList<>(snapshotShards.size() + currentShards.size());
            rebuilt.addAll(snapshotShards);
            for (LiveGraphShard cur : currentShards) {
                if (!cur.nodeToPk.isEmpty()) {
                    rebuilt.add(cur);
                }
            }
            this.liveShards = rebuilt;
            // Restore deferred shards so they rejoin the live set intact.
            List<LiveGraphShard> deferred = this.deferredShards;
            if (deferred != null) {
                this.liveShards.addAll(deferred);
            }
            this.frozenShards = null;
            this.deferredShards = null;
            currentDeferredVectors.set(0);  // Deferred shards have been restored to live set
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

        // nextNodeId is serialised as int64 after issue #256 — the in-place
        // v3 rewrite breaks backward-compatibility on purpose.
        long savedNextNodeId = metaBuf.getLong();

        this.dimension = dim;
        newPageId.set(status.newPageId);

        loadMultiSegmentFormat(metaBuf, dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
    }

    private void loadMultiSegmentFormat(ByteBuffer metaBuf, int dim, long savedNextNodeId,
                                         int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException, DataStorageManagerException {
        java.io.DataInputStream dis = new java.io.DataInputStream(
                new java.io.ByteArrayInputStream(metaBuf.array(), metaBuf.position(),
                        metaBuf.remaining()));
        long loadedGeneration;
        int numSegments;
        try {
            loadedGeneration = dis.readLong();
            numSegments = dis.readInt();
        } catch (IOException e) {
            throw new DataStorageManagerException("Failed to read segment count", e);
        }

        currentIndexStatusGeneration.set(loadedGeneration);

        if (dim == 0 || numSegments == 0) {
            loadPendingDeletes(dis);
            LOGGER.log(Level.INFO, "vector store {0} is empty (multi-segment), no load needed", indexName);
            return;
        }

        int maxSegId = -1;
        long maxGeneration = loadedGeneration;
        for (int s = 0; s < numSegments; s++) {
            int segId;
            long estimatedSize;
            String graphFilePath;
            long graphFileSize;
            String mapFilePath;
            long mapFileSize;
            long generation;
            try {
                segId = dis.readInt();
                estimatedSize = dis.readLong();
                graphFilePath = dis.readUTF();
                graphFileSize = dis.readLong();
                mapFilePath = dis.readUTF();
                mapFileSize = dis.readLong();
                generation = dis.readLong();
            } catch (IOException e) {
                throw new DataStorageManagerException("Failed to read segment metadata", e);
            }

            VectorSegment seg = new VectorSegment(segId);
            seg.estimatedSizeBytes = estimatedSize;
            seg.graphFilePath = graphFilePath;
            seg.graphFileSize = graphFileSize;
            seg.mapFilePath = mapFilePath.isEmpty() ? null : mapFilePath;
            seg.mapFileSize = mapFileSize;
            seg.generation = generation;

            Path mapFile = readMultipartMapDataToTempFile(seg);
            loadFusedPQSegment(seg, mapFile, dim, savedNextNodeId);

            segments.add(seg);
            if (segId > maxSegId) {
                maxSegId = segId;
            }
            if (generation > maxGeneration) {
                maxGeneration = generation;
            }
        }
        nextSegmentId.set(maxSegId + 1);
        currentIndexStatusGeneration.set(Math.max(currentIndexStatusGeneration.get(), maxGeneration));

        loadPendingDeletes(dis);

        int maxOrd = -1;
        for (VectorSegment seg : segments) {
            if (seg.maxOrdinal > maxOrd) {
                maxOrd = seg.maxOrdinal;
            }
        }
        // Prefer the persisted long nextNodeId when it is ahead of the
        // reconstructed maxOrd — this preserves the global monotonic
        // counter across restarts (issue #256).
        this.nextNodeId.set(Math.max((long) maxOrd + 1L, savedNextNodeId));

        initEmptyLiveShards(dim, savedBeamWidth, savedNeighborOverflow, savedAlpha);

        LOGGER.log(Level.INFO,
                "loaded vector store {0} (multi-segment): {1} segments, dimension {2}, "
                        + "generation {3}, pendingDeletes {4}",
                new Object[]{indexName, numSegments, dim,
                        currentIndexStatusGeneration.get(), pendingDeletes.size()});
    }

    /**
     * Reads the pendingDeletes list from the tail of the metadata stream.
     * Appends entries into {@link #pendingDeletes}.
     */
    private void loadPendingDeletes(java.io.DataInputStream dis) throws DataStorageManagerException {
        int pendingCount;
        try {
            pendingCount = dis.readInt();
        } catch (IOException e) {
            throw new DataStorageManagerException("Failed to read pendingDeletes count", e);
        }
        for (int i = 0; i < pendingCount; i++) {
            String filePath;
            long deadlineMs;
            long sinceGeneration;
            try {
                filePath = dis.readUTF();
                deadlineMs = dis.readLong();
                sinceGeneration = dis.readLong();
            } catch (IOException e) {
                throw new DataStorageManagerException("Failed to read pendingDeletes entry", e);
            }
            pendingDeletes.add(new PendingDelete(filePath, deadlineMs, sinceGeneration));
        }
    }

    /**
     * Loads a single FusedPQ segment. The map data is read from a one-shot
     * temp file (linear scan, then deleted). The graph is served on demand
     * through a {@link ReaderSupplier} backed by the storage manager's
     * multipart API — no resident copy on disk.
     */
    private void loadFusedPQSegment(VectorSegment seg, Path mapFile, int dim, long savedNextNodeId)
            throws IOException, DataStorageManagerException {
        if (seg.graphFilePath == null) {
            throw new IllegalStateException(
                    "loadFusedPQSegment requires seg.graphFilePath to be populated");
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

        ReaderSupplier readerSupplier = dataStorageManager.multipartIndexReaderSupplier(
                tableSpaceUUID,
                indexUUID + "_seg" + seg.segmentId,
                "graph",
                seg.graphFileSize);
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
     * Writes the graph and map data for one segment via the multipart API of
     * the underlying {@link herddb.storage.DataStorageManager}.
     */
    private SegmentWriteResult writeOneSegmentData(
            SegmentSlice s,
            ConcurrentHashMap<Integer, VectorFloat<?>> partVectors,
            ConcurrentHashMap<Integer, Bytes> partNodeToPk,
            VectorStorage partStorage,
            int snapshotDimension)
            throws IOException, DataStorageManagerException {

        Path graphTempFile = writeFusedPQGraphToTempFile(partVectors, partNodeToPk, snapshotDimension);
        try {
            long graphFileSize = Files.size(graphTempFile);
            uploadBytesTotal.addAndGet(graphFileSize);
            String graphFilePath;
            String segUuid = indexUUID + "_seg" + s.segmentId;
            uploadingActive.incrementAndGet();
            try {
                graphFilePath = dataStorageManager.writeMultipartIndexFile(
                        tableSpaceUUID,
                        segUuid,
                        "graph",
                        graphTempFile,
                        uploadBytesDone::addAndGet);
            } finally {
                uploadingActive.decrementAndGet();
            }
            trackProvisionalMultipartFile(segUuid, "graph");

            Path mapTempFile = writeFusedPQMapDataToTempFile(
                    new VectorStorageRandomAccessVectorValues(partStorage, snapshotDimension),
                    partNodeToPk);
            try {
                long mapFileSize = Files.size(mapTempFile);
                uploadBytesTotal.addAndGet(mapFileSize);
                String mapFilePath;
                uploadingActive.incrementAndGet();
                try {
                    mapFilePath = dataStorageManager.writeMultipartIndexFile(
                            tableSpaceUUID,
                            segUuid,
                            "map",
                            mapTempFile,
                            uploadBytesDone::addAndGet);
                } finally {
                    uploadingActive.decrementAndGet();
                }
                trackProvisionalMultipartFile(segUuid, "map");
                return new SegmentWriteResult(s.segmentId,
                        graphFilePath, graphFileSize,
                        mapFilePath, mapFileSize,
                        graphFileSize + mapFileSize);
            } finally {
                Files.deleteIfExists(mapTempFile);
            }
        } finally {
            Files.deleteIfExists(graphTempFile);
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
        writingGraphActive.incrementAndGet();
        try {
            int totalVectors = allNodeToPk.size();
            compactionNodesTotal.addAndGet(totalVectors);
            VectorStorage allStorage = new VectorStorage(allVectors.size());
            allVectors.forEach(allStorage::set);
            VectorStorageRandomAccessVectorValues allMravv =
                    new VectorStorageRandomAccessVectorValues(allStorage, dim, allVectors.size());
            BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(allMravv, similarityFunction);
            // Pre-size the base-layer DenseIntMap to totalVectors so the parallel insert
            // below never hits the spine-grow lock in jvector (issue #223).
            GraphIndexBuilder mergedBuilder = new GraphIndexBuilder(
                    bsp, dim, List.of(m), beamWidth, neighborOverflow, alpha,
                    ADD_HIERARCHY, REFINE_FINAL_GRAPH,
                    PhysicalCoreExecutor.pool(), CHECKPOINT_POOL, totalVectors);

            int progressInterval = Math.max(1000, totalVectors / 10);
            java.util.concurrent.ForkJoinTask<?> graphTask = CHECKPOINT_POOL.submit(() ->
                    allVectors.entrySet().parallelStream()
                        .filter(e -> allNodeToPk.containsKey(e.getKey()))
                        .forEach(e -> {
                            mergedBuilder.addGraphNode(e.getKey(), e.getValue());
                            long count = compactionNodesDone.incrementAndGet();
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
        } finally {
            writingGraphActive.decrementAndGet();
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
        Map<Integer, Integer> oldToNew = new java.util.HashMap<>(sortedNodeIds.size() * 2);  // avoid rehashing (issue #122)
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
                    if (vec == null) {
                        throw new IOException("writeFusedPQMapDataToTempFile: null vector at ordinal " + oldId);
                    }
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

    // -------------------------------------------------------------------------
    // Metadata persistence
    // -------------------------------------------------------------------------

    private void persistIndexStatusMultiSegment(
            List<VectorSegment> sealedSegments, List<VectorSegment> mergeableSegments,
            List<SegmentWriteResult> newSegmentResults,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {

        int totalSegments = sealedSegments.size() + mergeableSegments.size() + newSegmentResults.size();
        Set<Long> activePages = new HashSet<>();

        // Allocate the next generation; freshly-written segments get stamped with it.
        // Existing (sealed/mergeable) segments keep their stored generation.
        long newGeneration = currentIndexStatusGeneration.get() + 1;

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
            // nextNodeId widened to int64 after issue #256. Format version
            // stays at v3 — the loader refuses unknown versions, so mixing
            // old + new clients on the same checkpoint directory fails loud
            // at load time rather than silently truncating the counter.
            dos.writeLong(nextNodeId.get());
            dos.writeLong(newGeneration);
            dos.writeInt(totalSegments);

            for (VectorSegment seg : sealedSegments) {
                writeSegmentMeta(dos, seg.segmentId, seg.estimatedSizeBytes,
                        seg.graphFilePath, seg.graphFileSize,
                        seg.mapFilePath, seg.mapFileSize, seg.generation);
            }
            for (VectorSegment seg : mergeableSegments) {
                writeSegmentMeta(dos, seg.segmentId, seg.estimatedSizeBytes,
                        seg.graphFilePath, seg.graphFileSize,
                        seg.mapFilePath, seg.mapFileSize, seg.generation);
            }
            for (SegmentWriteResult swr : newSegmentResults) {
                writeSegmentMeta(dos, swr.segmentId, swr.estimatedSizeBytes,
                        swr.graphFilePath, swr.graphFileSize,
                        swr.mapFilePath, swr.mapFileSize, newGeneration);
            }

            List<PendingDelete> snapshotPending = new ArrayList<>(pendingDeletes);
            dos.writeInt(snapshotPending.size());
            for (PendingDelete pd : snapshotPending) {
                dos.writeUTF(pd.filePath);
                dos.writeLong(pd.deadlineMs);
                dos.writeLong(pd.sinceGeneration);
            }
        } catch (IOException e) {
            throw new DataStorageManagerException("Failed to serialize index metadata", e);
        }

        IndexStatus indexStatus = new IndexStatus(
                indexName, sequenceNumber,
                newPageId.get(), activePages, baos.toByteArray());

        dataStorageManager.indexCheckpoint(tableSpaceUUID, indexUUID, indexStatus, false);

        currentIndexStatusGeneration.set(newGeneration);
    }

    private static void writeSegmentMeta(
            java.io.DataOutputStream dos,
            int segmentId, long estimatedSizeBytes,
            String graphFilePath, long graphFileSize,
            String mapFilePath, long mapFileSize,
            long generation) throws IOException {
        dos.writeInt(segmentId);
        dos.writeLong(estimatedSizeBytes);
        dos.writeUTF(graphFilePath);
        dos.writeLong(graphFileSize);
        dos.writeUTF(mapFilePath != null ? mapFilePath : "");
        dos.writeLong(mapFileSize);
        dos.writeLong(generation);
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
     *
     * <p>The HNSW graph-overhead term is delegated to jvector's static helper
     * {@link OnHeapGraphIndex#estimatedBytesPerNode(int, float)} so the estimate
     * stays in sync with the actual per-node footprint (e.g. future changes to
     * {@code ConcurrentNeighborMap} / {@code Neighbors} layout).
     */
    static long estimatedBytesPerVector(int dim, int m, float neighborOverflow) {
        long graphBytesPerNode = OnHeapGraphIndex.estimatedBytesPerNode(m, neighborOverflow);
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
    private LiveGraphShard createEmptyLiveShard(int dim, int bw, float no, float a, long startNodeId) {
        int cap = computeEffectiveMaxLiveGraphSize();  // preallocate to avoid rehashing during inserts (issue #122)
        ConcurrentHashMap<Bytes, Integer> p2n = new ConcurrentHashMap<>(cap);
        ConcurrentHashMap<Integer, Bytes> n2p = new ConcurrentHashMap<>(cap);
        // Per-shard VectorStorage — sized to the shard cap so the int-indexed
        // backing array is bounded independent of nextNodeId (issue #256).
        VectorStorage storage = new VectorStorage(cap);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, dim);
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction);
        // Pass cap as initialCapacity so the jvector base-layer DenseIntMap is pre-sized
        // for the shard and concurrent addGraphNode avoids the spine-grow lock (issue #223).
        GraphIndexBuilder b = new GraphIndexBuilder(
                bsp, dim, List.of(m), bw, no, a, ADD_HIERARCHY, REFINE_FINAL_GRAPH,
                PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool(), cap);
        return new LiveGraphShard(p2n, n2p, ravv, b, storage, startNodeId);
    }

    /**
     * Publishes {@code candidate} as the new active live shard if the current
     * active shard is still full.
     *
     * <p>The caller is responsible for building {@code candidate} <em>outside</em>
     * any lock (jvector {@link GraphIndexBuilder} initialisation is the dominant
     * cost).  Only the publish — a volatile swap of {@link #liveShards} — happens
     * under {@code stateLock.writeLock()}, which keeps the write-lock critical
     * section small (issue #282).
     *
     * <p>Under burst contention up to K threads may concurrently build a candidate
     * shard; K-1 are discarded by the double-check inside the write lock.  That
     * trades a small amount of wasted allocation at rotation time for eliminating
     * the long hold while the builder is initialised.
     *
     * @param candidate a freshly built (empty) live shard
     */
    private void rotateLiveShard(LiveGraphShard candidate) {
        int cap = computeEffectiveMaxLiveGraphSize();
        stateLock.writeLock().lock();
        try {
            List<LiveGraphShard> cur = this.liveShards;
            LiveGraphShard curActive = cur.get(cur.size() - 1);
            if (curActive.nodeToPk.size() < cap) {
                // Another thread won the race and already published a new shard.
                return;
            }
            List<LiveGraphShard> newList = new ArrayList<>(cur);
            newList.add(candidate);
            this.liveShards = newList;
            LOGGER.log(Level.INFO,
                    "vector store {0}: rotated live graph shard, now {1} shards ({2} vectors in sealed shard)",
                    new Object[]{indexName, newList.size(), curActive.nodeToPk.size()});
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    private void initEmptyLiveShards(int dim, int bw, float no, float a) {
        LiveGraphShard shard = createEmptyLiveShard(dim, bw, no, a);
        this.liveShards = new ArrayList<>(Collections.singletonList(shard));
    }

    /**
     * Initialises the first live shard if the store has not yet seen any
     * vectors.  The caller must build {@code candidate} outside any lock;
     * only the publish (volatile swap of {@link #liveShards} and assignment
     * of {@link #dimension}) happens under {@code stateLock.writeLock()},
     * keeping the write-lock critical section small (issue #282).
     *
     * @param dim       the vector dimension detected from the first insert
     * @param candidate a freshly built (empty) live shard for {@code dim}
     */
    private void initBuilderForDimension(int dim, LiveGraphShard candidate) {
        stateLock.writeLock().lock();
        try {
            if (this.dimension == 0) {
                this.liveShards = new ArrayList<>(Collections.singletonList(candidate));
                this.dimension = dim;
            }
        } finally {
            stateLock.writeLock().unlock();
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

    private static List<Integer> toIntList(int[] arr) {
        List<Integer> list = new ArrayList<>(arr.length);
        for (int v : arr) {
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
     * Returns the current value of the global monotonic node-id counter.
     * Exposed for telemetry — dashboards can watch the burn rate and
     * alert long before the {@code long} space is exhausted (issue #256).
     */
    public long getNextNodeId() {
        return nextNodeId.get();
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

    public long getTotalCheckpointsDeferred() {
        return totalCheckpointsDeferred.get();
    }

    public long getLastSuccessfulCheckpointMs() {
        return lastSuccessfulCheckpointMs;
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

    /**
     * Test-only hook: fast-forward the global node-id counter past a
     * configured threshold so unit tests can exercise the {@code long}
     * overflow path without ingesting {@code 2^31} vectors (issue #256).
     * Must be called before any {@code addVector} in the test so the
     * first live shard's {@code startNodeId} adopts the seeded value.
     */
    public void seedNextNodeIdForTest(long value) {
        nextNodeId.set(value);
    }

    /**
     * Test-only hook: returns the active live shard (last in the list),
     * so tests can assert per-shard storage bounds. {@code null} if no
     * shard has been initialised yet.
     */
    public LiveGraphShard activeLiveShardForTest() {
        List<LiveGraphShard> shards = this.liveShards;
        if (shards == null || shards.isEmpty()) {
            return null;
        }
        return shards.get(shards.size() - 1);
    }

    /**
     * Test-only hook: returns a snapshot of all live shards so that tests
     * can assert shard count and per-shard invariants at rotation boundaries.
     */
    public List<LiveGraphShard> allLiveShardsForTest() {
        return new ArrayList<>(this.liveShards);
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
