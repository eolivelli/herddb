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

package herddb.indexing.vector;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.MemoryManager;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkIndexDataStorage;
import herddb.index.blink.BytesLongSizeEvaluator;
import herddb.index.blink.IncrementalBLinkPageCodec;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.ByteBufDataOutput;
import herddb.utils.Bytes;
import herddb.utils.VectorSearchRequestContext;
import herddb.utils.VisibleByteArrayOutputStream;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.RandomAccessOnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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
import herddb.index.vector.PinModeReaderSupplier;
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
     * {@code herddb.vectorindex.maxCheckpointDeferralMs}. Default: 10 min
     * (600 s) — wide enough to cover one full segment-count back-pressure
     * safety-timeout cycle ({@code compactionBackpressureMaxWaitMs}, default
     * 5 min) so a single timeout no longer forces a micro-segment checkpoint
     * (issue #562).
     * Non-final to let unit tests override after class load; production
     * code should only read, never write.
     */
    public static volatile long maxCheckpointDeferralMs =
            Math.max(0L, Long.getLong(
                    "herddb.vectorindex.maxCheckpointDeferralMs", 600_000L));

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
     * Multi-segment multipart metadata format, version 4. Carries per-segment
     * {@code generation}, a per-IndexStatus monotonic generation counter, a
     * {@code pendingDeletes} list driving the graph-merge compaction retention
     * protocol, and a mandatory per-segment {@code segmentUuid} string used by
     * the external optimizer's ZooKeeper segment registry.
     *
     * <p>Version 3 (which lacked the {@code segmentUuid} field) is no longer
     * supported for reading or writing. Any attempt to load a v3 index will
     * result in a clear boot failure with instructions to delete and re-create
     * the index.
     *
     * <p>V4 is now read-only-legacy: the writer always emits V5
     * ({@link #METADATA_VERSION_MULTI_SEGMENT_V5}). Existing V4 payloads are
     * still accepted on read; they receive {@code externalStorageKey = null}
     * per segment, which is correct since V4 was written before the adoption
     * feature existed.
     */
    private static final int METADATA_VERSION_MULTI_SEGMENT_V4 = 4;
    /**
     * V5 extends V4 with one additional field per segment: the
     * {@link VectorSegment#externalStorageKey} (written as a UTF string, empty
     * string = null). Required so that optimizer-adopted segments (which use a
     * 63-bit long-based storage key rather than the IS-local int-based key) can
     * be reopened correctly after a restart. Backward-compatible: V4 payloads are
     * still accepted; segments loaded from V4 get {@code externalStorageKey = null}
     * (correct, since V4 was written before the adoption feature existed).
     */
    private static final int METADATA_VERSION_MULTI_SEGMENT_V5 = 5;

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

    /**
     * PKs deleted during Phase B.
     *
     * <p>Backed by a {@link PagedPkSet} (BLink-paged storage with the existing
     * {@link MemoryManager} page replacement policy) so memory stays bounded
     * even when sustained delete traffic arrives during a multi-minute
     * checkpoint cycle (issue #290).
     */
    private volatile PagedPkSet pendingCheckpointDeletes;

    /** Max live vectors allowed during Phase B before back-pressure kicks in. */
    private volatile int liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;

    /** Signaled when Phase C completes. */
    private volatile CountDownLatch checkpointPhaseComplete;

    // -------------------------------------------------------------------------
    // On-disk state -- multiple segments
    // -------------------------------------------------------------------------

    /** On-disk segments. */
    private volatile List<VectorSegment> segments = new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * Incrementally-maintained sum of {@link VectorSegment#estimatedInMemoryBytes()}
     * across every segment currently registered in {@link #segments}
     * (issue #455).  Updated by {@link #registerSegmentMemoryEstimate} and
     * {@link #unregisterSegmentMemoryEstimate} at every mutation point on
     * the segments list, so {@link #estimatedMemoryUsageBytes()} can read it
     * in O(1) instead of iterating every segment on every back-pressure check.
     *
     * <p>Each segment's contribution is captured as a snapshot at registration
     * time and stored on the segment in
     * {@link VectorSegment#cachedEstimatedInMemoryBytes}, so the same value can
     * later be subtracted on unregistration.  The snapshot covers only the
     * pkData / pkOffsets / pkLengths heap arrays (≈16 B/vector for BIGINT PKs);
     * the BLink pk-to-ordinal tree is intentionally excluded because its
     * loaded-page footprint is already bounded by the {@link MemoryManager}'s
     * global index-page replacement policy budget, which the back-pressure code
     * enforces independently (issue #592).
     */
    private final AtomicLong onDiskSegmentsEstimatedMemoryBytes = new AtomicLong(0);

    /** Counter for assigning unique segment IDs to IS-locally-produced segments. Counts up from 0. */
    private final AtomicInteger nextSegmentId = new AtomicInteger(0);

    /**
     * Counter for assigning unique local storage IDs to adopted (optimizer-produced) segments.
     * Counts DOWN from {@link Integer#MAX_VALUE} so that adopted segment IDs occupy the
     * [Integer.MAX_VALUE, ...] space and IS-local segment IDs occupy the [0, ...] space,
     * making BLink storage key collisions impossible even if the checkpoint wipe path resets
     * {@link #nextSegmentId} to 0 while an adoption is in flight.
     *
     * <p>The counter is NOT reset by the "all-vectors-deleted" wipe path (unlike
     * {@code nextSegmentId}), because adopted segments live in a disjoint ID space.
     * On restart, it is re-initialised to {@code minAdoptedSegId - 1} so subsequent
     * adoptions continue below the lowest previously-used adopted ID.
     */
    private final AtomicInteger nextAdoptedSegmentLocalId = new AtomicInteger(Integer.MAX_VALUE);

    /**
     * Optional pluggable publisher invoked after each successful checkpoint to
     * register the freshly-emitted segments with an external registry (the
     * segmented-v2 ZooKeeper segment registry, see {@link SegmentPublisher}).
     * {@code null} means no external publication; this is the legacy behaviour.
     */
    private volatile SegmentPublisher segmentPublisher;

    /**
     * When {@code true}, {@link #start()} does NOT spawn the in-IS
     * {@link #vectorIndexCompactionLoop()} thread — compaction is expected to be
     * driven by an external index-optimizer service (see
     * {@code herddb.indexing.optimizer.IndexOptimizerEngine}). Tailer and
     * checkpoint loops still run as normal.
     */
    private volatile boolean externalCompactionEnabled;

    // segmentedV2Cached removed: v3 format is no longer supported; all indexes
    // are written in v4 format (which always includes segmentUuid per segment).

    /**
     * Test hook (package-private) — runs in Phase B AFTER the staged publish and
     * BEFORE the IndexStatus persist. Used by {@code PublisherHotSwapTest} to swap
     * {@link #segmentPublisher} mid-Phase-B and assert the captured snapshot is
     * what handles the commit (review-item R3 from pr-reviewer pass 2). Distinct
     * from {@link #checkpointPhaseBHook} (which fires at end of Phase B) — this
     * one is specifically wedged between {@code stageNewSegmentsBestEffort} and
     * {@code persistIndexStatusMultiSegment}.
     *
     * <p>Production code never sets this field. Tests outside this package use
     * reflection to install the hook (no public setter — keeps the API surface
     * clean per pr-reviewer pass-3 P3-4).
     */
    volatile Runnable betweenStageAndCommitHookForTests;

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
     *
     * <p>Backed by a {@link PagedPkSet} (BLink-paged storage with the
     * existing {@link MemoryManager} page replacement policy) so memory
     * stays bounded even when sustained delete traffic arrives during a
     * multi-minute compaction cycle (issue #290).
     */
    private volatile PagedPkSet pendingCompactionDeletes;

    // -------------------------------------------------------------------------
    // Compaction state
    // -------------------------------------------------------------------------

    /** Serialises compaction cycles. One run at a time. */
    private final ReentrantLock compactionLock = new ReentrantLock();

    /**
     * Segments removed from {@link #segments} by {@link #dropSegmentByUuid}
     * (typically fired by the optimizer-watcher's
     * {@code onSegmentReleased} callback) that are awaiting
     * {@link VectorSegment#close()} + BLink-storage drop (issue #535).
     *
     * <p>The close cannot run synchronously inside {@code dropSegmentByUuid}
     * because a concurrent {@link #runCompactionCycle} may hold live
     * references to the same {@code VectorSegment} object via its
     * candidate snapshot (taken at the start of the cycle without a
     * read-lock). Closing the segment while the cycle's snapshot still
     * references it nulls out {@code seg.onDiskGraph}, causing
     * {@link VectorIndexCompactor#rebuildSegmentStreaming} to throw
     * {@code CompactionException(CORRUPTION, "candidate segment N has no
     * on-disk graph")} on the next iteration of the candidate list — the
     * exact production symptom of issue #535.
     *
     * <p>The deque is drained at three points (all guarded by
     * {@link #compactionLock} so the drainer can never run alongside a
     * cycle that might still be using the queued segments):
     * <ul>
     *   <li>Opportunistically at the end of {@code dropSegmentByUuid}
     *       via {@link #drainPendingSegmentClosesOpportunistically} —
     *       fast path when no compaction is active.</li>
     *   <li>In {@link #runCompactionCycle}'s finally block, AFTER the
     *       cycle's snapshot is no longer being iterated, but BEFORE
     *       {@code compactionLock} is released — guarantees the
     *       drop+close pair is observable to the next cycle as a single
     *       atomic step.</li>
     *   <li>In {@link #close} after the compaction thread has been
     *       joined, so a queued segment is never leaked at shutdown.</li>
     * </ul>
     *
     * <p>Search safety: the drain holds {@code compactionLock} but NOT
     * {@link #stateLock}'s writeLock, so it does not directly serialize
     * with concurrent {@code searchInternal} callers. Safety nevertheless
     * holds because {@code dropSegmentByUuid} removes the segment from
     * {@code this.segments} under the writeLock (which waits for all
     * in-flight {@code searchInternal} readLock holders), so any later
     * search snapshot of {@code this.segments} will not see the queued
     * segment. The Stage-3 publish in
     * {@code atomicSwapCompactionResult} (issue #537, now fixed by
     * issue #538's rebuild-from-current-segments at Stage 3 / Phase C
     * Stage 2) also reads from {@code this.segments} under the writeLock
     * and therefore never re-introduces a queued segment.
     */
    private final ConcurrentLinkedDeque<VectorSegment>
            pendingSegmentCloses = new ConcurrentLinkedDeque<>();

    /** Background thread driving {@link #runCompactionCycle(long)}. */
    private volatile Thread vectorIndexCompactionThread;
    private final Object vectorIndexCompactionWakeup = new Object();
    private volatile boolean vectorIndexCompactionWakeupPending;

    /**
     * Default minimum number of candidate segments a compaction run must be
     * able to pick before the byte-threshold trigger fires. Configurable via
     * system property {@code herddb.vectorindex.compactionMinCount} (issue
     * #562); clamped to a minimum of 1. {@link herddb.indexing.IndexingServiceEngine}
     * passes this constant to {@link #configureCompaction} so the property
     * takes effect in production deployments.
     */
    public static final int DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT =
            Math.max(1, Integer.getInteger(
                    "herddb.vectorindex.compactionMinCount", 4));

    /**
     * Default live-node count below which a segment is treated as a
     * micro-segment by the compaction scheduler's micro-segment fast path
     * (issue #570). Micro-segments are produced by memory-pressure
     * checkpoints flushing a near-empty live shard; merging them is
     * essentially free and reclaims segment-count slots immediately.
     * Configurable in production via
     * {@code vector.index.compaction.microsegment.max.nodes}; {@code 0}
     * disables the fast path.
     */
    public static final long DEFAULT_VECTOR_INDEX_COMPACTION_MICROSEGMENT_MAX_NODES = 1000L;

    /**
     * Default hard cap on the number of input segments a single compaction
     * cycle may merge (issue #587). The downstream PQ-retraining step samples
     * training vectors per input segment with random remote-storage reads, so
     * its I/O cost scales with the input count; an unbounded selection of small
     * segments (53 in the incident) could stall a cycle for hours. The cap
     * bounds per-cycle I/O — leftover segments are merged by later cycles.
     * Configurable in production via {@code vector.index.compaction.maxInputs};
     * {@code 0} disables the cap.
     */
    public static final int DEFAULT_VECTOR_INDEX_COMPACTION_MAX_INPUTS = 16;

    /** Compaction policy knobs — defaults match IndexingServerConfiguration. */
    private volatile long vectorIndexCompactionIntervalMs = 5L * 60_000L;
    private volatile long vectorIndexCompactionMinBytes = 256L * 1024 * 1024;
    private volatile long vectorIndexCompactionMaxBytes = 1024L * 1024 * 1024;
    private volatile int vectorIndexCompactionMinCount = DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT;
    /**
     * Count-based compaction trigger (issue #285): fire compaction when the
     * segment count reaches this ceiling, even if the total-byte threshold
     * has not been met.  Prevents unbounded segment accumulation during
     * tailing catch-up when each checkpoint produces many small segments.
     */
    private volatile int vectorIndexCompactionMaxCount = 200;
    /**
     * Micro-segment fast-path threshold (issue #570): when at least two
     * on-disk segments have a live-node count strictly below this value the
     * compaction cycle merges only those micro-segments, relieving
     * segment-count back-pressure quickly. {@code 0} disables the fast path.
     */
    private volatile long vectorIndexCompactionMicroSegmentMaxNodes =
            DEFAULT_VECTOR_INDEX_COMPACTION_MICROSEGMENT_MAX_NODES;
    /**
     * Base hard cap on the number of input segments per compaction cycle
     * (issue #587). When tiered compaction is enabled this base value is
     * tier-scaled (2×/4×/8×) per cycle by {@link VectorIndexCompactor#computeTieredMaxInputs}
     * so the per-cycle drain rate keeps pace with the backlog and the cap
     * cannot starve the tailer. {@code 0} disables the cap. Pre-normalised by
     * {@link #setCompactionMaxInputs}.
     */
    private volatile int vectorIndexCompactionMaxInputs =
            DEFAULT_VECTOR_INDEX_COMPACTION_MAX_INPUTS;
    /**
     * Hard cap on the total bytes of source graph files that a single
     * streaming compaction cycle may download (issue #602).  After
     * {@link VectorIndexCompactor#chooseSegmentsToMerge} selects candidates,
     * segments are trimmed (largest-first) until the total is within this
     * budget.  Default is 100 GiB — enough to hold the vast majority of
     * real-world merge inputs while preventing runaway disk consumption on
     * small data-directory PVCs.  Set to {@link Long#MAX_VALUE} to disable.
     */
    private volatile long vectorIndexCompactionMaxInputBytes = 100L * 1024 * 1024 * 1024;
    private volatile long vectorIndexCompactionRetentionMs = 10L * 60_000L;
    /**
     * When {@code true} (the default), the per-cycle byte cap and segment
     * count cap are scaled up by {@link VectorIndexCompactor#tieredMultiplier}
     * so that compaction throughput stays proportional to the segment
     * accumulation rate (issue #354).
     */
    private volatile boolean vectorIndexCompactionTieredEnabled = true;
    /**
     * Segment-count back-pressure threshold (issue #354).  When the total
     * number of on-disk segments exceeds this value, {@link #addVectorInternal}
     * blocks and wakes the compaction thread before accepting new vectors.
     * Default is 500 — set to {@link Integer#MAX_VALUE} to disable.
     */
    private volatile int compactionBackpressureThreshold = 500;

    /**
     * Maximum wall-clock time (ms) a caller may spend in
     * {@link #waitForSegmentCountRelief} before back-pressure is
     * force-released with a SEVERE log (issue #370).
     * Default is 300 000 ms (5 min).  Set to {@link Long#MAX_VALUE} to
     * disable the safety timeout.
     */
    private volatile long compactionBackpressureMaxWaitMs = 300_000L;

    /**
     * Pressure-driven IS-local compaction kick fraction (companion to the external
     * index-optimizer). When {@link #externalCompactionEnabled} is {@code true},
     * the IS-local compaction loop stays idle until {@code segments.size()} crosses
     * {@code localCompactionKickFraction × compactionBackpressureThreshold}. Above
     * that threshold the local loop runs as a fallback, draining segments before
     * the back-pressure ceiling stalls the tailer. Default is {@code 0.7} —
     * giving the optimizer 70% of the segment budget before the IS kicks in.
     */
    private volatile double localCompactionKickFraction = 0.7d;

    /**
     * Master switch for the IS-local compaction fallback when
     * {@link #externalCompactionEnabled} is {@code true}. When {@code false},
     * suppresses the IS-local loop entirely (fully delegated to the external
     * optimizer); the tailer may then stall on back-pressure if the optimizer
     * cannot keep up. Default {@code true}: IS keeps a pressure-driven fallback
     * so the cluster never wedges on a slow optimizer.
     */
    private volatile boolean localCompactionEnabledWithOptimizer = true;

    /**
     * Pre-computed value of {@link #currentLocalCompactionKickThreshold()},
     * recomputed only inside {@link #setLocalCompactionKickFraction} and
     * {@link #setCompactionBackpressureThreshold}. Read on every
     * {@link #addVectorInternal} call (the wake-on-threshold-crossing fast
     * path) so the per-add cost stays at a single volatile load + int
     * compare instead of {@code Math.ceil(double × int)} + two volatile
     * reads. Kept consistent with the configured fraction/threshold by the
     * setters; never read or written outside of those.
     */
    private volatile int kickThresholdCached =
            (int) Math.max(1L, Math.ceil(0.7d * 500));

    /**
     * Counter: number of times the IS-local compaction loop ran a full cycle
     * <em>specifically</em> as a pressure-driven fallback (i.e., with
     * {@link #externalCompactionEnabled} {@code true} and segment count above the
     * kick threshold). Distinct from {@link #compactionRunsTotal} which counts
     * every cycle regardless of mode.
     */
    final AtomicLong localCompactionPressureRunsTotal = new AtomicLong();

    /**
     * Counter: number of cycles the IS-local loop short-circuited because the
     * segment count was below the kick threshold. Useful for verifying that
     * steady-state ingest is being handled by the optimizer (high counter value
     * = optimizer is keeping up; counter staying flat while
     * {@link #localCompactionPressureRunsTotal} climbs = optimizer is falling
     * behind).
     */
    final AtomicLong localCompactionSkippedBelowThresholdTotal = new AtomicLong();

    /**
     * Log a WARNING during Phase A when the total on-disk segment count
     * exceeds this threshold, making runaway accumulation visible without
     * having to parse checkpoint log lines.
     */
    static final int COMPACTION_SEGMENT_COUNT_WARN_THRESHOLD = 50;

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
    // Block-cache warmup (issue #322 / #569)
    // -------------------------------------------------------------------------

    /**
     * Approximate per-segment byte budget for the BFS block-cache warmup
     * (issue #322). A value of {@code 0} (the default) or negative disables
     * warmup entirely. Set by {@code IndexingServiceEngine} from
     * {@code vector.index.segmentCacheWarmupBytes} via
     * {@link #setWarmupBytesPerSegment(long)} so the store can warm each new
     * segment AT CREATION TIME — before it is published into the searchable
     * {@link #segments} list (issue #569).
     */
    private volatile long warmupBytesPerSegment;

    /**
     * Counter: total number of segments actually BFS-warmed by this store
     * since start (issue #569). With per-segment warm-at-creation in place,
     * this rises by exactly one per checkpoint (the new segment) and one per
     * compaction cycle (the merged output) in steady state; a jump equal to
     * the running segment count would indicate the post-checkpoint warm-all
     * is no longer idempotent — i.e. the death spiral has regressed.
     */
    final AtomicLong warmedSegmentsTotal = new AtomicLong();

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
     * Wall-clock nanoseconds the most recent {@code atomicSwapCompactionResult}
     * call held {@code stateLock.writeLock()} (issue #462). Diagnostic only — used
     * by the lock-progress regression tests to assert the writeLock window stays
     * tiny (≪ persist duration) so concurrent searches are not blocked behind
     * the compaction-swap critical section.
     */
    private final AtomicLong lastCompactionSwapWriteLockNanos = new AtomicLong(0);
    /**
     * Wall-clock nanoseconds the most recent Phase C of
     * {@code doCheckpointFusedPQThreePhase} held {@code stateLock.writeLock()}
     * (issue #462). Diagnostic only — used by the lock-progress regression
     * tests to assert the writeLock window stays tiny (≪ pending-deletes-apply
     * duration) so concurrent searches are not blocked behind Phase C.
     */
    private final AtomicLong lastPhaseCWriteLockNanos = new AtomicLong(0);
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
    // Segment-count back-pressure statistics (issue #354)
    // -------------------------------------------------------------------------

    /**
     * Monitor used to park {@link #addVectorInternal} callers while the
     * on-disk segment count exceeds {@link #compactionBackpressureThreshold}.
     * Notified after every successful compaction swap.
     */
    private final Object segmentCountMonitor = new Object();
    private volatile int segmentCountBackpressureActive;
    private final AtomicLong segmentCountBackpressureTotal = new AtomicLong(0);
    private final AtomicLong segmentCountBackpressureTimeMs = new AtomicLong(0);
    /**
     * Number of times segment-count back-pressure was force-released by
     * the safety timeout ({@link #compactionBackpressureMaxWaitMs}) rather
     * than by a successful compaction that reduced the segment count below
     * the threshold (issue #370).
     */
    private final AtomicLong segmentCountBackpressureTimeouts = new AtomicLong(0);
    /**
     * Generation counter incremented inside {@link #notifySegmentCountMonitor}
     * to satisfy SpotBugs NN_NAKED_NOTIFY: the monitor must see a visible
     * state change before {@code notifyAll()} is called.
     * Always accessed while holding {@link #segmentCountMonitor}, so no
     * {@code volatile} is needed; {@link #getSegmentCountGeneration()} acquires
     * the same lock so external readers also see the latest value.
     */
    private int segmentCountGeneration;

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

    // -------------------------------------------------------------------------
    // Recovery loading-progress counters (issue #381)
    //
    // Set during loadMultiSegmentFormat() so that gRPC callers (GetIndexStatus,
    // ListIndexes, DescribeIndex) can report loading progress to operators and
    // to the wait_for_indexes barrier instead of showing "0 vectors / 0 segments
    // / status=missing" for the entire cold-start window.
    // -------------------------------------------------------------------------

    /**
     * Number of segments that have been fully loaded (map downloaded + HNSW graph
     * opened) during the current or most recent {@link #loadMultiSegmentFormat} run.
     * 0 while no load is in progress or before the first segment completes.
     */
    private final AtomicInteger loadingSegmentsDone = new AtomicInteger(0);

    /**
     * Total number of segments to be loaded during the current or most recent
     * {@link #loadMultiSegmentFormat} run. 0 when no load is/was in progress.
     */
    private final AtomicInteger loadingSegmentsTotal = new AtomicInteger(0);

    /**
     * {@code true} while {@link #loadMultiSegmentFormat} is running; {@code false}
     * once loading completes (successfully or with an error).
     */
    private volatile boolean loadingFromStatus = false;

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

    /**
     * Number of times the IS-local compaction loop ran a full cycle as a
     * pressure-driven fallback (companion to the external optimizer). Non-zero
     * value = the optimizer is falling behind and the IS is draining segments
     * to keep the tailer from stalling on back-pressure. Steady state should
     * stay at zero (or grow only slowly) with the optimizer doing the work.
     */
    public long getLocalCompactionPressureRunsTotal() {
        return localCompactionPressureRunsTotal.get();
    }

    /**
     * Number of cycles the IS-local loop short-circuited because the segment
     * count was below the kick threshold. High and growing = the optimizer is
     * keeping up.
     */
    public long getLocalCompactionSkippedBelowThresholdTotal() {
        return localCompactionSkippedBelowThresholdTotal.get();
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
     * Returns the multipart storage key for a segment.
     *
     * <p>IS-locally-produced segments use the legacy key
     * {@code indexUUID + "_seg" + segmentId} (where {@code segmentId} is an {@code int}).
     * Externally-produced segments (optimizer outputs) carry a 63-bit long segment ID
     * that does not fit in {@code int}; their storage key is stored explicitly in
     * {@link VectorSegment#externalStorageKey} during adoption.
     */
    String segmentStorageKey(VectorSegment seg) {
        return seg.externalStorageKey != null
                ? seg.externalStorageKey
                : (indexUUID + "_seg" + seg.segmentId);
    }

    /**
     * Queues the two multipart files backing a segment (graph + map)
     * for retention-aware deletion. Called by the compaction swap step
     * for every input segment that the merged output replaces.
     */
    void queueSegmentPendingDelete(VectorSegment seg, long retentionMs) {
        long deadlineMs = System.currentTimeMillis() + retentionMs;
        long sinceGen = currentIndexStatusGeneration.get();
        String segUuid = segmentStorageKey(seg);
        pendingDeletes.add(new PendingDelete(
                encodeMultipartPath(segUuid, "graph"), deadlineMs, sinceGen));
        if (seg.mapFilePath != null) {
            pendingDeletes.add(new PendingDelete(
                    encodeMultipartPath(segUuid, "map"), deadlineMs, sinceGen));
        }
    }

    /**
     * Cleanup helper for the abort paths in
     * {@link #atomicSwapCompactionResult}: when a local compaction merge is
     * aborted AFTER {@code rebuildSegment} has already uploaded the merged
     * output's multipart files, this method (a) queues those files for
     * retention-aware deletion via the existing {@code pendingDeletes}
     * mechanism, and (b) closes the merged {@link VectorSegment} so its
     * file handles + mmap'd buffers are released. Without this, every abort
     * leaks up to {@code vector.index.compaction.maxBytes} of remote storage
     * — and aborts are by-design steady-state behaviour of the pressure-
     * driven local fallback when the optimizer races the IS.
     *
     * <p>The deadline is anchored to {@link #vectorIndexCompactionRetentionMs}
     * (same window as the inputs of a successful merge would pay), giving
     * any IS still loading the segment time to drop its reference. The
     * {@code sinceGen} is the current generation: the merged output was
     * never published in IndexStatus, so any generation ≥ the current one
     * trivially "doesn't reference it" for the shadow-ack gating.
     */
    void queueMergedOutputForDeletion(VectorSegment mergedOutput) {
        if (mergedOutput == null) {
            return;
        }
        long deadlineMs = System.currentTimeMillis() + vectorIndexCompactionRetentionMs;
        long sinceGen = currentIndexStatusGeneration.get();
        String segUuid = segmentStorageKey(mergedOutput);
        if (mergedOutput.graphFilePath != null) {
            pendingDeletes.add(new PendingDelete(
                    encodeMultipartPath(segUuid, "graph"), deadlineMs, sinceGen));
        }
        if (mergedOutput.mapFilePath != null) {
            pendingDeletes.add(new PendingDelete(
                    encodeMultipartPath(segUuid, "map"), deadlineMs, sinceGen));
        }
        try {
            mergedOutput.close();
        } catch (RuntimeException e) {
            // Broad catch is intentional and limited to logging — close()
            // surfaces BLink close failures and we must not let a stale handle
            // mask the real abort reason. The next reap pass will still
            // collect the multipart files queued above.
            LOGGER.log(Level.WARNING,
                    "vector store " + indexName
                            + ": ignoring close failure for aborted merged segment "
                            + mergedOutput.segmentId,
                    e);
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

    /**
     * Test hook fired inside {@code atomicSwapCompactionResult} (issue #462)
     * after Stage 1 (validate + build {@code newSegments} + queue
     * pending-deletes) finishes and BEFORE Stage 2 ({@code
     * persistIndexStatusMultiSegment}). Lets unit tests assert the
     * pre-persist invariant: {@code currentIndexStatusGeneration} unchanged,
     * {@code this.segments} still references the OLD list, {@code
     * mergedOutput.generation} already set to {@code oldGen + 1}.
     * Hook runs under {@code checkpointLock} but NOT under
     * {@code stateLock.writeLock()}.
     */
    private volatile Runnable atomicSwapPostBuildHook;

    /** Sets the test hook fired between Stage 1 and Stage 2 of
     * {@code atomicSwapCompactionResult}. For testing. */
    public void setAtomicSwapPostBuildHookForTest(Runnable hook) {
        this.atomicSwapPostBuildHook = hook;
    }

    /**
     * Test hook fired inside {@code atomicSwapCompactionResult} (issue #462)
     * after Stage 2 ({@code persistIndexStatusMultiSegment} returned
     * successfully) and BEFORE Stage 3 (writeLock-protected
     * publish + memory-counter swap). Lets unit tests assert the
     * post-persist / pre-publish invariant: {@code
     * currentIndexStatusGeneration} is now bumped, but {@code this.segments}
     * still references the OLD list and {@code stateLock.isWriteLocked()
     * == false}. Hook runs under {@code checkpointLock} but NOT under
     * {@code stateLock.writeLock()}.
     */
    private volatile Runnable atomicSwapPostPersistHook;

    /** Sets the test hook fired between Stage 2 and Stage 3 of
     * {@code atomicSwapCompactionResult}. For testing. */
    public void setAtomicSwapPostPersistHookForTest(Runnable hook) {
        this.atomicSwapPostPersistHook = hook;
    }

    /**
     * Test hook fired inside Phase C of {@code doCheckpointFusedPQThreePhase}
     * (issue #462) after the pending-checkpoint-deletes apply finishes and
     * BEFORE the Stage-2 writeLock acquisition. Lets the lock-progress
     * regression test inject a "search now" probe at the precise window where
     * the pre-fix code was holding the writeLock for seconds. Hook runs under
     * {@code checkpointLock} but NOT under {@code stateLock.writeLock()}.
     */
    private volatile Runnable phaseCPostDeletesApplyHook;

    /** Sets the test hook fired after Phase C's pending-deletes apply.
     * For testing. */
    public void setPhaseCPostDeletesApplyHookForTest(Runnable hook) {
        this.phaseCPostDeletesApplyHook = hook;
    }

    /**
     * Test hook (issue #535) fired inside {@link #runCompactionCycle} AFTER
     * the {@code candidates} list has been built by
     * {@link VectorIndexCompactor#chooseSegmentsToMerge} and BEFORE
     * {@link VectorIndexCompactor#rebuildSegment} is called. Lets a test
     * deterministically inject a concurrent mutation (e.g. a
     * {@link #dropSegmentByUuid} that closes one of the candidate
     * {@code VectorSegment} objects) while the cycle is holding live
     * references to those candidates — reproducing the race that surfaces
     * as {@code "candidate segment N has no on-disk graph"} on production
     * deployments where the optimizer-driven {@link
     * herddb.indexing.segment.SegmentAssignmentWatcher} fires
     * {@code onSegmentReleased} concurrently with an IS-local fallback
     * compaction.
     *
     * <p>Hook runs under {@link #compactionLock} but NOT under
     * {@link #stateLock}, so it can safely call any operation that takes
     * the state writeLock.
     */
    private volatile Runnable compactionPostCandidateSelectionHookForTests;

    /**
     * Sets the test hook fired between candidate selection and
     * {@code rebuildSegment} in {@link #runCompactionCycle}.
     * For testing only (issue #535 reproducer).
     */
    public void setCompactionPostCandidateSelectionHookForTest(Runnable hook) {
        this.compactionPostCandidateSelectionHookForTests = hook;
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
        /** Number of live vectors written to this segment. */
        final long vectorCount;
        /**
         * Sorted list of jvector {@code FeatureId} names written into the graph
         * file (e.g. {@code ["FUSED_PQ", "INLINE_VECTORS"]}). Carried through
         * to {@link NewSegmentInfo} and persisted in the ZK segment znode so the
         * external optimizer's merge policy can group candidates by feature set
         * (issue #543). Set immediately after construction by the writer that
         * knows which features were used.
         */
        List<String> jvectorFeatureIds;
        /**
         * Stable UUID stamped onto this segment once we know we will publish to a
         * {@link SegmentPublisher}. Populated by {@link #stampSegmentUuid(String)} in
         * Phase B; carried through to {@link NewSegmentInfo} and persisted in the v4
         * IndexStatus so the same UUID survives restarts. {@code null} when no publisher
         * is attached (i.e. the external optimizer is not enabled for this store).
         */
        String segmentUuid;

        SegmentWriteResult(int segmentId, String graphFilePath, long graphFileSize,
                           String mapFilePath, long mapFileSize, long estimatedSizeBytes) {
            this(segmentId, graphFilePath, graphFileSize, mapFilePath, mapFileSize,
                    estimatedSizeBytes, 0L);
        }

        SegmentWriteResult(int segmentId, String graphFilePath, long graphFileSize,
                           String mapFilePath, long mapFileSize, long estimatedSizeBytes,
                           long vectorCount) {
            this.segmentId = segmentId;
            this.graphFilePath = graphFilePath;
            this.graphFileSize = graphFileSize;
            this.mapFilePath = mapFilePath;
            this.mapFileSize = mapFileSize;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.vectorCount = vectorCount;
        }

        /** One-shot setter — Phase B stamps the UUID before Phase C persists IndexStatus. */
        void stampSegmentUuid(String uuid) {
            this.segmentUuid = uuid;
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

        // Try to load existing state. We separate "no prior state, start empty" (a
        // DataStorageManagerException from getIndexStatus when there is no checkpoint
        // yet) from "prior state exists but loadFromStatus rejected it" (a
        // DataStorageManagerException from loadFromStatus, e.g. unsupported metadata
        // version on rollback — review-item B4 from the second pr-reviewer pass).
        // The first is benign and we swallow it; the second is operator-actionable
        // and we propagate it so the boot fails fast instead of silently emptying
        // the index.
        IndexStatus status = null;
        try {
            status = dataStorageManager.getIndexStatus(
                    tableSpaceUUID, indexUUID, LogSequenceNumber.START_OF_TIME);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.INFO,
                    "no existing state for PersistentVectorStore {0}, starting empty: {1}",
                    new Object[]{indexName, e.getMessage()});
        }
        if (status != null && status.indexData != null && status.indexData.length > 0) {
            // Do NOT swallow exceptions from loadFromStatus — they indicate the
            // persisted state is unreadable by this binary (e.g. future format
            // version), which is operator-actionable and must surface as a boot
            // failure rather than a silent data-loss-equivalent regression.
            loadFromStatus(status);
            this.loadedLsn = status.sequenceNumber;
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
        //
        // Pressure-driven local fallback (companion to the external optimizer):
        // when {@link #externalCompactionEnabled} is {@code true} the thread
        // still runs but the cycle body short-circuits below the kick threshold
        // — the optimizer drives steady-state compaction, the IS-local loop
        // only fires when segment accumulation indicates the optimizer is
        // falling behind. Operators who want full delegation (no local
        // fallback) can set {@link #localCompactionEnabledWithOptimizer} to
        // {@code false}; the cycle then short-circuits unconditionally.
        vectorIndexCompactionThread = new Thread(this::vectorIndexCompactionLoop,
                "persistent-vector-store-vidxcompaction-" + indexName);
        vectorIndexCompactionThread.setDaemon(true);
        vectorIndexCompactionThread.start();
        if (externalCompactionEnabled) {
            LOGGER.log(Level.INFO,
                    "PersistentVectorStore {0}: external compaction enabled — IS-local"
                            + " compaction loop is pressure-driven (kickFraction={1},"
                            + " enabledWithOptimizer={2})",
                    new Object[]{indexName, localCompactionKickFraction,
                            localCompactionEnabledWithOptimizer});
        }

        // Reconcile the segmented-v2 registry with IndexStatus on every start
        // (review item A1+A3). This heals partial-publish state from a previous
        // crash: PROVISIONAL znodes whose UUID is in IndexStatus get promoted
        // to ACTIVE; PROVISIONAL znodes whose UUID is unknown get dropped;
        // IndexStatus segments with no znode get re-registered. We catch broad
        // Exception here on purpose — reconcile is best-effort, the IS must
        // start regardless.
        SegmentPublisher reconciler = this.segmentPublisher;
        if (reconciler != null) {
            try {
                reconciler.reconcileWithIndexStatus(buildExistingSegmentsInfoForReconcile());
            } catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "segment publisher.reconcileWithIndexStatus failed for {0}; "
                                + "continuing with un-reconciled state", indexName);
                LOGGER.log(Level.WARNING, "reconcile failure detail", e);
            }
        }

        LOGGER.log(Level.INFO, "PersistentVectorStore {0} started", indexName);
    }

    /**
     * Applies the compaction policy knobs from configuration. Must be
     * called before {@link #start()} for the values to influence the
     * first compaction cycle, but can also be called at any time to
     * re-tune a running store.
     */
    public void configureCompaction(long intervalMs, long minBytes, long maxBytes,
                                    int minCount, int maxCount, long retentionMs) {
        this.vectorIndexCompactionIntervalMs = intervalMs;
        this.vectorIndexCompactionMinBytes = minBytes;
        this.vectorIndexCompactionMaxBytes = maxBytes;
        this.vectorIndexCompactionMinCount = minCount;
        this.vectorIndexCompactionMaxCount = maxCount;
        this.vectorIndexCompactionRetentionMs = retentionMs;
    }

    /**
     * Enables or disables tiered compaction scaling (issue #354).
     * When enabled, the per-cycle byte cap and segment-count cap are
     * multiplied by a tier-dependent factor when the total on-disk
     * segment count exceeds the tier thresholds, keeping compaction
     * throughput proportional to the ingest rate.
     */
    public void setTieredCompactionEnabled(boolean enabled) {
        this.vectorIndexCompactionTieredEnabled = enabled;
    }

    /**
     * Sets the micro-segment fast-path threshold (issue #570). When at least
     * two on-disk segments have a live-node count strictly below
     * {@code microSegmentMaxNodes}, the next compaction cycle merges only
     * those micro-segments — a cheap, fast cycle that reclaims segment-count
     * slots and relieves back-pressure quickly. A value of {@code 0} (or
     * negative) disables the fast path, restoring the pre-#570 behaviour.
     * Can be called at any time to re-tune a running store.
     */
    public void setCompactionMicroSegmentMaxNodes(long microSegmentMaxNodes) {
        this.vectorIndexCompactionMicroSegmentMaxNodes =
                Math.max(0L, microSegmentMaxNodes);
    }

    /** Returns the current micro-segment fast-path threshold (issue #570). */
    public long getCompactionMicroSegmentMaxNodes() {
        return vectorIndexCompactionMicroSegmentMaxNodes;
    }

    /**
     * Sets the hard cap on the number of input segments a single compaction
     * cycle may merge (issue #587). A compaction cycle still fires on the same
     * triggers, but merges at most {@code maxInputs} segments per cycle
     * (smallest-first); any remaining segments are merged by subsequent cycles.
     * This bounds the per-cycle remote I/O of the downstream PQ-retraining
     * step, whose cost scales with the number of input segments.
     *
     * <p>The value is normalised via {@link VectorIndexCompactor#clampMaxInputs}:
     * {@code 0} (or negative) disables the cap; {@code 1} is clamped up to 2.
     * Can be called at any time to re-tune a running store.
     */
    public void setCompactionMaxInputs(int maxInputs) {
        this.vectorIndexCompactionMaxInputs =
                VectorIndexCompactor.clampMaxInputs(maxInputs);
    }

    /**
     * Returns the current compaction input cap (issue #587); {@code 0} means
     * the cap is disabled.
     */
    public int getCompactionMaxInputs() {
        return vectorIndexCompactionMaxInputs;
    }

    /**
     * Sets the maximum total bytes of source graph files that a single
     * streaming compaction cycle may download (issue #602).
     * After candidate selection, segments are trimmed (largest-first) until
     * the total download budget is within this cap.
     * Use {@link Long#MAX_VALUE} to disable the cap.
     *
     * @param maxInputBytes positive budget in bytes; values &lt;= 0 are silently
     *                      clamped to {@link Long#MAX_VALUE} (disabled)
     */
    public void setCompactionMaxInputBytes(long maxInputBytes) {
        this.vectorIndexCompactionMaxInputBytes =
                (maxInputBytes <= 0) ? Long.MAX_VALUE : maxInputBytes;
    }

    /**
     * Returns the current per-cycle download budget in bytes (issue #602).
     * {@link Long#MAX_VALUE} means the cap is disabled.
     */
    public long getCompactionMaxInputBytes() {
        return vectorIndexCompactionMaxInputBytes;
    }

    /**
     * Sets the segment-count back-pressure threshold (issue #354).
     * {@link #addVectorInternal} will block when {@code segments.size()}
     * exceeds this value, waking the compaction thread first.
     * Set to {@link Integer#MAX_VALUE} to disable.
     */
    public void setCompactionBackpressureThreshold(int threshold) {
        this.compactionBackpressureThreshold = threshold;
        recomputeKickThresholdCached();
    }

    /**
     * Sets the IS-local compaction kick fraction (companion to the external
     * optimizer). Must be in the open interval {@code (0.0, 1.0)}. Values
     * outside that range are rejected to prevent silently disabling either the
     * fallback (≥ 1.0) or both compactors (≤ 0.0).
     */
    public void setLocalCompactionKickFraction(double fraction) {
        if (!(fraction > 0.0d && fraction < 1.0d)) {
            throw new IllegalArgumentException(
                    "localCompactionKickFraction must be in (0.0, 1.0), got " + fraction);
        }
        this.localCompactionKickFraction = fraction;
        recomputeKickThresholdCached();
    }

    /**
     * Recomputes {@link #kickThresholdCached} from the current fraction +
     * back-pressure threshold. Called only by the two setters that mutate
     * those inputs. Kept private to enforce the invariant that the cached
     * value is always consistent with the configured fraction/threshold;
     * callers never write {@code kickThresholdCached} directly.
     */
    private void recomputeKickThresholdCached() {
        long t = (long) Math.ceil(localCompactionKickFraction * compactionBackpressureThreshold);
        if (t > Integer.MAX_VALUE) {
            this.kickThresholdCached = Integer.MAX_VALUE;
        } else {
            this.kickThresholdCached = (int) Math.max(1L, t);
        }
    }

    /**
     * Master switch for the IS-local compaction fallback when
     * {@link #externalCompactionEnabled} is {@code true}. See
     * {@link #localCompactionEnabledWithOptimizer}.
     */
    public void setLocalCompactionEnabledWithOptimizer(boolean enabled) {
        this.localCompactionEnabledWithOptimizer = enabled;
    }

    /**
     * Returns the current segment-count threshold above which the IS-local
     * compaction loop wakes from idle and runs a cycle. Always rounded UP
     * (ceiling) so a threshold of 1 segment is reachable even at small
     * back-pressure caps. Public so tests and Prometheus exporters can read
     * it directly. Reads {@link #kickThresholdCached} (a single volatile
     * load) — the cache is recomputed inside the setters that change the
     * inputs, so this method is allocation-free and branch-free.
     */
    public int currentLocalCompactionKickThreshold() {
        return kickThresholdCached;
    }

    /**
     * Installs (or replaces) the {@link SegmentPublisher} hook called after each
     * successful checkpoint. Pass {@code null} to disable.
     *
     * <p><b>Production wiring (issue #491):</b> the publisher reference is per
     * {@link PersistentVectorStore} instance, so per-index attachment is possible
     * at the API level. In production the
     * {@code IndexingServiceEngine}'s vector-store factory attaches a
     * {@code SegmentRegistryPublisher} to every store it creates whenever
     * {@code indexing.optimizer.enabled=true} and the metadata storage manager
     * is ZK-backed. Outside that path callers must invoke this method
     * explicitly (see {@code MixedModeIndexesTest} for a per-index attachment
     * pattern). Operators enabling {@code indexOptimizer.enabled=true} must
     * still satisfy the remaining production prerequisites listed in VECTOR.md
     * (real {@code SegmentMerger} SPI, {@code SegmentAssignmentWatcher}
     * wired, etc.).
     *
     * <p><b>Concurrency contract (review item C1):</b> changes take effect on the
     * NEXT checkpoint cycle, not the in-flight one. The checkpoint Phase B reads
     * the publisher reference once into a local snapshot and uses that snapshot
     * for both stage and commit — so a publisher swapped (or cleared) midway
     * never sees a torn read where stage went through the old publisher and
     * commit through the new one. Callers that need a guarantee about the
     * publisher being attached for a specific checkpoint should call this method
     * BEFORE issuing the {@code addVector} that will be observed by that
     * checkpoint.
     */
    public void setSegmentPublisher(SegmentPublisher publisher) {
        this.segmentPublisher = publisher;
    }

    /**
     * Drops every registry entry this store has published, via the wired
     * {@link SegmentPublisher#dropAllSegmentsForIndex}. Invoked by the IS
     * engine on DROP_INDEX or TRUNCATE_TABLE AFTER {@link #close()} and
     * BEFORE the {@code DataStorageManager.dropIndex} call deletes the
     * multipart files. No-op when no publisher is attached (legacy
     * single-IS mode). Best-effort: a registry error is logged but does
     * NOT propagate — the surrounding DROP/TRUNCATE flow must always
     * complete the local cleanup regardless of registry state.
     *
     * <p>Without this hook the segmented-v2 registry would be left with
     * orphan ACTIVE / TRANSFERRING / DEPRECATED znodes pointing at
     * now-deleted multipart files; other IS instances watching the registry
     * would observe phantom segments and (in the worst case) attempt
     * ownership-transfers for files that no longer exist.
     */
    public void dropAllRegistryEntries() {
        SegmentPublisher snapshot = this.segmentPublisher;
        if (snapshot == null) {
            return;
        }
        try {
            snapshot.dropAllSegmentsForIndex();
        } catch (RuntimeException e) {
            // Plugin boundary — never let a registry hiccup block the local
            // DROP/TRUNCATE flow. The next reconcile pass (or the operator)
            // will catch any residual state.
            LOGGER.log(Level.WARNING,
                    "dropAllRegistryEntries failed for index {0}; local DROP/TRUNCATE"
                            + " continues regardless: {1}",
                    new Object[]{indexName, e.getMessage()});
        }
    }

    /**
     * When {@code true}, the in-IS compaction loop is suppressed. Must be set
     * before {@link #start()} to influence the launch decision.
     */
    public void setExternalCompactionEnabled(boolean enabled) {
        this.externalCompactionEnabled = enabled;
    }

    public boolean isExternalCompactionEnabled() {
        return externalCompactionEnabled;
    }

    /** Visible for tests: the in-IS compaction thread (null when not running). */
    Thread getVectorIndexCompactionThread() {
        return vectorIndexCompactionThread;
    }

    /**
     * Sets the maximum wall-clock time (ms) a caller may spend in
     * segment-count back-pressure before it is force-released (issue #370).
     * Must be {@code > 0}.  Use {@link Long#MAX_VALUE} to disable the
     * safety timeout entirely.
     *
     * @throws IllegalArgumentException if {@code maxWaitMs <= 0}
     */
    public void setCompactionBackpressureMaxWaitMs(long maxWaitMs) {
        if (maxWaitMs <= 0) {
            throw new IllegalArgumentException(
                    "compactionBackpressureMaxWaitMs must be > 0; use Long.MAX_VALUE to disable");
        }
        this.compactionBackpressureMaxWaitMs = maxWaitMs;
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
     * Returns the number of segments fully loaded so far during the current or
     * most recent {@link #loadMultiSegmentFormat} call. When
     * {@link #isLoadingFromStatus()} is {@code false} this reflects the final
     * loaded segment count of the last completed load (or 0 if none).
     */
    public int getLoadingSegmentsDone() {
        return loadingSegmentsDone.get();
    }

    /**
     * Returns the total number of segments to be loaded during the current or
     * most recent {@link #loadMultiSegmentFormat} call. 0 when no load has ever
     * been initiated.
     */
    public int getLoadingSegmentsTotal() {
        return loadingSegmentsTotal.get();
    }

    /**
     * Returns {@code true} while {@link #loadMultiSegmentFormat} is executing.
     * Cleared to {@code false} once loading completes (successfully or with an error).
     */
    public boolean isLoadingFromStatus() {
        return loadingFromStatus;
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
            // Issue #455: drop the old segments' contributions from the
            // on-disk-segment memory counter before loadFromStatus re-registers
            // the new ones.
            for (VectorSegment seg : old) {
                unregisterSegmentMemoryEstimate(seg);
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
            // Pressure-driven gate: when an external optimizer is enabled, the
            // IS-local loop only runs as a fallback. Steady-state compaction is
            // the optimizer's job; the IS just drains accumulation when the
            // optimizer falls behind. The gate is intentionally rechecked here
            // (not just at thread launch) so an operator can flip the master
            // switch at runtime without restarting the IS.
            if (externalCompactionEnabled) {
                if (!localCompactionEnabledWithOptimizer) {
                    // Operator opted out of the local fallback. The optimizer
                    // is the only compactor; the tailer will hit back-pressure
                    // and stall if the optimizer cannot keep up.
                    return;
                }
                int kickThreshold = kickThresholdCached;
                int now = segments.size();
                if (now < kickThreshold) {
                    localCompactionSkippedBelowThresholdTotal.incrementAndGet();
                    return;
                }
                localCompactionPressureRunsTotal.incrementAndGet();
                LOGGER.log(Level.INFO,
                        "vector store {0}: pressure-driven local compaction (segments={1},"
                                + " kickThreshold={2}, backpressureThreshold={3}) — optimizer"
                                + " is falling behind",
                        new Object[]{indexName, now, kickThreshold,
                                compactionBackpressureThreshold});
            }

            long cycleStart = System.currentTimeMillis();
            long cycleId = compactionRunsTotal.incrementAndGet();

            List<VectorSegment> snapshot = new ArrayList<>(segments);

            // Tiered compaction (issue #354): scale the per-cycle byte cap and
            // segment-count cap when there are many segments, so that compaction
            // throughput stays proportional to the ingest rate.
            long effectiveMaxBytes = vectorIndexCompactionMaxBytes;
            int effectiveMaxCount = vectorIndexCompactionMaxCount;
            // Issue #587: the input-segment cap is tier-scaled together with the
            // byte/count caps so the per-cycle drain rate rises with the backlog
            // — that is what keeps the flat cap from starving the tailer.
            int effectiveMaxInputs = vectorIndexCompactionMaxInputs;
            if (vectorIndexCompactionTieredEnabled) {
                int tier = VectorIndexCompactor.tieredMultiplier(snapshot.size());
                if (tier > 1) {
                    effectiveMaxBytes = VectorIndexCompactor.computeTieredMaxBytes(
                            snapshot.size(), vectorIndexCompactionMaxBytes);
                    effectiveMaxCount = VectorIndexCompactor.computeTieredMaxCount(
                            snapshot.size(), vectorIndexCompactionMaxCount);
                    effectiveMaxInputs = VectorIndexCompactor.computeTieredMaxInputs(
                            snapshot.size(), vectorIndexCompactionMaxInputs);
                    LOGGER.log(Level.INFO,
                            "vector store {0}: tiered compaction — {1} segments → "
                                    + "effective maxBytes={2} ({3}×), effective maxCount={4} ({3}×),"
                                    + " effective maxInputs={5} ({3}×)",
                            new Object[]{indexName, snapshot.size(),
                                    effectiveMaxBytes, tier, effectiveMaxCount,
                                    effectiveMaxInputs});
                }
            }

            List<VectorSegment> candidates = VectorIndexCompactor.chooseSegmentsToMerge(
                    snapshot,
                    vectorIndexCompactionMinCount,
                    vectorIndexCompactionMinBytes,
                    effectiveMaxBytes,
                    effectiveMaxCount,
                    vectorIndexCompactionMicroSegmentMaxNodes,
                    // Issue #587: hard cap on input segments per cycle, tier-scaled
                    // above so the drain rate keeps pace with the backlog.
                    effectiveMaxInputs);

            // Issue #602: apply the per-cycle download budget cap AFTER candidate
            // selection. Tiered scaling can push effectiveMaxBytes to 8 GiB; with
            // large segments this could drive hundreds of GiBs of download traffic
            // per cycle. maxInputBytes provides a hard ceiling independent of tiers.
            // We trim the largest candidates first (the list is sorted smallest-first
            // by chooseSegmentsToMerge) so we keep the maximum number of candidates
            // while staying within budget. We always keep at least 2 to allow a
            // meaningful merge; if even 2 exceed the budget we proceed anyway (the
            // download will still be faster than per-node block-cache reads).
            long maxInputBytes = vectorIndexCompactionMaxInputBytes;
            if (maxInputBytes < Long.MAX_VALUE && !candidates.isEmpty()) {
                long totalBytes = 0L;
                for (VectorSegment seg : candidates) {
                    totalBytes += Math.max(0L, seg.graphFileSize);
                }
                if (totalBytes > maxInputBytes) {
                    // Trim from the tail (largest) until within budget or only 2 remain.
                    while (candidates.size() > 2) {
                        VectorSegment largest = candidates.get(candidates.size() - 1);
                        totalBytes -= Math.max(0L, largest.graphFileSize);
                        candidates.remove(candidates.size() - 1);
                        if (totalBytes <= maxInputBytes) {
                            break;
                        }
                    }
                    LOGGER.log(Level.INFO,
                            "vector store {0}: maxInputBytes cap ({1} bytes) trimmed"
                                    + " candidates to {2} segment(s) ({3} bytes)",
                            new Object[]{indexName, maxInputBytes, candidates.size(), totalBytes});
                }
            }

            if (candidates.isEmpty()) {
                if (snapshot.size() >= COMPACTION_SEGMENT_COUNT_WARN_THRESHOLD) {
                    LOGGER.log(Level.WARNING,
                            "vector store {0}: {1} segments accumulated but neither byte "
                                    + "threshold ({2} bytes) nor count trigger ({3} segments) "
                                    + "fired; consider lowering vector.index.compaction.minBytes "
                                    + "or vector.index.compaction.maxCount",
                            new Object[]{indexName, snapshot.size(),
                                    vectorIndexCompactionMinBytes,
                                    vectorIndexCompactionMaxCount});
                }
                // Notify even when no candidates were selected (issue #370): apply
                // threads parked in waitForSegmentCountRelief() must be woken after
                // every cycle so they can re-check the segment count and, if the
                // safety timeout has expired, release back-pressure.
                notifySegmentCountMonitor();
                return;
            }

            LOGGER.log(Level.INFO,
                    "vector store {0}: starting graph-merge compaction ({1} candidate segments)",
                    new Object[]{indexName, candidates.size()});

            compactionActive.set(1);

            // Allocate the BLink-backed pendingCompactionDeletes set + the
            // BLink-backed authority map (issue #290): both replace the prior
            // unbounded HashMap/HashSet structures so memory stays bounded by
            // the index page replacement policy.
            //
            // IMPORTANT (issue #372): authority is declared null here and assigned
            // as the very first statement inside the try block so that, if
            // createTempCompactionAuthorityMap() throws a RuntimeException, the
            // finally block is still entered and pendingDeletes.close() is called.
            // If authority were allocated before the try, a failure there would skip
            // the finally entirely and leak the already-allocated pendingDeletes BLink.
            PagedPkSet pendingDeletes = createTempPagedPkSet("cmpdel_" + cycleId);
            this.pendingCompactionDeletes = pendingDeletes;
            CompactionAuthorityMap authority = null;

            try {
                authority = createTempCompactionAuthorityMap(cycleId);
                // Stream live-shard PKs into the authority map (only updates
                // entries already in it — see VectorIndexCompactor). We pass
                // a flat Iterable<Bytes> view rather than materialising a
                // HashSet of every live PK, keeping live-shard memory bounded
                // for catch-up workloads with very large live shards.
                Iterable<Bytes> liveShardPks = collectLiveShardPksLazy();
                VectorIndexCompactor.buildAuthorityMap(
                        authority, candidates, snapshot, liveShardPks);

                long bytesRead = 0;
                long vectorsWritten = 0;
                long vectorsFiltered = 0;
                VectorIndexCompactor.RebuildResult rebuild = null;
                // Issue #535 — test hook: fires AFTER candidates + authority are
                // built but BEFORE rebuildSegment iterates the candidates'
                // onDiskGraph references. Lets a reproducer test simulate the
                // race where a concurrent dropSegmentByUuid (from the optimizer
                // watcher) closes a candidate VectorSegment under our feet.
                Runnable postCandSelHook = compactionPostCandidateSelectionHookForTests;
                if (postCandSelHook != null) {
                    postCandSelHook.run();
                }
                try {
                    rebuild = VectorIndexCompactor.rebuildSegment(this, candidates, authority);
                    if (rebuild == null) {
                        // Nothing survived the filter — everything in these inputs
                        // is tombstoned or superseded. Skip the rebuild; just
                        // swap the inputs out and queue them for retention.
                        atomicSwapCompactionResult(candidates, null, 0L, 0L);
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
                        notifySegmentCountMonitor();
                        return;
                    }

                    bytesRead = candidates.stream().mapToLong(s -> s.estimatedSizeBytes).sum();
                    vectorsWritten = rebuild.vectorCount;
                    vectorsFiltered = rebuild.filteredCount;

                    // Apply deletes that arrived during the rebuild. We capture
                    // the local reference defensively — the field could be
                    // cleared by a concurrent shutdown.
                    PagedPkSet lateDeletes = this.pendingCompactionDeletes;
                    if (lateDeletes != null) {
                        VectorSegment merged = rebuild.mergedSegment;
                        lateDeletes.forEach(merged::deletePk);
                    }

                    // Issue #569: warm the merged output's block cache BEFORE
                    // the atomic swap publishes it into the searchable
                    // `this.segments` list. The compaction cycle deliberately
                    // WAITS for this warmup to complete — it runs synchronously
                    // on the compaction thread — so the merged segment is hot
                    // the instant it becomes searchable. The cost is bounded:
                    // exactly one segment per cycle, capped by the per-segment
                    // warmup byte budget regardless of the merged size.
                    warmUpNewSegmentsBeforePublish(
                            java.util.Collections.singletonList(rebuild.mergedSegment),
                            "compaction merge output");

                    atomicSwapCompactionResult(candidates, rebuild.mergedSegment,
                            rebuild.bytesWritten, rebuild.vectorCount);

                    compactionSuccessesTotal.incrementAndGet();
                    compactionConsecutiveFailures.set(0);
                    long durationMs = System.currentTimeMillis() - cycleStart;
                    compactionLastDurationMs.set(durationMs);
                    compactionLastBytesRead.set(bytesRead);
                    compactionLastBytesWritten.set(rebuild.bytesWritten);
                    compactionLastInputSegments.set(candidates.size());
                    compactionLastOutputSegments.set(1);
                    compactionLivePkFilteredTotal.addAndGet(vectorsFiltered);

                    // Notify any addVector callers waiting on segment-count
                    // back-pressure (issue #354) that the segment list shrank.
                    notifySegmentCountMonitor();

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
                    // Clean up orphaned output files. Orphans may come from two
                    // places: a successful rebuild that subsequently failed (carried
                    // on rebuild.orphanPaths), or a rebuild that threw before the
                    // RebuildResult was constructed (carried on e.orphanPaths —
                    // issue #485 review item: pre-fix the rethrown IOException
                    // dropped the orphan list with the stack frame).
                    long now = System.currentTimeMillis();
                    long sinceGen = currentIndexStatusGeneration.get();
                    if (rebuild != null && rebuild.orphanPaths != null) {
                        for (String[] orphan : rebuild.orphanPaths) {
                            this.pendingDeletes.add(new PendingDelete(
                                    encodeMultipartPath(orphan[0], orphan[1]),
                                    now, sinceGen));
                        }
                    }
                    if (e.orphanPaths != null) {
                        for (String[] orphan : e.orphanPaths) {
                            this.pendingDeletes.add(new PendingDelete(
                                    encodeMultipartPath(orphan[0], orphan[1]),
                                    now, sinceGen));
                        }
                    }
                    // Notify parked apply threads immediately on failure (issue #370):
                    // wakes threads right after the cycle rather than waiting up to 100 ms
                    // for the next poll — letting them detect the safety timeout sooner.
                    notifySegmentCountMonitor();
                } catch (IOException e) {
                    recordCompactionFailure(VectorIndexCompactor.FailureReason.WRITE_IO);
                    LOGGER.log(Level.WARNING,
                            "vector store " + indexName + ": compaction I/O failure", e);
                    notifySegmentCountMonitor(); // immediate wakeup (issue #370)
                } catch (DataStorageManagerException e) {
                    recordCompactionFailure(VectorIndexCompactor.FailureReason.METADATA_IO);
                    LOGGER.log(Level.WARNING,
                            "vector store " + indexName + ": compaction metadata failure", e);
                    notifySegmentCountMonitor(); // immediate wakeup (issue #370)
                }
            } finally {
                // Drop the temporary BLink storages regardless of outcome so
                // we don't leak pages across cycles.
                this.pendingCompactionDeletes = null;
                try {
                    pendingDeletes.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING,
                            "vector store " + indexName
                                    + ": failed to close pendingCompactionDeletes set", e);
                }
                // authority is null when createTempCompactionAuthorityMap() threw
                // before the try block was entered (issue #372).
                if (authority != null) {
                    try {
                        authority.close();
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING,
                                "vector store " + indexName
                                        + ": failed to close compaction authority map", e);
                    }
                }
            }
        } finally {
            compactionActive.set(0);
            // Issue #535: drain segments queued for close by any concurrent
            // dropSegmentByUuid that landed during this cycle. Doing this
            // BEFORE releasing compactionLock guarantees the close cannot
            // race with this cycle's iteration of candidates (the snapshot
            // is fully consumed by now), and that the next cycle to acquire
            // compactionLock starts from an empty queue.
            drainPendingSegmentClosesUnderCompactionLock();
            compactionLock.unlock();
            // Belt-and-suspenders notify (issue #370): all normal and checked-exception
            // exit paths call notifySegmentCountMonitor() explicitly above for immediate
            // wakeup.  This backstop ensures unchecked-exception paths (e.g. RuntimeException
            // from createTempPagedPkSet / createTempCompactionAuthorityMap, or unchecked
            // throws from buildAuthorityMap) also signal waiting apply threads rather than
            // leaving them parked until the safety timeout.  A redundant notify is harmless:
            // waiting threads re-check the segment count and immediately re-park if still
            // above the threshold.
            notifySegmentCountMonitor();
        }
    }

    /**
     * Returns an {@link Iterable} that streams primary keys from every live
     * shard at iteration time. Unlike materialising a {@code HashSet} of all
     * live-shard PKs up-front, this lets the caller (the compaction authority
     * map population step) probe them one-by-one without holding all of them
     * in heap simultaneously, which matters for catch-up workloads where the
     * combined live shards can hold millions of PKs (issue #290).
     */
    private Iterable<Bytes> collectLiveShardPksLazy() {
        return () -> liveShards.stream()
                .flatMap(shard -> shard.pkToNode.keySet().stream())
                .iterator();
    }

    private static long countDeadPks(List<VectorSegment> candidates, CompactionAuthorityMap authority) {
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
                Integer owner = authority.getSegmentId(pk);
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
     *
     * <p>When a {@link SegmentPublisher} is wired (segmented-v2 mode), this
     * method also drives the registry-side staged-publish protocol around
     * the local swap:
     * <ol>
     *   <li><b>Stage</b> the merged output as PROVISIONAL in the registry
     *       BEFORE acquiring the local locks.</li>
     *   <li><b>Revalidate</b> every input is still ACTIVE in the registry —
     *       on drift (a concurrent optimizer or another IS deprecated an
     *       input under us), abort with {@code ABORTED_INPUT_GONE}.</li>
     *   <li><b>Persist IndexStatus</b> + in-memory swap (existing logic).</li>
     *   <li><b>Commit</b> the staged znode (PROVISIONAL → ACTIVE) and
     *       <b>CAS-deprecate</b> every input (post-lock, best-effort —
     *       reconcile-on-restart heals failures).</li>
     * </ol>
     *
     * <p>On any abort path (registry stage failure, registry revalidate
     * failure, or in-memory drift), the merged output's already-uploaded
     * multipart files are queued for the existing {@code pendingDeletes}
     * retention reaper via {@link #queueMergedOutputForDeletion} and the
     * merged {@link VectorSegment}'s file handles are released. Without
     * this the abort path would leak up to
     * {@code vector.index.compaction.maxBytes} of remote storage per
     * occurrence — and aborts are by-design steady-state behaviour of the
     * pressure-driven local fallback when the optimizer races the IS.
     *
     * <p><b>Three-stage local protocol (issue #462).</b> Inside the
     * {@code checkpointLock}, the local IndexStatus persist + in-memory swap
     * is split so the slow I/O does not hold {@code stateLock.writeLock()}.
     * Without this split a single {@code writeLock} window spans
     * {@link #persistIndexStatusMultiSegment} (local fsync + remote
     * shared-metadata PUT of every segment's metadata) — at 18&nbsp;K
     * segments that's seconds, long enough to starve concurrent searches
     * waiting on {@code stateLock.readLock()}.
     *
     * <ol>
     *   <li><b>Stage 1 (no writeLock).</b> Validate that every input is still
     *   in {@code segments} (under {@code checkpointLock} no other writer can
     *   be modifying it). Build {@code newSegments}; stamp
     *   {@code mergedOutput.generation = currentIndexStatusGeneration + 1}
     *   so the in-memory state matches what we are about to persist. Queue
     *   inputs for retention-aware deletion (CopyOnWriteArrayList — thread
     *   safe).</li>
     *
     *   <li><b>Stage 2 (no writeLock).</b> Run
     *   {@link #persistIndexStatusMultiSegment} — the slow part. Lock-free
     *   readers continue to see the OLD {@code this.segments} via the volatile
     *   field; their search results are eventually consistent. {@code
     *   currentIndexStatusGeneration} is bumped at the end of this call.</li>
     *
     *   <li><b>Stage 3 (writeLock — microseconds).</b> Atomically: register
     *   the merged output's memory estimate, publish
     *   {@code this.segments = newSegments}, unregister the inputs' memory
     *   estimates, set {@code dirty}.</li>
     * </ol>
     *
     * <p>Failure handling (issue #551): {@code queueSegmentPendingDelete}
     * runs <em>after</em> the post-swap {@code deprecateInputs} succeeds,
     * not in Stage 1. This honours the invariant that input files are
     * deleted only after the switch has been committed to both IndexStatus
     * (Stage 2) AND the ZK registry (post-swap). If {@code deprecateInputs}
     * fails the input files are NOT queued: they remain in MinIO as orphans
     * (intact but no longer referenced by IndexStatus) until a follow-up
     * cleanup mechanism reclaims them. This is strictly safer than the
     * pre-#551 behaviour where files were deleted by the retention reaper
     * regardless of ZK state, producing zombie segments (ZK shows ACTIVE
     * but files are gone) that broke every subsequent optimizer task
     * touching them. {@link #runCompactionCycle}'s existing {@code catch}
     * blocks observe Stage-2/Stage-3 failures and record them as before.
     */
    private void atomicSwapCompactionResult(List<VectorSegment> inputs,
                                            VectorSegment mergedOutput,
                                            long bytesWritten,
                                            long mergedVectorCount)
            throws VectorIndexCompactor.CompactionException, DataStorageManagerException {
        // Pre-lock registry coordination for the IS-local fallback path
        // (companion to the external optimizer). When a publisher is wired up
        // we MUST stage the merged output and revalidate the inputs in the
        // registry BEFORE persisting IndexStatus locally, otherwise a
        // concurrent optimizer may have already deprecated an input under us
        // and we would silently produce a duplicate ACTIVE segment.
        SegmentPublisher publisherSnapshot = this.segmentPublisher;
        List<NewSegmentInfo> stagedInfo = null;
        List<NewSegmentInfo> inputInfosForRegistry = null;
        if (publisherSnapshot != null && mergedOutput != null) {
            // Stamp UUID — same flow as Phase B: durable UUID survives a
            // restart so we cannot double-register the same physical file.
            // mergedOutput.segmentUuid should always be null here (freshly merged),
            // but guard defensively in case a future caller pre-stamps it.
            if (mergedOutput.segmentUuid == null) {
                mergedOutput.segmentUuid = java.util.UUID.randomUUID().toString();
            }
            long nextGen = currentIndexStatusGeneration.get() + 1;
            List<String> mergedFeatureIds = (mergedOutput.onDiskGraph != null)
                    ? RemoteSegmentGraphMerger.featureSetToStringList(
                            mergedOutput.onDiskGraph.getFeatureSet())
                    : null;
            NewSegmentInfo mergedInfo = new NewSegmentInfo(
                    mergedOutput.segmentId, mergedOutput.segmentUuid,
                    mergedOutput.graphFilePath, mergedOutput.graphFileSize,
                    mergedOutput.mapFilePath, mergedOutput.mapFileSize,
                    mergedOutput.estimatedSizeBytes, mergedVectorCount,
                    nextGen, LogSequenceNumber.START_OF_TIME,
                    mergedFeatureIds);
            stagedInfo = java.util.Collections.singletonList(mergedInfo);

            inputInfosForRegistry = new ArrayList<>(inputs.size());
            for (VectorSegment in : inputs) {
                // All segments in v4-format indexes carry a UUID. A null UUID here
                // means a segment was checkpointed before a publisher was ever attached
                // (the very first checkpoint after the publisher feature was enabled).
                // Such segments are not registered in ZK yet; they will be picked up by
                // the next reconcile pass. Skip them in the validate/deprecate set.
                if (in.segmentUuid != null) {
                    java.util.List<String> inFeatureIds =
                            (in.onDiskGraph != null)
                                    ? RemoteSegmentGraphMerger.featureSetToStringList(
                                            in.onDiskGraph.getFeatureSet())
                                    : null;
                    inputInfosForRegistry.add(new NewSegmentInfo(
                            in.segmentId, in.segmentUuid,
                            in.graphFilePath, in.graphFileSize,
                            in.mapFilePath, in.mapFileSize,
                            in.estimatedSizeBytes, /* vectorCount unknown for an existing input */ 0L,
                            in.generation, LogSequenceNumber.START_OF_TIME,
                            inFeatureIds));
                }
            }

            try {
                publisherSnapshot.stageNewSegments(stagedInfo);
            } catch (RuntimeException stageFailed) {
                // Stage failed (e.g. ZK unreachable). Abort the local merge.
                // Stage failure means no PROVISIONAL znode was created (or
                // creation was interrupted), so unstage is a no-op; we MUST
                // still queue the rebuild's already-uploaded multipart files
                // for cleanup, otherwise they leak indefinitely.
                LOGGER.log(Level.WARNING,
                        "vector store {0}: stage of merged segment failed; aborting"
                                + " local compaction merge",
                        new Object[]{indexName});
                queueMergedOutputForDeletion(mergedOutput);
                throw new VectorIndexCompactor.CompactionException(
                        VectorIndexCompactor.FailureReason.METADATA_IO,
                        "stage of merged segment failed: " + stageFailed.getMessage());
            }

            // Revalidate inputs are still ACTIVE in the registry. If a
            // concurrent optimizer (or another IS) already deprecated any
            // input we MUST abort: producing a merged output covering an
            // already-deprecated input would yield two ACTIVE segments
            // covering the same data, with duplicate PKs surfacing on search.
            if (!publisherSnapshot.revalidateInputsActive(inputInfosForRegistry)) {
                try {
                    publisherSnapshot.unstage(stagedInfo);
                } catch (RuntimeException ignored) {
                    // best-effort — reconcile-on-restart will sweep any leftover
                    // PROVISIONAL znode; our merged-output bytes are queued for
                    // local-side deletion via queueMergedOutputForDeletion below.
                }
                queueMergedOutputForDeletion(mergedOutput);
                throw new VectorIndexCompactor.CompactionException(
                        VectorIndexCompactor.FailureReason.ABORTED_INPUT_GONE,
                        "concurrent compactor deprecated at least one input under us");
            }
        }

        checkpointLock.lock();
        try {
            // ---------- STAGE 1 — validate + build (no writeLock). ----------
            // checkpointLock serialises compactions and checkpoints, but
            // does NOT exclude {@link #dropSegmentByUuid} (which takes only
            // {@code stateLock.writeLock()}). A drop landing in the Stage 1
            // → Stage 3 window can therefore mutate {@code this.segments}
            // under our feet. Stage 3 below handles this by REBUILDING the
            // published list from {@code this.segments} read freshly under
            // writeLock (issue #538), so the Stage-1 `newSegments` is now a
            // lower-bound estimate used only for the persist in Stage 2 and
            // for the pre-lock registry coordination above.
            List<VectorSegment> current = this.segments;
            // Canonical pre-sizing: HashSet rounds up to the next power of two,
            // so we want capacity = ceil(size / loadFactor). 0.75f is the
            // default load factor.
            Set<Integer> currentIds = new java.util.HashSet<>(
                    (int) (current.size() / 0.75f) + 1);
            for (VectorSegment s : current) {
                currentIds.add(s.segmentId);
            }
            for (VectorSegment in : inputs) {
                if (!currentIds.contains(in.segmentId)) {
                    // In-memory drift — undo the registry stage if any
                    // and queue the merged output's bytes for cleanup so
                    // they don't leak. Without this branch the abort path
                    // would leak up to vector.index.compaction.maxBytes
                    // of remote storage per occurrence.
                    if (publisherSnapshot != null && stagedInfo != null) {
                        try {
                            publisherSnapshot.unstage(stagedInfo);
                        } catch (RuntimeException ignored) {
                            // best-effort — reconcile-on-restart will sweep
                        }
                    }
                    queueMergedOutputForDeletion(mergedOutput);
                    throw new VectorIndexCompactor.CompactionException(
                            VectorIndexCompactor.FailureReason.ABORTED_INPUT_GONE,
                            "input segment " + in.segmentId + " disappeared from segment list");
                }
            }

            Set<Integer> inputIds = new java.util.HashSet<>(
                    (int) (inputs.size() / 0.75f) + 1);
            for (VectorSegment in : inputs) {
                inputIds.add(in.segmentId);
            }
            List<VectorSegment> newSegments =
                    new java.util.concurrent.CopyOnWriteArrayList<>();
            for (VectorSegment s : current) {
                if (!inputIds.contains(s.segmentId)) {
                    newSegments.add(s);
                }
            }
            if (mergedOutput != null) {
                // Fresh generation will be assigned by
                // persistIndexStatusMultiSegment; we set it now so the
                // in-memory state matches what we persist.
                mergedOutput.generation = currentIndexStatusGeneration.get() + 1;
                newSegments.add(mergedOutput);
            }

            // Issue #551: Inputs are NOT queued for retention-aware deletion here.
            // The queue is deferred to the post-swap block, after deprecateInputs()
            // succeeds (i.e. after the ZK registry has confirmed the inputs are
            // DEPRECATED). The previous ordering (queue BEFORE persist) created a
            // zombie-segment failure mode: if deprecateInputs() failed in the
            // post-swap block, the inputs remained ACTIVE in ZK but their files
            // were physically deleted by the retention reaper after
            // vectorIndexCompactionRetentionMs (10 minutes by default). Every
            // optimizer task subsequently targeting these phantom-ACTIVE segments
            // would fail to read their files — the symptom that motivated this
            // issue (4,222+ failed merges on bigann-100M).
            //
            // The new ordering is: persist IndexStatus (Stage 2), publish to
            // this.segments (Stage 3), then commit ZK ACTIVE→DEPRECATED, and ONLY
            // THEN queue files for retention-aware deletion. This honours the
            // invariant that files are deleted only after the "switch to the new
            // segments" is committed to metadata (both IndexStatus AND ZK).

            Runnable postBuildHook = atomicSwapPostBuildHook;
            if (postBuildHook != null) {
                postBuildHook.run();
            }

            // ---------- STAGE 2 — persist (no writeLock; the slow I/O). ----------
            // checkpointLock is held; concurrent compaction/checkpoint cannot
            // run. Concurrent searches read the OLD this.segments via the
            // volatile field — eventually consistent until Stage 3 publishes.
            List<VectorSegment> allSealedForPersist = new ArrayList<>(newSegments);
            persistIndexStatusMultiSegment(
                    allSealedForPersist,
                    java.util.Collections.emptyList(),
                    java.util.Collections.emptyList(),
                    LogSequenceNumber.START_OF_TIME);

            Runnable postPersistHook = atomicSwapPostPersistHook;
            if (postPersistHook != null) {
                postPersistHook.run();
            }

            // ---------- STAGE 3 — atomic publish (writeLock for ~µs). ----------
            // Issue #455: keep the on-disk-segment memory counter in sync
            // with the segments-list swap. The register/publish/unregister
            // triple runs under one writeLock so the counter and the
            // segments list flip together; concurrent addVector readers
            // (which take the readLock) cannot observe a half-updated state.
            long stage3Start = System.nanoTime();
            stateLock.writeLock().lock();
            try {
                // Re-apply pendingCompactionDeletes (issue #462 + #538):
                // the late-deletes reapply happens BELOW, AFTER
                // finalNewSegments is built, so the apply covers the full
                // set of segments being published — mergedOutput AND any
                // segments preserved from `this.segments` (including
                // adoptions that landed in the Stage 1 → Stage 3 window).
                // The reapply must run BEFORE the publish so a throw
                // unwinds with both the counter and `this.segments`
                // unchanged. See the corresponding block below.
                //
                // Issue #538 (root cause of the residual race tracked in
                // issue #537): REBUILD `finalNewSegments` from the CURRENT
                // `this.segments` rather than publishing the stale Stage-1
                // `newSegments`. Between Stage 1's snapshot and this Stage 3
                // window a concurrent {@link #dropSegmentByUuid} (typically
                // fired by the optimizer-watcher's {@code onSegmentReleased}
                // when the optimizer deprecates a NON-input segment in ZK)
                // may have removed segments from {@code this.segments} and
                // queued them on {@link #pendingSegmentCloses}. Publishing
                // the Stage-1 `newSegments` would silently RE-INSERT those
                // dropped segments — the cycle's finally drain would then
                // close them, leaving an orphan in {@code this.segments}
                // with {@code onDiskGraph == null}. That orphan trips the
                // null-check at {@code VectorIndexCompactor
                // .rebuildSegmentStreaming:780} on every subsequent cycle,
                // producing the persistent failure loop reported in issues
                // #535 and #538.
                //
                // The rebuild here is correct because:
                //   - we EXCLUDE all `inputs` by segmentId (the swap's intent);
                //   - we INCLUDE everything else from the CURRENT
                //     `this.segments`, so segments dropped between Stage 1
                //     and now are NOT republished AND segments adopted
                //     between Stage 1 and now (via
                //     {@link #adoptExternalSegment}) ARE preserved;
                //   - we add `mergedOutput` exactly once.
                //
                // The persisted IndexStatus from Stage 2 may diverge from
                // the published `this.segments` if a drop or adoption
                // landed in the Stage 1 → Stage 3 window. The next
                // checkpoint repersists consistently because the two
                // mutating callers ({@link #dropSegmentByUuid} and
                // {@link #adoptExternalSegment}) both call
                // {@code dirty.set(true)} themselves. In the (short) window
                // between this swap and the next checkpoint a restart would
                // load a stale IndexStatus referencing a segment whose
                // multipart files were already deleted by the optimizer.
                // {@code start()}'s {@code loadFromStatus} is all-or-nothing
                // (it runs BEFORE {@link #reconcileWithIndexStatus}), so
                // it would FAIL on the deleted segment and the IS process
                // would crash-loop until manual operator intervention OR
                // until the optimizer's retention has not yet expired
                // (typical retention >> checkpoint cadence, so this is
                // unlikely in practice). The pre-fix code had exactly the
                // same restart-window risk via the orphan-in-segments
                // state (the loaded segment would have
                // {@code onDiskGraph==null} and crash on first use); the
                // fix does not enlarge it.
                // ArrayList + final wrap keeps the writeLock window tight
                // vs. {@code CopyOnWriteArrayList.add}-in-loop (each add
                // re-copies the entire backing array — O(N²)).
                List<VectorSegment> currentAtStage3 = this.segments;
                int expectedSize = currentAtStage3.size() - inputs.size()
                        + (mergedOutput != null ? 1 : 0);
                ArrayList<VectorSegment> finalNewSegments =
                        new ArrayList<>(Math.max(0, expectedSize));
                for (VectorSegment s : currentAtStage3) {
                    if (!inputIds.contains(s.segmentId)) {
                        finalNewSegments.add(s);
                    }
                }
                if (mergedOutput != null) {
                    finalNewSegments.add(mergedOutput);
                }

                // Issue #538 (pr-reviewer pass-5 follow-up): reapply
                // `pendingCompactionDeletes` to ALL segments being
                // published, not just {@code mergedOutput}.
                //
                // The PRE-fix-5 reapply applied only to {@code mergedOutput}:
                // a remove that lands DURING the compaction cycle would
                // have been:
                //   (a) applied to existing on-disk segments by
                //       removeVector's direct iteration of
                //       {@code this.segments} at the time of the call;
                //   (b) added to {@code pendingCompactionDeletes} for
                //       the lock-free re-apply at the end of Stage 1
                //       (which targeted mergedOutput, the only segment
                //       removeVector couldn't iterate); AND
                //   (c) re-applied to mergedOutput here under writeLock
                //       to catch arrivals after Stage 1's lock-free
                //       forEach.
                //
                // With round-1 preserving adoptions in finalNewSegments,
                // an adoption landing AFTER a removeVector(P) but BEFORE
                // Stage 3 was MISSED by step (a) (the adopted segment
                // was not yet in {@code this.segments} when removeVector
                // ran) and by step (c) (the reapply targeted only
                // mergedOutput). Result: P stayed live in the adopted
                // segment after Stage 3 — a silent removeVector loss.
                //
                // The structural twin of the Phase C Stage 2 fix at
                // line 5119-5128 (issue #538 pr-reviewer pass-3): iterate
                // {@code finalNewSegments} so the reapply covers
                // adoptions. `seg.deletePk` is idempotent; for pks
                // already deleted by Stage 1's lock-free forEach,
                // the iteration silently returns false on each segment.
                //
                // ORDERING (matching round-4 Phase C Stage 2): the
                // reapply runs BEFORE
                // {@code registerSegmentMemoryEstimate(mergedOutput)}
                // AND BEFORE the {@code this.segments = ...} publish
                // AND BEFORE the {@code unregisterSegmentMemoryEstimate}
                // loop over inputs. A throw from `seg.deletePk` leaves
                // both the counter and `this.segments` unchanged.
                if (!finalNewSegments.isEmpty()) {
                    PagedPkSet lateDeletesReapply = this.pendingCompactionDeletes;
                    if (lateDeletesReapply != null) {
                        lateDeletesReapply.forEach(pk -> {
                            for (VectorSegment seg : finalNewSegments) {
                                if (seg.deletePk(pk)) {
                                    return;
                                }
                            }
                        });
                    }
                }

                if (mergedOutput != null) {
                    registerSegmentMemoryEstimate(mergedOutput);
                }
                this.segments = new java.util.concurrent.CopyOnWriteArrayList<>(
                        finalNewSegments);
                for (VectorSegment in : inputs) {
                    unregisterSegmentMemoryEstimate(in);
                }
                // Note: we deliberately do NOT add an explicit drift-detection
                // dirty bit here. The two mutators that produce drift —
                // {@link #dropSegmentByUuid} and {@link #adoptExternalSegment}
                // — both call {@code dirty.set(true)} themselves, so the
                // next checkpoint already runs to repersist IndexStatus
                // consistently. A drift bit based on
                // {@code finalNewSegments.size() != newSegments.size()}
                // would miss the (rare but possible) "1 drop + 1 adoption"
                // case anyway; a set-based check would be correct but
                // redundant.
                dirty.set(dirty.get() || totalLiveSize() > 0);
            } finally {
                stateLock.writeLock().unlock();
            }
            lastCompactionSwapWriteLockNanos.set(System.nanoTime() - stage3Start);

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

        // Post-swap registry coordination. The IndexStatus is durable; if any
        // of these calls fails the registry will be inconsistent until
        // reconcile-on-restart (commit) or the next optimizer tick (deprecate).
        // Best-effort by design — we never roll back a durable IndexStatus
        // change because of a registry hiccup.
        //
        // Issue #551: Input files are queued for retention-aware deletion HERE
        // (not in Stage 1 as before), and only after the ZK registry confirms
        // the inputs are DEPRECATED. This honours the invariant that files are
        // deleted only after the "switch to the new segments" is committed to
        // both metadata stores (IndexStatus + ZK). See the Stage 1 block above
        // for the full rationale.
        //
        // The variable defaults to false: input files are queued for deletion
        // ONLY when one of three explicit paths flips it to true (publisher
        // null, null-UUID inputs, or a successful deprecateInputs). Any new
        // code path that exits this block without explicit handling will
        // therefore default to "do not delete" — fail-safe.
        boolean inputsSafeToDelete = false;
        if (publisherSnapshot == null) {
            // No publisher: no ZK metadata to coordinate. IndexStatus is the
            // sole metadata store and is already durable (Stage 2), so the
            // switch is fully committed. Safe to delete input files.
            inputsSafeToDelete = true;
        } else if (stagedInfo == null) {
            // Publisher attached, but no merged output was ever staged. There
            // are two ways to land here:
            //   (a) `mergedOutput == null` — empty-result compaction (every
            //       PK in the inputs was tombstoned; the rebuild produced
            //       nothing). The inputs were nevertheless removed from
            //       `this.segments` and from IndexStatus.
            //   (b) `mergedOutput != null` but the caller never built
            //       `stagedInfo` for it (defensive; should not happen with
            //       the current code path).
            // In BOTH cases we did NOT call deprecateInputs on the inputs,
            // so ZK still shows them as ACTIVE. Deleting their files here
            // would re-create the zombie-segment hazard this PR is closing.
            // Leave `inputsSafeToDelete = false` and let the optimizer fold
            // the orphan ACTIVE inputs on its next tick (the same recovery
            // path used when deprecateInputs fails — see below).
            if (!inputs.isEmpty()) {
                LOGGER.log(Level.WARNING,
                        "vector store {0}: empty-result / unstaged compaction with"
                                + " publisher attached — {1} input segments remain"
                                + " ACTIVE in ZK; their files will NOT be queued for"
                                + " deletion to avoid zombie-segment failure. Optimizer"
                                + " will fold orphan ACTIVE inputs on next tick.",
                        new Object[]{indexName, inputs.size()});
            }
        } else {
            // publisher != null && stagedInfo != null: the standard
            // merged-output path.
            try {
                publisherSnapshot.commitStagedSegments(stagedInfo);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING,
                        "vector store {0}: commit of merged segment failed; reconcile"
                                + " on restart will heal: {1}",
                        new Object[]{indexName, e.getMessage()});
            }
            if (inputInfosForRegistry != null && !inputInfosForRegistry.isEmpty()
                    && mergedOutput != null) {
                long retentionUntil = System.currentTimeMillis()
                        + vectorIndexCompactionRetentionMs;
                try {
                    publisherSnapshot.deprecateInputs(inputInfosForRegistry,
                            mergedOutput.segmentUuid, retentionUntil);
                    // ZK now shows the inputs as DEPRECATED. The switch is fully
                    // committed (IndexStatus + ZK) so it is safe to schedule the
                    // input files for deletion.
                    inputsSafeToDelete = true;
                } catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING,
                            "vector store {0}: deprecate of {1} input segments failed;"
                                    + " input files will NOT be queued for deletion to"
                                    + " avoid zombie-segment failure (files would be"
                                    + " deleted while ZK still shows ACTIVE). Optimizer"
                                    + " will fold orphan ACTIVE inputs on next tick: {2}",
                            new Object[]{indexName, inputInfosForRegistry.size(),
                                    e.getMessage()});
                    // Intentionally leave inputsSafeToDelete = false. The input
                    // files stay in MinIO as orphans-but-intact (no zombie). The
                    // optimizer's existing orphan-ACTIVE fold path (see
                    // IndexOptimizerEngine) reclaims them on the next merge
                    // covering this region — typically minutes, not indefinite.
                }
            } else {
                // Either inputInfosForRegistry is empty (all inputs had null
                // segmentUuid — the very-first-checkpoint-with-publisher case;
                // no ZK record exists to be inconsistent with) or mergedOutput
                // is null (caught by the stagedInfo == null branch above; this
                // path is defensive only). Safe to queue: there is nothing in
                // ZK to leave behind.
                inputsSafeToDelete = true;
            }
        }

        if (inputsSafeToDelete) {
            for (VectorSegment in : inputs) {
                queueSegmentPendingDelete(in, vectorIndexCompactionRetentionMs);
            }
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

    /**
     * Package-private accessor for the configured scratch directory. Used by
     * {@link VectorIndexCompactor#rebuildSegmentStreaming} (issue #485) when
     * it needs a local temp file for the merged graph + map files.
     */
    Path tmpDirectory() {
        return tmpDirectory;
    }

    /**
     * Package-private accessor for the tablespace UUID. Used by
     * {@link VectorIndexCompactor} to construct the correct storage key for
     * downloading source segment graph files during streaming compaction (issue #602).
     */
    String tableSpaceUUID() {
        return tableSpaceUUID;
    }

    /**
     * Package-private accessor for the data storage manager. Used by
     * {@link VectorIndexCompactor} to eagerly download source segment graph
     * files to local temp files before the merge hot path (issue #602).
     */
    herddb.storage.DataStorageManager dataStorageManager() {
        return dataStorageManager;
    }

    SegmentWriteResult writeSyntheticShard(LiveGraphShard syntheticShard,
                                           int segmentId,
                                           int dim) throws IOException, DataStorageManagerException {
        return writeShardAsFusedPQSegment(syntheticShard, segmentId, dim);
    }

    /**
     * Uploads a streaming-compaction output (issue #485). The merged graph
     * and map files have already been produced locally by
     * {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor}
     * (graph) and {@code VectorIndexCompactor.writeStreamingCompactedMapFile}
     * (map). This helper just uploads both via the multipart API and mirrors
     * the bookkeeping that {@link #writeShardAsFusedPQSegment} performs:
     * compaction-progress counters, multipart provisional-file tracking, and
     * a {@link SegmentWriteResult} suitable for handing back to
     * {@link #preloadCompactedSegment}.
     *
     * <p>The caller is responsible for deleting both temp files (their
     * lifecycle is owned by the streaming rebuild loop).
     */
    SegmentWriteResult writeStreamingCompactedSegment(Path graphTempFile,
                                                      Path mapTempFile,
                                                      int segmentId,
                                                      long vectorCount)
            throws IOException, DataStorageManagerException {
        if (graphTempFile == null || mapTempFile == null) {
            throw new IllegalArgumentException(
                    "writeStreamingCompactedSegment: graph and map temp files must be non-null");
        }
        compactionNodesTotal.addAndGet(vectorCount);
        writingGraphActive.incrementAndGet();
        try {
            String segUuid = indexUUID + "_seg" + segmentId;

            // Upload graph
            long graphSize = Files.size(graphTempFile);
            uploadBytesTotal.addAndGet(graphSize);
            String graphFilePath;
            uploadingActive.incrementAndGet();
            try {
                graphFilePath = dataStorageManager.writeMultipartIndexFile(
                        tableSpaceUUID, segUuid, "graph",
                        graphTempFile, uploadBytesDone::addAndGet);
            } finally {
                uploadingActive.decrementAndGet();
            }
            // Issue #616 review item: track-after-success here is INTENTIONAL and
            // benign. writeStreamingCompactedSegment runs on the compaction code
            // path, where provisionalMultipartFiles is NOT installed (only Phase B
            // of indexCheckpoint installs it; see the only assignment at the top
            // of doCheckpointFusedPQThreePhase). trackProvisionalMultipartFile is
            // therefore a no-op on this path — both pre- and post-track placements
            // are equivalent here. Compaction's own orphan-cleanup mechanism is
            // independent: VectorIndexCompactor.rebuildSegmentStreaming wraps any
            // failure from writeStreamingCompactedSegment into a CompactionException
            // carrying a failureOrphans list (see VectorIndexCompactor.java around
            // line 1330), which runCompactionCycle's catch arm then queues for the
            // retention-aware pendingDeletes reaper. Do NOT pre-track here without
            // first removing that orphan-list mechanism, or both pipelines would
            // attempt to delete the same files.
            trackProvisionalMultipartFile(segUuid, "graph");

            // Upload map
            long mapSize = Files.size(mapTempFile);
            uploadBytesTotal.addAndGet(mapSize);
            String mapFilePath;
            uploadingActive.incrementAndGet();
            try {
                mapFilePath = dataStorageManager.writeMultipartIndexFile(
                        tableSpaceUUID, segUuid, "map",
                        mapTempFile, uploadBytesDone::addAndGet);
            } finally {
                uploadingActive.decrementAndGet();
            }
            // Issue #616 review item: see the matching comment on the graph
            // upload above — track-after-success is intentional on the compaction
            // path because provisionalMultipartFiles is not installed here.
            trackProvisionalMultipartFile(segUuid, "map");
            compactionNodesDone.addAndGet(vectorCount);
            return new SegmentWriteResult(segmentId,
                    graphFilePath, graphSize,
                    mapFilePath, mapSize,
                    graphSize + mapSize,
                    vectorCount);
        } finally {
            writingGraphActive.decrementAndGet();
        }
    }

    /**
     * Invalidates {@link #cachedPQ} (issue #485). Called after a streaming
     * compaction successfully swaps in a merged segment whose PQ codebook
     * was retrained internally by jvector's {@code PQRetrainer} on a balanced
     * sample of the compaction inputs. The cached codebook now reflects a
     * stale distribution; nulling it forces the next FusedPQ-eligible
     * checkpoint to retrain (one extra K-Means run) instead of reusing the
     * stale codebook.
     */
    void invalidateCachedPq() {
        this.cachedPQ = null;
        this.pqSegmentsSinceTraining.set(0);
    }

    /**
     * Test-only accessor: returns the currently-cached PQ codebook (or
     * {@code null} when none). Used by streaming-compaction tests (issue
     * #485) to assert that {@link #invalidateCachedPq()} fires after a
     * successful streaming swap.
     */
    ProductQuantization getCachedPqForTest() {
        return cachedPQ;
    }

    /**
     * Process-wide switch for the streaming compaction engine introduced
     * in issue #485. When {@code true}, vector-index compaction is driven
     * by jvector's {@code OnDiskGraphIndexCompactor} (memory bounded by
     * {@code O(taskWindowSize × maxDegree × float[dim])}); when {@code
     * false}, the legacy in-memory {@code GraphIndexBuilder} rebuild path
     * is used instead.
     *
     * <p>Public façade over {@link VectorIndexCompactor#streamingCompactionEnabled}
     * (the underlying class is package-private). Called by
     * {@code IndexingServiceEngine.start()} when the
     * {@code vector.index.compaction.streaming.enabled} config key is
     * resolved at IS startup. Operators may also set the flag via the
     * {@code herddb.vectorindex.streamingCompactionEnabled} system
     * property — the config key wins.
     */
    public static void setStreamingCompactionEnabled(boolean enabled) {
        VectorIndexCompactor.streamingCompactionEnabled = enabled;
    }

    /** Returns the current value of {@link #setStreamingCompactionEnabled(boolean)}. */
    public static boolean isStreamingCompactionEnabled() {
        return VectorIndexCompactor.streamingCompactionEnabled;
    }

    /**
     * Process-wide switch for the jvector async IO pipeline introduced in
     * issue #547. When {@code true}, {@link VectorSegment#search} calls
     * {@link io.github.jbellis.jvector.graph.GraphSearcher#setAsyncPipelineEnabled(boolean)
     * setAsyncPipelineEnabled(true)} on every {@code GraphSearcher} before each
     * search, activating the 2-slot speculative read pipeline on the FusedPQ
     * layer-0 path. Disabled by default until a production profile confirms a
     * net win (see issue #547 acceptance criteria).
     *
     * <p>Called by {@code IndexingServiceEngine.start()} when the
     * {@code indexing.vector.search.asyncPipelineEnabled} config key is resolved
     * at IS startup. Operators may also set the flag via the
     * {@code herddb.vectorindex.searchAsyncPipelineEnabled} system property —
     * the config key wins.
     */
    static volatile boolean searchAsyncPipelineEnabled = false;

    /** Enables or disables the jvector async IO pipeline (issue #547). */
    public static void setSearchAsyncPipelineEnabled(boolean enabled) {
        searchAsyncPipelineEnabled = enabled;
    }

    /** Returns the current value of {@link #setSearchAsyncPipelineEnabled(boolean)}. */
    public static boolean isSearchAsyncPipelineEnabled() {
        return searchAsyncPipelineEnabled;
    }

    /**
     * Test-only accessor: returns a defensive snapshot of the on-disk
     * segment list. Used by streaming-compaction tests (issue #485) to walk
     * the merged segment's {@code pkOffsets / pkLengths / pkData} and assert
     * deletion equivalence + dense-ordinal output.
     */
    List<VectorSegment> getOnDiskSegmentsSnapshotForTest() {
        return new ArrayList<>(segments);
    }

    /**
     * Returns the current number of on-disk (sealed) segments held by this store.
     * Useful for cross-module tests that need to observe the effect of
     * {@link #adoptExternalSegment} / {@link #dropSegmentByUuid} without
     * depending on the package-private {@link VectorSegment} type.
     */
    public int getOnDiskSegmentCount() {
        return segments.size();
    }

    /**
     * Test-only accessor: returns the {@code externalStorageKey} of the
     * on-disk segment at the given list index (0-based). Used by
     * {@code OptimizerMergeAdoptionRecallTest.adoptedSegmentSurvivesRestart}
     * to verify that the V5 IndexStatus round-trip preserves the
     * {@code externalStorageKey} field so a restart reattaches to the
     * same optimizer-produced multipart files.
     *
     * @param idx 0-based position in the current segments list
     * @return the {@code externalStorageKey} string, or {@code null} for
     *         IS-locally-produced segments
     * @throws IndexOutOfBoundsException if {@code idx >= getOnDiskSegmentCount()}
     */
    public String getOnDiskSegmentExternalStorageKeyForTest(int idx) {
        return segments.get(idx).externalStorageKey;
    }

    VectorSegment preloadCompactedSegment(SegmentWriteResult swr) throws IOException, DataStorageManagerException {
        VectorSegment seg = new VectorSegment(swr.segmentId);
        seg.segmentUuid = swr.segmentUuid;
        seg.estimatedSizeBytes = swr.estimatedSizeBytes;
        seg.graphFilePath = swr.graphFilePath;
        seg.graphFileSize = swr.graphFileSize;
        seg.mapFilePath = swr.mapFilePath;
        seg.mapFileSize = swr.mapFileSize;
        Path mapFile = readMultipartMapDataToTempFile(seg);
        loadFusedPQSegment(seg, mapFile, dimension, nextNodeId.get());
        return seg;
    }

    /**
     * Adopts an externally-produced segment (e.g. output of the index-optimizer service)
     * into this store's active segment list. The segment's graph and map multipart files
     * must already be present in the underlying {@link herddb.storage.DataStorageManager}
     * under the key {@code indexUUID + "_seg" + externalSegmentId}.
     *
     * <p>The heavy IO (map-file download + graph loading) is performed outside the
     * write lock to avoid blocking concurrent search queries. A second idempotency check
     * under the write lock guards against duplicate adoption if the watcher fires twice
     * for the same segment.
     *
     * <p>If {@code dimension} is not yet initialised (i.e. the store has never
     * checkpointed), an {@link IllegalStateException} is thrown — the optimizer cannot
     * produce merged segments before the IS has produced at least one checkpoint, so
     * this state is unexpected and should be surfaced.
     */
    @Override
    public boolean adoptExternalSegment(String segmentUuid, long externalSegmentId,
            String graphFilePath, long graphFileSize,
            String mapFilePath, long mapFileSize,
            long generation) throws IOException, DataStorageManagerException {
        int snapshotDimension = dimension;
        if (snapshotDimension == 0) {
            throw new IllegalStateException(
                    "adoptExternalSegment called on store " + indexName
                            + " before dimension is known — store must have checkpointed at least once");
        }
        // Quick idempotency check outside the write lock (avoids IO if already loaded).
        for (VectorSegment existing : segments) {
            if (segmentUuid.equals(existing.segmentUuid)) {
                LOGGER.log(Level.FINE,
                        "adoptExternalSegment: segment {0} already loaded in store {1}; skipping",
                        new Object[]{segmentUuid, indexName});
                return false;
            }
        }

        // Use the adopted-segment ID counter that counts DOWN from Integer.MAX_VALUE.
        // This keeps adopted-segment BLink storage keys
        // (indexUUID + "_seg" + localSegId + "_pktonode") in the [MAX_INT, ...] space,
        // completely disjoint from IS-locally-produced segment keys in the [0, ...] space.
        // No lock is needed here: the two counters never share range, so even a concurrent
        // checkpoint wipe that resets nextSegmentId to 0 cannot cause a collision with
        // this adoption's localSegId.
        int localSegId = nextAdoptedSegmentLocalId.getAndDecrement();
        VectorSegment tentative = new VectorSegment(localSegId);
        tentative.segmentUuid = segmentUuid;
        tentative.externalStorageKey = indexUUID + "_seg" + externalSegmentId;
        tentative.graphFilePath = graphFilePath;
        tentative.graphFileSize = graphFileSize;
        tentative.mapFilePath = mapFilePath;
        tentative.mapFileSize = mapFileSize;
        tentative.generation = generation;
        tentative.estimatedSizeBytes = graphFileSize + mapFileSize;

        // The caller may be a ZK-watcher dispatch thread. NIO FileChannel reads
        // will throw ClosedByInterruptException if the thread's interrupt flag is
        // already set (e.g. from a previous ScheduledExecutorService shutdown in a
        // co-running test, or from a benign wakeup on the dispatch thread). Clearing
        // the flag before the IO section prevents spurious failures; the flag is
        // restored afterwards so any legitimate "please stop" signal is not lost.
        boolean wasInterrupted = Thread.interrupted();
        boolean loadedOk = false;
        try {
            Path mapFile = readMultipartMapDataToTempFile(tentative);
            loadFusedPQSegment(tentative, mapFile, snapshotDimension, nextNodeId.get());
            loadedOk = true;
        } finally {
            if (wasInterrupted) {
                // Restore the interrupt flag so callers/wrappers can observe it.
                Thread.currentThread().interrupt();
            }
            if (!loadedOk) {
                try {
                    tentative.close();
                } catch (RuntimeException closeEx) {
                    // Narrow catch: close() throws only RuntimeException from BLink cleanup.
                    LOGGER.log(Level.WARNING,
                            "adoptExternalSegment: error closing tentative segment " + segmentUuid
                                    + " after load failure in store " + indexName, closeEx);
                }
                // Reclaim the BLink storage entry that loadFusedPQSegment may have created
                // via createSegmentBLinks before the failure. If readMultipartMapDataToTempFile
                // failed before loadFusedPQSegment ran, dropSegmentBLinkStorage is a no-op
                // (DataStorageManagerException is caught and logged internally).
                dropSegmentBLinkStorage(tentative);
            }
        }

        // Issue #569: an adopted segment is published with warmedUp == false
        // (the VectorSegment default). It is therefore warmed by the next
        // post-checkpoint warm-all sweep (warmUpBlockCache), which is
        // idempotent and skips already-warmed segments. We deliberately do
        // NOT warm inline here: adoption runs on a ZK-watcher dispatch thread
        // and is rare, the checkpoint/compaction creation paths cover the
        // warm-before-publish guarantee for IS-produced segments, and the
        // sweep keeps adopted segments converging to "warm" without adding
        // I/O to the watcher thread.

        // Under write lock: re-check idempotency, then publish.
        boolean alreadyAdopted = false;
        stateLock.writeLock().lock();
        try {
            for (VectorSegment existing : segments) {
                if (segmentUuid.equals(existing.segmentUuid)) {
                    // Adopted by a concurrent adoption or watcher re-fire; discard
                    // tentative AFTER releasing the lock to avoid IO under writeLock.
                    alreadyAdopted = true;
                    return false;
                }
            }
            List<VectorSegment> newList = new java.util.concurrent.CopyOnWriteArrayList<>(segments);
            newList.add(tentative);
            segments = newList;
            registerSegmentMemoryEstimate(tentative);
            // Mark the store dirty so the next checkpoint() call serialises the new
            // segment list (including the adopted segment's externalStorageKey) to the
            // IndexStatus. Without this, the checkpoint gate treats adoption as a
            // no-op and the adopted segment is invisible on restart.
            dirty.set(true);
            LOGGER.log(Level.INFO,
                    "adoptExternalSegment: loaded segment {0} (storageKey={1}, generation={2})"
                            + " into store {3}; total on-disk segments now {4}",
                    new Object[]{segmentUuid, tentative.externalStorageKey,
                            generation, indexName, segments.size()});
        } finally {
            stateLock.writeLock().unlock();
            if (alreadyAdopted) {
                // Close the tentative segment and reclaim its BLink storage outside
                // the writeLock to avoid holding the lock during potential IO.
                tentative.close();
                // Reclaim the BLink storage entry materialised by createSegmentBLinks
                // inside loadFusedPQSegment (which completed successfully before the
                // duplicate was detected under the writeLock).
                dropSegmentBLinkStorage(tentative);
            }
        }
        return true;
    }

    /**
     * Removes a segment from the active list by its UUID and releases its resources.
     * Called when the segment-assignment watcher reports that a segment previously owned
     * by this IS instance has been deprecated (e.g. superseded by an optimizer merge).
     *
     * <p>Idempotent: if the UUID is not present, this method returns immediately.
     * Does <em>not</em> queue the segment's files for deletion — the optimizer manages
     * the file lifecycle for segments it produced.
     */
    @Override
    public void dropSegmentByUuid(String segmentUuid) {
        try {
            VectorSegment found = null;
            stateLock.writeLock().lock();
            try {
                List<VectorSegment> newList = new java.util.concurrent.CopyOnWriteArrayList<>();
                for (VectorSegment s : segments) {
                    if (segmentUuid.equals(s.segmentUuid)) {
                        found = s;
                    } else {
                        newList.add(s);
                    }
                }
                if (found == null) {
                    return;
                }
                segments = newList;
                // Mark dirty so the next checkpoint() serialises the reduced segment list.
                // Without this, dropping a segment leaves a stale entry in the IndexStatus
                // on disk (the checkpoint gate would see no changes and skip).
                dirty.set(true);
                LOGGER.log(Level.INFO,
                        "dropSegmentByUuid: removed segment {0} from store {1};"
                                + " total on-disk segments now {2}",
                        new Object[]{segmentUuid, indexName, segments.size()});
                // Issue #535: do NOT close `found` synchronously here. A concurrent
                // runCompactionCycle may have snapshotted `segments` (volatile read,
                // no readLock) at the start of the cycle and may still hold a live
                // reference to `found` via its `candidates` list. Closing `found`
                // immediately would null out `found.onDiskGraph` (the only place that
                // field is ever written to null — see {@link VectorSegment#close})
                // and the cycle's next iteration of `candidates` in
                // {@link VectorIndexCompactor#rebuildSegmentStreaming} (line ~780)
                // would throw {@code CompactionException(CORRUPTION,
                // "candidate segment N has no on-disk graph (streaming compaction)")},
                // matching the exact failure observed during the bigann-100M k3s
                // benchmark (issue #535).
                //
                // Instead, queue `found` for deferred close. The drainer is guarded
                // by {@link #compactionLock} so it cannot run while a cycle is in
                // progress — i.e. close happens AFTER any in-flight cycle has
                // released its references. The drain also calls
                // {@code unregisterSegmentMemoryEstimate} on `found` (review item:
                // counter and actual heap must release in lockstep — releasing the
                // counter here would create a window where the counter is low but
                // the segment's BLink pages / pkData arrays are still resident).
                //
                // Search safety: searchInternal snapshots `this.segments` under
                // stateLock.readLock() and uses the VectorSegment objects under the
                // same readLock. The writeLock above guarantees no readLock holder
                // overlaps the segment-list mutation, so any concurrent searcher
                // either (a) finishes before we publish `newList` (writeLock waits
                // for outstanding readLock holders) or (b) starts after we publish
                // `newList` and never sees `found`. Either way, no search will
                // dereference `found.onDiskGraph` after we drain. The
                // {@code atomicSwapCompactionResult} Stage-3 rebuild and the
                // {@code doCheckpointFusedPQThreePhase} Phase C Stage 2 rebuild
                // (both fixed by issue #538) read `this.segments` under the
                // writeLock and never re-introduce a removed segment.
                pendingSegmentCloses.add(found);
            } finally {
                stateLock.writeLock().unlock();
            }
        } finally {
            // Issue #535 / pr-reviewer follow-up: drain in finally so the queue
            // is also drained when `found == null` (a no-op lookup that follows
            // an earlier successful drop), AND when an exception inside the
            // writeLock body unwinds. Without this, a previous drop's queued
            // close could be stranded until the next compaction cycle runs.
            //
            // The drain is outside the writeLock to keep the writeLock window
            // tight and avoid lock-order inversion against compactionLock.
            drainPendingSegmentClosesOpportunistically();
        }
    }

    /**
     * Issue #617: removes a segment from the active list by its
     * <em>multipart storage key</em>. Shares the same lock discipline and
     * deferred-close machinery as {@link #dropSegmentByUuid}.
     *
     * <p>Matching is by {@link #segmentStorageKey(VectorSegment)}, so this
     * method works for both IS-locally-produced segments (legacy key
     * {@code indexUUID + "_seg" + segmentId}) and adopted ones
     * ({@code externalStorageKey}).
     *
     * <p>Idempotent: when no segment matches, returns
     * {@link SegmentDropResult#NOT_FOUND} without mutating state.
     */
    @Override
    public SegmentDropResult dropSegmentByStorageKey(String storageKey) {
        if (storageKey == null || storageKey.isEmpty()) {
            return SegmentDropResult.NOT_FOUND;
        }
        try {
            VectorSegment found = null;
            stateLock.writeLock().lock();
            try {
                List<VectorSegment> newList = new java.util.concurrent.CopyOnWriteArrayList<>();
                for (VectorSegment s : segments) {
                    if (found == null && storageKey.equals(segmentStorageKey(s))) {
                        found = s;
                    } else {
                        newList.add(s);
                    }
                }
                if (found == null) {
                    return SegmentDropResult.NOT_FOUND;
                }
                segments = newList;
                // Mirror dropSegmentByUuid: mark dirty so the next checkpoint
                // serialises the reduced segment list. Without this the
                // checkpoint gate would skip and leave a stale entry on disk.
                dirty.set(true);
                long vectorsLost = found.liveCount.get();
                LOGGER.log(Level.SEVERE,
                        "dropSegmentByStorageKey: operator removed segment {0} from"
                                + " store {1} (vectors_lost={2}); total on-disk"
                                + " segments now {3}",
                        new Object[]{storageKey, indexName, vectorsLost, segments.size()});
                // Same deferred-close protocol as dropSegmentByUuid (issue #535):
                // a concurrent compaction cycle may still hold a reference to
                // `found.onDiskGraph`; queue the close so it runs after the
                // cycle releases its compactionLock.
                pendingSegmentCloses.add(found);
                return new SegmentDropResult(true, vectorsLost);
            } finally {
                stateLock.writeLock().unlock();
            }
        } finally {
            drainPendingSegmentClosesOpportunistically();
        }
    }

    /**
     * Reconciles the store's adopted (externally-produced) segment set against
     * the ZK-reported ownership snapshot. Any segment that was adopted (i.e.
     * {@code seg.externalStorageKey != null}) but whose UUID is absent from
     * {@code knownUuids} is dropped via {@link #dropSegmentByUuid}.
     *
     * <p>Called at IS startup, after {@link
     * herddb.indexing.segment.SegmentAssignmentWatcher#watchIndex} completes its
     * initial synchronous scan, to handle the case where the IS was down while
     * the optimizer transitioned and deleted the adopted segment from ZK. Without
     * this reconcile, the orphaned segment would stay in the active search path
     * indefinitely (and fail at search time if the optimizer also deleted its
     * underlying multipart files).
     *
     * <p>Idempotent and safe to call multiple times. This method never adds
     * segments — it only drops ones that are gone from ZK.
     *
     * @param knownUuids the set of segment UUIDs currently visible in the ZK
     *                   registry for this index (from
     *                   {@link herddb.indexing.segment.SegmentAssignmentWatcher#snapshotKnownSegments()})
     */
    @Override
    public void reconcileAdoptedSegments(Set<String> knownUuids) {
        List<String> toDropUuids = new ArrayList<>();
        for (VectorSegment seg : segments) {
            if (seg.externalStorageKey != null
                    && seg.segmentUuid != null
                    && !knownUuids.contains(seg.segmentUuid)) {
                toDropUuids.add(seg.segmentUuid);
            }
        }
        for (String uuid : toDropUuids) {
            LOGGER.log(Level.WARNING,
                    "reconcileAdoptedSegments: dropping orphaned adopted segment {0}"
                            + " from store {1} (not present in ZK)",
                    new Object[]{uuid, indexName});
            dropSegmentByUuid(uuid);
        }
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

    // Package-private (not private) so MemoryPressureTriggerZeroLiveVectorsTest
    // can exercise the issue #563 zero-live-vectors guard as a whitebox test.
    boolean shouldTriggerMemoryPressureCheckpoint() {
        // Issue #563: skip the trigger entirely when there are no in-memory
        // vectors to checkpoint. A memory-pressure checkpoint only moves the
        // in-memory HNSW shards onto disk; it cannot reduce the on-disk segment
        // pkData/BLink footprint that {@link #estimatedMemoryUsageBytes()} (and
        // the global budget) also count. With many on-disk segments that
        // footprint alone can permanently exceed the 70% threshold, so without
        // this guard the compaction loop fires a spurious, no-op Phase A every
        // cycle even though there are 0 live vectors.
        //
        // The count spans live + frozen + deferred shards — the same shard set
        // {@link #getLiveShardMemoryBytes()} measures — so a checkpoint that
        // defers shards (parking un-persisted vectors in deferredShards) is not
        // mistaken for an empty store.
        if (liveFrozenDeferredNodeCount() == 0) {
            return false;
        }
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

    /**
     * Blocks {@link #addVectorInternal} when the on-disk segment count
     * exceeds {@link #compactionBackpressureThreshold} (issue #354).
     *
     * <p>Wakes the compaction thread before parking, then polls every 100 ms
     * until either the segment count falls below the threshold or the store
     * is shut down. The same {@link ThreadPoolExecutor.CallerRunsPolicy}
     * that propagates memory back-pressure to the tailer thread propagates
     * this back-pressure too.
     */
    private void waitForSegmentCountRelief() {
        long startMs = System.currentTimeMillis();
        long maxWaitMs = compactionBackpressureMaxWaitMs;
        // Compute deadline with overflow protection: if startMs + maxWaitMs wraps
        // (possible for very large but non-MAX_VALUE values) treat as no deadline.
        long deadlineMs;
        if (maxWaitMs == Long.MAX_VALUE || maxWaitMs >= Long.MAX_VALUE - startMs) {
            deadlineMs = Long.MAX_VALUE;
        } else {
            deadlineMs = startMs + maxWaitMs;
        }
        segmentCountBackpressureActive = 1;
        segmentCountBackpressureTotal.incrementAndGet();
        int threshold = compactionBackpressureThreshold;
        LOGGER.log(Level.WARNING,
                "vector store {0} segment-count back-pressure: {1} segments exceeds "
                        + "threshold {2}, blocking addVector and waking compaction",
                new Object[]{indexName, segments.size(), threshold});

        // Wake the compaction thread so it drains segments while we wait.
        synchronized (vectorIndexCompactionWakeup) {
            vectorIndexCompactionWakeupPending = true;
            vectorIndexCompactionWakeup.notifyAll();
        }

        boolean timedOut = false;
        synchronized (segmentCountMonitor) {
            // segmentCountGeneration is incremented by notifySegmentCountMonitor each
            // time a compaction cycle completes, giving SpotBugs a traceable
            // producer-consumer relationship (NN_NAKED_NOTIFY avoidance).
            while (running && segments.size() > threshold && segmentCountGeneration >= 0) {
                long remaining = deadlineMs - System.currentTimeMillis();
                if (remaining <= 0) {
                    // Safety net (issue #370): compaction has not been able to relieve
                    // back-pressure within maxWaitMs.  Log SEVERE and break out so the
                    // IS does not hang permanently.  The caller will continue inserting
                    // even though segment count is still above the threshold; operators
                    // must investigate compaction failures reported in preceding WARNINGs.
                    timedOut = true;
                    break;
                }
                try {
                    segmentCountMonitor.wait(Math.min(100, remaining));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        segmentCountBackpressureActive = 0;
        long elapsedMs = System.currentTimeMillis() - startMs;
        segmentCountBackpressureTimeMs.addAndGet(elapsedMs);
        if (timedOut) {
            segmentCountBackpressureTimeouts.incrementAndGet();
            LOGGER.log(Level.SEVERE,
                    "vector store {0}: segment-count back-pressure safety timeout after {1} ms "
                            + "({2} segments still exceeds threshold {3}, consecutive compaction "
                            + "failures={4}). Releasing back-pressure to prevent permanent IS hang. "
                            + "Investigate compaction failures and consider raising "
                            + "vector.index.compaction.backpressure.max.wait.ms or adjusting "
                            + "compaction policy settings.",
                    new Object[]{indexName, elapsedMs, segments.size(), threshold,
                            compactionConsecutiveFailures.get()});
        } else {
            LOGGER.log(Level.INFO,
                    "vector store {0} segment-count back-pressure released after {1} ms "
                            + "({2} segments remaining)",
                    new Object[]{indexName, elapsedMs, segments.size()});
        }
    }

    /**
     * Notifies any threads parked in {@link #waitForSegmentCountRelief}
     * that the segment list has shrunk (called after each successful swap).
     *
     * <p>{@code segmentCountGeneration} is incremented before {@code notifyAll()}
     * to give SpotBugs a visible state change inside the synchronized block
     * (avoids the NN_NAKED_NOTIFY warning).
     */
    private void notifySegmentCountMonitor() {
        synchronized (segmentCountMonitor) {
            segmentCountGeneration++;
            segmentCountMonitor.notifyAll();
        }
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
        // Unblock any addVector callers that are parked on segment-count
        // back-pressure so they see running=false and exit cleanly.
        notifySegmentCountMonitor();

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
        closePendingCheckpointDeletesQuietly();
        this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
        CountDownLatch latch = this.checkpointPhaseComplete;
        if (latch != null) {
            latch.countDown();
            this.checkpointPhaseComplete = null;
        }
        // Issue #535: drain any segments queued for close by a deferred
        // dropSegmentByUuid that landed after the compaction thread exited.
        // After the join above, no new cycle can start, so no contention
        // for compactionLock — but we still take it for invariant clarity.
        //
        // tryLock with a 30s budget guards against the (pathological) case
        // where the compaction thread hung mid-cycle and was not actually
        // released by the earlier vct.join(10000) — without the timeout
        // close() would block here indefinitely. On timeout we log SEVERE
        // and continue; any queued segments will leak their BLink storage
        // entries (acceptable at shutdown — the process is going away).
        boolean drainLockHeld = false;
        try {
            drainLockHeld = compactionLock.tryLock(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (drainLockHeld) {
            try {
                drainPendingSegmentClosesUnderCompactionLock();
            } finally {
                compactionLock.unlock();
            }
        } else {
            LOGGER.log(Level.SEVERE,
                    "vector store {0}: compactionLock not released within 30s"
                            + " during close() — {1} segments queued for deferred"
                            + " close will leak their BLink storage entries",
                    new Object[]{indexName, pendingSegmentCloses.size()});
        }
        // Issue #455: drop every segment's contribution from the on-disk
        // memory counter before closing them, so the counter goes back to 0
        // (verified by PersistentVectorStoreEstimatedMemoryCounterTest).
        for (VectorSegment seg : segments) {
            unregisterSegmentMemoryEstimate(seg);
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

        // Pressure-driven local-compaction wake (companion to issue #354
        // back-pressure). When an external optimizer is enabled the IS-local
        // loop sleeps below the kick threshold; this poke wakes it the
        // INSTANT segment count reaches the threshold instead of waiting for
        // its idle interval to elapse, so we minimise the window where
        // segments could keep accumulating toward the back-pressure ceiling.
        // The {@code >=} matches the {@code <} gate in {@code runCompactionCycle}
        // — at exactly {@code kickThreshold} segments the cycle runs, so the
        // wake must fire at that boundary too. Cheap: a few volatile reads
        // and an int compare on the existing fast path; the synchronized
        // {@code wakeVectorIndexCompaction} is skipped entirely when a wake
        // is already pending (sustained-pressure ingest would otherwise pay
        // a redundant notify on every add).
        int currentSegments = segments.size();
        if (externalCompactionEnabled
                && localCompactionEnabledWithOptimizer
                && !vectorIndexCompactionWakeupPending
                && currentSegments >= kickThresholdCached) {
            wakeVectorIndexCompaction();
        }

        // Segment-count back-pressure (issue #354): block when there are too
        // many on-disk segments to prevent unbounded accumulation.
        if (currentSegments > compactionBackpressureThreshold) {
            waitForSegmentCountRelief();
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
            PagedPkSet pending = pendingCheckpointDeletes;
            if (pending != null) {
                pending.add(pk);
            }
            // Track delete for in-flight compaction: if we are rebuilding
            // segments right now, the merged output must see this delete
            // before it is published.
            PagedPkSet pendingCompact = pendingCompactionDeletes;
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

        List<List<Map.Entry<Bytes, Float>>> partials;

        // The read lock protects three concerns:
        //   1. On-disk segments (Phase 1): the segment snapshot and task execution
        //      must happen atomically with respect to dropSegmentByUuid, which
        //      removes a segment from `this.segments` under the write lock. The
        //      write lock cannot be acquired while we hold the read lock, so a
        //      search that enters here and snapshots `this.segments` is guaranteed
        //      to observe a consistent list — by the time the writeLock is
        //      acquired by a concurrent drop, this search has already completed
        //      and released the read lock.
        //
        //      Post-issue-#535: dropSegmentByUuid NO LONGER closes the segment
        //      synchronously under the writeLock. The close is deferred to
        //      drainPendingSegmentClosesUnderCompactionLock (drained either
        //      opportunistically after the dropSegmentByUuid writeLock release
        //      or in runCompactionCycle's finally). The drain holds
        //      compactionLock but NOT stateLock.writeLock — and crucially, the
        //      drain operates only on segments that are NO LONGER in
        //      this.segments (they were removed BEFORE being queued). So a
        //      search that snapshotted this.segments AFTER the writeLock-protected
        //      removal cannot reference a queued segment; a search that
        //      snapshotted BEFORE the removal already completed (the writeLock
        //      waited for the readLock to release). Either way the drain's
        //      seg.close() is invisible to in-flight searches.
        //
        //      The Stage-3 republish race tracked separately in issue #537 has
        //      been FIXED by issue #538: both atomicSwapCompactionResult Stage 3
        //      and doCheckpointFusedPQThreePhase Phase C Stage 2 now rebuild
        //      `finalNewSegments` from a fresh read of `this.segments` under
        //      the writeLock, so a just-dropped segment is never re-introduced
        //      by either publish path.
        //   2. Live in-memory shards (Phase 2): liveShards is updated under the
        //      write lock during addVectorInternal's shard-rotation path; holding
        //      the read lock keeps the shard list stable across task creation.
        //   3. Frozen + deferred shards (Phase 3): these are dropped by Phase C of
        //      checkpoint under the write lock (issue #129); the read lock prevents
        //      a searcher's GraphSearcher from accessing freed memory.
        stateLock.readLock().lock();
        try {
            // Phase 1: on-disk segments (snapshotted and tasked under read lock so
            // that dropSegmentByUuid's writeLock-protected removal cannot land
            // between the snapshot and task execution — see invariant above).
            final List<VectorSegment> currentSegments = this.segments;
            for (final VectorSegment seg : currentSegments) {
                if (ctx != null) {
                    ctx.recordSegmentQueried();
                }
                tasks.add(wrapInContext(ctx, () -> {
                    List<Map.Entry<Bytes, Float>> local = new ArrayList<>(perSourceK);
                    seg.search(qv, perSourceK, similarityFunction, local);
                    return local;
                }));
            }

            // Phase 2: live in-memory shards (no pending-deletes filtering).
            for (final LiveGraphShard shard : liveShards) {
                if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                    if (ctx != null) {
                        ctx.recordSegmentQueried();
                    }
                    tasks.add(wrapInContext(ctx, () -> searchLiveShard(shard, qv, perSourceK, null)));
                }
            }

            // Phase 3: frozen + deferred shards (the lock was already required for these).
            final PagedPkSet pending = pendingCheckpointDeletes;
            List<LiveGraphShard> frozen = frozenShards;
            if (frozen != null) {
                for (final LiveGraphShard shard : frozen) {
                    if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                        if (ctx != null) {
                            ctx.recordSegmentQueried();
                        }
                        tasks.add(wrapInContext(ctx,
                                () -> searchLiveShard(shard, qv, perSourceK, pending)));
                    }
                }
            }
            List<LiveGraphShard> deferred = deferredShards;
            if (deferred != null) {
                for (final LiveGraphShard shard : deferred) {
                    if (shard.builder != null && !shard.nodeToPk.isEmpty()) {
                        if (ctx != null) {
                            ctx.recordSegmentQueried();
                        }
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
                                                          int perSourceK, PagedPkSet pendingDeletes) {
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
     *   <li>On-disk segments' in-memory footprint (pkData / pkOffsets / pkLengths
     *       arrays only, ≈16 B/vector for BIGINT PKs) — read in O(1) from
     *       {@link #onDiskSegmentsEstimatedMemoryBytes}, an incrementally-
     *       maintained counter populated by
     *       {@link #registerSegmentMemoryEstimate} at every mutation of
     *       {@link #segments} (issue #455).  The BLink pk-to-ordinal tree is
     *       intentionally excluded: its page-cache footprint is bounded by the
     *       {@link MemoryManager}'s index-page replacement policy independently,
     *       and including it caused permanent over-counting that triggered
     *       spurious back-pressure stalls (issue #592).</li>
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
        // On-disk segments: O(1) read of the incremental counter (issue #455).
        total += onDiskSegmentsEstimatedMemoryBytes.get();
        return total;
    }

    /**
     * Returns the estimated heap footprint of the in-memory HNSW shards only —
     * the live, frozen, and deferred {@link LiveGraphShard} graphs — excluding
     * the on-disk segment pkData/pkOffsets/pkLengths contribution that
     * {@link #estimatedMemoryUsageBytes()} additionally counts.
     *
     * <p>Issue #563: this is the figure that genuinely shrinks when a
     * checkpoint runs (the live shard is snapshotted onto disk and reset to
     * empty). The memory-pressure checkpoint trigger and the
     * {@code live_vectors_memory_bytes} diagnostic field must use this value
     * rather than {@link #estimatedMemoryUsageBytes()}: the on-disk segment
     * footprint is managed independently (segment eviction / compaction) and
     * counting it toward the live-vector limit produces spurious, no-op
     * Phase A triggers once enough segments accumulate.
     *
     * @return estimated bytes of heap occupied by the live/frozen/deferred shards
     */
    public long getLiveShardMemoryBytes() {
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

    /**
     * Returns the in-memory footprint of all currently registered on-disk
     * {@link VectorSegment} objects (pkData/pkOffsets/pkLengths arrays only,
     * ≈16 B/vector for BIGINT PKs; BLink is excluded — see issue #592).
     *
     * <p>Exposed for testing so that tests can assert the on-disk segment
     * contribution separately from the live-shard contribution, which is
     * non-zero even for empty shards (the underlying {@code OnHeapGraphIndex}
     * retains a small fixed overhead).
     *
     * <p>This is an O(1) read of the incremental counter
     * {@link #onDiskSegmentsEstimatedMemoryBytes} (issue #455); it does not
     * iterate over the segments list.
     *
     * @return estimated bytes held by all current on-disk segments
     */
    public long getOnDiskSegmentsEstimatedMemoryBytes() {
        return onDiskSegmentsEstimatedMemoryBytes.get();
    }

    // -------------------------------------------------------------------------
    // On-disk segment memory bookkeeping (issue #455)
    //
    // Every site that mutates {@link #segments} must call
    // {@link #registerSegmentMemoryEstimate} for newly-added segments and
    // {@link #unregisterSegmentMemoryEstimate} for segments being removed,
    // so that {@link #onDiskSegmentsEstimatedMemoryBytes} stays consistent
    // with the iterated sum of {@link VectorSegment#estimatedInMemoryBytes()}
    // captured at registration time.  The unit test
    // PersistentVectorStoreEstimatedMemoryCounterTest cross-checks the counter
    // against the iterated sum at every lifecycle event.
    // -------------------------------------------------------------------------

    /**
     * Captures the segment's current memory estimate as a snapshot, stores it
     * on the segment in {@link VectorSegment#cachedEstimatedInMemoryBytes},
     * marks {@link VectorSegment#registeredInMemoryCounter}, and adds the
     * snapshot to {@link #onDiskSegmentsEstimatedMemoryBytes}.  Must be called
     * exactly once per segment, after pkData/pkOffsets/pkLengths have been
     * populated (i.e. after {@code loadFusedPQSegment}), while the caller
     * holds {@code stateLock.writeLock()} (or, for the load path, while no
     * other thread can observe the store yet).  The snapshot covers only
     * pkData/pkOffsets/pkLengths; BLink is excluded (issue #592).
     *
     * <p>If the segment is already registered the call is a no-op so retry
     * paths cannot double-count.  The {@code registeredInMemoryCounter} flag
     * is the bookkeeping sentinel — kept distinct from
     * {@code cachedEstimatedInMemoryBytes} so that a legitimately-zero-byte
     * snapshot (e.g. an empty post-load segment) is not mistaken for "not yet
     * registered".
     */
    private void registerSegmentMemoryEstimate(VectorSegment seg) {
        if (seg.registeredInMemoryCounter) {
            // Already registered (e.g. recovery retry); avoid double-count.
            return;
        }
        long bytes = seg.estimatedInMemoryBytes();
        seg.cachedEstimatedInMemoryBytes = bytes;
        seg.registeredInMemoryCounter = true;
        if (bytes != 0) {
            onDiskSegmentsEstimatedMemoryBytes.addAndGet(bytes);
        }
    }

    /**
     * Subtracts the snapshot captured by
     * {@link #registerSegmentMemoryEstimate} from
     * {@link #onDiskSegmentsEstimatedMemoryBytes} and clears the bookkeeping
     * state on the segment.  Idempotent: a segment that was never registered
     * (or was already unregistered) is a no-op.  Must be called while holding
     * {@code stateLock.writeLock()} on every regular mutation path; the
     * shutdown close paths run after the compaction/checkpoint threads have
     * been joined and back-pressure waiters released, so no concurrent writer
     * can race the unregister loop there.
     */
    private void unregisterSegmentMemoryEstimate(VectorSegment seg) {
        if (!seg.registeredInMemoryCounter) {
            return;
        }
        long bytes = seg.cachedEstimatedInMemoryBytes;
        seg.cachedEstimatedInMemoryBytes = 0L;
        seg.registeredInMemoryCounter = false;
        if (bytes != 0) {
            onDiskSegmentsEstimatedMemoryBytes.addAndGet(-bytes);
        }
    }

    /**
     * Test-only helper: returns the live sum of
     * {@link VectorSegment#estimatedInMemoryBytes()} across every segment
     * currently in {@link #segments}, without touching
     * {@link #onDiskSegmentsEstimatedMemoryBytes}.
     *
     * <p>Tests use this as the independent ground truth against which the
     * counter is compared (issue #455 — a missed register/unregister at any
     * future mutation site would cause the two values to diverge).
     *
     * <p>Since BLink is intentionally excluded from
     * {@link VectorSegment#estimatedInMemoryBytes()} (issue #592), the counter
     * and the iterated sum are always consistent: both count only the stable
     * pkData/pkOffsets/pkLengths arrays, which do not change after registration.
     *
     * @return iterated sum of {@code seg.estimatedInMemoryBytes()} across
     *         the current segment list
     */
    public long sumOnDiskSegmentMemoryBytesByIterationForTesting() {
        long total = 0;
        for (VectorSegment seg : segments) {
            total += seg.estimatedInMemoryBytes();
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

    /**
     * Pre-warms the {@link herddb.remote.SegmentBlockCache} for each loaded
     * segment by performing a BFS traversal of Layer 0 starting from the
     * segment's HNSW entry node. Every
     * {@link ImmutableGraphIndex.View#getNeighborsIterator} call on Layer 0
     * reads through the segment's {@link ReaderSupplier}, which routes through
     * the {@link herddb.remote.SegmentBlockCache} and populates it with the
     * blocks most likely to be accessed by subsequent search queries.
     * Higher layers (L1+) are already in Java heap after
     * {@link OnDiskGraphIndex#load}, so no I/O is needed for them.
     *
     * <p>The BFS visits at most
     * {@code warmupBytesPerSegment / approxBytesPerNode} nodes per segment
     * (where {@code approxBytesPerNode ≈ graphFileSize / idUpperBound}),
     * and always at least the entry node. Starting from the entry node
     * guarantees the most critical block is warm regardless of where the
     * entry-node ordinal falls within the file — a sequential read from
     * offset 0 would miss it for large segments.
     *
     * <p>A value of {@code 0} or negative is a no-op. Individual segment
     * failures are logged at WARNING and skipped; the warmup is best-effort
     * and never aborts the watermark save.
     *
     * <p><b>Idempotency (issue #569).</b> Segments are warmed exactly once,
     * AT CREATION TIME, before they are published into the searchable
     * {@link #segments} list — checkpoint Phase C-prep and compaction both
     * call {@link #warmUpSegment} on their freshly-built segment. This
     * sweep therefore only does real I/O for segments that were never warmed
     * at creation — i.e. segments loaded by {@code loadFromStatus} on
     * restart. For an already-warmed segment {@link #warmUpSegment} returns
     * immediately, so the post-checkpoint warm-all degenerates to a cheap
     * no-op in steady state. This is what breaks the
     * warmup→checkpoint→warmup death spiral: the old behaviour re-warmed
     * <em>all</em> segments after <em>every</em> checkpoint, monopolising
     * memory and I/O and starving compaction.
     *
     * <p>In local-file or in-memory storage modes the reads route through the
     * in-memory buffer; they complete without error but have no block-cache
     * effect.
     *
     * @param warmupBytesPerSegment approximate byte budget per segment; the
     *     BFS stops when the estimated bytes consumed exceed this value
     */
    public void warmUpBlockCache(long warmupBytesPerSegment) {
        if (warmupBytesPerSegment <= 0) {
            return;
        }
        List<VectorSegment> currentSegments = this.segments;
        if (currentSegments == null || currentSegments.isEmpty()) {
            return;
        }
        long startMs = System.currentTimeMillis();
        int warmedSegments = 0;
        long totalNodesVisited = 0;
        for (VectorSegment seg : currentSegments) {
            // Already-warmed segments (the steady-state case once every
            // segment is warmed at creation) return 0 here without any I/O.
            int nodesVisited = warmUpSegment(seg, warmupBytesPerSegment);
            if (nodesVisited > 0) {
                warmedSegments++;
                totalNodesVisited += nodesVisited;
            }
        }
        long elapsedMs = System.currentTimeMillis() - startMs;
        if (warmedSegments > 0) {
            LOGGER.log(Level.INFO,
                    "warmUpBlockCache {0}: warmed {1}/{2} segments (others already warm), "
                            + "{3} nodes total in {4} ms",
                    new Object[]{indexName, warmedSegments, currentSegments.size(),
                            totalNodesVisited, elapsedMs});
        } else {
            LOGGER.log(Level.FINE,
                    "warmUpBlockCache {0}: all {1} segments already warm — no-op",
                    new Object[]{indexName, currentSegments.size()});
        }
    }

    /**
     * Sets the per-segment block-cache warmup byte budget (issue #322 / #569).
     * A value of {@code 0} or negative disables warmup. Called by
     * {@code IndexingServiceEngine} before {@link #start()} so that segments
     * created by subsequent checkpoints and compactions are warmed at
     * creation time. See {@link #warmupBytesPerSegment}.
     */
    public void setWarmupBytesPerSegment(long warmupBytesPerSegment) {
        this.warmupBytesPerSegment = warmupBytesPerSegment;
    }

    /**
     * Per-segment byte budget for the frontier (pinned) region BFS pass
     * (issue #578). When {@code -1} (the default) the budget mirrors
     * {@link #warmupBytesPerSegment}. When {@code 0} or negative (and not
     * {@code -1}) the pin BFS is disabled. Set via
     * {@link #setPinBytesPerSegment(long)}.
     */
    private volatile long pinBytesPerSegment = -1;

    /**
     * Sets the per-segment byte budget for the frontier-warmup BFS pass
     * (issue #578). A value of {@code 0} disables pin-warmup. A value of
     * {@code -1} (the default) makes the pin budget mirror
     * {@link #warmupBytesPerSegment}.
     */
    public void setPinBytesPerSegment(long pinBytesPerSegment) {
        this.pinBytesPerSegment = pinBytesPerSegment;
    }

    /**
     * Total number of segments actually BFS-warmed by this store since start
     * (issue #569). Exposed for tests and Prometheus exporters.
     */
    public long getWarmedSegmentsTotal() {
        return warmedSegmentsTotal.get();
    }

    /**
     * Warms a single segment's block cache (issue #569), idempotently.
     *
     * <p>This is the per-segment building block called both by the
     * post-checkpoint warm-all sweep ({@link #warmUpBlockCache}) and — the
     * point of issue #569 — directly by the checkpoint and compaction code
     * paths on a freshly-built segment BEFORE it is published into the
     * searchable {@link #segments} list. Warming before publish guarantees
     * the first ANN query against a new segment finds hot cache blocks
     * instead of issuing cold streaming reads, without the old behaviour of
     * re-warming every loaded segment after every checkpoint.
     *
     * <p>The method is idempotent: once a segment has been warmed (or found
     * to have nothing to warm) its {@link VectorSegment#warmedUp} flag is set
     * and subsequent calls return {@code 0} immediately. A real I/O error
     * leaves the flag unset so the next sweep retries.
     *
     * @param seg the segment to warm
     * @param bytesPerSegment approximate byte budget for the BFS; {@code <= 0}
     *     disables warmup (no-op, flag left unset so a later enable still works)
     * @return number of graph nodes visited (0 if disabled, already warm,
     *     nothing to warm, or an I/O error occurred)
     */
    int warmUpSegment(VectorSegment seg, long bytesPerSegment) {
        if (bytesPerSegment <= 0 || seg == null) {
            return 0;
        }
        if (seg.warmedUp) {
            return 0;
        }
        OnDiskGraphIndex odg = seg.onDiskGraph;
        if (odg == null || seg.graphFileSize <= 0) {
            // Nothing to warm — mark warmed so the warm-all sweep skips it
            // for good rather than re-checking it on every checkpoint.
            seg.warmedUp = true;
            return 0;
        }
        int idUpperBound = odg.getIdUpperBound();
        if (idUpperBound <= 0) {
            seg.warmedUp = true;
            return 0;
        }
        // Estimate bytes-per-node: Layer-0 data dominates file size.
        long approxBytesPerNode = Math.max(1L, seg.graphFileSize / idUpperBound);
        // Always warm at least the entry node regardless of budget.
        int nodeLimit = (int) Math.min(
                idUpperBound,
                Math.max(1L, bytesPerSegment / approxBytesPerNode));
        int nodesVisited = warmUpSegmentBfs(seg, odg, nodeLimit);
        if (nodesVisited > 0) {
            seg.warmedUp = true;
            warmedSegmentsTotal.incrementAndGet();
            // After the main warm pass, run the pin BFS to promote
            // entry-frontier blocks into the eviction-resistant frontier region.
            long pinBudget = this.pinBytesPerSegment;
            if (pinBudget == -1) {
                pinBudget = bytesPerSegment;
            }
            if (pinBudget > 0) {
                pinSegmentBfs(seg, odg, approxBytesPerNode, pinBudget);
            }
            return nodesVisited;
        }
        // nodesVisited == 0 means warmUpSegmentBfs hit an I/O error (it
        // always visits at least the entry node on success). Leave
        // warmedUp == false so a later sweep retries this segment.
        return 0;
    }

    /**
     * Warms every segment in {@code newSegments} that has not yet been warmed
     * (issue #569), using the configured {@link #warmupBytesPerSegment}
     * budget. Called by the checkpoint and compaction paths on their
     * freshly-built segments BEFORE the segments are published into the
     * searchable {@link #segments} list.
     *
     * <p>No-op when warmup is disabled ({@code warmupBytesPerSegment <= 0}).
     * Best-effort: per-segment I/O errors are swallowed inside
     * {@link #warmUpSegment} / {@link #warmUpSegmentBfs} and never abort the
     * checkpoint or compaction.
     */
    private void warmUpNewSegmentsBeforePublish(List<VectorSegment> newSegments,
                                                String context) {
        long budget = this.warmupBytesPerSegment;
        if (budget <= 0 || newSegments == null || newSegments.isEmpty()) {
            return;
        }
        long startMs = System.currentTimeMillis();
        int warmed = 0;
        long totalNodes = 0;
        for (VectorSegment seg : newSegments) {
            int nodes = warmUpSegment(seg, budget);
            if (nodes > 0) {
                warmed++;
                totalNodes += nodes;
            }
        }
        if (warmed > 0) {
            LOGGER.log(Level.INFO,
                    "warmUpBlockCache {0}: pre-publish warm of {1} new segment(s) ({2}), "
                            + "{3} nodes in {4} ms",
                    new Object[]{indexName, warmed, context, totalNodes,
                            System.currentTimeMillis() - startMs});
        }
    }

    /**
     * BFS over Layer 0 of {@code seg} starting from its HNSW entry node.
     * Every {@link ImmutableGraphIndex.View#getNeighborsIterator} call on
     * Level 0 reads from disk through the
     * {@link herddb.remote.SegmentBlockCache}. The traversal stops once
     * {@code nodeLimit} distinct nodes have been visited.
     *
     * @return number of nodes visited (0 on error)
     */
    private int warmUpSegmentBfs(VectorSegment seg, OnDiskGraphIndex odg, int nodeLimit) {
        try {
            ImmutableGraphIndex.View view = odg.getView();
            try {
                ImmutableGraphIndex.NodeAtLevel entry = view.entryNode();
                java.util.Set<Integer> visited = new java.util.HashSet<>();
                java.util.ArrayDeque<Integer> queue = new java.util.ArrayDeque<>();
                queue.add(entry.node);
                visited.add(entry.node);
                while (!queue.isEmpty() && visited.size() < nodeLimit) {
                    int node = queue.poll();
                    // Level-0 read: routes through RemoteRandomAccessReader → SegmentBlockCache
                    NodesIterator neighbors = view.getNeighborsIterator(0, node);
                    while (neighbors.hasNext() && visited.size() < nodeLimit) {
                        int neighbor = neighbors.nextInt();
                        if (visited.add(neighbor)) {
                            queue.add(neighbor);
                        }
                    }
                }
                return visited.size();
            } finally {
                view.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                    "warmUpBlockCache {0}: I/O error warming segment {1}: {2}",
                    new Object[]{indexName, seg.segmentId, e.getMessage()});
            return 0;
        } catch (java.io.UncheckedIOException e) {
            // getView() and getNeighborsIterator() wrap I/O errors as UncheckedIOException
            LOGGER.log(Level.WARNING,
                    "warmUpBlockCache {0}: I/O error warming segment {1}: {2}",
                    new Object[]{indexName, seg.segmentId,
                            e.getCause() != null ? e.getCause().getMessage() : e.getMessage()});
            return 0;
        } catch (Exception e) {
            // Broad catch: warming is best-effort; any unexpected failure from an
            // unknown ReaderSupplier implementation must not abort the watermark save.
            LOGGER.log(Level.WARNING,
                    "warmUpBlockCache " + indexName + ": unexpected error warming segment "
                            + seg.segmentId, e);
            return 0;
        }
    }

    /**
     * Performs a secondary BFS over Layer 0 of {@code seg} using a
     * <em>pin-mode</em> reader so that every accessed block is inserted into
     * the frontier (pinned) region of the {@link herddb.remote.SegmentBlockCache}
     * instead of the ordinary evictable region (issue #578).
     *
     * <p>The pin BFS is only possible when:
     * <ol>
     *   <li>The segment's {@code onDiskReaderSupplier} is a
     *       {@link RemoteRandomAccessReader.Supplier} (not a local-disk supplier).</li>
     *   <li>The supplier's block cache has a frontier region configured
     *       ({@link RemoteRandomAccessReader.Supplier#hasFrontierCacheActive()}).</li>
     * </ol>
     * When either condition is not met, this method returns silently — the ordinary
     * warm pass (which already loaded the blocks into the main cache) is
     * sufficient.
     *
     * <p>Upper-layer HNSW data is heap-resident after
     * {@code OnDiskGraphIndex.loadInMemoryLayers} so only Level-0 reads actually
     * go through the remote reader — meaning only Layer-0 blocks end up pinned,
     * which is exactly the right set (entry-frontier blocks).
     *
     * @param seg              segment whose frontier region is being warmed
     * @param odg              the already-open {@link OnDiskGraphIndex} for that segment
     * @param approxBytesPerNode estimated bytes per graph node, used to cap the BFS
     * @param pinBytesPerSegment frontier budget; nodes visited = min(size, budget / bpn)
     */
    void pinSegmentBfs(VectorSegment seg, OnDiskGraphIndex odg,
                               long approxBytesPerNode, long pinBytesPerSegment) {
        io.github.jbellis.jvector.disk.ReaderSupplier rs = seg.onDiskReaderSupplier;
        if (!(rs instanceof PinModeReaderSupplier)) {
            // Not a pin-capable reader supplier (e.g. local disk in unit tests): skip.
            return;
        }
        PinModeReaderSupplier pinCapable = (PinModeReaderSupplier) rs;
        if (!pinCapable.hasFrontierCacheActive()) {
            // Frontier region not configured: skip (no benefit, avoid wasted reads).
            return;
        }
        ReaderSupplier pinSupplier = pinCapable.withPinMode();
        int idUpperBound = odg.getIdUpperBound();
        int nodeLimit = (int) Math.min(
                idUpperBound,
                Math.max(1L, pinBytesPerSegment / approxBytesPerNode));
        try {
            RandomAccessReader pinReader = pinSupplier.get();
            OnDiskGraphIndex.View pinView = odg.new View(pinReader);
            try {
                ImmutableGraphIndex.NodeAtLevel entry = pinView.entryNode();
                Set<Integer> visited = new HashSet<>();
                ArrayDeque<Integer> queue = new ArrayDeque<>();
                queue.add(entry.node);
                visited.add(entry.node);
                while (!queue.isEmpty() && visited.size() < nodeLimit) {
                    int node = queue.poll();
                    // Level-0 read: routes through pin-mode RemoteRandomAccessReader
                    // → SegmentBlockCache.pinBlock → frontier cache
                    NodesIterator neighbors = pinView.getNeighborsIterator(0, node);
                    while (neighbors.hasNext() && visited.size() < nodeLimit) {
                        int neighbor = neighbors.nextInt();
                        if (visited.add(neighbor)) {
                            queue.add(neighbor);
                        }
                    }
                }
                LOGGER.log(Level.FINE,
                        "pinSegmentBfs {0}: pinned {1} frontier nodes for segment {2}",
                        new Object[]{indexName, visited.size(), seg.segmentId});
            } finally {
                pinView.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                    "pinSegmentBfs {0}: I/O error pinning segment {1}: {2}",
                    new Object[]{indexName, seg.segmentId, e.getMessage()});
        } catch (UncheckedIOException e) {
            LOGGER.log(Level.WARNING,
                    "pinSegmentBfs {0}: I/O error pinning segment {1}: {2}",
                    new Object[]{indexName, seg.segmentId,
                            e.getCause() != null ? e.getCause().getMessage() : e.getMessage()});
        } catch (Exception e) {
            // Broad catch: pin BFS is best-effort; any unexpected failure from
            // an unknown ReaderSupplier implementation must not abort warmup.
            LOGGER.log(Level.WARNING,
                    "pinSegmentBfs " + indexName + ": unexpected error pinning segment "
                            + seg.segmentId, e);
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
                // Issue #455: drop every segment's contribution from the
                // on-disk memory counter before closing them.
                for (VectorSegment seg : segments) {
                    unregisterSegmentMemoryEstimate(seg);
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

            // Segment-count back-pressure deferral gate (issue #562).
            //
            // When the segment count already exceeds the back-pressure
            // threshold, checkpointing a tiny live shard would seal those few
            // vectors into a new on-disk micro-segment (e.g. 3 nodes),
            // incrementing the segment count and making back-pressure WORSE.
            // Such micro-segments are far below vectorIndexCompactionMinBytes
            // so tiered compaction never selects them and they accumulate
            // unboundedly throughout a long compaction cycle.
            //
            // While above the threshold we therefore defer UNCONDITIONALLY
            // (ignoring the time-bounded deferralBoundMs): the trickle of
            // vectors released by the back-pressure safety timeout can safely
            // wait in the live shard until compaction drains the segment count
            // back below the threshold, at which point the time-bounded gate
            // below resumes governing the flush. The live shard stays bounded
            // by maxLiveGraphSize rotation, and the memory-pressure escape
            // hatch still forces a checkpoint if heap is genuinely tight.
            if (minLiveGate > 0
                    && totalLiveVectors < minLiveGate
                    && !anySegmentDirty
                    && !segments.isEmpty()
                    && !shouldTriggerMemoryPressureCheckpoint()
                    && segments.size() > compactionBackpressureThreshold) {
                LOGGER.log(Level.INFO,
                        "checkpoint {0}: deferred during segment-count back-pressure "
                                + "({1} live vectors < {2} threshold, {3} segments > "
                                + "back-pressure threshold {4}) — avoiding micro-segment creation",
                        new Object[]{indexName, totalLiveVectors, minLiveGate,
                                segments.size(), compactionBackpressureThreshold});
                totalCheckpointsDeferred.incrementAndGet();
                return false;
            }

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

            // Warn when the total on-disk segment count is unusually high.
            // This makes runaway accumulation (issue #285) visible in the logs
            // without needing to inspect checkpoint detail lines.
            int totalOnDiskSegments = sealedSegments.size() + mergeableSegments.size();
            if (totalOnDiskSegments > COMPACTION_SEGMENT_COUNT_WARN_THRESHOLD) {
                LOGGER.log(Level.WARNING,
                        "checkpoint {0} Phase A: high on-disk segment count: {1} segments "
                                + "({2} sealed + {3} mergeable); graph-merge compaction "
                                + "may not be keeping up",
                        new Object[]{indexName, totalOnDiskSegments,
                                sealedSegments.size(), mergeableSegments.size()});
            }

            this.frozenShards = snapshotShards;
            // Use a BLink-paged PK set so memory stays bounded even when
            // sustained delete traffic arrives during a multi-minute Phase B
            // (issue #290). The total-checkpoint counter is monotonic across
            // the store's lifetime, so the temp store name never collides
            // with a leftover from a crashed cycle.
            this.pendingCheckpointDeletes = createTempPagedPkSet(
                    "ckpdel_" + totalCheckpointCount.get());
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
        } catch (IOException | RuntimeException | Error e) {
            // Issue #616: include Error in the catch list so an Error thrown
            // by writeShardAsFusedPQSegment (e.g. Netty's OutOfDirectMemoryError,
            // which extends OutOfMemoryError extends Error — the failure mode
            // cited in the issue's reproduction) still drives the rollback
            // pipeline. In the parallel Phase B path,
            // awaitAllOrFirstFailure already wraps a non-IOException /
            // non-RuntimeException Throwable into an IOException; but in the
            // serial path (shardTasks.size() == 1, which forces parallelism=1
            // at doCheckpointFusedPQPhaseB) the Error propagates directly, so
            // without this broadening the rollback + recovery would be
            // skipped and the partial multipart files queued by the issue
            // #616 pre-track would not be deleted.
            //
            // The broad Error catch is bounded: we run a small, fixed set of
            // safe operations (log line + atomic counter bumps + remote-file
            // deletes + writeLock'd state restore), then rethrow the original
            // Throwable unchanged so the JVM's normal Error-handling semantics
            // (e.g. thread termination for VirtualMachineError) are preserved
            // at the call site.
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
                    seg.segmentUuid = swr.segmentUuid;
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
            // Issue #551: Phase B has ALREADY persisted an IndexStatus referencing
            // these new segments (persistIndexStatusMultiSegment ran near the end of
            // doCheckpointFusedPQPhaseB and returned successfully — otherwise we
            // would be in the Phase-B catch block above, not here). The PROVISIONAL
            // znodes were also best-effort committed to ACTIVE.
            //
            // Calling rollbackProvisionalArtefacts() at this point would physically
            // delete MinIO files that are now durably referenced by the persisted
            // IndexStatus, creating the zombie-segment failure mode observed on
            // bigann-100M: ZK shows ACTIVE, files are gone, every subsequent
            // optimizer task targeting that segment fails (4,222+ failed merges in
            // the production incident that motivated this issue). On IS restart,
            // reconcileWithIndexStatus promotes any PROVISIONAL znode whose UUID is
            // in IndexStatus to ACTIVE without checking file existence; the IS then
            // crash-loops on the first load attempt.
            //
            // Clear the trackers WITHOUT deleting. The new segments belong to the
            // durable state; the next successful indexCheckpoint cycle either
            // re-uses them (preload retry) or rewrites them. On restart, the
            // segments load cleanly because the files survive on disk.
            //
            // Defence in depth (pr-reviewer pass on #552): the
            // `provisionalPageIds` tracker is currently DEAD CODE — no caller
            // populates it; only the multipart tracker is fed by
            // {@link #trackProvisionalMultipartFile}. A future contributor
            // who reintroduces page-level tracking must NOT use this code
            // path to delete pages after Phase B has persisted IndexStatus
            // (same zombie-segment failure mode as multipart). The assert
            // makes that constraint load-bearing: if anyone populates the
            // page tracker in Phase B, they MUST either drain it before
            // Phase C-prep can fail, or re-design this handler.
            List<Long> stalePages = this.provisionalPageIds;
            assert stalePages == null || stalePages.isEmpty()
                    : "provisionalPageIds populated but not handled in Phase C-prep "
                            + "rollback (would leak pages OR re-create zombie-segment "
                            + "hazard if reintroduced naively); see issue #551 review.";
            this.provisionalPageIds = null;
            this.provisionalMultipartFiles = null;
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

        // Issue #569: warm the block cache of the freshly-built segments NOW —
        // while they are loaded but still NOT searchable (they join the
        // visible `this.segments` list only at Phase C Stage 2 below). Warming
        // here, on the checkpoint thread, means the first ANN query against a
        // new segment finds hot cache blocks; it also makes the post-checkpoint
        // warm-all sweep idempotent (each segment is warmed exactly once),
        // which is what breaks the warmup→checkpoint→warmup death spiral.
        warmUpNewSegmentsBeforePublish(preloadedSegments, "checkpoint Phase C-prep");

        // ---------- Phase C — two-stage protocol (issue #462). ----------
        //
        // Stage 1 (no writeLock; under checkpointLock):
        //   - Build newSegments (sealed + mergeable + preloaded), reset their
        //     dirty flags.
        //   - Apply pendingCheckpointDeletes — O(P × N) at scale, the slow part.
        //
        // Stage 2 (writeLock for ~µs):
        //   - Close snapshotShards' builders (no concurrent search references
        //     them: the writeLock excludes readers; we also clear
        //     frozenShards/deferredShards before releasing).
        //   - Register memory for preloadedSegments.
        //   - Publish this.segments = newSegments.
        //   - Update nextNodeId, log summary, restore deferred → liveShards.
        //   - Clear frozenShards / deferredShards / pendingCheckpointDeletes.
        //
        // Concurrency invariants the Stage-1 lock-free deletes-apply relies on:
        //   - checkpointLock is held → no concurrent checkpoint/compaction
        //     mutates `segments`.
        //   - seg.deletePk is concurrent-safe with concurrent search readers
        //     (BLink.delete is internally synchronized; offsets[ord]=-1 is
        //     benign for readers, who skip ord<0 in acceptBits and
        //     getPkForOrdinal).
        //   - seg.deletePk is concurrent-safe with concurrent removeVector
        //     callers (which only take readLock): BLink.delete returns the
        //     ordinal to exactly one caller; the racing caller observes null
        //     and skips the offsets/liveCount mutations, so liveCount is
        //     decremented exactly once.

        // Stage 1: build + apply pending deletes.
        List<VectorSegment> newSegments = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (VectorSegment sealed : sealedSegments) {
            sealed.dirty = false;
            newSegments.add(sealed);
        }
        for (VectorSegment mergeable : mergeableSegments) {
            mergeable.dirty = false;
            newSegments.add(mergeable);
        }
        newSegments.addAll(preloadedSegments);

        PagedPkSet pending = this.pendingCheckpointDeletes;
        if (pending != null) {
            pending.forEach(pk -> {
                for (VectorSegment seg : newSegments) {
                    if (seg.deletePk(pk)) {
                        return;
                    }
                }
            });
        }

        Runnable postDeletesApplyHook = phaseCPostDeletesApplyHook;
        if (postDeletesApplyHook != null) {
            postDeletesApplyHook.run();
        }

        // Stage 2: writeLock for the atomic state transition.
        long stage2Start = System.nanoTime();
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

            // Issue #538: REBUILD `finalNewSegments` from the CURRENT
            // `this.segments` rather than publishing the stale Phase-A
            // snapshot. Between Phase A's writeLock release and this
            // Stage-2 writeLock acquisition (which spans the slow Phase B
            // I/O and Phase C-prep loads), a concurrent
            // {@link #dropSegmentByUuid} (fired by the optimizer-watcher's
            // {@code onSegmentReleased}) may have removed segments from
            // {@code this.segments} AND opportunistically closed them via
            // the deferred-close drain (PR #536) — because the checkpoint
            // holds only {@code checkpointLock}, NOT {@code compactionLock},
            // so the drain's {@code compactionLock.tryLock} from
            // {@code drainPendingSegmentClosesOpportunistically} succeeds.
            // Publishing the Phase-A snapshot (which still contains those
            // segments) would silently RE-INSERT closed segments into
            // {@code this.segments}, leaving orphans with
            // {@code onDiskGraph == null}. This is the structural twin of
            // the compaction-side bug fixed in
            // {@link #atomicSwapCompactionResult} Stage 3; both surfaces
            // produced the production failure mode reported in issues
            // #535 / #537 / #538.
            //
            // Concurrent {@link #adoptExternalSegment} is also handled
            // correctly: an adoption landed in the Phase A → Stage 2
            // window is in {@code currentAtStage2} but was NOT in the
            // Phase-A snapshot. Iterating {@code currentAtStage2}
            // directly (rather than the snapshot) preserves the adoption.
            // {@code preloadedSegments} are the NEW segments produced by
            // this checkpoint's Phase B from live shards; they are
            // guaranteed to not be in {@code currentAtStage2} (Phase B's
            // segment IDs are freshly allocated), so concatenating is
            // collision-free.
            //
            // ArrayList + final wrap minimises the writeLock window vs.
            // {@code CopyOnWriteArrayList.add}-in-loop (each {@code add}
            // would re-copy the entire backing array, O(N²)). The
            // CopyOnWriteArrayList constructor still allocates one backing
            // array of size N, so the saving is O(N²) → O(N).
            List<VectorSegment> currentAtStage2 = this.segments;
            ArrayList<VectorSegment> finalNewSegments = new ArrayList<>(
                    currentAtStage2.size() + preloadedSegments.size());
            finalNewSegments.addAll(currentAtStage2);
            finalNewSegments.addAll(preloadedSegments);

            // Issue #538 (pr-reviewer pass-3 follow-up): reapply
            // late-arriving deletes in `pendingCheckpointDeletes` to ALL
            // segments being published, not just `preloadedSegments`.
            //
            // Stage 1's lock-free apply at line ~4998 iterates the Phase-A
            // snapshot of `this.segments` plus `preloadedSegments`. With
            // the round-3 fix, `finalNewSegments` may ALSO contain segments
            // that landed via `adoptExternalSegment` during the Phase A →
            // Stage 2 window. Those adopted segments did not see the
            // Stage-1 apply, and `removeVector`'s direct iteration over
            // `this.segments` only sees segments present at the moment of
            // the removeVector call — if the removeVector arrived BEFORE
            // the adoption, the adopted segment did not receive the delete.
            //
            // Re-applying to the full {@code finalNewSegments} here ensures
            // any late delete is honoured by the adopted segment too.
            // `seg.deletePk` is idempotent: for pks already removed in
            // Stage 1 it returns false (BLink miss) and the loop continues
            // to the next segment.
            //
            // ORDERING (pr-reviewer pass-4 follow-up): the reapply runs
            // BEFORE {@code registerSegmentMemoryEstimate} so that if
            // {@code seg.deletePk} throws (e.g. an unchecked
            // {@code UncheckedIOException} bubbling out of a BLink page
            // unload), unwinding leaves the counter UNCHANGED and
            // {@code this.segments} still pointing at the OLD list — no
            // half-updated state. Matches the {@link
            // #atomicSwapCompactionResult} Stage-3 ordering of "reapply →
            // register → publish".
            //
            // Cost: O(|pending| × |finalNewSegments|). The constant
            // factor is small because each `deletePk` is a single BLink
            // search (~1 μs); typical |pending| at this point is bounded by
            // the application's remove-rate × Phase B duration (seconds),
            // typically a few hundred entries.
            PagedPkSet pendingForReapply = this.pendingCheckpointDeletes;
            if (pendingForReapply != null && !finalNewSegments.isEmpty()) {
                pendingForReapply.forEach(pk -> {
                    for (VectorSegment seg : finalNewSegments) {
                        if (seg.deletePk(pk)) {
                            return;
                        }
                    }
                });
            }

            // Issue #455: sealedSegments + mergeableSegments together cover
            // every segment that was previously in this.segments (their union
            // was built by the Phase A partition loop), so they remain
            // registered.  Only the freshly-built preloadedSegments need to
            // join the on-disk memory counter here.
            //
            // Capture snapshots BEFORE publishing newSegments: if any
            // estimatedInMemoryBytes() call were ever to throw (e.g. an
            // unforeseen runtime failure), unwinding before the publish
            // leaves both this.segments and the counter consistent —
            // no half-updated-counter window.
            //
            // Issue #538 pr-reviewer pass-4: ordering is now `reapply →
            // register → publish`, so a throw in the reapply above also
            // leaves the counter untouched. Same invariant as the
            // {@link #atomicSwapCompactionResult} Stage-3 ordering.
            for (VectorSegment seg : preloadedSegments) {
                registerSegmentMemoryEstimate(seg);
            }

            this.segments = new java.util.concurrent.CopyOnWriteArrayList<>(finalNewSegments);

            int maxOrd = -1;
            for (VectorSegment seg : finalNewSegments) {
                if (seg.maxOrdinal > maxOrd) {
                    maxOrd = seg.maxOrdinal;
                }
            }
            this.nextNodeId.set(Math.max((long) maxOrd + 1L, nextNodeId.get()));

            int totalNodes = (int) onDiskNodeToPkSize() + totalLiveSize();
            LOGGER.log(Level.INFO,
                    "checkpoint {0} Phase C: {1} nodes across {2} segments (FusedPQ), "
                            + "{3} new live inserts during checkpoint",
                    new Object[]{indexName, totalNodes, finalNewSegments.size(), totalLiveSize()});

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
            closePendingCheckpointDeletesQuietly();
            this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
            // Issue #538 (pr-reviewer pass-3 follow-up): OR-pattern so a
            // concurrent {@link #dropSegmentByUuid} or
            // {@link #adoptExternalSegment} that landed in the Phase A →
            // Stage 2 window and set {@code dirty=true} is preserved. With
            // the OLD overwrite ({@code dirty.set(totalLiveSize() > 0)})
            // the dirty signal was clobbered on a quiet index — the
            // IndexStatus-vs-segments drift this fix introduces would
            // then persist indefinitely because the next periodic
            // checkpoint would see {@code dirty == false} and skip.
            // Matches the OR pattern in {@link #atomicSwapCompactionResult}
            // Stage 3 (used to signal a similar drift case).
            dirty.set(dirty.get() || totalLiveSize() > 0);

            recordSegmentSizeDistribution();
        } finally {
            CountDownLatch latch = this.checkpointPhaseComplete;
            this.checkpointPhaseComplete = null;
            stateLock.writeLock().unlock();
            lastPhaseCWriteLockNanos.set(System.nanoTime() - stage2Start);
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

        // Heap snapshot at Phase B entry. Phase B duration is provably
        // O(N_live_shards) in this implementation (each shard is written from
        // its already-built per-shard OnHeapGraphIndex with no global-graph
        // search — see writeShardAsFusedPQSegment), so an unusually slow
        // Phase B is almost always GC-pressure driven, not algorithmic
        // (issue #614). Logging the heap utilisation at start and end makes
        // such events unambiguous in the logs without needing a heap dump.
        Runtime rt = Runtime.getRuntime();
        long heapUsedAtStart = rt.totalMemory() - rt.freeMemory();
        long heapMaxAtStart = rt.maxMemory();
        long heapPctAtStart = heapMaxAtStart > 0
                ? (heapUsedAtStart * 100L / heapMaxAtStart) : -1L;
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: starting ({1} shards, heap used={2} MB / max={3} MB = {4}%)",
                new Object[]{indexName, snapshotShards.size(),
                        heapUsedAtStart / (1024L * 1024L),
                        heapMaxAtStart / (1024L * 1024L),
                        heapPctAtStart});

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
        long heapUsedAtEnd = rt.totalMemory() - rt.freeMemory();
        long heapMaxAtEnd = rt.maxMemory();
        long heapPctAtEnd = heapMaxAtEnd > 0
                ? (heapUsedAtEnd * 100L / heapMaxAtEnd) : -1L;
        LOGGER.log(Level.INFO,
                "checkpoint {0} Phase B: completed in {1} ms ({2} shard segments, {3} total vectors, {4} bytes;"
                        + " heap used={5} MB / max={6} MB = {7}%, delta {8}%)",
                new Object[]{indexName, phaseBElapsedMs, newSegmentResults.size(),
                        totalShardVectors, bytesWritten,
                        heapUsedAtEnd / (1024L * 1024L),
                        heapMaxAtEnd / (1024L * 1024L),
                        heapPctAtEnd,
                        heapPctAtEnd - heapPctAtStart});

        // Order the results by segmentId so that the persisted IndexStatus and
        // the in-memory segment list are both deterministic.
        newSegmentResults.sort(java.util.Comparator.comparingInt(r -> r.segmentId));

        // === Two-phase publish (review item A1+A3) ===
        //
        // 1. Stamp UUIDs (review item A2) before any persistence so the UUID is
        //    durable in IndexStatus even if we crash mid-way.
        // 2. STAGE: register PROVISIONAL znodes BEFORE IndexStatus persist.
        // 3. Persist IndexStatus.
        // 4. COMMIT: flip PROVISIONAL → ACTIVE.
        //
        // If we crash:
        //   - between (1) and (2): no ZK record, no IndexStatus update — clean.
        //   - between (2) and (3): PROVISIONAL znodes exist but their UUIDs are
        //     not in IndexStatus. On next start, reconcile drops them.
        //   - between (3) and (4): PROVISIONAL znodes exist AND their UUIDs are
        //     in IndexStatus. On next start, reconcile promotes them to ACTIVE.
        //
        // Stage and commit failures are best-effort — the local IndexStatus has
        // (or will have) the durable record; reconciliation on next start heals
        // any gaps.
        SegmentPublisher publisherSnapshot = this.segmentPublisher;
        if (publisherSnapshot != null) {
            for (SegmentWriteResult swr : newSegmentResults) {
                if (swr.segmentUuid == null) {
                    swr.stampSegmentUuid(java.util.UUID.randomUUID().toString());
                }
            }
        }

        List<NewSegmentInfo> publishInfo = (publisherSnapshot != null)
                ? buildPublishInfo(newSegmentResults, sequenceNumber, currentIndexStatusGeneration.get() + 1)
                : null;

        if (publisherSnapshot != null) {
            stageNewSegmentsBestEffort(publisherSnapshot, publishInfo);
        }

        // Test hook for review-item R3 (second pr-reviewer pass): invoked AFTER stage
        // and BEFORE commit. Production code never sets this field; tests use it via
        // reflection to exercise the publisher-snapshot torn-read protection by
        // swapping the segmentPublisher field at this exact moment and asserting
        // the snapshot (publisherSnapshot, captured above) is what actually receives
        // the commit.
        Runnable midCheckpointHook = betweenStageAndCommitHookForTests;
        if (midCheckpointHook != null) {
            midCheckpointHook.run();
        }

        persistIndexStatusMultiSegment(sealedSegments, mergeableSegments, newSegmentResults, sequenceNumber);

        if (publisherSnapshot != null) {
            commitStagedSegmentsBestEffort(publisherSnapshot, publishInfo);
        }

        return newSegmentResults;
    }

    /**
     * Builds the {@link NewSegmentInfo} list for a checkpoint cycle from its
     * {@link SegmentWriteResult}s. Used both by {@link #stageNewSegmentsBestEffort}
     * (before IndexStatus persist) and {@link #commitStagedSegmentsBestEffort}
     * (after) so the same identity flows through both phases.
     *
     * <p>All {@link SegmentWriteResult}s passed here must already have their
     * {@code segmentUuid} stamped by the caller (the Phase B publish block).
     * Any entry with a null UUID is skipped with a warning — this should not
     * happen in normal operation.
     */
    private List<NewSegmentInfo> buildPublishInfo(
            List<SegmentWriteResult> newSegmentResults, LogSequenceNumber sequenceNumber,
            long generation) {
        List<NewSegmentInfo> info = new ArrayList<>(newSegmentResults.size());
        for (SegmentWriteResult swr : newSegmentResults) {
            if (swr.segmentUuid == null) {
                // Defensive: should not occur because the Phase B loop stamps
                // every UUID before calling this method.
                LOGGER.log(Level.WARNING,
                        "skipping publish of segment {0} (segmentId={1}) — UUID was not stamped",
                        new Object[]{indexName, swr.segmentId});
                continue;
            }
            info.add(new NewSegmentInfo(
                    swr.segmentId, swr.segmentUuid,
                    swr.graphFilePath, swr.graphFileSize,
                    swr.mapFilePath, swr.mapFileSize,
                    swr.estimatedSizeBytes, swr.vectorCount, generation,
                    sequenceNumber,
                    swr.jvectorFeatureIds));
        }
        return info;
    }

    /**
     * Best-effort {@link SegmentPublisher#stageNewSegments} call: registers
     * PROVISIONAL znodes BEFORE IndexStatus is durable. Failures are logged but
     * don't fail the checkpoint — the next reconciliation pass will heal any
     * gaps. Broad catch is intentional: the publisher is a plugin boundary.
     */
    private void stageNewSegmentsBestEffort(SegmentPublisher publisher, List<NewSegmentInfo> info) {
        if (info == null || info.isEmpty()) {
            return;
        }
        try {
            publisher.stageNewSegments(info);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "segment publisher.stage failed for index {0} ({1} segments); "
                            + "checkpoint will continue, reconcile-on-restart will heal",
                    new Object[]{indexName, info.size()});
            LOGGER.log(Level.WARNING, "stage failure detail", e);
        }
    }

    /**
     * Best-effort {@link SegmentPublisher#commitStagedSegments} call: flips
     * PROVISIONAL znodes to ACTIVE after IndexStatus is durable. Failures are
     * logged; the next reconciliation pass will promote any orphan PROVISIONAL
     * whose UUID is now in IndexStatus.
     */
    private void commitStagedSegmentsBestEffort(SegmentPublisher publisher, List<NewSegmentInfo> info) {
        if (info == null || info.isEmpty()) {
            return;
        }
        try {
            publisher.commitStagedSegments(info);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING,
                    "segment publisher.commit failed for index {0} ({1} segments); "
                            + "checkpoint succeeded locally, reconcile-on-restart will heal",
                    new Object[]{indexName, info.size()});
            LOGGER.log(Level.WARNING, "commit failure detail", e);
        }
    }

    /**
     * Walks the in-memory segments (loaded from IndexStatus) and produces a
     * {@link NewSegmentInfo} list suitable for {@link SegmentPublisher#reconcileWithIndexStatus}.
     * Called once at {@link #start()} when a publisher is attached.
     *
     * <p>All segments in a v4 IndexStatus should carry a UUID. A null UUID can only
     * appear for a segment checkpointed before the publisher was first attached (the
     * first checkpoint after the optimizer feature was enabled writes UUIDs only for
     * newly-emitted segments, not for pre-existing ones loaded from a v4 status). Such
     * segments are skipped here; they will be picked up by the next checkpoint that
     * re-seals them with a freshly assigned UUID.
     */
    private List<NewSegmentInfo> buildExistingSegmentsInfoForReconcile() {
        List<NewSegmentInfo> info = new ArrayList<>(segments.size());
        long generation = currentIndexStatusGeneration.get();
        for (VectorSegment seg : segments) {
            if (seg.segmentUuid == null) {
                // No UUID: this segment was checkpointed before the publisher was
                // attached. Skip it — the next checkpoint will assign a UUID.
                LOGGER.log(Level.FINE,
                        "segment {0} (index {1}) has no UUID; skipping reconcile entry",
                        new Object[]{seg.segmentId, indexName});
                continue;
            }
            // baseLsn for already-loaded segments is unknown post-restart (the
            // sequence-number was an attribute of the IndexStatus snapshot, not of
            // the segment); use START_OF_TIME as a sentinel — the registry never
            // overwrites baseLsn during reconcile so an existing znode keeps its
            // original value.
            //
            // Derive feature IDs from the loaded on-disk graph when available.
            // onDiskGraph may be null for segments that have not yet been opened
            // (e.g. lazy-loaded segments pre-start). In that case pass null:
            //   * If the znode already exists, reconcileWithIndexStatus does NOT
            //     call buildMetadata on it (it only re-registers MISSING znodes),
            //     so the existing znode's jvectorFeatureIds value is preserved.
            //   * If the znode is genuinely missing and the graph has not been
            //     opened, the re-registered znode will carry null featureIds.
            //     AggressivePolicy places that segment in the "null" group; it will
            //     still be merged (via the legacy rebuild fallback), and the next
            //     checkpoint that opens the graph will stamp a real value.
            //     Pre-opening every lazy segment here would require a remote download
            //     just to read a FeatureId set, which is too expensive for reconcile.
            List<String> featureIds = (seg.onDiskGraph != null)
                    ? RemoteSegmentGraphMerger.featureSetToStringList(seg.onDiskGraph.getFeatureSet())
                    : null;
            info.add(new NewSegmentInfo(
                    seg.segmentId, seg.segmentUuid,
                    seg.graphFilePath, seg.graphFileSize,
                    seg.mapFilePath, seg.mapFileSize,
                    seg.estimatedSizeBytes, /* vectorCount unknown post-restart */ 0L,
                    Math.max(seg.generation, generation),
                    LogSequenceNumber.START_OF_TIME,
                    featureIds));
        }
        return info;
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

        // Per-step timing for the four sub-phases of a shard write
        // (issue #614 observability). Initialised to start so a thrown
        // exception leaves them as a deterministic "0 ms for everything
        // after the throw" view in any future telemetry that consumes them.
        long shardStartNanos = System.nanoTime();
        long pqDoneNanos = shardStartNanos;
        long graphWriteDoneNanos = shardStartNanos;
        long graphUploadDoneNanos = shardStartNanos;

        writingGraphActive.incrementAndGet();
        try {
            // Get the shard's already-built OnHeapGraphIndex (no rebuild!)
            OnHeapGraphIndex shardGraph = (OnHeapGraphIndex) shard.builder.getGraph();

            // Issue #605: use idUpperBound (not shardSize / nodeToPk.size()) as the
            // VectorStorageShardView size so that pqv.count covers the full original-ordinal
            // range [0, idUpperBound). When vectors are deleted from a live shard,
            // markNodeDeleted() is called but vectorStorage.remove() is NOT (that only
            // happens when builder == null). maxNodeId is monotonically increasing, so
            // idUpperBound stays at "total ever added" even after deletions.  The jvector
            // writer calls pqv.get(originalOrdinal) using original graph ordinals, which
            // can reach idUpperBound-1 even though nodeToPk.size() == idUpperBound-1
            // (one deletion means live count == total-1 but highest ordinal is still total-1).
            // Using shardSize here would make pqv.count == shardSize, and
            // pqv.get(shardSize) would throw IndexOutOfBoundsException.
            int graphIdUpperBound = shardGraph.getIdUpperBound();

            // Create a zero-copy view over this shard's per-shard VectorStorage.
            // Size = graphIdUpperBound (not shardSize) — see issue #605 comment above.
            VectorStorageShardView shardView = new VectorStorageShardView(
                    shard.vectorStorage, graphIdUpperBound, snapshotDimension);

            // Determine whether FusedPQ is used for this shard.
            // Small shards (< MIN_VECTORS_FOR_FUSED_PQ) do not write the FusedPQ
            // feature, so PQ training is skipped entirely for them (it was
            // computed but unused in the original code — issue #281).
            // Use shardSize (live count) for this check, not graphIdUpperBound.
            int pqSubspaces = Math.max(1, snapshotDimension / 4);
            boolean useFusedPQForShard = shardSize >= MIN_VECTORS_FOR_FUSED_PQ;
            // For FusedPQ-eligible shards, reuse the cached codebook when possible
            // instead of re-running K-Means from scratch (issue #281).
            ProductQuantization pq = useFusedPQForShard
                    ? getOrTrainPQ(shardView, pqSubspaces) : null;
            PQVectors pqv = (pq != null) ? pq.encodeAll(shardView, PhysicalCoreExecutor.pool()) : null;
            pqDoneNanos = System.nanoTime();

            // Write graph + features to temp file, streaming via suppliers
            Path tempFile = Files.createTempFile(
                    tmpDirectory, "herddb-vector-shard-", ".idx");
            // Capture the written feature set outside the writer try-with-resources
            // so it can be stamped onto SegmentWriteResult after the writer closes.
            // Derived from suppliers.keySet() so this list cannot drift if a future
            // change adds or removes a feature from the writer (issue #543).
            List<String> writtenFeatureIds;
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
                    writtenFeatureIds = RemoteSegmentGraphMerger.featureSetToStringList(suppliers.keySet());
                }
                success = true;
                graphWriteDoneNanos = System.nanoTime();

                // Upload graph file
                long graphSize = Files.size(tempFile);
                uploadBytesTotal.addAndGet(graphSize);
                String graphFilePath;
                String segUuid = indexUUID + "_seg" + segmentId;
                // Issue #616: register the segUuid in the provisional-multipart-files
                // tracker BEFORE the upload starts, not after it returns. If the upload
                // aborts mid-flight (e.g. OutOfDirectMemoryError from Netty's direct
                // buffer pool while writing block 0), any partial S3 / file-server
                // blocks were tracked-for-cleanup; the outer Phase B catch arm's
                // rollbackProvisionalArtefacts() call then deletes them. Pre-fix the
                // track happened only on successful return, leaking partial-upload
                // blocks indefinitely — see the issue #616 reproduction. The pre-track
                // is safe under success too: when Phase B + Phase C-prep complete the
                // outer code clears provisionalMultipartFiles to null WITHOUT deleting
                // anything (see the post-Phase-C-prep block in indexCheckpoint). It is
                // also safe when the upload wrote zero bytes: rollback's
                // deleteMultipartIndexFile call is a no-op against a missing key and
                // the rollback path already swallows DataStorageManagerException /
                // RuntimeException from such deletes (see rollbackProvisionalArtefacts).
                trackProvisionalMultipartFile(segUuid, "graph");
                uploadingActive.incrementAndGet();
                try {
                    graphFilePath = dataStorageManager.writeMultipartIndexFile(
                            tableSpaceUUID,
                            segUuid, "graph",
                            tempFile, uploadBytesDone::addAndGet);
                } catch (Throwable e) {
                    // Issue #616: log SEVERE with the failing segment identity BEFORE
                    // the exception propagates to awaitAllOrFirstFailure (which only
                    // surfaces the FIRST failure when shards run in parallel). Without
                    // this log the outer Phase B catch in doCheckpointFusedPQThreePhase
                    // prints "Phase B exception" with no shard identity — operators
                    // cannot tell which segment's upload aborted, which is the
                    // diagnostic gap the issue is trying to close.
                    //
                    // The broad Throwable catch is required and intentional:
                    //   (a) Netty surfaces direct-buffer allocation failures as
                    //       OutOfDirectMemoryError, which extends OutOfMemoryError
                    //       (an Error, NOT a RuntimeException) — this is the exact
                    //       motivating failure cited in issue #616's reproduction.
                    //       A narrower catch would skip the diagnostic for the very
                    //       case the issue is trying to fix.
                    //   (b) DataStorageManagerException is a RuntimeException
                    //       subclass; IOException is checked. Both are caught.
                    //   (c) We re-throw the original Throwable unchanged so the
                    //       outer Phase B failure handling (awaitAllOrFirstFailure
                    //       wrap + rollbackProvisionalArtefacts +
                    //       recoverFromPhaseBFailure) runs exactly as before.
                    LOGGER.log(Level.SEVERE,
                            "checkpoint " + indexName + " Phase B: graph multipart"
                                    + " write FAILED for segUuid=" + segUuid
                                    + " (segmentId=" + segmentId
                                    + ", vectors=" + shardSize
                                    + ") — partial multipart state queued for rollback"
                                    + " cleanup", e);
                    throw e;
                } finally {
                    uploadingActive.decrementAndGet();
                }
                graphUploadDoneNanos = System.nanoTime();

                ConcurrentHashMap<Integer, Bytes> ordinalToPk = buildOrdinalToPk(shard);
                Path mapFile = writeFusedPQMapDataToTempFile(shardView, ordinalToPk);
                try {
                    long mapSize = Files.size(mapFile);
                    uploadBytesTotal.addAndGet(mapSize);
                    String mapFilePath;
                    // Issue #616: pre-track the map multipart file for the same reason
                    // as the graph upload above. See the matching comment there.
                    trackProvisionalMultipartFile(segUuid, "map");
                    uploadingActive.incrementAndGet();
                    try {
                        mapFilePath = dataStorageManager.writeMultipartIndexFile(
                                tableSpaceUUID,
                                segUuid, "map",
                                mapFile, uploadBytesDone::addAndGet);
                    } catch (Throwable e) {
                        // Issue #616: see the matching catch on the graph upload
                        // above for why the catch is Throwable, not narrower.
                        LOGGER.log(Level.SEVERE,
                                "checkpoint " + indexName + " Phase B: map multipart"
                                        + " write FAILED for segUuid=" + segUuid
                                        + " (segmentId=" + segmentId
                                        + ", vectors=" + shardSize
                                        + ") — partial multipart state queued for"
                                        + " rollback cleanup", e);
                        throw e;
                    } finally {
                        uploadingActive.decrementAndGet();
                    }
                    compactionNodesDone.addAndGet(shardSize);
                    SegmentWriteResult swr = new SegmentWriteResult(segmentId,
                            graphFilePath, graphSize,
                            mapFilePath, mapSize,
                            graphSize + mapSize,
                            shardSize);
                    // Record which jvector features were written so the optimizer
                    // can group segments by feature set (issue #543).
                    // writtenFeatureIds was derived from suppliers.keySet() inside the
                    // writer block so it cannot drift if future code adds/removes features.
                    swr.jvectorFeatureIds = writtenFeatureIds;
                    // Per-step timing breakdown for the shard write (issue #614).
                    // Helps distinguish CPU-bound work (PQ train/encode, graph build)
                    // from I/O-bound work (uploads) when a Phase B runs unusually
                    // long. All four sub-steps are local to this shard — none of
                    // them touch the global on-disk graph.
                    long mapWriteUploadDoneNanos = System.nanoTime();
                    long pqMs = (pqDoneNanos - shardStartNanos) / 1_000_000L;
                    long graphWriteMs = (graphWriteDoneNanos - pqDoneNanos) / 1_000_000L;
                    long graphUploadMs = (graphUploadDoneNanos - graphWriteDoneNanos) / 1_000_000L;
                    long mapMs = (mapWriteUploadDoneNanos - graphUploadDoneNanos) / 1_000_000L;
                    long totalMs = (mapWriteUploadDoneNanos - shardStartNanos) / 1_000_000L;
                    LOGGER.log(Level.INFO,
                            "checkpoint {0} Phase B: shard segment {1} ({2} vectors) written —"
                                    + " pq={3} ms, graph-write={4} ms, graph-upload={5} ms,"
                                    + " map-write+upload={6} ms, total={7} ms",
                            new Object[]{indexName, segmentId, shardSize,
                                    pqMs, graphWriteMs, graphUploadMs, mapMs, totalMs});
                    return swr;
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
            closePendingCheckpointDeletesQuietly();
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
        if (version == 3) {
            // v3 format (no per-segment segmentUuid) is no longer supported.
            // v3 indexes were written before the external optimizer's ZooKeeper
            // segment registry was introduced. Delete the index and re-create it
            // to produce a v4 checkpoint with per-segment UUIDs.
            throw new DataStorageManagerException(
                    "vector index metadata version 3 is no longer supported for index " + indexName
                            + ". The v3 format lacks per-segment UUIDs required by the external"
                            + " optimizer. Delete the index and re-create it to produce a v4"
                            + " checkpoint, or downgrade to a binary that supports v3.");
        }
        if (version != METADATA_VERSION_MULTI_SEGMENT_V4
                && version != METADATA_VERSION_MULTI_SEGMENT_V5) {
            // Unknown future version — the previous binary cannot read it.
            // Fail fast: an immediate boot failure is far easier to diagnose
            // than a silent data-loss-equivalent regression.
            throw new DataStorageManagerException(
                    "unsupported vector index metadata version " + version + " for " + indexName
                            + " (this binary supports v" + METADATA_VERSION_MULTI_SEGMENT_V5
                            + "). Either re-deploy a binary that supports v" + version
                            + " OR delete the index and re-create it.");
        }

        int dim = metaBuf.getInt();
        int savedM = metaBuf.getInt();
        int savedBeamWidth = metaBuf.getInt();
        float savedNeighborOverflow = metaBuf.getFloat();
        float savedAlpha = metaBuf.getFloat();
        /* boolean savedAddHierarchy = */ metaBuf.get();
        /* boolean savedFusedPQ = */ metaBuf.get();

        long savedNextNodeId = metaBuf.getLong();

        this.dimension = dim;
        newPageId.set(status.newPageId);

        loadMultiSegmentFormat(metaBuf, version, dim, savedNextNodeId,
                savedBeamWidth, savedNeighborOverflow, savedAlpha);
    }

    private void loadMultiSegmentFormat(ByteBuffer metaBuf, int metaVersion, int dim,
                                         long savedNextNodeId,
                                         int savedBeamWidth, float savedNeighborOverflow,
                                         float savedAlpha)
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

        // ----------------------------------------------------------------
        // Phase 1 (serial): parse all segment metadata from the binary
        // stream — this is fast (no I/O) and must be done in order.
        // ----------------------------------------------------------------
        List<VectorSegment> segList = new ArrayList<>(numSegments);
        for (int s = 0; s < numSegments; s++) {
            int segId;
            long estimatedSize;
            String graphFilePath;
            long graphFileSize;
            String mapFilePath;
            long mapFileSize;
            long generation;
            String segmentUuid;
            String externalStorageKey;
            try {
                segId = dis.readInt();
                estimatedSize = dis.readLong();
                graphFilePath = dis.readUTF();
                graphFileSize = dis.readLong();
                mapFilePath = dis.readUTF();
                mapFileSize = dis.readLong();
                generation = dis.readLong();
                // v4+ format always includes a segmentUuid field. An empty string
                // means the segment has no UUID (should not occur in v4+ except during
                // a checkpoint that was written before the publisher was first attached;
                // treated as null for reconcile purposes).
                String raw = dis.readUTF();
                segmentUuid = raw.isEmpty() ? null : raw;
                // v5 adds externalStorageKey so that optimizer-adopted segments
                // (which use a 63-bit long-based storage key instead of the
                // IS-local int-based key) can be reopened correctly after restart.
                // V4 payloads predate adoption, so externalStorageKey is always null.
                if (metaVersion >= METADATA_VERSION_MULTI_SEGMENT_V5) {
                    String extRaw = dis.readUTF();
                    externalStorageKey = extRaw.isEmpty() ? null : extRaw;
                } else {
                    externalStorageKey = null;
                }
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
            seg.segmentUuid = segmentUuid;
            seg.externalStorageKey = externalStorageKey;
            segList.add(seg);
        }

        // ----------------------------------------------------------------
        // Phase 2 (parallel): download each segment's map file and open
        // its on-disk graph. Tasks are submitted to CHECKPOINT_POOL so
        // that the existing thread budget controls parallelism — no extra
        // threads are created. Each segment is independent; shared state
        // (loadingSegmentsDone, the counter below) is updated atomically.
        //
        // Publication order invariant (for gRPC GetIndexStatus consumers):
        //   loadingSegmentsTotal and loadingSegmentsDone are set BEFORE
        //   loadingFromStatus is flipped to true, so any reader who observes
        //   isLoadingFromStatus()==true is guaranteed to also see a non-zero
        //   loadingSegmentsTotal. The reverse order would allow a brief window
        //   where loading=true but total=0.
        //
        // Failure semantics: if any segment fails to load, ALL previously
        // opened BLinks (seg.onDiskPkToNode) for segments in segList are
        // closed and none are registered into the segments list (all-or-nothing).
        // This is a deliberate choice: partial state after a storage error is
        // harder to reason about than a clean failure that forces a full retry
        // on the next start(). start() callers are expected to handle this
        // exception by propagating it up and allowing the service to restart.
        // ----------------------------------------------------------------
        loadingSegmentsTotal.set(numSegments);   // set counters BEFORE the flag (invariant above)
        loadingSegmentsDone.set(0);
        loadingFromStatus = true;
        LOGGER.log(Level.INFO,
                "loading vector store {0}: {1} segments, direct-S3={2}",
                new Object[]{indexName, numSegments,
                        dataStorageManager.supportsDirectMultipartDownload()});
        Throwable loadingFailure = null;
        try {
            List<Future<?>> futures = new ArrayList<>(numSegments);
            for (VectorSegment seg : segList) {
                Future<?> f = CHECKPOINT_POOL.submit((Callable<Void>) () -> {
                    Path mapFile = readMultipartMapDataToTempFile(seg);
                    loadFusedPQSegment(seg, mapFile, dim, savedNextNodeId);
                    int done = loadingSegmentsDone.incrementAndGet();
                    if (done == 1 || done % 100 == 0 || done == numSegments) {
                        LOGGER.log(Level.INFO,
                                "loading vector store {0}: {1}/{2} segments loaded",
                                new Object[]{indexName, done, numSegments});
                    }
                    return null;
                });
                futures.add(f);
            }
            // Collect results. On the first failure, cancel all remaining pending
            // tasks (so they don't consume pool threads after we've given up),
            // then collect per-task failures to log before rethrowing.
            for (int i = 0; i < futures.size(); i++) {
                Future<?> f = futures.get(i);
                try {
                    f.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    LOGGER.log(Level.WARNING,
                            "segment load failed for store {0} segment index {1}: {2}",
                            new Object[]{indexName, i, cause});
                    if (loadingFailure == null) {
                        loadingFailure = cause;
                        // Cancel all remaining futures; already-running tasks are
                        // interrupted where possible.
                        for (Future<?> g : futures) {
                            g.cancel(true);
                        }
                    }
                } catch (CancellationException ignored) {
                    // Expected: after the first failure triggers g.cancel(true), subsequent
                    // f.get() calls on already-cancelled futures throw CancellationException.
                    // Continue draining so the original loadingFailure is rethrown below
                    // as an IOException/DataStorageManagerException, not as an opaque
                    // CancellationException.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while loading segments", e);
                }
            }
        } finally {
            loadingFromStatus = false;
            // All-or-nothing cleanup: if loading failed, close every BLink that
            // was opened in phase 2 to avoid leaking paged-storage pages. None of
            // these segments are registered in this.segments yet, so closing them
            // here is safe and does not affect search.
            if (loadingFailure != null) {
                for (VectorSegment seg : segList) {
                    seg.close();
                }
            }
        }
        if (loadingFailure instanceof IOException) {
            throw (IOException) loadingFailure;
        }
        if (loadingFailure instanceof DataStorageManagerException) {
            throw (DataStorageManagerException) loadingFailure;
        }
        if (loadingFailure != null) {
            throw new DataStorageManagerException("Segment load failed", loadingFailure);
        }

        // ----------------------------------------------------------------
        // Phase 3 (serial): register segments and update counters.
        // ----------------------------------------------------------------
        int maxIsLocalSegId = -1;
        // nextAdoptedSegmentLocalId counts DOWN from Integer.MAX_VALUE; on restart
        // we initialise it to one below the smallest adopted ID seen in this store.
        int minAdoptedSegId = Integer.MAX_VALUE;
        boolean hasAdoptedSegments = false;
        long maxGeneration = loadedGeneration;
        for (VectorSegment seg : segList) {
            segments.add(seg);
            // Issue #455: snapshot the segment's in-memory footprint into the
            // on-disk counter so estimatedMemoryUsageBytes() is O(1).
            registerSegmentMemoryEstimate(seg);
            if (seg.externalStorageKey == null) {
                // IS-locally-produced segment: contributes to nextSegmentId counter.
                if (seg.segmentId > maxIsLocalSegId) {
                    maxIsLocalSegId = seg.segmentId;
                }
            } else {
                // Adopted (optimizer-produced) segment: contributes to the DOWN counter.
                hasAdoptedSegments = true;
                if (seg.segmentId < minAdoptedSegId) {
                    minAdoptedSegId = seg.segmentId;
                }
            }
            if (seg.generation > maxGeneration) {
                maxGeneration = seg.generation;
            }
        }
        nextSegmentId.set(maxIsLocalSegId + 1);
        if (hasAdoptedSegments) {
            // Resume the adopted-segment counter one below the lowest previously-used
            // adopted ID so subsequent adoptions stay below all existing adopted IDs.
            nextAdoptedSegmentLocalId.set(minAdoptedSegId - 1);
        }
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
                segmentStorageKey(seg),
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
    // Issue #616 review item: writeOneSegmentData is reachable only through
    // buildSegmentsInParallel, which currently has no in-tree caller — this is
    // dormant Phase B code preserved for the parallel-slice rebuild path.
    // The track-after-success pattern below mirrors the pre-#616 form of
    // writeShardAsFusedPQSegment; if this method is re-enabled it MUST adopt
    // the same pre-track + Throwable-catch layout as the patched live path
    // (search for "Issue #616:" in writeShardAsFusedPQSegment) or partial
    // multipart uploads will again leak into remote storage on Phase B failure.
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
                        graphFileSize + mapFileSize,
                        partNodeToPk.size());
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
                List<Feature> features = List.of(
                        new FusedPQ(mergedGraph.maxDegree(), pq),
                        new InlineVectors(dim));
                try (RandomAccessOnDiskGraphIndexWriter writer =
                        GraphWriterFactory.openWriter(mergedGraph, tempFile, totalVectors, features)) {
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
     *
     * <p>When {@link DataStorageManager#supportsDirectMultipartDownload()} is
     * {@code true}, the download is issued directly to object storage (e.g. GCS),
     * bypassing the gRPC file-server round-trips that dominate cold-start recovery
     * time (issue #381). Otherwise, falls back to the original path through the
     * multipart reader supplier.
     *
     * <p>The caller is responsible for deleting the returned file.
     */
    private Path readMultipartMapDataToTempFile(VectorSegment seg)
            throws IOException, DataStorageManagerException {
        if (seg.mapFilePath == null || seg.mapFileSize == 0) {
            // Empty or missing map — return an empty temp file
            return Files.createTempFile(tmpDirectory, "herddb-vector-map-empty-", ".tmp");
        }
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-vector-map-", ".tmp");
        boolean success = false;
        try {
            if (dataStorageManager.supportsDirectMultipartDownload()) {
                // Fast path: download blocks directly from object storage, skipping the
                // file-server gRPC hops. The map-file data is never cached (it is written
                // to a temp file and deleted immediately after loading), so there is no
                // benefit to routing through the SegmentBlockCache.
                dataStorageManager.downloadMultipartIndexFile(
                        tableSpaceUUID,
                        segmentStorageKey(seg),
                        "map",
                        seg.mapFileSize,
                        tempFile);
            } else {
                // Fallback: sequential read via the RemoteRandomAccessReader /
                // SegmentBlockCache pipeline (original gRPC path).
                io.github.jbellis.jvector.disk.ReaderSupplier supplier =
                        dataStorageManager.multipartIndexReaderSupplier(
                                tableSpaceUUID,
                                segmentStorageKey(seg),
                                "map",
                                seg.mapFileSize);
                try (io.github.jbellis.jvector.disk.RandomAccessReader reader = supplier.get();
                     FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                     BufferedOutputStream bos = new BufferedOutputStream(fos, CHUNK_SIZE)) {
                    reader.seek(0);
                    byte[] buf = new byte[CHUNK_SIZE];
                    long remaining = seg.mapFileSize;
                    while (remaining > 0) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        byte[] readBuf = toRead == buf.length ? buf : new byte[toRead];
                        reader.readFully(readBuf);
                        bos.write(readBuf, 0, toRead);
                        remaining -= toRead;
                    }
                }
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

        // Write v5 format: v5 adds externalStorageKey per-segment so that
        // optimizer-adopted segments survive a restart. v4 (which lacked that
        // field) is still accepted for reading but will never be written.
        // v3 (which lacked segmentUuid) is no longer supported for reading or
        // writing (issue #499).

        VisibleByteArrayOutputStream baos = new VisibleByteArrayOutputStream(256);
        try (java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
            dos.writeInt(METADATA_VERSION_MULTI_SEGMENT_V5);
            dos.writeInt(dimension);
            dos.writeInt(m);
            dos.writeInt(beamWidth);
            dos.writeFloat(neighborOverflow);
            dos.writeFloat(alpha);
            dos.writeByte(ADD_HIERARCHY ? 1 : 0);
            dos.writeByte(1); // fusedPQ
            dos.writeLong(nextNodeId.get());
            dos.writeLong(newGeneration);
            dos.writeInt(totalSegments);

            for (VectorSegment seg : sealedSegments) {
                writeSegmentMeta(dos, seg.segmentId, seg.segmentUuid, seg.estimatedSizeBytes,
                        seg.graphFilePath, seg.graphFileSize,
                        seg.mapFilePath, seg.mapFileSize, seg.generation,
                        seg.externalStorageKey);
            }
            for (VectorSegment seg : mergeableSegments) {
                writeSegmentMeta(dos, seg.segmentId, seg.segmentUuid, seg.estimatedSizeBytes,
                        seg.graphFilePath, seg.graphFileSize,
                        seg.mapFilePath, seg.mapFileSize, seg.generation,
                        seg.externalStorageKey);
            }
            for (SegmentWriteResult swr : newSegmentResults) {
                // IS-produced segments have no external storage key.
                writeSegmentMeta(dos, swr.segmentId, swr.segmentUuid, swr.estimatedSizeBytes,
                        swr.graphFilePath, swr.graphFileSize,
                        swr.mapFilePath, swr.mapFileSize, newGeneration,
                        /* externalStorageKey= */ null);
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
            int segmentId, String segmentUuid, long estimatedSizeBytes,
            String graphFilePath, long graphFileSize,
            String mapFilePath, long mapFileSize,
            long generation, String externalStorageKey) throws IOException {
        dos.writeInt(segmentId);
        dos.writeLong(estimatedSizeBytes);
        dos.writeUTF(graphFilePath);
        dos.writeLong(graphFileSize);
        dos.writeUTF(mapFilePath != null ? mapFilePath : "");
        dos.writeLong(mapFileSize);
        dos.writeLong(generation);
        // v4+ format always includes segmentUuid. Empty string represents "no UUID assigned"
        // (written for segments that predate the publisher attachment; read back as null).
        dos.writeUTF(segmentUuid != null ? segmentUuid : "");
        // v5 adds externalStorageKey so that optimizer-adopted segments survive a restart.
        // Empty string represents null (no external key — IS-produced segments).
        dos.writeUTF(externalStorageKey != null ? externalStorageKey : "");
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

    /**
     * Issue #535: drains every queued {@link VectorSegment} in
     * {@link #pendingSegmentCloses}, closing each one, dropping its
     * BLink storage entry, and unregistering its memory estimate.
     * Caller MUST hold {@link #compactionLock} so the close cannot race
     * with a {@link #runCompactionCycle} that has already snapshotted
     * {@code this.segments} and would dereference {@code seg.onDiskGraph}
     * on the (now-closed) segment.
     *
     * <p>Safe to call repeatedly: subsequent calls observe an empty queue
     * and exit early. {@link VectorSegment#close()} is itself idempotent
     * (its {@code if (odg != null)} guard).
     *
     * <p>{@link #unregisterSegmentMemoryEstimate} is called from inside
     * this drain rather than at {@link #dropSegmentByUuid}'s mutation
     * site so the on-disk-segment memory counter and the actual heap
     * (BLink pages, pkData, pkOffsets/pkLengths) release in lockstep
     * (pr-reviewer follow-up). Releasing the counter eagerly at the
     * dropSegmentByUuid mutation site would create a window where
     * {@link #getOnDiskSegmentsEstimatedMemoryBytes} reports a value
     * lower than the heap actually occupies, biasing the
     * memory-pressure heuristics low.
     */
    private void drainPendingSegmentClosesUnderCompactionLock() {
        // Sanity-check the lock invariant (assertions are no-ops in production).
        assert compactionLock.isHeldByCurrentThread()
                : "drainPendingSegmentClosesUnderCompactionLock requires compactionLock";
        VectorSegment seg;
        while ((seg = pendingSegmentCloses.pollFirst()) != null) {
            unregisterSegmentMemoryEstimate(seg);
            try {
                seg.close();
            } catch (RuntimeException closeEx) {
                // Narrow catch: VectorSegment.close() throws only
                // RuntimeException (BLink cleanup). Logging-and-continue is
                // the correct behaviour — the segment's bookkeeping must
                // not block the drain of subsequent segments.
                LOGGER.log(Level.WARNING,
                        "drainPendingSegmentCloses: error closing segment "
                                + seg.segmentId + " in store " + indexName, closeEx);
            }
            dropSegmentBLinkStorage(seg);
        }
    }

    /**
     * Issue #535: opportunistic drain — runs only if {@link #compactionLock}
     * is currently free AND not already held by the current thread.
     * Called from {@link #dropSegmentByUuid} immediately after the
     * writeLock release, so a "drop while no compaction is running" path
     * closes synchronously (preserving the original behaviour for the
     * common case where the watcher fires while the IS is idle). When
     * compaction IS running, this is a no-op and the cycle's own finally
     * block drains the queue when it completes.
     *
     * <p>The {@code isHeldByCurrentThread} guard is critical because
     * {@link java.util.concurrent.locks.ReentrantLock} returns {@code true}
     * from {@code tryLock} when the calling thread already holds it
     * (re-entrance). Without the guard, a {@code dropSegmentByUuid} call
     * fired from inside a {@link #runCompactionCycle}-driven code path
     * (e.g., a test hook, or any future caller that runs on the
     * compaction thread itself) would acquire the lock re-entrantly and
     * close the segment immediately — reintroducing the very race this
     * fix is meant to eliminate.
     */
    private void drainPendingSegmentClosesOpportunistically() {
        if (pendingSegmentCloses.isEmpty()) {
            return;
        }
        if (compactionLock.isHeldByCurrentThread()) {
            // The current thread is already inside a compaction cycle (or
            // some other code path that took compactionLock). Reentering
            // the lock here would let the drain run alongside that cycle's
            // own snapshot iteration. Skip — the cycle's finally block
            // drains the queue before releasing compactionLock.
            return;
        }
        if (!compactionLock.tryLock()) {
            // Another thread is running a compaction. Its finally block
            // drains pendingSegmentCloses before releasing compactionLock,
            // so the queue is guaranteed to be empty by the time the next
            // caller observes the lock as free.
            return;
        }
        try {
            drainPendingSegmentClosesUnderCompactionLock();
        } finally {
            compactionLock.unlock();
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
    // Temporary BLink factories for compaction & checkpoint memory bounding
    // (issue #290).
    //
    // The compaction authority map and the pendingCompactionDeletes /
    // pendingCheckpointDeletes sets all share the same problem: they may
    // accumulate hundreds of millions of PKs during a long-running cycle and
    // exhaust the JVM heap if held in unbounded HashMaps. Backing them with
    // BLink<Bytes, Long> reuses the same paged-storage / page-replacement
    // policy we already use for seg.onDiskPkToNode, so memory stays bounded
    // by the index page budget.
    //
    // Each temporary index is created with a unique name (cycle-id qualified
    // by the caller) and dropped via TempBlinkHandle.drop() in a finally
    // block. The provisional name uses the {@code _tmp_} infix so that
    // operator-side cleanup tooling can identify orphans left behind by
    // ungraceful JVM exits.
    // -------------------------------------------------------------------------

    /**
     * Creates a fresh BLink-backed compaction authority map for the supplied
     * cycle id. The returned wrapper owns the BLink and its temp storage; it
     * <strong>must</strong> be closed in a {@code finally} block so the temp
     * pages are dropped.
     */
    CompactionAuthorityMap createTempCompactionAuthorityMap(long cycleId) {
        String storeName = indexUUID + "_tmp_cmpaut_" + cycleId;
        try {
            dataStorageManager.initIndex(tableSpaceUUID, storeName);
        } catch (DataStorageManagerException e) {
            throw new RuntimeException("Failed to init compaction authority BLink storage "
                    + storeName + " for vector store " + indexName, e);
        }
        BLink<Bytes, Long> blink = new BLink<>(
                memoryManager.getMaxLogicalPageSize(),
                BytesLongSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesLongStorage(storeName));
        return new CompactionAuthorityMap(blink, () -> dropTempIndexQuietly(storeName));
    }

    /**
     * Closes and clears the {@link #pendingCheckpointDeletes} field, swallowing
     * any close failures (best-effort cleanup so a checkpoint never fails just
     * because the temp PK set could not be released).
     */
    private void closePendingCheckpointDeletesQuietly() {
        PagedPkSet set = this.pendingCheckpointDeletes;
        this.pendingCheckpointDeletes = null;
        if (set != null) {
            try {
                set.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "vector store " + indexName
                                + ": failed to close pendingCheckpointDeletes set", e);
            }
        }
    }

    /**
     * Creates a fresh BLink-backed PK set with the given purpose tag (used to
     * build a unique temp store name). The returned wrapper owns the BLink
     * and its temp storage; it <strong>must</strong> be closed in a
     * {@code finally} block so the temp pages are dropped. Callers
     * disambiguate concurrent or back-to-back instances by adding a
     * monotonically increasing token to the {@code purposeTag}.
     */
    PagedPkSet createTempPagedPkSet(String purposeTag) {
        String storeName = indexUUID + "_tmp_pkset_" + purposeTag;
        try {
            dataStorageManager.initIndex(tableSpaceUUID, storeName);
        } catch (DataStorageManagerException e) {
            throw new RuntimeException("Failed to init paged PK set BLink storage "
                    + storeName + " for vector store " + indexName, e);
        }
        BLink<Bytes, Long> blink = new BLink<>(
                memoryManager.getMaxLogicalPageSize(),
                BytesLongSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesLongStorage(storeName));
        return new PagedPkSet(blink, () -> dropTempIndexQuietly(storeName));
    }

    /**
     * Best-effort drop of a temporary index. Failures are logged but never
     * propagated — the temp index leaking pages is preferable to letting a
     * cleanup failure abort the surrounding operation.
     */
    private void dropTempIndexQuietly(String storeName) {
        try {
            dataStorageManager.dropIndex(tableSpaceUUID, storeName);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.WARNING,
                    "vector store " + indexName
                            + ": failed to drop temp BLink storage " + storeName, e);
        }
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
            dataStorageManager.writeIndexPage(tableSpaceUUID, storeName, pageId,
                    new DataStorageManager.DataWriter() {
                        @Override
                        public void write(ByteBufDataOutput out) throws IOException {
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
                                        // writeArray(Bytes) calls Bytes.writeTo(ByteBuf):
                                        // zero-copy for off-heap IndexKeySlab keys (issue #497).
                                        out.writeArray(x);
                                        out.writeVLong(y);
                                    }
                                } catch (IOException e) {
                                    throw new java.io.UncheckedIOException(e);
                                }
                            });
                            out.writeByte(NODE_PAGE_END_BLOCK);
                        }
                        @Override
                        public int sizeEstimate() {
                            return IncrementalBLinkPageCodec.nodePageSizeEstimate(data);
                        }
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
        closePendingCheckpointDeletesQuietly();
        liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;
        CountDownLatch latch = this.checkpointPhaseComplete;
        if (latch != null) {
            latch.countDown();
            this.checkpointPhaseComplete = null;
        }
        // Issue #455: drop every segment's contribution from the on-disk
        // memory counter before closing them.
        for (VectorSegment seg : segments) {
            unregisterSegmentMemoryEstimate(seg);
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

    /**
     * Returns the {@link #indexUUID} so the indexing-service can embed it in
     * the {@link herddb.indexing.WatermarkSnapshot} for checkpoint recovery
     * without full DML log replay (issue #368).
     */
    @Override
    public String getStoreUUID() {
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

    /**
     * Total number of in-memory vectors across the live, frozen, and deferred
     * shards — every vector that a checkpoint could still flush to disk.
     *
     * <p>Differs from {@link #getLiveNodeCount()}, which omits
     * {@code deferredShards} for historical diagnostic reasons. The
     * memory-pressure checkpoint guard (issue #563) uses this fuller count so
     * it stays consistent with {@link #getLiveShardMemoryBytes()}: a Phase A
     * byte-cap deferral parks shards holding genuine un-persisted vectors in
     * {@code deferredShards}, and those must not be mistaken for "nothing to
     * checkpoint".
     *
     * <p>The three list reads are intentionally non-atomic — the value is a
     * best-effort snapshot for the compaction loop, which re-evaluates the
     * condition on every cycle.
     */
    int liveFrozenDeferredNodeCount() {
        int count = totalLiveSize();
        List<LiveGraphShard> frozen = frozenShards;
        if (frozen != null) {
            for (LiveGraphShard shard : frozen) {
                count += shard.nodeToPk.size();
            }
        }
        List<LiveGraphShard> deferred = deferredShards;
        if (deferred != null) {
            for (LiveGraphShard shard : deferred) {
                count += shard.nodeToPk.size();
            }
        }
        return count;
    }

    public int getOnDiskNodeCount() {
        return (int) onDiskNodeToPkSize();
    }

    /**
     * Issue #617: snapshot of the multipart storage keys of every on-disk
     * segment currently registered in this store. Used by the operator-
     * facing {@code DeleteSegment} RPC handler to enumerate candidates
     * (via {@code describe-index --json}'s {@code corrupted_segments} list)
     * and to verify presence before mutating state.
     *
     * <p>The returned list is a defensive copy and is stable across
     * concurrent {@link #dropSegmentByStorageKey} / compaction swaps. It
     * may legitimately contain duplicate-looking entries on a transient
     * adoption race: callers MUST treat the result as a snapshot and
     * re-read it if they need a fresh view.
     */
    public java.util.List<String> getSegmentStorageKeysSnapshot() {
        // CopyOnWriteArrayList iterator is snapshot-safe; no readLock needed
        // for the enumeration itself, only for any subsequent mutation that
        // wants to act on the snapshot atomically (that is the caller's job).
        java.util.List<String> out = new java.util.ArrayList<>(segments.size());
        for (VectorSegment s : segments) {
            out.add(segmentStorageKey(s));
        }
        return out;
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

    /**
     * Returns the current minimum candidate-segment count for the byte-threshold
     * compaction trigger. Defaults to {@link #DEFAULT_VECTOR_INDEX_COMPACTION_MIN_COUNT}
     * unless overridden by {@link #configureCompaction} (issue #562).
     */
    public int getVectorIndexCompactionMinCount() {
        return vectorIndexCompactionMinCount;
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

    /**
     * Returns the wall-clock nanoseconds the most recent
     * {@code atomicSwapCompactionResult} call held {@code stateLock.writeLock()}
     * (issue #462). Used by the lock-progress regression tests to assert the
     * writeLock window stays tiny (≪ persist duration) so concurrent searches
     * are not blocked behind the compaction-swap critical section.
     */
    public long getLastCompactionSwapWriteLockNanos() {
        return lastCompactionSwapWriteLockNanos.get();
    }

    /**
     * Returns the wall-clock nanoseconds the most recent Phase C of
     * {@code doCheckpointFusedPQThreePhase} held {@code stateLock.writeLock()}
     * (issue #462). Used by the lock-progress regression tests to assert the
     * writeLock window stays tiny (≪ pending-deletes-apply duration) so
     * concurrent searches are not blocked behind Phase C.
     */
    public long getLastPhaseCWriteLockNanos() {
        return lastPhaseCWriteLockNanos.get();
    }

    /**
     * Returns the estimated heap footprint of the live (in-memory) vectors.
     *
     * <p>Issue #563: this used to delegate to {@link #estimatedMemoryUsageBytes()},
     * which also includes the on-disk segment pkData/BLink footprint — so the
     * {@code live_vectors_memory_bytes} diagnostic field reported gigabytes of
     * "live" memory even when {@code live_node_count} was 0. It now returns the
     * live/frozen/deferred shard footprint only; the on-disk contribution is
     * surfaced separately via {@link #getOnDiskSegmentsEstimatedMemoryBytes()}.
     */
    public long getLiveVectorsMemoryBytes() {
        return getLiveShardMemoryBytes();
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

    /** Returns the total number of segment-count back-pressure events (issue #354). */
    public long getSegmentCountBackpressureTotal() {
        return segmentCountBackpressureTotal.get();
    }

    /** Returns the total wall-clock milliseconds spent in segment-count back-pressure (issue #354). */
    public long getSegmentCountBackpressureTimeMs() {
        return segmentCountBackpressureTimeMs.get();
    }

    /** Returns {@code true} while a caller is blocked in segment-count back-pressure (issue #354). */
    public boolean isSegmentCountBackpressureActive() {
        return segmentCountBackpressureActive != 0;
    }

    /**
     * Returns the number of times segment-count back-pressure was force-released
     * by the safety timeout rather than by a successful compaction (issue #370).
     */
    public long getSegmentCountBackpressureTimeouts() {
        return segmentCountBackpressureTimeouts.get();
    }

    /**
     * Returns the current segment-count-monitor generation counter.
     * Incremented by every {@link #notifySegmentCountMonitor()} call.
     * Exposed for testing — callers can verify that a compaction cycle
     * (including the empty-candidates and failure paths) notified the monitor.
     *
     * <p>Acquires {@code segmentCountMonitor} to guarantee the latest value is
     * visible to any calling thread, matching the lock used by the writer.
     */
    public int getSegmentCountGeneration() {
        synchronized (segmentCountMonitor) {
            return segmentCountGeneration;
        }
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
