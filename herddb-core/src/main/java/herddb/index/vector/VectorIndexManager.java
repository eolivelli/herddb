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

import herddb.codec.RecordSerializer;
import herddb.core.AbstractIndexManager;
import herddb.core.AbstractTableManager;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkIndexDataStorage;
import herddb.index.blink.BytesBytesSizeEvaluator;
import herddb.index.blink.BytesLongSizeEvaluator;
import herddb.log.CommitLog;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.Table;
import herddb.model.TableContext;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.MapRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.diversity.VamanaDiversityProvider;
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
import java.io.DataOutputStream;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Vector index manager backed by jvector (OnHeapGraphIndex / HNSW-style).
 * Supports only FLOATARRAY columns and does not support UNIQUE constraint.
 *
 * <p>When {@code fusedPQ} is enabled (default), checkpoints use jvector's
 * {@link OnDiskGraphIndex} format with FusedPQ + InlineVectors features for
 * faster approximate scoring at search time. On load, a hybrid approach is
 * used: the loaded on-disk graph is searched with FusedPQ scoring, and new
 * inserts since the last checkpoint are searched in-memory. Results are merged.
 *
 * @author enrico.olivelli
 */
@SuppressWarnings({"deprecation"})
public class VectorIndexManager extends AbstractIndexManager {

    private static final Logger LOGGER = Logger.getLogger(VectorIndexManager.class.getName());

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

    /** Number of threads used for parallel index rebuild. */
    private static final int REBUILD_THREADS = Integer.getInteger("herddb.vectorindex.rebuild.threads", 8);

    private static final ThreadFactory DAEMON_THREAD_FACTORY = r -> {
        Thread t = new Thread(r, "vector-index-rebuild");
        t.setDaemon(true);
        return t;
    };

    /* property keys for CREATE INDEX ... WITH */
    public static final String PROP_M = "m";
    public static final String PROP_BEAM_WIDTH = "beamWidth";
    public static final String PROP_NEIGHBOR_OVERFLOW = "neighborOverflow";
    public static final String PROP_ALPHA = "alpha";
    public static final String PROP_FUSED_PQ = "fusedPQ";
    public static final String PROP_SIMILARITY = "similarity";
    public static final String PROP_MAX_SEGMENT_SIZE = "maxSegmentSize";

    /* instance hyper-parameters (read from index properties) */
    private final int m;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final boolean fusedPQ;
    private final VectorSimilarityFunction similarityFunction;
    private final long maxSegmentSize;

    private final MemoryManager memoryManager;

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

    // -------------------------------------------------------------------------
    // In-memory state – LIVE inserts (new since last checkpoint)
    // -------------------------------------------------------------------------

    /** nodeId → vector (for scoring during incremental inserts after load).
     *  Volatile: swapped to a new empty map in Phase A of checkpoint. */
    private volatile ConcurrentHashMap<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();

    /** primary-key bytes → jvector node ID (live inserts only).
     *  Volatile: swapped in Phase A of checkpoint. */
    private volatile ConcurrentHashMap<Bytes, Integer> pkToNode = new ConcurrentHashMap<>();

    /** jvector node ID → primary-key bytes (live inserts only).
     *  Volatile: swapped in Phase A of checkpoint. */
    private volatile ConcurrentHashMap<Integer, Bytes> nodeToPk = new ConcurrentHashMap<>();

    /** Monotonically increasing node-ID counter. */
    private final AtomicInteger nextNodeId = new AtomicInteger(0);

    /** Page-ID counter, same pattern as BRINIndexManager. */
    private final AtomicLong newPageId = new AtomicLong(1);

    /** Tracks whether the index has been modified since the last successful checkpoint. */
    private final AtomicBoolean dirty = new AtomicBoolean(true);

    private volatile int dimension = 0;
    private volatile RandomAccessVectorValues mravv = null;
    private volatile GraphIndexBuilder builder = null;

    // -------------------------------------------------------------------------
    // Frozen state – snapshot captured in Phase A of checkpoint, searchable during Phase B
    // -------------------------------------------------------------------------

    /** Frozen builder from Phase A — searched during Phase B, closed in Phase C. */
    private volatile GraphIndexBuilder frozenBuilder;
    /** Frozen RAVV from Phase A — used for search during Phase B. */
    private volatile RandomAccessVectorValues frozenMravv;
    /** Frozen nodeToPk from Phase A — used for PK resolution during Phase B search. */
    private volatile ConcurrentHashMap<Integer, Bytes> frozenNodeToPk;

    /** PKs deleted during Phase B that may exist in frozen state or mergeable segments being rewritten.
     *  Non-null only during Phase B. */
    private volatile Set<Bytes> pendingCheckpointDeletes;

    /** Max live vectors allowed during Phase B before back-pressure kicks in.
     *  When reached, recordInserted blocks until Phase C completes. */
    private volatile int liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;

    /** Signaled when Phase C completes. DML threads wait on this if live cap is exceeded. */
    private volatile CountDownLatch checkpointPhaseComplete;

    // -------------------------------------------------------------------------
    // On-disk state – multiple segments, each containing an independent FusedPQ graph
    // -------------------------------------------------------------------------

    /** On-disk segments. CopyOnWriteArrayList for safe concurrent search during checkpoint. */
    private volatile List<VectorSegment> segments = new java.util.concurrent.CopyOnWriteArrayList<>();

    /** Counter for assigning unique segment IDs. */
    private final AtomicInteger nextSegmentId = new AtomicInteger(0);

    /** Protects rebuild/checkpoint from seeing partially-updated in-memory state.
     *  DML operations (recordInserted/recordDeleted) acquire the read lock (shared),
     *  allowing concurrent inserts. Checkpoint and rebuild state-swaps acquire the
     *  write lock (exclusive). */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public VectorIndexManager(Index index,
                               MemoryManager memoryManager,
                               AbstractTableManager tableManager,
                               CommitLog log,
                               DataStorageManager dataStorageManager,
                               TableSpaceManager tableSpaceManager,
                               String tableSpaceUUID,
                               long transaction,
                               int writeLockTimeout,
                               int readLockTimeout,
                               long serverMaxSegmentSize,
                               StatsLogger statsLogger) {
        super(index, tableManager, dataStorageManager, tableSpaceUUID, log,
                transaction, writeLockTimeout, readLockTimeout);
        this.memoryManager = memoryManager;
        Map<String, String> props = index.properties;
        this.m = intProp(props, PROP_M, DEFAULT_M);
        this.beamWidth = intProp(props, PROP_BEAM_WIDTH, DEFAULT_BEAM_WIDTH);
        this.neighborOverflow = floatProp(props, PROP_NEIGHBOR_OVERFLOW, DEFAULT_NEIGHBOR_OVERFLOW);
        this.alpha = floatProp(props, PROP_ALPHA, DEFAULT_ALPHA);
        this.fusedPQ = boolProp(props, PROP_FUSED_PQ, true);
        this.similarityFunction = parseSimilarity(props.getOrDefault(PROP_SIMILARITY, "cosine"));
        // Per-index override takes precedence over server config
        this.maxSegmentSize = longProp(props, PROP_MAX_SEGMENT_SIZE, serverMaxSegmentSize);
        registerMetrics(statsLogger);
    }

    private void registerMetrics(StatsLogger statsLogger) {
        statsLogger.registerGauge("node_count", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return getNodeCount();
            }
        });
        statsLogger.registerGauge("live_node_count", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return getLiveNodeCount();
            }
        });
        statsLogger.registerGauge("ondisk_node_count", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return getOnDiskNodeCount();
            }
        });
        statsLogger.registerGauge("segment_count", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return getSegmentCount();
            }
        });
        statsLogger.registerGauge("dimension", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return getDimension();
            }
        });
        statsLogger.registerGauge("estimated_size_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return getEstimatedSizeBytes();
            }
        });
        statsLogger.registerGauge("live_vectors_memory_bytes", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }
            @Override
            public Long getSample() {
                return (long) vectors.size() * dimension * Float.BYTES;
            }
        });
        statsLogger.registerGauge("dirty", new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }
            @Override
            public Integer getSample() {
                return dirty.get() ? 1 : 0;
            }
        });
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Integer.parseInt(v);
    }

    private static float floatProp(Map<String, String> props, String key, float defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Float.parseFloat(v);
    }

    private static long longProp(Map<String, String> props, String key, long defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Long.parseLong(v);
    }

    private static boolean boolProp(Map<String, String> props, String key, boolean defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Boolean.parseBoolean(v);
    }

    private static VectorSimilarityFunction parseSimilarity(String value) {
        switch (value.toLowerCase()) {
            case "cosine":
                return VectorSimilarityFunction.COSINE;
            case "euclidean":
                return VectorSimilarityFunction.EUCLIDEAN;
            case "dot":
                return VectorSimilarityFunction.DOT_PRODUCT;
            default:
                throw new IllegalArgumentException("unknown similarity function: " + value
                        + " (supported: cosine, euclidean, dot)");
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    protected boolean doStart(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        LOGGER.log(Level.FINE, "start VECTOR index {0} uuid {1}",
                new Object[]{index.name, index.uuid});

        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            LOGGER.log(Level.FINE, "loaded empty vector index {0}", index.name);
            return true;
        }

        IndexStatus status;
        try {
            status = dataStorageManager.getIndexStatus(tableSpaceUUID, index.uuid, sequenceNumber);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.SEVERE,
                    "cannot load index {0} due to {1}, it will be rebuilt",
                    new Object[]{index.name, e});
            return false;
        }

        if (status.indexData == null || status.indexData.length == 0) {
            LOGGER.log(Level.INFO,
                    "no index data for {0}, treating as empty index (no rebuild needed)",
                    index.name);
            return true;
        }

        try {
            return loadFromStatus(status);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE,
                    "cannot load vector index {0}: {1}, it will be rebuilt",
                    new Object[]{index.name, e});
            resetState();
            return false;
        }
    }

    /** Metadata version for multi-segment format. */
    private static final int METADATA_VERSION_MULTI_SEGMENT = 3;

    private boolean loadFromStatus(IndexStatus status) throws IOException, DataStorageManagerException {
        ByteBuffer metaBuf = ByteBuffer.wrap(status.indexData);

        int version = metaBuf.getInt();
        if (version != METADATA_VERSION_SIMPLE && version != METADATA_VERSION_FUSEDPQ
                && version != METADATA_VERSION_MULTI_SEGMENT) {
            LOGGER.log(Level.SEVERE,
                    "unsupported vector index metadata version {0} for {1}, rebuilding",
                    new Object[]{version, index.name});
            return false;
        }

        int dim = metaBuf.getInt();
        int savedM = metaBuf.getInt();
        int savedBeamWidth = metaBuf.getInt();
        float savedNeighborOverflow = metaBuf.getFloat();
        float savedAlpha = metaBuf.getFloat();
        /* boolean savedAddHierarchy = */ metaBuf.get(); // reserved

        boolean savedFusedPQ = false;
        if (version >= METADATA_VERSION_FUSEDPQ) {
            savedFusedPQ = metaBuf.get() != 0;
        }

        int savedNextNodeId = metaBuf.getInt();

        this.dimension = dim;
        newPageId.set(status.newPageId);

        if (version == METADATA_VERSION_MULTI_SEGMENT) {
            return loadMultiSegmentFormat(metaBuf, dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
        }

        // Legacy single-segment format (v1 or v2)
        int numGraphChunks = metaBuf.getInt();
        long[] graphChunkPageIds = new long[numGraphChunks];
        for (int i = 0; i < numGraphChunks; i++) {
            graphChunkPageIds[i] = metaBuf.getLong();
        }

        int numMapChunks = metaBuf.getInt();
        long[] mapChunkPageIds = new long[numMapChunks];
        for (int i = 0; i < numMapChunks; i++) {
            mapChunkPageIds[i] = metaBuf.getLong();
        }

        if (dim == 0 || numGraphChunks == 0) {
            LOGGER.log(Level.INFO, "vector index {0} is empty, no rebuild needed", index.name);
            return true;
        }

        Path mapFile = readChunksToTempFile(mapChunkPageIds, TYPE_VECTOR_MAPCHUNK);
        Path graphFile = readChunksToTempFile(graphChunkPageIds, TYPE_VECTOR_GRAPHCHUNK);

        if (savedFusedPQ) {
            VectorSegment seg = new VectorSegment(0);
            seg.estimatedSizeBytes = (long) numGraphChunks * CHUNK_SIZE;
            seg.graphPageIds = toLongList(graphChunkPageIds);
            seg.mapPageIds = toLongList(mapChunkPageIds);
            boolean ok = loadFusedPQSegment(seg, mapFile, graphFile, dim, savedNextNodeId);
            if (ok) {
                segments.add(seg);
                nextSegmentId.set(1);
                // Compute nextNodeId from max ordinal in the segment
                int maxOrd = -1;
                try (Stream<Map.Entry<Bytes, Bytes>> stream = seg.scanNodeToPk()) {
                    maxOrd = stream.mapToInt(e -> bytesToOrdinal(e.getKey())).max().orElse(-1);
                }
                this.nextNodeId.set(maxOrd + 1);
                initEmptyLiveBuilder(dim, savedBeamWidth, savedNeighborOverflow, savedAlpha);
            }
            return ok;
        } else {
            try {
                return loadSimpleFormat(mapFile, graphFile,
                        dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
            } finally {
                Files.deleteIfExists(mapFile);
                Files.deleteIfExists(graphFile);
            }
        }
    }

    private boolean loadMultiSegmentFormat(ByteBuffer metaBuf, int dim, int savedNextNodeId,
                                            int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException, DataStorageManagerException {
        int numSegments = metaBuf.getInt();

        if (dim == 0 || numSegments == 0) {
            LOGGER.log(Level.INFO, "vector index {0} is empty (multi-segment), no rebuild needed", index.name);
            return true;
        }

        int maxSegId = -1;
        for (int s = 0; s < numSegments; s++) {
            int segId = metaBuf.getInt();
            long estimatedSize = metaBuf.getLong();
            int numGraphChunks = metaBuf.getInt();
            long[] graphChunkPageIds = new long[numGraphChunks];
            for (int i = 0; i < numGraphChunks; i++) {
                graphChunkPageIds[i] = metaBuf.getLong();
            }
            int numMapChunks = metaBuf.getInt();
            long[] mapChunkPageIds = new long[numMapChunks];
            for (int i = 0; i < numMapChunks; i++) {
                mapChunkPageIds[i] = metaBuf.getLong();
            }

            Path mapFile = readChunksToTempFile(mapChunkPageIds, TYPE_VECTOR_MAPCHUNK);
            Path graphFile = readChunksToTempFile(graphChunkPageIds, TYPE_VECTOR_GRAPHCHUNK);

            VectorSegment seg = new VectorSegment(segId);
            seg.estimatedSizeBytes = estimatedSize;
            seg.graphPageIds = toLongList(graphChunkPageIds);
            seg.mapPageIds = toLongList(mapChunkPageIds);
            if (!loadFusedPQSegment(seg, mapFile, graphFile, dim, savedNextNodeId)) {
                return false;
            }
            segments.add(seg);
            if (segId > maxSegId) {
                maxSegId = segId;
            }
        }
        nextSegmentId.set(maxSegId + 1);

        // Compute nextNodeId from max ordinal across all segments
        int maxOrd = -1;
        for (VectorSegment seg : segments) {
            try (Stream<Map.Entry<Bytes, Bytes>> stream = seg.scanNodeToPk()) {
                int segMax = stream.mapToInt(e -> bytesToOrdinal(e.getKey())).max().orElse(-1);
                if (segMax > maxOrd) {
                    maxOrd = segMax;
                }
            }
        }
        this.nextNodeId.set(maxOrd + 1);

        initEmptyLiveBuilder(dim, savedBeamWidth, savedNeighborOverflow, savedAlpha);

        LOGGER.log(Level.INFO,
                "loaded vector index {0} (multi-segment): {1} segments, dimension {2}",
                new Object[]{index.name, numSegments, dim});
        return true;
    }

    /**
     * Loads a single FusedPQ segment from map and graph temp files.
     * The segment's BLinks and graph are populated. On success returns true.
     */
    private boolean loadFusedPQSegment(VectorSegment seg, Path mapFile, Path graphFile,
                                        int dim, int savedNextNodeId) throws IOException {
        createSegmentBLinks(seg);

        // First pass: read all entries into temp lists to compute sizes
        int entryCount;
        int maxOrdinal = -1;
        int totalPkBytes = 0;
        int[] tempOrdinals;
        int[] tempPkOffsets;
        int[] tempPkLengths;
        byte[] allPkData;

        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()), CHUNK_SIZE))) {
            entryCount = dis.readInt();
            tempOrdinals = new int[entryCount];
            tempPkLengths = new int[entryCount];
            // Collect all PK bytes into a growable buffer
            java.io.ByteArrayOutputStream pkBuf = new java.io.ByteArrayOutputStream(entryCount * 8);
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
                totalPkBytes += pkLen;

                Bytes pk = Bytes.from_array(pkBytes);
                seg.onDiskPkToNode.insert(pk, (long) ordinal);
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            allPkData = pkBuf.toByteArray();
        }

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
        seg.liveCount = entryCount;

        Files.deleteIfExists(mapFile);

        ReaderSupplier readerSupplier = new SegmentedMappedReader.Supplier(graphFile);
        seg.onDiskGraph = OnDiskGraphIndex.load(readerSupplier);
        seg.onDiskReaderSupplier = readerSupplier;
        seg.onDiskGraphFile = graphFile;

        LOGGER.log(Level.INFO,
                "loaded vector segment {0} for index {1}: {2} nodes",
                new Object[]{seg.segmentId, index.name, seg.size()});
        return true;
    }

    private void initEmptyLiveBuilder(int dim, int bw, float no, float a) {
        this.mravv = new MapRandomAccessVectorValues(vectors, dim);
        BuildScoreProvider bsp =
                BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
        this.builder = new GraphIndexBuilder(
                bsp, dim, m, bw, no, a, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
    }

    private boolean loadSimpleFormat(Path mapFile, Path graphFile,
                                     int dim, int savedNextNodeId,
                                     int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException {
        // Restore pk/vector maps from temp file
        // Use DataInputStream to avoid Integer.MAX_VALUE limit of MappedByteBuffer
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()), CHUNK_SIZE))) {
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int nodeId = dis.readInt();
                int pkLen = dis.readInt();
                byte[] pkData = new byte[pkLen];
                dis.readFully(pkData);
                int floatCount = dis.readInt();
                float[] floats = new float[floatCount];
                for (int j = 0; j < floatCount; j++) {
                    floats[j] = dis.readFloat();
                }
                Bytes pk = Bytes.from_array(pkData);
                VectorFloat<?> vec = VTS.createFloatVector(floats);
                vectors.put(nodeId, vec);
                pkToNode.put(pk, nodeId);
                nodeToPk.put(nodeId, pk);
            }
        }

        this.nextNodeId.set(savedNextNodeId);
        this.mravv = new MapRandomAccessVectorValues(vectors, dim);

        // Load OnHeapGraphIndex from temp file
        // Use SegmentedMappedReader to avoid Integer.MAX_VALUE limit of MappedByteBuffer
        try (SegmentedMappedReader reader = new SegmentedMappedReader(graphFile)) {

            BuildScoreProvider bsp =
                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            VamanaDiversityProvider diversityProvider = new VamanaDiversityProvider(bsp, savedAlpha);
            OnHeapGraphIndex loadedGraph =
                    OnHeapGraphIndex.load(reader, dim, savedNeighborOverflow, diversityProvider);
            this.builder = new GraphIndexBuilder(
                    bsp, dim, loadedGraph,
                    savedBeamWidth, savedNeighborOverflow, savedAlpha,
                    REFINE_FINAL_GRAPH,
                    PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());
        }

        LOGGER.log(Level.INFO,
                "loaded vector index {0} (simple): {1} nodes, dimension {2}",
                new Object[]{index.name, vectors.size(), dim});
        return true;
    }

    // loadFusedPQFormat removed — replaced by loadFusedPQSegment + multi-segment support

    @Override
    public void rebuild() throws DataStorageManagerException {
        final long start = System.currentTimeMillis();
        long currentTableSize = tableManager.getStats().getTablesize();
        LOGGER.log(Level.INFO, "rebuilding vector index {0} - {1} records using {2} threads",
                new Object[] {index.name, currentTableSize, REBUILD_THREADS});
        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);
        resetState();
        Table table = tableManager.getTable();
        AtomicLong count = new AtomicLong();
        // Mutable locals: reassigned when a batch is flushed mid-scan
        ThreadPoolExecutor[] executorHolder = {new ThreadPoolExecutor(
                REBUILD_THREADS, REBUILD_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(REBUILD_THREADS),
                DAEMON_THREAD_FACTORY,
                new ThreadPoolExecutor.CallerRunsPolicy())};
        AtomicReference<Throwable> error = new AtomicReference<>();
        // Use file-backed storage for vectors and BLink-backed storage for nodeToPk
        // to avoid unbounded heap usage during rebuild.
        // FileBackedVectorValues is lazily initialized when dimension is discovered from first non-null vector.
        String[] rebuildStoreNameHolder = {index.uuid + "_rebuild_nodetopk_" + System.nanoTime()};
        dataStorageManager.initIndex(tableSpaceUUID, rebuildStoreNameHolder[0]);
        long pageSize = memoryManager.getMaxLogicalPageSize();
        BLink<Bytes, Bytes>[] rebuildNodeToPkHolder = new BLink[]{new BLink<>(pageSize, BytesBytesSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesBytesStorage(rebuildStoreNameHolder[0]))};
        final AtomicReference<FileBackedVectorValues> rebuildVectorsRef = new AtomicReference<>();

        // Multi-segment rebuild state
        AtomicInteger batchNodeCount = new AtomicInteger(0);
        AtomicInteger maxNodesPerSegmentRef = new AtomicInteger(Integer.MAX_VALUE);
        List<SegmentWriteResult> completedSegments = new ArrayList<>();
        // Track all BLink store names for cleanup on error
        List<String> allRebuildStoreNames = new ArrayList<>();
        allRebuildStoreNames.add(rebuildStoreNameHolder[0]);
        // Track all FileBackedVectorValues for deferred cleanup
        // (background threads in ForkJoinPool may still reference them after flush)
        List<FileBackedVectorValues> allRebuildVectors = new ArrayList<>();

        try {
            tableManager.scanForIndexRebuild(r -> {
                if (error.get() != null) {
                    throw new DataStorageManagerException("error during rebuild", error.get());
                }
                herddb.utils.DataAccessor values = r.getDataAccessor(table);
                Bytes key = RecordSerializer.serializeIndexKey(values, table, table.primaryKey);
                Bytes indexKey = RecordSerializer.serializeIndexKey(values, index, index.columnNames);
                if (builder == null) {
                    // Single-threaded until builder is initialized (typically first non-null vector)
                    try {
                        rebuildInsert(key, indexKey, rebuildVectorsRef, rebuildNodeToPkHolder[0],
                                currentTableSize, batchNodeCount, maxNodesPerSegmentRef);
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                } else {
                    executorHolder[0].execute(() -> {
                        try {
                            rebuildInsert(key, indexKey, rebuildVectorsRef, rebuildNodeToPkHolder[0],
                                    currentTableSize, batchNodeCount, maxNodesPerSegmentRef);
                        } catch (Throwable t) {
                            LOGGER.log(Level.SEVERE, "rebuild failed", t);
                            error.compareAndSet(null, t);
                        }
                    });
                }

                // Check if current batch reached the segment threshold
                if (batchNodeCount.get() >= maxNodesPerSegmentRef.get()
                        && dimension > 0 && fusedPQ && dimension >= MIN_DIM_FOR_FUSED_PQ
                        && builder != null) {
                    try {
                        // Drain thread pool before flushing
                        executorHolder[0].shutdown();
                        executorHolder[0].awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                        if (error.get() != null) {
                            throw new DataStorageManagerException("error during rebuild", error.get());
                        }

                        int batchSize = batchNodeCount.get();
                        if (batchSize >= MIN_VECTORS_FOR_FUSED_PQ) {
                            SegmentWriteResult swr = flushRebuildBatch(
                                    rebuildVectorsRef.get(), rebuildNodeToPkHolder[0], batchSize);
                            completedSegments.add(swr);

                            // Defer close of FileBackedVectorValues (background threads may still reference it)
                            FileBackedVectorValues oldFbv = rebuildVectorsRef.getAndSet(null);
                            if (oldFbv != null) {
                                allRebuildVectors.add(oldFbv);
                            }
                            closeRebuildNodeToPk(rebuildNodeToPkHolder[0], rebuildStoreNameHolder[0]);

                            // Create new batch resources
                            rebuildStoreNameHolder[0] = index.uuid + "_rebuild_nodetopk_" + System.nanoTime();
                            dataStorageManager.initIndex(tableSpaceUUID, rebuildStoreNameHolder[0]);
                            allRebuildStoreNames.add(rebuildStoreNameHolder[0]);
                            rebuildNodeToPkHolder[0] = new BLink<>(pageSize, BytesBytesSizeEvaluator.INSTANCE,
                                    memoryManager.getIndexPageReplacementPolicy(),
                                    new BytesBytesStorage(rebuildStoreNameHolder[0]));
                            nextNodeId.set(0);
                            batchNodeCount.set(0);

                            // Re-init builder with fresh file-backed storage
                            initNewRebuildBatch(rebuildVectorsRef, currentTableSize);

                            LOGGER.log(Level.INFO,
                                    "rebuild vector index {0}: starting new batch (segment threshold: {1} vectors)",
                                    new Object[]{index.name, maxNodesPerSegmentRef.get()});
                        }

                        // New thread pool for next batch
                        executorHolder[0] = new ThreadPoolExecutor(
                                REBUILD_THREADS, REBUILD_THREADS,
                                0L, TimeUnit.MILLISECONDS,
                                new ArrayBlockingQueue<>(REBUILD_THREADS),
                                DAEMON_THREAD_FACTORY,
                                new ThreadPoolExecutor.CallerRunsPolicy());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DataStorageManagerException("rebuild interrupted during segment flush", e);
                    } catch (IOException e) {
                        throw new DataStorageManagerException("Failed to flush segment during rebuild", e);
                    }
                }

                long value = count.incrementAndGet();
                if (value % 100000 == 0) {
                    long elapsed = System.currentTimeMillis() - start;
                    long percent = (value * 100) / currentTableSize;
                    FileBackedVectorValues fbv = rebuildVectorsRef.get();
                    LOGGER.log(Level.INFO,
                            "rebuild vector index {0} in progress, indexed {1} records ({2}%), started {3} ms ago: {4} nodes, {5} segments flushed",
                            new Object[]{index.name, value, percent, elapsed,
                                    fbv != null ? fbv.size() : 0, completedSegments.size()});
                }
            });
            executorHolder[0].shutdown();
            executorHolder[0].awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            if (error.get() != null) {
                throw new DataStorageManagerException(error.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException("rebuild interrupted", e);
        } finally {
            executorHolder[0].shutdownNow();
        }
        FileBackedVectorValues rebuildVectors = rebuildVectorsRef.get();
        int remainingBatchCount = batchNodeCount.get();

        if (!completedSegments.isEmpty()) {
            // Multi-segment rebuild path: at least one segment was already flushed
            try {
                // Flush remaining batch if non-empty
                if (remainingBatchCount > 0 && builder != null) {
                    if (remainingBatchCount >= MIN_VECTORS_FOR_FUSED_PQ) {
                        SegmentWriteResult swr = flushRebuildBatch(
                                rebuildVectors, rebuildNodeToPkHolder[0], remainingBatchCount);
                        completedSegments.add(swr);
                    } else {
                        // Small final batch (< MIN_VECTORS_FOR_FUSED_PQ): keep as live state.
                        // These vectors will be merged into segments at the next checkpoint.
                        LOGGER.log(Level.INFO,
                                "rebuild vector index {0}: final batch has {1} vectors (below {2}), keeping as live state",
                                new Object[]{index.name, remainingBatchCount, MIN_VECTORS_FOR_FUSED_PQ});
                        try (Stream<Map.Entry<Bytes, Bytes>> scanStream =
                                rebuildNodeToPkHolder[0].scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY)) {
                            scanStream.forEach(e -> {
                                int nodeId = bytesToOrdinal(e.getKey());
                                Bytes pk = e.getValue();
                                this.nodeToPk.put(nodeId, pk);
                                this.pkToNode.put(pk, nodeId);
                                if (rebuildVectors != null) {
                                    this.vectors.put(nodeId, rebuildVectors.getVector(nodeId));
                                }
                            });
                        }
                    }
                }

                // Persist segments to storage before acquiring the state lock,
                // so we don't hold the lock during I/O to the remote file service.
                LogSequenceNumber lsn = log.getLastSequenceNumber();
                persistIndexStatusMultiSegment(Collections.emptyList(), completedSegments, lsn, false);

                // Pre-load all segments from persisted pages (I/O heavy, done outside lock)
                List<VectorSegment> newSegments = new java.util.concurrent.CopyOnWriteArrayList<>();
                for (SegmentWriteResult swr : completedSegments) {
                    Path reloadMapFile = readChunksToTempFile(
                            swr.mapPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_MAPCHUNK);
                    Path reloadGraphFile = readChunksToTempFile(
                            swr.graphPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_GRAPHCHUNK);
                    VectorSegment seg = new VectorSegment(swr.segmentId);
                    seg.estimatedSizeBytes = swr.estimatedSizeBytes;
                    seg.graphPageIds = swr.graphPageIds;
                    seg.mapPageIds = swr.mapPageIds;
                    loadFusedPQSegment(seg, reloadMapFile, reloadGraphFile, dimension, Integer.MAX_VALUE);
                    newSegments.add(seg);
                }

                // Swap in-memory state atomically under the lock so that a
                // concurrent checkpoint() never sees a partially-updated state.
                stateLock.writeLock().lock();
                try {
                    // Close builder from the last batch (it references FileBackedVectorValues
                    // that will be closed soon, so we must not reuse it)
                    boolean hasLiveData = !vectors.isEmpty();
                    GraphIndexBuilder oldBuilder = this.builder;
                    if (oldBuilder != null) {
                        try {
                            oldBuilder.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }

                    this.segments = newSegments;
                    this.builder = null;
                    this.mravv = null;

                    if (hasLiveData) {
                        // Small final batch kept as live state: build fresh graph from live vectors.
                        initEmptyLiveBuilder(dimension, beamWidth, neighborOverflow, alpha);
                        for (Map.Entry<Integer, VectorFloat<?>> entry : vectors.entrySet()) {
                            builder.addGraphNode(entry.getKey(), entry.getValue());
                        }
                        dirty.set(true);
                    } else {
                        // Recompute nextNodeId from all segments
                        int maxOrd = -1;
                        for (VectorSegment seg : segments) {
                            try (Stream<Map.Entry<Bytes, Bytes>> stream = seg.scanNodeToPk()) {
                                int segMax = stream.mapToInt(e -> bytesToOrdinal(e.getKey())).max().orElse(-1);
                                if (segMax > maxOrd) {
                                    maxOrd = segMax;
                                }
                            }
                        }
                        this.nextNodeId.set(maxOrd + 1);
                        initEmptyLiveBuilder(dimension, beamWidth, neighborOverflow, alpha);
                        dirty.set(false);
                    }
                } finally {
                    stateLock.writeLock().unlock();
                }

                int totalNodes = (int) onDiskNodeToPkSize() + vectors.size();
                LOGGER.log(Level.INFO,
                        "rebuilt vector index {0} in {1} ms: {2} nodes across {3} segments, {4} live (FusedPQ multi-segment)",
                        new Object[]{index.name, System.currentTimeMillis() - start, totalNodes,
                                segments.size(), vectors.size()});
            } catch (IOException e) {
                throw new DataStorageManagerException("Failed to write FusedPQ during rebuild", e);
            } finally {
                closeRebuildVectors(rebuildVectors);
                for (FileBackedVectorValues fbv : allRebuildVectors) {
                    closeRebuildVectors(fbv);
                }
                closeRebuildNodeToPk(rebuildNodeToPkHolder[0], rebuildStoreNameHolder[0]);
            }
        } else {
            // Single-batch path: no segments flushed during scan
            int rebuildCount = (int) rebuildNodeToPkHolder[0].size();
            // Build FusedPQ directly if conditions are met, avoiding duplicate work at next checkpoint
            if (fusedPQ && dimension >= MIN_DIM_FOR_FUSED_PQ
                    && rebuildCount >= MIN_VECTORS_FOR_FUSED_PQ && builder != null) {
                try {
                    builder.cleanup();
                    OnHeapGraphIndex graph = (OnHeapGraphIndex) builder.getGraph();
                    List<Long> graphPageIds = writeFusedPQGraph(graph, mravv, dimension);
                    List<Long> mapPageIds = writeFusedPQMapData(rebuildVectors, rebuildNodeToPkHolder[0]);
                    LogSequenceNumber lsn = log.getLastSequenceNumber();
                    long estimatedSize = (long) graphPageIds.size() * CHUNK_SIZE;

                    // Build single-segment result
                    int segId = nextSegmentId.getAndIncrement();
                    List<SegmentWriteResult> segResults = new ArrayList<>();
                    segResults.add(new SegmentWriteResult(segId, graphPageIds, mapPageIds, estimatedSize, Collections.emptyList()));
                    persistIndexStatusMultiSegment(Collections.emptyList(), segResults, lsn, false);

                    // Pre-load segment from persisted pages (I/O heavy, outside lock)
                    Path reloadMapFile = readChunksToTempFile(
                            mapPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_MAPCHUNK);
                    Path reloadGraphFile = readChunksToTempFile(
                            graphPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_GRAPHCHUNK);
                    VectorSegment newSeg = new VectorSegment(segId);
                    newSeg.estimatedSizeBytes = estimatedSize;
                    newSeg.graphPageIds = graphPageIds;
                    newSeg.mapPageIds = mapPageIds;
                    loadFusedPQSegment(newSeg, reloadMapFile, reloadGraphFile, dimension, nextNodeId.get());

                    // Swap state atomically under the lock
                    stateLock.writeLock().lock();
                    try {
                        GraphIndexBuilder oldBuilder = this.builder;
                        if (oldBuilder != null) {
                            try {
                                oldBuilder.close();
                            } catch (IOException e) {
                                // ignore
                            }
                        }
                        this.builder = null;
                        this.mravv = null;
                        segments.add(newSeg);
                        initEmptyLiveBuilder(dimension, beamWidth, neighborOverflow, alpha);
                        dirty.set(false);
                    } finally {
                        stateLock.writeLock().unlock();
                    }

                    LOGGER.log(Level.INFO,
                            "rebuilt vector index {0} in {1} ms: {2} nodes (FusedPQ written directly)",
                            new Object[]{index.name, System.currentTimeMillis() - start, rebuildCount});
                } catch (IOException e) {
                    throw new DataStorageManagerException("Failed to write FusedPQ during rebuild", e);
                } finally {
                    closeRebuildVectors(rebuildVectors);
                    closeRebuildNodeToPk(rebuildNodeToPkHolder[0], rebuildStoreNameHolder[0]);
                }
            } else {
                try {
                    // Small dataset or FusedPQ not applicable: populate instance fields, let checkpoint() handle
                    // Collect data from rebuild BLink before acquiring lock
                    ConcurrentHashMap<Integer, Bytes> rebuildNodeToPkMap = new ConcurrentHashMap<>();
                    ConcurrentHashMap<Integer, VectorFloat<?>> rebuildVectorsMap = new ConcurrentHashMap<>();
                    try (Stream<Map.Entry<Bytes, Bytes>> scanStream =
                            rebuildNodeToPkHolder[0].scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY)) {
                        scanStream.forEach(e -> {
                            int nodeId = bytesToOrdinal(e.getKey());
                            Bytes pk = e.getValue();
                            rebuildNodeToPkMap.put(nodeId, pk);
                            if (rebuildVectors != null) {
                                rebuildVectorsMap.put(nodeId, rebuildVectors.getVector(nodeId));
                            }
                        });
                    }

                    // Swap state atomically under the lock
                    stateLock.writeLock().lock();
                    try {
                        for (Map.Entry<Integer, Bytes> e : rebuildNodeToPkMap.entrySet()) {
                            this.nodeToPk.put(e.getKey(), e.getValue());
                        }
                        this.vectors.putAll(rebuildVectorsMap);
                        // Re-create mravv wrapping the instance vectors map so that
                        // the live search path and future inserts use the correct backing store
                        if (dimension > 0 && builder != null) {
                            this.mravv = new MapRandomAccessVectorValues(vectors, dimension);
                            BuildScoreProvider bsp =
                                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
                            OnHeapGraphIndex graph = (OnHeapGraphIndex) builder.getGraph();
                            try {
                                builder.close();
                            } catch (IOException e) {
                                // ignore
                            }
                            this.builder = new GraphIndexBuilder(
                                    bsp, dimension, graph,
                                    beamWidth, neighborOverflow, alpha,
                                    REFINE_FINAL_GRAPH,
                                    PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());
                        }
                        dirty.set(true);
                    } finally {
                        stateLock.writeLock().unlock();
                    }
                    long elapsed = System.currentTimeMillis() - start;
                    LOGGER.log(Level.INFO,
                            "rebuilt vector index {0} in {1} ms: {2} nodes",
                            new Object[]{index.name, elapsed, rebuildCount});
                } finally {
                    closeRebuildVectors(rebuildVectors);
                    closeRebuildNodeToPk(rebuildNodeToPkHolder[0], rebuildStoreNameHolder[0]);
                }
            }
        }
    }

    private void closeRebuildNodeToPk(BLink<Bytes, Bytes> rebuildNodeToPk, String storeName) {
        if (rebuildNodeToPk != null) {
            rebuildNodeToPk.close();
        }
        try {
            dataStorageManager.dropIndex(tableSpaceUUID, storeName);
        } catch (DataStorageManagerException e) {
            LOGGER.log(Level.WARNING, "Failed to clean up rebuild BLink storage " + storeName, e);
        }
    }

    private static void closeRebuildVectors(FileBackedVectorValues rebuildVectors) {
        if (rebuildVectors != null) {
            try {
                rebuildVectors.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close rebuild vectors file", e);
            }
        }
    }

    /**
     * Flushes the current rebuild batch as a FusedPQ segment.
     * Called during rebuild when the batch reaches maxNodesPerSegment.
     */
    private SegmentWriteResult flushRebuildBatch(
            FileBackedVectorValues batchVectors,
            BLink<Bytes, Bytes> batchNodeToPk,
            int batchSize) throws IOException, DataStorageManagerException {
        builder.cleanup();
        OnHeapGraphIndex graph = (OnHeapGraphIndex) builder.getGraph();
        List<Long> graphPageIds = writeFusedPQGraph(graph, mravv, dimension);
        List<Long> mapPageIds = writeFusedPQMapData(batchVectors, batchNodeToPk);
        long estimatedSize = (long) graphPageIds.size() * CHUNK_SIZE;
        int segId = nextSegmentId.getAndIncrement();

        // Close builder after writing
        try {
            builder.close();
        } catch (IOException e) {
            // ignore
        }
        this.builder = null;
        this.mravv = null;

        LOGGER.log(Level.INFO,
                "rebuild vector index {0}: segment {1} declared full ({2} vectors, ~{3} bytes)",
                new Object[]{index.name, segId, batchSize, estimatedSize});

        return new SegmentWriteResult(segId, graphPageIds, mapPageIds, estimatedSize, Collections.emptyList());
    }

    /**
     * Re-initializes builder and file-backed storage for a new rebuild batch.
     */
    private void initNewRebuildBatch(
            AtomicReference<FileBackedVectorValues> rebuildVectorsRef,
            long expectedSize) throws IOException {
        FileBackedVectorValues fbv = FileBackedVectorValues.create(
                dimension, Math.max(expectedSize, 16),
                java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")));
        rebuildVectorsRef.set(fbv);
        this.mravv = fbv;
        BuildScoreProvider bsp =
                BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
        this.builder = new GraphIndexBuilder(
                bsp, dimension, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
    }

    // -------------------------------------------------------------------------
    // Test hook: called during Phase B of checkpoint (after Phase A releases write lock).
    // Package-private for testing concurrent DML during checkpoint.
    // -------------------------------------------------------------------------
    private volatile Runnable checkpointPhaseBHook;

    /** Sets a hook that runs during Phase B of checkpoint (after Phase A releases the write lock).
     *  For testing concurrent DML during checkpoint. */
    public void setCheckpointPhaseBHook(Runnable hook) {
        this.checkpointPhaseBHook = hook;
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        try {
            return doCheckpoint(sequenceNumber, pin);
        } catch (IOException e) {
            throw new DataStorageManagerException(e);
        }
    }

    /**
     * Checkpoint dispatcher: uses single-lock for simple format (< 256 vectors, always fast),
     * and three-phase for FusedPQ format (can be slow, needs concurrent DML).
     */
    private List<PostCheckpointAction> doCheckpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws IOException, DataStorageManagerException {

        // First, determine under write lock whether we need FusedPQ or simple
        stateLock.writeLock().lock();
        try {
            boolean anySegmentDirty = segments.stream().anyMatch(s -> s.dirty);
            if (!dirty.get() && !anySegmentDirty) {
                LOGGER.log(Level.FINE, "checkpoint vector index {0}: skipped (no changes)", index.name);
                return Collections.emptyList();
            }

            boolean hasLiveNodes = builder != null && !nodeToPk.isEmpty();
            boolean hasOnDiskNodes = onDiskNodeToPkSize() > 0;

            if (!hasLiveNodes && !hasOnDiskNodes && builder == null && segments.isEmpty()) {
                IndexStatus emptyStatus = new IndexStatus(
                        index.name, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                List<PostCheckpointAction> result = new ArrayList<>();
                result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, emptyStatus, pin));
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint vector index {0}: empty", index.name);
                return result;
            }

            if (dimension == 0) {
                IndexStatus emptyStatus = new IndexStatus(
                        index.name, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                List<PostCheckpointAction> result = new ArrayList<>();
                result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, emptyStatus, pin));
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint vector index {0}: empty dimension", index.name);
                return result;
            }

            int totalActiveVectors = (int) onDiskNodeToPkSize() + nodeToPk.size();

            if (totalActiveVectors == 0 && !segments.isEmpty()) {
                for (VectorSegment seg : segments) {
                    seg.close();
                }
                segments = new java.util.concurrent.CopyOnWriteArrayList<>();
                nextSegmentId.set(0);
                IndexStatus emptyStatus = new IndexStatus(
                        index.name, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
                List<PostCheckpointAction> result = new ArrayList<>();
                result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, emptyStatus, pin));
                dirty.set(false);
                LOGGER.log(Level.INFO, "checkpoint vector index {0}: all vectors deleted, saving empty", index.name);
                return result;
            }

            boolean useFusedPQ = fusedPQ
                    && dimension >= MIN_DIM_FOR_FUSED_PQ
                    && totalActiveVectors >= MIN_VECTORS_FOR_FUSED_PQ;

            if (!useFusedPQ) {
                // Simple format: small index (< 256 vectors), checkpoint is fast.
                // Use single-lock approach — data stays in live maps.
                return doCheckpointSimpleUnderLock(sequenceNumber, pin);
            }
        } finally {
            stateLock.writeLock().unlock();
        }

        // FusedPQ path: use three-phase checkpoint
        return doCheckpointFusedPQThreePhase(sequenceNumber, pin);
    }

    /**
     * Simple format checkpoint — runs entirely under write lock.
     * Used for small indexes (< 256 vectors) where checkpoint is always fast.
     * Data remains in live maps after checkpoint for continued in-memory search.
     * Must be called with stateLock.writeLock() held.
     */
    private List<PostCheckpointAction> doCheckpointSimpleUnderLock(LogSequenceNumber sequenceNumber, boolean pin)
            throws IOException, DataStorageManagerException {
        // If we have on-disk segments, materialize their data into live state first
        if (!segments.isEmpty()) {
            ConcurrentHashMap<Integer, VectorFloat<?>> allVectors = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Bytes> allNodeToPk = new ConcurrentHashMap<>();
            int seqId = 0;
            for (Map.Entry<Integer, Bytes> e : nodeToPk.entrySet()) {
                VectorFloat<?> vec = vectors.get(e.getKey());
                if (vec != null) {
                    allVectors.put(seqId, vec);
                    allNodeToPk.put(seqId, e.getValue());
                    seqId++;
                }
            }
            for (VectorSegment seg : segments) {
                if (seg.onDiskGraph != null) {
                    try (OnDiskGraphIndex.View view = seg.onDiskGraph.getView();
                         Stream<Map.Entry<Bytes, Bytes>> scanStream = seg.scanNodeToPk()) {
                        List<Map.Entry<Bytes, Bytes>> entries = scanStream.collect(java.util.stream.Collectors.toList());
                        for (Map.Entry<Bytes, Bytes> e : entries) {
                            int ordinal = bytesToOrdinal(e.getKey());
                            VectorFloat<?> vec = view.getVector(ordinal);
                            allVectors.put(seqId, vec);
                            allNodeToPk.put(seqId, e.getValue());
                            seqId++;
                        }
                    }
                }
                seg.close();
            }
            segments = new java.util.concurrent.CopyOnWriteArrayList<>();
            nextSegmentId.set(0);

            vectors.clear();
            vectors.putAll(allVectors);
            nodeToPk.clear();
            nodeToPk.putAll(allNodeToPk);
            pkToNode.clear();
            for (Map.Entry<Integer, Bytes> e : allNodeToPk.entrySet()) {
                pkToNode.put(e.getValue(), e.getKey());
            }
            nextNodeId.set(seqId);

            GraphIndexBuilder oldBuilder = this.builder;
            if (oldBuilder != null) {
                try {
                    oldBuilder.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            this.mravv = new MapRandomAccessVectorValues(vectors, dimension);
            BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            this.builder = new GraphIndexBuilder(
                    bsp, dimension, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
            for (Map.Entry<Integer, VectorFloat<?>> e : allVectors.entrySet()) {
                builder.addGraphNode(e.getKey(), e.getValue());
            }
            builder.cleanup();
        }

        if (builder != null) {
            builder.cleanup();
        }
        vectors.keySet().retainAll(nodeToPk.keySet());

        List<Long> graphPageIds;
        Path graphTmpFile = Files.createTempFile("herddb-vector-graph-", ".tmp");
        try {
            if (builder != null) {
                try (DataOutputStream graphDos = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(graphTmpFile.toFile()), CHUNK_SIZE))) {
                    ((OnHeapGraphIndex) builder.getGraph()).save(graphDos);
                }
            }
            graphPageIds = writeChunks(graphTmpFile, TYPE_VECTOR_GRAPHCHUNK);
        } finally {
            Files.deleteIfExists(graphTmpFile);
        }
        List<Long> mapPageIds;
        Path mapTmpFile = serializeMapDataToFile(vectors, nodeToPk);
        try {
            mapPageIds = writeChunks(mapTmpFile, TYPE_VECTOR_MAPCHUNK);
        } finally {
            Files.deleteIfExists(mapTmpFile);
        }
        int totalNodes = nodeToPk.size();

        List<PostCheckpointAction> result = new ArrayList<>();
        result.addAll(persistIndexStatusSimple(graphPageIds, mapPageIds, totalNodes, false, sequenceNumber, pin));
        dirty.set(false);
        LOGGER.log(Level.INFO,
                "checkpoint vector index {0}: {1} nodes (simple), {2} graph pages, {3} map pages",
                new Object[]{index.name, totalNodes, graphPageIds.size(), mapPageIds.size()});
        return result;
    }

    /**
     * Three-phase FusedPQ checkpoint:
     * <ul>
     *   <li><b>Phase A</b> (brief write lock): snapshot live state, swap to new empty maps/builder</li>
     *   <li><b>Phase B</b> (no lock): build graphs, compute PQ, write to disk</li>
     *   <li><b>Phase C</b> (brief write lock): load new segments, apply pending deletes, swap segments</li>
     * </ul>
     */
    private List<PostCheckpointAction> doCheckpointFusedPQThreePhase(LogSequenceNumber sequenceNumber, boolean pin)
            throws IOException, DataStorageManagerException {

        // =====================================================================
        // Phase A: snapshot + swap (brief write lock)
        // =====================================================================
        ConcurrentHashMap<Integer, VectorFloat<?>> snapshotVectors;
        ConcurrentHashMap<Bytes, Integer> snapshotPkToNode;
        ConcurrentHashMap<Integer, Bytes> snapshotNodeToPk;
        GraphIndexBuilder snapshotBuilder;
        RandomAccessVectorValues snapshotMravv;
        List<VectorSegment> sealedSegments;
        List<VectorSegment> mergeableSegments;
        int snapshotDimension;

        stateLock.writeLock().lock();
        try {
            snapshotDimension = dimension;

            // Snapshot live state
            snapshotVectors = this.vectors;
            snapshotPkToNode = this.pkToNode;
            snapshotNodeToPk = this.nodeToPk;
            snapshotBuilder = this.builder;
            snapshotMravv = this.mravv;

            // Classify segments
            sealedSegments = new ArrayList<>();
            mergeableSegments = new ArrayList<>();
            for (VectorSegment seg : segments) {
                if (seg.isSealed(maxSegmentSize)) {
                    sealedSegments.add(seg);
                } else {
                    mergeableSegments.add(seg);
                }
            }

            // Set up frozen state for search during Phase B
            this.frozenBuilder = snapshotBuilder;
            this.frozenMravv = snapshotMravv;
            this.frozenNodeToPk = snapshotNodeToPk;
            this.pendingCheckpointDeletes = ConcurrentHashMap.newKeySet();
            this.checkpointPhaseComplete = new CountDownLatch(1);
            this.liveVectorCapDuringCheckpoint = Math.max(10000, snapshotNodeToPk.size() / 2);

            // Swap to new empty live state — DML proceeds immediately after write lock release
            this.vectors = new ConcurrentHashMap<>();
            this.pkToNode = new ConcurrentHashMap<>();
            this.nodeToPk = new ConcurrentHashMap<>();
            initEmptyLiveBuilder(snapshotDimension, beamWidth, neighborOverflow, alpha);
            dirty.set(false);

            LOGGER.log(Level.INFO,
                    "checkpoint vector index {0} Phase A: snapshotted {1} live + {2} on-disk vectors, "
                            + "{3} sealed + {4} mergeable segments",
                    new Object[]{index.name, snapshotNodeToPk.size(), onDiskNodeToPkSize(),
                            sealedSegments.size(), mergeableSegments.size()});
        } finally {
            stateLock.writeLock().unlock();
        }

        // =====================================================================
        // Phase B: build graphs, write to disk (NO lock)
        // =====================================================================
        List<PostCheckpointAction> result = new ArrayList<>();
        List<SegmentWriteResult> newSegmentResults;
        try {
            // Test hook for concurrent DML testing
            Runnable hook = checkpointPhaseBHook;
            if (hook != null) {
                hook.run();
            }

            newSegmentResults = doCheckpointFusedPQPhaseB(
                    snapshotVectors, snapshotNodeToPk, snapshotBuilder,
                    snapshotDimension, sealedSegments, mergeableSegments,
                    sequenceNumber, pin, result);
        } catch (IOException | RuntimeException e) {
            // Phase B failed — restore frozen state back to live
            recoverFromPhaseBFailure(snapshotVectors, snapshotPkToNode, snapshotNodeToPk,
                    snapshotBuilder, snapshotMravv);
            throw e;
        }

        // =====================================================================
        // Phase C: load segments, swap, cleanup (brief write lock)
        // =====================================================================
        stateLock.writeLock().lock();
        try {
            // Close frozen builder (frees graph memory)
            if (snapshotBuilder != null) {
                try {
                    snapshotBuilder.close();
                } catch (IOException e) {
                    // ignore
                }
            }

            if (newSegmentResults != null) {
                // Close mergeable segments (frees off-heap mmap + BLink) BEFORE loading new ones
                for (VectorSegment seg : mergeableSegments) {
                    seg.close();
                }

                // Load new segments from storage
                List<VectorSegment> newSegments = new java.util.concurrent.CopyOnWriteArrayList<>();
                for (VectorSegment sealed : sealedSegments) {
                    sealed.dirty = false;
                    newSegments.add(sealed);
                }
                for (SegmentWriteResult swr : newSegmentResults) {
                    Path reloadMapFile = readChunksToTempFile(
                            swr.mapPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_MAPCHUNK);
                    Path reloadGraphFile = readChunksToTempFile(
                            swr.graphPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_GRAPHCHUNK);
                    VectorSegment seg = new VectorSegment(swr.segmentId);
                    seg.estimatedSizeBytes = swr.estimatedSizeBytes;
                    seg.graphPageIds = swr.graphPageIds;
                    seg.mapPageIds = swr.mapPageIds;
                    loadFusedPQSegment(seg, reloadMapFile, reloadGraphFile, snapshotDimension, nextNodeId.get());
                    newSegments.add(seg);
                }

                // Apply pending deletes to newly loaded segments
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

                // Atomic swap
                this.segments = newSegments;

                // Recompute nextNodeId: max of segment ordinals vs current nextNodeId
                int maxOrd = -1;
                for (VectorSegment seg : newSegments) {
                    try (Stream<Map.Entry<Bytes, Bytes>> stream = seg.scanNodeToPk()) {
                        int segMax = stream.mapToInt(e2 -> bytesToOrdinal(e2.getKey())).max().orElse(-1);
                        if (segMax > maxOrd) {
                            maxOrd = segMax;
                        }
                    }
                }
                this.nextNodeId.set(Math.max(maxOrd + 1, nextNodeId.get()));

                int totalNodes = (int) onDiskNodeToPkSize() + nodeToPk.size();
                LOGGER.log(Level.INFO,
                        "checkpoint vector index {0} Phase C: {1} nodes across {2} segments (FusedPQ), "
                                + "{3} new live inserts during checkpoint",
                        new Object[]{index.name, totalNodes, newSegments.size(), nodeToPk.size()});
            } else {
                // FusedPQ fell back to simple path in Phase B
                // Segments were already cleared in Phase B
                int totalNodes = nodeToPk.size() + (int) onDiskNodeToPkSize();
                LOGGER.log(Level.INFO,
                        "checkpoint vector index {0} Phase C: {1} nodes (simple fallback)",
                        new Object[]{index.name, totalNodes});
            }

            // Clear frozen state
            this.frozenBuilder = null;
            this.frozenMravv = null;
            this.frozenNodeToPk = null;
            this.pendingCheckpointDeletes = null;
            this.liveVectorCapDuringCheckpoint = Integer.MAX_VALUE;

            dirty.set(nodeToPk.size() > 0); // dirty if new inserts arrived during Phase B
        } finally {
            // Signal waiting DML threads before releasing write lock
            CountDownLatch latch = this.checkpointPhaseComplete;
            this.checkpointPhaseComplete = null;
            stateLock.writeLock().unlock();
            if (latch != null) {
                latch.countDown();
            }
        }

        return result;
    }

    /**
     * Recovers from a Phase B failure by merging frozen state back into live state.
     */
    private void recoverFromPhaseBFailure(
            ConcurrentHashMap<Integer, VectorFloat<?>> snapshotVectors,
            ConcurrentHashMap<Bytes, Integer> snapshotPkToNode,
            ConcurrentHashMap<Integer, Bytes> snapshotNodeToPk,
            GraphIndexBuilder snapshotBuilder,
            RandomAccessVectorValues snapshotMravv) {
        stateLock.writeLock().lock();
        try {
            LOGGER.log(Level.WARNING,
                    "checkpoint vector index {0}: Phase B failed, restoring frozen state", index.name);

            // Merge any new DML that happened during Phase B back into the snapshot maps
            ConcurrentHashMap<Integer, VectorFloat<?>> currentVectors = this.vectors;
            ConcurrentHashMap<Bytes, Integer> currentPkToNode = this.pkToNode;
            ConcurrentHashMap<Integer, Bytes> currentNodeToPk = this.nodeToPk;

            snapshotVectors.putAll(currentVectors);
            snapshotPkToNode.putAll(currentPkToNode);
            snapshotNodeToPk.putAll(currentNodeToPk);

            // Close the new empty builder
            GraphIndexBuilder newBuilder = this.builder;
            if (newBuilder != null) {
                try {
                    newBuilder.close();
                } catch (IOException e) {
                    // ignore
                }
            }

            // Restore the snapshot as the live state
            this.vectors = snapshotVectors;
            this.pkToNode = snapshotPkToNode;
            this.nodeToPk = snapshotNodeToPk;
            this.builder = snapshotBuilder;
            this.mravv = snapshotMravv;

            // Re-add new DML nodes to the restored builder
            if (snapshotBuilder != null) {
                for (Map.Entry<Integer, VectorFloat<?>> e : currentVectors.entrySet()) {
                    if (!snapshotNodeToPk.containsKey(e.getKey())
                            || snapshotNodeToPk.get(e.getKey()) != currentNodeToPk.get(e.getKey())) {
                        // This was a new insert during Phase B — skip re-adding to builder
                        // as we already merged maps, we just need the graph node
                        try {
                            snapshotBuilder.addGraphNode(e.getKey(), e.getValue());
                        } catch (Exception ex) {
                            LOGGER.log(Level.WARNING, "Failed to re-add node during recovery", ex);
                        }
                    }
                }
            }

            // Clear frozen state
            this.frozenBuilder = null;
            this.frozenMravv = null;
            this.frozenNodeToPk = null;
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

    /**
     * Phase B for FusedPQ checkpoint. Runs without any lock.
     * Returns the list of SegmentWriteResults to be loaded in Phase C.
     */
    private List<SegmentWriteResult> doCheckpointFusedPQPhaseB(
            ConcurrentHashMap<Integer, VectorFloat<?>> snapshotVectors,
            ConcurrentHashMap<Integer, Bytes> snapshotNodeToPk,
            GraphIndexBuilder snapshotBuilder,
            int snapshotDimension,
            List<VectorSegment> sealedSegments,
            List<VectorSegment> mergeableSegments,
            LogSequenceNumber sequenceNumber, boolean pin,
            List<PostCheckpointAction> resultActions)
            throws IOException, DataStorageManagerException {

        if (snapshotBuilder != null) {
            snapshotBuilder.cleanup();
        }

        // Collect all vectors from snapshot live state + mergeable segments into sequential lists
        List<VectorFloat<?>> poolVectorsList = new ArrayList<>();
        List<Bytes> poolPkList = new ArrayList<>();

        for (Map.Entry<Integer, Bytes> e : snapshotNodeToPk.entrySet()) {
            VectorFloat<?> vec = snapshotVectors.get(e.getKey());
            if (vec != null) {
                poolVectorsList.add(vec);
                poolPkList.add(e.getValue());
            }
        }

        for (VectorSegment seg : mergeableSegments) {
            if (seg.onDiskGraph != null) {
                try (OnDiskGraphIndex.View view = seg.onDiskGraph.getView();
                     Stream<Map.Entry<Bytes, Bytes>> scanStream = seg.scanNodeToPk()) {
                    scanStream.forEach(e -> {
                        int ordinal = bytesToOrdinal(e.getKey());
                        Bytes pk = e.getValue();
                        VectorFloat<?> vec = view.getVector(ordinal);
                        poolVectorsList.add(vec);
                        poolPkList.add(pk);
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
        if (poolVectorsList.size() < MIN_VECTORS_FOR_FUSED_PQ && poolVectorsList.size() > 0) {
            // Close all segments
            for (VectorSegment seg : mergeableSegments) {
                seg.close();
            }
            for (VectorSegment seg : sealedSegments) {
                seg.close();
            }
            stateLock.writeLock().lock();
            try {
                segments = new java.util.concurrent.CopyOnWriteArrayList<>();
                nextSegmentId.set(0);
            } finally {
                stateLock.writeLock().unlock();
            }

            // Build maps from pool for simple checkpoint
            ConcurrentHashMap<Integer, VectorFloat<?>> poolVecs = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Bytes> poolNodeToPk = new ConcurrentHashMap<>();
            for (int i = 0; i < poolVectorsList.size(); i++) {
                poolVecs.put(i, poolVectorsList.get(i));
                poolNodeToPk.put(i, poolPkList.get(i));
            }
            MapRandomAccessVectorValues poolMravv = new MapRandomAccessVectorValues(poolVecs, snapshotDimension);
            BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(poolMravv, similarityFunction);
            GraphIndexBuilder poolBuilder = new GraphIndexBuilder(
                    bsp, snapshotDimension, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
            for (Map.Entry<Integer, VectorFloat<?>> e : poolVecs.entrySet()) {
                poolBuilder.addGraphNode(e.getKey(), e.getValue());
            }
            poolBuilder.cleanup();

            // Write simple checkpoint
            List<Long> graphPageIds;
            Path graphTmpFile = Files.createTempFile("herddb-vector-graph-", ".tmp");
            try {
                try (DataOutputStream graphDos = new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(graphTmpFile.toFile()), CHUNK_SIZE))) {
                    ((OnHeapGraphIndex) poolBuilder.getGraph()).save(graphDos);
                }
                graphPageIds = writeChunks(graphTmpFile, TYPE_VECTOR_GRAPHCHUNK);
            } finally {
                Files.deleteIfExists(graphTmpFile);
            }
            List<Long> mapPageIds;
            Path mapTmpFile = serializeMapDataToFile(poolVecs, poolNodeToPk);
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
            resultActions.addAll(persistIndexStatusSimple(graphPageIds, mapPageIds,
                    poolNodeToPk.size(), false, sequenceNumber, pin));
            LOGGER.log(Level.INFO,
                    "checkpoint vector index {0}: {1} nodes (simple fallback from FusedPQ)",
                    new Object[]{index.name, poolNodeToPk.size()});
            return null; // Signal to Phase C: simple path was used, no segments to load
        }

        // Estimate avgBytesPerVector from existing segments
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

        List<SegmentWriteResult> newSegmentResults = new ArrayList<>();
        int start = 0;
        while (start < poolVectorsList.size()) {
            int end = Math.min(start + maxNodesPerSegment, poolVectorsList.size());

            if (end < poolVectorsList.size()
                    && (poolVectorsList.size() - end) < MIN_VECTORS_FOR_FUSED_PQ) {
                end = poolVectorsList.size();
            }

            ConcurrentHashMap<Integer, VectorFloat<?>> partVectors = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Bytes> partNodeToPk = new ConcurrentHashMap<>();
            for (int i = start; i < end; i++) {
                int seqId = i - start;
                partVectors.put(seqId, poolVectorsList.get(i));
                partNodeToPk.put(seqId, poolPkList.get(i));
            }

            int segId = nextSegmentId.getAndIncrement();
            List<Long> graphPageIds = writeFusedPQGraph(partVectors, partNodeToPk, snapshotDimension);
            List<Long> mapPageIds = writeFusedPQMapData(
                    new MapRandomAccessVectorValues(partVectors, snapshotDimension), partNodeToPk);
            long estimatedSize = (long) graphPageIds.size() * CHUNK_SIZE;
            newSegmentResults.add(new SegmentWriteResult(segId, graphPageIds, mapPageIds, estimatedSize,
                    Collections.emptyList()));

            start = end;
        }

        resultActions.addAll(persistIndexStatusMultiSegment(sealedSegments, newSegmentResults, sequenceNumber, pin));

        return newSegmentResults;
    }

    /** Holds the result of writing a single segment during checkpoint. */
    private static class SegmentWriteResult {
        final int segmentId;
        final List<Long> graphPageIds;
        final List<Long> mapPageIds;
        final long estimatedSizeBytes;
        final List<Integer> nodeIds;

        SegmentWriteResult(int segmentId, List<Long> graphPageIds, List<Long> mapPageIds,
                           long estimatedSizeBytes, List<Integer> nodeIds) {
            this.segmentId = segmentId;
            this.graphPageIds = graphPageIds;
            this.mapPageIds = mapPageIds;
            this.estimatedSizeBytes = estimatedSizeBytes;
            this.nodeIds = nodeIds;
        }
    }

    /**
     * Persists index status for simple (non-FusedPQ) format.
     */
    private List<PostCheckpointAction> persistIndexStatusSimple(
            List<Long> graphPageIds, List<Long> mapPageIds,
            int totalNodes, boolean useFusedPQ,
            LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
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
                index.name, sequenceNumber,
                newPageId.get(), activePages, metaBuf.array());

        return dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, indexStatus, pin);
    }

    /**
     * Persists index status for multi-segment FusedPQ format (version 3).
     */
    private List<PostCheckpointAction> persistIndexStatusMultiSegment(
            List<VectorSegment> sealedSegments, List<SegmentWriteResult> newSegmentResults,
            LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {

        int totalSegments = sealedSegments.size() + newSegmentResults.size();

        // Header: version(4) + dim(4) + m(4) + bw(4) + no(4) + alpha(4) + hier(1) + fusedPQ(1) + nextNodeId(4) + numSegments(4) = 34
        int metaSize = 34;
        // Per segment: segId(4) + estimatedSize(8) + numGraphChunks(4) + graphPageIds(8*N) + numMapChunks(4) + mapPageIds(8*N)
        for (VectorSegment seg : sealedSegments) {
            metaSize += 4 + 8 + 4 + seg.graphPageIds.size() * 8 + 4 + seg.mapPageIds.size() * 8;
        }
        for (SegmentWriteResult swr : newSegmentResults) {
            metaSize += 4 + 8 + 4 + swr.graphPageIds.size() * 8 + 4 + swr.mapPageIds.size() * 8;
        }

        ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
        metaBuf.putInt(METADATA_VERSION_MULTI_SEGMENT);
        metaBuf.putInt(dimension);
        metaBuf.putInt(m);
        metaBuf.putInt(beamWidth);
        metaBuf.putFloat(neighborOverflow);
        metaBuf.putFloat(alpha);
        metaBuf.put((byte) (ADD_HIERARCHY ? 1 : 0));
        metaBuf.put((byte) 1); // fusedPQ
        metaBuf.putInt(nextNodeId.get());
        metaBuf.putInt(totalSegments);

        Set<Long> activePages = new HashSet<>();

        // Write sealed segments
        for (VectorSegment seg : sealedSegments) {
            metaBuf.putInt(seg.segmentId);
            metaBuf.putLong(seg.estimatedSizeBytes);
            metaBuf.putInt(seg.graphPageIds.size());
            for (long id : seg.graphPageIds) {
                metaBuf.putLong(id);
                activePages.add(id);
            }
            metaBuf.putInt(seg.mapPageIds.size());
            for (long id : seg.mapPageIds) {
                metaBuf.putLong(id);
                activePages.add(id);
            }
        }

        // Write new segments
        for (SegmentWriteResult swr : newSegmentResults) {
            metaBuf.putInt(swr.segmentId);
            metaBuf.putLong(swr.estimatedSizeBytes);
            metaBuf.putInt(swr.graphPageIds.size());
            for (long id : swr.graphPageIds) {
                metaBuf.putLong(id);
                activePages.add(id);
            }
            metaBuf.putInt(swr.mapPageIds.size());
            for (long id : swr.mapPageIds) {
                metaBuf.putLong(id);
                activePages.add(id);
            }
        }

        IndexStatus indexStatus = new IndexStatus(
                index.name, sequenceNumber,
                newPageId.get(), activePages, metaBuf.array());

        return dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, indexStatus, pin);
    }

    /**
     * Writes the merged graph as FusedPQ on-disk format.
     * Returns list of page IDs for the graph chunks.
     */
    private List<Long> writeFusedPQGraph(ConcurrentHashMap<Integer, VectorFloat<?>> allVectors,
                                          ConcurrentHashMap<Integer, Bytes> allNodeToPk,
                                          int dim) throws IOException, DataStorageManagerException {
        if (allNodeToPk.isEmpty()) {
            return writeChunks(new byte[0], TYPE_VECTOR_GRAPHCHUNK);
        }

        // Build a fresh OnHeapGraphIndex from all vectors
        MapRandomAccessVectorValues allMravv = new MapRandomAccessVectorValues(allVectors, dim);
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(allMravv, similarityFunction);
        GraphIndexBuilder mergedBuilder = new GraphIndexBuilder(
                bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
        allVectors.entrySet().parallelStream()
                .filter(e -> allNodeToPk.containsKey(e.getKey()))
                .forEach(e -> mergedBuilder.addGraphNode(e.getKey(), e.getValue()));
        mergedBuilder.cleanup();
        OnHeapGraphIndex mergedGraph = (OnHeapGraphIndex) mergedBuilder.getGraph();

        // Compute PQ: subspaces = dim/4, exactly 256 clusters (FusedPQ requirement)
        int pqSubspaces = Math.max(1, dim / 4);
        ProductQuantization pq = ProductQuantization.compute(allMravv, pqSubspaces, 256, true);
        PQVectors pqv = pq.encodeAll(allMravv, PhysicalCoreExecutor.pool());

        // Write to temp file
        Path tempFile = Files.createTempFile("herddb-vector-", ".idx");
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
            return writeChunks(tempFile, TYPE_VECTOR_GRAPHCHUNK);
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
     * Writes an already-built graph as FusedPQ on-disk format, reusing the existing graph
     * instead of rebuilding it from scratch. Used by rebuild() to avoid building the graph twice.
     */
    private List<Long> writeFusedPQGraph(OnHeapGraphIndex existingGraph,
                                          RandomAccessVectorValues existingMravv,
                                          int dim) throws IOException, DataStorageManagerException {
        // Compute PQ: subspaces = dim/4, exactly 256 clusters (FusedPQ requirement)
        int pqSubspaces = Math.max(1, dim / 4);
        ProductQuantization pq = ProductQuantization.compute(existingMravv, pqSubspaces, 256, true);
        PQVectors pqv = pq.encodeAll(existingMravv, PhysicalCoreExecutor.pool());

        // Write to temp file
        Path tempFile = Files.createTempFile("herddb-vector-", ".idx");
        try {
            try (OnDiskGraphIndexWriter writer = new OnDiskGraphIndexWriter.Builder(existingGraph, tempFile)
                    .with(new FusedPQ(existingGraph.maxDegree(), pq))
                    .with(new InlineVectors(dim))
                    .build()) {
                ImmutableGraphIndex.View view = existingGraph.getView();
                EnumMap<FeatureId, IntFunction<io.github.jbellis.jvector.graph.disk.feature.Feature.State>> suppliers =
                        new EnumMap<>(FeatureId.class);
                suppliers.put(FeatureId.FUSED_PQ,
                        ordinal -> new FusedPQ.State(view, pqv, ordinal));
                suppliers.put(FeatureId.INLINE_VECTORS,
                        ordinal -> new InlineVectors.State(existingMravv.getVector(ordinal)));
                writer.write(suppliers);
            }
            return writeChunks(tempFile, TYPE_VECTOR_GRAPHCHUNK);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /**
     * Writes map data for FusedPQ format: (newOrdinal, pk, vector) entries.
     * The ordinals written are the ORIGINAL node IDs (which will become the sequential
     * ordinals after OnDiskGraphIndexWriter renumbers them). We store the renumbered
     * ordinals so load can reconstruct the on-disk ordinal → pk mapping.
     */
    private List<Long> writeFusedPQMapData(RandomAccessVectorValues allVectors,
                                            ConcurrentHashMap<Integer, Bytes> allNodeToPk)
            throws IOException, DataStorageManagerException {
        // Compute sequential renumbering so we know what ordinals will be on disk
        // OnDiskGraphIndexWriter assigns sequential ordinals in node-ID order
        List<Integer> sortedNodeIds = new ArrayList<>(allNodeToPk.keySet());
        java.util.Collections.sort(sortedNodeIds);
        Map<Integer, Integer> oldToNew = new java.util.HashMap<>();
        for (int i = 0; i < sortedNodeIds.size(); i++) {
            oldToNew.put(sortedNodeIds.get(i), i);
        }

        Path mapTmpFile = Files.createTempFile("herddb-vector-map-", ".tmp");
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

    private List<Long> writeFusedPQMapData(RandomAccessVectorValues allVectors,
                                            BLink<Bytes, Bytes> allNodeToPk)
            throws IOException, DataStorageManagerException {
        // Scan BLink to get sorted ordinals (BLink keys are ordinalToBytes, which sorts correctly)
        List<Integer> sortedNodeIds = new ArrayList<>();
        try (Stream<Map.Entry<Bytes, Bytes>> scanStream =
                allNodeToPk.scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY)) {
            scanStream.forEach(e -> sortedNodeIds.add(bytesToOrdinal(e.getKey())));
        }

        // Compute sequential renumbering
        Map<Integer, Integer> oldToNew = new java.util.HashMap<>();
        for (int i = 0; i < sortedNodeIds.size(); i++) {
            oldToNew.put(sortedNodeIds.get(i), i);
        }

        Path mapTmpFile = Files.createTempFile("herddb-vector-map-", ".tmp");
        try {
            try (BufferedOutputStream bos = new BufferedOutputStream(
                    new FileOutputStream(mapTmpFile.toFile()), CHUNK_SIZE)) {
                int entryCount = sortedNodeIds.size();
                writeInt(bos, entryCount);

                for (int oldId : sortedNodeIds) {
                    int newOrdinal = oldToNew.get(oldId);
                    Bytes pk = allNodeToPk.search(ordinalToBytes(oldId));
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

    private static void skipFully(DataInputStream dis, long n) throws IOException {
        while (n > 0) {
            int skipped = dis.skipBytes((int) Math.min(n, Integer.MAX_VALUE));
            if (skipped <= 0) {
                // skipBytes may return 0; fall back to reading a byte
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
     * Reads index chunk pages into a temporary file and returns the path.
     */
    private Path readChunksToTempFile(long[] pageIds, int expectedChunkType)
            throws IOException, DataStorageManagerException {
        Path tempFile = Files.createTempFile("herddb-vector-", ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tempFile.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, CHUNK_SIZE)) {
            for (long pageId : pageIds) {
                byte[] chunkData = dataStorageManager.readIndexPage(
                        tableSpaceUUID, index.uuid, pageId,
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
     * Streams a file into CHUNK_SIZE pieces, writes each as an index page,
     * and returns the list of assigned page IDs.
     * Writes at least one page even if the file is empty (zero-length chunk).
     */
    private List<Long> writeChunks(Path file, int chunkType) throws DataStorageManagerException, IOException {
        List<Long> pageIds = new ArrayList<>();
        long fileSize = Files.size(file);
        if (fileSize == 0) {
            long pageId = newPageId.getAndIncrement();
            dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
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
                dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
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
     * Splits {@code data} into CHUNK_SIZE pieces, writes each as an index page,
     * and returns the list of assigned page IDs.
     * Writes at least one page even if data is empty (zero-length chunk).
     */
    private List<Long> writeChunks(byte[] data, int chunkType) throws DataStorageManagerException {
        List<Long> pageIds = new ArrayList<>();
        if (data.length == 0) {
            long pageId = newPageId.getAndIncrement();
            dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
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
            dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
                out.writeVInt(chunkType);
                out.writeVInt(len);
                out.write(data, off, len);
            });
            pageIds.add(pageId);
        }
        return pageIds;
    }

    /**
     * Serialises the pk/vector map into a temp file:
     * [ entryCount:int ]
     * for each entry:
     *   [ nodeId:int | pkLen:int | pkBytes | floatCount:int | floats ]
     * Returns the path to the temp file (caller must delete after use).
     */
    private Path serializeMapDataToFile(ConcurrentHashMap<Integer, VectorFloat<?>> vecs,
                                        ConcurrentHashMap<Integer, Bytes> nodeToKey) throws IOException {
        Path tmpFile = Files.createTempFile("herddb-vector-map-", ".tmp");
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(tmpFile.toFile()), CHUNK_SIZE)) {
            List<Map.Entry<Integer, Bytes>> entries = new ArrayList<>(nodeToKey.entrySet());
            int entryCount = entries.size();
            writeInt(bos, entryCount);

            for (Map.Entry<Integer, Bytes> e : entries) {
                int nodeId = e.getKey();
                byte[] pkBytes = e.getValue().to_array();
                VectorFloat<?> vec = vecs.get(nodeId);
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

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpaceUUID, index.uuid, sequenceNumber);
    }

    @Override
    public void close() {
        GraphIndexBuilder b = this.builder;
        if (b != null) {
            try {
                b.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "error closing vector index builder for " + index.name, e);
            }
        }
        // Close frozen builder if checkpoint was in progress
        GraphIndexBuilder fb = this.frozenBuilder;
        if (fb != null) {
            try {
                fb.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING,
                        "error closing frozen vector index builder for " + index.name, e);
            }
            this.frozenBuilder = null;
            this.frozenMravv = null;
            this.frozenNodeToPk = null;
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
    }

    // -------------------------------------------------------------------------
    // DML operations
    // -------------------------------------------------------------------------

    @Override
    public void recordInserted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        if (indexKey == null) {
            return;
        }
        float[] floats = indexKey.to_float_array();
        if (floats.length == 0) {
            return;
        }

        // Back-pressure: if checkpoint Phase B is active and live cap exceeded,
        // wait for Phase C to complete before proceeding.
        CountDownLatch latch = checkpointPhaseComplete;
        if (latch != null && nodeToPk.size() >= liveVectorCapDuringCheckpoint) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DataStorageManagerException("interrupted waiting for checkpoint", e);
            }
        }

        stateLock.readLock().lock();
        try {
            if (dimension == 0) {
                initBuilderForDimension(floats.length);
            }
            if (floats.length != dimension) {
                LOGGER.log(Level.WARNING,
                        "vector dimension mismatch on insert: expected {0} but got {1}, skipping",
                        new Object[]{dimension, floats.length});
                return;
            }
            VectorFloat<?> vec = VTS.createFloatVector(floats);
            int nodeId = nextNodeId.getAndIncrement();
            vectors.put(nodeId, vec);
            pkToNode.put(key, nodeId);
            nodeToPk.put(nodeId, key);
            builder.addGraphNode(nodeId, vec);
            dirty.set(true);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        if (indexKey == null) {
            return;
        }
        stateLock.readLock().lock();
        try {
            // Check on-disk segments first
            for (VectorSegment seg : segments) {
                if (seg.deletePk(key)) {
                    dirty.set(true);
                    break;
                }
            }
            // Track delete for Phase B awareness: if a checkpoint is in progress,
            // the PK may exist in frozen state or in mergeable segments being rewritten.
            Set<Bytes> pending = pendingCheckpointDeletes;
            if (pending != null) {
                pending.add(key);
            }
            // Check live nodes
            Integer nodeId = pkToNode.remove(key);
            if (nodeId == null) {
                return;
            }
            nodeToPk.remove(nodeId);
            dirty.set(true);
            GraphIndexBuilder b = builder;
            if (b != null) {
                b.markNodeDeleted(nodeId);
            } else {
                vectors.remove(nodeId);
            }
        } finally {
            stateLock.readLock().unlock();
        }
    }

    @Override
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded)
            throws DataStorageManagerException {
        recordDeleted(key, indexKeyRemoved);
        recordInserted(key, indexKeyAdded);
    }

    /**
     * Optimized insert used during rebuild only.
     * Unlike {@link #recordInserted}, this skips {@code pkToNode} (not needed during rebuild)
     * and {@code dirty} flag, and uses caller-provided maps instead of instance fields.
     *
     * @param batchNodeCount if non-null, incremented after each successful insert
     * @param maxNodesPerSegmentRef if non-null, computed lazily after dimension is discovered
     */
    private void rebuildInsert(Bytes key, Bytes indexKey,
                               AtomicReference<FileBackedVectorValues> rebuildVectorsRef,
                               BLink<Bytes, Bytes> rebuildNodeToPk,
                               long expectedSize,
                               AtomicInteger batchNodeCount,
                               AtomicInteger maxNodesPerSegmentRef) {
        if (indexKey == null) {
            return;
        }
        float[] floats = indexKey.to_float_array();
        if (floats.length == 0) {
            return;
        }
        if (dimension == 0) {
            try {
                FileBackedVectorValues fbv = FileBackedVectorValues.create(
                        floats.length, Math.max(expectedSize, 16),
                        java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")));
                rebuildVectorsRef.set(fbv);
                initBuilderForDimension(floats.length, fbv);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("Failed to create file-backed vector storage", e);
            }
            // Compute maxNodesPerSegment now that dimension is known
            if (maxNodesPerSegmentRef != null) {
                long avgBytesPerVector = (long) dimension * Float.BYTES * 2;
                int computed = (int) Math.max(MIN_VECTORS_FOR_FUSED_PQ,
                        maxSegmentSize / Math.max(1, avgBytesPerVector));
                maxNodesPerSegmentRef.compareAndSet(Integer.MAX_VALUE, computed);
            }
        }
        FileBackedVectorValues rebuildVectors = rebuildVectorsRef.get();
        if (rebuildVectors == null) {
            return;
        }
        if (floats.length != dimension) {
            LOGGER.log(Level.WARNING,
                    "vector dimension mismatch on insert: expected {0} but got {1}, skipping",
                    new Object[]{dimension, floats.length});
            return;
        }
        VectorFloat<?> vec = VTS.createFloatVector(floats);
        int nodeId = nextNodeId.getAndIncrement();
        rebuildVectors.putVector(nodeId, vec);
        rebuildNodeToPk.insert(ordinalToBytes(nodeId), key);
        builder.addGraphNode(nodeId, vec);
        if (batchNodeCount != null) {
            batchNodeCount.incrementAndGet();
        }
    }

    @Override
    public void truncate() throws DataStorageManagerException {
        resetState();
        truncateIndexData();
        dirty.set(true);
    }

    @Override
    public boolean valueAlreadyMapped(Bytes key, Bytes primaryKey) throws DataStorageManagerException {
        return false; // vector index does not enforce uniqueness
    }

    // -------------------------------------------------------------------------
    // ANN search – used by VectorANNScanOp planner operation
    // -------------------------------------------------------------------------

    /**
     * Performs an approximate nearest-neighbor search against the vector index.
     *
     * @param queryVector the query embedding (must have the same dimension as indexed vectors)
     * @param topK        maximum number of results to return
     * @return list of (primaryKey, score) pairs ordered best-first;
     *         may be shorter than topK if the index has fewer live nodes
     */
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK)
            throws StatementExecutionException {
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>();
        VectorFloat<?> qv = VTS.createFloatVector(queryVector);

        // Search all on-disk segments
        // Take a snapshot of the segments list for safe concurrent access
        List<VectorSegment> currentSegments = this.segments;
        try {
            for (VectorSegment seg : currentSegments) {
                seg.search(qv, topK, similarityFunction, results);
            }
        } catch (Exception | OutOfMemoryError e) {
            throw new StatementExecutionException("vector index search failed: " + e.getMessage(), e);
        }

        // Search live in-memory builder
        GraphIndexBuilder b = builder;
        if (b != null && !nodeToPk.isEmpty()) {
            int k = Math.min(topK, nodeToPk.size());
            ImmutableGraphIndex graph = b.getGraph();
            SearchResult result = GraphSearcher.search(
                    qv, k, mravv, similarityFunction, graph, Bits.ALL);
            for (SearchResult.NodeScore ns : result.getNodes()) {
                Bytes pk = nodeToPk.get(ns.node);
                if (pk != null) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                }
            }
        }

        // Search frozen builder (during Phase B of checkpoint)
        GraphIndexBuilder fb = frozenBuilder;
        ConcurrentHashMap<Integer, Bytes> fNodeToPk = frozenNodeToPk;
        if (fb != null && fNodeToPk != null && !fNodeToPk.isEmpty()) {
            RandomAccessVectorValues fMravv = frozenMravv;
            if (fMravv != null) {
                int k = Math.min(topK, fNodeToPk.size());
                try {
                    ImmutableGraphIndex graph = fb.getGraph();
                    SearchResult result = GraphSearcher.search(
                            qv, k, fMravv, similarityFunction, graph, Bits.ALL);
                    Set<Bytes> pending = pendingCheckpointDeletes;
                    for (SearchResult.NodeScore ns : result.getNodes()) {
                        Bytes pk = fNodeToPk.get(ns.node);
                        if (pk != null && (pending == null || !pending.contains(pk))) {
                            results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                        }
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING,
                            "error searching frozen builder for " + index.name, e);
                }
            }
        }

        // Merge and sort by score descending, take top-K
        results.sort((a, b2) -> Float.compare(b2.getValue(), a.getValue()));
        return results.size() <= topK ? results : results.subList(0, topK);
    }

    // -------------------------------------------------------------------------
    // Not yet implemented – scanner() deferred; use search() via VectorANNScanOp
    // -------------------------------------------------------------------------

    @Override
    protected Stream<Bytes> scanner(IndexOperation operation,
                                    StatementEvaluationContext context,
                                    TableContext tableContext) throws StatementExecutionException {
        throw new UnsupportedOperationException(
                "Vector index scan not yet supported by the SQL planner");
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private synchronized void initBuilderForDimension(int dim) {
        initBuilderForDimension(dim, vectors);
    }

    private synchronized void initBuilderForDimension(int dim, Map<Integer, VectorFloat<?>> vectorsMap) {
        if (this.dimension == 0) {
            this.mravv = new MapRandomAccessVectorValues(vectorsMap, dim);
            BuildScoreProvider bsp =
                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            this.builder = new GraphIndexBuilder(
                    bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
            // Set dimension LAST: concurrent readers check dimension != 0 to skip init,
            // so builder must be visible before dimension becomes non-zero.
            this.dimension = dim;
        }
    }

    private synchronized void initBuilderForDimension(int dim, RandomAccessVectorValues ravv) {
        if (this.dimension == 0) {
            this.mravv = ravv;
            BuildScoreProvider bsp =
                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            this.builder = new GraphIndexBuilder(
                    bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
            // Set dimension LAST (see above)
            this.dimension = dim;
        }
    }

    private void resetState() {
        GraphIndexBuilder b = this.builder;
        if (b != null) {
            try {
                b.close();
            } catch (IOException e) {
                // ignore on reset
            }
        }
        // Clean up frozen state if checkpoint was in progress
        GraphIndexBuilder fb = this.frozenBuilder;
        if (fb != null) {
            try {
                fb.close();
            } catch (IOException e) {
                // ignore on reset
            }
        }
        frozenBuilder = null;
        frozenMravv = null;
        frozenNodeToPk = null;
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
        vectors.clear();
        pkToNode.clear();
        nodeToPk.clear();
        nextNodeId.set(0);
        nextSegmentId.set(0);
        dimension = 0;
        builder = null;
        mravv = null;
    }

    // -------------------------------------------------------------------------
    // BLink helpers for on-disk ordinal ↔ PK maps
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

    private void createSegmentBLinks(VectorSegment seg) {
        long pageSize = memoryManager.getMaxLogicalPageSize();
        String pkToNodeName = index.uuid + "_seg" + seg.segmentId + "_pktonode";
        try {
            dataStorageManager.initIndex(tableSpaceUUID, pkToNodeName);
        } catch (DataStorageManagerException e) {
            throw new RuntimeException("Failed to init BLink storage for vector index " + index.name
                    + " segment " + seg.segmentId, e);
        }
        seg.onDiskPkToNode = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesLongStorage(pkToNodeName));
    }

    private long onDiskNodeToPkSize() {
        long total = 0;
        for (VectorSegment seg : segments) {
            total += seg.size();
        }
        return total;
    }

    private static List<Long> toLongList(long[] arr) {
        List<Long> list = new ArrayList<>(arr.length);
        for (long v : arr) {
            list.add(v);
        }
        return list;
    }

    // -------------------------------------------------------------------------
    // BLink data storage implementations (disk-backed via DataStorageManager)
    // -------------------------------------------------------------------------

    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;
    private static final byte BLINK_INNER_NODE_PAGE = 1;
    private static final byte BLINK_LEAF_NODE_PAGE = 2;

    /**
     * BLink storage for {@code BLink<Bytes, Long>} (pkToNode map).
     * Follows the same pattern as {@code BLinkKeyToPageIndex.BLinkIndexDataStorageImpl}.
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

    /**
     * BLink storage for {@code BLink<Bytes, Bytes>} (nodeToPk map).
     */
    private final class BytesBytesStorage implements BLinkIndexDataStorage<Bytes, Bytes> {
        private final String storeName;

        BytesBytesStorage(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void loadNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadNodePageImpl(pageId, data);
        }

        private void loadNodePageImpl(long pageId, Map<Bytes, Long> data) throws IOException {
            dataStorageManager.readIndexPage(tableSpaceUUID, storeName, pageId, in -> {
                long version = in.readVLong();
                long flags = in.readVLong();
                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted BLink page " + pageId);
                }
                byte rtype = in.readByte();
                if (rtype != BLINK_INNER_NODE_PAGE) {
                    throw new IOException("Wrong page type " + rtype + " expected " + BLINK_INNER_NODE_PAGE);
                }
                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {
                    switch (block) {
                        case NODE_PAGE_KEY_VALUE_BLOCK:
                            data.put(in.readBytes(), in.readVLong());
                            break;
                        case NODE_PAGE_INF_BLOCK:
                            data.put(Bytes.POSITIVE_INFINITY, in.readVLong());
                            break;
                        default:
                            throw new IOException("Wrong block type " + block);
                    }
                }
                return data;
            });
        }

        @Override
        public void loadLeafPage(long pageId, Map<Bytes, Bytes> data) throws IOException {
            dataStorageManager.readIndexPage(tableSpaceUUID, storeName, pageId, in -> {
                long version = in.readVLong();
                long flags = in.readVLong();
                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted BLink page " + pageId);
                }
                byte rtype = in.readByte();
                if (rtype != BLINK_LEAF_NODE_PAGE) {
                    throw new IOException("Wrong page type " + rtype + " expected " + BLINK_LEAF_NODE_PAGE);
                }
                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {
                    switch (block) {
                        case NODE_PAGE_KEY_VALUE_BLOCK:
                            data.put(in.readBytes(), in.readBytes());
                            break;
                        case NODE_PAGE_INF_BLOCK:
                            data.put(Bytes.POSITIVE_INFINITY, in.readBytes());
                            break;
                        default:
                            throw new IOException("Wrong block type " + block);
                    }
                }
                return data;
            });
        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            return writeNodePage(NEW_PAGE, data);
        }

        @Override
        public long createLeafPage(Map<Bytes, Bytes> data) throws IOException {
            return writeLeafPage(NEW_PAGE, data);
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            writeNodePage(pageId, data);
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Bytes> data) throws IOException {
            writeLeafPage(pageId, data);
        }

        private long writeNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            if (pageId == NEW_PAGE) {
                pageId = newPageId.getAndIncrement();
            }
            dataStorageManager.writeIndexPage(tableSpaceUUID, storeName, pageId, out -> {
                out.writeVLong(1);
                out.writeVLong(0);
                out.writeByte(BLINK_INNER_NODE_PAGE);
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

        private long writeLeafPage(long pageId, Map<Bytes, Bytes> data) throws IOException {
            if (pageId == NEW_PAGE) {
                pageId = newPageId.getAndIncrement();
            }
            dataStorageManager.writeIndexPage(tableSpaceUUID, storeName, pageId, out -> {
                out.writeVLong(1);
                out.writeVLong(0);
                out.writeByte(BLINK_LEAF_NODE_PAGE);
                data.forEach((x, y) -> {
                    try {
                        if (x == Bytes.POSITIVE_INFINITY) {
                            out.writeByte(NODE_PAGE_INF_BLOCK);
                            out.writeArray(y.to_array());
                        } else {
                            out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                            out.writeArray(x.to_array());
                            out.writeArray(y.to_array());
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
    // Accessors for tests
    // -------------------------------------------------------------------------

    public int getNodeCount() {
        ConcurrentHashMap<Integer, Bytes> frozen = frozenNodeToPk;
        int frozenCount = frozen != null ? frozen.size() : 0;
        return nodeToPk.size() + frozenCount + (int) onDiskNodeToPkSize();
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isFusedPQEnabled() {
        return fusedPQ;
    }

    public int getVectorsMapSize() {
        ConcurrentHashMap<Integer, VectorFloat<?>> frozenVecs = null;
        ConcurrentHashMap<Integer, Bytes> frozen = frozenNodeToPk;
        if (frozen != null) {
            // During Phase B, vectors field has been swapped. Approximate frozen vectors size.
            frozenVecs = null; // We don't keep a frozen vectors ref, but frozenNodeToPk size is equivalent
        }
        return vectors.size() + (frozen != null ? frozen.size() : 0);
    }

    public int getLiveNodeCount() {
        ConcurrentHashMap<Integer, Bytes> frozen = frozenNodeToPk;
        int frozenCount = frozen != null ? frozen.size() : 0;
        return nodeToPk.size() + frozenCount;
    }

    public int getOnDiskNodeCount() {
        return (int) onDiskNodeToPkSize();
    }

    public int getPkToNodeSize() {
        return pkToNode.size();
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

    public boolean isDirty() {
        return dirty.get();
    }

    public boolean isCheckpointActive() {
        return frozenBuilder != null;
    }
}
