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
import io.github.jbellis.jvector.disk.ByteBufferReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.MapRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.diversity.VamanaDiversityProvider;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.util.AbstractMap;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

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

    /* instance hyper-parameters (read from index properties) */
    private final int m;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final boolean fusedPQ;
    private final VectorSimilarityFunction similarityFunction;

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

    /** nodeId → vector (for scoring during incremental inserts after load) */
    private final ConcurrentHashMap<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();

    /** primary-key bytes → jvector node ID (live inserts only) */
    private final ConcurrentHashMap<Bytes, Integer> pkToNode = new ConcurrentHashMap<>();

    /** jvector node ID → primary-key bytes (live inserts only) */
    private final ConcurrentHashMap<Integer, Bytes> nodeToPk = new ConcurrentHashMap<>();

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
    // On-disk state – loaded from FusedPQ checkpoint
    // -------------------------------------------------------------------------

    /** Loaded on-disk graph (FusedPQ format). Null if not using FusedPQ or not yet checkpointed. */
    private volatile OnDiskGraphIndex onDiskGraph = null;

    /** Temp file holding the on-disk graph data (kept alive as the ReaderSupplier source). */
    private volatile Path onDiskGraphFile = null;

    /** ReaderSupplier backed by onDiskGraphFile; must be closed before deleting the file. */
    private volatile ReaderSupplier onDiskReaderSupplier = null;

    /** on-disk sequential ordinal (as 4-byte big-endian Bytes) → primary-key bytes. BLink-backed for paging. */
    private volatile BLink<Bytes, Bytes> onDiskNodeToPk = null;

    /** primary-key bytes → on-disk sequential ordinal (as Long). BLink-backed for paging. */
    private volatile BLink<Bytes, Long> onDiskPkToNode = null;

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
                               int readLockTimeout) {
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
    }

    private static int intProp(Map<String, String> props, String key, int defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Integer.parseInt(v);
    }

    private static float floatProp(Map<String, String> props, String key, float defaultVal) {
        String v = props.get(key);
        return v == null ? defaultVal : Float.parseFloat(v);
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

    private boolean loadFromStatus(IndexStatus status) throws IOException, DataStorageManagerException {
        ByteBuffer metaBuf = ByteBuffer.wrap(status.indexData);

        int version = metaBuf.getInt();
        if (version != METADATA_VERSION_SIMPLE && version != METADATA_VERSION_FUSEDPQ) {
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

        // Empty index case (no inserts ever happened)
        if (dim == 0 || numGraphChunks == 0) {
            LOGGER.log(Level.INFO, "vector index {0} is empty, no rebuild needed", index.name);
            return true;
        }

        // --- Load map and graph chunks into temp files ---
        Path mapFile = readChunksToTempFile(mapChunkPageIds, TYPE_VECTOR_MAPCHUNK);
        Path graphFile = readChunksToTempFile(graphChunkPageIds, TYPE_VECTOR_GRAPHCHUNK);

        this.dimension = dim;
        newPageId.set(status.newPageId);

        if (savedFusedPQ) {
            return loadFusedPQFormat(mapFile, graphFile,
                    dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
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

    private boolean loadSimpleFormat(Path mapFile, Path graphFile,
                                     int dim, int savedNextNodeId,
                                     int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException {
        // Restore pk/vector maps from temp file
        try (FileChannel mapCh = FileChannel.open(mapFile, StandardOpenOption.READ)) {
            ByteBuffer mapBuf = mapCh.map(FileChannel.MapMode.READ_ONLY, 0, mapCh.size());
            int entryCount = mapBuf.getInt();
            for (int i = 0; i < entryCount; i++) {
                int nodeId = mapBuf.getInt();
                int pkLen = mapBuf.getInt();
                byte[] pkData = new byte[pkLen];
                mapBuf.get(pkData);
                int floatCount = mapBuf.getInt();
                float[] floats = new float[floatCount];
                for (int j = 0; j < floatCount; j++) {
                    floats[j] = mapBuf.getFloat();
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
        try (FileChannel graphCh = FileChannel.open(graphFile, StandardOpenOption.READ)) {
            ByteBuffer graphBuf = graphCh.map(FileChannel.MapMode.READ_ONLY, 0, graphCh.size());
            ByteBufferReader reader = new ByteBufferReader(graphBuf);

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

    private boolean loadFusedPQFormat(Path mapFile, Path graphFile,
                                      int dim, int savedNextNodeId,
                                      int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException {
        // Create fresh BLink instances for on-disk maps
        createOnDiskBLinks();

        // Map data stores (newOrdinal, pk, vector) — newOrdinals are sequential 0..N-1
        try (FileChannel mapCh = FileChannel.open(mapFile, StandardOpenOption.READ)) {
            ByteBuffer mapBuf = mapCh.map(FileChannel.MapMode.READ_ONLY, 0, mapCh.size());
            int entryCount = mapBuf.getInt();
            int maxOrdinal = -1;
            for (int i = 0; i < entryCount; i++) {
                int ordinal = mapBuf.getInt();
                int pkLen = mapBuf.getInt();
                byte[] pkData = new byte[pkLen];
                mapBuf.get(pkData);
                int floatCount = mapBuf.getInt();
                // skip vector floats (not needed for on-disk graph; vectors are stored inline)
                mapBuf.position(mapBuf.position() + floatCount * Float.BYTES);
                Bytes pk = Bytes.from_array(pkData);
                onDiskNodeToPk.insert(ordinalToBytes(ordinal), pk);
                onDiskPkToNode.insert(pk, (long) ordinal);
                if (ordinal > maxOrdinal) {
                    maxOrdinal = ordinal;
                }
            }
            // Live inserts start after the loaded on-disk ordinals
            this.nextNodeId.set(maxOrdinal + 1);
        }
        // Map file fully consumed
        Files.deleteIfExists(mapFile);

        // Load OnDiskGraphIndex from temp file (file stays alive for graph's lifetime)
        ReaderSupplier readerSupplier = new io.github.jbellis.jvector.disk.SimpleMappedReader.Supplier(graphFile);
        this.onDiskGraph = OnDiskGraphIndex.load(readerSupplier);
        this.onDiskReaderSupplier = readerSupplier;
        this.onDiskGraphFile = graphFile;

        // Create an empty live builder for new inserts
        this.mravv = new MapRandomAccessVectorValues(vectors, dim);
        BuildScoreProvider bsp =
                BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
        this.builder = new GraphIndexBuilder(
                bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);

        LOGGER.log(Level.INFO,
                "loaded vector index {0} (FusedPQ): {1} on-disk nodes, dimension {2}",
                new Object[]{index.name, onDiskNodeToPkSize(), dim});
        return true;
    }

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
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                REBUILD_THREADS, REBUILD_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(REBUILD_THREADS),
                DAEMON_THREAD_FACTORY,
                new ThreadPoolExecutor.CallerRunsPolicy());
        AtomicReference<Throwable> error = new AtomicReference<>();
        // Use file-backed storage for vectors and BLink-backed storage for nodeToPk
        // to avoid unbounded heap usage during rebuild.
        // FileBackedVectorValues is lazily initialized when dimension is discovered from first non-null vector.
        String rebuildStoreName = index.uuid + "_rebuild_nodetopk_" + System.nanoTime();
        dataStorageManager.initIndex(tableSpaceUUID, rebuildStoreName);
        long pageSize = memoryManager.getMaxLogicalPageSize();
        BLink<Bytes, Bytes> rebuildNodeToPk = new BLink<>(pageSize, BytesBytesSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesBytesStorage(rebuildStoreName));
        final AtomicReference<FileBackedVectorValues> rebuildVectorsRef = new AtomicReference<>();
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
                        rebuildInsert(key, indexKey, rebuildVectorsRef, rebuildNodeToPk, currentTableSize);
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    }
                } else {
                    executor.execute(() -> {
                        try {
                            rebuildInsert(key, indexKey, rebuildVectorsRef, rebuildNodeToPk, currentTableSize);
                        } catch (Throwable t) {
                            LOGGER.log(Level.SEVERE, "rebuild failed", t);
                            error.compareAndSet(null, t);
                        }
                    });
                }
                long value = count.incrementAndGet();
                if (value % 100000 == 0) {
                    long elapsed = System.currentTimeMillis() - start;
                    long percent = (value * 100) / currentTableSize;
                    FileBackedVectorValues fbv = rebuildVectorsRef.get();
                    LOGGER.log(Level.INFO,
                            "rebuild vector index {0} in progress, indexed {1} records ({2}%), started {3} ms ago: {4} nodes",
                            new Object[]{index.name, value, percent, elapsed, fbv != null ? fbv.size() : 0});
                }
            });
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            if (error.get() != null) {
                throw new DataStorageManagerException(error.get());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataStorageManagerException("rebuild interrupted", e);
        } finally {
            executor.shutdownNow();
        }
        FileBackedVectorValues rebuildVectors = rebuildVectorsRef.get();
        int rebuildCount = (int) rebuildNodeToPk.size();
        // Build FusedPQ directly if conditions are met, avoiding duplicate work at next checkpoint
        if (fusedPQ && dimension >= MIN_DIM_FOR_FUSED_PQ
                && rebuildCount >= MIN_VECTORS_FOR_FUSED_PQ && builder != null) {
            try {
                builder.cleanup();
                OnHeapGraphIndex graph = (OnHeapGraphIndex) builder.getGraph();
                List<Long> graphPageIds = writeFusedPQGraph(graph, mravv, dimension);
                List<Long> mapPageIds = writeFusedPQMapData(rebuildVectors, rebuildNodeToPk);
                LogSequenceNumber lsn = log.getLastSequenceNumber();
                persistIndexStatus(graphPageIds, mapPageIds, rebuildCount, true, lsn, false);

                // Close old builder before loading FusedPQ format
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

                // Reload from persisted pages into on-disk state
                Path reloadMapFile = readChunksToTempFile(
                        mapPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_MAPCHUNK);
                Path reloadGraphFile = readChunksToTempFile(
                        graphPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_GRAPHCHUNK);
                loadFusedPQFormat(reloadMapFile, reloadGraphFile,
                        dimension, nextNodeId.get(), beamWidth, neighborOverflow, alpha);

                dirty.set(false);

                LOGGER.log(Level.INFO,
                        "rebuilt vector index {0} in {1} ms: {2} nodes (FusedPQ written directly)",
                        new Object[]{index.name, System.currentTimeMillis() - start, rebuildCount});
            } catch (IOException e) {
                throw new DataStorageManagerException("Failed to write FusedPQ during rebuild", e);
            } finally {
                closeRebuildVectors(rebuildVectors);
                closeRebuildNodeToPk(rebuildNodeToPk, rebuildStoreName);
            }
        } else {
            try {
                // Small dataset or FusedPQ not applicable: populate instance fields, let checkpoint() handle
                try (Stream<Map.Entry<Bytes, Bytes>> scanStream =
                        rebuildNodeToPk.scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY)) {
                    scanStream.forEach(e -> {
                        int nodeId = bytesToOrdinal(e.getKey());
                        Bytes pk = e.getValue();
                        this.nodeToPk.put(nodeId, pk);
                        if (rebuildVectors != null) {
                            this.vectors.put(nodeId, rebuildVectors.getVector(nodeId));
                        }
                    });
                }
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
                long elapsed = System.currentTimeMillis() - start;
                LOGGER.log(Level.INFO,
                        "rebuilt vector index {0} in {1} ms: {2} nodes",
                        new Object[]{index.name, elapsed, rebuildCount});
            } finally {
                closeRebuildVectors(rebuildVectors);
                closeRebuildNodeToPk(rebuildNodeToPk, rebuildStoreName);
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

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        try {
            return doCheckpoint(sequenceNumber, pin);
        } catch (IOException e) {
            throw new DataStorageManagerException(e);
        }
    }

    private List<PostCheckpointAction> doCheckpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws IOException, DataStorageManagerException {

        if (!dirty.get()) {
            LOGGER.log(Level.FINE, "checkpoint vector index {0}: skipped (no changes)", index.name);
            return Collections.emptyList();
        }

        boolean hasLiveNodes = builder != null && !nodeToPk.isEmpty();
        boolean hasOnDiskNodes = onDiskNodeToPkSize() > 0;

        if (!hasLiveNodes && !hasOnDiskNodes && builder == null && onDiskGraph == null) {
            // Nothing indexed yet – persist empty metadata
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

        // Count total active vectors (on-disk + live)
        int totalActiveVectors = (int) onDiskNodeToPkSize() + nodeToPk.size();

        // Decide whether to use FusedPQ for this checkpoint
        // Requirements: fusedPQ enabled, dimension >= 8, and enough vectors for 256 PQ clusters
        boolean useFusedPQ = fusedPQ
                && dimension >= MIN_DIM_FOR_FUSED_PQ
                && totalActiveVectors >= MIN_VECTORS_FOR_FUSED_PQ;

        List<Long> graphPageIds;
        List<Long> mapPageIds;
        int totalNodes;

        if (useFusedPQ) {
            // --- Build a merged graph: on-disk data + live inserts ---
            // For simplicity, we materialize all active vectors into a single in-memory graph,
            // then write it as a FusedPQ on-disk format.
            ConcurrentHashMap<Integer, VectorFloat<?>> allVectors = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Bytes> allNodeToPk = new ConcurrentHashMap<>();

            // Include live inserts (from current builder)
            if (builder != null) {
                builder.cleanup();
            }
            // Only include vectors for nodes that are still live (not deleted)
            for (Map.Entry<Integer, Bytes> e : nodeToPk.entrySet()) {
                int nid = e.getKey();
                VectorFloat<?> vec = vectors.get(nid);
                if (vec != null) {
                    allVectors.put(nid, vec);
                    allNodeToPk.put(nid, e.getValue());
                }
            }

            // Include on-disk nodes that haven't been deleted
            // We need their vectors; re-read from on-disk graph inline vectors
            if (onDiskGraph != null && onDiskNodeToPk != null) {
                try (OnDiskGraphIndex.View view = onDiskGraph.getView();
                     Stream<Map.Entry<Bytes, Bytes>> scanStream =
                             onDiskNodeToPk.scan(Bytes.EMPTY_ARRAY, Bytes.POSITIVE_INFINITY)) {
                    scanStream.forEach(e -> {
                        int ordinal = bytesToOrdinal(e.getKey());
                        Bytes pk = e.getValue();
                        VectorFloat<?> vec = view.getVector(ordinal);
                        allVectors.put(ordinal, vec);
                        allNodeToPk.put(ordinal, pk);
                    });
                }
            }

            graphPageIds = writeFusedPQGraph(allVectors, allNodeToPk, dimension);
            mapPageIds = writeFusedPQMapData(new MapRandomAccessVectorValues(allVectors, dimension), allNodeToPk);
            totalNodes = allNodeToPk.size();
        } else {
            // Simple format: only serialize live builder
            if (builder != null) {
                builder.cleanup();
            }
            // Purge orphan vectors (deleted nodes whose vectors were kept for the builder)
            vectors.keySet().retainAll(nodeToPk.keySet());

            // For simple format: write graph to temp file then chunk it
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
            Path mapTmpFile = serializeMapDataToFile(vectors, nodeToPk);
            try {
                mapPageIds = writeChunks(mapTmpFile, TYPE_VECTOR_MAPCHUNK);
            } finally {
                Files.deleteIfExists(mapTmpFile);
            }
            totalNodes = nodeToPk.size();
        }

        List<PostCheckpointAction> result = new ArrayList<>();
        result.addAll(persistIndexStatus(graphPageIds, mapPageIds, totalNodes,
                useFusedPQ, sequenceNumber, pin));

        // After a successful FusedPQ checkpoint, reload from the bytes just written
        // so that the on-disk graph is ready and live state is cleared.
        if (useFusedPQ) {
            GraphIndexBuilder oldBuilder = this.builder;
            if (oldBuilder != null) {
                try {
                    oldBuilder.close();
                } catch (IOException e) {
                    // ignore
                }
            }
            OnDiskGraphIndex oldOdg = this.onDiskGraph;
            if (oldOdg != null) {
                try {
                    oldOdg.close();
                } catch (Exception e) {
                    // ignore
                }
            }
            vectors.clear();
            pkToNode.clear();
            nodeToPk.clear();
            closeOnDiskBLinks();
            this.builder = null;
            this.onDiskGraph = null;
            closeAndDeleteOnDiskGraphFile();

            // Reconstruct from persisted pages via temp files
            Path reloadMapFile = readChunksToTempFile(
                    mapPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_MAPCHUNK);
            Path reloadGraphFile = readChunksToTempFile(
                    graphPageIds.stream().mapToLong(Long::longValue).toArray(), TYPE_VECTOR_GRAPHCHUNK);
            loadFusedPQFormat(reloadMapFile, reloadGraphFile,
                    dimension, nextNodeId.get(), beamWidth, neighborOverflow, alpha);
        }

        dirty.set(false);
        LOGGER.log(Level.INFO,
                "checkpoint vector index {0}: {1} nodes, {2} graph pages, {3} map pages, fusedPQ={4}",
                new Object[]{index.name, totalNodes, graphPageIds.size(), mapPageIds.size(), useFusedPQ});
        return result;
    }

    /**
     * Builds index metadata and persists it via the data storage manager.
     */
    private List<PostCheckpointAction> persistIndexStatus(
            List<Long> graphPageIds, List<Long> mapPageIds,
            int totalNodes, boolean useFusedPQ,
            LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        int metaSize = useFusedPQ
                ? 30 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8
                : 29 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8;

        ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
        if (useFusedPQ) {
            metaBuf.putInt(METADATA_VERSION_FUSEDPQ);
        } else {
            metaBuf.putInt(METADATA_VERSION_SIMPLE);
        }
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
                                        "page " + pageId + ": expected type " +
                                                expectedChunkType + " but got " + type);
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
                final byte[] chunk = Arrays.copyOf(buf, bytesRead);
                long pageId = newPageId.getAndIncrement();
                dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
                    out.writeVInt(chunkType);
                    out.writeVInt(chunk.length);
                    out.write(chunk);
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
            final int len = Math.min(CHUNK_SIZE, data.length - offset);
            final byte[] chunk = Arrays.copyOfRange(data, offset, offset + len);
            long pageId = newPageId.getAndIncrement();
            dataStorageManager.writeIndexPage(tableSpaceUUID, index.uuid, pageId, out -> {
                out.writeVInt(chunkType);
                out.writeVInt(chunk.length);
                out.write(chunk);
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
        OnDiskGraphIndex odg = this.onDiskGraph;
        if (odg != null) {
            try {
                odg.close();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING,
                        "error closing on-disk graph index for " + index.name, e);
            }
        }
        closeAndDeleteOnDiskGraphFile();
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
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        if (indexKey == null) {
            return;
        }
        // Check on-disk nodes first
        BLink<Bytes, Long> p2n = this.onDiskPkToNode;
        Long onDiskOrdinal = p2n != null ? p2n.delete(key) : null;
        if (onDiskOrdinal != null) {
            BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
            if (n2p != null) {
                n2p.delete(ordinalToBytes(onDiskOrdinal.intValue()));
            }
            dirty.set(true);
            // No need to mark deleted in OnDiskGraphIndex — filtered at search time
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
            // Vector kept in map until builder.cleanup() purges the deleted node,
            // because the builder may still visit this node during neighbor search.
        } else {
            vectors.remove(nodeId);
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
     */
    private void rebuildInsert(Bytes key, Bytes indexKey,
                               AtomicReference<FileBackedVectorValues> rebuildVectorsRef,
                               BLink<Bytes, Bytes> rebuildNodeToPk,
                               long expectedSize) {
        if (indexKey == null) {
            return;
        }
        float[] floats = indexKey.to_float_array();
        if (floats.length == 0) {
            return;
        }
        if (dimension == 0) {
            try {
                FileBackedVectorValues fbv = new FileBackedVectorValues(
                        floats.length, Math.max(expectedSize, 16),
                        java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")));
                rebuildVectorsRef.set(fbv);
                initBuilderForDimension(floats.length, fbv);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException("Failed to create file-backed vector storage", e);
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
    public List<Map.Entry<Bytes, Float>> search(float[] queryVector, int topK) {
        List<Map.Entry<Bytes, Float>> results = new ArrayList<>();
        VectorFloat<?> qv = VTS.createFloatVector(queryVector);

        // --- Search on-disk graph (FusedPQ) ---
        OnDiskGraphIndex odg = this.onDiskGraph;
        if (odg != null && onDiskNodeToPkSize() > 0) {
            searchOnDiskGraph(odg, qv, topK, results);
        }

        // --- Search live in-memory builder ---
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

        // Merge and sort by score descending, take top-K
        results.sort((a, b2) -> Float.compare(b2.getValue(), a.getValue()));
        return results.size() <= topK ? results : results.subList(0, topK);
    }

    private void searchOnDiskGraph(OnDiskGraphIndex odg, VectorFloat<?> qv, int topK,
                                    List<Map.Entry<Bytes, Float>> results) {
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p == null) {
            return;
        }
        int activeCount = (int) n2p.size();
        int k = Math.min(topK, activeCount);
        if (k == 0) {
            return;
        }
        // Accept ordinals that exist in the BLink (not deleted)
        Bits acceptBits = ordinal -> n2p.search(ordinalToBytes(ordinal)) != null;
        try {
            GraphSearcher searcher = new GraphSearcher(odg);
            OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ApproximateScoreFunction approxSF =
                    view.approximateScoreFunctionFor(qv, similarityFunction);
            io.github.jbellis.jvector.graph.similarity.ScoreFunction.ExactScoreFunction reranker =
                    view.rerankerFor(qv, similarityFunction);
            DefaultSearchScoreProvider ssp = new DefaultSearchScoreProvider(approxSF, reranker);
            SearchResult sr = searcher.search(ssp, k, acceptBits);
            for (SearchResult.NodeScore ns : sr.getNodes()) {
                Bytes pk = n2p.search(ordinalToBytes(ns.node));
                if (pk != null) {
                    results.add(new AbstractMap.SimpleImmutableEntry<>(pk, ns.score));
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "error searching on-disk graph for " + index.name, e);
        }
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
            this.dimension = dim;
            this.mravv = new MapRandomAccessVectorValues(vectorsMap, dim);
            BuildScoreProvider bsp =
                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            this.builder = new GraphIndexBuilder(
                    bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
        }
    }

    private synchronized void initBuilderForDimension(int dim, RandomAccessVectorValues ravv) {
        if (this.dimension == 0) {
            this.dimension = dim;
            this.mravv = ravv;
            BuildScoreProvider bsp =
                    BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
            this.builder = new GraphIndexBuilder(
                    bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);
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
        OnDiskGraphIndex odg = this.onDiskGraph;
        if (odg != null) {
            try {
                odg.close();
            } catch (Exception e) {
                // ignore on reset
            }
        }
        vectors.clear();
        pkToNode.clear();
        nodeToPk.clear();
        closeOnDiskBLinks();
        nextNodeId.set(0);
        dimension = 0;
        builder = null;
        mravv = null;
        onDiskGraph = null;
        closeAndDeleteOnDiskGraphFile();
    }

    private void closeAndDeleteOnDiskGraphFile() {
        ReaderSupplier rs = this.onDiskReaderSupplier;
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                // ignore
            }
            this.onDiskReaderSupplier = null;
        }
        Path f = this.onDiskGraphFile;
        if (f != null) {
            try {
                Files.deleteIfExists(f);
            } catch (IOException e) {
                // ignore
            }
            this.onDiskGraphFile = null;
        }
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

    private void createOnDiskBLinks() {
        long pageSize = memoryManager.getMaxLogicalPageSize();
        String nodeToPkName = index.uuid + "_nodetopk";
        String pkToNodeName = index.uuid + "_pktonode";
        try {
            dataStorageManager.initIndex(tableSpaceUUID, nodeToPkName);
            dataStorageManager.initIndex(tableSpaceUUID, pkToNodeName);
        } catch (DataStorageManagerException e) {
            throw new RuntimeException("Failed to init BLink storage for vector index " + index.name, e);
        }
        this.onDiskNodeToPk = new BLink<>(pageSize, BytesBytesSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesBytesStorage(nodeToPkName));
        this.onDiskPkToNode = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                memoryManager.getIndexPageReplacementPolicy(),
                new BytesLongStorage(pkToNodeName));
    }

    private void closeOnDiskBLinks() {
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        if (n2p != null) {
            n2p.close();
            this.onDiskNodeToPk = null;
        }
        BLink<Bytes, Long> p2n = this.onDiskPkToNode;
        if (p2n != null) {
            p2n.close();
            this.onDiskPkToNode = null;
        }
    }

    private long onDiskNodeToPkSize() {
        BLink<Bytes, Bytes> n2p = this.onDiskNodeToPk;
        return n2p != null ? n2p.size() : 0;
    }

    private long onDiskPkToNodeSize() {
        BLink<Bytes, Long> p2n = this.onDiskPkToNode;
        return p2n != null ? p2n.size() : 0;
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
        return nodeToPk.size() + (int) onDiskNodeToPkSize();
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isFusedPQEnabled() {
        return fusedPQ;
    }

    public int getVectorsMapSize() {
        return vectors.size();
    }

    public int getLiveNodeCount() {
        return nodeToPk.size();
    }

    public int getOnDiskNodeCount() {
        return (int) onDiskNodeToPkSize();
    }

    public int getPkToNodeSize() {
        return pkToNode.size();
    }
}
