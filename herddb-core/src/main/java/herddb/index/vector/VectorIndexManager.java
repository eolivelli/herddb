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
import herddb.core.PostCheckpointAction;
import herddb.core.TableSpaceManager;
import herddb.index.IndexOperation;
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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    /** Maximum bytes per persisted page chunk (1 MB). */
    static final int CHUNK_SIZE = 1_048_576;

    /** Page-type tag written at the start of each graph-data page. */
    static final int TYPE_VECTOR_GRAPHCHUNK = 12;
    /** Page-type tag written at the start of each pk/vector-map page. */
    static final int TYPE_VECTOR_MAPCHUNK = 13;

    /** Metadata version for the legacy OnHeapGraphIndex format. */
    private static final int METADATA_VERSION_LEGACY = 1;
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

    private volatile int dimension = 0;
    private volatile MapRandomAccessVectorValues mravv = null;
    private volatile GraphIndexBuilder builder = null;

    // -------------------------------------------------------------------------
    // On-disk state – loaded from FusedPQ checkpoint
    // -------------------------------------------------------------------------

    /** Loaded on-disk graph (FusedPQ format). Null if not using FusedPQ or not yet checkpointed. */
    private volatile OnDiskGraphIndex onDiskGraph = null;

    /** Raw bytes of the on-disk graph (kept in memory as the ReaderSupplier source). */
    private volatile byte[] onDiskGraphBytes = null;

    /** on-disk sequential ordinal → primary-key bytes */
    private final ConcurrentHashMap<Integer, Bytes> onDiskNodeToPk = new ConcurrentHashMap<>();

    /** primary-key bytes → on-disk sequential ordinal */
    private final ConcurrentHashMap<Bytes, Integer> onDiskPkToNode = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public VectorIndexManager(Index index,
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
        if (version != METADATA_VERSION_LEGACY && version != METADATA_VERSION_FUSEDPQ) {
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

        // --- Load map chunks → restore vectors + pk mappings ---
        ByteArrayOutputStream mapBaos = new ByteArrayOutputStream();
        for (long pageId : mapChunkPageIds) {
            byte[] chunkData = dataStorageManager.readIndexPage(
                    tableSpaceUUID, index.uuid, pageId,
                    in -> {
                        int type = in.readVInt();
                        if (type != TYPE_VECTOR_MAPCHUNK) {
                            throw new IOException(
                                    "page " + pageId + ": expected TYPE_VECTOR_MAPCHUNK(" +
                                            TYPE_VECTOR_MAPCHUNK + ") but got " + type);
                        }
                        int len = in.readVInt();
                        byte[] data = new byte[len];
                        in.readArray(len, data);
                        return data;
                    });
            mapBaos.write(chunkData);
        }

        // --- Load graph chunks ---
        ByteArrayOutputStream graphBaos = new ByteArrayOutputStream();
        for (long pageId : graphChunkPageIds) {
            byte[] chunkData = dataStorageManager.readIndexPage(
                    tableSpaceUUID, index.uuid, pageId,
                    in -> {
                        int type = in.readVInt();
                        if (type != TYPE_VECTOR_GRAPHCHUNK) {
                            throw new IOException(
                                    "page " + pageId + ": expected TYPE_VECTOR_GRAPHCHUNK(" +
                                            TYPE_VECTOR_GRAPHCHUNK + ") but got " + type);
                        }
                        int len = in.readVInt();
                        byte[] data = new byte[len];
                        in.readArray(len, data);
                        return data;
                    });
            graphBaos.write(chunkData);
        }

        this.dimension = dim;
        newPageId.set(status.newPageId);

        if (savedFusedPQ) {
            return loadFusedPQFormat(mapBaos.toByteArray(), graphBaos.toByteArray(),
                    dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
        } else {
            return loadLegacyFormat(mapBaos.toByteArray(), graphBaos.toByteArray(),
                    dim, savedNextNodeId, savedBeamWidth, savedNeighborOverflow, savedAlpha);
        }
    }

    private boolean loadLegacyFormat(byte[] mapBytes, byte[] graphBytes,
                                     int dim, int savedNextNodeId,
                                     int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException {
        // Restore pk/vector maps (original nodeIds)
        ByteBuffer mapBuf = ByteBuffer.wrap(mapBytes);
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

        this.nextNodeId.set(savedNextNodeId);
        this.mravv = new MapRandomAccessVectorValues(vectors, dim);

        // Load OnHeapGraphIndex
        ByteBuffer graphBuf = ByteBuffer.wrap(graphBytes);
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

        LOGGER.log(Level.INFO,
                "loaded vector index {0} (legacy): {1} nodes, dimension {2}",
                new Object[]{index.name, vectors.size(), dim});
        return true;
    }

    private boolean loadFusedPQFormat(byte[] mapBytes, byte[] graphBytes,
                                      int dim, int savedNextNodeId,
                                      int savedBeamWidth, float savedNeighborOverflow, float savedAlpha)
            throws IOException {
        // Map data stores (newOrdinal, pk, vector) — newOrdinals are sequential 0..N-1
        ByteBuffer mapBuf = ByteBuffer.wrap(mapBytes);
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
            onDiskNodeToPk.put(ordinal, pk);
            onDiskPkToNode.put(pk, ordinal);
            if (ordinal > maxOrdinal) {
                maxOrdinal = ordinal;
            }
        }

        // Live inserts start after the loaded on-disk ordinals
        this.nextNodeId.set(maxOrdinal + 1);

        // Load OnDiskGraphIndex from bytes
        final byte[] graphBytesRef = graphBytes;
        ReaderSupplier readerSupplier = () -> new ByteBufferReader(ByteBuffer.wrap(graphBytesRef));
        this.onDiskGraph = OnDiskGraphIndex.load(readerSupplier);
        this.onDiskGraphBytes = graphBytesRef;

        // Create an empty live builder for new inserts
        this.mravv = new MapRandomAccessVectorValues(vectors, dim);
        BuildScoreProvider bsp =
                BuildScoreProvider.randomAccessScoreProvider(mravv, similarityFunction);
        this.builder = new GraphIndexBuilder(
                bsp, dim, m, beamWidth, neighborOverflow, alpha, ADD_HIERARCHY, REFINE_FINAL_GRAPH);

        LOGGER.log(Level.INFO,
                "loaded vector index {0} (FusedPQ): {1} on-disk nodes, dimension {2}",
                new Object[]{index.name, onDiskNodeToPk.size(), dim});
        return true;
    }

    @Override
    public void rebuild() throws DataStorageManagerException {
        long start = System.currentTimeMillis();
        LOGGER.log(Level.FINE, "rebuilding vector index {0}", index.name);
        dataStorageManager.initIndex(tableSpaceUUID, index.uuid);
        resetState();
        Table table = tableManager.getTable();
        tableManager.scanForIndexRebuild(r -> {
            herddb.utils.DataAccessor values = r.getDataAccessor(table);
            Bytes key = RecordSerializer.serializeIndexKey(values, table, table.primaryKey);
            Bytes indexKey = RecordSerializer.serializeIndexKey(values, index, index.columnNames);
            recordInserted(key, indexKey);
        });
        long elapsed = System.currentTimeMillis() - start;
        LOGGER.log(Level.INFO,
                "rebuilt vector index {0} in {1} ms: {2} nodes",
                new Object[]{index.name, elapsed, vectors.size()});
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

        boolean hasLiveNodes = builder != null && !nodeToPk.isEmpty();
        boolean hasOnDiskNodes = !onDiskNodeToPk.isEmpty();

        if (!hasLiveNodes && !hasOnDiskNodes && builder == null && onDiskGraph == null) {
            // Nothing indexed yet – persist empty metadata
            IndexStatus emptyStatus = new IndexStatus(
                    index.name, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, emptyStatus, pin));
            LOGGER.log(Level.INFO, "checkpoint vector index {0}: empty", index.name);
            return result;
        }

        if (dimension == 0) {
            IndexStatus emptyStatus = new IndexStatus(
                    index.name, sequenceNumber, newPageId.get(), new HashSet<>(), new byte[0]);
            List<PostCheckpointAction> result = new ArrayList<>();
            result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, emptyStatus, pin));
            LOGGER.log(Level.INFO, "checkpoint vector index {0}: empty dimension", index.name);
            return result;
        }

        // Count total active vectors (on-disk + live)
        int totalActiveVectors = onDiskNodeToPk.size() + nodeToPk.size();

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
            allVectors.putAll(vectors);
            allNodeToPk.putAll(nodeToPk);

            // Include on-disk nodes that haven't been deleted
            // We need their vectors; re-read from on-disk graph inline vectors
            if (onDiskGraph != null) {
                try (OnDiskGraphIndex.View view = onDiskGraph.getView()) {
                    for (Map.Entry<Integer, Bytes> e : onDiskNodeToPk.entrySet()) {
                        int ordinal = e.getKey();
                        Bytes pk = e.getValue();
                        VectorFloat<?> vec = view.getVector(ordinal);
                        // Assign a new nodeId for this vector in the merged set
                        // Use ordinal offset to avoid collisions with live inserts
                        // Strategy: use a reserved range for on-disk nodes
                        // Since live nextNodeId starts at maxOnDiskOrdinal+1, we just use ordinal directly
                        allVectors.put(ordinal, vec);
                        allNodeToPk.put(ordinal, pk);
                    }
                }
            }

            graphPageIds = writeFusedPQGraph(allVectors, allNodeToPk, dimension);
            mapPageIds = writeFusedPQMapData(allVectors, allNodeToPk);
            totalNodes = allNodeToPk.size();
        } else {
            // Legacy format: only serialize live builder
            if (builder != null) {
                builder.cleanup();
            }

            // Include on-disk vectors in the live builder before saving if we need to consolidate
            // For legacy format: just save the current live state
            ByteArrayOutputStream graphBaos = new ByteArrayOutputStream();
            if (builder != null) {
                try (DataOutputStream graphDos = new DataOutputStream(graphBaos)) {
                    ((OnHeapGraphIndex) builder.getGraph()).save(graphDos);
                }
            }
            graphPageIds = writeChunks(graphBaos.toByteArray(), TYPE_VECTOR_GRAPHCHUNK);
            mapPageIds = writeChunks(serializeMapData(vectors, nodeToPk), TYPE_VECTOR_MAPCHUNK);
            totalNodes = nodeToPk.size();
        }

        // ---- Build metadata ----
        int metaSize = useFusedPQ
                ? 30 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8  // v2: +1 byte for fusedPQ flag
                : 29 + 4 + graphPageIds.size() * 8 + 4 + mapPageIds.size() * 8; // v1

        ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
        if (useFusedPQ) {
            metaBuf.putInt(METADATA_VERSION_FUSEDPQ);
        } else {
            metaBuf.putInt(METADATA_VERSION_LEGACY);
        }
        metaBuf.putInt(dimension);
        metaBuf.putInt(m);
        metaBuf.putInt(beamWidth);
        metaBuf.putFloat(neighborOverflow);
        metaBuf.putFloat(alpha);
        metaBuf.put((byte) (ADD_HIERARCHY ? 1 : 0));
        if (useFusedPQ) {
            metaBuf.put((byte) 1); // fusedPQ flag
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

        List<PostCheckpointAction> result = new ArrayList<>();
        result.addAll(dataStorageManager.indexCheckpoint(tableSpaceUUID, index.uuid, indexStatus, pin));

        LOGGER.log(Level.INFO,
                "checkpoint vector index {0}: {1} nodes, {2} graph pages, {3} map pages, fusedPQ={4}",
                new Object[]{index.name, totalNodes, graphPageIds.size(), mapPageIds.size(), useFusedPQ});
        return result;
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
        for (Map.Entry<Integer, VectorFloat<?>> e : allVectors.entrySet()) {
            if (allNodeToPk.containsKey(e.getKey())) { // only active nodes
                mergedBuilder.addGraphNode(e.getKey(), e.getValue());
            }
        }
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
            byte[] graphBytes = Files.readAllBytes(tempFile);
            return writeChunks(graphBytes, TYPE_VECTOR_GRAPHCHUNK);
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
     * Writes map data for FusedPQ format: (newOrdinal, pk, vector) entries.
     * The ordinals written are the ORIGINAL node IDs (which will become the sequential
     * ordinals after OnDiskGraphIndexWriter renumbers them). We store the renumbered
     * ordinals so load can reconstruct the on-disk ordinal → pk mapping.
     */
    private List<Long> writeFusedPQMapData(ConcurrentHashMap<Integer, VectorFloat<?>> allVectors,
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int entryCount = sortedNodeIds.size();
        writeInt(baos, entryCount);

        for (int oldId : sortedNodeIds) {
            int newOrdinal = oldToNew.get(oldId);
            Bytes pk = allNodeToPk.get(oldId);
            byte[] pkBytes = pk.to_array();
            VectorFloat<?> vec = allVectors.get(oldId);

            writeInt(baos, newOrdinal);
            writeInt(baos, pkBytes.length);
            baos.write(pkBytes);
            int floatCount = vec.length();
            writeInt(baos, floatCount);
            for (int j = 0; j < floatCount; j++) {
                int bits = Float.floatToIntBits(vec.get(j));
                writeInt(baos, bits);
            }
        }
        return writeChunks(baos.toByteArray(), TYPE_VECTOR_MAPCHUNK);
    }

    private static void writeInt(ByteArrayOutputStream baos, int v) {
        baos.write((v >>> 24) & 0xFF);
        baos.write((v >>> 16) & 0xFF);
        baos.write((v >>> 8) & 0xFF);
        baos.write(v & 0xFF);
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
     * Serialises the pk/vector map into a flat byte array:
     * [ entryCount:int ]
     * for each entry:
     *   [ nodeId:int | pkLen:int | pkBytes | floatCount:int | floats ]
     */
    private byte[] serializeMapData(ConcurrentHashMap<Integer, VectorFloat<?>> vecs,
                                    ConcurrentHashMap<Integer, Bytes> nodeToKey) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        List<Map.Entry<Integer, Bytes>> entries = new ArrayList<>(nodeToKey.entrySet());
        int entryCount = entries.size();
        writeInt(baos, entryCount);

        for (Map.Entry<Integer, Bytes> e : entries) {
            int nodeId = e.getKey();
            byte[] pkBytes = e.getValue().to_array();
            VectorFloat<?> vec = vecs.get(nodeId);
            if (vec == null) {
                continue;
            }
            int floatCount = vec.length();
            writeInt(baos, nodeId);
            writeInt(baos, pkBytes.length);
            baos.write(pkBytes);
            writeInt(baos, floatCount);
            for (int j = 0; j < floatCount; j++) {
                int bits = Float.floatToIntBits(vec.get(j));
                writeInt(baos, bits);
            }
        }
        return baos.toByteArray();
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
    }

    @Override
    public void recordDeleted(Bytes key, Bytes indexKey) throws DataStorageManagerException {
        if (indexKey == null) {
            return;
        }
        // Check on-disk nodes first
        Integer onDiskOrdinal = onDiskPkToNode.remove(key);
        if (onDiskOrdinal != null) {
            onDiskNodeToPk.remove(onDiskOrdinal);
            // No need to mark deleted in OnDiskGraphIndex — filtered at search time
        }
        // Check live nodes
        Integer nodeId = pkToNode.remove(key);
        if (nodeId == null) {
            return;
        }
        nodeToPk.remove(nodeId);
        GraphIndexBuilder b = builder;
        if (b != null) {
            b.markNodeDeleted(nodeId);
        }
    }

    @Override
    public void recordUpdated(Bytes key, Bytes indexKeyRemoved, Bytes indexKeyAdded)
            throws DataStorageManagerException {
        recordDeleted(key, indexKeyRemoved);
        recordInserted(key, indexKeyAdded);
    }

    @Override
    public void truncate() throws DataStorageManagerException {
        resetState();
        truncateIndexData();
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
        if (odg != null && !onDiskNodeToPk.isEmpty()) {
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
        Set<Integer> activeOrdinals = onDiskNodeToPk.keySet();
        int k = Math.min(topK, activeOrdinals.size());
        if (k == 0) {
            return;
        }
        Bits acceptBits = activeOrdinals::contains;
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
                Bytes pk = onDiskNodeToPk.get(ns.node);
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
        if (this.dimension == 0) {
            this.dimension = dim;
            this.mravv = new MapRandomAccessVectorValues(vectors, dim);
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
        onDiskNodeToPk.clear();
        onDiskPkToNode.clear();
        nextNodeId.set(0);
        dimension = 0;
        builder = null;
        mravv = null;
        onDiskGraph = null;
        onDiskGraphBytes = null;
    }

    // -------------------------------------------------------------------------
    // Accessors for tests
    // -------------------------------------------------------------------------

    public int getNodeCount() {
        return nodeToPk.size() + onDiskNodeToPk.size();
    }

    public int getDimension() {
        return dimension;
    }

    public boolean isFusedPQEnabled() {
        return fusedPQ;
    }
}
