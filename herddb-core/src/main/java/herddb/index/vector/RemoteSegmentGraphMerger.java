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
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.CompactionProgressListener;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.RandomAccessOnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.FixedBitSet;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.LongBinaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone graph-merge utility used by the index-optimizer service to combine
 * a set of remote-stored {@code segmented-v2} vector segments into a single
 * larger segment without going through {@link PersistentVectorStore}.
 *
 * <p>The optimizer pod has no in-memory {@link VectorSegment}s and no live
 * {@code IndexingService}; all it has is the metadata znodes, a
 * {@link DataStorageManager} pointing at remote storage, and a scratch
 * directory. This class:
 * <ol>
 *   <li>Downloads each input's map file (which carries every {@code (ordinal,
 *       pk, vector)} tuple as written by {@code writeFusedPQMapDataToTempFile}).</li>
 *   <li>Drops every entry whose segment-local ordinal is in the supplied
 *       tombstone bitset (the optimizer pre-loads each input's
 *       {@code TombstoneOverlay} and hands the tombstoned-ordinal arrays in
 *       through {@link RemoteSegmentInput#tombstonedOrdinals}).</li>
 *   <li>De-duplicates surviving entries across segments by primary key,
 *       preferring the one from the highest-generation source.</li>
 *   <li>Builds a fresh {@code GraphIndexBuilder} over the surviving vectors,
 *       writes a FusedPQ + InlineVectors {@code OnDiskGraphIndex}, and a
 *       companion map file in the same wire format as
 *       {@code writeFusedPQMapDataToTempFile}.</li>
 *   <li>Uploads both files via {@link DataStorageManager#writeMultipartIndexFile}
 *       under {@code multipartUuid = indexUuid + "_seg" + outputSegmentId}.</li>
 * </ol>
 *
 * <p>The output is identical in every observable way to a segment produced
 * by {@code PersistentVectorStore.writeShardAsFusedPQSegment} — same on-disk
 * layout, same map-file format — so the indexing-service tier can load it
 * with {@code OnDiskGraphIndex.load(...)} as it would any other segment.
 *
 * <p>This class is nominally stateless and thread-hostile (every {@link #merge}
 * call builds its own local state and tears it down before returning). The two
 * optional callback fields ({@link #phaseListener} and {@link #batchListener})
 * are written by the caller immediately before each {@link #merge} call and
 * cleared after — no concurrency between writers. The HTTP-server thread reads
 * {@link #lastMergeTimings} after the merge is complete; the field is
 * {@code volatile} to ensure the write is visible without synchronisation.
 * Callers are responsible for serialising calls.
 */
public final class RemoteSegmentGraphMerger {

    private static final Logger LOGGER = Logger.getLogger(RemoteSegmentGraphMerger.class.getName());

    /**
     * Minimum number of vectors required to write the FusedPQ feature. Must
     * match {@link PersistentVectorStore#MIN_VECTORS_FOR_FUSED_PQ} exactly so
     * that every code path (IS checkpoint, IS-local compaction, external
     * optimizer merge) produces segments with the same feature set for the same
     * vector count. A mismatch is the root cause of issue #543: the optimizer
     * previously used 65,536 while the IS used 256, causing
     * {@code OnDiskGraphIndexCompactor.validateFeatures} to throw
     * {@code "Each source must have the same features"} whenever an
     * optimizer-produced segment ended up in the same merge batch as an
     * IS-checkpoint segment.
     */
    static final int MIN_VECTORS_FOR_FUSED_PQ = PersistentVectorStore.MIN_VECTORS_FOR_FUSED_PQ;

    /** Block size used for streaming downloads via the multipart reader. */
    private static final int DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024;

    /**
     * How often (in vectors) the batch-progress callback fires during the
     * legacy graph-build phase. Chosen to be large enough that the callback
     * overhead is negligible, and small enough to give sub-1 % granularity for
     * a 1 M-vector merge.
     */
    public static final int BATCH_PROGRESS_INTERVAL = 5_000;

    /**
     * Sanity caps for the input map-file header values. A corrupt or partially
     * uploaded multipart file can carry garbage int32 values; we surface those
     * as {@link IOException} rather than letting them coerce us into giant
     * allocations or unbounded loops (review item B.2#3 from the first
     * pr-reviewer pass).
     *
     * <p>{@code MAX_ENTRIES} matches the per-segment cap used by the in-IS
     * compactor ({@code MAX_TOMBSTONED_ORDINALS = 1<<28}); a single segment
     * never holds more vectors than that. {@code MAX_PK_LEN} bounds the
     * per-entry primary-key length.
     */
    static final int MAX_ENTRIES_PER_MAP_FILE = 1 << 28;
    static final int MAX_PK_LEN = 1 << 16;

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private final DataStorageManager dataStorageManager;
    private final Path tmpDirectory;
    private final int graphM;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final VectorSimilarityFunction similarity;

    // -------------------------------------------------------------------------
    // Progress / observability hooks (set by caller before each merge call).
    // -------------------------------------------------------------------------

    /**
     * Optional phase-change callback. Receives the new phase name as a
     * {@code String} at each major transition within the merge.
     *
     * <p>Written by the caller from the optimizer thread immediately before
     * each {@link #merge} call (via {@link #setPhaseListener}), and cleared
     * from the same thread in a {@code finally} block after {@link #merge}
     * returns. The HTTP server thread reads the effects only through the
     * {@code MergeProgress} object that receives callbacks, not this field
     * directly. Declared {@code volatile} so the assignment is visible to the
     * optimizer thread's own subsequent reads within {@link #merge} if it were
     * ever called from a thread that set the listener on a different thread;
     * also guards any accidental double-checked read inside the merge logic.
     */
    private volatile Consumer<String> phaseListener;

    /**
     * Optional batch-progress callback. Receives {@code (written, total)} as
     * a pair of {@code long}s fired every {@value #BATCH_PROGRESS_INTERVAL}
     * vectors during the legacy graph-build phase. Same lifecycle as
     * {@link #phaseListener}; declared {@code volatile} for the same reason.
     *
     * <p>{@link LongBinaryOperator} is used here purely as a convenient
     * two-{@code long} consumer; the return value is ignored.
     */
    private volatile LongBinaryOperator batchListener;

    /**
     * Timing breakdown of the last completed merge. Written at the end of
     * {@link #merge} (either path) and visible to the HTTP-server thread via
     * the {@code volatile} guarantee.
     */
    private volatile MergePhaseTimings lastMergeTimings;

    // -------------------------------------------------------------------------
    // Accessors for the callbacks and timing
    // -------------------------------------------------------------------------

    /**
     * Sets the phase-change listener. Pass {@code null} to remove. Must be
     * called from the same thread that calls {@link #merge}.
     */
    public void setPhaseListener(Consumer<String> listener) {
        this.phaseListener = listener;
    }

    /**
     * Sets the batch-progress listener. Pass {@code null} to remove. Must be
     * called from the same thread that calls {@link #merge}.
     */
    public void setBatchListener(LongBinaryOperator listener) {
        this.batchListener = listener;
    }

    /**
     * Returns the timing breakdown of the last completed merge, or {@code null}
     * if {@link #merge} has never been called on this instance.
     */
    public MergePhaseTimings getLastMergeTimings() {
        return lastMergeTimings;
    }

    /** Returns the configured graph degree M. Used for /tmp-usage estimation in log messages. */
    public int getGraphM() {
        return graphM;
    }

    // -------------------------------------------------------------------------
    // Timing breakdown value object
    // -------------------------------------------------------------------------

    /**
     * Per-phase wall-clock durations (in milliseconds) from the most recently
     * completed merge. All times are for the merge path that was actually
     * taken; unused fields are zero.
     */
    public static final class MergePhaseTimings {
        /** Time downloading input map (+ graph for streaming) files. */
        public final long downloadMs;
        /**
         * Time for PQ K-means training + vector encoding (legacy path only;
         * 0 for the streaming path and for shards below the FusedPQ threshold).
         */
        public final long pqTrainingMs;
        /**
         * Time building / compacting the graph (legacy: authority-map + GraphIndexBuilder;
         * streaming: OnDiskGraphIndexCompactor + output map-file write).
         */
        public final long compactionMs;
        /** Time uploading output graph + map files. */
        public final long uploadMs;

        public MergePhaseTimings(long downloadMs, long pqTrainingMs,
                                 long compactionMs, long uploadMs) {
            this.downloadMs   = downloadMs;
            this.pqTrainingMs = pqTrainingMs;
            this.compactionMs = compactionMs;
            this.uploadMs     = uploadMs;
        }
    }

    public RemoteSegmentGraphMerger(DataStorageManager dataStorageManager,
                                    Path tmpDirectory,
                                    int graphM,
                                    int beamWidth,
                                    float neighborOverflow,
                                    float alpha,
                                    VectorSimilarityFunction similarity) {
        this.dataStorageManager = Objects.requireNonNull(dataStorageManager, "dataStorageManager");
        this.tmpDirectory = Objects.requireNonNull(tmpDirectory, "tmpDirectory");
        this.graphM = graphM;
        this.beamWidth = beamWidth;
        this.neighborOverflow = neighborOverflow;
        this.alpha = alpha;
        this.similarity = Objects.requireNonNull(similarity, "similarity");
    }

    // -------------------------------------------------------------------------
    // Inputs / outputs
    // -------------------------------------------------------------------------

    /**
     * Description of one input segment for {@link #merge}. The optimizer pod
     * builds these from the registry znodes and the just-loaded tombstone
     * overlays.
     */
    public static final class RemoteSegmentInput {

        public final String tablespaceUuid;
        public final String indexUuid;
        public final String segmentUuid;
        public final long segmentId;
        public final long mapFileSize;
        /**
         * Exact size of the segment's graph multipart file, in bytes.
         * Used by the streaming-compaction path (issue #485) to drive the
         * graph download without probing the remote object. Derived by the
         * caller as {@code metadata.sizeBytes - metadata.mapFileSize}
         * (since {@code sizeBytes = graphFileSize + mapFileSize} per
         * {@link PersistentVectorStore.SegmentWriteResult}). The
         * legacy in-memory merge path ignores this field.
         *
         * <p>Set to {@code 0} when unknown (the streaming path will refuse
         * to merge such an input).
         */
        public final long graphFileSize;
        /**
         * Generation as recorded in the registry znode. Used for tie-breaking
         * when the same primary key appears in multiple input segments — the
         * higher-generation source wins (mirrors the in-IS authority map's
         * "highest-generation owner" rule).
         */
        public final long generation;
        /**
         * Segment-local ordinals that have been tombstoned. May be empty.
         * Caller is expected to load the latest {@code TombstoneOverlay} for
         * the segment (via {@code TombstoneOverlayManager.loadOverlay}) and
         * pass {@code overlay.getTombstonedOrdinals()} here, or an empty array
         * when no overlay exists.
         */
        public final int[] tombstonedOrdinals;

        /**
         * Convenience constructor that defaults {@link #graphFileSize} to 0.
         * Retained so the legacy merge path keeps working for callers that
         * have not yet been updated to plumb the graph size through. The
         * streaming path will refuse to merge any input whose graphFileSize
         * is 0 — operators must update the SPI caller (e.g.,
         * {@code RemoteSegmentMerger}) to pass the real value.
         */
        public RemoteSegmentInput(String tablespaceUuid, String indexUuid, String segmentUuid,
                                  long segmentId, long mapFileSize, long generation,
                                  int[] tombstonedOrdinals) {
            this(tablespaceUuid, indexUuid, segmentUuid, segmentId, mapFileSize,
                    /* graphFileSize */ 0L, generation, tombstonedOrdinals);
        }

        public RemoteSegmentInput(String tablespaceUuid, String indexUuid, String segmentUuid,
                                  long segmentId, long mapFileSize, long graphFileSize,
                                  long generation, int[] tombstonedOrdinals) {
            this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
            this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
            this.segmentUuid = Objects.requireNonNull(segmentUuid, "segmentUuid");
            this.segmentId = segmentId;
            this.mapFileSize = mapFileSize;
            this.graphFileSize = graphFileSize;
            this.generation = generation;
            this.tombstonedOrdinals = tombstonedOrdinals == null
                    ? new int[0]
                    : tombstonedOrdinals.clone();
        }
    }

    /** Outcome of a successful merge — handed to the caller for znode publishing. */
    public static final class MergeOutput {
        public final String tablespaceUuid;
        public final String indexUuid;
        public final long segmentId;
        public final String graphPath;
        public final long graphFileSize;
        public final String mapPath;
        public final long mapFileSize;
        public final long vectorCount;
        public final long droppedTombstones;
        public final long droppedDuplicates;
        /**
         * Sorted list of jvector {@code FeatureId} names written into the merged
         * graph file (e.g. {@code ["FUSED_PQ", "INLINE_VECTORS"]}). Propagated
         * into {@code SegmentMetadata.jvectorFeatureIds} by the caller so the
         * optimizer's merge policy can filter by feature set (issue #543).
         */
        public final List<String> featureIds;

        public MergeOutput(String tablespaceUuid, String indexUuid, long segmentId,
                           String graphPath, long graphFileSize,
                           String mapPath, long mapFileSize,
                           long vectorCount, long droppedTombstones, long droppedDuplicates,
                           List<String> featureIds) {
            this.tablespaceUuid = tablespaceUuid;
            this.indexUuid = indexUuid;
            this.segmentId = segmentId;
            this.graphPath = graphPath;
            this.graphFileSize = graphFileSize;
            this.mapPath = mapPath;
            this.mapFileSize = mapFileSize;
            this.vectorCount = vectorCount;
            this.droppedTombstones = droppedTombstones;
            this.droppedDuplicates = droppedDuplicates;
            this.featureIds = featureIds;
        }

        /** Sum of graph and map file sizes — convenient for {@code SegmentMetadata.sizeBytes}. */
        public long totalSizeBytes() {
            return graphFileSize + mapFileSize;
        }
    }

    // -------------------------------------------------------------------------
    // Merge
    // -------------------------------------------------------------------------

    /**
     * Reads every input segment's map file from remote storage, drops
     * tombstoned entries, de-duplicates surviving entries by PK preferring the
     * highest-generation source, builds a merged graph, and uploads both the
     * graph and a fresh map file. Returns metadata about the produced output.
     *
     * <p>Returns {@code null} when after tombstone + duplicate filtering no
     * vectors remain — the caller should treat this as "merge declined" and
     * not publish anything (the inputs are fully obsolete and will be
     * reaped through the normal retention path).
     *
     * @throws IOException                  on local I/O failures (temp file, jvector writer)
     * @throws DataStorageManagerException  on remote storage failures
     */
    public MergeOutput merge(List<RemoteSegmentInput> inputs,
                             String outputTablespaceUuid,
                             String outputIndexUuid,
                             long outputSegmentId,
                             int dim) throws IOException, DataStorageManagerException {
        Objects.requireNonNull(inputs, "inputs");
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs must be non-empty");
        }
        if (dim <= 0) {
            throw new IllegalArgumentException("dim must be positive: " + dim);
        }

        // Issue #485 — streaming compaction. Run the on-disk merge engine
        // when (a) the streaming flag is on, (b) there are at least two
        // inputs (jvector's OnDiskGraphIndexCompactor rejects single-source
        // construction), and (c) every input has a non-zero graphFileSize.
        // Any other configuration falls back to the legacy in-memory rebuild.
        if (VectorIndexCompactor.streamingCompactionEnabled
                && inputs.size() >= 2
                && allInputsHaveGraphFileSize(inputs)) {
            return mergeStreaming(inputs, outputTablespaceUuid, outputIndexUuid,
                    outputSegmentId, dim);
        }
        if (VectorIndexCompactor.streamingCompactionEnabled) {
            // Streaming is on, but the dispatch fence rejected the cycle —
            // log so operators correlate optimizer-pod compaction behavior with
            // the IS-side STREAMING_FALLBACK_TO_LEGACY_TOTAL counter.
            String reason;
            if (inputs.size() < 2) {
                reason = "fewer than 2 inputs (got " + inputs.size() + ")";
            } else {
                reason = "one or more inputs has graphFileSize <= 0";
            }
            LOGGER.log(Level.INFO,
                    "RemoteSegmentGraphMerger: falling back to legacy in-memory rebuild "
                            + "(streaming flag is on but {0})", reason);
        }
        return mergeLegacy(inputs, outputTablespaceUuid, outputIndexUuid,
                outputSegmentId, dim);
    }

    private static boolean allInputsHaveGraphFileSize(List<RemoteSegmentInput> inputs) {
        for (RemoteSegmentInput in : inputs) {
            if (in.graphFileSize <= 0L) {
                return false;
            }
        }
        return true;
    }

    /**
     * Bridge between {@link java.util.function.LongBinaryOperator} (the batch-progress
     * callback type used throughout this class) and {@link CompactionProgressListener}
     * (which has a {@code void onProgress(long, long)} contract).
     *
     * <p>The JDK provides no {@code LongBiConsumer} specialisation, so callers must use
     * {@code LongBinaryOperator} and return a documented no-op {@code 0L}. SpotBugs would
     * otherwise flag the ignored return value as
     * {@code RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT}.
     */
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "LongBinaryOperator used for side-effect only; 0L return is a documented no-op sentinel")
    private static void fireBatchProgress(LongBinaryOperator cb, long completed, long total) {
        cb.applyAsLong(completed, total);
    }

    /**
     * Legacy in-memory rebuild: reads each input's map file (carrying
     * {@code (ordinal, pk, vector)} tuples), de-duplicates by PK across
     * sources keeping the highest generation, builds a fresh
     * {@code GraphIndexBuilder}, writes a FusedPQ + InlineVectors graph,
     * and uploads both files. Memory cost is
     * {@code O(numTotalNodes × dimension)} — the original 1 GB compaction
     * cap was sized to keep this path within heap.
     *
     * <p>Issue #485 split this body out of {@link #merge} so the dispatcher
     * can route to {@link #mergeStreaming} when the streaming flag is on.
     */
    private MergeOutput mergeLegacy(List<RemoteSegmentInput> inputs,
                                    String outputTablespaceUuid,
                                    String outputIndexUuid,
                                    long outputSegmentId,
                                    int dim) throws IOException, DataStorageManagerException {
        long startNanos = System.nanoTime();

        // 1. Stream each input's map file to a local temp file. We never hold
        //    every map in memory — even for a 1M-vector merge that would be
        //    a few GiB. The temp files are all deleted at the end of merge().
        notifyPhase("downloading");
        List<Path> mapTempFiles = new ArrayList<>(inputs.size());
        long droppedTombstones = 0;
        long droppedDuplicates = 0;
        try {
            for (RemoteSegmentInput in : inputs) {
                mapTempFiles.add(downloadMapFile(in));
            }
            long downloadNanos = System.nanoTime();

            // 2. First pass: walk every map file and decide which (pk, vec) to keep.
            //    Authority map: pk -> (generation, vector). Higher generation wins.
            //    PERF: the inner reads are sequential against the BufferedInputStream;
            //    the de-duplication HashMap is bounded by the union of input PKs (which
            //    is also the upper bound on the merged segment's vector count).
            notifyPhase("compacting");
            Map<Bytes, AuthorityEntry> authority = new HashMap<>();
            for (int i = 0; i < inputs.size(); i++) {
                RemoteSegmentInput in = inputs.get(i);
                Path mapFile = mapTempFiles.get(i);
                BitSet tombstoneSet = buildTombstoneSet(in.tombstonedOrdinals);
                long[] perInputCounters = new long[2]; // [tombstones, duplicates]
                accumulateAuthority(in, mapFile, tombstoneSet, authority, perInputCounters, dim);
                droppedTombstones += perInputCounters[0];
                droppedDuplicates += perInputCounters[1];
            }
            int keptCount = authority.size();
            if (keptCount == 0) {
                LOGGER.log(Level.INFO,
                        "RemoteSegmentGraphMerger: every input vector was tombstoned or duplicated"
                                + " (inputs={0}, droppedTombstones={1}, droppedDuplicates={2});"
                                + " declining merge",
                        new Object[]{inputs.size(), droppedTombstones, droppedDuplicates});
                return null;
            }

            // 3. Build the merged graph over the surviving (pk, vector) pairs.
            //    We assign new ordinals 0..keptCount-1 in iteration order; the
            //    ordering is the natural HashMap iteration order which is fine
            //    because the consumer only cares about (pk -> ordinal -> vector)
            //    consistency, not about a specific layout.
            BuildArtefacts artefacts = buildGraph(authority, dim, keptCount);
            long compactionNanos = System.nanoTime();

            // 4. Write the graph and map files locally. Allocate inside the
            //    outer try so a failure on the second createTempFile call
            //    doesn't leak the first one (issue #485 review item B.7#2).
            notifyPhase("pq-training");
            Path graphTempFile = null;
            Path mapOutTempFile = null;
            boolean uploadedGraph = false;
            boolean uploadedMap = false;
            String graphPath = null;
            String mapPath = null;
            long graphSize;
            long mapSize;
            long pqNanos = 0L; // assigned inside the try block on the success path
            String multipartUuid = outputIndexUuid + "_seg" + outputSegmentId;
            try {
                graphTempFile = Files.createTempFile(tmpDirectory,
                        "herddb-merger-graph-", ".idx");
                mapOutTempFile = Files.createTempFile(tmpDirectory,
                        "herddb-merger-map-", ".tmp");
                // writeGraph performs PQ training internally; we notify the phase
                // before and record the elapsed time after.
                writeGraph(artefacts, dim, graphTempFile);
                pqNanos = System.nanoTime();
                graphSize = Files.size(graphTempFile);
                writeMapFile(artefacts, mapOutTempFile);
                mapSize = Files.size(mapOutTempFile);

                // 5. Upload both. If the second upload fails we delete the first
                //    so we don't leak partial output.
                notifyPhase("uploading");
                graphPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "graph",
                        graphTempFile, /* progress */ null);
                uploadedGraph = true;
                mapPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "map",
                        mapOutTempFile, /* progress */ null);
                uploadedMap = true;
            } finally {
                if (graphTempFile != null) {
                    Files.deleteIfExists(graphTempFile);
                }
                if (mapOutTempFile != null) {
                    Files.deleteIfExists(mapOutTempFile);
                }
                // Close the builder; we no longer need any of its in-memory state.
                try {
                    artefacts.builder.close();
                } catch (IOException ignored) {
                    // Builder close is best-effort — we already wrote the graph and
                    // there's nothing the caller can do about a close failure.
                }
                if (uploadedGraph && !uploadedMap) {
                    try {
                        dataStorageManager.deleteMultipartIndexFile(
                                outputTablespaceUuid, multipartUuid, "graph");
                    } catch (DataStorageManagerException cleanupErr) {
                        // Broad catch (storage is the plugin boundary): log and continue;
                        // the orphan file is a leak the caller must reap, not corruption.
                        LOGGER.log(Level.WARNING,
                                "merger orphan-graph cleanup failed for {0}: {1}",
                                new Object[]{multipartUuid, cleanupErr.getMessage()});
                    }
                }
            }

            long uploadNanos = System.nanoTime();
            long elapsedMs = (uploadNanos - startNanos) / 1_000_000L;
            lastMergeTimings = new MergePhaseTimings(
                    (downloadNanos - startNanos) / 1_000_000L,
                    (pqNanos - compactionNanos) / 1_000_000L,
                    (compactionNanos - downloadNanos) / 1_000_000L,
                    (uploadNanos - pqNanos) / 1_000_000L);
            LOGGER.log(Level.INFO,
                    "RemoteSegmentGraphMerger: merged {0} inputs into segment {1}/{2}_seg{3}"
                            + " ({4} kept, {5} tombstoned, {6} duplicates dropped,"
                            + " total={7} ms, download={8} ms, compaction={9} ms,"
                            + " pqTraining={10} ms, upload={11} ms)",
                    new Object[]{inputs.size(), outputTablespaceUuid, outputIndexUuid,
                            outputSegmentId, keptCount, droppedTombstones, droppedDuplicates,
                            elapsedMs, lastMergeTimings.downloadMs,
                            lastMergeTimings.compactionMs, lastMergeTimings.pqTrainingMs,
                            lastMergeTimings.uploadMs});
            return new MergeOutput(outputTablespaceUuid, outputIndexUuid, outputSegmentId,
                    graphPath, graphSize, mapPath, mapSize,
                    keptCount, droppedTombstones, droppedDuplicates,
                    featureIdsForVectorCount(keptCount));
        } finally {
            for (Path tmp : mapTempFiles) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                    // Best-effort tmp cleanup; ENOENT is fine.
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Feature-set helpers (issue #543)
    // -------------------------------------------------------------------------

    /**
     * Returns a sorted, unmodifiable {@code List<String>} of feature ID names
     * for the output of a legacy merge that produced {@code vectorCount} vectors.
     * The list is determined by {@link #MIN_VECTORS_FOR_FUSED_PQ}: outputs with
     * fewer vectors carry only {@code INLINE_VECTORS}; larger outputs carry both
     * {@code FUSED_PQ} and {@code INLINE_VECTORS}.
     */
    private static List<String> featureIdsForVectorCount(long vectorCount) {
        if (vectorCount >= MIN_VECTORS_FOR_FUSED_PQ) {
            return Collections.unmodifiableList(java.util.Arrays.asList("FUSED_PQ", "INLINE_VECTORS"));
        } else {
            return Collections.singletonList("INLINE_VECTORS");
        }
    }

    /**
     * Returns a sorted, unmodifiable {@code List<String>} of feature ID names
     * derived from the first source's feature set. Called by the streaming path
     * after uniformity has been validated — all sources share the same features.
     */
    private static List<String> featureIdsFromSources(List<OnDiskGraphIndex> sources) {
        if (sources.isEmpty()) {
            return Collections.emptyList();
        }
        return featureSetToStringList(sources.get(0).getFeatureSet());
    }

    /**
     * Converts a {@code Set<FeatureId>} into a sorted {@code List<String>} of
     * feature names. Sorting ensures the representation is canonical so two
     * segments with the same logical feature set always produce the same list.
     *
     * <p>Public so that {@link PersistentVectorStore} can stamp the feature list
     * on existing on-disk segments without duplicating the conversion logic.
     */
    public static List<String> featureSetToStringList(Set<FeatureId> featureIds) {
        List<String> names = new ArrayList<>(featureIds.size());
        for (FeatureId fid : featureIds) {
            names.add(fid.name());
        }
        Collections.sort(names);
        return Collections.unmodifiableList(names);
    }


    /**
     * Returns {@code true} when every source graph has the same {@code FeatureId}
     * keyset as the first. {@code false} indicates heterogeneous inputs that
     * would cause {@code OnDiskGraphIndexCompactor.validateFeatures} to throw.
     *
     * @param sources opened on-disk graphs (parallel to {@code inputs})
     * @param inputs  matching input descriptors (used only for logging)
     */
    private static boolean allSourcesHaveUniformFeatures(List<OnDiskGraphIndex> sources,
                                                          List<RemoteSegmentInput> inputs) {
        if (sources.size() <= 1) {
            return true;
        }
        Set<FeatureId> ref = sources.get(0).getFeatureSet();
        for (int s = 1; s < sources.size(); s++) {
            if (!sources.get(s).getFeatureSet().equals(ref)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Best-effort delete of the multipart files produced by a previous
     * {@link #merge} that the caller has decided to abandon (e.g. the
     * post-merge revalidation aborted the run). Idempotent: missing files
     * are not an error.
     */
    public void deleteOutput(MergeOutput out) {
        if (out == null) {
            return;
        }
        String multipartUuid = out.indexUuid + "_seg" + out.segmentId;
        deleteIgnoringMissing(out.tablespaceUuid, multipartUuid, "graph");
        deleteIgnoringMissing(out.tablespaceUuid, multipartUuid, "map");
    }

    private void deleteIgnoringMissing(String tablespaceUuid, String multipartUuid, String fileType) {
        try {
            dataStorageManager.deleteMultipartIndexFile(tablespaceUuid, multipartUuid, fileType);
        } catch (DataStorageManagerException e) {
            // Broad catch (storage layer): cleanup is best-effort. The caller
            // is already aborting the merge run; leaking a multipart artefact
            // is wasted bytes the next reconcile cycle can sweep.
            LOGGER.log(Level.WARNING,
                    "merger output cleanup failed for {0}/{1}: {2}",
                    new Object[]{multipartUuid, fileType, e.getMessage()});
        }
    }

    // -------------------------------------------------------------------------
    // Streaming compaction (issue #485): drives OnDiskGraphIndexCompactor on
    // local copies of each input's graph + map files.
    // -------------------------------------------------------------------------

    /**
     * Streaming N:1 merge driven by jvector's
     * {@link io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexCompactor}.
     * Memory cost is bounded by
     * {@code O(taskWindowSize × maxDegree × float[dim])} instead of the
     * legacy path's {@code O(numTotalNodes × dim)}, lifting the historical
     * 1 GB cap on per-cycle byte input.
     *
     * <p>High-level flow:
     * <ol>
     *   <li>Download every input's graph + map files into local temp files.</li>
     *   <li>Walk each map file once to build per-source {@code Bytes[ord]} PK
     *       arrays. This is bounded by the union of input PKs, far smaller
     *       than the per-vector allocation the legacy path performs.</li>
     *   <li>Build a global authority map (highest-generation source per PK
     *       wins; ties go to the first-observed source for determinism, same
     *       rule the legacy path uses).</li>
     *   <li>Build per-source {@link FixedBitSet} of live ordinals
     *       ({@code alive ⇔ PK is authoritative AND ord ∉ tombstoneSet}).</li>
     *   <li>Open each input's graph as {@link OnDiskGraphIndex}, assert
     *       {@code INLINE_VECTORS} via
     *       {@link VectorIndexCompactor#requireInlineVectorsFeature(int, java.util.Map)}.</li>
     *   <li>Build per-source {@link VectorIndexCompactor.DenseLiveOrdinalMapper}
     *       so the merged graph is dense {@code 0..keptCount-1}.</li>
     *   <li>Run {@code OnDiskGraphIndexCompactor.compact(...)}.</li>
     *   <li>Walk each source's live ordinals and emit
     *       {@code (newOrd, pk, vec)} tuples to the output map file (vector
     *       read via {@link OnDiskGraphIndex.View#getVectorInto}).</li>
     *   <li>Upload both files; on a partial upload failure, best-effort
     *       delete the half-published graph so the caller's
     *       {@link #deleteOutput} does not need to track a stale uuid.</li>
     * </ol>
     */
    private MergeOutput mergeStreaming(List<RemoteSegmentInput> inputs,
                                       String outputTablespaceUuid,
                                       String outputIndexUuid,
                                       long outputSegmentId,
                                       int dim) throws IOException, DataStorageManagerException {
        long startNanos = System.nanoTime();
        int n = inputs.size();
        List<Path> mapTemps = new ArrayList<>(n);
        List<Path> graphTemps = new ArrayList<>(n);
        List<ReaderSupplier> readerSuppliers = new ArrayList<>(n);
        long droppedTombstones = 0L;
        long droppedDuplicates = 0L;

        try {
            // 1. Download every input's graph + map files.
            notifyPhase("downloading");
            for (RemoteSegmentInput in : inputs) {
                mapTemps.add(downloadMapFile(in));
                graphTemps.add(downloadGraphFile(in));
            }
            long downloadNanos = System.nanoTime();

            // 2. Walk every map file once: per-source PK arrays + per-source size.
            //    Validates the same wire-level invariants accumulateAuthority does
            //    (entryCount cap, pkLen cap, dim match) so corrupt inputs are caught
            //    before we hand them to the compactor.
            List<Bytes[]> perSourcePks = new ArrayList<>(n);
            for (int s = 0; s < n; s++) {
                perSourcePks.add(readPksFromMapFile(mapTemps.get(s), inputs.get(s), dim));
            }

            // 3. Build authority across all sources. Ties go to the first-observed
            //    source so the streaming path matches the legacy mergeLegacy ordering.
            //    Value: long[] of {generation, sourceIdx, ord}.
            Map<Bytes, long[]> authority = new HashMap<>();
            for (int s = 0; s < n; s++) {
                RemoteSegmentInput in = inputs.get(s);
                BitSet tombstoneSet = buildTombstoneSet(in.tombstonedOrdinals);
                Bytes[] pks = perSourcePks.get(s);
                for (int ord = 0; ord < pks.length; ord++) {
                    Bytes pk = pks[ord];
                    if (pk == null) {
                        continue;
                    }
                    if (tombstoneSet.get(ord)) {
                        droppedTombstones++;
                        continue;
                    }
                    long[] existing = authority.get(pk);
                    if (existing == null) {
                        authority.put(pk, new long[]{in.generation, s, ord});
                    } else if (in.generation > existing[0]) {
                        // Strictly newer — displace the existing winner; the
                        // displaced entry becomes a duplicate-drop.
                        existing[0] = in.generation;
                        existing[1] = s;
                        existing[2] = ord;
                        droppedDuplicates++;
                    } else {
                        // Equal-or-lower generation: this entry loses, existing
                        // winner stays. Matches mergeLegacy's first-observed rule.
                        droppedDuplicates++;
                    }
                }
            }
            if (authority.isEmpty()) {
                LOGGER.log(Level.INFO,
                        "RemoteSegmentGraphMerger (streaming): every input vector was"
                                + " tombstoned or duplicated (inputs={0},"
                                + " droppedTombstones={1}, droppedDuplicates={2});"
                                + " declining merge",
                        new Object[]{n, droppedTombstones, droppedDuplicates});
                return null;
            }

            // 4. Build per-source FixedBitSet from authority winners.
            List<FixedBitSet> liveBitsets = new ArrayList<>(n);
            for (int s = 0; s < n; s++) {
                int srcSize = perSourcePks.get(s).length;
                liveBitsets.add(new FixedBitSet(Math.max(1, srcSize)));
            }
            int keptCount = 0;
            for (Map.Entry<Bytes, long[]> e : authority.entrySet()) {
                long[] winner = e.getValue();
                int srcIdx = (int) winner[1];
                int ord = (int) winner[2];
                liveBitsets.get(srcIdx).set(ord);
                keptCount++;
            }

            // 5. Open OnDiskGraphIndex per input + INLINE_VECTORS guard.
            List<OnDiskGraphIndex> sources = new ArrayList<>(n);
            for (int s = 0; s < n; s++) {
                ReaderSupplier rs = ReaderSupplierFactory.open(graphTemps.get(s));
                readerSuppliers.add(rs);
                OnDiskGraphIndex odg;
                try {
                    odg = OnDiskGraphIndex.load(rs);
                } catch (RuntimeException re) {
                    throw new IOException("OnDiskGraphIndex.load failed for input "
                            + inputs.get(s).segmentUuid + " (streaming merge)", re);
                }
                try {
                    VectorIndexCompactor.requireInlineVectorsFeature(s, odg.getFeatures());
                } catch (VectorIndexCompactor.CompactionException ce) {
                    // Translate to IOException at the merger's contract boundary
                    // (the SPI contract is IOException / DataStorageManagerException;
                    // CompactionException is internal to VectorIndexCompactor).
                    throw new IOException(ce.getMessage(), ce);
                }
                sources.add(odg);
            }

            // 5b. Feature-set uniformity check (issue #543). jvector's
            //     OnDiskGraphIndexCompactor.validateFeatures requires every
            //     source to share exactly the same FeatureId keyset. If the
            //     sources are heterogeneous (e.g. some FusedPQ, some
            //     InlineVectors-only) the compactor would throw
            //     "Each source must have the same features". Detect this
            //     BEFORE calling the compactor and fall back to the legacy
            //     in-memory rebuild, which works from map-file vectors and
            //     does not require uniform graph features.
            if (!allSourcesHaveUniformFeatures(sources, inputs)) {
                // Graph files are already downloaded; close readers and let
                // the finally block clean up the temp files. The legacy path
                // downloads only the map files, which it needs anyway.
                LOGGER.log(Level.WARNING,
                        "RemoteSegmentGraphMerger (streaming): detected heterogeneous"
                                + " feature sets across {0} input segments"
                                + " — falling back to legacy in-memory rebuild."
                                + " Segment feature details logged at FINE level.",
                        n);
                if (LOGGER.isLoggable(Level.FINE)) {
                    for (int s = 0; s < n; s++) {
                        LOGGER.log(Level.FINE,
                                "  segment {0}: featureIds={1}",
                                new Object[]{inputs.get(s).segmentUuid,
                                        featureSetToStringList(sources.get(s).getFeatureSet())});
                    }
                }
                // Close reader suppliers before falling through to the finally block.
                for (ReaderSupplier rs : readerSuppliers) {
                    try {
                        rs.close();
                    } catch (IOException ignored) {
                        // best-effort
                    }
                }
                readerSuppliers.clear();
                // The legacy rebuild reads map files from remote storage;
                // temp map files downloaded in step 1 are already on disk.
                // Pass the inputs directly; the legacy path re-downloads its
                // own copies so we just let the finally block delete ours.
                return mergeLegacy(inputs, outputTablespaceUuid, outputIndexUuid,
                        outputSegmentId, dim);
            }

            // 6. Build dense per-source mappers. Align bitset length to the
            //    graph's level-0 size (jvector validates the bitset bounds).
            List<OrdinalMapper> mappers = new ArrayList<>(n);
            int globalBase = 0;
            for (int s = 0; s < n; s++) {
                int srcSize = sources.get(s).size(0);
                FixedBitSet live = liveBitsets.get(s);
                if (live.length() != srcSize) {
                    FixedBitSet aligned = new FixedBitSet(Math.max(1, srcSize));
                    int last = Math.min(live.length(), srcSize);
                    for (int ord = 0; ord < last; ord++) {
                        if (live.get(ord)) {
                            aligned.set(ord);
                        }
                    }
                    liveBitsets.set(s, aligned);
                    live = aligned;
                }
                VectorIndexCompactor.DenseLiveOrdinalMapper mapper =
                        new VectorIndexCompactor.DenseLiveOrdinalMapper(live, srcSize, globalBase);
                mappers.add(mapper);
                globalBase += mapper.liveCount();
            }

            // 7. Run the streaming compactor + write the output map file.
            //    Allocate both temp files before the inner try so a failure
            //    on the second allocation doesn't leak the first; allocations
            //    happen inside the outer try so the existing finally cleans up.
            notifyPhase("compacting");
            // Notify the initial batch state so the HTTP /status endpoint immediately
            // shows a non-zero denominator when entering the "compacting" phase. The
            // batchListener is read once here so the same reference is used for both
            // the initial call and the CompactionProgressListener lambda below.
            LongBinaryOperator batchCb = batchListener;
            if (batchCb != null) {
                fireBatchProgress(batchCb, 0L, keptCount);
            }
            Path graphOutTemp = null;
            Path mapOutTemp = null;
            String multipartUuid = outputIndexUuid + "_seg" + outputSegmentId;
            boolean uploadedGraph = false;
            boolean uploadedMap = false;
            String graphPath = null;
            String mapPath = null;
            long graphSize;
            long mapSize;
            try {
                graphOutTemp = Files.createTempFile(tmpDirectory,
                        "herddb-merger-stream-graph-", ".idx");
                mapOutTemp = Files.createTempFile(tmpDirectory,
                        "herddb-merger-stream-map-", ".tmp");

                OnDiskGraphIndexCompactor compactor = new OnDiskGraphIndexCompactor(
                        sources, liveBitsets, mappers, similarity,
                        PhysicalCoreExecutor.pool());
                // Build a typed CompactionProgressListener from the batch callback so
                // jvector can push (completedBatches, totalBatches) updates back to
                // MergeProgress without log-message parsing. The listener is null when
                // no observer is registered (avoids an empty lambda allocation).
                // fireBatchProgress() is used to avoid a lambda that ignores the
                // LongBinaryOperator return value (SpotBugs RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT).
                CompactionProgressListener progressListener = batchCb != null
                        ? (completed, total) -> fireBatchProgress(batchCb, completed, total)
                        : null;
                try {
                    compactor.compact(graphOutTemp, progressListener);
                } catch (java.io.FileNotFoundException | RuntimeException e) {
                    throw new IOException("OnDiskGraphIndexCompactor.compact failed"
                            + " (streaming merge)", e);
                }
                writeStreamingOutputMapFile(sources, perSourcePks, liveBitsets, mappers,
                        mapOutTemp, dim);
                graphSize = Files.size(graphOutTemp);
                mapSize = Files.size(mapOutTemp);
                long compactionNanos = System.nanoTime();

                // 8. Upload. On a partial upload (graph succeeded, map failed)
                //    best-effort delete the orphan graph so the caller's
                //    abandon path doesn't see a half-published output.
                notifyPhase("uploading");
                graphPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "graph",
                        graphOutTemp, /* progress */ null);
                uploadedGraph = true;
                mapPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "map",
                        mapOutTemp, /* progress */ null);
                uploadedMap = true;
                long uploadNanos = System.nanoTime();
                lastMergeTimings = new MergePhaseTimings(
                        (downloadNanos - startNanos) / 1_000_000L,
                        /* pqTrainingMs */ 0L,
                        (compactionNanos - downloadNanos) / 1_000_000L,
                        (uploadNanos - compactionNanos) / 1_000_000L);
            } finally {
                if (graphOutTemp != null) {
                    try {
                        Files.deleteIfExists(graphOutTemp);
                    } catch (IOException ignored) {
                        // best-effort; orphan tmp does not affect remote state.
                    }
                }
                if (mapOutTemp != null) {
                    try {
                        Files.deleteIfExists(mapOutTemp);
                    } catch (IOException ignored) {
                        // same.
                    }
                }
                if (uploadedGraph && !uploadedMap) {
                    try {
                        dataStorageManager.deleteMultipartIndexFile(
                                outputTablespaceUuid, multipartUuid, "graph");
                    } catch (DataStorageManagerException cleanupErr) {
                        // Broad catch (storage is the plugin boundary):
                        // log and continue; orphan is a leak the caller must
                        // reap, not corruption.
                        LOGGER.log(Level.WARNING,
                                "streaming merger orphan-graph cleanup failed for {0}: {1}",
                                new Object[]{multipartUuid, cleanupErr.getMessage()});
                    }
                }
            }

            MergePhaseTimings timings = lastMergeTimings; // set inside inner try
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            LOGGER.log(Level.INFO,
                    "RemoteSegmentGraphMerger (streaming): merged {0} inputs into segment"
                            + " {1}/{2}_seg{3} ({4} kept, {5} tombstoned, {6} duplicates"
                            + " dropped, total={7} ms, download={8} ms, compaction={9} ms,"
                            + " upload={10} ms)",
                    new Object[]{n, outputTablespaceUuid, outputIndexUuid,
                            outputSegmentId, keptCount, droppedTombstones, droppedDuplicates,
                            elapsedMs,
                            timings != null ? timings.downloadMs : -1,
                            timings != null ? timings.compactionMs : -1,
                            timings != null ? timings.uploadMs : -1});
            return new MergeOutput(outputTablespaceUuid, outputIndexUuid, outputSegmentId,
                    graphPath, graphSize, mapPath, mapSize,
                    keptCount, droppedTombstones, droppedDuplicates,
                    featureIdsFromSources(sources));
        } finally {
            for (ReaderSupplier rs : readerSuppliers) {
                try {
                    rs.close();
                } catch (IOException e) {
                    LOGGER.log(Level.FINE,
                            "ignoring reader-supplier close failure (streaming merge)", e);
                }
            }
            for (Path p : mapTemps) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort tmp cleanup
                }
            }
            for (Path p : graphTemps) {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // same
                }
            }
        }
    }

    /**
     * Walks one input's downloaded map file and returns an array indexed by
     * source-local ordinal whose entries are the per-ordinal {@link Bytes}
     * primary key (or {@code null} for ordinals that were never written).
     * Each {@code (ordinal, pkLen, pk, dim, floats)} tuple is validated
     * against the same caps {@link #accumulateAuthority} enforces, then the
     * float payload is skipped (the streaming path reads vectors from the
     * graph file later).
     */
    private Bytes[] readPksFromMapFile(Path mapFile, RemoteSegmentInput in, int expectedDim)
            throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()),
                        DOWNLOAD_CHUNK_SIZE))) {
            int entryCount = dis.readInt();
            if (entryCount < 0 || entryCount > MAX_ENTRIES_PER_MAP_FILE) {
                throw new IOException("malformed map file " + mapFile
                        + ": entryCount " + entryCount
                        + " outside [0, " + MAX_ENTRIES_PER_MAP_FILE + "]");
            }
            // Two-pass: first sweep to discover maxOrdinal so we can size the
            // output array tightly. We could pre-allocate generously but for
            // very large segments that wastes substantial memory.
            // Cheaper: collect (ord, pk) into an ArrayList, find max, then
            // populate the array. Single allocation, no resizing.
            ArrayList<int[]> ordsAndLen = new ArrayList<>(entryCount);
            ArrayList<byte[]> pkBytes = new ArrayList<>(entryCount);
            int maxOrd = -1;
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                if (ordinal < 0) {
                    throw new IOException("malformed map file " + mapFile
                            + ": negative ordinal " + ordinal + " at entry " + i);
                }
                int pkLen = dis.readInt();
                if (pkLen < 0 || pkLen > MAX_PK_LEN) {
                    throw new IOException("malformed map file " + mapFile
                            + ": pkLen " + pkLen + " outside [0, " + MAX_PK_LEN
                            + "] at entry " + i);
                }
                byte[] raw = new byte[pkLen];
                dis.readFully(raw);
                int floatCount = dis.readInt();
                if (floatCount != expectedDim) {
                    throw new IOException("dimension mismatch in input " + mapFile
                            + " (segment " + in.segmentUuid + "): expected " + expectedDim
                            + ", got " + floatCount + " at entry " + i);
                }
                skipFully(dis, (long) floatCount * Float.BYTES);
                ordsAndLen.add(new int[]{ordinal, pkLen});
                pkBytes.add(raw);
                if (ordinal > maxOrd) {
                    maxOrd = ordinal;
                }
            }
            // Use Math.max(1, maxOrd + 1): an empty map file → array of length 1
            // so FixedBitSet construction never sees length 0.
            Bytes[] pks = new Bytes[Math.max(1, maxOrd + 1)];
            for (int i = 0; i < entryCount; i++) {
                int[] ol = ordsAndLen.get(i);
                pks[ol[0]] = Bytes.from_array(pkBytes.get(i));
            }
            return pks;
        }
    }

    /**
     * Writes the output map file for the streaming path. Walks each source's
     * live bitset in (sourceIdx, oldOrd) ascending order, reads the vector
     * via {@link OnDiskGraphIndex.View#getVectorInto}, and emits the
     * {@code (newOrd, pkLen, pk, dim, floats)} tuple. Wire format matches
     * {@code PersistentVectorStore.writeFusedPQMapDataToTempFile} verbatim
     * so the indexing-service can reload the merged segment with no
     * format-detection logic.
     */
    private void writeStreamingOutputMapFile(
            List<OnDiskGraphIndex> sources,
            List<Bytes[]> perSourcePks,
            List<FixedBitSet> liveBitsets,
            List<OrdinalMapper> mappers,
            Path mapTempFile,
            int dim) throws IOException {
        boolean ok = false;
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapTempFile.toFile()), DOWNLOAD_CHUNK_SIZE);
             DataOutputStream dos = new DataOutputStream(bos)) {
            // Total entry count = sum of live cardinalities — equals authority.size()
            // by construction; recompute to keep the writer self-contained.
            int entryCount = 0;
            for (FixedBitSet b : liveBitsets) {
                entryCount += b.cardinality();
            }
            dos.writeInt(entryCount);
            VectorFloat<?> tmp = VTS.createFloatVector(dim);
            for (int s = 0; s < sources.size(); s++) {
                OnDiskGraphIndex odg = sources.get(s);
                FixedBitSet live = liveBitsets.get(s);
                OrdinalMapper mapper = mappers.get(s);
                Bytes[] pks = perSourcePks.get(s);
                int srcSize = odg.size(0);
                OnDiskGraphIndex.View view;
                try {
                    view = (OnDiskGraphIndex.View) odg.getView();
                } catch (RuntimeException re) {
                    throw new IOException("getView failed for source " + s
                            + " (streaming merge map writer)", re);
                }
                try {
                    for (int ord = 0; ord < srcSize; ord++) {
                        if (!live.get(ord)) {
                            continue;
                        }
                        Bytes pk = (ord < pks.length) ? pks[ord] : null;
                        if (pk == null) {
                            // Should never happen — the bitset is built from
                            // per-source PK presence + authority.
                            throw new IOException("streaming merge: live ordinal "
                                    + ord + " in source " + s + " has no PK");
                        }
                        try {
                            view.getVectorInto(ord, tmp, 0);
                        } catch (RuntimeException re) {
                            throw new IOException("getVectorInto failed at ord "
                                    + ord + " of source " + s
                                    + " (streaming merge map writer)", re);
                        }
                        if (tmp.length() != dim) {
                            throw new IOException("dimension mismatch at ord " + ord
                                    + " of source " + s + ": expected " + dim
                                    + " got " + tmp.length());
                        }
                        int newOrdinal = mapper.oldToNew(ord);
                        byte[] pkBytes = pk.to_array();
                        dos.writeInt(newOrdinal);
                        dos.writeInt(pkBytes.length);
                        dos.write(pkBytes);
                        dos.writeInt(dim);
                        for (int j = 0; j < dim; j++) {
                            dos.writeInt(Float.floatToIntBits(tmp.get(j)));
                        }
                    }
                } finally {
                    try {
                        view.close();
                    } catch (IOException e) {
                        LOGGER.log(Level.FINE,
                                "ignoring view close in streaming merge map writer", e);
                    }
                }
            }
            ok = true;
        } finally {
            if (!ok) {
                try {
                    Files.deleteIfExists(mapTempFile);
                } catch (IOException ignored) {
                    // best-effort
                }
            }
        }
    }

    /**
     * Mirrors {@link #downloadMapFile} for the graph file. Required by the
     * streaming path so the merger can open each input's graph as an
     * {@link OnDiskGraphIndex} and feed it to {@code OnDiskGraphIndexCompactor}.
     */
    private Path downloadGraphFile(RemoteSegmentInput in)
            throws IOException, DataStorageManagerException {
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-merger-input-graph-", ".tmp");
        boolean ok = false;
        try {
            String multipartUuid = in.indexUuid + "_seg" + in.segmentId;
            if (dataStorageManager.supportsDirectMultipartDownload()) {
                dataStorageManager.downloadMultipartIndexFile(
                        in.tablespaceUuid, multipartUuid, "graph", in.graphFileSize, tempFile);
            } else {
                ReaderSupplier supplier = dataStorageManager.multipartIndexReaderSupplier(
                        in.tablespaceUuid, multipartUuid, "graph", in.graphFileSize);
                try (RandomAccessReader reader = supplier.get();
                     FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                     BufferedOutputStream bos = new BufferedOutputStream(fos, DOWNLOAD_CHUNK_SIZE)) {
                    reader.seek(0L);
                    byte[] buf = new byte[DOWNLOAD_CHUNK_SIZE];
                    long remaining = in.graphFileSize;
                    while (remaining > 0L) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        byte[] chunk = (toRead == buf.length) ? buf : new byte[toRead];
                        reader.readFully(chunk);
                        bos.write(chunk, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
            ok = true;
            return tempFile;
        } finally {
            if (!ok) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // best-effort
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Map file streaming + tombstone filter
    // -------------------------------------------------------------------------

    private Path downloadMapFile(RemoteSegmentInput in) throws IOException, DataStorageManagerException {
        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-merger-input-", ".tmp");
        boolean ok = false;
        try {
            String multipartUuid = in.indexUuid + "_seg" + in.segmentId;
            if (dataStorageManager.supportsDirectMultipartDownload()) {
                dataStorageManager.downloadMultipartIndexFile(
                        in.tablespaceUuid, multipartUuid, "map", in.mapFileSize, tempFile);
            } else {
                ReaderSupplier supplier = dataStorageManager.multipartIndexReaderSupplier(
                        in.tablespaceUuid, multipartUuid, "map", in.mapFileSize);
                try (RandomAccessReader reader = supplier.get();
                     FileOutputStream fos = new FileOutputStream(tempFile.toFile());
                     BufferedOutputStream bos = new BufferedOutputStream(fos, DOWNLOAD_CHUNK_SIZE)) {
                    reader.seek(0L);
                    // The caller (RemoteSegmentMerger) refuses inputs whose
                    // mapFileSize is unset (issue #484 round 3), so by the
                    // time we get here in.mapFileSize is the EXACT byte size
                    // of the remote map file. Read exactly that many bytes
                    // in chunks; readFully will throw IOException if the
                    // remote file is shorter than advertised, which is the
                    // right failure mode for a corrupt or truncated upload.
                    byte[] buf = new byte[DOWNLOAD_CHUNK_SIZE];
                    long remaining = in.mapFileSize;
                    while (remaining > 0L) {
                        int toRead = (int) Math.min(buf.length, remaining);
                        byte[] chunk = (toRead == buf.length) ? buf : new byte[toRead];
                        reader.readFully(chunk);
                        bos.write(chunk, 0, toRead);
                        remaining -= toRead;
                    }
                }
            }
            ok = true;
            return tempFile;
        } finally {
            if (!ok) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // ENOENT is fine; we never opened the file.
                }
            }
        }
    }

    private static BitSet buildTombstoneSet(int[] tombstonedOrdinals) {
        if (tombstonedOrdinals == null || tombstonedOrdinals.length == 0) {
            return new BitSet(0);
        }
        int max = 0;
        for (int o : tombstonedOrdinals) {
            if (o > max) {
                max = o;
            }
        }
        BitSet b = new BitSet(max + 1);
        for (int o : tombstonedOrdinals) {
            if (o >= 0) {
                b.set(o);
            }
        }
        return b;
    }

    /**
     * Walks one input's map file, dropping tombstoned ordinals, and either
     * inserts the surviving (pk, vector) into the authority map or — if
     * a same-PK entry from an earlier input had a higher generation —
     * counts it as a duplicate-drop.
     *
     * <p>Validates every {@code int} read from the file against
     * {@link #MAX_ENTRIES_PER_MAP_FILE} / {@link #MAX_PK_LEN} / {@code dim}
     * so a corrupt or partially-uploaded input cannot coerce the merger
     * into a giant allocation or a corrupt graph (review items B.1#2 and
     * B.2#3 from the first pr-reviewer pass).
     */
    private void accumulateAuthority(RemoteSegmentInput in, Path mapFile,
                                     BitSet tombstoneSet,
                                     Map<Bytes, AuthorityEntry> authority,
                                     long[] perInputCounters,
                                     int expectedDim) throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()),
                        DOWNLOAD_CHUNK_SIZE))) {
            int entryCount = dis.readInt();
            if (entryCount < 0 || entryCount > MAX_ENTRIES_PER_MAP_FILE) {
                throw new IOException("malformed map file " + mapFile
                        + ": entryCount " + entryCount
                        + " outside [0, " + MAX_ENTRIES_PER_MAP_FILE + "]");
            }
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                if (ordinal < 0) {
                    throw new IOException("malformed map file " + mapFile
                            + ": negative ordinal " + ordinal + " at entry " + i);
                }
                int pkLen = dis.readInt();
                if (pkLen < 0 || pkLen > MAX_PK_LEN) {
                    throw new IOException("malformed map file " + mapFile
                            + ": pkLen " + pkLen + " outside [0, " + MAX_PK_LEN
                            + "] at entry " + i);
                }
                byte[] pkBytes = new byte[pkLen];
                dis.readFully(pkBytes);
                int floatCount = dis.readInt();
                if (floatCount != expectedDim) {
                    // Dimension-mismatch: refuse to merge. Silently building a graph
                    // with mismatched-dimension vectors would corrupt the InlineVectors
                    // feature and tank recall on every search (review item B.1#2 from
                    // the first pr-reviewer pass).
                    throw new IOException("dimension mismatch in input " + mapFile
                            + " (segment " + in.segmentUuid + "): expected " + expectedDim
                            + ", got " + floatCount + " at entry " + i);
                }
                if (tombstoneSet.get(ordinal)) {
                    // Skip the floats; we still need to consume them to keep the stream aligned.
                    skipFully(dis, (long) floatCount * Float.BYTES);
                    perInputCounters[0]++;
                    continue;
                }
                Bytes pk = Bytes.from_array(pkBytes);
                AuthorityEntry existing = authority.get(pk);
                if (existing != null && existing.generation >= in.generation) {
                    // Earlier input already owns this PK at a higher (or equal — keep first
                    // observation as a deterministic tiebreak) generation. Skip and record.
                    skipFully(dis, (long) floatCount * Float.BYTES);
                    perInputCounters[1]++;
                    continue;
                }
                if (existing != null) {
                    // We are about to replace a lower-generation owner; the previous one
                    // becomes a "duplicate dropped".
                    perInputCounters[1]++;
                }
                VectorFloat<?> vec = VTS.createFloatVector(floatCount);
                for (int j = 0; j < floatCount; j++) {
                    int bits = dis.readInt();
                    vec.set(j, Float.intBitsToFloat(bits));
                }
                authority.put(pk, new AuthorityEntry(vec, in.generation));
            }
        }
    }

    private static final class AuthorityEntry {
        final VectorFloat<?> vector;
        final long generation;

        AuthorityEntry(VectorFloat<?> vector, long generation) {
            this.vector = vector;
            this.generation = generation;
        }
    }

    private static void skipFully(DataInputStream dis, long bytes) throws IOException {
        long remaining = bytes;
        while (remaining > 0L) {
            long skipped = dis.skip(remaining);
            if (skipped <= 0L) {
                // skip() returns 0 at EOF; surface as a clean error rather than spinning.
                if (dis.read() < 0) {
                    throw new IOException("unexpected EOF while skipping map-file padding ("
                            + bytes + " requested, " + (bytes - remaining) + " consumed)");
                }
                remaining--;
            } else {
                remaining -= skipped;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Graph build (in-memory) + on-disk write
    // -------------------------------------------------------------------------

    private static final class BuildArtefacts {
        final GraphIndexBuilder builder;
        final VectorStorage storage;
        final VectorStorageRandomAccessVectorValues ravv;
        final List<Bytes> ordinalToPk;

        BuildArtefacts(GraphIndexBuilder builder, VectorStorage storage,
                       VectorStorageRandomAccessVectorValues ravv, List<Bytes> ordinalToPk) {
            this.builder = builder;
            this.storage = storage;
            this.ravv = ravv;
            this.ordinalToPk = ordinalToPk;
        }
    }

    private BuildArtefacts buildGraph(Map<Bytes, AuthorityEntry> authority, int dim, int keptCount) {
        VectorStorage storage = new VectorStorage(keptCount);
        VectorStorageRandomAccessVectorValues ravv =
                new VectorStorageRandomAccessVectorValues(storage, dim, keptCount);
        BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarity);
        GraphIndexBuilder builder = new GraphIndexBuilder(
                bsp, dim, List.of(graphM), beamWidth, neighborOverflow, alpha,
                /* addHierarchy */ false, /* refineFinalGraph */ false,
                ForkJoinPool.commonPool(), ForkJoinPool.commonPool(), keptCount);
        List<Bytes> ordinalToPk = new ArrayList<>(keptCount);
        int ord = 0;
        LongBinaryOperator batchCb = batchListener;
        for (Map.Entry<Bytes, AuthorityEntry> e : authority.entrySet()) {
            ordinalToPk.add(e.getKey());
            storage.set(ord, e.getValue().vector);
            try {
                builder.addGraphNode(ord, e.getValue().vector);
            } catch (RuntimeException re) {
                // The builder is the plugin boundary (jvector). Add a clear
                // diagnostic hint rather than letting an opaque failure escape.
                throw new IllegalStateException(
                        "GraphIndexBuilder.addGraphNode failed at ordinal " + ord
                                + " (" + keptCount + " total)", re);
            }
            // Fire batch-progress callback every BATCH_PROGRESS_INTERVAL vectors
            // so the HTTP /status endpoint can show fine-grained build progress.
            if (batchCb != null && (ord % BATCH_PROGRESS_INTERVAL == 0)) {
                fireBatchProgress(batchCb, ord, keptCount);
            }
            ord++;
        }
        // Final batch-progress notification at 100%.
        if (batchCb != null) {
            fireBatchProgress(batchCb, keptCount, keptCount);
        }
        try {
            builder.cleanup();
        } catch (RuntimeException re) {
            throw new IllegalStateException("GraphIndexBuilder.cleanup failed in merger", re);
        }
        return new BuildArtefacts(builder, storage, ravv, ordinalToPk);
    }

    private void writeGraph(BuildArtefacts art, int dim, Path graphTempFile) throws IOException {
        OnHeapGraphIndex graph = (OnHeapGraphIndex) art.builder.getGraph();
        int shardSize = art.ordinalToPk.size();

        boolean useFusedPQ = shardSize >= MIN_VECTORS_FOR_FUSED_PQ;
        int pqSubspaces = Math.max(1, dim / 4);
        // Mirror PersistentVectorStore.getOrTrainPQ's defaults verbatim
        // (clusterCount=256, centerData=true) so the merged segment is
        // PQ-compatible with everything the IS-side writer produces.
        ProductQuantization pq;
        if (useFusedPQ) {
            // Task #3 (issue #503): emit a start log so operators can distinguish
            // "PQ K-means is running" from "process is hung / GC-stalled". The
            // jvector ProductQuantization.compute() runs K-means internally and
            // does not expose per-iteration callbacks, so we log start + elapsed.
            LOGGER.log(Level.INFO,
                    "PQ training starting: {0} vectors, dim={1}, subspaces={2},"
                            + " clusters=256 — this may take several minutes",
                    new Object[]{shardSize, dim, pqSubspaces});
            long pqStartNanos = System.nanoTime();
            pq = ProductQuantization.compute(art.ravv, pqSubspaces,
                    /* clusterCount */ 256, /* centerData */ true);
            long pqElapsedMs = (System.nanoTime() - pqStartNanos) / 1_000_000L;
            LOGGER.log(Level.INFO,
                    "PQ training complete in {0} ms ({1} subspaces, {2} clusters)",
                    new Object[]{pqElapsedMs, pqSubspaces, 256});
        } else {
            pq = null;
        }
        PQVectors pqv = (pq != null) ? pq.encodeAll(art.ravv, ForkJoinPool.commonPool()) : null;

        List<Feature> features = new ArrayList<>(2);
        if (useFusedPQ) {
            features.add(new FusedPQ(graph.maxDegree(), pq));
        }
        features.add(new InlineVectors(dim));
        try (RandomAccessOnDiskGraphIndexWriter writer =
                GraphWriterFactory.openWriter(graph, graphTempFile, shardSize, features)) {
            ImmutableGraphIndex.View view = graph.getView();
            EnumMap<FeatureId, IntFunction<Feature.State>> suppliers =
                    new EnumMap<>(FeatureId.class);
            if (useFusedPQ) {
                suppliers.put(FeatureId.FUSED_PQ,
                        ordinal -> new FusedPQ.State(view, pqv, ordinal));
            }
            suppliers.put(FeatureId.INLINE_VECTORS,
                    ordinal -> new InlineVectors.State(art.ravv.getVector(ordinal)));
            writer.write(suppliers);
        }
    }

    /**
     * Writes the map file in the exact same wire format that
     * {@code PersistentVectorStore.writeFusedPQMapDataToTempFile} produces, so
     * the indexing-service tier can reload the merged segment with no
     * format-detection logic.
     */
    private void writeMapFile(BuildArtefacts art, Path mapTempFile) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(
                new FileOutputStream(mapTempFile.toFile()), DOWNLOAD_CHUNK_SIZE);
             DataOutputStream dos = new DataOutputStream(bos)) {
            int entryCount = art.ordinalToPk.size();
            dos.writeInt(entryCount);
            for (int ord = 0; ord < entryCount; ord++) {
                Bytes pk = art.ordinalToPk.get(ord);
                byte[] pkBytes = pk.to_array();
                VectorFloat<?> vec = art.ravv.getVector(ord);
                if (vec == null) {
                    throw new IOException("merger map writer: null vector at ordinal " + ord);
                }
                dos.writeInt(ord);
                dos.writeInt(pkBytes.length);
                dos.write(pkBytes);
                int floatCount = vec.length();
                dos.writeInt(floatCount);
                for (int j = 0; j < floatCount; j++) {
                    dos.writeInt(Float.floatToIntBits(vec.get(j)));
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Internal callback helpers
    // -------------------------------------------------------------------------

    /**
     * Fires the phase-change listener if one is set. No-op when
     * {@link #phaseListener} is {@code null}.
     */
    private void notifyPhase(String phase) {
        Consumer<String> cb = phaseListener;
        if (cb != null) {
            cb.accept(phase);
        }
    }
}
