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

import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.FusedPQ;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.IntFunction;
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
 * <p>This class is stateless and thread-hostile (every {@link #merge} call
 * builds its own state and tears it down before returning). The caller is
 * responsible for serialising calls if it really wants to.
 */
public final class RemoteSegmentGraphMerger {

    private static final Logger LOGGER = Logger.getLogger(RemoteSegmentGraphMerger.class.getName());

    /**
     * Mirror of the constant in {@code PersistentVectorStore}: shards smaller
     * than this are written without FusedPQ (PQ training cost outweighs
     * scoring benefit for tiny graphs, and the InlineVectors path covers
     * search just fine). Kept at the same value for behavioural parity.
     */
    private static final int MIN_VECTORS_FOR_FUSED_PQ = 64 * 1024;

    /** Block size used for streaming downloads via the multipart reader. */
    private static final int DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024;

    private static final VectorTypeSupport VTS =
            VectorizationProvider.getInstance().getVectorTypeSupport();

    private final DataStorageManager dataStorageManager;
    private final Path tmpDirectory;
    private final int graphM;
    private final int beamWidth;
    private final float neighborOverflow;
    private final float alpha;
    private final VectorSimilarityFunction similarity;

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

        public RemoteSegmentInput(String tablespaceUuid, String indexUuid, String segmentUuid,
                                  long segmentId, long mapFileSize, long generation,
                                  int[] tombstonedOrdinals) {
            this.tablespaceUuid = Objects.requireNonNull(tablespaceUuid, "tablespaceUuid");
            this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
            this.segmentUuid = Objects.requireNonNull(segmentUuid, "segmentUuid");
            this.segmentId = segmentId;
            this.mapFileSize = mapFileSize;
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

        public MergeOutput(String tablespaceUuid, String indexUuid, long segmentId,
                           String graphPath, long graphFileSize,
                           String mapPath, long mapFileSize,
                           long vectorCount, long droppedTombstones, long droppedDuplicates) {
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

        long startNanos = System.nanoTime();

        // 1. Stream each input's map file to a local temp file. We never hold
        //    every map in memory — even for a 1M-vector merge that would be
        //    a few GiB. The temp files are all deleted at the end of merge().
        List<Path> mapTempFiles = new ArrayList<>(inputs.size());
        long droppedTombstones = 0;
        long droppedDuplicates = 0;
        try {
            for (RemoteSegmentInput in : inputs) {
                mapTempFiles.add(downloadMapFile(in));
            }

            // 2. First pass: walk every map file and decide which (pk, vec) to keep.
            //    Authority map: pk -> (generation, vector). Higher generation wins.
            //    PERF: the inner reads are sequential against the BufferedInputStream;
            //    the de-duplication HashMap is bounded by the union of input PKs (which
            //    is also the upper bound on the merged segment's vector count).
            Map<Bytes, AuthorityEntry> authority = new HashMap<>();
            for (int i = 0; i < inputs.size(); i++) {
                RemoteSegmentInput in = inputs.get(i);
                Path mapFile = mapTempFiles.get(i);
                BitSet tombstoneSet = buildTombstoneSet(in.tombstonedOrdinals);
                long[] perInputCounters = new long[2]; // [tombstones, duplicates]
                accumulateAuthority(in, mapFile, tombstoneSet, authority, perInputCounters);
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

            // 4. Write the graph and map files locally.
            Path graphTempFile = Files.createTempFile(tmpDirectory,
                    "herddb-merger-graph-", ".idx");
            Path mapOutTempFile = Files.createTempFile(tmpDirectory,
                    "herddb-merger-map-", ".tmp");
            boolean uploadedGraph = false;
            boolean uploadedMap = false;
            String graphPath = null;
            String mapPath = null;
            long graphSize;
            long mapSize;
            String multipartUuid = outputIndexUuid + "_seg" + outputSegmentId;
            try {
                writeGraph(artefacts, dim, graphTempFile);
                graphSize = Files.size(graphTempFile);
                writeMapFile(artefacts, mapOutTempFile);
                mapSize = Files.size(mapOutTempFile);

                // 5. Upload both. If the second upload fails we delete the first
                //    so we don't leak partial output.
                graphPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "graph",
                        graphTempFile, /* progress */ null);
                uploadedGraph = true;
                mapPath = dataStorageManager.writeMultipartIndexFile(
                        outputTablespaceUuid, multipartUuid, "map",
                        mapOutTempFile, /* progress */ null);
                uploadedMap = true;
            } finally {
                Files.deleteIfExists(graphTempFile);
                Files.deleteIfExists(mapOutTempFile);
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

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            LOGGER.log(Level.INFO,
                    "RemoteSegmentGraphMerger: merged {0} inputs into segment {1}/{2}_seg{3}"
                            + " ({4} kept, {5} tombstoned, {6} duplicates dropped, {7} ms)",
                    new Object[]{inputs.size(), outputTablespaceUuid, outputIndexUuid,
                            outputSegmentId, keptCount, droppedTombstones, droppedDuplicates,
                            elapsedMs});
            return new MergeOutput(outputTablespaceUuid, outputIndexUuid, outputSegmentId,
                    graphPath, graphSize, mapPath, mapSize,
                    keptCount, droppedTombstones, droppedDuplicates);
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
     */
    private void accumulateAuthority(RemoteSegmentInput in, Path mapFile,
                                     BitSet tombstoneSet,
                                     Map<Bytes, AuthorityEntry> authority,
                                     long[] perInputCounters) throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new BufferedInputStream(new FileInputStream(mapFile.toFile()),
                        DOWNLOAD_CHUNK_SIZE))) {
            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int ordinal = dis.readInt();
                int pkLen = dis.readInt();
                if (pkLen < 0) {
                    throw new IOException("malformed map file " + mapFile + ": negative pkLen " + pkLen);
                }
                byte[] pkBytes = new byte[pkLen];
                dis.readFully(pkBytes);
                int floatCount = dis.readInt();
                if (floatCount < 0) {
                    throw new IOException("malformed map file " + mapFile + ": negative floatCount " + floatCount);
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
            ord++;
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
        ProductQuantization pq = useFusedPQ
                ? ProductQuantization.compute(art.ravv, pqSubspaces,
                        /* clusterCount */ 256, /* centerData */ true)
                : null;
        PQVectors pqv = (pq != null) ? pq.encodeAll(art.ravv, ForkJoinPool.commonPool()) : null;

        OnDiskGraphIndexWriter.Builder writerBuilder = new OnDiskGraphIndexWriter.Builder(
                graph, graphTempFile);
        if (useFusedPQ) {
            writerBuilder.with(new FusedPQ(graph.maxDegree(), pq));
        }
        try (OnDiskGraphIndexWriter writer = writerBuilder
                .with(new InlineVectors(dim))
                .build()) {
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
}
