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
package herddb.indexing.optimizer;

import herddb.index.vector.RemoteSegmentGraphMerger;
import herddb.indexing.segment.SegmentMetadata;
import herddb.indexing.segment.SegmentState;
import herddb.indexing.segment.TombstoneOverlay;
import herddb.indexing.segment.TombstoneOverlayManager;
import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Production {@link SegmentMerger} for the index-optimizer service (issue #484).
 *
 * <p>Wraps {@link RemoteSegmentGraphMerger} (the actual graph rebuild) and
 * handles the optimizer-side concerns it doesn't know about:
 * <ul>
 *   <li>Loading each input's latest {@code TombstoneOverlay} via
 *       {@link TombstoneOverlayManager#loadOverlay} when the segment carries
 *       a {@code tombstonePath} + a non-zero {@code overlayGeneration}, so
 *       tombstoned ordinals are dropped during the merge.</li>
 *   <li>Inferring the merged segment's {@code (vectorDimension)} from the
 *       inputs (we trust they all share a dimension; mismatches throw).</li>
 *   <li>Allocating a fresh {@code segmentId} as a 63-bit non-negative random
 *       long so the merger's outputs never collide with IS-allocated ids.</li>
 *   <li>Building the output {@link SegmentMetadata}: ACTIVE, fresh UUID,
 *       generation = {@code max(input.generation) + 1}, base LSN = the
 *       latest of the inputs', sizeBytes/vectorCount from the merger output,
 *       no tombstones (the merge applied them all already).</li>
 *   <li>Implementing {@link #abandon(SegmentMetadata)} via
 *       {@link RemoteSegmentGraphMerger#deleteOutput} so an aborted merge
 *       doesn't leak multipart artefacts.</li>
 * </ul>
 *
 * <p>This class is intentionally not registered via {@code ServiceLoader}.
 * The optimizer's bootstrap ({@link IndexOptimizerMain}) instantiates it
 * directly because it requires runtime-supplied state (a
 * {@link DataStorageManager} bound to remote storage, a tmp directory). The
 * SPI mechanism is preserved for unit tests that want to plug in a
 * synthetic merger via {@code META-INF/services}.
 */
public final class RemoteSegmentMerger implements SegmentMerger {

    private static final Logger LOGGER = Logger.getLogger(RemoteSegmentMerger.class.getName());

    /**
     * Vector dimension used by the merger when re-building the graph. Must
     * match the IS-side index dimension; the optimizer reads it from
     * configuration because the SegmentMetadata znode does not carry the
     * dimension as a first-class field.
     */
    private final int dim;
    private final RemoteSegmentGraphMerger graphMerger;
    private final DataStorageManager dataStorageManager;
    /** Cumulative count of merge invocations — exposed for observability tests. */
    private final AtomicLong invocations = new AtomicLong();

    /**
     * @param dataStorageManager  remote-backed DSM the merger uses to download inputs and
     *                            upload the merged graph + map files
     * @param tmpDirectory        local scratch directory for staging
     * @param dim                 vector dimension of the index being merged
     * @param graphM              jvector graph degree
     * @param beamWidth           jvector beam width during build
     * @param neighborOverflow    jvector neighbor-overflow factor
     * @param alpha               jvector alpha factor
     * @param similarity          vector similarity function (must match the IS that produced the inputs)
     */
    public RemoteSegmentMerger(DataStorageManager dataStorageManager,
                               Path tmpDirectory,
                               int dim,
                               int graphM,
                               int beamWidth,
                               float neighborOverflow,
                               float alpha,
                               VectorSimilarityFunction similarity) {
        this.dataStorageManager = Objects.requireNonNull(dataStorageManager, "dataStorageManager");
        if (dim <= 0) {
            throw new IllegalArgumentException("dim must be positive: " + dim);
        }
        this.dim = dim;
        this.graphMerger = new RemoteSegmentGraphMerger(
                dataStorageManager, tmpDirectory, graphM, beamWidth,
                neighborOverflow, alpha, similarity);
    }

    public long getInvocationCount() {
        return invocations.get();
    }

    @Override
    public SegmentMetadata merge(List<SegmentMetadata> inputs, int newOwnerInstance) throws Exception {
        Objects.requireNonNull(inputs, "inputs");
        if (inputs.isEmpty()) {
            return null;
        }
        invocations.incrementAndGet();

        SegmentMetadata sample = inputs.get(0);
        String tablespaceUuid = sample.getTablespaceUuid();
        String indexUuid = sample.getIndexUuid();
        for (SegmentMetadata m : inputs) {
            if (!tablespaceUuid.equals(m.getTablespaceUuid())) {
                throw new IllegalArgumentException(
                        "merger inputs disagree on tablespaceUuid: "
                                + tablespaceUuid + " vs " + m.getTablespaceUuid());
            }
            if (!indexUuid.equals(m.getIndexUuid())) {
                throw new IllegalArgumentException(
                        "merger inputs disagree on indexUuid: "
                                + indexUuid + " vs " + m.getIndexUuid());
            }
            if (m.getSegmentId() == SegmentMetadata.NO_SEGMENT_ID) {
                throw new IllegalArgumentException(
                        "merger input " + m.getSegmentUuid()
                                + " has no segmentId — cannot reconstruct multipart path");
            }
        }

        // Translate every SegmentMetadata input into a RemoteSegmentInput,
        // pre-loading the latest TombstoneOverlay where the znode points at one.
        List<RemoteSegmentGraphMerger.RemoteSegmentInput> graphInputs = new ArrayList<>(inputs.size());
        long maxGeneration = 0L;
        long maxMapFileSize = 0L;
        LogSequenceNumber latestBaseLsn = null;
        for (SegmentMetadata m : inputs) {
            int[] tombstoned = loadTombstonedOrdinalsBestEffort(m);
            // The optimizer needs the EXACT map-file size to drive the
            // multipart reader. Production readers (RemoteRandomAccessReader,
            // SegmentedMappedReader) treat the size passed at construction
            // as authoritative — they don't probe the underlying object —
            // and the direct-multipart-download path computes block count
            // from that size and throws "Block N not found" past the actual
            // file end (review item B.1.1 from the third pr-reviewer pass).
            // We therefore refuse to merge any input whose znode predates
            // the {@code mapFileSize} field (issue #484): a clean failure
            // mode is much better than a half-read map file producing a
            // corrupt graph. The next IS checkpoint will rewrite the znode
            // with {@code mapFileSize} set and unstick the segment.
            if (m.getMapFileSize() <= 0L) {
                throw new LegacyMetadataException(m.getSegmentUuid(),
                        m.getSizeBytes(),
                        "input segment " + m.getSegmentUuid() + " has no mapFileSize"
                                + " (legacy znode predating issue #484);"
                                + " skipping merge until IS recheckpoints the segment");
            }
            long mapFileSizeHint = m.getMapFileSize();
            graphInputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    m.getTablespaceUuid(), m.getIndexUuid(), m.getSegmentUuid(),
                    m.getSegmentId(), mapFileSizeHint, m.getGeneration(),
                    tombstoned));
            if (m.getGeneration() > maxGeneration) {
                maxGeneration = m.getGeneration();
            }
            if (mapFileSizeHint > maxMapFileSize) {
                maxMapFileSize = mapFileSizeHint;
            }
            LogSequenceNumber base = m.baseLsn();
            if (base != null && (latestBaseLsn == null || compareLsn(base, latestBaseLsn) > 0)) {
                latestBaseLsn = base;
            }
        }

        long outputSegmentId = newRandomSegmentId();
        RemoteSegmentGraphMerger.MergeOutput output = graphMerger.merge(
                graphInputs, tablespaceUuid, indexUuid, outputSegmentId, dim);
        if (output == null) {
            // The graph merger declined — every input vector was tombstoned or duplicated.
            return null;
        }

        String mergedUuid = UUID.randomUUID().toString();
        SegmentMetadata.Builder builder = SegmentMetadata.builder()
                .segmentUuid(mergedUuid)
                .tablespaceUuid(tablespaceUuid)
                .tableName(sample.getTableName())
                .indexUuid(indexUuid)
                .indexName(sample.getIndexName())
                .state(SegmentState.ACTIVE)
                .ownerInstanceId(newOwnerInstance)
                .pendingOwnerInstanceId(SegmentMetadata.NO_INSTANCE)
                .segmentId(outputSegmentId)
                .graphPath(output.graphPath)
                .mapPath(output.mapPath)
                // The merged segment starts with no tombstones. Any future
                // deletes against the merged segment will produce a fresh
                // overlay starting at generation 1 via the IS-side path.
                .tombstonePath(null)
                .tombstoneLsn(SegmentMetadata.NO_LSN_LEDGER_ID, SegmentMetadata.NO_LSN_OFFSET)
                .overlayGeneration(0L)
                .sizeBytes(output.totalSizeBytes())
                // Populate the new mapFileSize field (issue #484 round 2)
                // so any future merge of this output goes straight through
                // the modern multipart-reader code path.
                .mapFileSize(output.mapFileSize)
                .vectorCount(output.vectorCount)
                .generation(maxGeneration + 1L)
                .createdAtEpochMillis(System.currentTimeMillis());
        if (latestBaseLsn != null) {
            builder.baseLsn(latestBaseLsn);
        }
        SegmentMetadata merged = builder.build();
        LOGGER.log(Level.INFO,
                "RemoteSegmentMerger: produced segment {0} (segmentId={1}, generation={2},"
                        + " {3} vectors, {4} bytes, droppedTombstones={5}, droppedDuplicates={6})",
                new Object[]{mergedUuid, outputSegmentId, maxGeneration + 1L,
                        output.vectorCount, output.totalSizeBytes(),
                        output.droppedTombstones, output.droppedDuplicates});
        return merged;
    }

    @Override
    public void abandon(SegmentMetadata produced) {
        if (produced == null || produced.getSegmentId() == SegmentMetadata.NO_SEGMENT_ID) {
            return;
        }
        // Reconstruct the MergeOutput just enough to drive the cleanup. The
        // file sizes / vector counts are irrelevant to deleteOutput, which
        // only needs (tablespaceUuid, indexUuid, segmentId).
        graphMerger.deleteOutput(new RemoteSegmentGraphMerger.MergeOutput(
                produced.getTablespaceUuid(), produced.getIndexUuid(), produced.getSegmentId(),
                produced.getGraphPath(), 0L,
                produced.getMapPath(), 0L,
                produced.getVectorCount(), 0L, 0L));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Bounded probe window for loading legacy ({@code overlayGeneration == 0})
     * znodes (review item B.1#1 from the first pr-reviewer pass). Mirrors the
     * window used by
     * {@code IndexOptimizerEngine.deleteSegmentFilesBestEffort} when reaping
     * pre-overlayGeneration znodes; bounded so a misbehaving znode does not
     * stall the merge with O(N) DSM round-trips.
     */
    static final long LEGACY_OVERLAY_GENERATION_PROBE_MAX = 32L;

    private int[] loadTombstonedOrdinalsBestEffort(SegmentMetadata m) {
        if (m.getTombstonePath() == null) {
            return new int[0];
        }
        long generation = m.getOverlayGeneration();
        if (generation > 0L) {
            // Modern znode (post-issue #484): the generation is recorded
            // explicitly, so we load exactly that file.
            try {
                TombstoneOverlay overlay = TombstoneOverlayManager.loadOverlay(
                        dataStorageManager, m.getTablespaceUuid(), m.getIndexUuid(),
                        m.getSegmentUuid(), generation);
                return overlay.getTombstonedOrdinals();
            } catch (IOException | DataStorageManagerException e) {
                // A corrupt or unreachable overlay must NOT silently turn into
                // "no tombstones applied" — that would resurrect deleted vectors
                // after the merge. Surface the error so the engine logs it,
                // bumps mergeFailuresTotal, and retries next tick.
                throw new TombstoneLoadFailedException(m.getSegmentUuid(), generation, e);
            }
        }
        // Legacy znode (overlayGeneration unset, but tombstonePath != null —
        // i.e. written by an IS that pre-dates the overlayGeneration field).
        // Probe the bounded window 32..1 highest-first; the highest existing
        // generation is the authoritative one because TombstoneOverlayManager
        // deletes the previous generation immediately after a successful flush.
        for (long gen = LEGACY_OVERLAY_GENERATION_PROBE_MAX; gen >= 1L; gen--) {
            try {
                TombstoneOverlay overlay = TombstoneOverlayManager.loadOverlay(
                        dataStorageManager, m.getTablespaceUuid(), m.getIndexUuid(),
                        m.getSegmentUuid(), gen);
                LOGGER.log(Level.INFO,
                        "loaded legacy tombstone overlay for segment {0} via probe (generation={1})",
                        new Object[]{m.getSegmentUuid(), gen});
                return overlay.getTombstonedOrdinals();
            } catch (IOException | DataStorageManagerException probeMiss) {
                // Per-generation miss is normal during the descending probe; only
                // the highest existing one survives. Continue.
            }
        }
        // tombstonePath is set but no overlay file exists in the probed window.
        // We MUST refuse the merge — silently treating this as "no tombstones"
        // would resurrect deleted vectors. The engine will log mergeFailures
        // and retry; an operator can then inspect the segment state.
        throw new TombstoneLoadFailedException(m.getSegmentUuid(), 0L,
                new IOException("legacy znode has tombstonePath=" + m.getTombstonePath()
                        + " but no overlay file found in probe window 1.."
                        + LEGACY_OVERLAY_GENERATION_PROBE_MAX));
    }

    private static long newRandomSegmentId() {
        // Full 63-bit non-negative random id. Collisions across the optimizer
        // pod's lifetime are negligible (~2^-63 per allocation) and the
        // leader-lock already serialises optimizer instances, so two pods
        // can't allocate concurrently in production.
        return UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
    }

    private static int compareLsn(LogSequenceNumber a, LogSequenceNumber b) {
        if (a.ledgerId != b.ledgerId) {
            return Long.compare(a.ledgerId, b.ledgerId);
        }
        return Long.compare(a.offset, b.offset);
    }

    /**
     * Thrown when the merger cannot load a tombstone overlay it needs to
     * apply during a merge. This is a fatal error for the run — we'd rather
     * decline the merge than publish a segment that resurrects tombstoned
     * vectors.
     */
    public static final class TombstoneLoadFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public TombstoneLoadFailedException(String segmentUuid, long overlayGeneration, Throwable cause) {
            super("failed to load tombstone overlay generation " + overlayGeneration
                    + " for segment " + segmentUuid + " — refusing to merge to avoid"
                    + " resurrecting deleted vectors", cause);
        }
    }

    /**
     * Thrown when the merger encounters an input segment whose znode predates
     * issue #484 (i.e. {@code mapFileSize <= 0}). Production storage readers
     * treat the size passed at construction as authoritative and would happily
     * over-read past the actual map-file end, producing torn graphs and
     * "Block N not found" errors deeper in the multipart pipeline. Refusing
     * the merge lets the engine bump {@code mergeFailuresTotal} and proceed
     * to the next index; the next IS checkpoint will rewrite the legacy
     * znode with {@code mapFileSize} set, unsticking the segment.
     */
    public static final class LegacyMetadataException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public LegacyMetadataException(String segmentUuid, long sizeBytes, String message) {
            super(message + " (segmentUuid=" + segmentUuid
                    + ", combinedSizeBytes=" + sizeBytes + ")");
        }
    }
}
