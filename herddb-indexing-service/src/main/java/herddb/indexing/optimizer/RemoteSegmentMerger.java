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
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Production {@link SegmentMerger} for the index-optimizer service (issue #484,
 * issue #516).
 *
 * <p>Wraps {@link RemoteSegmentGraphMerger} (the actual graph rebuild) and
 * handles the optimizer-side concerns it doesn't know about:
 * <ul>
 *   <li>Resolving per-index jvector build parameters via
 *       {@link IndexMergeConfigProvider} at merge time (issue #516). A new
 *       {@link RemoteSegmentGraphMerger} is created for each merge call using
 *       the resolved config, so indexes with different dimensions, similarity
 *       functions, or graph degrees are all handled correctly.</li>
 *   <li>Loading each input's latest {@code TombstoneOverlay} via
 *       {@link TombstoneOverlayManager#loadOverlay} when the segment carries
 *       a {@code tombstonePath} + a non-zero {@code overlayGeneration}, so
 *       tombstoned ordinals are dropped during the merge.</li>
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
    private static final MemoryMXBean MEMORY_MX = ManagementFactory.getMemoryMXBean();

    private final DataStorageManager dataStorageManager;
    private final Path tmpDirectory;
    private final IndexMergeConfigProvider configProvider;

    /** Cumulative count of merge invocations — exposed for observability tests. */
    private final AtomicLong invocations = new AtomicLong();

    /**
     * The graph merger built for the most recent {@link #merge} call. Kept
     * so that:
     * <ul>
     *   <li>{@link #getLastMergeTimings()} can delegate to it from the HTTP
     *       server thread after the merge returns.</li>
     *   <li>{@link #setMergeProgress} applied before the merge call is
     *       forwarded to the freshly-constructed merger inside {@link #merge}.</li>
     * </ul>
     * Written inside {@link #merge} (single-threaded scheduler); read by the
     * HTTP server thread — {@code volatile} ensures visibility.
     */
    private volatile RemoteSegmentGraphMerger lastGraphMerger;

    /**
     * Progress holder set by the engine before each {@link #merge} call and
     * cleared (set to {@code null}) in the engine's {@code finally} block
     * after the call returns. Stored here so it can be wired into the
     * per-call {@link RemoteSegmentGraphMerger} that is created inside
     * {@link #merge}. Written and read on the single-threaded scheduler —
     * {@code volatile} for visibility to the HTTP server thread.
     */
    private volatile MergeProgress pendingProgress;

    /**
     * @param dataStorageManager  remote-backed DSM the merger uses to download inputs and
     *                            upload the merged graph + map files
     * @param tmpDirectory        local scratch directory for staging
     * @param configProvider      source of per-index jvector build parameters (issue #516)
     */
    public RemoteSegmentMerger(DataStorageManager dataStorageManager,
                               Path tmpDirectory,
                               IndexMergeConfigProvider configProvider) {
        this.dataStorageManager = Objects.requireNonNull(dataStorageManager, "dataStorageManager");
        this.tmpDirectory = Objects.requireNonNull(tmpDirectory, "tmpDirectory");
        this.configProvider = Objects.requireNonNull(configProvider, "configProvider");
    }

    public long getInvocationCount() {
        return invocations.get();
    }

    /**
     * Sets the live-state holder that receives phase-change and batch-progress
     * callbacks during the next {@link #merge} call. Pass {@code null} to stop
     * forwarding. Must be called from the same thread that calls {@link #merge}.
     *
     * <p>The progress object is stored and then applied to the
     * {@link RemoteSegmentGraphMerger} that is created at the start of each
     * {@link #merge} call, so this method may be called before or after
     * previous merges without risk of cross-contamination.
     */
    public void setMergeProgress(MergeProgress progress) {
        this.pendingProgress = progress;
    }

    /**
     * Returns the per-phase timing breakdown of the last completed merge
     * (delegated from the most recently created {@link RemoteSegmentGraphMerger}),
     * or {@code null} if no merge has been performed yet.
     */
    public RemoteSegmentGraphMerger.MergePhaseTimings getLastMergeTimings() {
        RemoteSegmentGraphMerger last = lastGraphMerger;
        return last != null ? last.getLastMergeTimings() : null;
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

        // Resolve per-index config from the provider. This may call the IS gRPC
        // API on a cache-miss; any Exception bubbles to the engine which logs
        // it and skips this index for the tick.
        IndexMergeConfig config = configProvider.getMergeConfig(
                tablespaceUuid,
                sample.getTableName(),
                sample.getIndexName(),
                indexUuid);

        // Build a fresh RemoteSegmentGraphMerger for this merge using the
        // per-index config. Construction is cheap (a few field assignments);
        // the DataStorageManager is shared.
        RemoteSegmentGraphMerger graphMerger = new RemoteSegmentGraphMerger(
                dataStorageManager, tmpDirectory,
                config.graphM, config.beamWidth,
                config.neighborOverflow, config.alpha,
                config.similarity);
        this.lastGraphMerger = graphMerger;

        // Wire progress callbacks before calling merge.
        MergeProgress progress = pendingProgress;
        if (progress != null) {
            graphMerger.setPhaseListener(progress::updatePhase);
            graphMerger.setBatchListener((written, total) -> {
                progress.updateBatchProgress(written, total);
                return 0L;
            });
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
            // Issue #485: derive graphFileSize from the existing znode fields
            // (sizeBytes = graphFileSize + mapFileSize per
            // PersistentVectorStore.SegmentWriteResult). The streaming merge
            // path needs the exact graph size to drive the multipart reader;
            // the legacy in-memory path ignores it. Compute and pass through
            // unconditionally — the dispatcher in RemoteSegmentGraphMerger.merge
            // chooses the path.
            long graphFileSizeHint = m.getSizeBytes() - m.getMapFileSize();
            if (graphFileSizeHint <= 0L) {
                throw new LegacyMetadataException(m.getSegmentUuid(),
                        m.getSizeBytes(),
                        "input segment " + m.getSegmentUuid() + " has invalid"
                                + " graphFileSize=" + graphFileSizeHint
                                + " (sizeBytes=" + m.getSizeBytes()
                                + " mapFileSize=" + m.getMapFileSize()
                                + "); skipping merge until IS recheckpoints the segment");
            }
            graphInputs.add(new RemoteSegmentGraphMerger.RemoteSegmentInput(
                    m.getTablespaceUuid(), m.getIndexUuid(), m.getSegmentUuid(),
                    m.getSegmentId(), mapFileSizeHint, graphFileSizeHint, m.getGeneration(),
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

        // Peek the vector dimension from the first input's map file. The map file
        // embeds floatCount (= dim) per entry; reading the first entry's header
        // avoids any dependency on the static index metadata (Index.properties
        // does not carry dim — it is embedded in the map file binary format).
        // By this point all inputs have been validated to have mapFileSize > 0.
        int dim = peekDimFromMapFile(sample);

        // Log JVM memory snapshot + estimated /tmp usage at the start of each
        // merge so operators can sanity-check container headroom (issue #503).
        logMemoryBudgetAtMergeStart(inputs, tablespaceUuid, indexUuid, outputSegmentId,
                config.graphM, dim);

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
        RemoteSegmentGraphMerger last = lastGraphMerger;
        if (last == null) {
            // No merge was ever attempted on this merger instance (should not
            // happen in production since abandon is called after merge).
            LOGGER.log(Level.WARNING,
                    "abandon called on RemoteSegmentMerger with no prior merge "
                            + "(segment={0}); cannot clean up multipart files",
                    produced.getSegmentUuid());
            return;
        }
        last.deleteOutput(new RemoteSegmentGraphMerger.MergeOutput(
                produced.getTablespaceUuid(), produced.getIndexUuid(), produced.getSegmentId(),
                produced.getGraphPath(), 0L,
                produced.getMapPath(), 0L,
                produced.getVectorCount(), 0L, 0L));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Logs the JVM memory snapshot and estimated /tmp usage at merge start so
     * operators can sanity-check whether the available headroom is sufficient.
     */
    private void logMemoryBudgetAtMergeStart(List<SegmentMetadata> inputs,
                                              String tablespaceUuid,
                                              String indexUuid,
                                              long outputSegmentId,
                                              int graphM,
                                              int dim) {
        long totalVectors = 0;
        long totalInputSizeBytes = 0;
        for (SegmentMetadata m : inputs) {
            totalVectors += m.getVectorCount();
            totalInputSizeBytes += m.getSizeBytes();
        }
        long heapUsedMb  = MEMORY_MX.getHeapMemoryUsage().getUsed()  / (1024 * 1024);
        long heapMaxMb   = MEMORY_MX.getHeapMemoryUsage().getMax()   / (1024 * 1024);
        long directBytes = PlatformDependent.usedDirectMemory();
        long directMaxBytes = PlatformDependent.maxDirectMemory();
        long directUsedMb = directBytes >= 0 ? directBytes / (1024 * 1024) : -1L;
        long directMaxMb  = directMaxBytes >= 0 ? directMaxBytes / (1024 * 1024) : -1L;

        // Estimated /tmp disk usage for graph + map files.
        long estimatedGraphBytes = (long) (totalVectors * graphM * 4 * 1.5);
        long estimatedMapBytes   = totalVectors * ((long) dim * 4 + 24);
        long estimatedTmpGib     = (estimatedGraphBytes + estimatedMapBytes) / (1024 * 1024 * 1024);

        long cgroupLimitGib = readCgroupMemoryLimitGib();

        LOGGER.log(Level.INFO,
                "Starting merge: {0} inputs, {1} vectors, dim={2},"
                        + " indexUuid={3}, outputSegmentId={4},"
                        + " inputSizeBytes={5}",
                new Object[]{inputs.size(), totalVectors, dim,
                        indexUuid, outputSegmentId, totalInputSizeBytes});
        LOGGER.log(Level.INFO,
                "  estimated /tmp usage: graph≈{0} GiB, map≈{1} GiB, total≈{2} GiB",
                new Object[]{estimatedGraphBytes / (1024 * 1024 * 1024),
                        estimatedMapBytes / (1024 * 1024 * 1024),
                        estimatedTmpGib});
        LOGGER.log(Level.INFO,
                "  JVM heap: {0} MiB used / {1} MiB max",
                new Object[]{heapUsedMb, heapMaxMb});
        LOGGER.log(Level.INFO,
                "  JVM direct: {0} MiB used / {1} MiB max (Netty PlatformDependent)",
                new Object[]{directUsedMb, directMaxMb});
        if (cgroupLimitGib >= 0) {
            LOGGER.log(Level.INFO,
                    "  container memory limit: {0} GiB (from /sys/fs/cgroup/memory.max)",
                    cgroupLimitGib);
        }
    }

    /**
     * Reads the container memory limit from the cgroup v2 file
     * {@code /sys/fs/cgroup/memory.max}. Returns -1 when the file is not
     * accessible (non-Linux hosts, cgroup v1) or when its value is
     * {@code "max"} (unlimited). Best-effort: any I/O failure is swallowed.
     */
    private static long readCgroupMemoryLimitGib() {
        try {
            java.nio.file.Path p = java.nio.file.Paths.get("/sys/fs/cgroup/memory.max");
            if (!java.nio.file.Files.exists(p)) {
                return -1L;
            }
            String raw = new String(java.nio.file.Files.readAllBytes(p),
                    java.nio.charset.StandardCharsets.UTF_8).trim();
            if ("max".equalsIgnoreCase(raw)) {
                return -1L;
            }
            return Long.parseLong(raw) / (1024L * 1024 * 1024);
        } catch (IOException | NumberFormatException e) {
            // Best-effort: silently swallow; the log line is just advisory.
            return -1L;
        }
    }

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
     * Peeks the vector dimension from the first entry of the given segment's
     * map file. The map file binary format is:
     * <pre>
     *   int  entryCount
     *   // for each entry:
     *   int  ordinal
     *   int  pkLen
     *   byte[pkLen] pk
     *   int  floatCount  (= dim)
     *   int[floatCount] floatBits  (not read here)
     * </pre>
     * Only the first entry's header is read (at most {@code 16 + pkLen} bytes).
     * This avoids downloading the full map file just to discover the dimension.
     *
     * <p>The caller must ensure the segment's {@code mapFileSize > 0} and that
     * the map file contains at least one entry (i.e. the segment is non-empty).
     *
     * @throws IOException                 on remote I/O failures
     * @throws DataStorageManagerException on storage-layer errors
     * @throws IllegalStateException       if the map file is empty or the first
     *                                     entry contains an implausible dimension
     */
    int peekDimFromMapFile(SegmentMetadata m)
            throws IOException, DataStorageManagerException {
        String multipartUuid = m.getIndexUuid() + "_seg" + m.getSegmentId();
        ReaderSupplier supplier = dataStorageManager.multipartIndexReaderSupplier(
                m.getTablespaceUuid(), multipartUuid, "map", m.getMapFileSize());
        try (RandomAccessReader reader = supplier.get()) {
            reader.seek(0L);
            int entryCount = reader.readInt();
            if (entryCount <= 0) {
                throw new IllegalStateException(
                        "Cannot peek dim from map file of segment " + m.getSegmentUuid()
                        + ": entryCount=" + entryCount
                        + " (empty or corrupt map file)");
            }
            reader.readInt(); // ordinal (skip)
            int pkLen = reader.readInt();
            if (pkLen < 0 || pkLen > 65_536) {
                throw new IllegalStateException(
                        "Cannot peek dim from map file of segment " + m.getSegmentUuid()
                        + ": pkLen=" + pkLen + " (out of range, likely corrupt)");
            }
            // Skip the primary-key bytes.
            byte[] pk = new byte[pkLen];
            reader.readFully(pk);
            int floatCount = reader.readInt();
            if (floatCount <= 0 || floatCount > 65_536) {
                throw new IllegalStateException(
                        "Cannot peek dim from map file of segment " + m.getSegmentUuid()
                        + ": floatCount=" + floatCount
                        + " (out of range, likely corrupt)");
            }
            return floatCount;
        }
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
