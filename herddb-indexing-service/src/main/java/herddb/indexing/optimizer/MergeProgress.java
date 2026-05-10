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

import io.netty.util.internal.PlatformDependent;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

/**
 * Thread-safe (via {@code volatile} fields) live state of the current or most
 * recent merge cycle. Written by the single-threaded optimizer scheduler;
 * read concurrently by the HTTP server thread for {@code GET /status}.
 *
 * <p>All fields are {@code volatile} so a plain read from the HTTP thread sees
 * a consistent, up-to-date value without synchronisation overhead. There is no
 * atomicity guarantee across <em>multiple</em> fields — the snapshot captures a
 * point-in-time picture that may straddle two sequential writes, but for a
 * monitoring endpoint this is acceptable.
 */
public final class MergeProgress {

    private static final MemoryMXBean MEMORY_MX = ManagementFactory.getMemoryMXBean();
    private static final List<GarbageCollectorMXBean> GC_BEANS =
            ManagementFactory.getGarbageCollectorMXBeans();

    // -------------------------------------------------------------------------
    // Live-state fields — written by optimizer thread, read by HTTP thread.
    // -------------------------------------------------------------------------

    /**
     * Current phase. One of: {@code "idle"}, {@code "downloading"},
     * {@code "pq-training"}, {@code "compacting"}, {@code "uploading"},
     * {@code "registering-zk"}, {@code "deprecating-inputs"}.
     */
    volatile String phase = "idle";

    /** UUID assigned to the merge cycle at start; {@code null} when idle. */
    volatile String mergeId = null;

    /** Number of input segments being merged (0 when idle). */
    volatile int inputSegments = 0;

    /**
     * Total vector count across all input segments (0 when idle).
     * Summed from {@code SegmentMetadata.getVectorCount()} at the start of the
     * merge; used as {@code batches_total} during the graph-build phase.
     */
    volatile long inputVectors = 0;

    /**
     * Vectors (or download batches) completed so far in the current phase.
     * Updated by the merger's batch-progress callback every
     * {@value RemoteSegmentGraphMerger#BATCH_PROGRESS_INTERVAL} vectors during
     * the legacy graph-build phase.
     */
    volatile long batchesWritten = 0;

    /** Total batches for the current phase; 0 when unknown (e.g. PQ-training). */
    volatile long batchesTotal = 0;

    /** Wall-clock epoch-ms when the current merge started; 0 when idle. */
    volatile long mergeStartMs = 0;

    // -------------------------------------------------------------------------
    // Lifecycle helpers (called from the optimizer thread)
    // -------------------------------------------------------------------------

    void enterIdle() {
        phase = "idle";
        mergeId = null;
        inputSegments = 0;
        inputVectors = 0;
        batchesWritten = 0;
        batchesTotal = 0;
        mergeStartMs = 0;
    }

    void enterMerge(String id, int segments, long vectors) {
        mergeId = id;
        inputSegments = segments;
        inputVectors = vectors;
        batchesWritten = 0;
        batchesTotal = vectors;
        mergeStartMs = System.currentTimeMillis();
        phase = "downloading";
    }

    void updatePhase(String newPhase) {
        phase = newPhase;
        batchesWritten = 0;
    }

    void updateBatchProgress(long written, long total) {
        batchesWritten = written;
        batchesTotal = total;
    }

    // -------------------------------------------------------------------------
    // Snapshot (called from the HTTP thread)
    // -------------------------------------------------------------------------

    /**
     * Captures a consistent-ish snapshot of the current state, augmented with
     * a live JVM memory reading. Safe to call from any thread.
     */
    Snapshot snapshot() {
        // Capture volatile fields in locals to reduce visibility hazards.
        String snapPhase       = phase;
        String snapMergeId     = mergeId;
        int    snapSegments    = inputSegments;
        long   snapVectors     = inputVectors;
        long   snapWritten     = batchesWritten;
        long   snapTotal       = batchesTotal;
        long   snapStartMs     = mergeStartMs;

        long heapUsed = MEMORY_MX.getHeapMemoryUsage().getUsed();
        long heapMax  = MEMORY_MX.getHeapMemoryUsage().getMax();
        // Netty's direct-memory counter: -1 when Netty defers to JDK accounting.
        long directUsed = PlatformDependent.usedDirectMemory();
        long directMax  = PlatformDependent.maxDirectMemory();

        long gcCount = 0;
        long gcTimeMs = 0;
        for (GarbageCollectorMXBean gc : GC_BEANS) {
            long cnt = gc.getCollectionCount();
            long t   = gc.getCollectionTime();
            if (cnt >= 0) {
                gcCount += cnt;
            }
            if (t >= 0) {
                gcTimeMs += t;
            }
        }

        long elapsedMs = (snapStartMs > 0 && !"idle".equals(snapPhase))
                ? System.currentTimeMillis() - snapStartMs
                : 0;
        long etaMs = estimateEta(snapWritten, snapTotal, elapsedMs);

        return new Snapshot(snapPhase, snapMergeId, snapSegments, snapVectors,
                snapWritten, snapTotal, snapStartMs, elapsedMs, etaMs,
                heapUsed, heapMax, directUsed, directMax, gcCount, gcTimeMs);
    }

    private static long estimateEta(long written, long total, long elapsedMs) {
        if (written <= 0 || total <= 0 || written >= total) {
            return -1;
        }
        return elapsedMs * (total - written) / written;
    }

    // -------------------------------------------------------------------------
    // Snapshot value object
    // -------------------------------------------------------------------------

    /**
     * Point-in-time snapshot of the merge progress, including a fresh JVM
     * memory reading. Immutable; safe to share across threads.
     */
    static final class Snapshot {
        final String phase;
        final String mergeId;
        final int inputSegments;
        final long inputVectors;
        final long batchesWritten;
        final long batchesTotal;
        final long mergeStartMs;
        final long elapsedMs;
        final long etaMs;
        final long heapUsedBytes;
        final long heapMaxBytes;
        final long directUsedBytes;
        final long directMaxBytes;
        final long gcCount;
        final long gcTimeMs;

        Snapshot(String phase, String mergeId, int inputSegments, long inputVectors,
                 long batchesWritten, long batchesTotal, long mergeStartMs,
                 long elapsedMs, long etaMs,
                 long heapUsedBytes, long heapMaxBytes,
                 long directUsedBytes, long directMaxBytes,
                 long gcCount, long gcTimeMs) {
            this.phase = phase;
            this.mergeId = mergeId;
            this.inputSegments = inputSegments;
            this.inputVectors = inputVectors;
            this.batchesWritten = batchesWritten;
            this.batchesTotal = batchesTotal;
            this.mergeStartMs = mergeStartMs;
            this.elapsedMs = elapsedMs;
            this.etaMs = etaMs;
            this.heapUsedBytes = heapUsedBytes;
            this.heapMaxBytes = heapMaxBytes;
            this.directUsedBytes = directUsedBytes;
            this.directMaxBytes = directMaxBytes;
            this.gcCount = gcCount;
            this.gcTimeMs = gcTimeMs;
        }

        double pctComplete() {
            if (batchesTotal <= 0) {
                return 0.0;
            }
            return Math.min(100.0, batchesWritten * 100.0 / batchesTotal);
        }

        long heapUsedMb() {
            return heapUsedBytes / (1024 * 1024);
        }

        long heapMaxMb() {
            return heapMaxBytes / (1024 * 1024);
        }

        long directUsedMb() {
            return directUsedBytes < 0 ? -1L : directUsedBytes / (1024 * 1024);
        }

        long directMaxMb() {
            return directMaxBytes < 0 ? -1L : directMaxBytes / (1024 * 1024);
        }
    }
}
