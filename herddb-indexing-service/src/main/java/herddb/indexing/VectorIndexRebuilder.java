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
package herddb.indexing;

import herddb.codec.DataAccessorForFullRecord;
import herddb.index.vector.AbstractVectorStore;
import herddb.index.vector.PersistentVectorStore;
import herddb.index.vector.VectorIndexManager;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.TableStatus;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Issue #471 — back-fills a freshly-created vector index from the
 * pinned table checkpoint left behind by
 * {@code TableSpaceManager.createIndex}.
 *
 * <p>Streams pages one at a time from
 * {@link DataStorageManager#fullTableScan(String, String,
 * LogSequenceNumber, FullTableScanConsumer)} at the LSN encoded in
 * {@link VectorIndexManager#PROP_REBUILD_LSN}, decodes each record's
 * vector column via {@link DataAccessorForFullRecord}, applies the
 * shard predicate to skip keys owned by other IndexingService
 * instances, and inserts the (key, vector) pairs into the freshly-
 * created {@link AbstractVectorStore} via the public
 * {@link AbstractVectorStore#addVector(Bytes, float[])} path.
 *
 * <p><b>Back-pressure</b>: the rebuild calls only {@code addVector},
 * so all three of {@link PersistentVectorStore}'s back-pressure
 * layers apply unchanged: Phase B/C checkpoint cap
 * ({@code liveVectorCapDuringCheckpoint}), memory budget
 * ({@code maxVectorMemoryBytes} / {@code memoryBudget}), and segment
 * count ({@code compactionBackpressureThreshold}). When ingest hits
 * any cap, {@code addVector} blocks the rebuilder thread, which
 * naturally throttles the {@code fullTableScan} consumer's
 * {@code acceptPage} callback — the pages-per-second rate matches
 * what the vector store can sustain. Required for tables in the 20 B-row
 * range where the page-flush rate would otherwise overwhelm
 * compaction.
 *
 * <p><b>Memory bound</b>: each {@code acceptPage} callback receives
 * one page worth of records (bounded by {@code maxLogicalPageSize},
 * typically a few MB). The rebuilder iterates the list and drops the
 * reference at the end of the call — no internal buffering or copy.
 * Heap pressure from the scan is therefore O(one page), independent
 * of table size.
 *
 * <p><b>Crash safety</b>: this method runs synchronously on the
 * tailer thread inside {@code IndexingServiceEngine.applyEntry}, so
 * {@code lastProcessedLsn} cannot advance past the {@code CREATE_INDEX}
 * entry until the rebuild returns. A crash mid-rebuild leaves the
 * engine's watermark at LSN &lt; {@code CREATE_INDEX}, so the
 * {@code CREATE_INDEX} entry is replayed on restart and the rebuild
 * re-runs from scratch. The {@link AbstractVectorStore#addVector}
 * path is idempotent at the (key, vector) level (last write wins on
 * the same key), so a partial rebuild followed by a full re-rebuild
 * converges to the correct end state.
 *
 * @author enrico.olivelli
 */
public final class VectorIndexRebuilder {

    private static final Logger LOGGER = Logger.getLogger(VectorIndexRebuilder.class.getName());

    /**
     * Default progress-log granularity. Tuned for 20 B-row tables: at
     * 100 k records/sec the rebuilder logs once every ~10 s; at 1 M
     * records/sec it logs once a second. Smaller values flood the
     * log; larger values blind operators to a stuck scan.
     */
    public static final int DEFAULT_PROGRESS_LOG_INTERVAL_RECORDS = 1_000_000;

    private final DataStorageManager dsm;
    private final String tablespaceUUID;
    private final Table table;
    private final Index index;
    private final AbstractVectorStore store;
    private final Predicate<Bytes> shardOwnership;
    private final VectorIndexRebuildMetrics metrics;
    private final int progressLogIntervalRecords;

    public VectorIndexRebuilder(DataStorageManager dsm,
                                String tablespaceUUID,
                                Table table,
                                Index index,
                                AbstractVectorStore store,
                                Predicate<Bytes> shardOwnership,
                                VectorIndexRebuildMetrics metrics) {
        this(dsm, tablespaceUUID, table, index, store, shardOwnership, metrics,
                DEFAULT_PROGRESS_LOG_INTERVAL_RECORDS);
    }

    public VectorIndexRebuilder(DataStorageManager dsm,
                                String tablespaceUUID,
                                Table table,
                                Index index,
                                AbstractVectorStore store,
                                Predicate<Bytes> shardOwnership,
                                VectorIndexRebuildMetrics metrics,
                                int progressLogIntervalRecords) {
        this.dsm = Objects.requireNonNull(dsm, "dsm");
        this.tablespaceUUID = Objects.requireNonNull(tablespaceUUID, "tablespaceUUID");
        this.table = Objects.requireNonNull(table, "table");
        this.index = Objects.requireNonNull(index, "index");
        this.store = Objects.requireNonNull(store, "store");
        this.shardOwnership = Objects.requireNonNull(shardOwnership, "shardOwnership");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (progressLogIntervalRecords <= 0) {
            throw new IllegalArgumentException(
                    "progressLogIntervalRecords must be > 0, got " + progressLogIntervalRecords);
        }
        this.progressLogIntervalRecords = progressLogIntervalRecords;
    }

    /**
     * Streams the table's pinned checkpoint, indexes every record
     * whose primary key the local IS owns, and seals the vector
     * store's live shard before returning.
     *
     * @throws IllegalStateException when the index does not carry
     *     {@link VectorIndexManager#PROP_REBUILD_LSN} (caller bug —
     *     this rebuilder must only be invoked on indexes that
     *     {@code TableSpaceManager.createIndex} marked
     *     {@code rebuild=true}).
     * @throws RuntimeException wrapping any
     *     {@link DataStorageManagerException} from the underlying
     *     scan. Wrapping is deliberate: the engine's tailer-thread
     *     apply path catches {@code RuntimeException} but advances
     *     the watermark only on clean returns, so a thrown rebuild
     *     keeps the watermark at LSN &lt; {@code CREATE_INDEX} and
     *     the entry is replayed on restart.
     */
    public void run() {
        String encodedLsn = index.properties.get(VectorIndexManager.PROP_REBUILD_LSN);
        if (encodedLsn == null) {
            throw new IllegalStateException(
                    "rebuild requires " + VectorIndexManager.PROP_REBUILD_LSN
                            + " on index " + index.tablespace + "." + index.table + "." + index.name);
        }
        LogSequenceNumber rebuildLsn = VectorIndexManager.decodeRebuildLsn(encodedLsn);
        if (index.columnNames.length != 1) {
            throw new IllegalStateException(
                    "vector index must have exactly one column, got " + index.columnNames.length);
        }
        String vectorColumn = index.columnNames[0];

        // Issue #471: write start-time BEFORE the isInterrupted
        // check below so that an interrupt-before-start exit is still
        // visible to operators (start > 0, end may be 0 or briefly
        // 0 until the finally below writes it). This makes the
        // metric promise "every exit path updates start AND end"
        // hold uniformly, including the interrupt-before-start path.
        long startNanos = System.nanoTime();
        long startMillis = System.currentTimeMillis();
        metrics.lastRebuildStartTimeMillis = startMillis;

        // Issue #471: cooperative cancellation — early-out if the
        // tailer thread is already interrupted before we even start.
        // This is the load-bearing check for the engine-shutdown
        // path; further checks per-page guard against the case where
        // the interrupt arrives mid-scan.
        if (Thread.currentThread().isInterrupted()) {
            // Update the end-time before throwing so the gauge
            // promise ("end >= start ⇔ no rebuild in flight") holds.
            metrics.lastRebuildEndTimeMillis = System.currentTimeMillis();
            throw new RuntimeException(
                    "rebuild interrupted before start at LSN " + rebuildLsn
                            + " for " + index.tablespace + "." + index.table
                            + "." + index.name);
        }

        // Per-call counters — distinct from the engine-wide LongAdders
        // in metrics, which are cumulative across rebuilds. We need
        // both: the per-call values drive progress logging; the
        // engine-wide values feed Prometheus.
        long[] localScanned = {0L};
        long[] localIndexed = {0L};
        long[] lastLogScanned = {0L};
        long[] lastLogNanos = {startNanos};

        LOGGER.log(Level.INFO,
                "rebuild starting for {0}.{1}.{2} at LSN {3} (vectorColumn={4})",
                new Object[]{index.tablespace, index.table, index.name, rebuildLsn, vectorColumn});

        FullTableScanConsumer consumer = new FullTableScanConsumer() {
            @Override
            public void acceptTableStatus(TableStatus status) {
                // Defence-in-depth: the DSM must return the
                // checkpoint at exactly rebuildLsn; a future
                // regression that returns a different checkpoint
                // (e.g. because the pinned LSN was reclaimed by a
                // periodic activator-driven cleanup) would silently
                // back-fill from the wrong snapshot. Surface
                // immediately.
                if (!rebuildLsn.equals(status.sequenceNumber)) {
                    throw new IllegalStateException(
                            "rebuild scan returned checkpoint at " + status.sequenceNumber
                                    + " but rebuild LSN is " + rebuildLsn
                                    + " for " + index.tablespace + "." + index.table
                                    + "." + index.name);
                }
                LOGGER.log(Level.INFO,
                        "rebuild {0}.{1}.{2}: pinned checkpoint at LSN {3}, {4} active pages",
                        new Object[]{index.tablespace, index.table, index.name,
                                status.sequenceNumber, status.activePages.size()});
            }

            @Override
            public void acceptPage(long pageId, List<Record> records) {
                // Issue #471: cooperative cancellation. If the engine
                // is being shut down, the tailer thread is interrupted
                // (IndexingServiceEngine.close → tailerThread.interrupt).
                // Without this check, addVector's back-pressure waits
                // would absorb the interrupt and the rebuild would
                // continue burning CPU on the next-page fetch until
                // the entire scan finished naturally — many hours on a
                // 20 B-row table, well past any close() deadline.
                if (Thread.currentThread().isInterrupted()) {
                    throw new RuntimeException(
                            "rebuild interrupted at LSN " + rebuildLsn
                                    + " for " + index.tablespace + "." + index.table
                                    + "." + index.name);
                }
                for (Record record : records) {
                    localScanned[0]++;
                    metrics.recordsScanned.increment();
                    if (!shardOwnership.test(record.key)) {
                        continue;
                    }
                    DataAccessorForFullRecord accessor = new DataAccessorForFullRecord(table, record);
                    float[] vector = extractVector(accessor, vectorColumn, index);
                    if (vector == null) {
                        // Null / type-mismatch vectors are not
                        // indexable. Tolerate them — the corresponding
                        // row exists in the table and any subsequent
                        // UPDATE will land via the tailer's
                        // applyUpdate path. extractVector logs a
                        // type-mismatch WARNING (rate-limited).
                        continue;
                    }
                    // The blocking call: addVector respects the
                    // PersistentVectorStore back-pressure contract
                    // (Phase B/C cap, memory budget, segment count).
                    // When pressure activates, this thread parks and
                    // the next-page fetch is throttled accordingly.
                    store.addVector(record.key, vector);
                    localIndexed[0]++;
                    metrics.recordsIndexed.increment();

                    if (localScanned[0] - lastLogScanned[0] >= progressLogIntervalRecords) {
                        long now = System.nanoTime();
                        long deltaMs = TimeUnit.NANOSECONDS.toMillis(now - lastLogNanos[0]);
                        long deltaScanned = localScanned[0] - lastLogScanned[0];
                        long recordsPerSec = deltaMs > 0 ? (1000L * deltaScanned) / deltaMs : 0L;
                        LOGGER.log(Level.INFO,
                                "rebuild {0}.{1}.{2}: progress scanned={3}, indexed={4},"
                                        + " rate={5} rec/s (last batch={6} ms)",
                                new Object[]{index.tablespace, index.table, index.name,
                                        localScanned[0], localIndexed[0],
                                        recordsPerSec, deltaMs});
                        lastLogScanned[0] = localScanned[0];
                        lastLogNanos[0] = now;
                    }
                }
            }

            @Override
            public void endTable() {
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                LOGGER.log(Level.INFO,
                        "rebuild {0}.{1}.{2} scan complete: scanned={3}, indexed={4}, elapsed={5} ms",
                        new Object[]{index.tablespace, index.table, index.name,
                                localScanned[0], localIndexed[0], elapsedMs});
            }
        };

        try {
            try {
                dsm.fullTableScan(tablespaceUUID, table.uuid, rebuildLsn, consumer);
            } catch (DataStorageManagerException scanErr) {
                throw new RuntimeException(
                        "rebuild aborted for " + index.tablespace + "." + index.table
                                + "." + index.name + " at " + rebuildLsn
                                + " (scan or back-pressure failure)", scanErr);
            }

            // Seal the live shard so a crash immediately after the
            // scan does not lose all the work. PersistentVectorStore.
            // checkpoint is the only path that flushes the in-memory
            // live shard to disk; without this call, the rebuild's
            // records linger in RAM until the engine's next periodic
            // checkpoint, and a restart in that window would force a
            // full re-rebuild.
            // Best-effort: a checkpoint failure is logged but does NOT
            // fail the rebuild — the rebuild itself succeeded; the
            // next engine-driven checkpoint will catch up.
            if (store instanceof PersistentVectorStore) {
                try {
                    ((PersistentVectorStore) store).checkpoint();
                } catch (DataStorageManagerException checkpointErr) {
                    LOGGER.log(Level.WARNING,
                            "rebuild " + index.tablespace + "." + index.table + "." + index.name
                                    + ": post-rebuild checkpoint failed; rebuild data will be"
                                    + " sealed by the next engine-driven checkpoint",
                            checkpointErr);
                }
            }

            long totalElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            LOGGER.log(Level.INFO,
                    "rebuild {0}.{1}.{2} done: scanned={3}, indexed={4}, total={5} ms",
                    new Object[]{index.tablespace, index.table, index.name,
                            localScanned[0], localIndexed[0], totalElapsedMs});
        } finally {
            // Update the end-time on EVERY exit — success, scan
            // failure, addVector RuntimeException, interrupt, anything.
            // The metric promise is "lastRebuildStartTimeMillis >
            // lastRebuildEndTimeMillis ⇔ rebuild in flight"; without
            // this finally the gauge would lie forever after any
            // failure path that wasn't a DataStorageManagerException.
            metrics.lastRebuildEndTimeMillis = System.currentTimeMillis();
        }
    }

    /**
     * Decodes the vector column from a record using the same path the
     * tailer's {@code applyInsert}/{@code applyUpdate} use. Mirroring
     * that path is load-bearing — a divergence would cause the
     * rebuild and the subsequent live DML to populate the vector
     * store with subtly different vector bytes.
     *
     * <p>Returns {@code null} when the column is missing, stored as
     * SQL NULL, or carries a non-{@code float[]} value (a schema
     * mismatch). The non-null type-mismatch case is logged at
     * WARNING with rate-limiting (one log line per
     * {@link #typeMismatchLogCounter} threshold crossing) so a
     * silent-skip on every record cannot go undetected, but a
     * mass-mismatch incident does not flood the log.
     */
    private float[] extractVector(DataAccessorForFullRecord accessor, String columnName, Index idx) {
        Object value = accessor.get(columnName);
        if (value == null) {
            return null;
        }
        if (value instanceof float[]) {
            return (float[]) value;
        }
        // Type mismatch — observable but rate-limited.
        long mismatchCount = ++typeMismatchLogCounter;
        if (mismatchCount == 1
                || mismatchCount == 10
                || mismatchCount == 100
                || mismatchCount % 1000 == 0) {
            LOGGER.log(Level.WARNING,
                    "rebuild {0}.{1}.{2}: vector column {3} has unexpected type {4}"
                            + " (mismatchCount={5}); record will be skipped",
                    new Object[]{idx.tablespace, idx.table, idx.name,
                            columnName, value.getClass().getName(), mismatchCount});
        }
        return null;
    }

    /**
     * Per-rebuilder running count of records that had a non-null
     * non-{@code float[]} value in the vector column. Used to
     * rate-limit the WARNING log in {@link #extractVector} so a
     * mass-mismatch incident does not flood the log. Not exposed as
     * a metric; the operator-visible signal is
     * {@code recordsScanned > 0 && recordsIndexed == 0} plus the
     * WARNING line itself.
     */
    private long typeMismatchLogCounter = 0L;
}
