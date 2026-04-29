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
package herddb.indexing.segment;

import herddb.log.LogSequenceNumber;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-segment tombstone tracker for segmented-v2 vector indexes.
 *
 * <p>Holds the in-memory set of tombstoned ordinals for one ACTIVE/TRANSFERRING
 * segment, the LSN watermark up to which deletions have been applied, and the
 * machinery to flush the overlay to remote storage and CAS the segment registry
 * znode. Only the segment's current owner instance should call
 * {@link #flush()}; readers (e.g. a new owner during ownership transfer) use
 * the static {@link #loadOverlay(DataStorageManager, String, String, String, long)}
 * helper to fetch a previously-published overlay from S3.
 *
 * <p>Thread-safety: {@link #markDeleted} and {@link #flush} synchronise on the
 * manager instance. Concurrent {@code markDeleted} calls are cheap (just a
 * sorted-set add); {@code flush} grabs a snapshot under the lock and then
 * uploads outside the lock.
 */
public final class TombstoneOverlayManager {

    private static final Logger LOGGER = Logger.getLogger(TombstoneOverlayManager.class.getName());

    /** Multipart fileType prefix — overlays are written as {@code prefix + generation}. */
    static final String FILE_TYPE_PREFIX = "tombstones-";

    /** Multipart {@code uuid} component grouping all overlays for a single segment. */
    static String multipartUuidFor(String indexUuid, String segmentUuid) {
        return indexUuid + "_seg_" + segmentUuid;
    }

    static String fileTypeFor(long overlayGeneration) {
        return FILE_TYPE_PREFIX + overlayGeneration;
    }

    private final DataStorageManager dataStorageManager;
    private final SegmentRegistryClient registry;
    private final String tablespaceUuid;
    private final String indexUuid;
    private final String segmentUuid;
    private final Path tmpDirectory;

    /**
     * In-memory tombstone set. Accessed only under {@code this} monitor.
     */
    private final SortedSet<Integer> tombstoned = new TreeSet<>();

    /** LSN watermark up to which all deletions have been observed. */
    private LogSequenceNumber watermarkLsn = LogSequenceNumber.START_OF_TIME;

    /** Overlay generation last successfully published to remote storage. */
    private long lastPublishedGeneration = 0L;

    /**
     * @param tmpDirectory local scratch directory for staging overlay payloads
     *                     before multipart upload. The manager creates and deletes
     *                     a small temp file per flush.
     */
    public TombstoneOverlayManager(DataStorageManager dataStorageManager,
                                   SegmentRegistryClient registry,
                                   String tablespaceUuid,
                                   String indexUuid,
                                   String segmentUuid,
                                   Path tmpDirectory) {
        this.dataStorageManager = dataStorageManager;
        this.registry = registry;
        this.tablespaceUuid = tablespaceUuid;
        this.indexUuid = indexUuid;
        this.segmentUuid = segmentUuid;
        this.tmpDirectory = tmpDirectory;
    }

    public synchronized int tombstoneCount() {
        return tombstoned.size();
    }

    public synchronized LogSequenceNumber getWatermarkLsn() {
        return watermarkLsn;
    }

    public synchronized long getLastPublishedGeneration() {
        return lastPublishedGeneration;
    }

    public String getSegmentUuid() {
        return segmentUuid;
    }

    /**
     * Records a tombstone for the given segment-local ordinal at the given LSN.
     * Idempotent: marking the same ordinal twice is a no-op. The watermark LSN
     * advances to {@code max(current, lsn)}.
     */
    public synchronized void markDeleted(int ordinal, LogSequenceNumber lsn) {
        if (ordinal < 0) {
            throw new IllegalArgumentException("ordinal must be non-negative");
        }
        tombstoned.add(ordinal);
        if (compareLsn(lsn, watermarkLsn) > 0) {
            watermarkLsn = lsn;
        }
    }

    /**
     * Bulk-load tombstones from a previously-fetched overlay (used during ownership
     * transfer or restart). Existing in-memory state is replaced.
     */
    public synchronized void replaceFromOverlay(TombstoneOverlay overlay) {
        tombstoned.clear();
        for (int i = 0; i < overlay.ordinalCount(); i++) {
            tombstoned.add(overlay.ordinalAt(i));
        }
        watermarkLsn = overlay.getTombstoneLsn();
        lastPublishedGeneration = overlay.getOverlayGeneration();
    }

    /**
     * Snapshots the current state, uploads a new overlay file, and CASes the segment
     * znode to point at it. Returns the new overlay (with bumped generation) or
     * {@link Optional#empty()} if the in-memory state hasn't changed since the last
     * successful flush.
     *
     * <p>The CAS uses the supplied {@link VersionedSegmentMetadata} as the expected
     * version; on conflict the caller should re-read the znode and retry. We do NOT
     * retry inside this method to keep the lock scope tight.
     */
    public Optional<VersionedSegmentMetadata> flush(VersionedSegmentMetadata current)
            throws IOException, DataStorageManagerException, SegmentRegistryException {
        int[] snapshotOrdinals;
        LogSequenceNumber snapshotLsn;
        long snapshotGeneration;
        synchronized (this) {
            if (tombstoned.isEmpty() && lastPublishedGeneration == 0L) {
                // Never had any tombstones at all; no need to upload an empty overlay.
                return Optional.empty();
            }
            snapshotOrdinals = new int[tombstoned.size()];
            int idx = 0;
            for (Integer o : tombstoned) {
                snapshotOrdinals[idx++] = o;
            }
            snapshotLsn = watermarkLsn;
            snapshotGeneration = lastPublishedGeneration + 1;
        }

        TombstoneOverlay overlay = new TombstoneOverlay(
                segmentUuid, snapshotLsn, snapshotGeneration, snapshotOrdinals);
        byte[] payload = overlay.serialize();

        Path tempFile = Files.createTempFile(tmpDirectory, "herddb-tombstones-", ".bin");
        boolean success = false;
        String multipartPath;
        long fileSize;
        try {
            Files.write(tempFile, payload, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            fileSize = Files.size(tempFile);
            multipartPath = dataStorageManager.writeMultipartIndexFile(
                    tablespaceUuid,
                    multipartUuidFor(indexUuid, segmentUuid),
                    fileTypeFor(snapshotGeneration),
                    tempFile, null);
            success = true;
        } finally {
            if (!success) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                }
            } else {
                Files.deleteIfExists(tempFile);
            }
        }

        SegmentMetadata updated = current.metadata().toBuilder()
                .tombstonePath(multipartPath)
                .tombstoneLsn(snapshotLsn)
                .build();
        VersionedSegmentMetadata after;
        try {
            after = registry.casUpdateSegment(current, updated);
        } catch (SegmentRegistryException e) {
            // Best-effort cleanup of the just-uploaded overlay file so we don't leak it
            // when the registry refuses our CAS. The next flush attempt will publish a
            // fresh generation; this one is wasted.
            try {
                dataStorageManager.deleteMultipartIndexFile(
                        tablespaceUuid,
                        multipartUuidFor(indexUuid, segmentUuid),
                        fileTypeFor(snapshotGeneration));
            } catch (DataStorageManagerException cleanupErr) {
                // Logged at WARNING — broad catch is intentional: cleanup must never mask
                // the real error from the CAS attempt.
                LOGGER.log(Level.WARNING,
                        "failed to clean up orphan tombstone overlay generation {0} for segment {1}",
                        new Object[]{snapshotGeneration, segmentUuid});
            }
            throw e;
        }

        synchronized (this) {
            lastPublishedGeneration = snapshotGeneration;
        }
        LOGGER.log(Level.FINE,
                "flushed tombstone overlay generation {0} for segment {1} ({2} ordinals, lsn={3}, {4} bytes)",
                new Object[]{snapshotGeneration, segmentUuid, snapshotOrdinals.length, snapshotLsn, fileSize});
        return Optional.of(after);
    }

    /**
     * Reads the overlay file referenced by a {@link SegmentMetadata#getTombstonePath()}.
     * The file is opened via the multipart reader API and read fully into memory.
     */
    public static TombstoneOverlay loadOverlay(DataStorageManager dsm,
                                               String tablespaceUuid,
                                               String indexUuid,
                                               String segmentUuid,
                                               long overlayGeneration)
            throws IOException, DataStorageManagerException {
        ReaderSupplier supplier = dsm.multipartIndexReaderSupplier(
                tablespaceUuid,
                multipartUuidFor(indexUuid, segmentUuid),
                fileTypeFor(overlayGeneration),
                /* fileSize hint */ Long.MAX_VALUE);
        try (RandomAccessReader reader = supplier.get()) {
            reader.seek(0);
            // Overlay files are tiny (a few hundred bytes for a sparse delete pattern,
            // up to a few MiB for very dense deletes). Read fully into memory.
            int header = readIntFully(reader);
            if (header != TombstoneOverlay.WIRE_FORMAT_VERSION) {
                throw new IOException("unexpected tombstone overlay version: " + header);
            }
            int uuidLen = readIntFully(reader);
            byte[] uuidBytes = new byte[uuidLen];
            reader.readFully(uuidBytes);
            long lsnLedgerId = readLongFully(reader);
            long lsnOffset = readLongFully(reader);
            long generation = readLongFully(reader);
            int count = readIntFully(reader);
            int[] ordinals = new int[count];
            for (int i = 0; i < count; i++) {
                ordinals[i] = readIntFully(reader);
            }
            return new TombstoneOverlay(
                    new String(uuidBytes, java.nio.charset.StandardCharsets.UTF_8),
                    new LogSequenceNumber(lsnLedgerId, lsnOffset),
                    generation, ordinals);
        }
    }

    private static int readIntFully(RandomAccessReader reader) throws IOException {
        byte[] buf = new byte[4];
        reader.readFully(buf);
        return ((buf[0] & 0xff) << 24) | ((buf[1] & 0xff) << 16) | ((buf[2] & 0xff) << 8) | (buf[3] & 0xff);
    }

    private static long readLongFully(RandomAccessReader reader) throws IOException {
        byte[] buf = new byte[8];
        reader.readFully(buf);
        long v = 0L;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[i] & 0xffL);
        }
        return v;
    }

    private static int compareLsn(LogSequenceNumber a, LogSequenceNumber b) {
        if (a.ledgerId != b.ledgerId) {
            return Long.compare(a.ledgerId, b.ledgerId);
        }
        return Long.compare(a.offset, b.offset);
    }

    /** Visible for tests: returns the in-memory tombstoned ordinals as an array. */
    public synchronized int[] snapshotOrdinals() {
        int[] out = new int[tombstoned.size()];
        int i = 0;
        for (Integer o : tombstoned) {
            out[i++] = o;
        }
        return Arrays.copyOf(out, out.length);
    }
}
