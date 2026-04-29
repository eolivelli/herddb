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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Per-segment tombstone overlay: the set of segment-local ordinals that have
 * been deleted since the segment was sealed, paired with the LSN watermark up
 * to which deletions have been applied. The overlay is the only mutable
 * piece of segment state that survives ownership transfer — the segment graph
 * and map files themselves remain byte-immutable.
 *
 * <p>Wire format v1:
 * <pre>
 *   int    version (= 1)
 *   int    segmentUuid length (UTF-8 byte count)
 *   bytes  segmentUuid (UTF-8)
 *   long   tombstoneLsn.ledgerId
 *   long   tombstoneLsn.offset
 *   long   overlayGeneration  (monotonic per-segment, increases on every flush)
 *   int    ordinal count
 *   int[]  tombstoned ordinals (ascending; duplicates rejected on read)
 * </pre>
 *
 * <p>The overlay is conceptually a set; the on-disk representation sorts ordinals
 * for determinism and debuggability. A bitmap representation could be more
 * compact for dense deletion patterns, but the int-array form is tiny for the
 * typical sparse case (a few thousand deletes per multi-million-vector segment)
 * and has trivial reader code; we will revisit if measurement demands it.
 */
public final class TombstoneOverlay {

    public static final int WIRE_FORMAT_VERSION = 1;

    private final String segmentUuid;
    private final LogSequenceNumber tombstoneLsn;
    private final long overlayGeneration;
    private final int[] tombstonedOrdinals;

    public TombstoneOverlay(String segmentUuid, LogSequenceNumber tombstoneLsn,
                            long overlayGeneration, int[] tombstonedOrdinals) {
        this.segmentUuid = Objects.requireNonNull(segmentUuid, "segmentUuid");
        this.tombstoneLsn = Objects.requireNonNull(tombstoneLsn, "tombstoneLsn");
        this.overlayGeneration = overlayGeneration;
        this.tombstonedOrdinals = tombstonedOrdinals == null
                ? new int[0]
                : Arrays.copyOf(tombstonedOrdinals, tombstonedOrdinals.length);
    }

    public String getSegmentUuid() {
        return segmentUuid;
    }

    public LogSequenceNumber getTombstoneLsn() {
        return tombstoneLsn;
    }

    public long getOverlayGeneration() {
        return overlayGeneration;
    }

    /**
     * Returns a defensive copy of the tombstoned-ordinal array. For large overlays,
     * callers needing read-only access should use {@link #ordinalAt(int)} +
     * {@link #ordinalCount()}.
     */
    public int[] getTombstonedOrdinals() {
        return Arrays.copyOf(tombstonedOrdinals, tombstonedOrdinals.length);
    }

    public int ordinalCount() {
        return tombstonedOrdinals.length;
    }

    public int ordinalAt(int index) {
        return tombstonedOrdinals[index];
    }

    /**
     * Serialise to the wire format above.
     */
    public byte[] serialize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    16 + segmentUuid.length() + 4 * tombstonedOrdinals.length);
            try (DataOutputStream dos = new DataOutputStream(baos)) {
                dos.writeInt(WIRE_FORMAT_VERSION);
                byte[] uuidBytes = segmentUuid.getBytes(StandardCharsets.UTF_8);
                dos.writeInt(uuidBytes.length);
                dos.write(uuidBytes);
                dos.writeLong(tombstoneLsn.ledgerId);
                dos.writeLong(tombstoneLsn.offset);
                dos.writeLong(overlayGeneration);
                dos.writeInt(tombstonedOrdinals.length);
                for (int ord : tombstonedOrdinals) {
                    dos.writeInt(ord);
                }
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize tombstone overlay", e);
        }
    }

    /**
     * Deserialise from the wire format. The supplied bytes must have been produced
     * by {@link #serialize()} (no other format is supported).
     *
     * @throws IOException if the bytes are truncated or carry an unknown version.
     */
    public static TombstoneOverlay deserialize(byte[] data) throws IOException {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int version = dis.readInt();
            if (version != WIRE_FORMAT_VERSION) {
                throw new IOException("unsupported tombstone overlay wire version: " + version);
            }
            int uuidLen = dis.readInt();
            if (uuidLen < 0 || uuidLen > 1 << 12) {
                throw new IOException("implausible segment uuid length: " + uuidLen);
            }
            byte[] uuidBytes = new byte[uuidLen];
            dis.readFully(uuidBytes);
            String segmentUuid = new String(uuidBytes, StandardCharsets.UTF_8);
            long lsnLedgerId = dis.readLong();
            long lsnOffset = dis.readLong();
            long overlayGeneration = dis.readLong();
            int count = dis.readInt();
            if (count < 0) {
                throw new IOException("implausible tombstone count: " + count);
            }
            int[] ordinals = new int[count];
            for (int i = 0; i < count; i++) {
                ordinals[i] = dis.readInt();
            }
            return new TombstoneOverlay(segmentUuid,
                    new LogSequenceNumber(lsnLedgerId, lsnOffset),
                    overlayGeneration, ordinals);
        }
    }
}
