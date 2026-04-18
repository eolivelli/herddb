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

package herddb.index.blink;

import herddb.utils.ByteArrayCursor;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Small per-checkpoint manifest for the incremental BLink format.
 *
 * <p>Stored as the opaque {@code indexData} byte array inside the standard
 * {@code IndexStatus} returned by
 * {@code DataStorageManager.indexCheckpoint}. The manifest contains only
 * constant-size data plus small references to sidecar pages (the snapshot
 * chunks and the delta-log entries) — never the full node list. The full node
 * list lives in the snapshot chunk pages and is rebuilt at recovery by
 * reading the snapshot and replaying the delta chain.</p>
 *
 * <p>The format starts with a version marker large enough that the legacy
 * {@code BLinkKeyToPageIndex.MetadataSerializer.read} will immediately throw
 * {@code "Unknown BLink node metadata version 100"} when it encounters an
 * incremental manifest. Symmetrically, the incremental reader rejects any
 * version &lt; {@value #INCREMENTAL_VERSION_MIN} as a legacy format.</p>
 */
public final class IncrementalBLinkManifest {

    /**
     * Lowest version number reserved for the incremental format. Kept well
     * above the legacy version numbers (0, 1, 2) so crossing the streams is
     * detected deterministically.
     */
    public static final long INCREMENTAL_VERSION_MIN = 100L;

    /**
     * Current version of the incremental manifest.
     */
    public static final long CURRENT_VERSION = 100L;

    private static final long NO_FLAGS = 0L;

    /**
     * Monotonically increasing epoch counter. Incremented on every
     * checkpoint. Used to order snapshot / delta writes and to drive
     * tombstone-based garbage collection.
     */
    public final long epoch;

    /**
     * Value count mirrored from {@code BLink.size()}.
     */
    public final long values;

    /**
     * {@code BLink.nextID.get()} at checkpoint time — the next page id to
     * allocate on recovery.
     */
    public final long nextID;

    /**
     * Anchor pointers captured from the {@code BLink} tree.
     */
    public final long anchorTop;
    public final int anchorTopHeight;
    public final long anchorFast;
    public final int anchorFastHeight;
    public final long anchorFirst;

    /**
     * Epoch at which the snapshot was written. A snapshot is an unordered
     * collection of {@link SnapshotChunkRef}s; together they contain the full
     * {@code BLinkMetadata} valid at {@link #snapshotEpoch}.
     */
    public final long snapshotEpoch;

    public final List<SnapshotChunkRef> snapshotChunks;

    /**
     * Delta-chain entries to apply on top of the snapshot to rebuild the
     * current {@code BLinkMetadata}.
     */
    public final List<DeltaRef> deltaChain;

    public IncrementalBLinkManifest(
            long epoch, long values, long nextID,
            long anchorTop, int anchorTopHeight,
            long anchorFast, int anchorFastHeight,
            long anchorFirst,
            long snapshotEpoch, List<SnapshotChunkRef> snapshotChunks,
            List<DeltaRef> deltaChain
    ) {
        this.epoch = epoch;
        this.values = values;
        this.nextID = nextID;
        this.anchorTop = anchorTop;
        this.anchorTopHeight = anchorTopHeight;
        this.anchorFast = anchorFast;
        this.anchorFastHeight = anchorFastHeight;
        this.anchorFirst = anchorFirst;
        this.snapshotEpoch = snapshotEpoch;
        this.snapshotChunks = Collections.unmodifiableList(new ArrayList<>(snapshotChunks));
        this.deltaChain = Collections.unmodifiableList(new ArrayList<>(deltaChain));
    }

    /**
     * Reference to one snapshot chunk page.
     */
    public static final class SnapshotChunkRef {
        public final long pageId;
        public final int chunkIndex;
        public final int totalChunks;

        public SnapshotChunkRef(long pageId, int chunkIndex, int totalChunks) {
            this.pageId = pageId;
            this.chunkIndex = chunkIndex;
            this.totalChunks = totalChunks;
        }
    }

    /**
     * Reference to one delta-log page.
     */
    public static final class DeltaRef {
        public final long epoch;
        public final long pageId;

        public DeltaRef(long epoch, long pageId) {
            this.epoch = epoch;
            this.pageId = pageId;
        }
    }

    public byte[] serialize() throws IOException {
        VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(bos)) {
            out.writeVLong(CURRENT_VERSION);
            out.writeVLong(NO_FLAGS);

            out.writeVLong(epoch);
            out.writeVLong(values);
            out.writeVLong(nextID);

            out.writeZLong(anchorTop);
            out.writeVInt(anchorTopHeight);
            out.writeZLong(anchorFast);
            out.writeVInt(anchorFastHeight);
            out.writeZLong(anchorFirst);

            out.writeVLong(snapshotEpoch);
            out.writeVInt(snapshotChunks.size());
            for (SnapshotChunkRef ref : snapshotChunks) {
                out.writeVLong(ref.pageId);
                out.writeVInt(ref.chunkIndex);
                out.writeVInt(ref.totalChunks);
            }

            out.writeVInt(deltaChain.size());
            for (DeltaRef ref : deltaChain) {
                out.writeVLong(ref.epoch);
                out.writeVLong(ref.pageId);
            }
        }
        return bos.toByteArray();
    }

    public static IncrementalBLinkManifest deserialize(byte[] data) throws IOException {
        try (ByteArrayCursor cur = ByteArrayCursor.wrap(data)) {
            long version = cur.readVLong();
            if (version < INCREMENTAL_VERSION_MIN) {
                throw new LegacyFormatDetectedException(
                        "data appears to be in legacy BLink format (version marker "
                                + version + "); incremental reader refuses to parse it");
            }
            if (version != CURRENT_VERSION) {
                throw new IOException("unsupported incremental BLink manifest version " + version);
            }

            long flags = cur.readVLong();
            if (flags != NO_FLAGS) {
                throw new IOException("unsupported incremental BLink manifest flags " + flags);
            }

            long epoch = cur.readVLong();
            long values = cur.readVLong();
            long nextID = cur.readVLong();

            long anchorTop = cur.readZLong();
            int anchorTopHeight = cur.readVInt();
            long anchorFast = cur.readZLong();
            int anchorFastHeight = cur.readVInt();
            long anchorFirst = cur.readZLong();

            long snapshotEpoch = cur.readVLong();
            int numSnapshotChunks = cur.readVInt();
            List<SnapshotChunkRef> snapshotChunks = new ArrayList<>(numSnapshotChunks);
            for (int i = 0; i < numSnapshotChunks; i++) {
                long pageId = cur.readVLong();
                int chunkIndex = cur.readVInt();
                int totalChunks = cur.readVInt();
                snapshotChunks.add(new SnapshotChunkRef(pageId, chunkIndex, totalChunks));
            }

            int numDeltas = cur.readVInt();
            List<DeltaRef> deltaChain = new ArrayList<>(numDeltas);
            for (int i = 0; i < numDeltas; i++) {
                long deltaEpoch = cur.readVLong();
                long pageId = cur.readVLong();
                deltaChain.add(new DeltaRef(deltaEpoch, pageId));
            }

            return new IncrementalBLinkManifest(
                    epoch, values, nextID,
                    anchorTop, anchorTopHeight,
                    anchorFast, anchorFastHeight,
                    anchorFirst,
                    snapshotEpoch, snapshotChunks, deltaChain);
        }
    }

    /**
     * Thrown when the incremental manifest reader encounters a legacy-format
     * payload. Callers convert this into a user-visible
     * {@code DataStorageManagerException} with guidance on switching the
     * {@code herddb.index.pk.mode} system property.
     */
    public static final class LegacyFormatDetectedException extends IOException {
        private static final long serialVersionUID = 1L;

        public LegacyFormatDetectedException(String message) {
            super(message);
        }
    }
}
