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

import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.utils.ByteBufCursor;
import herddb.utils.ByteBufDataOutput;
import herddb.utils.Bytes;
import herddb.utils.HerdDBByteBufAllocators;
import herddb.utils.IndexKeySlab;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serialisation helpers for the incremental BLink sidecar pages.
 *
 * <p>Two kinds of sidecar pages are written via
 * {@code DataStorageManager.writeIndexPage}:
 * <ul>
 *   <li>{@link #PAGE_KIND_SNAPSHOT_CHUNK} — one chunk of the full node-metadata
 *       list; on recovery all chunks for a snapshot epoch are read and merged.</li>
 *   <li>{@link #PAGE_KIND_DELTA} — the per-checkpoint delta recording upserted
 *       node metadata, logically-removed node ids, and dead store-page ids for
 *       garbage collection.</li>
 * </ul>
 *
 * <p>The legacy {@code BLinkKeyToPageIndex} index-page codec writes pages with
 * a {@code vlong version, vlong flags, byte kind} header where {@code kind} is
 * {@code 1} (INNER) or {@code 2} (LEAF). The incremental codec uses the same
 * three-field header with distinct {@code kind} values so the two formats can
 * coexist in the same index directory without ambiguity.</p>
 */
final class IncrementalBLinkPageCodec {

    static final long PAGE_VERSION = 1L;

    static final byte PAGE_KIND_SNAPSHOT_CHUNK = 10;
    static final byte PAGE_KIND_DELTA = 11;

    private static final long PAGE_FLAGS = 0L;

    // Shared markers for the rightSep encoding (matches BLinkKeyToPageIndex style)
    private static final byte RIGHTSEP_INF = 0;
    private static final byte RIGHTSEP_BYTES = 1;

    /**
     * Defensive upper bound on the per-chunk node count read from disk.
     * The default {@code DEFAULT_SNAPSHOT_CHUNK_SIZE} in
     * {@link IncrementalBLinkKeyToPageIndex} is 50,000 nodes; this cap is
     * orders of magnitude above any plausible legitimate value, so it
     * never trips on a healthy page but blocks a corrupted {@code vint}
     * count from triggering eight separate large-array allocations
     * inside {@link #readNodeMetadataArray} (issue #411 review).
     */
    static final int MAX_NODES_PER_CHUNK = 1 << 22; // ~4.2M

    private IncrementalBLinkPageCodec() {
    }

    // ---------------- snapshot chunk ----------------

    static void writeSnapshotChunk(
            ByteBufDataOutput out,
            long epoch, int chunkIndex, int totalChunks,
            List<BLinkNodeMetadata<Bytes>> nodes
    ) throws IOException {
        out.writeVLong(PAGE_VERSION);
        out.writeVLong(PAGE_FLAGS);
        out.writeByte(PAGE_KIND_SNAPSHOT_CHUNK);

        out.writeVLong(epoch);
        out.writeVInt(chunkIndex);
        out.writeVInt(totalChunks);

        out.writeVInt(nodes.size());
        for (BLinkNodeMetadata<Bytes> node : nodes) {
            writeNodeMetadata(out, node);
        }
    }

    static SnapshotChunkContents readSnapshotChunk(ByteBufCursor in) throws IOException {
        long version = in.readVLong();
        long flags = in.readVLong();
        byte kind = in.readByte();
        if (version != PAGE_VERSION || flags != PAGE_FLAGS || kind != PAGE_KIND_SNAPSHOT_CHUNK) {
            throw new IOException("not a snapshot-chunk page (version=" + version
                    + ", flags=" + flags + ", kind=" + kind + ")");
        }
        long epoch = in.readVLong();
        int chunkIndex = in.readVInt();
        int totalChunks = in.readVInt();
        int count = in.readVInt();
        BLinkNodeMetadata<Bytes>[] nodes = readNodeMetadataArray(in, count);
        return new SnapshotChunkContents(epoch, chunkIndex, totalChunks, nodes);
    }

    static final class SnapshotChunkContents {
        final long epoch;
        final int chunkIndex;
        final int totalChunks;
        final BLinkNodeMetadata<Bytes>[] nodes;

        SnapshotChunkContents(long epoch, int chunkIndex, int totalChunks,
                              BLinkNodeMetadata<Bytes>[] nodes) {
            this.epoch = epoch;
            this.chunkIndex = chunkIndex;
            this.totalChunks = totalChunks;
            this.nodes = nodes;
        }
    }

    // ---------------- delta ----------------

    static void writeDelta(
            ByteBufDataOutput out,
            long epoch,
            List<BLinkNodeMetadata<Bytes>> upserted,
            long[] removedNodeIds,
            long[] deadStoreIds
    ) throws IOException {
        out.writeVLong(PAGE_VERSION);
        out.writeVLong(PAGE_FLAGS);
        out.writeByte(PAGE_KIND_DELTA);

        out.writeVLong(epoch);

        out.writeVInt(upserted.size());
        for (BLinkNodeMetadata<Bytes> node : upserted) {
            writeNodeMetadata(out, node);
        }

        out.writeVInt(removedNodeIds.length);
        for (long id : removedNodeIds) {
            out.writeVLong(id);
        }

        out.writeVInt(deadStoreIds.length);
        for (long id : deadStoreIds) {
            out.writeVLong(id);
        }
    }

    static DeltaContents readDelta(ByteBufCursor in) throws IOException {
        long version = in.readVLong();
        long flags = in.readVLong();
        byte kind = in.readByte();
        if (version != PAGE_VERSION || flags != PAGE_FLAGS || kind != PAGE_KIND_DELTA) {
            throw new IOException("not a delta page (version=" + version
                    + ", flags=" + flags + ", kind=" + kind + ")");
        }
        long epoch = in.readVLong();

        int upsertedCount = in.readVInt();
        BLinkNodeMetadata<Bytes>[] upserted = readNodeMetadataArray(in, upsertedCount);

        int removedCount = in.readVInt();
        long[] removedNodeIds = new long[removedCount];
        for (int i = 0; i < removedCount; i++) {
            removedNodeIds[i] = in.readVLong();
        }

        int deadCount = in.readVInt();
        long[] deadStoreIds = new long[deadCount];
        for (int i = 0; i < deadCount; i++) {
            deadStoreIds[i] = in.readVLong();
        }

        return new DeltaContents(epoch, upserted, removedNodeIds, deadStoreIds);
    }

    static final class DeltaContents {
        final long epoch;
        final BLinkNodeMetadata<Bytes>[] upserted;
        final long[] removedNodeIds;
        final long[] deadStoreIds;

        DeltaContents(long epoch, BLinkNodeMetadata<Bytes>[] upserted,
                      long[] removedNodeIds, long[] deadStoreIds) {
            this.epoch = epoch;
            this.upserted = upserted;
            this.removedNodeIds = removedNodeIds;
            this.deadStoreIds = deadStoreIds;
        }
    }

    // ---------------- shared node-metadata codec ----------------

    private static void writeNodeMetadata(ByteBufDataOutput out, BLinkNodeMetadata<Bytes> node)
            throws IOException {
        out.writeBoolean(node.leaf);
        out.writeVLong(node.id);
        out.writeVLong(node.storeId);
        out.writeVInt(node.keys);
        out.writeVLong(node.bytes);
        out.writeZLong(node.outlink);
        out.writeZLong(node.rightlink);
        if (node.rightsep == Bytes.POSITIVE_INFINITY) {
            out.writeByte(RIGHTSEP_INF);
        } else {
            out.writeByte(RIGHTSEP_BYTES);
            // writeArray(Bytes) uses Bytes.writeTo(ByteBuf) — zero-copy for
            // off-heap-backed rightsep keys (issue #497).
            out.writeArray(node.rightsep);
        }
    }

    /**
     * Two-pass node-metadata array reader (issue #411). The first pass
     * reads each node's plain fields and the rightsep raw bytes (or
     * INF marker) into local arrays so we can size and allocate a single
     * off-heap {@link IndexKeySlab} that holds every rightsep across
     * the snapshot chunk / delta. The second pass packs the rightseps
     * into the slab (gated by {@link IndexKeySlab#OFFHEAP_KEY_BYTES_THRESHOLD},
     * so tiny snapshots keep the on-heap fast path) and builds the
     * {@link BLinkNodeMetadata} array. Each rightsep returned via
     * {@link IndexKeySlab#wrap(int, int)} carries a strong reference to
     * the slab owner, anchoring the slab against premature GC for the
     * BLink's lifetime; long-lived split-key handoffs that promote one
     * of these rightseps later go through
     * {@code SizeEvaluator.detachSeparator}, copying the bytes onto the
     * heap and breaking the anchor (see issue #411).
     */
    private static BLinkNodeMetadata<Bytes>[] readNodeMetadataArray(ByteBufCursor in, int count)
            throws IOException {
        if (count < 0) {
            throw new IOException("corrupted node-metadata array: negative count " + count);
        }
        if (count > MAX_NODES_PER_CHUNK) {
            // Refuse before we allocate eight scratch arrays of size `count`.
            // A healthy chunk is bounded by DEFAULT_SNAPSHOT_CHUNK_SIZE
            // (50,000 nodes); anything beyond MAX_NODES_PER_CHUNK is almost
            // certainly a corrupted vint that would otherwise blow up the
            // GC at allocation time.
            throw new IOException("node-metadata array count " + count
                    + " exceeds MAX_NODES_PER_CHUNK (" + MAX_NODES_PER_CHUNK
                    + "); page is corrupted");
        }
        if (count == 0) {
            return newArray(0);
        }
        boolean[] leafArr = new boolean[count];
        long[] idArr = new long[count];
        long[] storeIdArr = new long[count];
        int[] keysArr = new int[count];
        long[] bytesArr = new long[count];
        long[] outlinkArr = new long[count];
        long[] rightlinkArr = new long[count];
        boolean[] rightsepInf = new boolean[count];
        byte[][] rightsepBytes = new byte[count][];
        long totalRightsepBytes = 0L;
        for (int i = 0; i < count; i++) {
            leafArr[i] = in.readBoolean();
            idArr[i] = in.readVLong();
            storeIdArr[i] = in.readVLong();
            keysArr[i] = in.readVInt();
            bytesArr[i] = in.readVLong();
            outlinkArr[i] = in.readZLong();
            rightlinkArr[i] = in.readZLong();
            byte sepKind = in.readByte();
            if (sepKind == RIGHTSEP_INF) {
                rightsepInf[i] = true;
            } else if (sepKind == RIGHTSEP_BYTES) {
                byte[] rs = in.readArray();
                if (rs == null) {
                    // The writer never emits a null rightsep array (it always
                    // calls writeArray on a non-null Bytes), so a null here
                    // means the on-disk page is corrupted. Fail fast with an
                    // actionable message.
                    throw new IOException("corrupted node metadata: null rightsep at index " + i);
                }
                rightsepBytes[i] = rs;
                totalRightsepBytes += (long) rs.length;
            } else {
                throw new IOException("unknown rightsep marker " + sepKind);
            }
        }
        IndexKeySlab slabOwner = (totalRightsepBytes >= IndexKeySlab.OFFHEAP_KEY_BYTES_THRESHOLD
                && totalRightsepBytes <= (long) Integer.MAX_VALUE)
                ? new IndexKeySlab(totalRightsepBytes,
                        HerdDBByteBufAllocators.indexPagesAllocator())
                : null;
        BLinkNodeMetadata<Bytes>[] result = newArray(count);
        for (int i = 0; i < count; i++) {
            Bytes rightsep;
            if (rightsepInf[i]) {
                rightsep = Bytes.POSITIVE_INFINITY;
            } else if (slabOwner != null) {
                int off = slabOwner.append(rightsepBytes[i]);
                rightsep = slabOwner.wrap(off, rightsepBytes[i].length);
            } else {
                rightsep = Bytes.from_array(rightsepBytes[i]);
            }
            result[i] = new BLinkNodeMetadata<>(leafArr[i], idArr[i], storeIdArr[i],
                    keysArr[i], bytesArr[i], outlinkArr[i], rightlinkArr[i], rightsep);
        }
        return result;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static BLinkNodeMetadata<Bytes>[] newArray(int size) {
        return (BLinkNodeMetadata<Bytes>[]) new BLinkNodeMetadata[size];
    }

    /**
     * Rebuild a full {@link BLinkMetadata} from a snapshot's node list plus a
     * sequence of deltas. The returned metadata also carries the anchor data
     * supplied by the caller (read from the manifest, not from the snapshot
     * page itself — the snapshot only carries node metadata).
     */
    static BLinkMetadata<Bytes> rebuild(
            Map<Long, BLinkNodeMetadata<Bytes>> byNodeId,
            long nextID, long fast, int fastHeight, long top, int topHeight,
            long first, long values
    ) {
        List<BLinkNodeMetadata<Bytes>> list = new ArrayList<>(byNodeId.values());
        return new BLinkMetadata<>(nextID, fast, fastHeight, top, topHeight, first, values, list);
    }
}
