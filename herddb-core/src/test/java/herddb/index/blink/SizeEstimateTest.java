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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.utils.ByteBufDataOutput;
import herddb.utils.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Verifies that the {@link IncrementalBLinkPageCodec} size-estimate methods
 * never under-count the actual bytes written by the corresponding write method.
 *
 * <p>For purely VInt/VLong-encoded payloads (no zig-zag fields), the estimate
 * must be exact. For payloads that include zig-zag-encoded fields (outlink /
 * rightlink in node metadata), the estimate is conservative
 * ({@code estimate >= actual}).
 */
public class SizeEstimateTest {

    // -------------------------------------------------------------------------
    // Constants mirrored from BLinkKeyToPageIndex (package-private there)
    // -------------------------------------------------------------------------

    private static final byte INNER_NODE_PAGE = 1;
    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;

    // -------------------------------------------------------------------------
    // nodePageSizeEstimate — no POSITIVE_INFINITY key: must be exact
    // -------------------------------------------------------------------------

    @Test
    public void nodePageSizeEstimate_noInfKey_isExact() throws IOException {
        Map<Bytes, Long> data = new LinkedHashMap<>();
        // 1-byte VLong page id (< 128), 1-byte key length
        data.put(Bytes.from_string("a"), 1L);
        // 2-byte key length (len=200 → varIntLen=2), large page id
        data.put(Bytes.from_array(new byte[200]), 0x3FFFL);
        // 4-byte VLong page id, short key
        data.put(Bytes.from_string("longerkey"), 0x10000000L);

        ByteBuf buf = Unpooled.buffer(512);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            writeNodePage(out, data, INNER_NODE_PAGE);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.nodePageSizeEstimate(data);
            assertEquals(
                    "nodePageSizeEstimate (no INF key) must be exact: actual=" + actual,
                    actual, estimate);
        } finally {
            buf.release();
        }
    }

    @Test
    public void nodePageSizeEstimate_withInfKey_isConservative() throws IOException {
        Map<Bytes, Long> data = new LinkedHashMap<>();
        data.put(Bytes.from_string("key"), 42L);
        // pageId=99 → 1-byte VLong, but estimate uses 9 bytes for INF entries
        data.put(Bytes.POSITIVE_INFINITY, 99L);

        ByteBuf buf = Unpooled.buffer(256);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            writeNodePage(out, data, INNER_NODE_PAGE);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.nodePageSizeEstimate(data);
            assertTrue(
                    "nodePageSizeEstimate (with INF key) must be >= actual: "
                            + "estimate=" + estimate + " actual=" + actual,
                    estimate >= actual);
        } finally {
            buf.release();
        }
    }

    @Test
    public void nodePageSizeEstimate_emptyMap_isExact() throws IOException {
        Map<Bytes, Long> data = new LinkedHashMap<>();

        ByteBuf buf = Unpooled.buffer(16);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            writeNodePage(out, data, INNER_NODE_PAGE);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.nodePageSizeEstimate(data);
            assertEquals("nodePageSizeEstimate (empty map) must be exact", actual, estimate);
        } finally {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // nodeMetadataSizeEstimate — always conservative (zlong worst-case)
    // -------------------------------------------------------------------------

    @Test
    public void nodeMetadataSizeEstimate_normalRightsep_isConservative() throws IOException {
        BLinkNodeMetadata<Bytes> node = new BLinkNodeMetadata<>(
                true,                    // leaf
                10L,                     // id
                20L,                     // storeId
                5,                       // keys
                1024L,                   // bytes
                0L,                      // outlink (zlong → 1 byte actual, 9 estimate)
                0L,                      // rightlink (same)
                Bytes.from_string("rightsep-key")
        );
        // Verify via snapshotChunk round-trip (writeNodeMetadata is private)
        ByteBuf buf = Unpooled.buffer(256);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeSnapshotChunk(out, 1L, 0, 1,
                    Collections.singletonList(node));
            int chunkActual = buf.writerIndex();
            int chunkEstimate = IncrementalBLinkPageCodec.snapshotChunkSizeEstimate(
                    1L, 0, 1, Collections.singletonList(node));
            assertTrue(
                    "snapshotChunkSizeEstimate must be >= actual for node with normal rightsep: "
                            + "estimate=" + chunkEstimate + " actual=" + chunkActual,
                    chunkEstimate >= chunkActual);
            // sanity: the per-node estimate itself is positive
            int nodeEstimate = IncrementalBLinkPageCodec.nodeMetadataSizeEstimate(node);
            assertTrue("nodeMetadataSizeEstimate must be positive", nodeEstimate > 0);
        } finally {
            buf.release();
        }
    }

    @Test
    public void nodeMetadataSizeEstimate_nullRightsep_isConservative() throws IOException {
        BLinkNodeMetadata<Bytes> node = new BLinkNodeMetadata<>(
                false, 1L, 2L, 3, 100L, 0L, 0L, null
        );
        ByteBuf buf = Unpooled.buffer(256);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeSnapshotChunk(out, 1L, 0, 1,
                    Collections.singletonList(node));
            int chunkActual = buf.writerIndex();
            int chunkEstimate = IncrementalBLinkPageCodec.snapshotChunkSizeEstimate(
                    1L, 0, 1, Collections.singletonList(node));
            assertTrue(
                    "snapshotChunkSizeEstimate must be >= actual for node with null rightsep: "
                            + "estimate=" + chunkEstimate + " actual=" + chunkActual,
                    chunkEstimate >= chunkActual);
            // per-node sanity
            int nodeEstimate = IncrementalBLinkPageCodec.nodeMetadataSizeEstimate(node);
            assertTrue("nodeMetadataSizeEstimate (null rightsep) must be positive", nodeEstimate > 0);
        } finally {
            buf.release();
        }
    }

    @Test
    public void nodeMetadataSizeEstimate_infRightsep_isConservative() throws IOException {
        BLinkNodeMetadata<Bytes> node = new BLinkNodeMetadata<>(
                true, 5L, 6L, 0, 0L, 0L, 0L, Bytes.POSITIVE_INFINITY
        );
        ByteBuf buf = Unpooled.buffer(256);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeSnapshotChunk(out, 1L, 0, 1,
                    Collections.singletonList(node));
            int chunkActual = buf.writerIndex();
            int chunkEstimate = IncrementalBLinkPageCodec.snapshotChunkSizeEstimate(
                    1L, 0, 1, Collections.singletonList(node));
            assertTrue(
                    "snapshotChunkSizeEstimate must be >= actual for node with INF rightsep: "
                            + "estimate=" + chunkEstimate + " actual=" + chunkActual,
                    chunkEstimate >= chunkActual);
            int nodeEstimate = IncrementalBLinkPageCodec.nodeMetadataSizeEstimate(node);
            assertTrue("nodeMetadataSizeEstimate (INF rightsep) must be positive", nodeEstimate > 0);
        } finally {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // snapshotChunkSizeEstimate
    // -------------------------------------------------------------------------

    @Test
    public void snapshotChunkSizeEstimate_multipleNodes_isConservative() throws IOException {
        List<BLinkNodeMetadata<Bytes>> nodes = buildNodeList(6);

        ByteBuf buf = Unpooled.buffer(1024);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeSnapshotChunk(out, 7L, 1, 3, nodes);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.snapshotChunkSizeEstimate(7L, 1, 3, nodes);
            assertTrue(
                    "snapshotChunkSizeEstimate must be >= actual: "
                            + "estimate=" + estimate + " actual=" + actual,
                    estimate >= actual);
        } finally {
            buf.release();
        }
    }

    @Test
    public void snapshotChunkSizeEstimate_emptyList_isConservative() throws IOException {
        List<BLinkNodeMetadata<Bytes>> nodes = Collections.emptyList();

        ByteBuf buf = Unpooled.buffer(32);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeSnapshotChunk(out, 1L, 0, 1, nodes);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.snapshotChunkSizeEstimate(1L, 0, 1, nodes);
            assertTrue(
                    "snapshotChunkSizeEstimate (empty) must be >= actual: "
                            + "estimate=" + estimate + " actual=" + actual,
                    estimate >= actual);
        } finally {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // deltaSizeEstimate
    // -------------------------------------------------------------------------

    @Test
    public void deltaSizeEstimate_withData_isConservative() throws IOException {
        List<BLinkNodeMetadata<Bytes>> upserted = buildNodeList(4);
        long[] removedNodeIds = {1L, 2L, 127L, 128L, Long.MAX_VALUE};
        long[] deadStoreIds   = {10L, 9999L};

        ByteBuf buf = Unpooled.buffer(1024);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeDelta(out, 3L, upserted, removedNodeIds, deadStoreIds);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.deltaSizeEstimate(
                    3L, upserted, removedNodeIds, deadStoreIds);
            assertTrue(
                    "deltaSizeEstimate must be >= actual: "
                            + "estimate=" + estimate + " actual=" + actual,
                    estimate >= actual);
        } finally {
            buf.release();
        }
    }

    @Test
    public void deltaSizeEstimate_empty_isConservative() throws IOException {
        List<BLinkNodeMetadata<Bytes>> upserted = Collections.emptyList();
        long[] removedNodeIds = {};
        long[] deadStoreIds   = {};

        ByteBuf buf = Unpooled.buffer(32);
        try {
            ByteBufDataOutput out = new ByteBufDataOutput(buf);
            IncrementalBLinkPageCodec.writeDelta(out, 1L, upserted, removedNodeIds, deadStoreIds);
            int actual = buf.writerIndex();
            int estimate = IncrementalBLinkPageCodec.deltaSizeEstimate(
                    1L, upserted, removedNodeIds, deadStoreIds);
            assertTrue(
                    "deltaSizeEstimate (empty) must be >= actual: "
                            + "estimate=" + estimate + " actual=" + actual,
                    estimate >= actual);
        } finally {
            buf.release();
        }
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    /**
     * Writes the BLink node-page body using the same wire format as
     * {@code BLinkKeyToPageIndex.createPage} / {@code IncrementalBLinkKeyToPageIndex}
     * / {@code PersistentVectorStore.writePage}.
     */
    private static void writeNodePage(ByteBufDataOutput out, Map<Bytes, Long> data, byte type)
            throws IOException {
        out.writeVLong(1L); // PAGE_VERSION
        out.writeVLong(0L); // PAGE_FLAGS
        out.writeByte(type);
        for (Map.Entry<Bytes, Long> e : data.entrySet()) {
            Bytes k = e.getKey();
            if (k == Bytes.POSITIVE_INFINITY) {
                out.writeByte(NODE_PAGE_INF_BLOCK);
                out.writeVLong(e.getValue());
            } else {
                out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                out.writeArray(k);
                out.writeVLong(e.getValue());
            }
        }
        out.writeByte(NODE_PAGE_END_BLOCK);
    }

    /**
     * Builds a list of {@link BLinkNodeMetadata} instances for use in
     * snapshot-chunk and delta tests. The last node always gets
     * {@link Bytes#POSITIVE_INFINITY} as its rightsep (tree right-most node);
     * all others get a short ASCII key.
     */
    private static List<BLinkNodeMetadata<Bytes>> buildNodeList(int count) {
        List<BLinkNodeMetadata<Bytes>> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Bytes rightsep = (i == count - 1)
                    ? Bytes.POSITIVE_INFINITY
                    : Bytes.from_string("key-" + i);
            list.add(new BLinkNodeMetadata<>(
                    i % 2 == 0,              // leaf
                    (long) i,                // id
                    (long) (i + 1000),       // storeId
                    i * 10,                  // keys
                    (long) (i * 512),        // bytes
                    i == 0 ? 0L : (long) (i - 1),  // outlink
                    (long) (i + 1),                 // rightlink
                    rightsep
            ));
        }
        return list;
    }
}
