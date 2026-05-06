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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.util.List;
import org.junit.After;
import org.junit.Test;

/**
 * Issue #411: when {@link IncrementalBLinkPageCodec} loads a snapshot
 * chunk / delta whose aggregate rightsep bytes exceed
 * {@link herddb.utils.IndexKeySlab#OFFHEAP_KEY_BYTES_THRESHOLD}, every
 * non-INFINITY rightsep must come back as off-heap-backed
 * ({@link Bytes#isOffHeap()} returns {@code true}). Below the threshold
 * the codec must transparently fall back to on-heap allocations.
 *
 * <p>Round-trip lookup correctness is also verified at every reload:
 * the slab-pack must not change comparison semantics.
 */
public class IncrementalBLinkRightSepOffHeapTest {

    private IncrementalBLinkKeyToPageIndex idx;
    private MemoryDataStorageManager ds;

    @After
    public void closeResources() {
        try {
            if (idx != null) {
                idx.close();
            }
        } finally {
            if (ds != null) {
                try {
                    ds.close();
                } catch (Exception ignored) {
                    // not actionable in test teardown
                }
            }
        }
    }

    @Test
    public void rightsepsLoadedFromSnapshotAreOffHeapWhenAggregateExceedsThreshold() throws Exception {
        // We need MANY leaves so that aggregate rightsep bytes exceed the
        // 4 KiB IndexKeySlab threshold. With the per-entry overhead (~120 B
        // for 24-B keys + Long values) and a 2 KiB page, each leaf holds
        // ~14 entries. 4096 keys ⇒ ~290 leaves ⇒ ~290 × 24 B ≈ 7 KiB of
        // rightsep bytes — well above the threshold.
        final int n = 4096;
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), 2048L);
        ds = new MemoryDataStorageManager();
        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        for (int i = 0; i < n; i++) {
            idx.put(Bytes.from_array(makeKey(i)), (long) i);
        }
        List<PostCheckpointAction> actions = idx.checkpoint(new LogSequenceNumber(1L, 1L), false);
        for (PostCheckpointAction a : actions) {
            a.run();
        }
        idx.close();

        // Reload via the incremental codec: this exercises
        // IncrementalBLinkPageCodec.readSnapshotChunk (and possibly readDelta),
        // which is the path migrated by issue #411.
        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(new LogSequenceNumber(1L, 1L), false);

        // Round-trip lookup: bytes survive the slab-pack round-trip.
        for (int i = 0; i < n; i++) {
            Long v = idx.get(Bytes.from_array(makeKey(i)));
            assertNotNull("missing key " + i, v);
            assertEquals(Long.valueOf(i), v);
        }
        assertEquals((long) n, idx.size());

        // Strong invariant: at least one rightsep loaded from the snapshot
        // chunk is off-heap-backed. Without issue #411's slab-pack at
        // IncrementalBLinkPageCodec, every rightsep would arrive on-heap.
        assertTrue(
                "issue #411: at least one rightsep loaded from the incremental codec must be off-heap",
                BLinkTestReflection.anyRightSepOffHeap(idx));
    }

    @Test
    public void smallSnapshotFallsBackToHeap() throws Exception {
        // 4 tiny keys, well below the 4 KiB rightsep slab threshold.
        // The slab-pack path must NOT fire; rightseps come back on-heap.
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        ds = new MemoryDataStorageManager();
        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        for (int i = 0; i < 4; i++) {
            idx.put(Bytes.from_int(i), (long) i);
        }
        List<PostCheckpointAction> actions = idx.checkpoint(new LogSequenceNumber(1L, 1L), false);
        for (PostCheckpointAction a : actions) {
            a.run();
        }
        idx.close();

        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(new LogSequenceNumber(1L, 1L), false);
        for (int i = 0; i < 4; i++) {
            assertEquals(Long.valueOf(i), idx.get(Bytes.from_int(i)));
        }
        assertEquals(4L, idx.size());

        // No off-heap rightseps under the threshold (the only rightsep is
        // the root's POSITIVE_INFINITY anyway, but the helper handles that).
        assertEquals("below-threshold codec must not pack rightseps off-heap",
                -1, BLinkTestReflection.firstOffHeapRightSep(idx));
    }

    private static byte[] makeKey(int i) {
        byte[] out = new byte[24];
        out[0] = (byte) (i >>> 24);
        out[1] = (byte) (i >>> 16);
        out[2] = (byte) (i >>> 8);
        out[3] = (byte) i;
        for (int j = 4; j < out.length; j++) {
            out[j] = (byte) ((i * 31 + j) & 0xff);
        }
        return out;
    }
}
