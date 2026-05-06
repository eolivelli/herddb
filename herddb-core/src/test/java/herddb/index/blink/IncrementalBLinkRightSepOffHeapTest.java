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

        // Stronger invariant: EVERY non-INFINITY rightsep loaded from the
        // snapshot must be off-heap-backed. A regression where the second
        // pass slab-packed only a fraction of rightseps (e.g. an off-by-one
        // or partial loop) would still satisfy "at least one off-heap" but
        // be caught here.
        int onHeapNonInfIdx = BLinkTestReflection.firstOnHeapNonInfRightSep(idx);
        assertEquals(
                "issue #411: every non-INFINITY rightsep loaded from the codec must be"
                        + " off-heap-backed when the aggregate exceeds the slab threshold"
                        + " (offending node index=" + onHeapNonInfIdx + ")",
                -1, onHeapNonInfIdx);
        // Also assert at least one was non-INF, otherwise the test is vacuous.
        assertTrue("test must observe ≥ 1 non-INF rightsep to be meaningful",
                BLinkTestReflection.countNonInfRightSeps(idx) >= 1);
    }

    @Test
    public void smallSnapshotFallsBackToHeap() throws Exception {
        // We need a non-trivial number of leaves (so several non-INF
        // rightseps are persisted) but the aggregate rightsep bytes must
        // stay BELOW the 4 KiB slab threshold. With 4-byte keys (~96 B
        // per BLink entry) and a 1 KiB page (~7 entries/leaf) we can fit
        // ~7 leaves with 50 keys total ⇒ ~6 non-INF rightseps × 4 B ≈
        // 24 B aggregate ≪ 4 KiB. The codec must take the on-heap path.
        final int n = 50;
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), 1024L);
        ds = new MemoryDataStorageManager();
        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        for (int i = 0; i < n; i++) {
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
        for (int i = 0; i < n; i++) {
            assertEquals(Long.valueOf(i), idx.get(Bytes.from_int(i)));
        }
        assertEquals((long) n, idx.size());

        // Pre-condition: the test setup must produce real (non-INF) rightseps,
        // otherwise the assertion below is vacuous (a single-leaf tree has
        // only POSITIVE_INFINITY, which the on-heap helper skips).
        int nonInf = BLinkTestReflection.countNonInfRightSeps(idx);
        assertTrue("test setup must produce ≥ 1 non-INF rightsep so the on-heap"
                + " fallback assertion is meaningful (got " + nonInf + ")",
                nonInf >= 1);

        // Below-threshold invariant: NO non-INFINITY rightsep is off-heap;
        // the codec must have taken the on-heap fallback path.
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
