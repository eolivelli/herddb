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
 * Verifies issue #411: at every BLink split / half-merge, the separator
 * promoted to {@code rightsep} is detached from the donor's per-page
 * {@link herddb.utils.IndexKeySlab} via
 * {@link herddb.utils.Bytes#materialiseAndDetach()}, so a single separator
 * never pins the slab indefinitely.
 *
 * <p>The test inserts enough keys to force splits, reloads the BLink
 * (which exercises {@code half_merge} during page eviction churn under
 * load), and asserts that no live node's rightsep is off-heap-backed —
 * meaning every separator went through the detach hook.
 */
public class BLinkSeparatorDetachTest {

    private BLinkKeyToPageIndex idx;
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
    public void noRightsepStaysOffHeapAfterSplits() throws Exception {
        // To exercise the issue #411 detach hook against an off-heap lastKey
        // we need (a) multiple leaves (so splits actually fire) and (b) each
        // leaf's aggregate key bytes ≥ IndexKeySlab.OFFHEAP_KEY_BYTES_THRESHOLD
        // (so BLinkIndexDataStorageImpl.loadPage slab-packs the keys, making
        // lastKey off-heap at split time). 64-byte keys + 32 KiB pages ⇒
        // ~178 entries/leaf; after a leaf split each half holds ~89 entries
        // × 64 B = ~5.7 KiB worth of keys, well above the 4 KiB slab
        // threshold ⇒ slab-pack fires per leaf on reload. With 1024 initial
        // keys we get ~6 leaves; after reload we add 512 more keys so
        // post-reload splits fire on leaves already populated with off-heap
        // shared-slab keys, exercising detachSeparator's off-heap branch.
        final int n = 1024;
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), 32L * 1024L);
        ds = new MemoryDataStorageManager();
        idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
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

        // Reload — disk-load path repopulates rightseps via the legacy
        // BLinkMetadata.MetadataSerializer (which emits each rightsep as
        // an on-heap byte[]). LEAF KEYS, however, come back off-heap from
        // BLinkIndexDataStorageImpl.loadPage when the per-page key bytes
        // exceed the slab threshold — so post-reload splits will see an
        // off-heap lastKey at the rightsep handoff site, exercising
        // detachSeparator's off-heap branch.
        idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(new LogSequenceNumber(1L, 1L), false);

        // Round-trip lookup correctness sanity-check: detach must not
        // change comparison semantics.
        for (int i = 0; i < n; i++) {
            Long v = idx.get(Bytes.from_array(makeKey(i)));
            assertNotNull("missing key " + i, v);
            assertEquals(Long.valueOf(i), v);
        }
        assertEquals((long) n, idx.size());

        // Force more splits / merges by inserting an interleaved batch so
        // detachSeparator must fire on the live tree (not just on metadata
        // load). Without the hook a multi-byte rightsep would be off-heap.
        for (int i = n; i < n + n / 2; i++) {
            idx.put(Bytes.from_array(makeKey(i)), (long) i);
        }

        // The test's invariant only means anything if splits actually fired
        // — guard against a too-generous future page size silently making
        // this test vacuous (one leaf ⇒ no rightseps ⇒ -1 trivially passes).
        int loadedNodes = BLinkTestReflection.nodeCount(idx);
        assertTrue("test setup must produce splits so detachSeparator runs (got "
                + loadedNodes + " loaded nodes; need ≥ 2)",
                loadedNodes >= 2);

        // Sanity: at least one Bytes key in the tree is off-heap (the keys
        // themselves still live in their per-page slabs — we are NOT
        // detaching every key, only the separators). This also confirms
        // the lastKey at split time was off-heap, which is the only
        // configuration where detachSeparator's branch is meaningful.
        assertTrue("expected some leaf key to remain off-heap-backed",
                BLinkTestReflection.anyKeyOffHeap(idx));

        // Core invariant: no live rightsep is off-heap-backed. The detach
        // hook must have fired at every split / half-merge promotion.
        int offHeapRightSepIdx = BLinkTestReflection.firstOffHeapRightSep(idx);
        assertEquals(
                "issue #411: every BLink rightsep must be on-heap after detach (offending node index="
                        + offHeapRightSepIdx + ")",
                -1, offHeapRightSepIdx);
    }

    private static byte[] makeKey(int i) {
        // Deterministic 64-byte key. The 4-byte big-endian int prefix
        // controls sort order; the trailing bytes pad each key out so a
        // 32 KiB leaf fills with enough raw key bytes (~89 keys × 64 B
        // ≈ 5.7 KiB) for BLinkIndexDataStorageImpl.loadPage to slab-pack
        // on reload.
        byte[] out = new byte[64];
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
