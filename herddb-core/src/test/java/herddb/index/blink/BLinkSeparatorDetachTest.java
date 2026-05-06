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
        // 1024 keys × ~24 bytes each = ~24 KiB → many leaf splits. Each split
        // promotes the donor leaf's lastKey to rightsep; without the detach
        // hook, those rightseps would all be off-heap-backed (slab-anchored
        // to the donor's page slab).
        final int n = 1024;
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
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

        // Reload — disk-load path repopulates rightseps via metadata. The
        // detach hook fires later only on subsequent splits; on a fresh load
        // rightseps come straight off the metadata which (with this build)
        // does NOT slab-pack them (BLinkMetadata.MetadataSerializer emits
        // each rightsep as its own on-heap byte[]). They stay on-heap
        // throughout this test, which is exactly what we assert.
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
        // Sanity: at least one Bytes key in the tree is off-heap (the keys
        // themselves still live in their per-page slabs — we are NOT
        // detaching every key, only the separators).
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
        // Deterministic 24-byte key matching BLinkOffHeapKeysTest's helper
        // so we share the same ~6 KiB-per-page slab profile.
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
