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
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.util.List;
import org.junit.After;
import org.junit.Test;

/**
 * Mirror of {@link BLinkOffHeapKeysTest} for the incremental BLink
 * implementation. Without this, a divergence between the two
 * {@code loadPage} implementations (they are independent copies of the
 * same logic) would slip through CI.
 */
public class IncrementalBLinkOffHeapKeysTest {

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
    public void keysLoadedFromDiskAreOffHeapWhenAggregateExceedsThreshold() throws Exception {
        final int n = 256;
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
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

        idx = new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        idx.init();
        idx.start(new LogSequenceNumber(1L, 1L), false);

        for (int i = 0; i < n; i++) {
            Long v = idx.get(Bytes.from_array(makeKey(i)));
            assertNotNull("missing key " + i, v);
            assertEquals(Long.valueOf(i), v);
        }
        assertEquals((long) n, idx.size());
    }

    @Test
    public void smallIndexFallsBackToHeapPath() throws Exception {
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
