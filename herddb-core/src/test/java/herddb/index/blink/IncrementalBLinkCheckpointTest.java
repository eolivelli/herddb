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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.core.MemoryManager;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import org.junit.Test;

/**
 * Focused tests for {@link IncrementalBLinkKeyToPageIndex} covering
 * checkpoint / recovery behaviour, format-mismatch detection, and the basic
 * manifest contents after each checkpoint.
 */
public class IncrementalBLinkCheckpointTest {

    private static MemoryManager newMem() {
        return new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
    }

    @Test
    public void checkpointThenRecoverRestoresAllKeys() throws Exception {
        MemoryManager mem = newMem();
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        IncrementalBLinkKeyToPageIndex idx = new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        idx.start(LogSequenceNumber.START_OF_TIME, true);
        for (int i = 0; i < 200; i++) {
            idx.put(Bytes.from_int(i), (long) i);
        }
        LogSequenceNumber lsn1 = new LogSequenceNumber(1, 100);
        idx.checkpoint(lsn1, false);

        IncrementalBLinkManifest m1 = idx.getCurrentManifest();
        assertNotNull(m1);
        assertEquals(1L, m1.epoch);
        assertFalse("first checkpoint should have written a snapshot",
                m1.snapshotChunks.isEmpty());
        assertTrue("first checkpoint should have no delta chain",
                m1.deltaChain.isEmpty());

        // Reopen and verify every key is still there.
        idx.close();
        IncrementalBLinkKeyToPageIndex idx2 = new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        idx2.start(lsn1, false);
        assertEquals(200L, idx2.size());
        for (int i = 0; i < 200; i++) {
            assertEquals(Long.valueOf(i), idx2.get(Bytes.from_int(i)));
        }
        idx2.close();
    }

    @Test
    public void secondCheckpointProducesDeltaNotSnapshot() throws Exception {
        MemoryManager mem = newMem();
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        IncrementalBLinkKeyToPageIndex idx = new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        idx.start(LogSequenceNumber.START_OF_TIME, true);

        for (int i = 0; i < 50; i++) {
            idx.put(Bytes.from_int(i), (long) i);
        }
        idx.checkpoint(new LogSequenceNumber(1, 1), false);
        IncrementalBLinkManifest m1 = idx.getCurrentManifest();

        // Another mutation + checkpoint — delta path.
        idx.put(Bytes.from_int(9999), 9999L);
        idx.checkpoint(new LogSequenceNumber(1, 2), false);

        IncrementalBLinkManifest m2 = idx.getCurrentManifest();
        assertEquals(m1.epoch + 1, m2.epoch);
        // Snapshot reused — same chunk references.
        assertEquals(m1.snapshotChunks.size(), m2.snapshotChunks.size());
        if (!m1.snapshotChunks.isEmpty()) {
            assertEquals(m1.snapshotChunks.get(0).pageId, m2.snapshotChunks.get(0).pageId);
        }
        // Delta chain grew by one.
        assertEquals(m1.deltaChain.size() + 1, m2.deltaChain.size());

        idx.close();
    }

    @Test
    public void recoveryReplaysDeltasOverSnapshot() throws Exception {
        MemoryManager mem = newMem();
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        IncrementalBLinkKeyToPageIndex idx = new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        idx.start(LogSequenceNumber.START_OF_TIME, true);

        for (int i = 0; i < 30; i++) {
            idx.put(Bytes.from_int(i), (long) i);
        }
        idx.checkpoint(new LogSequenceNumber(1, 1), false);

        // Insertions → delta 1
        for (int i = 30; i < 60; i++) {
            idx.put(Bytes.from_int(i), (long) i);
        }
        idx.checkpoint(new LogSequenceNumber(1, 2), false);

        // Delete some keys → delta 2
        for (int i = 10; i < 20; i++) {
            idx.remove(Bytes.from_int(i));
        }
        LogSequenceNumber last = new LogSequenceNumber(1, 3);
        idx.checkpoint(last, false);
        idx.close();

        // Recover from the last LSN and verify final state.
        IncrementalBLinkKeyToPageIndex r = new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        r.start(last, false);
        assertEquals(50L, r.size()); // 60 inserted - 10 deleted
        for (int i = 0; i < 10; i++) {
            assertEquals(Long.valueOf(i), r.get(Bytes.from_int(i)));
        }
        for (int i = 10; i < 20; i++) {
            assertEquals("key " + i + " should have been removed", null, r.get(Bytes.from_int(i)));
        }
        for (int i = 20; i < 60; i++) {
            assertEquals(Long.valueOf(i), r.get(Bytes.from_int(i)));
        }
        r.close();
    }

    @Test
    public void legacyFormatTriggersClearError() throws Exception {
        MemoryManager mem = newMem();
        MemoryDataStorageManager ds = new MemoryDataStorageManager();

        // Write data with the legacy impl.
        BLinkKeyToPageIndex legacy = new BLinkKeyToPageIndex("ts", "tbl", mem, ds);
        legacy.start(LogSequenceNumber.START_OF_TIME, true);
        for (int i = 0; i < 10; i++) {
            legacy.put(Bytes.from_int(i), (long) i);
        }
        LogSequenceNumber lsn = new LogSequenceNumber(1, 1);
        legacy.checkpoint(lsn, false);
        legacy.close();

        // Now attempt to open with the incremental impl at the same LSN.
        IncrementalBLinkKeyToPageIndex incr =
                new IncrementalBLinkKeyToPageIndex("ts", "tbl", mem, ds);
        try {
            incr.start(lsn, false);
            fail("expected DataStorageManagerException");
        } catch (DataStorageManagerException expected) {
            String msg = expected.getMessage();
            assertNotNull(msg);
            assertTrue("message should mention 'legacy': " + msg, msg.contains("legacy"));
            assertTrue("message should mention the system property: " + msg,
                    msg.contains("herddb.index.pk.mode"));
        }
    }
}
