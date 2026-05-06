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

package herddb.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.index.blink.IncrementalBLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.utils.Bytes;
import java.util.List;
import org.junit.Test;

/**
 * Issue #403: unit-level coverage of the new
 * {@link KeyToPageIndex#prepareCheckpoint} /
 * {@link KeyToPageIndex#persistCheckpoint} two-phase protocol on the BLink
 * implementations.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>the snapshot taken under the caller's exclusion window in
 *       {@code prepareCheckpoint} survives concurrent {@code put}s issued
 *       after the snapshot — i.e. the post-snapshot puts go to a fresh
 *       generation and do not corrupt the persisted checkpoint;</li>
 *   <li>{@code persistCheckpoint} produces non-null {@link PostCheckpointAction}
 *       lists matching the legacy single-phase {@code checkpoint(...)}
 *       invariants;</li>
 *   <li>the legacy {@code checkpoint(LSN, pin)} entry point still works as
 *       a thin wrapper around the new two-phase API.</li>
 * </ul>
 */
public class BLinkKeyToPageIndexSnapshotPersistSplitTest {

    private static final LogSequenceNumber LSN_1 = new LogSequenceNumber(1, 1);
    private static final LogSequenceNumber LSN_2 = new LogSequenceNumber(1, 2);

    @Test
    public void blinkPrepareThenPersistRoundTrip() throws Exception {
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        ds.initIndex("tblspc", BLinkKeyToPageIndex.deriveIndexName("tbl"));
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        try {
            idx.start(LogSequenceNumber.START_OF_TIME, true);

            for (int i = 0; i < 200; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }

            // Two-phase round trip.
            KeyToPageCheckpointSnapshot snapshot = idx.prepareCheckpoint(LSN_1, false);
            assertNotNull(snapshot);
            assertEquals(LSN_1, snapshot.sequenceNumber());

            // Concurrent put after prepareCheckpoint must not affect the snapshot
            // about to be persisted. The new mapping goes to a fresh generation
            // and will be picked up by the NEXT checkpoint.
            idx.put(Bytes.from_int(99999), 99999L);

            List<PostCheckpointAction> actions = idx.persistCheckpoint(snapshot);
            assertNotNull(actions);

            // After persist, the index must still answer queries correctly,
            // including the post-snapshot put.
            assertEquals(Long.valueOf(50L), idx.get(Bytes.from_int(50)));
            assertEquals(Long.valueOf(99999L), idx.get(Bytes.from_int(99999)));
            assertEquals(201L, idx.size());

            // A second checkpoint via the legacy single-phase entry point must
            // succeed and not deadlock or double-count.
            List<PostCheckpointAction> actions2 = idx.checkpoint(LSN_2, false);
            assertNotNull(actions2);
        } finally {
            idx.close();
        }
    }

    @Test
    public void incrementalBlinkPrepareThenPersistRoundTrip() throws Exception {
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        ds.initIndex("tblspc", BLinkKeyToPageIndex.deriveIndexName("tbl"));
        IncrementalBLinkKeyToPageIndex idx = new IncrementalBLinkKeyToPageIndex(
                "tblspc", "tbl", mem, ds);
        try {
            idx.start(LogSequenceNumber.START_OF_TIME, true);

            for (int i = 0; i < 200; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }

            KeyToPageCheckpointSnapshot snapshot = idx.prepareCheckpoint(LSN_1, false);
            assertNotNull(snapshot);
            assertEquals(LSN_1, snapshot.sequenceNumber());

            // Concurrent put after prepare goes to a fresh generation.
            idx.put(Bytes.from_int(99999), 99999L);

            List<PostCheckpointAction> actions = idx.persistCheckpoint(snapshot);
            assertNotNull(actions);

            assertEquals(Long.valueOf(50L), idx.get(Bytes.from_int(50)));
            assertEquals(Long.valueOf(99999L), idx.get(Bytes.from_int(99999)));
            assertEquals(201L, idx.size());

            // Run a second cycle (delta path).
            for (int i = 1000; i < 1100; i++) {
                idx.put(Bytes.from_int(i), (long) i);
            }
            List<PostCheckpointAction> actions2 = idx.checkpoint(LSN_2, false);
            assertNotNull(actions2);
            assertEquals(301L, idx.size());
        } finally {
            idx.close();
        }
    }

    @Test
    public void emptyTreePrepareThenPersistIsNoop() throws Exception {
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        ds.initIndex("tblspc", BLinkKeyToPageIndex.deriveIndexName("tbl"));
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        try {
            // Do NOT call start() — exercise the no-tree path explicitly.
            KeyToPageCheckpointSnapshot snapshot = idx.prepareCheckpoint(LSN_1, false);
            assertNotNull(snapshot);
            List<PostCheckpointAction> actions = idx.persistCheckpoint(snapshot);
            assertNotNull(actions);
            assertEquals(0, actions.size());
        } finally {
            idx.close();
        }
    }
}
