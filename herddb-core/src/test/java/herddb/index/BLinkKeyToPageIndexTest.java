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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import herddb.core.AbstractIndexManager;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.blink.BLink;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.mem.MemoryDataStorageManager;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.storage.DataStorageManagerException;
import herddb.utils.Bytes;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.junit.Test;

/**
 * Base test suite for {@link  BLinkKeyToPageIndex}
 *
 * @author diego.salvi
 */
public class BLinkKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {

        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);

        return new ResourceCloseKeyToPageIndex(idx, ds);

    }

    private final class ResourceCloseKeyToPageIndex implements KeyToPageIndex {

        KeyToPageIndex delegate;
        AutoCloseable[] closeables;

        public ResourceCloseKeyToPageIndex(KeyToPageIndex index, AutoCloseable... closeables) {
            this.delegate = index;
            this.closeables = closeables;
        }

        @Override
        public void close() {
            delegate.close();

            IllegalStateException exception = null;
            for (AutoCloseable closeable : closeables) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = new IllegalStateException("Failed to properly close resources", e);
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }

            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public long getUsedMemory() {
            return delegate.getUsedMemory();
        }

        @Override
        public boolean requireLoadAtStartup() {
            return delegate.requireLoadAtStartup();
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void start(LogSequenceNumber sequenceNumber, boolean created) throws DataStorageManagerException {
            delegate.start(sequenceNumber, created);
        }

        @Override
        public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
                throws DataStorageManagerException {
            return delegate.checkpoint(sequenceNumber, pin);
        }

        @Override
        public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
            delegate.unpinCheckpoint(sequenceNumber);
        }

        @Override
        public void truncate() {
            delegate.truncate();
        }

        @Override
        public void dropData() {
            delegate.dropData();
        }

        @Override
        public void init() throws DataStorageManagerException {
            delegate.init();
        }

        @Override
        public Stream<Entry<Bytes, Long>> scanner(
                IndexOperation operation, StatementEvaluationContext context,
                TableContext tableContext, AbstractIndexManager index
        )
                throws DataStorageManagerException, StatementExecutionException {
            return delegate.scanner(operation, context, tableContext, index);
        }

        @Override
        public void put(Bytes key, Long currentPage) {
            delegate.put(key, currentPage);
        }

        @Override
        public boolean put(Bytes key, Long newPage, Long expectedPage) {
            return delegate.put(key, newPage, expectedPage);
        }

        @Override
        public boolean containsKey(Bytes key) {
            return delegate.containsKey(key);
        }

        @Override
        public Long get(Bytes key) {
            return delegate.get(key);
        }

        @Override
        public Long remove(Bytes key) {
            return delegate.remove(key);
        }

        @Override
        public boolean isSortedAscending(int[] pkTypes) {
            return delegate.isSortedAscending(pkTypes);
        }

    }

    /**
     * Issue #321: {@link BLinkKeyToPageIndex#snapshotInfo()} must return a
     * per-node layout list (not just summary counters), so the Web UI can
     * render the BLink layout on disk and the loaded blocks.
     */
    @Test
    public void snapshotInfoReportsNodesList() throws Exception {
        // Tiny page size so the tree splits into multiple nodes after a few
        // hundred inserts.
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * 4096L, 4096L);
        try (MemoryDataStorageManager ds = new MemoryDataStorageManager()) {
            BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
            try {
                idx.init();
                idx.start(LogSequenceNumber.START_OF_TIME, true);

                final int rows = 1000;
                for (int i = 0; i < rows; i++) {
                    idx.put(Bytes.from_int(i), (long) i);
                }

                BLinkKeyToPageIndex.PrimaryIndexSnapshot snapshot = idx.snapshotInfo();
                assertNotNull(snapshot);
                assertEquals(rows, snapshot.getEntries());
                assertNotNull("nodes list must not be null", snapshot.getNodes());
                assertFalse("nodes list must not be empty", snapshot.getNodes().isEmpty());
                assertFalse("truncated flag must be false for a small tree",
                        snapshot.isTruncated());

                int loadedFromList = 0;
                long sumLeafKeys = 0;
                for (BLink.NodeInfo info : snapshot.getNodes()) {
                    assertTrue("sizeBytes must be positive, got " + info.getSizeBytes(),
                            info.getSizeBytes() > 0);
                    assertTrue("keys must be non-negative, got " + info.getKeys(),
                            info.getKeys() >= 0);
                    if (info.isLoaded()) {
                        loadedFromList++;
                    }
                    if (info.isLeaf()) {
                        sumLeafKeys += info.getKeys();
                    }
                }
                assertEquals(
                        "loaded count from per-node list must match summary",
                        snapshot.getLoadedNodes(),
                        loadedFromList);
                assertEquals(
                        "totalNodes must match the per-node list size when not truncated",
                        snapshot.getNodes().size(),
                        snapshot.getTotalNodes());
                assertTrue(
                        "totalNodes must be >= loadedNodes",
                        snapshot.getTotalNodes() >= snapshot.getLoadedNodes());
                assertEquals(
                        "sum of leaf keys must equal entries",
                        snapshot.getEntries(),
                        sumLeafKeys);
            } finally {
                idx.close();
            }
        }
    }

    /**
     * Issue #321: when the tree has more nodes than the per-snapshot cap,
     * {@code snapshotInfo()} must return a truncated list and set the
     * {@code truncated} flag.
     */
    @Test
    public void snapshotInfoSetsTruncatedFlagWhenLimitExceeded() throws Exception {
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * 4096L, 4096L);
        try (MemoryDataStorageManager ds = new MemoryDataStorageManager()) {
            BLinkKeyToPageIndex idx = new BLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
            int previousLimit = BLinkKeyToPageIndex.setSnapshotNodeLimit(2);
            try {
                idx.init();
                idx.start(LogSequenceNumber.START_OF_TIME, true);

                for (int i = 0; i < 500; i++) {
                    idx.put(Bytes.from_int(i), (long) i);
                }

                BLinkKeyToPageIndex.PrimaryIndexSnapshot snapshot = idx.snapshotInfo();
                assertNotNull(snapshot);
                assertEquals(2, snapshot.getNodes().size());
                assertTrue("truncated flag must be set when nodes > limit",
                        snapshot.isTruncated());
                assertTrue(
                        "totalNodes must reflect the full tree, not the truncated sample",
                        snapshot.getTotalNodes() > snapshot.getNodes().size());
            } finally {
                BLinkKeyToPageIndex.setSnapshotNodeLimit(previousLimit);
                idx.close();
            }
        }
    }

}
