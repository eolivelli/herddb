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

import herddb.core.MemoryManager;
import herddb.index.blink.IncrementalBLinkKeyToPageIndex;
import herddb.mem.MemoryDataStorageManager;

/**
 * Runs the shared {@link KeyToPageIndexTest} contract against the
 * {@link IncrementalBLinkKeyToPageIndex} — the new default PK index
 * implementation.
 */
public class IncrementalBLinkKeyToPageIndexTest extends KeyToPageIndexTest {

    @Override
    KeyToPageIndex createIndex() {
        MemoryManager mem = new MemoryManager(5 * (1L << 20), 0, 10 * (128L << 10), (128L << 10));
        MemoryDataStorageManager ds = new MemoryDataStorageManager();
        IncrementalBLinkKeyToPageIndex idx =
                new IncrementalBLinkKeyToPageIndex("tblspc", "tbl", mem, ds);
        return new ResourceCloseKeyToPageIndex(idx, ds);
    }

    private final class ResourceCloseKeyToPageIndex implements KeyToPageIndex {

        private final KeyToPageIndex delegate;
        private final AutoCloseable[] closeables;

        ResourceCloseKeyToPageIndex(KeyToPageIndex index, AutoCloseable... closeables) {
            this.delegate = index;
            this.closeables = closeables;
        }

        @Override
        public void close() {
            delegate.close();
            IllegalStateException exception = null;
            for (AutoCloseable c : closeables) {
                try {
                    c.close();
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
        public void init() throws herddb.storage.DataStorageManagerException {
            delegate.init();
        }

        @Override
        public void start(herddb.log.LogSequenceNumber sequenceNumber, boolean created)
                throws herddb.storage.DataStorageManagerException {
            delegate.start(sequenceNumber, created);
        }

        @Override
        public java.util.List<herddb.core.PostCheckpointAction> checkpoint(
                herddb.log.LogSequenceNumber sequenceNumber, boolean pin)
                throws herddb.storage.DataStorageManagerException {
            return delegate.checkpoint(sequenceNumber, pin);
        }

        @Override
        public void unpinCheckpoint(herddb.log.LogSequenceNumber sequenceNumber)
                throws herddb.storage.DataStorageManagerException {
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
        public java.util.stream.Stream<java.util.Map.Entry<herddb.utils.Bytes, Long>> scanner(
                IndexOperation operation, herddb.model.StatementEvaluationContext context,
                herddb.model.TableContext tableContext, herddb.core.AbstractIndexManager index)
                throws herddb.storage.DataStorageManagerException,
                       herddb.model.StatementExecutionException {
            return delegate.scanner(operation, context, tableContext, index);
        }

        @Override
        public void put(herddb.utils.Bytes key, Long currentPage) {
            delegate.put(key, currentPage);
        }

        @Override
        public boolean put(herddb.utils.Bytes key, Long newPage, Long expectedPage) {
            return delegate.put(key, newPage, expectedPage);
        }

        @Override
        public boolean containsKey(herddb.utils.Bytes key) {
            return delegate.containsKey(key);
        }

        @Override
        public Long get(herddb.utils.Bytes key) {
            return delegate.get(key);
        }

        @Override
        public Long remove(herddb.utils.Bytes key) {
            return delegate.remove(key);
        }

        @Override
        public boolean isSortedAscending(int[] pkTypes) {
            return delegate.isSortedAscending(pkTypes);
        }
    }
}
