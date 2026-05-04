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
package herddb.indexing;

import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.core.RecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Forwarding wrapper around a {@link DataStorageManager}. Test helpers extend
 * this to override only the methods they need to spy on or stub.
 *
 * <p>This is a test-only utility; production code never instantiates it.
 */
abstract class ForwardingDataStorageManager extends DataStorageManager {

    private final DataStorageManager delegate;

    ForwardingDataStorageManager(DataStorageManager delegate) {
        this.delegate = delegate;
    }

    DataStorageManager delegate() {
        return delegate;
    }

    @Override
    public List<Record> readPage(String tableSpace, String uuid, Long pageId)
            throws DataStorageManagerException {
        return delegate.readPage(tableSpace, uuid, pageId);
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        delegate.initIndex(tableSpace, uuid);
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        delegate.initTable(tableSpace, uuid);
    }

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        delegate.initTablespace(tableSpace);
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String uuid, Long pageId, DataReader<X> reader)
            throws DataStorageManagerException {
        return delegate.readIndexPage(tableSpace, uuid, pageId, reader);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, FullTableScanConsumer consumer)
            throws DataStorageManagerException {
        delegate.fullTableScan(tableSpace, uuid, consumer);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, LogSequenceNumber sequenceNumber,
                              FullTableScanConsumer consumer)
            throws DataStorageManagerException {
        delegate.fullTableScan(tableSpace, uuid, sequenceNumber, consumer);
    }

    @Override
    public void writePage(String tableSpace, String uuid, long pageId, Collection<Record> newPage)
            throws DataStorageManagerException {
        delegate.writePage(tableSpace, uuid, pageId, newPage);
    }

    @Override
    public boolean supportsLazyPageLoad() {
        return delegate.supportsLazyPageLoad();
    }

    @Override
    public void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer) {
        delegate.writeIndexPage(tableSpace, uuid, pageId, writer);
    }

    @Override
    public CompletableFuture<Void> writeIndexPageAsync(String tableSpace, String uuid, long pageId,
                                                       DataWriter writer) {
        return delegate.writeIndexPageAsync(tableSpace, uuid, pageId, writer);
    }

    @Override
    public void deleteIndexPage(String tableSpace, String uuid, long pageId)
            throws DataStorageManagerException {
        delegate.deleteIndexPage(tableSpace, uuid, pageId);
    }

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          Path tempFile, LongConsumer progress)
            throws IOException, DataStorageManagerException {
        return delegate.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
    }

    @Override
    public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
            String tableSpace, String uuid, String fileType, long fileSize)
            throws DataStorageManagerException {
        return delegate.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
    }

    @Override
    public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
            throws DataStorageManagerException {
        delegate.deleteMultipartIndexFile(tableSpace, uuid, fileType);
    }

    @Override
    public boolean supportsVectorIndexes() {
        return delegate.supportsVectorIndexes();
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid,
                                                      TableStatus tableStatus, boolean pin)
            throws DataStorageManagerException {
        return delegate.tableCheckpoint(tableSpace, uuid, tableStatus, pin);
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
                                                      IndexStatus indexStatus, boolean pin)
            throws DataStorageManagerException {
        return delegate.indexCheckpoint(tableSpace, uuid, indexStatus, pin);
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return delegate.getActualNumberOfPages(tableSpace, uuid);
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return delegate.getLatestTableStatus(tableSpace, uuid);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        return delegate.getTableStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        return delegate.getIndexStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public void start() throws DataStorageManagerException {
        delegate.start();
    }

    @Override
    public void close() throws DataStorageManagerException {
        delegate.close();
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        delegate.eraseTablespaceData(tableSpace);
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return delegate.loadTables(sequenceNumber, tableSpace);
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return delegate.loadIndexes(sequenceNumber, tableSpace);
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace,
                                 Consumer<Transaction> consumer)
            throws DataStorageManagerException {
        delegate.loadTransactions(sequenceNumber, tableSpace, consumer);
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(String tableSpace, LogSequenceNumber sequenceNumber,
                                                        List<Table> tables, List<Index> indexlist,
                                                        boolean prepareActions)
            throws DataStorageManagerException {
        return delegate.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace,
                                                                          LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        return delegate.writeCheckpointSequenceNumber(tableSpace, sequenceNumber);
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace,
                                                                          LogSequenceNumber sequenceNumber,
                                                                          Collection<Transaction> transactions)
            throws DataStorageManagerException {
        return delegate.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace)
            throws DataStorageManagerException {
        return delegate.getLastcheckpointSequenceNumber(tableSpace);
    }

    @Override
    public void dropTable(String tablespace, String name) throws DataStorageManagerException {
        delegate.dropTable(tablespace, name);
    }

    @Override
    public KeyToPageIndex createKeyToPageMap(String tablespace, String name, MemoryManager memoryManager)
            throws DataStorageManagerException {
        return delegate.createKeyToPageMap(tablespace, name, memoryManager);
    }

    @Override
    public void releaseKeyToPageMap(String tablespace, String name, KeyToPageIndex index) {
        delegate.releaseKeyToPageMap(tablespace, name, index);
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return delegate.createRecordSetFactory();
    }

    @Override
    public void cleanupAfterTableBoot(String tablespace, String name, Set<Long> activePagesAtBoot)
            throws DataStorageManagerException {
        delegate.cleanupAfterTableBoot(tablespace, name, activePagesAtBoot);
    }

    @Override
    public void truncateIndex(String tableSpaceUUID, String name) throws DataStorageManagerException {
        delegate.truncateIndex(tableSpaceUUID, name);
    }

    @Override
    public void dropIndex(String tableSpaceUUID, String name) throws DataStorageManagerException {
        delegate.dropIndex(tableSpaceUUID, name);
    }
}
