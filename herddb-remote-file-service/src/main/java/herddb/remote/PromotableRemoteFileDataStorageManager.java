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

package herddb.remote;

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
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DataStorageManager that starts in read-only mode (delegating to {@link ReadReplicaDataStorageManager})
 * and can be promoted to writable mode (delegating to {@link RemoteFileDataStorageManager}) when
 * a shared-storage replica becomes leader.
 * <p>
 * On promotion, the current checkpoint metadata from S3 is written to local disk to bootstrap
 * the {@link RemoteFileDataStorageManager}'s local metadata manager.
 *
 * @author enrico.olivelli
 */
public class PromotableRemoteFileDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(PromotableRemoteFileDataStorageManager.class.getName());

    private final ReadReplicaDataStorageManager readOnlyDelegate;
    private final RemoteFileServiceClient client;
    private final SharedCheckpointMetadataManager metadataManager;
    private final Path dataDirectory;
    private final Path tmpDirectory;
    private final int swapThreshold;

    private volatile DataStorageManager activeDelegate;
    private volatile boolean promoted = false;

    public PromotableRemoteFileDataStorageManager(
            ReadReplicaDataStorageManager readOnlyDelegate,
            RemoteFileServiceClient client,
            SharedCheckpointMetadataManager metadataManager,
            Path dataDirectory,
            Path tmpDirectory,
            int swapThreshold) {
        this.readOnlyDelegate = readOnlyDelegate;
        this.client = client;
        this.metadataManager = metadataManager;
        this.dataDirectory = dataDirectory;
        this.tmpDirectory = tmpDirectory;
        this.swapThreshold = swapThreshold;
        this.activeDelegate = readOnlyDelegate;
    }

    /**
     * Promotes this storage manager from read-only to writable mode.
     * Creates a {@link RemoteFileDataStorageManager} backed by the same
     * {@link RemoteFileServiceClient} and enables it for writes.
     */
    public synchronized void promoteToWritable() throws DataStorageManagerException {
        if (promoted) {
            return;
        }
        LOGGER.log(Level.INFO, "Promoting storage manager to writable mode");

        RemoteFileDataStorageManager writableDelegate = new RemoteFileDataStorageManager(
                dataDirectory, tmpDirectory, swapThreshold, client);
        writableDelegate.setSharedCheckpointMetadataManager(metadataManager);
        writableDelegate.start();

        this.activeDelegate = writableDelegate;
        this.promoted = true;
        LOGGER.log(Level.INFO, "Storage manager promoted to writable mode");
    }

    public boolean isPromoted() {
        return promoted;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws DataStorageManagerException {
        activeDelegate.start();
    }

    @Override
    public void close() throws DataStorageManagerException {
        activeDelegate.close();
    }

    // -------------------------------------------------------------------------
    // All operations delegated to activeDelegate
    // -------------------------------------------------------------------------

    @Override
    public List<Record> readPage(String tableSpace, String uuid, Long pageId) throws DataStorageManagerException {
        return activeDelegate.readPage(tableSpace, uuid, pageId);
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String uuid, Long pageId, DataReader<X> reader) throws DataStorageManagerException {
        return activeDelegate.readIndexPage(tableSpace, uuid, pageId, reader);
    }

    @Override
    public void writePage(String tableSpace, String uuid, long pageId, Collection<Record> newPage) throws DataStorageManagerException {
        activeDelegate.writePage(tableSpace, uuid, pageId, newPage);
    }

    @Override
    public void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer) {
        activeDelegate.writeIndexPage(tableSpace, uuid, pageId, writer);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, FullTableScanConsumer consumer) throws DataStorageManagerException {
        activeDelegate.fullTableScan(tableSpace, uuid, consumer);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, LogSequenceNumber sequenceNumber, FullTableScanConsumer consumer) throws DataStorageManagerException {
        activeDelegate.fullTableScan(tableSpace, uuid, sequenceNumber, consumer);
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid, TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
        return activeDelegate.tableCheckpoint(tableSpace, uuid, tableStatus, pin);
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid, IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
        return activeDelegate.indexCheckpoint(tableSpace, uuid, indexStatus, pin);
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String uuid) throws DataStorageManagerException {
        return activeDelegate.getActualNumberOfPages(tableSpace, uuid);
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String uuid) throws DataStorageManagerException {
        return activeDelegate.getLatestTableStatus(tableSpace, uuid);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return activeDelegate.getTableStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return activeDelegate.getIndexStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        activeDelegate.initTablespace(tableSpace);
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        activeDelegate.initTable(tableSpace, uuid);
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        activeDelegate.initIndex(tableSpace, uuid);
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        return activeDelegate.loadTables(sequenceNumber, tableSpace);
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace) throws DataStorageManagerException {
        return activeDelegate.loadIndexes(sequenceNumber, tableSpace);
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace, Consumer<Transaction> consumer) throws DataStorageManagerException {
        activeDelegate.loadTransactions(sequenceNumber, tableSpace, consumer);
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(String tableSpace, LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist, boolean prepareActions) throws DataStorageManagerException {
        return activeDelegate.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace, LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return activeDelegate.writeCheckpointSequenceNumber(tableSpace, sequenceNumber);
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace, LogSequenceNumber sequenceNumber, Collection<Transaction> transactions) throws DataStorageManagerException {
        return activeDelegate.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace) throws DataStorageManagerException {
        return activeDelegate.getLastcheckpointSequenceNumber(tableSpace);
    }

    @Override
    public void dropTable(String tableSpace, String uuid) throws DataStorageManagerException {
        activeDelegate.dropTable(tableSpace, uuid);
    }

    @Override
    public void dropIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        activeDelegate.dropIndex(tableSpace, uuid);
    }

    @Override
    public void truncateIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        activeDelegate.truncateIndex(tableSpace, uuid);
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        activeDelegate.eraseTablespaceData(tableSpace);
    }

    @Override
    public KeyToPageIndex createKeyToPageMap(String tableSpace, String uuid, MemoryManager memoryManager) throws DataStorageManagerException {
        return activeDelegate.createKeyToPageMap(tableSpace, uuid, memoryManager);
    }

    @Override
    public void releaseKeyToPageMap(String tableSpace, String uuid, KeyToPageIndex index) {
        activeDelegate.releaseKeyToPageMap(tableSpace, uuid, index);
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return activeDelegate.createRecordSetFactory();
    }

    @Override
    public void cleanupAfterTableBoot(String tableSpace, String uuid, Set<Long> activePagesAtBoot) throws DataStorageManagerException {
        activeDelegate.cleanupAfterTableBoot(tableSpace, uuid, activePagesAtBoot);
    }

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          java.nio.file.Path tempFile)
            throws java.io.IOException, DataStorageManagerException {
        return activeDelegate.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile);
    }

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          java.nio.file.Path tempFile,
                                          java.util.function.LongConsumer progress)
            throws java.io.IOException, DataStorageManagerException {
        return activeDelegate.writeMultipartIndexFile(tableSpace, uuid, fileType, tempFile, progress);
    }

    @Override
    public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
            String tableSpace, String uuid, String fileType, long fileSize)
            throws DataStorageManagerException {
        return activeDelegate.multipartIndexReaderSupplier(tableSpace, uuid, fileType, fileSize);
    }

    @Override
    public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
            throws DataStorageManagerException {
        activeDelegate.deleteMultipartIndexFile(tableSpace, uuid, fileType);
    }
}
