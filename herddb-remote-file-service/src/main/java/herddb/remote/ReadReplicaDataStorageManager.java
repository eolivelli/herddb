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
import herddb.file.FileRecordSetFactory;
import herddb.index.KeyToPageIndex;
import herddb.index.blink.BLinkKeyToPageIndex;
import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Record;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.storage.DataPageDoesNotExistException;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.FullTableScanConsumer;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.ByteArrayCursor;
import herddb.utils.ByteBufCursor;
import herddb.utils.Bytes;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Read-only DataStorageManager for shared-storage read replicas.
 * <p>
 * Reads both data/index pages AND metadata (TableStatus, table definitions, etc.)
 * from remote storage (S3 via RemoteFileServiceClient). All write operations throw
 * {@link UnsupportedOperationException}.
 * <p>
 * This storage manager has NO local disk dependency for metadata. Pages are read on-demand
 * from S3, and the CachingObjectStorage on the file server provides local caching.
 *
 * @author enrico.olivelli
 */
public class ReadReplicaDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(ReadReplicaDataStorageManager.class.getName());

    private final RemoteFileServiceClient client;
    private final SharedCheckpointMetadataManager metadataManager;
    private final Path tmpDir;
    private final int swapThreshold;

    public ReadReplicaDataStorageManager(
            RemoteFileServiceClient client,
            SharedCheckpointMetadataManager metadataManager,
            Path tmpDir,
            int swapThreshold) {
        this.client = client;
        this.metadataManager = metadataManager;
        this.tmpDir = tmpDir;
        this.swapThreshold = swapThreshold;
    }

    // -------------------------------------------------------------------------
    // Remote page paths (same convention as RemoteFileDataStorageManager)
    // -------------------------------------------------------------------------

    private static String remoteDataPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/data/" + pageId + ".page";
    }

    private static String remoteIndexPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/index/" + pageId + ".page";
    }

    private static String remoteMultipartPath(String tableSpace, String uuid, String fileType) {
        return tableSpace + "/" + uuid + "/multipart/" + fileType;
    }

    // -------------------------------------------------------------------------
    // Multipart index file reads (used by vector indexes on read replicas)
    // -------------------------------------------------------------------------

    @Override
    public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
            String tableSpace, String uuid, String fileType, long fileSize)
            throws DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        int writeBlockSize = client.getBlockSize();
        int bufferSize = RemoteFileDataStorageManager.READ_BUFFER_SIZE;
        LOGGER.log(Level.FINE,
                "multipartIndexReaderSupplier: {0} fileSize={1} writeBlockSize={2} bufferSize={3}",
                new Object[]{logicalPath, fileSize, writeBlockSize, bufferSize});
        return new RemoteRandomAccessReader.Supplier(
                client, logicalPath, fileSize, writeBlockSize, bufferSize, null);
    }

    private static UnsupportedOperationException readOnly(String op) {
        return new UnsupportedOperationException(
                op + " is not supported on ReadReplicaDataStorageManager — read replicas never write.");
    }

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          java.nio.file.Path tempFile,
                                          java.util.function.LongConsumer progress) {
        throw readOnly("writeMultipartIndexFile");
    }

    @Override
    public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType) {
        throw readOnly("deleteMultipartIndexFile");
    }

    // -------------------------------------------------------------------------
    // Page deserialization (matches FileDataStorageManager format)
    // -------------------------------------------------------------------------

    private static List<Record> deserializeDataPage(byte[] data) throws IOException, DataStorageManagerException {
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(data)) {
            long version = dataIn.readVLong();
            long flags = dataIn.readVLong();
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted remote data page");
            }
            int numRecords = dataIn.readInt();
            List<Record> result = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                Bytes key = dataIn.readBytesNoCopy();
                Bytes value = dataIn.readBytesNoCopy();
                result.add(new Record(key, value));
            }
            return result;
        }
    }

    private static <X> X deserializeIndexPage(byte[] data, DataReader<X> reader)
            throws IOException, DataStorageManagerException {
        try (ByteBufCursor dataIn = ByteBufCursor.wrap(data)) {
            long version = dataIn.readVLong();
            long flags = dataIn.readVLong();
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted remote index page");
            }
            return reader.read(dataIn);
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws DataStorageManagerException {
        // nothing to start — no local state
    }

    @Override
    public void close() throws DataStorageManagerException {
        // nothing to close
    }

    // -------------------------------------------------------------------------
    // Read operations — delegated to remote storage
    // -------------------------------------------------------------------------

    @Override
    public List<Record> readPage(String tableSpace, String uuid, Long pageId)
            throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        byte[] data = client.readFile(path);
        if (data == null) {
            throw new DataPageDoesNotExistException(
                    "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        try {
            return deserializeDataPage(data);
        } catch (IOException e) {
            throw new DataStorageManagerException("Error reading remote data page: " + path, e);
        }
    }

    @Override
    public <X> X readIndexPage(String tableSpace, String uuid, Long pageId, DataReader<X> reader)
            throws DataStorageManagerException {
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        byte[] data = client.readFile(path);
        if (data == null) {
            throw new DataStorageManagerException(
                    "No such remote index page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        try {
            return deserializeIndexPage(data, reader);
        } catch (IOException e) {
            throw new DataStorageManagerException("Error reading remote index page: " + path, e);
        }
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, FullTableScanConsumer consumer)
            throws DataStorageManagerException {
        TableStatus status = getLatestTableStatus(tableSpace, uuid);
        doFullTableScan(tableSpace, uuid, status, consumer);
    }

    @Override
    public void fullTableScan(String tableSpace, String uuid, LogSequenceNumber sequenceNumber,
            FullTableScanConsumer consumer) throws DataStorageManagerException {
        TableStatus status = getTableStatus(tableSpace, uuid, sequenceNumber);
        doFullTableScan(tableSpace, uuid, status, consumer);
    }

    private void doFullTableScan(String tableSpace, String uuid, TableStatus status,
            FullTableScanConsumer consumer) throws DataStorageManagerException {
        consumer.acceptTableStatus(status);
        List<Long> activePages = new ArrayList<>(status.activePages.keySet());
        activePages.sort(null);
        for (long pageId : activePages) {
            List<Record> records = readPage(tableSpace, uuid, pageId);
            consumer.acceptPage(pageId, records);
        }
        consumer.endTable();
    }

    // -------------------------------------------------------------------------
    // Metadata — read from shared checkpoint metadata on S3
    // -------------------------------------------------------------------------

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace)
            throws DataStorageManagerException {
        return metadataManager.readCheckpointLsn(tableSpace);
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return metadataManager.readTableDefinitions(tableSpace, sequenceNumber);
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return metadataManager.readIndexDefinitions(tableSpace, sequenceNumber);
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace,
            Consumer<Transaction> consumer) throws DataStorageManagerException {
        metadataManager.readTransactions(tableSpace, sequenceNumber, consumer);
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String uuid)
            throws DataStorageManagerException {
        LogSequenceNumber lsn = getLastcheckpointSequenceNumber(tableSpace);
        return metadataManager.readTableStatus(tableSpace, uuid, lsn);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        return metadataManager.readTableStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String uuid, LogSequenceNumber sequenceNumber)
            throws DataStorageManagerException {
        return metadataManager.readIndexStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public int getActualNumberOfPages(String tableSpace, String uuid)
            throws DataStorageManagerException {
        TableStatus status = getLatestTableStatus(tableSpace, uuid);
        return status.activePages.size();
    }

    // -------------------------------------------------------------------------
    // Init / no-op (replicas don't create structures)
    // -------------------------------------------------------------------------

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        // no-op on read replica
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        // no-op on read replica
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        // no-op on read replica
    }

    @Override
    public void cleanupAfterTableBoot(String tableSpace, String uuid, Set<Long> activePagesAtBoot)
            throws DataStorageManagerException {
        // no-op on read replica — we don't own the pages
    }

    // -------------------------------------------------------------------------
    // Index and record set factory
    // -------------------------------------------------------------------------

    @Override
    public KeyToPageIndex createKeyToPageMap(String tableSpace, String uuid,
            MemoryManager memoryManager) throws DataStorageManagerException {
        return new BLinkKeyToPageIndex(tableSpace, uuid, memoryManager, this);
    }

    @Override
    public void releaseKeyToPageMap(String tableSpace, String uuid, KeyToPageIndex index) {
        if (index != null) {
            index.close();
        }
    }

    @Override
    public RecordSetFactory createRecordSetFactory() {
        return new FileRecordSetFactory(tmpDir, swapThreshold);
    }

    // -------------------------------------------------------------------------
    // Write operations — all rejected on read replicas
    // -------------------------------------------------------------------------

    @Override
    public void writePage(String tableSpace, String uuid, long pageId, Collection<Record> newPage)
            throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support write operations");
    }

    @Override
    public void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer) {
        throw new UnsupportedOperationException("Read replica does not support write operations");
    }

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid,
            TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support checkpoint operations");
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
            IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support checkpoint operations");
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(String tableSpace,
            LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist,
            boolean prepareActions) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support write operations");
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support write operations");
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace,
            LogSequenceNumber sequenceNumber, Collection<Transaction> transactions)
            throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support write operations");
    }

    @Override
    public void dropTable(String tableSpace, String uuid) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support drop operations");
    }

    @Override
    public void dropIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support drop operations");
    }

    @Override
    public void truncateIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support truncate operations");
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        throw new UnsupportedOperationException("Read replica does not support erase operations");
    }
}
