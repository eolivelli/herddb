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
import herddb.file.FileDataStorageManager;
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
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * DataStorageManager that stores page data on remote RemoteFileService instances
 * and keeps metadata locally (checkpoint files, table/index metadata, transactions).
 *
 * @author enrico.olivelli
 */
public class RemoteFileDataStorageManager extends DataStorageManager {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileDataStorageManager.class.getName());

    private final FileDataStorageManager localMetadataManager;
    private final RemoteFileServiceClient client;
    private final Path tmpDir;
    private final int swapThreshold;

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client) {
        this.tmpDir = tmpDir;
        this.swapThreshold = swapThreshold;
        this.client = client;
        this.localMetadataManager = new FileDataStorageManager(
                localMetadataDir, tmpDir, swapThreshold,
                false, false, false, false, false,
                new NullStatsLogger());
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() throws DataStorageManagerException {
        localMetadataManager.start();
    }

    @Override
    public void close() throws DataStorageManagerException {
        localMetadataManager.close();
    }

    // -------------------------------------------------------------------------
    // Remote page paths
    // -------------------------------------------------------------------------

    private static String remoteDataPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/data/" + pageId + ".page";
    }

    private static String remoteIndexPagePath(String tableSpace, String uuid, long pageId) {
        return tableSpace + "/" + uuid + "/index/" + pageId + ".page";
    }

    private static String remoteDataPrefix(String tableSpace, String uuid) {
        return tableSpace + "/" + uuid + "/data/";
    }

    private static String remoteIndexPrefix(String tableSpace, String uuid) {
        return tableSpace + "/" + uuid + "/index/";
    }

    private static String remoteTablespacePrefix(String tableSpace) {
        return tableSpace + "/";
    }

    private static long pageIdFromRemotePath(String path) {
        int slash = path.lastIndexOf('/');
        String filename = path.substring(slash + 1);
        if (filename.endsWith(".page")) {
            try {
                return Long.parseLong(filename.substring(0, filename.length() - ".page".length()));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    // -------------------------------------------------------------------------
    // Page serialization (matches FileDataStorageManager format)
    // -------------------------------------------------------------------------

    private static VisibleByteArrayOutputStream serializeDataPage(Collection<Record> records) throws IOException {
        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(4096);
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(oo)) {
            out.writeVLong(1); // version
            out.writeVLong(0); // flags
            out.writeInt(records.size());
            for (Record record : records) {
                out.writeArray(record.key);
                out.writeArray(record.value);
            }
            out.flush();
            long hash = XXHash64Utils.hash(oo.getBuffer(), 0, oo.size());
            out.writeLong(hash);
            out.flush();
        }
        return oo;
    }

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

    private static VisibleByteArrayOutputStream serializeIndexPage(DataWriter writer) throws IOException {
        VisibleByteArrayOutputStream oo = new VisibleByteArrayOutputStream(4096);
        try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(oo)) {
            out.writeVLong(1); // version
            out.writeVLong(0); // flags
            writer.write(out);
            out.flush();
            long hash = XXHash64Utils.hash(oo.getBuffer(), 0, oo.size());
            out.writeLong(hash);
            out.flush();
        }
        return oo;
    }

    private static <X> X deserializeIndexPage(byte[] data, DataReader<X> reader)
            throws IOException, DataStorageManagerException {
        try (ByteArrayCursor dataIn = ByteArrayCursor.wrap(data)) {
            long version = dataIn.readVLong();
            long flags = dataIn.readVLong();
            if (version != 1 || flags != 0) {
                throw new DataStorageManagerException("corrupted remote index page");
            }
            return reader.read(dataIn);
        }
    }

    // -------------------------------------------------------------------------
    // Remote page operations
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
    public void writePage(String tableSpace, String uuid, long pageId,
            Collection<Record> newPage) throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        try {
            VisibleByteArrayOutputStream oo = serializeDataPage(newPage);
            client.writeFile(path, oo.getBuffer(), 0, oo.size());
        } catch (IOException e) {
            throw new DataStorageManagerException("Error writing remote data page: " + path, e);
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
    public void writeIndexPage(String tableSpace, String uuid, long pageId, DataWriter writer) {
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        try {
            VisibleByteArrayOutputStream oo = serializeIndexPage(writer);
            client.writeFile(path, oo.getBuffer(), 0, oo.size());
        } catch (IOException e) {
            throw new RuntimeException("Error writing remote index page: " + path, e);
        }
    }

    // -------------------------------------------------------------------------
    // Full table scan
    // -------------------------------------------------------------------------

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
    // Checkpoint — local metadata + remote page cleanup PostCheckpointActions
    // -------------------------------------------------------------------------

    @Override
    public List<PostCheckpointAction> tableCheckpoint(String tableSpace, String uuid,
            TableStatus tableStatus, boolean pin) throws DataStorageManagerException {
        // Delegate local metadata file writing + old metadata file cleanup to localMetadataManager
        List<PostCheckpointAction> result = new ArrayList<>(
                localMetadataManager.tableCheckpoint(tableSpace, uuid, tableStatus, pin));

        // Add remote page deletion actions for stale pages
        final Map<Long, Integer> pins = pinTableAndGetPages(tableSpace, uuid, tableStatus, pin);
        long maxPageId = tableStatus.activePages.keySet().stream()
                .max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);

        List<String> remotePages = client.listFiles(remoteDataPrefix(tableSpace, uuid));
        for (String remotePath : remotePages) {
            long pageId = pageIdFromRemotePath(remotePath);
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !tableStatus.activePages.containsKey(pageId)
                    && pageId < maxPageId) {
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote page " + pageId, remotePath, client));
            }
        }
        return result;
    }

    @Override
    public List<PostCheckpointAction> indexCheckpoint(String tableSpace, String uuid,
            IndexStatus indexStatus, boolean pin) throws DataStorageManagerException {
        List<PostCheckpointAction> result = new ArrayList<>(
                localMetadataManager.indexCheckpoint(tableSpace, uuid, indexStatus, pin));

        final Map<Long, Integer> pins = pinIndexAndGetPages(tableSpace, uuid, indexStatus, pin);
        long maxPageId = indexStatus.activePages.stream()
                .max(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);

        List<String> remotePages = client.listFiles(remoteIndexPrefix(tableSpace, uuid));
        for (String remotePath : remotePages) {
            long pageId = pageIdFromRemotePath(remotePath);
            if (pageId > 0
                    && !pins.containsKey(pageId)
                    && !indexStatus.activePages.contains(pageId)
                    && pageId < maxPageId) {
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote index page " + pageId, remotePath, client));
            }
        }
        return result;
    }

    private static class RemoteDeletePageAction extends PostCheckpointAction {
        private final String remotePath;
        private final RemoteFileServiceClient client;

        RemoteDeletePageAction(String tableSpace, String tableName, String description,
                String remotePath, RemoteFileServiceClient client) {
            super(tableSpace, tableName, description);
            this.remotePath = remotePath;
            this.client = client;
        }

        @Override
        public void run() {
            LOGGER.log(Level.FINE, description);
            client.deleteFile(remotePath);
        }
    }

    // -------------------------------------------------------------------------
    // Table/index structure operations
    // -------------------------------------------------------------------------

    @Override
    public void initTablespace(String tableSpace) throws DataStorageManagerException {
        localMetadataManager.initTablespace(tableSpace);
    }

    @Override
    public void initTable(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.initTable(tableSpace, uuid);
    }

    @Override
    public void initIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.initIndex(tableSpace, uuid);
    }

    @Override
    public void dropTable(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.dropTable(tableSpace, uuid);
        client.deleteByPrefix(remoteDataPrefix(tableSpace, uuid));
    }

    @Override
    public void dropIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.dropIndex(tableSpace, uuid);
        client.deleteByPrefix(remoteIndexPrefix(tableSpace, uuid));
    }

    @Override
    public void truncateIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.truncateIndex(tableSpace, uuid);
        client.deleteByPrefix(remoteIndexPrefix(tableSpace, uuid));
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        localMetadataManager.eraseTablespaceData(tableSpace);
        client.deleteByPrefix(remoteTablespacePrefix(tableSpace));
    }

    @Override
    public void cleanupAfterTableBoot(String tableSpace, String uuid, Set<Long> activePagesAtBoot)
            throws DataStorageManagerException {
        // Delete stale remote pages not in the active set
        List<String> remotePages = client.listFiles(remoteDataPrefix(tableSpace, uuid));
        for (String remotePath : remotePages) {
            long pageId = pageIdFromRemotePath(remotePath);
            if (pageId > 0 && !activePagesAtBoot.contains(pageId)) {
                LOGGER.log(Level.FINE, "cleanupAfterTableBoot: deleting stale remote page {0}", remotePath);
                client.deleteFile(remotePath);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Metadata delegation to localMetadataManager
    // -------------------------------------------------------------------------

    @Override
    public int getActualNumberOfPages(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return localMetadataManager.getActualNumberOfPages(tableSpace, uuid);
    }

    @Override
    public TableStatus getLatestTableStatus(String tableSpace, String uuid)
            throws DataStorageManagerException {
        return localMetadataManager.getLatestTableStatus(tableSpace, uuid);
    }

    @Override
    public TableStatus getTableStatus(String tableSpace, String uuid,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return localMetadataManager.getTableStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public IndexStatus getIndexStatus(String tableSpace, String uuid,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return localMetadataManager.getIndexStatus(tableSpace, uuid, sequenceNumber);
    }

    @Override
    public List<Table> loadTables(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.loadTables(sequenceNumber, tableSpace);
    }

    @Override
    public List<Index> loadIndexes(LogSequenceNumber sequenceNumber, String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.loadIndexes(sequenceNumber, tableSpace);
    }

    @Override
    public void loadTransactions(LogSequenceNumber sequenceNumber, String tableSpace,
            Consumer<Transaction> consumer) throws DataStorageManagerException {
        localMetadataManager.loadTransactions(sequenceNumber, tableSpace, consumer);
    }

    @Override
    public Collection<PostCheckpointAction> writeTables(String tableSpace,
            LogSequenceNumber sequenceNumber, List<Table> tables, List<Index> indexlist,
            boolean prepareActions) throws DataStorageManagerException {
        return localMetadataManager.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        return localMetadataManager.writeCheckpointSequenceNumber(tableSpace, sequenceNumber);
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace,
            LogSequenceNumber sequenceNumber, Collection<Transaction> transactions)
            throws DataStorageManagerException {
        return localMetadataManager.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);
    }

    @Override
    public LogSequenceNumber getLastcheckpointSequenceNumber(String tableSpace)
            throws DataStorageManagerException {
        return localMetadataManager.getLastcheckpointSequenceNumber(tableSpace);
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
}
