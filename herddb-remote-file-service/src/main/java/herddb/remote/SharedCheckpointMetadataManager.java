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

import herddb.log.LogSequenceNumber;
import herddb.model.Index;
import herddb.model.Table;
import herddb.model.Transaction;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.storage.TableStatus;
import herddb.utils.ExtendedDataInputStream;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.SimpleByteArrayInputStream;
import herddb.utils.VisibleByteArrayOutputStream;
import herddb.utils.XXHash64Utils;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages checkpoint metadata on remote storage (S3 via RemoteFileServiceClient).
 * <p>
 * The leader publishes checkpoint metadata here so that shared-storage read replicas
 * can discover and load consistent checkpoint snapshots without local disk or WAL replay.
 * <p>
 * Metadata is organized under a {@code _metadata/} prefix to avoid collisions with data pages:
 * <ul>
 *   <li>{@code {tableSpace}/_metadata/latest.checkpoint} - latest checkpoint LSN (binary)</li>
 *   <li>{@code {tableSpace}/_metadata/tables.{ledgerId}.{offset}.tablesmetadata} - table definitions</li>
 *   <li>{@code {tableSpace}/_metadata/indexes.{ledgerId}.{offset}.tablesmetadata} - index definitions</li>
 *   <li>{@code {tableSpace}/_metadata/transactions.{ledgerId}.{offset}.tx} - transactions</li>
 *   <li>{@code {tableSpace}/_metadata/{uuid}.{ledgerId}.{offset}.tablestatus} - table status</li>
 *   <li>{@code {tableSpace}/_metadata/{uuid}.{ledgerId}.{offset}.indexstatus} - index status</li>
 * </ul>
 *
 * @author enrico.olivelli
 */
public class SharedCheckpointMetadataManager {

    private static final Logger LOGGER = Logger.getLogger(SharedCheckpointMetadataManager.class.getName());

    private final RemoteFileServiceClient client;

    public SharedCheckpointMetadataManager(RemoteFileServiceClient client) {
        this.client = client;
    }

    // -------------------------------------------------------------------------
    // Path helpers
    // -------------------------------------------------------------------------

    private static String latestCheckpointPath(String tableSpace) {
        return tableSpace + "/_metadata/latest.checkpoint";
    }

    private static String tablesMetadataPath(String tableSpace, LogSequenceNumber lsn) {
        return tableSpace + "/_metadata/tables." + lsn.ledgerId + "." + lsn.offset + ".tablesmetadata";
    }

    private static String indexesMetadataPath(String tableSpace, LogSequenceNumber lsn) {
        return tableSpace + "/_metadata/indexes." + lsn.ledgerId + "." + lsn.offset + ".tablesmetadata";
    }

    private static String transactionsPath(String tableSpace, LogSequenceNumber lsn) {
        return tableSpace + "/_metadata/transactions." + lsn.ledgerId + "." + lsn.offset + ".tx";
    }

    private static String tableStatusPath(String tableSpace, String tableUuid, LogSequenceNumber lsn) {
        return tableSpace + "/_metadata/" + tableUuid + "." + lsn.ledgerId + "." + lsn.offset + ".tablestatus";
    }

    private static String indexStatusPath(String tableSpace, String indexUuid, LogSequenceNumber lsn) {
        return tableSpace + "/_metadata/" + indexUuid + "." + lsn.ledgerId + "." + lsn.offset + ".indexstatus";
    }

    private static String metadataPrefix(String tableSpace) {
        return tableSpace + "/_metadata/";
    }

    // -------------------------------------------------------------------------
    // Write methods (used by leader)
    // -------------------------------------------------------------------------

    /**
     * Publishes table definitions to remote storage.
     */
    public void writeTableDefinitions(String tableSpace, LogSequenceNumber lsn,
                                      List<Table> tables) throws DataStorageManagerException {
        String path = tablesMetadataPath(tableSpace, lsn);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(bos)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                dout.writeUTF(tableSpace);
                dout.writeZLong(lsn.ledgerId);
                dout.writeZLong(lsn.offset);
                dout.writeInt(tables.size());
                for (Table t : tables) {
                    byte[] tableSerialized = t.serialize();
                    dout.writeArray(tableSerialized);
                }
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.FINE, "Published table definitions for {0} at {1}", new Object[]{tableSpace, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write table definitions to " + path, err);
        }
    }

    /**
     * Publishes index definitions to remote storage.
     */
    public void writeIndexDefinitions(String tableSpace, LogSequenceNumber lsn,
                                      List<Index> indexes) throws DataStorageManagerException {
        String path = indexesMetadataPath(tableSpace, lsn);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(bos)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                dout.writeUTF(tableSpace);
                dout.writeZLong(lsn.ledgerId);
                dout.writeZLong(lsn.offset);
                dout.writeInt(indexes != null ? indexes.size() : 0);
                if (indexes != null) {
                    for (Index idx : indexes) {
                        byte[] indexSerialized = idx.serialize();
                        dout.writeArray(indexSerialized);
                    }
                }
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.FINE, "Published index definitions for {0} at {1}", new Object[]{tableSpace, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write index definitions to " + path, err);
        }
    }

    /**
     * Publishes a table's checkpoint status to remote storage.
     */
    public void writeTableStatus(String tableSpace, String tableUuid,
                                 TableStatus tableStatus) throws DataStorageManagerException {
        LogSequenceNumber lsn = tableStatus.sequenceNumber;
        String path = tableStatusPath(tableSpace, tableUuid, lsn);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            XXHash64Utils.HashingOutputStream hashOut = new XXHash64Utils.HashingOutputStream(bos);
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(hashOut)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                tableStatus.serialize(dout);
                dout.writeLong(hashOut.hash());
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.FINE, "Published table status for {0}/{1} at {2}",
                    new Object[]{tableSpace, tableUuid, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write table status to " + path, err);
        }
    }

    /**
     * Publishes an index's checkpoint status to remote storage.
     */
    public void writeIndexStatus(String tableSpace, String indexUuid,
                                 IndexStatus indexStatus) throws DataStorageManagerException {
        LogSequenceNumber lsn = indexStatus.sequenceNumber;
        String path = indexStatusPath(tableSpace, indexUuid, lsn);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            XXHash64Utils.HashingOutputStream hashOut = new XXHash64Utils.HashingOutputStream(bos);
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(hashOut)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                indexStatus.serialize(dout);
                dout.writeLong(hashOut.hash());
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.FINE, "Published index status for {0}/{1} at {2}",
                    new Object[]{tableSpace, indexUuid, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write index status to " + path, err);
        }
    }

    /**
     * Publishes transaction state to remote storage.
     */
    public void writeTransactions(String tableSpace, LogSequenceNumber lsn,
                                  Collection<Transaction> transactions) throws DataStorageManagerException {
        String path = transactionsPath(tableSpace, lsn);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(bos)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                dout.writeUTF(tableSpace);
                dout.writeZLong(lsn.ledgerId);
                dout.writeZLong(lsn.offset);
                dout.writeInt(transactions.size());
                for (Transaction t : transactions) {
                    t.serialize(dout);
                }
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.FINE, "Published transactions for {0} at {1}", new Object[]{tableSpace, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write transactions to " + path, err);
        }
    }

    /**
     * Writes the latest checkpoint LSN marker. This must be called LAST, after all other
     * metadata files for the checkpoint have been written. It acts as an atomic commit
     * marker — replicas only read checkpoints whose latest marker is present.
     */
    public void writeCheckpointLsn(String tableSpace, LogSequenceNumber lsn) throws DataStorageManagerException {
        String path = latestCheckpointPath(tableSpace);
        try {
            VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream dout = new ExtendedDataOutputStream(bos)) {
                dout.writeVLong(1); // version
                dout.writeVLong(0); // flags
                dout.writeUTF(tableSpace);
                dout.writeZLong(lsn.ledgerId);
                dout.writeZLong(lsn.offset);
            }
            client.writeFile(path, bos.toByteArray());
            LOGGER.log(Level.INFO, "Published checkpoint LSN for {0}: {1}", new Object[]{tableSpace, lsn});
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to write checkpoint LSN to " + path, err);
        }
    }

    // -------------------------------------------------------------------------
    // Read methods (used by replicas)
    // -------------------------------------------------------------------------

    /**
     * Reads the latest checkpoint LSN from remote storage.
     *
     * @return the latest LSN, or {@link LogSequenceNumber#START_OF_TIME} if no checkpoint exists yet
     */
    public LogSequenceNumber readCheckpointLsn(String tableSpace) throws DataStorageManagerException {
        String path = latestCheckpointPath(tableSpace);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                return LogSequenceNumber.START_OF_TIME;
            }
            try (InputStream in = new SimpleByteArrayInputStream(data);
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted checkpoint LSN file at " + path);
                }
                String readTableSpace = din.readUTF();
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                return new LogSequenceNumber(ledgerId, offset);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read checkpoint LSN from " + path, err);
        }
    }

    /**
     * Reads table definitions from a specific checkpoint.
     */
    public List<Table> readTableDefinitions(String tableSpace, LogSequenceNumber lsn) throws DataStorageManagerException {
        String path = tablesMetadataPath(tableSpace, lsn);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                if (lsn.isStartOfTime()) {
                    return Collections.emptyList();
                }
                throw new DataStorageManagerException("Table definitions not found at " + path);
            }
            try (InputStream in = new BufferedInputStream(new ByteArrayInputStream(data));
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted table list file at " + path);
                }
                String readName = din.readUTF();
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                int numTables = din.readInt();
                List<Table> res = new ArrayList<>();
                for (int i = 0; i < numTables; i++) {
                    byte[] tableData = din.readArray();
                    Table table = Table.deserialize(tableData);
                    res.add(table);
                }
                return Collections.unmodifiableList(res);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read table definitions from " + path, err);
        }
    }

    /**
     * Reads index definitions from a specific checkpoint.
     */
    public List<Index> readIndexDefinitions(String tableSpace, LogSequenceNumber lsn) throws DataStorageManagerException {
        String path = indexesMetadataPath(tableSpace, lsn);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                if (lsn.isStartOfTime()) {
                    return Collections.emptyList();
                }
                throw new DataStorageManagerException("Index definitions not found at " + path);
            }
            try (InputStream in = new BufferedInputStream(new ByteArrayInputStream(data));
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index list file at " + path);
                }
                String readName = din.readUTF();
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                int numIndexes = din.readInt();
                List<Index> res = new ArrayList<>();
                for (int i = 0; i < numIndexes; i++) {
                    byte[] indexData = din.readArray();
                    Index index = Index.deserialize(indexData);
                    res.add(index);
                }
                return Collections.unmodifiableList(res);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read index definitions from " + path, err);
        }
    }

    /**
     * Reads a table's checkpoint status from remote storage.
     */
    public TableStatus readTableStatus(String tableSpace, String tableUuid,
                                       LogSequenceNumber lsn) throws DataStorageManagerException {
        String path = tableStatusPath(tableSpace, tableUuid, lsn);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                return TableStatus.buildTableStatusForNewCreatedTable(tableUuid);
            }
            XXHash64Utils.verifyBlockWithFooter(data, 0, data.length);
            try (InputStream in = new SimpleByteArrayInputStream(data);
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted table status file at " + path);
                }
                return TableStatus.deserialize(din);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read table status from " + path, err);
        }
    }

    /**
     * Reads an index's checkpoint status from remote storage.
     */
    public IndexStatus readIndexStatus(String tableSpace, String indexUuid,
                                       LogSequenceNumber lsn) throws DataStorageManagerException {
        String path = indexStatusPath(tableSpace, indexUuid, lsn);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                return null;
            }
            XXHash64Utils.verifyBlockWithFooter(data, 0, data.length);
            try (InputStream in = new SimpleByteArrayInputStream(data);
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted index status file at " + path);
                }
                return IndexStatus.deserialize(din);
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read index status from " + path, err);
        }
    }

    /**
     * Reads transactions from a specific checkpoint.
     */
    public void readTransactions(String tableSpace, LogSequenceNumber lsn,
                                 Consumer<Transaction> consumer) throws DataStorageManagerException {
        String path = transactionsPath(tableSpace, lsn);
        try {
            byte[] data = client.readFile(path);
            if (data == null) {
                return; // no transactions file
            }
            try (InputStream in = new BufferedInputStream(new ByteArrayInputStream(data));
                 ExtendedDataInputStream din = new ExtendedDataInputStream(in)) {
                long version = din.readVLong();
                long flags = din.readVLong();
                if (version != 1 || flags != 0) {
                    throw new DataStorageManagerException("corrupted transactions file at " + path);
                }
                String readName = din.readUTF();
                long ledgerId = din.readZLong();
                long offset = din.readZLong();
                int numTransactions = din.readInt();
                for (int i = 0; i < numTransactions; i++) {
                    Transaction tx = Transaction.deserialize(tableSpace, din);
                    consumer.accept(tx);
                }
            }
        } catch (IOException err) {
            throw new DataStorageManagerException("Failed to read transactions from " + path, err);
        }
    }

    // -------------------------------------------------------------------------
    // Hydrate local metadata dir from remote (bootstrap for ephemeral pods)
    // -------------------------------------------------------------------------

    /**
     * Downloads all index-status and table-status files for {@code tableSpace} from
     * remote storage into {@code localMetadataDir}, writing them at the exact
     * on-disk paths that {@link herddb.file.FileDataStorageManager} expects.
     * <p>
     * This is used by the indexing service (and any other {@code RemoteFileDataStorageManager}
     * consumer that runs on an ephemeral local volume) to bootstrap its local metadata
     * cache on restart, so that {@code getIndexStatus}/{@code getLatestIndexStatus} lookups
     * find the checkpoints that were published to S3 before the disk was wiped.
     * <p>
     * The byte-for-byte format of {@code *.indexstatus} and {@code *.tablestatus} objects
     * on S3 is identical to what {@code FileDataStorageManager} writes locally (both
     * emit {@code writeVLong(1) + writeVLong(0) + status.serialize() + XXHash64 footer}),
     * so we can copy the payload verbatim without deserializing.
     * <p>
     * Implementation: downloads to a staging directory first, then atomically renames
     * it over the target path. On failure, the staging dir is removed and the target
     * is left untouched — callers see either the full snapshot or nothing.
     *
     * @return the number of files hydrated (0 if nothing was found on remote)
     */
    public int hydrateLocalMetadataDir(Path localMetadataDir, String tableSpace) throws IOException {
        String prefix = metadataPrefix(tableSpace);
        List<String> remoteFiles;
        try {
            remoteFiles = client.listFiles(prefix);
        } catch (RuntimeException e) {
            throw new IOException("Failed to list remote metadata files under " + prefix, e);
        }
        LOGGER.log(Level.INFO,
                "hydrateLocalMetadataDir: listed {0} file(s) under {1}",
                new Object[]{remoteFiles == null ? 0 : remoteFiles.size(), prefix});
        if (remoteFiles == null || remoteFiles.isEmpty()) {
            LOGGER.log(Level.INFO,
                    "hydrateLocalMetadataDir: no remote metadata found under {0}, skipping",
                    prefix);
            return 0;
        }

        Path stagingDir = localMetadataDir.resolve(".hydrate-staging-" + System.nanoTime());
        Path tableSpaceStaging = stagingDir.resolve(tableSpace + ".tablespace");
        int filesWritten = 0;
        try {
            Files.createDirectories(tableSpaceStaging);
            for (String remotePath : remoteFiles) {
                // Ignore the atomic-commit marker (not a checkpoint artefact needing
                // a corresponding on-disk file) and anything that is not status.
                if (remotePath.equals(latestCheckpointPath(tableSpace))) {
                    continue;
                }
                String fileName = remotePath;
                int slash = fileName.lastIndexOf('/');
                if (slash >= 0) {
                    fileName = fileName.substring(slash + 1);
                }

                // Expected naming: {uuid}.{ledger}.{offset}.{ext}
                //   ext in { indexstatus, tablestatus }
                String[] parts = fileName.split("\\.");
                if (parts.length < 4) {
                    continue;
                }
                String ext = parts[parts.length - 1];
                boolean isIndexStatus = "indexstatus".equals(ext);
                boolean isTableStatus = "tablestatus".equals(ext);
                if (!isIndexStatus && !isTableStatus) {
                    continue;
                }
                // Reconstruct uuid (may contain '.') — it's everything before the last 3 dots.
                StringBuilder uuidSb = new StringBuilder();
                for (int i = 0; i < parts.length - 3; i++) {
                    if (i > 0) {
                        uuidSb.append('.');
                    }
                    uuidSb.append(parts[i]);
                }
                String uuid = uuidSb.toString();
                String ledger = parts[parts.length - 3];
                String offset = parts[parts.length - 2];

                // FileDataStorageManager path layout (see FileDataStorageManager.java):
                //   {root}/{tableSpace}/{uuid}.{index|table}/{ledger}.{offset}.tableorindexcheckpoint
                String subDirName = uuid + (isIndexStatus ? ".index" : ".table");
                Path targetSubDir = tableSpaceStaging.resolve(subDirName);
                Files.createDirectories(targetSubDir);
                Path targetFile = targetSubDir.resolve(
                        ledger + "." + offset + ".checkpoint");

                byte[] data;
                try {
                    data = client.readFile(remotePath);
                } catch (RuntimeException e) {
                    throw new IOException("Failed to read remote metadata file " + remotePath, e);
                }
                if (data == null) {
                    // concurrent deletion — skip silently
                    continue;
                }
                Files.write(targetFile, data);
                filesWritten++;
            }

            if (filesWritten == 0) {
                // Nothing hydrated: drop the empty staging dir.
                deleteRecursively(stagingDir);
                LOGGER.log(Level.INFO,
                        "hydrateLocalMetadataDir: nothing to hydrate for {0}", tableSpace);
                return 0;
            }

            // Atomic rename of the tablespace directory into place. If a previous
            // (possibly partial) dir exists, remove it first — we trust S3 as the
            // source of truth on a fresh boot.
            Path targetTableSpaceDir = localMetadataDir.resolve(tableSpace + ".tablespace");
            if (Files.exists(targetTableSpaceDir)) {
                deleteRecursively(targetTableSpaceDir);
            }
            Files.createDirectories(targetTableSpaceDir.getParent());
            try {
                Files.move(tableSpaceStaging, targetTableSpaceDir,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicErr) {
                // Some filesystems (e.g. tmpfs across mounts) don't support ATOMIC_MOVE
                // on directories — fall back to non-atomic move.
                Files.move(tableSpaceStaging, targetTableSpaceDir);
            }
            // Clean up outer staging wrapper (now empty apart from what was moved).
            deleteRecursively(stagingDir);
            LOGGER.log(Level.INFO,
                    "hydrateLocalMetadataDir: wrote {0} file(s) for tableSpace {1}",
                    new Object[]{filesWritten, tableSpace});
            return filesWritten;
        } catch (IOException err) {
            // Roll back — leave target untouched.
            try {
                deleteRecursively(stagingDir);
            } catch (IOException ignored) {
                // best-effort cleanup
            }
            throw err;
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
    }

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    /**
     * Deletes old checkpoint metadata files, retaining only those at or after
     * the specified LSN.
     */
    public void cleanupOldMetadata(String tableSpace, LogSequenceNumber retainLsn) throws DataStorageManagerException {
        String prefix = metadataPrefix(tableSpace);
        try {
            List<String> files = client.listFiles(prefix);
            String latestPath = latestCheckpointPath(tableSpace);
            for (String file : files) {
                // never delete the latest pointer
                if (file.equals(latestPath)) {
                    continue;
                }
                LogSequenceNumber fileLsn = parseLsnFromMetadataPath(file);
                if (fileLsn != null && retainLsn.after(fileLsn)) {
                    LOGGER.log(Level.FINE, "Cleaning up old metadata file: {0}", file);
                    client.deleteFile(file);
                }
            }
        } catch (Exception err) {
            LOGGER.log(Level.WARNING, "Failed to cleanup old metadata for " + tableSpace, err);
        }
    }

    /**
     * Attempts to parse the LSN ({ledgerId}.{offset}) from a metadata file path.
     * Returns null if the path doesn't match the expected pattern.
     */
    static LogSequenceNumber parseLsnFromMetadataPath(String path) {
        // Expected patterns:
        //   .../{name}.{ledgerId}.{offset}.{extension}
        String fileName = path;
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0) {
            fileName = path.substring(lastSlash + 1);
        }
        // Remove extension
        String[] parts = fileName.split("\\.");
        // We need at least: name, ledgerId, offset, extension = 4 parts
        if (parts.length < 4) {
            return null;
        }
        try {
            long ledgerId = Long.parseLong(parts[parts.length - 3]);
            long offset = Long.parseLong(parts[parts.length - 2]);
            return new LogSequenceNumber(ledgerId, offset);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
