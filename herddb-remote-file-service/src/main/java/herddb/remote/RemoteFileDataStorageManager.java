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
import herddb.index.KeyToPageIndexFactory;
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
import herddb.utils.ByteBufCursor;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.XXHash64Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.stats.NullStatsLogger;

/**
 * DataStorageManager that stores page data on remote RemoteFileService instances
 * and keeps metadata locally (checkpoint files, table/index metadata, transactions).
 *
 * @author enrico.olivelli
 */
public class RemoteFileDataStorageManager extends DataStorageManager
        implements herddb.server.RemoteFileStorageManager {

    private static final Logger LOGGER = Logger.getLogger(RemoteFileDataStorageManager.class.getName());

    private final FileDataStorageManager localMetadataManager;
    private final RemoteFileServiceClient client;
    private final Path tmpDir;
    private final int swapThreshold;

    /**
     * When set, checkpoint metadata (TableStatus, IndexStatus, table/index definitions,
     * checkpoint LSN) is also published to remote storage so that shared-storage read
     * replicas can consume it.
     */
    private volatile SharedCheckpointMetadataManager sharedCheckpointMetadataManager;

    /**
     * Tracks the set of active data page IDs as of the last successful tableCheckpoint per
     * "{tableSpace}/{uuid}" key. Used to compute the stale-page diff without a full remote listFiles.
     * Populated on the first checkpoint after boot (via listFiles fallback); subsequent checkpoints
     * use the diff path.  Entries are evicted when a table or tablespace is dropped.
     */
    private final ConcurrentHashMap<String, Set<Long>> lastCheckpointedDataPages = new ConcurrentHashMap<>();

    /**
     * Same as {@link #lastCheckpointedDataPages} but for index pages.
     */
    private final ConcurrentHashMap<String, Set<Long>> lastCheckpointedIndexPages = new ConcurrentHashMap<>();

    /**
     * Deferred page deletions keyed by "{tableSpace}/{uuid}". Each entry records pages
     * that became stale at a specific checkpoint LSN and are waiting until it is safe to
     * actually delete them from remote storage. Populated only when retention is enabled.
     */
    private final ConcurrentHashMap<String, List<PendingDeletion>> pendingDataDeletions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<PendingDeletion>> pendingIndexDeletions = new ConcurrentHashMap<>();

    /**
     * When retention is enabled, page deletions are deferred according to the following rules:
     * <ul>
     *   <li>wait at least {@link #minRetentionMillis} after a page became stale (safety grace)</li>
     *   <li>delete when the min replica LSN (from {@link #minReplicaLsnSupplier}) has advanced
     *       past the page's stale-LSN</li>
     *   <li>force-delete after {@link #maxRetentionMillis} even if replicas are behind (safety cap)</li>
     * </ul>
     */
    private volatile boolean retentionEnabled = false;
    private volatile Function<String, LogSequenceNumber> minReplicaLsnSupplier = ts -> null;
    private volatile long minRetentionMillis = 0L;
    private volatile long maxRetentionMillis = Long.MAX_VALUE;

    private final LazyValueCache lazyValueCache;

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client) {
        this(localMetadataDir, tmpDir, swapThreshold, client, new LazyValueCache(0L));
    }

    public RemoteFileDataStorageManager(
            Path localMetadataDir, Path tmpDir, int swapThreshold,
            RemoteFileServiceClient client, LazyValueCache lazyValueCache) {
        this.tmpDir = tmpDir;
        this.swapThreshold = swapThreshold;
        this.client = client;
        this.lazyValueCache = lazyValueCache == null ? new LazyValueCache(0L) : lazyValueCache;
        this.localMetadataManager = new FileDataStorageManager(
                localMetadataDir, tmpDir, swapThreshold,
                false, false, false, false, false,
                new NullStatsLogger());
    }

    /** Value cache used by lazy page loads. */
    LazyValueCache getLazyValueCache() {
        return lazyValueCache;
    }

    /** Client used for all remote I/O. Visible for lazy-page loading. */
    RemoteFileServiceClient getClient() {
        return client;
    }

    /**
     * Enables publication of checkpoint metadata to remote storage for shared-storage read replicas.
     */
    @Override
    public void setSharedCheckpointMetadataManager(herddb.server.SharedCheckpointMetadata manager) {
        this.sharedCheckpointMetadataManager = (SharedCheckpointMetadataManager) manager;
    }

    /**
     * Enables deferred page deletion so that shared-storage read replicas can safely consume
     * pages from old checkpoints. See the class-level documentation for the retention model.
     *
     * @param minReplicaLsnSupplier given a tableSpace UUID, returns the minimum checkpoint LSN
     *        across all currently-registered replicas, or {@code null} if no replicas are tracked
     * @param minRetentionMillis minimum grace period before a page can be deleted, even if all
     *        replicas have advanced past its stale-LSN
     * @param maxRetentionMillis maximum time a page can be retained; after this, it is force-deleted
     *        even if some replicas are still behind (they will need to re-bootstrap)
     */
    @Override
    public void setRetentionPolicy(
            Function<String, LogSequenceNumber> minReplicaLsnSupplier,
            long minRetentionMillis,
            long maxRetentionMillis) {
        this.minReplicaLsnSupplier = minReplicaLsnSupplier != null ? minReplicaLsnSupplier : ts -> null;
        this.minRetentionMillis = Math.max(0, minRetentionMillis);
        this.maxRetentionMillis = maxRetentionMillis <= 0 ? Long.MAX_VALUE : maxRetentionMillis;
        this.retentionEnabled = true;
        LOGGER.log(Level.INFO,
                "Deferred page deletion enabled: minRetention={0}ms, maxRetention={1}ms",
                new Object[]{this.minRetentionMillis, this.maxRetentionMillis});
    }

    // visible for testing
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // visible for testing
    int pendingDataDeletionCount(String tableSpace, String uuid) {
        List<PendingDeletion> pending = pendingDataDeletions.get(tableSpace + "/" + uuid);
        return pending == null ? 0 : pending.size();
    }

    // visible for testing
    int pendingIndexDeletionCount(String tableSpace, String uuid) {
        List<PendingDeletion> pending = pendingIndexDeletions.get(tableSpace + "/" + uuid);
        return pending == null ? 0 : pending.size();
    }

    private static final class PendingDeletion {
        final LogSequenceNumber staleAt;
        final long scheduledAtMillis;
        final String remotePath;
        final String description;
        final String tableSpace;
        final String uuid;

        PendingDeletion(LogSequenceNumber staleAt, long scheduledAtMillis, String remotePath,
                        String description, String tableSpace, String uuid) {
            this.staleAt = staleAt;
            this.scheduledAtMillis = scheduledAtMillis;
            this.remotePath = remotePath;
            this.description = description;
            this.tableSpace = tableSpace;
            this.uuid = uuid;
        }
    }

    /**
     * Evaluates pending deletions for a table/index and returns the subset that is safe to
     * execute now. Safe deletions are removed from the pending list.
     */
    private List<PostCheckpointAction> promotePendingDeletions(
            ConcurrentHashMap<String, List<PendingDeletion>> store, String key, String tableSpace) {
        List<PendingDeletion> pending = store.get(key);
        if (pending == null || pending.isEmpty()) {
            return Collections.emptyList();
        }
        LogSequenceNumber minReplicaLsn = null;
        try {
            minReplicaLsn = minReplicaLsnSupplier.apply(tableSpace);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to query min replica LSN for " + tableSpace
                    + "; retaining pages conservatively", e);
        }
        long now = currentTimeMillis();
        List<PostCheckpointAction> toRun = new ArrayList<>();
        synchronized (pending) {
            Iterator<PendingDeletion> it = pending.iterator();
            while (it.hasNext()) {
                PendingDeletion pd = it.next();
                long ageMs = now - pd.scheduledAtMillis;
                boolean minGracePassed = ageMs >= minRetentionMillis;
                // A replica at LSN L does not reference pages that became stale at LSN pd.staleAt
                // when L >= pd.staleAt. after() is strictly-after, so we check !pd.staleAt.after(L).
                boolean replicasAdvanced = minReplicaLsn != null
                        && !pd.staleAt.after(minReplicaLsn);
                boolean forceByMaxAge = ageMs >= maxRetentionMillis;
                boolean safe = (minGracePassed && replicasAdvanced) || forceByMaxAge;
                if (safe) {
                    toRun.add(new RemoteDeletePageAction(pd.tableSpace, pd.uuid, pd.description,
                            pd.remotePath, client));
                    it.remove();
                    if (forceByMaxAge && !replicasAdvanced) {
                        LOGGER.log(Level.WARNING,
                                "Force-deleting page {0} after {1}ms (min replica LSN: {2}, stale at: {3})",
                                new Object[]{pd.remotePath, ageMs, minReplicaLsn, pd.staleAt});
                    }
                }
            }
        }
        return toRun;
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

    /** System property to override the multipart block size (bytes). Default: 4 MB. */
    public static final String MULTIPART_BLOCK_SIZE_PROPERTY = "herddb.remote.multipart.blockSize";
    private static final int MULTIPART_BLOCK_SIZE =
            Integer.getInteger(MULTIPART_BLOCK_SIZE_PROPERTY, 4 * 1024 * 1024);

    /**
     * System property to override the read-buffer size used by
     * {@link RemoteRandomAccessReader} when serving vector-index searches over
     * remote multipart graph files. Default: 16384 bytes (16 KiB). See
     * issue #104 — this buffer is intentionally decoupled from
     * {@link #MULTIPART_BLOCK_SIZE} so that HNSW graph traversals do not fetch
     * multi-MiB windows per miss.
     *
     * <p>The 16 KiB default is sized to absorb a single jvector logical read in
     * one gRPC call. The dominant per-node read during search is the full-
     * resolution vector fetched by {@code OnDiskGraphIndex.getVectorInto}
     * for re-ranking, which reads {@code dimension * 4} bytes in a single
     * {@code readFloatVector} call — 3840 bytes for GIST1M (dim=960), 6144 bytes
     * for 1536-dim embeddings. A 4 KiB buffer would split those reads across
     * two gRPC round-trips whenever the position is unaligned; 16 KiB keeps a
     * raw vector up to ~4096 dimensions in a single fetch while still being
     * 256× smaller than the 4 MiB write block and an exact divisor of it.
     */
    public static final String READ_BUFFER_SIZE_PROPERTY = "herddb.vector.remote.read.bufferSize";
    static final int READ_BUFFER_SIZE =
            Integer.getInteger(READ_BUFFER_SIZE_PROPERTY, 16 * 1024);

    private static String remoteMultipartPath(String tableSpace, String uuid, String fileType) {
        return tableSpace + "/" + uuid + "/multipart/" + fileType;
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

    private static ByteBuf serializeIndexPage(DataWriter writer) throws IOException {
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(4096);
        try {
            ByteBufOutputStream bufOut = new ByteBufOutputStream(buf);
            XXHash64Utils.HashingOutputStream hashOut = new XXHash64Utils.HashingOutputStream(bufOut);
            try (ExtendedDataOutputStream out = new ExtendedDataOutputStream(hashOut)) {
                out.writeVLong(1); // version
                out.writeVLong(0); // flags
                writer.write(out);
                out.flush();
                long hash = hashOut.hash(); // hash of data bytes only, before footer
                out.writeLong(hash); // footer
                out.flush();
            }
        } catch (IOException e) {
            buf.release();
            throw e;
        }
        return buf;
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
    // Remote page operations
    // -------------------------------------------------------------------------

    @Override
    public List<Record> readPage(String tableSpace, String uuid, Long pageId)
            throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        int blockSize = client.getBlockSize();
        byte[] headerBytes = client.readFileRange(path, 0L,
                LazyDataPageFormat.FIXED_HEADER_SIZE, blockSize);
        if (headerBytes == null || headerBytes.length < LazyDataPageFormat.FIXED_HEADER_SIZE) {
            throw new DataPageDoesNotExistException(
                    "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        LazyDataPageFormat.FixedHeader h;
        ByteBuf headerBuf = io.netty.buffer.Unpooled.wrappedBuffer(headerBytes);
        try {
            h = LazyDataPageFormat.readHeader(headerBuf);
        } finally {
            headerBuf.release();
        }
        long totalSize = h.totalSize();
        if (totalSize > (long) Integer.MAX_VALUE) {
            throw new DataStorageManagerException(
                    "remote page too big to read eagerly: " + path + " totalSize=" + totalSize);
        }
        byte[] full = client.readFileRange(path, 0L, (int) totalSize, blockSize);
        if (full == null || full.length < totalSize) {
            throw new DataStorageManagerException(
                    "short read for remote page " + path + ": expected " + totalSize
                            + " got " + (full == null ? 0 : full.length));
        }
        ByteBuf buf = io.netty.buffer.Unpooled.wrappedBuffer(full);
        try {
            return LazyDataPageFormat.readAllRecords(buf);
        } finally {
            buf.release();
        }
    }

    @Override
    public void writePage(String tableSpace, String uuid, long pageId,
            Collection<Record> newPage) throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        ByteBuf buf = LazyDataPageFormat.write(newPage);
        try {
            writeAsMultipart(path, buf);
        } finally {
            buf.release();
        }
        // Any cached values under this (tableSpace/uuid/pageId) now refer to
        // stale bytes — drop them so a subsequent lazy read sees the new page.
        lazyValueCache.invalidateForPage(tableSpace, uuid, pageId);
    }

    /**
     * Writes {@code buf} as a multipart object at {@code path} so that
     * subsequent {@link RemoteFileServiceClient#readFileRange} calls can
     * satisfy byte-range reads. Blocks larger than a single server block
     * are split into consecutive blocks.
     */
    private void writeAsMultipart(String path, ByteBuf buf) {
        final int blockSize = client.getBlockSize();
        final int total = buf.readableBytes();
        if (total == 0) {
            // empty content: still create block 0 so readers see a valid file
            client.writeFileBlock(path, 0L, io.netty.buffer.Unpooled.EMPTY_BUFFER);
            return;
        }
        long blockIndex = 0L;
        int pos = buf.readerIndex();
        int remaining = total;
        while (remaining > 0) {
            int chunk = Math.min(remaining, blockSize);
            ByteBuf slice = buf.slice(pos, chunk);
            client.writeFileBlock(path, blockIndex, slice);
            pos += chunk;
            remaining -= chunk;
            blockIndex++;
        }
    }

    // -------------------------------------------------------------------------
    // Lazy read helpers (range-read v2 pages)
    // -------------------------------------------------------------------------

    /**
     * Reads the 22-byte fixed header of a v2 page via a byte-range read and
     * returns the parsed counts/sizes. Throws
     * {@link DataPageDoesNotExistException} if the remote file is absent.
     */
    LazyDataPageFormat.FixedHeader readPageHeader(String tableSpace, String uuid, long pageId)
            throws DataStorageManagerException {
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        byte[] headerBytes;
        try {
            headerBytes = client.readFileRange(path, 0L,
                    LazyDataPageFormat.FIXED_HEADER_SIZE, client.getBlockSize());
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                throw new DataPageDoesNotExistException(
                        "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
            }
            throw new DataStorageManagerException("Error reading remote page header: " + path, e);
        }
        if (headerBytes == null || headerBytes.length < LazyDataPageFormat.FIXED_HEADER_SIZE) {
            throw new DataPageDoesNotExistException(
                    "No such remote page: " + tableSpace + "_" + uuid + "." + pageId);
        }
        ByteBuf tmp = io.netty.buffer.Unpooled.wrappedBuffer(headerBytes);
        try {
            return LazyDataPageFormat.readHeader(tmp);
        } finally {
            tmp.release();
        }
    }

    /**
     * Reads the index section of a v2 page via a byte-range read and returns
     * the per-record metadata (key + value offset + value length).
     */
    List<LazyDataPageFormat.RecordMetadata> readPageIndex(String tableSpace, String uuid,
            long pageId, LazyDataPageFormat.FixedHeader h) throws DataStorageManagerException {
        if (h.indexSize == 0) {
            return Collections.emptyList();
        }
        String path = remoteDataPagePath(tableSpace, uuid, pageId);
        byte[] indexBytes;
        try {
            indexBytes = client.readFileRange(path,
                    LazyDataPageFormat.FIXED_HEADER_SIZE, h.indexSize, client.getBlockSize());
        } catch (RuntimeException e) {
            throw new DataStorageManagerException("Error reading remote page index: " + path, e);
        }
        if (indexBytes == null || indexBytes.length < h.indexSize) {
            throw new DataStorageManagerException("Short read for remote page index: " + path);
        }
        ByteBuf tmp = io.netty.buffer.Unpooled.wrappedBuffer(indexBytes);
        try {
            return LazyDataPageFormat.readIndex(tmp, h.numRecords);
        } finally {
            tmp.release();
        }
    }

    /**
     * Fetches a single record value from a v2 page, consulting the value
     * cache first and issuing a byte-range read against remote storage on
     * miss. Returns a freshly-owned {@code byte[]}.
     */
    byte[] readPageValue(String tableSpace, String uuid, long pageId,
            LazyDataPageFormat.FixedHeader h, long valueOffset, int valueLength)
            throws DataStorageManagerException {
        if (valueLength == 0) {
            return new byte[0];
        }
        LazyValueCache.ValueKey key = new LazyValueCache.ValueKey(
                tableSpace, uuid, pageId, valueOffset);
        try {
            return lazyValueCache.getOrFetch(key, () -> {
                long absolute = LazyDataPageFormat.absoluteValueOffset(h, valueOffset);
                String path = remoteDataPagePath(tableSpace, uuid, pageId);
                byte[] bytes = client.readFileRange(path, absolute, valueLength, client.getBlockSize());
                if (bytes == null || bytes.length < valueLength) {
                    throw new IllegalStateException("Short read for value at "
                            + path + "[" + absolute + "+" + valueLength + "]");
                }
                return bytes;
            });
        } catch (IllegalStateException e) {
            throw new DataStorageManagerException(e.getMessage(), e);
        }
    }

    private static boolean isNotFound(RuntimeException e) {
        // readFileRangeAsync wraps io.grpc status exceptions; surface common
        // "not found" conditions as DataPageDoesNotExistException.
        Throwable t = e;
        while (t != null) {
            String msg = t.getMessage();
            if (msg != null && (msg.contains("NOT_FOUND") || msg.contains("does not exist"))) {
                return true;
            }
            t = t.getCause();
        }
        return false;
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
            ByteBuf buf = serializeIndexPage(writer);
            try {
                client.writeFile(path, buf);
            } finally {
                buf.release();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing remote index page: " + path, e);
        }
    }

    @Override
    public void deleteIndexPage(String tableSpace, String uuid, long pageId)
            throws DataStorageManagerException {
        // Used by PersistentVectorStore's Phase-B rollback path to reclaim
        // pages that were written but never made it into a durable
        // IndexStatus checkpoint. Without this override we fall through to
        // the no-op base implementation and leak S3 objects until the next
        // successful indexCheckpoint sweep — which may never come if the
        // failure cause (e.g. remote storage unreachable) keeps recurring.
        String path = remoteIndexPagePath(tableSpace, uuid, pageId);
        try {
            client.deleteFile(path);
        } catch (RuntimeException ignored) {
            // Idempotent: deleting a page that was never written must not
            // throw. Log-worthy but not fatal; the caller already has the
            // original failure in hand.
            LOGGER.log(Level.FINE,
                    "deleteIndexPage: non-fatal error deleting {0}: {1}",
                    new Object[]{path, ignored.getMessage()});
        }
    }

    // -------------------------------------------------------------------------
    // Multipart large-file support (FusedPQ graphs, map data, etc.)
    // -------------------------------------------------------------------------

    @Override
    public String writeMultipartIndexFile(String tableSpace, String uuid, String fileType,
                                          Path tempFile, LongConsumer progress)
            throws IOException, DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        int blockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        long totalBytes;
        try (java.io.InputStream raw = java.nio.file.Files.newInputStream(tempFile);
             java.io.InputStream in = progress != null
                     ? new CountingInputStream(raw, progress)
                     : raw) {
            totalBytes = client.writeMultipartFile(logicalPath, in, blockSize);
        }
        LOGGER.log(Level.INFO,
                "writeMultipartIndexFile: {0} written {1} bytes in blocks of {2}",
                new Object[]{logicalPath, totalBytes, blockSize});
        return logicalPath;
    }

    /**
     * Wraps an InputStream and reports the number of bytes read since the last
     * report via a {@link LongConsumer}. Used by Phase-B multipart uploads so
     * that the PersistentVectorStore can expose mid-flight upload progress.
     */
    private static final class CountingInputStream extends java.io.FilterInputStream {
        private final LongConsumer progress;

        CountingInputStream(java.io.InputStream in, LongConsumer progress) {
            super(in);
            this.progress = progress;
        }

        @Override
        public int read() throws IOException {
            int r = super.read();
            if (r >= 0) {
                progress.accept(1L);
            }
            return r;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int r = super.read(b, off, len);
            if (r > 0) {
                progress.accept(r);
            }
            return r;
        }
    }

    @Override
    public io.github.jbellis.jvector.disk.ReaderSupplier multipartIndexReaderSupplier(
            String tableSpace, String uuid, String fileType, long fileSize)
            throws DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        int writeBlockSize = Math.max(client.getBlockSize(), MULTIPART_BLOCK_SIZE);
        return new RemoteRandomAccessReader.Supplier(
                client, logicalPath, fileSize, writeBlockSize, READ_BUFFER_SIZE, null);
    }

    @Override
    public void deleteMultipartIndexFile(String tableSpace, String uuid, String fileType)
            throws DataStorageManagerException {
        String logicalPath = remoteMultipartPath(tableSpace, uuid, fileType);
        try {
            client.deleteFile(logicalPath);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING,
                    "deleteMultipartIndexFile: non-fatal error deleting {0}: {1}",
                    new Object[]{logicalPath, e.getMessage()});
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
        Set<Long> currentActivePages = tableStatus.activePages.keySet();
        String key = tableSpace + "/" + uuid;

        Set<Long> previousActivePages = lastCheckpointedDataPages.get(key);
        List<long[]> newlyStale = new ArrayList<>(); // [pageId] or [-1] when only path is known
        List<String> newlyStalePaths = new ArrayList<>();
        if (previousActivePages != null) {
            // Fast path: diff against the previous checkpoint — no remote listing needed
            for (Long pageId : previousActivePages) {
                if (!pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remoteDataPagePath(tableSpace, uuid, pageId));
                }
            }
        } else {
            // First checkpoint after boot: enumerate all remote files to find orphans
            LOGGER.log(Level.INFO, "tableCheckpoint {0}/{1}: using full remote listing (first checkpoint after boot)",
                    new Object[]{tableSpace, uuid});
            List<String> remotePages = client.listFiles(remoteDataPrefix(tableSpace, uuid));
            for (String remotePath : remotePages) {
                long pageId = pageIdFromRemotePath(remotePath);
                if (pageId > 0
                        && !pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remotePath);
                }
            }
        }
        lastCheckpointedDataPages.put(key, new HashSet<>(currentActivePages));

        // Emit data-page deletion actions: either deferred (with retention) or immediate
        if (retentionEnabled) {
            long now = currentTimeMillis();
            List<PendingDeletion> bucket = pendingDataDeletions.computeIfAbsent(key,
                    k -> Collections.synchronizedList(new ArrayList<>()));
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                bucket.add(new PendingDeletion(tableStatus.sequenceNumber, now, newlyStalePaths.get(i),
                        "delete remote page " + pageId, tableSpace, uuid));
            }
            result.addAll(promotePendingDeletions(pendingDataDeletions, key, tableSpace));
        } else {
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote page " + pageId, newlyStalePaths.get(i), client));
            }
        }

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTableStatus(tableSpace, uuid, tableStatus);
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
        Set<Long> currentActivePages = indexStatus.activePages;
        String key = tableSpace + "/" + uuid;

        Set<Long> previousActivePages = lastCheckpointedIndexPages.get(key);
        List<long[]> newlyStale = new ArrayList<>();
        List<String> newlyStalePaths = new ArrayList<>();
        if (previousActivePages != null) {
            // Fast path: diff against the previous checkpoint — no remote listing needed
            for (Long pageId : previousActivePages) {
                if (!pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remoteIndexPagePath(tableSpace, uuid, pageId));
                }
            }
        } else {
            // First checkpoint after boot: enumerate all remote files to find orphans
            LOGGER.log(Level.INFO, "indexCheckpoint {0}/{1}: using full remote listing (first checkpoint after boot)",
                    new Object[]{tableSpace, uuid});
            List<String> remotePages = client.listFiles(remoteIndexPrefix(tableSpace, uuid));
            for (String remotePath : remotePages) {
                long pageId = pageIdFromRemotePath(remotePath);
                if (pageId > 0
                        && !pins.containsKey(pageId)
                        && !currentActivePages.contains(pageId)
                        && pageId < maxPageId) {
                    newlyStale.add(new long[]{pageId});
                    newlyStalePaths.add(remotePath);
                }
            }
        }
        lastCheckpointedIndexPages.put(key, new HashSet<>(currentActivePages));

        // Emit index-page deletion actions: either deferred (with retention) or immediate
        if (retentionEnabled) {
            long now = currentTimeMillis();
            List<PendingDeletion> bucket = pendingIndexDeletions.computeIfAbsent(key,
                    k -> Collections.synchronizedList(new ArrayList<>()));
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                bucket.add(new PendingDeletion(indexStatus.sequenceNumber, now, newlyStalePaths.get(i),
                        "delete remote index page " + pageId, tableSpace, uuid));
            }
            result.addAll(promotePendingDeletions(pendingIndexDeletions, key, tableSpace));
        } else {
            for (int i = 0; i < newlyStale.size(); i++) {
                long pageId = newlyStale.get(i)[0];
                result.add(new RemoteDeletePageAction(tableSpace, uuid,
                        "delete remote index page " + pageId, newlyStalePaths.get(i), client));
            }
        }

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeIndexStatus(tableSpace, uuid, indexStatus);
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
        String key = tableSpace + "/" + uuid;
        lastCheckpointedDataPages.remove(key);
        pendingDataDeletions.remove(key);
        lazyValueCache.invalidateForTable(tableSpace, uuid);
    }

    @Override
    public void dropIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.dropIndex(tableSpace, uuid);
        client.deleteByPrefix(remoteIndexPrefix(tableSpace, uuid));
        String key = tableSpace + "/" + uuid;
        lastCheckpointedIndexPages.remove(key);
        pendingIndexDeletions.remove(key);
    }

    @Override
    public void truncateIndex(String tableSpace, String uuid) throws DataStorageManagerException {
        localMetadataManager.truncateIndex(tableSpace, uuid);
        client.deleteByPrefix(remoteIndexPrefix(tableSpace, uuid));
        String key = tableSpace + "/" + uuid;
        lastCheckpointedIndexPages.remove(key);
        pendingIndexDeletions.remove(key);
    }

    @Override
    public void eraseTablespaceData(String tableSpace) throws DataStorageManagerException {
        localMetadataManager.eraseTablespaceData(tableSpace);
        client.deleteByPrefix(remoteTablespacePrefix(tableSpace));
        String prefix = tableSpace + "/";
        lastCheckpointedDataPages.keySet().removeIf(k -> k.startsWith(prefix));
        lastCheckpointedIndexPages.keySet().removeIf(k -> k.startsWith(prefix));
        pendingDataDeletions.keySet().removeIf(k -> k.startsWith(prefix));
        pendingIndexDeletions.keySet().removeIf(k -> k.startsWith(prefix));
        lazyValueCache.invalidateForTablespace(tableSpace);
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
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeTables(tableSpace, sequenceNumber, tables, indexlist, prepareActions);

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTableDefinitions(tableSpace, sequenceNumber, tables);
            shared.writeIndexDefinitions(tableSpace, sequenceNumber, indexlist);
        }

        return result;
    }

    @Override
    public Collection<PostCheckpointAction> writeCheckpointSequenceNumber(String tableSpace,
            LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeCheckpointSequenceNumber(tableSpace, sequenceNumber);

        // Publish to shared storage for read replicas — this is written LAST,
        // acting as the atomic commit marker for the checkpoint metadata
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeCheckpointLsn(tableSpace, sequenceNumber);
        }

        return result;
    }

    @Override
    public Collection<PostCheckpointAction> writeTransactionsAtCheckpoint(String tableSpace,
            LogSequenceNumber sequenceNumber, Collection<Transaction> transactions)
            throws DataStorageManagerException {
        Collection<PostCheckpointAction> result =
                localMetadataManager.writeTransactionsAtCheckpoint(tableSpace, sequenceNumber, transactions);

        // Publish to shared storage for read replicas
        SharedCheckpointMetadataManager shared = this.sharedCheckpointMetadataManager;
        if (shared != null) {
            shared.writeTransactions(tableSpace, sequenceNumber, transactions);
        }

        return result;
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
        return KeyToPageIndexFactory.create(tableSpace, uuid, memoryManager, this);
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
