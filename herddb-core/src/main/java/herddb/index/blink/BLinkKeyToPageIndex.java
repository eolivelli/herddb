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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.core.AbstractIndexManager;
import herddb.core.HerdDBInternalException;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
import herddb.index.KeyToPageCheckpointSnapshot;
import herddb.index.KeyToPageIndex;
import herddb.index.PrimaryIndexPrefixScan;
import herddb.index.PrimaryIndexRangeScan;
import herddb.index.PrimaryIndexSeek;
import herddb.index.blink.BLinkMetadata.BLinkNodeMetadata;
import herddb.log.LogSequenceNumber;
import herddb.model.ColumnTypes;
import herddb.model.InvalidNullValueForKeyException;
import herddb.model.RecordFunction;
import herddb.model.StatementEvaluationContext;
import herddb.model.StatementExecutionException;
import herddb.model.TableContext;
import herddb.sql.SQLRecordKeyFunction;
import herddb.storage.DataStorageManager;
import herddb.storage.DataStorageManagerException;
import herddb.storage.IndexStatus;
import herddb.utils.ByteArrayCursor;
import herddb.utils.Bytes;
import herddb.utils.ExtendedDataOutputStream;
import herddb.utils.HerdDBByteBufAllocators;
import herddb.utils.IndexKeySlab;
import herddb.utils.SystemProperties;
import herddb.utils.VisibleByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Implementation of {@link KeyToPageIndex} with a backing {@link BLink} paged and stored to {@link DataStorageManager}.
 *
 * @author diego.salvi
 */
public class BLinkKeyToPageIndex implements KeyToPageIndex {

    private static final Logger LOGGER = Logger.getLogger(BLinkKeyToPageIndex.class.getName());

    /**
     * Maximum number of dirty BLink nodes that a single
     * {@link #checkpoint(LogSequenceNumber, boolean)} invocation will keep in
     * flight when dispatching their page writes through
     * {@link DataStorageManager#writeIndexPageAsync}. Tuned via the
     * {@code herddb.checkpoint.flush.parallelism} system property (matches the
     * user-facing {@code server.checkpoint.flush.parallelism} server config).
     * Defaults to 16 to match
     * {@code ServerConfiguration.PROPERTY_CHECKPOINT_FLUSH_PARALLELISM_DEFAULT}.
     */
    private static final int CHECKPOINT_FLUSH_PARALLELISM =
            Math.max(1, SystemProperties.getIntSystemProperty(
                    "herddb.checkpoint.flush.parallelism", 16));

    public static final byte METADATA_PAGE = 0;
    public static final byte INNER_NODE_PAGE = 1;
    public static final byte LEAF_NODE_PAGE = 2;

    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;

    private static final int METADATA_PAGE_END_BLOCK = 0;
    private static final int METADATA_PAGE_NODE_BLOCK = 1;

    private final String tableSpace;
    private final String indexName;

    private final MemoryManager memoryManager;
    private final DataStorageManager dataStorageManager;

    private final AtomicLong newPageId;

    private final BLinkIndexDataStorage<Bytes, Long> indexDataStorage;

    private volatile BLink<Bytes, Long> tree;

    /**
     * When non-null, {@link BLinkIndexDataStorageImpl#createPage} dispatches
     * node writes through
     * {@link DataStorageManager#writeIndexPageAsync(String, String, long, DataStorageManager.DataWriter)}
     * and accumulates the returned futures on this batch. Set only for the
     * duration of {@link #checkpoint(LogSequenceNumber, boolean)} so DML-driven
     * page writes (splits, deletes) continue to go through the synchronous
     * {@link DataStorageManager#writeIndexPage} path. Access is protected by
     * the fact that checkpoint is serialized at the caller (the TableManager
     * Phase-C write lock).
     */
    private volatile AsyncIndexWriteBatch currentBatch;

    private final AtomicBoolean closed;

    public static String deriveIndexName(String tableName) {
        return tableName + "_primary";
    }

    public BLinkKeyToPageIndex(String tableSpace, String tableName, MemoryManager memoryManager, DataStorageManager dataStorageManager) {
        super();
        this.tableSpace = tableSpace;
        this.indexName = deriveIndexName(tableName);

        this.memoryManager = memoryManager;
        this.dataStorageManager = dataStorageManager;

        this.newPageId = new AtomicLong(1);
        this.indexDataStorage = new BLinkIndexDataStorageImpl();

        this.closed = new AtomicBoolean(false);
    }

    @Override
    public long size() {
        return getTree().size();
    }

    @Override
    public void put(Bytes key, Long currentPage) {
        try {
            getTree().insert(key, currentPage);
        } catch (UncheckedIOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public boolean put(Bytes key, Long newPage, Long expectedPage) {
        try {
            return getTree().insert(key, newPage, expectedPage);
        } catch (UncheckedIOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public boolean containsKey(Bytes key) {
        try {
            return getTree().search(key) != null;
        } catch (UncheckedIOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public Long get(Bytes key) {
        try {
            return getTree().search(key);
        } catch (UncheckedIOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public Long remove(Bytes key) {
        try {
            return getTree().delete(key);
        } catch (UncheckedIOException err) {
            throw new HerdDBInternalException(err);
        }
    }

    @Override
    public boolean isSortedAscending(int[] pkTypes) {
        // Composite PKs are byte-sortable when every component is, but the
        // planner currently only consumes single-column ascending PKs, so
        // we keep the simple guard here.
        return pkTypes.length == 1 && ColumnTypes.isByteSortable(pkTypes[0]);
    }

    @Override
    public Bytes getMinKey() {
        Entry<Bytes, Long> e = getTree().getFirstEntry();
        return e == null ? null : e.getKey();
    }

    @Override
    public Bytes getMaxKey() {
        Entry<Bytes, Long> e = getTree().getLastEntry();
        return e == null ? null : e.getKey();
    }

    @Override
    public Stream<Entry<Bytes, Long>> scanner(
            IndexOperation operation, StatementEvaluationContext context,
            TableContext tableContext, AbstractIndexManager index
    ) throws DataStorageManagerException, StatementExecutionException {
        if (operation instanceof PrimaryIndexSeek) {
            PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
            Bytes key = computeKeyValue(seek.value, context, tableContext);
            if (key == null) {
                return Stream.empty();
            }
            Long pageId = getTree().search(key);
            if (pageId == null) {
                return Stream.empty();
            }
            return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, pageId));
        }

        if (operation instanceof PrimaryIndexPrefixScan) {

            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
//            SQLRecordKeyFunction value = sis.value;
            Bytes firstKey = computeKeyValue(scan.value, context, tableContext);
            if (firstKey == null) {
                return Stream.empty();
            }
            Bytes lastKey = firstKey.next();

            return getTree().scan(firstKey, lastKey);
        }

        // Remember that the IndexOperation can return more records
        // every predicate (WHEREs...) will always be evaluated anyway on every record, in order to guarantee correctness
        if (index != null) {
            return index.recordSetScanner(operation, context, tableContext, this);
        }

        if (operation == null) {
            Stream<Map.Entry<Bytes, Long>> baseStream = getTree().scan(null, null);
            return baseStream;
        } else if (operation instanceof PrimaryIndexRangeScan) {

            Bytes refminvalue;
            PrimaryIndexRangeScan sis = (PrimaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                refminvalue = minKey.computeNewValue(null, context, tableContext);
            } else {
                refminvalue = null;
            }
            Bytes refmaxvalue;
            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                refmaxvalue = maxKey.computeNewValue(null, context, tableContext);
            } else {
                refmaxvalue = null;
            }
            return getTree().scan(refminvalue, refmaxvalue, refmaxvalue != null);
        }

        throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());

    }

    private static Bytes computeKeyValue(RecordFunction keyFun, StatementEvaluationContext context, TableContext tableContext) throws StatementExecutionException {
        try {
            return keyFun.computeNewValue(null, context, tableContext);
        } catch (InvalidNullValueForKeyException invalidKeyValueException) {
            // select * from table where k=null
            // select * from table where firstColumnInKey=null
            return null;
        }
    }

    @Override
    public void close() throws DataStorageManagerException {

        if (closed.compareAndSet(false, true)) {
            final BLink<Bytes, Long> tree = this.tree;
            this.tree = null;
            if (tree != null) {
                tree.close();
            }
        } else {
            throw new DataStorageManagerException("Index " + indexName + " already closed");
        }
    }

    @Override
    public void truncate() {
        getTree().truncate();
    }

    @Override
    public void dropData() {
        truncate();
        dataStorageManager.dropIndex(tableSpace, indexName);
    }

    @Override
    public long getUsedMemory() {
        return getTree().getUsedMemory();
    }

    /**
     * Maximum number of {@link BLink.NodeInfo per-node entries} that
     * {@link #snapshotInfo()} will include in its response. Larger trees are
     * truncated; the {@link PrimaryIndexSnapshot#isTruncated() truncated} flag
     * is set so the UI can warn the operator. Tests may override this via
     * {@link #setSnapshotNodeLimit(int)}.
     */
    public static final int DEFAULT_SNAPSHOT_NODE_LIMIT = 10_000;

    private static volatile int snapshotNodeLimit = DEFAULT_SNAPSHOT_NODE_LIMIT;

    /**
     * Test hook: override the per-snapshot node cap. Returns the previous
     * value so callers can restore it.
     */
    public static int setSnapshotNodeLimit(int newLimit) {
        int prev = snapshotNodeLimit;
        snapshotNodeLimit = newLimit;
        return prev;
    }

    /**
     * Read-only snapshot of the primary key index tree, used by the Web UI
     * v2 backend to render a "primary index" summary panel and a per-node
     * layout heatmap.
     *
     * <p>The snapshot does not force a checkpoint: it only reports the
     * counters already maintained by the in-memory tree (number of keys,
     * number of nodes currently in memory, total memory occupancy) plus a
     * weakly-consistent per-node view (id, on-disk page, loaded/dirty flags,
     * keys and size). Concurrent mutations may produce slightly inconsistent
     * per-node reads — the snapshot is for visualisation only.
     */
    public PrimaryIndexSnapshot snapshotInfo() {
        BLink<Bytes, Long> tree = getTree();
        int totalNodes = tree.nodes();
        int limit = snapshotNodeLimit;
        java.util.List<BLink.NodeInfo> sample = tree.snapshotNodes(limit);
        boolean truncated = limit > 0 && totalNodes > sample.size();
        // Count nodes currently resident in memory from the per-node sample.
        // When truncated, this is a lower bound — same convention as the
        // visualisation use case that requested this metric.
        int loadedNodes = 0;
        for (BLink.NodeInfo info : sample) {
            if (info.isLoaded()) {
                loadedNodes++;
            }
        }
        return new PrimaryIndexSnapshot(
                tree.size(),
                loadedNodes,
                tree.getUsedMemory(),
                sample,
                truncated,
                totalNodes);
    }

    /**
     * Immutable view returned by {@link #snapshotInfo()}.
     */
    public static final class PrimaryIndexSnapshot {

        private final long entries;
        private final int loadedNodes;
        private final long usedMemoryBytes;
        private final java.util.List<BLink.NodeInfo> nodes;
        private final boolean truncated;
        private final int totalNodes;

        /**
         * Legacy 3-argument constructor retained for backwards compatibility
         * with non-BLink callers (fallback branch in the UI backend, plus
         * any external consumer that does not need per-node details). The
         * new fields default to an empty node list, {@code truncated=false}
         * and {@code totalNodes=loadedNodes}.
         */
        public PrimaryIndexSnapshot(long entries, int loadedNodes, long usedMemoryBytes) {
            this(entries, loadedNodes, usedMemoryBytes, Collections.emptyList(),
                    false, loadedNodes);
        }

        public PrimaryIndexSnapshot(long entries, int loadedNodes, long usedMemoryBytes,
                                    java.util.List<BLink.NodeInfo> nodes, boolean truncated,
                                    int totalNodes) {
            this.entries = entries;
            this.loadedNodes = loadedNodes;
            this.usedMemoryBytes = usedMemoryBytes;
            this.nodes = nodes == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new java.util.ArrayList<>(nodes));
            this.truncated = truncated;
            this.totalNodes = totalNodes;
        }

        public long getEntries() {
            return entries;
        }

        public int getLoadedNodes() {
            return loadedNodes;
        }

        public long getUsedMemoryBytes() {
            return usedMemoryBytes;
        }

        public java.util.List<BLink.NodeInfo> getNodes() {
            return nodes;
        }

        public boolean isTruncated() {
            return truncated;
        }

        public int getTotalNodes() {
            return totalNodes;
        }
    }

    @Override
    public boolean requireLoadAtStartup() {
        return false;
    }

    @Override
    public void init() throws DataStorageManagerException {
        dataStorageManager.initIndex(tableSpace, indexName);
    }

    @Override
    public void start(LogSequenceNumber sequenceNumber, boolean created) throws DataStorageManagerException {

        if (!created) {
            LOGGER.log(Level.INFO, " start index {0}", new Object[]{indexName});
        }
        /* Actually the same size */
        final long pageSize = memoryManager.getMaxLogicalPageSize();

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            /* Empty index (booting from the start) */
            tree = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                    memoryManager.getPKPageReplacementPolicy(), indexDataStorage);
            if (!created) {
                LOGGER.log(Level.INFO, "loaded empty index {0}", new Object[]{indexName});
            }
        } else {
            IndexStatus status = dataStorageManager.getIndexStatus(tableSpace, indexName, sequenceNumber);
            try {
                BLinkMetadata<Bytes> metadata = MetadataSerializer.INSTANCE.read(status.indexData);

                tree = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                        memoryManager.getPKPageReplacementPolicy(), indexDataStorage,
                        metadata);
            } catch (IOException e) {
                throw new DataStorageManagerException(e);
            }

            newPageId.set(status.newPageId);
            LOGGER.log(Level.INFO, "loaded index {0}: {1} keys", new Object[]{indexName, tree.size()});
        }
    }

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin) throws DataStorageManagerException {
        // Single-phase entry point: kept for callers that do not opt into the
        // fuzzy two-phase protocol (tests, dump, restoreRawDumpedEntryLogs).
        // Production callers in TableManager Phase C use prepareCheckpoint +
        // persistCheckpoint directly so the slow remote I/O runs outside the
        // checkpoint write lock (issue #403).
        KeyToPageCheckpointSnapshot snapshot = prepareCheckpoint(sequenceNumber, pin);
        return persistCheckpoint(snapshot);
    }

    @Override
    public KeyToPageCheckpointSnapshot prepareCheckpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        /* Tree can be null if no data was inserted (tree creation deferred to check evaluate key size) */
        final BLink<Bytes, Long> tree = this.tree;
        if (tree == null) {
            return new BLinkCheckpointSnapshot(sequenceNumber, pin, /*empty*/ true,
                    null, null, null, 0L);
        }

        /*
         * Install a per-checkpoint batch so node-page writes go through
         * writeIndexPageAsync and run concurrently (issue #202). The BLink
         * tree traversal itself remains sequential, but the remote I/O is
         * fanned out up to CHECKPOINT_FLUSH_PARALLELISM in flight. All
         * futures are awaited inside persistCheckpoint, before we write the
         * IndexStatus, so the checkpoint marker never references a page
         * whose bytes are not yet durable.
         *
         * Issue #403: the BLink traversal that mutates the per-node "dirty"
         * flags must run while the caller still excludes DML (TableManager's
         * checkpointLock write). The waitBatch + indexCheckpoint write run
         * outside the lock inside persistCheckpoint.
         */
        AsyncIndexWriteBatch batch = new AsyncIndexWriteBatch(CHECKPOINT_FLUSH_PARALLELISM);
        BLinkMetadata<Bytes> metadata;
        currentBatch = batch;
        try {
            metadata = getTree().checkpoint();
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        } finally {
            currentBatch = null;
        }

        byte[] metaPage;
        try {
            metaPage = MetadataSerializer.INSTANCE.write(metadata);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Set<Long> activePages = new HashSet<>();
        metadata.nodes.forEach(node -> activePages.add(node.storeId));

        return new BLinkCheckpointSnapshot(sequenceNumber, pin, /*empty*/ false,
                metaPage, activePages, batch, newPageId.get());
    }

    @Override
    public List<PostCheckpointAction> persistCheckpoint(KeyToPageCheckpointSnapshot snapshot)
            throws DataStorageManagerException {
        BLinkCheckpointSnapshot s = (BLinkCheckpointSnapshot) snapshot;
        if (s.empty) {
            return Collections.emptyList();
        }
        // Wait for the BLink node-page bytes referenced by the metadata to be
        // durable on storage. This blocks on remote I/O but does NOT hold the
        // TableManager.checkpointLock write — concurrent DML can proceed.
        awaitBatch(s.pendingNodeWrites);

        IndexStatus indexStatus = new IndexStatus(indexName, s.sequenceNumber(),
                s.newPageIdAtSnapshot, s.activePages, s.metaPage);
        List<PostCheckpointAction> result = new ArrayList<>(
                dataStorageManager.indexCheckpoint(tableSpace, indexName, indexStatus, s.pin));

        LOGGER.log(Level.INFO, "checkpoint index {0} finished: logpos {1}, {2} pages",
                new Object[]{indexName, s.sequenceNumber(), Integer.toString(s.activePages.size())});
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE, "checkpoint index {0} finished: logpos {1}, pages {2}",
                    new Object[]{indexName, s.sequenceNumber(), s.activePages.toString()});
        }
        return result;
    }

    /**
     * Snapshot taken under the caller's exclusion window in
     * {@link #prepareCheckpoint(LogSequenceNumber, boolean)} and consumed
     * outside the lock by
     * {@link #persistCheckpoint(KeyToPageCheckpointSnapshot)}. See issue #403.
     */
    static final class BLinkCheckpointSnapshot implements KeyToPageCheckpointSnapshot {

        private final LogSequenceNumber sequenceNumber;
        final boolean pin;
        final boolean empty;
        final byte[] metaPage;
        final Set<Long> activePages;
        final AsyncIndexWriteBatch pendingNodeWrites;
        final long newPageIdAtSnapshot;

        BLinkCheckpointSnapshot(LogSequenceNumber sequenceNumber, boolean pin, boolean empty,
                byte[] metaPage, Set<Long> activePages, AsyncIndexWriteBatch pendingNodeWrites,
                long newPageIdAtSnapshot) {
            this.sequenceNumber = sequenceNumber;
            this.pin = pin;
            this.empty = empty;
            this.metaPage = metaPage;
            this.activePages = activePages;
            this.pendingNodeWrites = pendingNodeWrites;
            this.newPageIdAtSnapshot = newPageIdAtSnapshot;
        }

        @Override
        public LogSequenceNumber sequenceNumber() {
            return sequenceNumber;
        }
    }

    /**
     * Joins every async index-page write submitted during checkpoint. If any
     * future failed, surface the first root cause as a
     * {@link DataStorageManagerException}; other failed futures are silently
     * drained so that by the time we return all gRPC activity is quiesced and
     * the checkpoint can fail cleanly without leaking work to the next phase.
     */
    private void awaitBatch(AsyncIndexWriteBatch batch) throws DataStorageManagerException {
        List<CompletableFuture<Void>> futures = batch.snapshot();
        if (futures.isEmpty()) {
            return;
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof DataStorageManagerException) {
                throw (DataStorageManagerException) cause;
            }
            throw new DataStorageManagerException(
                    "Error awaiting async BLink index page writes for index " + indexName, cause);
        }
    }

    /**
     * Per-checkpoint batch coordinating async BLink node page writes. Holds
     * a semaphore bounding the number of in-flight writes and a list of the
     * submitted futures. Not thread-safe for concurrent batch creation; only
     * one batch is active at a time because checkpoint is serialized by the
     * TableManager Phase-C write lock.
     */
    private static final class AsyncIndexWriteBatch {
        final Semaphore permits;
        // Guarded by `this` since futures are added by the (single) traversal
        // thread but may be completed by arbitrary gRPC callback threads.
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();

        AsyncIndexWriteBatch(int parallelism) {
            this.permits = new Semaphore(parallelism);
        }

        synchronized void add(CompletableFuture<Void> future) {
            futures.add(future);
        }

        synchronized List<CompletableFuture<Void>> snapshot() {
            return new ArrayList<>(futures);
        }
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpace, indexName, sequenceNumber);
    }

    /**
     * Retrieve {@link BLink} tree checking the index status
     */
    private BLink<Bytes, Long> getTree() {
        final BLink<Bytes, Long> tree = this.tree;

        if (tree == null) {
            if (closed.get()) {
                throw new DataStorageManagerException("Index " + indexName + " already closed");
            } else {
                throw new DataStorageManagerException("Index " + indexName + " still not started");
            }
        }

        return tree;
    }



    public static final class MetadataSerializer {

        public static final MetadataSerializer INSTANCE = new MetadataSerializer();

        /**
         * Original Version
         */
        private static final long VERSION_0 = 0L;

        /**
         * Added flags options after version in metadata and forced byte size recalculation due to changes
         * on size evaluation algorithm
         *
         * @since 0.6.0
         */
        private static final long VERSION_1 = 1L;

        /**
         * Removed empty field in BLinkNodeMetadata
         *
         * @since 0.9.0
         */
        private static final long VERSION_2 = 2L;

        public static final long CURRENT_VERSION = VERSION_2;

        private static final long NO_FLAGS = 0L;

        public byte[] write(BLinkMetadata<Bytes> metadata) throws IOException {

            final VisibleByteArrayOutputStream bos = new VisibleByteArrayOutputStream();
            try (ExtendedDataOutputStream edos = new ExtendedDataOutputStream(bos)) {

                /* data version */
                edos.writeVLong(CURRENT_VERSION);

                /* flags for future implementations, actually unused */
                edos.writeVLong(NO_FLAGS);
                edos.writeByte(METADATA_PAGE);

                edos.writeVLong(metadata.nextID);

                edos.writeVLong(metadata.fast);
                edos.writeVInt(metadata.fastheight);

                edos.writeVLong(metadata.top);
                edos.writeVInt(metadata.topheight);

                edos.writeVLong(metadata.first);
                edos.writeVLong(metadata.values);

                for (BLinkNodeMetadata<Bytes> node : metadata.nodes) {

                    edos.writeVInt(METADATA_PAGE_NODE_BLOCK);

                    edos.writeBoolean(node.leaf);

                    edos.writeVLong(node.id);
                    edos.writeVLong(node.storeId);

                    edos.writeVInt(node.keys);
                    edos.writeVLong(node.bytes);

                    edos.writeZLong(node.outlink);
                    edos.writeZLong(node.rightlink);

                    boolean hasInf = node.rightsep == Bytes.POSITIVE_INFINITY;

                    edos.writeBoolean(hasInf);

                    if (!hasInf) {
                        edos.writeArray(node.rightsep.to_array());
                    }
                }

                edos.writeVInt(METADATA_PAGE_END_BLOCK);

            }

            return bos.toByteArray();

        }

        @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "flags still not used but it must be forcefully read")
        public BLinkMetadata<Bytes> read(byte[] data) throws IOException {

            try (ByteArrayCursor edis = ByteArrayCursor.wrap(data)) {

                long version = edis.readVLong();

                /*
                 * Check if byte size needs to be recalculated (between v.0 and v.1 was changed size evaluation
                 * algorithm so v.0 stored size is meaningless)
                 */
                boolean recalculateSize = version == VERSION_0;

                /* flags for future implementations, actually unused (exists from version 1) */
                @SuppressWarnings("unused")
                long flags = version > VERSION_0 ? edis.readVLong() : NO_FLAGS;

                byte rtype = edis.readByte();

                if (rtype != METADATA_PAGE) {
                    throw new IOException("Wrong page type " + rtype + " expected " + METADATA_PAGE);
                }

                long nextID = edis.readVLong();

                long fast = edis.readVLong();
                int fastheight = edis.readVInt();

                long top = edis.readVLong();
                int topheight = edis.readVInt();

                long first = edis.readVLong();
                long values = edis.readVLong();

                List<BLinkNodeMetadata<Bytes>> nodes = new LinkedList<>();

                int block;

                while ((block = edis.readVInt()) != METADATA_PAGE_END_BLOCK) {

                    if (block != METADATA_PAGE_NODE_BLOCK) {
                        throw new IOException("Wrong block type " + block);
                    }

                    BLinkNodeMetadata<Bytes> node;

                    if (version == VERSION_2) {
                        node = readV2(edis, recalculateSize);

                    } else if (version == VERSION_1) {
                        node = readV1(edis, recalculateSize);

                    } else if (version == VERSION_0) {
                        node = readV0(edis, recalculateSize);

                    } else {
                        throw new IllegalArgumentException("Unknown BLink node metadata version " + version);
                    }

                    nodes.add(node);
                }

                return new BLinkMetadata<>(nextID, fast, fastheight, top, topheight, first, values, nodes);
            }

        }


        /**
         * Read node metadata V2
         */
        public BLinkNodeMetadata<Bytes> readV2(ByteArrayCursor cursor, boolean recalculateSize) throws IOException {

            final boolean leaf = cursor.readBoolean();

            long id = cursor.readVLong();
            long storeId = cursor.readVLong();

            int keys = cursor.readVInt();
            long bytes = cursor.readVLong();

            if (recalculateSize) {
                /* Set size to unknown to force node size recalculation */
                bytes = BLink.UNKNOWN_SIZE;
            }

            long outlink = cursor.readZLong();
            long rightlink = cursor.readZLong();

            boolean hasInf = cursor.readBoolean();

            Bytes rightsep;
            if (hasInf) {
                rightsep = Bytes.POSITIVE_INFINITY;
            } else {
                rightsep = cursor.readBytes();
            }

            return new BLinkNodeMetadata<>(leaf, id, storeId, keys, bytes, outlink, rightlink, rightsep);

        }

        /**
         * Read node metadata V1
         */
        public BLinkNodeMetadata<Bytes> readV1(ByteArrayCursor cursor, boolean recalculateSize) throws IOException {
            /* Actually node metadata v0 and v1 are identical, this method exists only to be more explicit */
            return readV0(cursor, recalculateSize);
        }

        /**
         * Read node metadata V0
         */
        public BLinkNodeMetadata<Bytes> readV0(ByteArrayCursor cursor, boolean recalculateSize) throws IOException {

            final boolean leaf = cursor.readBoolean();

            long id = cursor.readVLong();
            long storeId = cursor.readVLong();

            /* Boolean read, we don't need "empty" flag anymore but still exists in this version */
            cursor.readBoolean();

            int keys = cursor.readVInt();
            long bytes = cursor.readVLong();

            if (recalculateSize) {
                /* Set size to unknown to force node size recalculation */
                bytes = BLink.UNKNOWN_SIZE;
            }

            long outlink = cursor.readZLong();
            long rightlink = cursor.readZLong();

            boolean hasInf = cursor.readBoolean();

            Bytes rightsep;
            if (hasInf) {
                rightsep = Bytes.POSITIVE_INFINITY;
            } else {
                rightsep = cursor.readBytes();
            }

            return new BLinkNodeMetadata<>(leaf, id, storeId, keys, bytes, outlink, rightlink, rightsep);

        }
    }

    private final class BLinkIndexDataStorageImpl implements BLinkIndexDataStorage<Bytes, Long> {

        @Override
        public void loadNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, INNER_NODE_PAGE, data);
        }

        @Override
        public void loadLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            loadPage(pageId, LEAF_NODE_PAGE, data);
        }

        private void loadPage(long pageId, byte type, Map<Bytes, Long> map) throws IOException {

            dataStorageManager.readIndexPage(tableSpace, indexName, pageId, in -> {

                long version = in.readVLong();

                /* flags for future implementations, actually unused */
                long flags = in.readVLong();

                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted index page " + pageId);
                }

                byte rtype = in.readByte();

                if (rtype != type) {
                    throw new IOException("Wrong page type " + rtype + " expected " + type);
                }

                // Issue #399 step 4: collect all (key bytes, value) pairs
                // first so we can size and allocate a single off-heap slab
                // from HerdDBByteBufAllocators.indexPagesAllocator() and
                // pack every key into it. Below the threshold we keep the
                // legacy on-heap path to avoid the slab overhead on tiny
                // pages.
                List<byte[]> keyBytesList = new ArrayList<>();
                List<Long> valueList = new ArrayList<>();
                List<Long> infiniteValueList = new ArrayList<>();
                long totalKeyBytes = 0L;

                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {

                    switch (block) {

                        case NODE_PAGE_KEY_VALUE_BLOCK: {
                            byte[] k = in.readArray();
                            if (k == null) {
                                // Defensive: writer never emits a null array
                                // (it always passes a non-null byte[] to
                                // out.writeArray), so this means the on-disk
                                // page is corrupted. Fail fast — the previous
                                // map.put(in.readBytes(), …) path would have
                                // NPEd inside the TreeMap; this is more
                                // actionable.
                                throw new IOException("corrupted index page "
                                        + pageId + ": null key");
                            }
                            long v = in.readVLong();
                            keyBytesList.add(k);
                            valueList.add(v);
                            totalKeyBytes += (long) k.length;
                            break;
                        }

                        case NODE_PAGE_INF_BLOCK:
                            infiniteValueList.add(in.readVLong());
                            break;

                        default:
                            throw new IOException("Wrong node block type " + block);

                    }
                }

                if (totalKeyBytes >= IndexKeySlab.OFFHEAP_KEY_BYTES_THRESHOLD
                        && totalKeyBytes <= (long) Integer.MAX_VALUE) {
                    IndexKeySlab slabOwner = new IndexKeySlab(totalKeyBytes,
                            HerdDBByteBufAllocators.indexPagesAllocator());
                    for (int i = 0; i < keyBytesList.size(); i++) {
                        byte[] k = keyBytesList.get(i);
                        int off = slabOwner.append(k);
                        map.put(slabOwner.wrap(off, k.length), valueList.get(i));
                    }
                } else {
                    // On-heap fallback: total below the threshold, or the
                    // theoretical >2 GiB upper bound we cannot fit into one
                    // ByteBuf — keep the original behaviour.
                    for (int i = 0; i < keyBytesList.size(); i++) {
                        map.put(Bytes.from_array(keyBytesList.get(i)), valueList.get(i));
                    }
                }
                for (Long v : infiniteValueList) {
                    map.put(Bytes.POSITIVE_INFINITY, v);
                }

                return map;

            });

        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            return createPage(NEW_PAGE, data, INNER_NODE_PAGE);
        }

        @Override
        public long createLeafPage(Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            return createPage(NEW_PAGE, data, LEAF_NODE_PAGE);
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            createPage(pageId, data, INNER_NODE_PAGE);
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            /* Both node ids and leaf values are Long, direct both to a common method */
            createPage(pageId, data, LEAF_NODE_PAGE);
        }

        private long createPage(long pageId, Map<Bytes, Long> data, byte type) throws IOException {
            /* Write/overwrite switch */
            if (pageId == NEW_PAGE) {
                pageId = newPageId.getAndIncrement();
            }

            DataStorageManager.DataWriter writer = out -> {

                /* Data version */
                out.writeVLong(1);

                /* flags for future implementations, actually unused */
                out.writeVLong(0);

                out.writeByte(type);

                data.forEach((x, y) -> {
                    try {
                        if (x == Bytes.POSITIVE_INFINITY) {
                            // Handle special case for +inf key
                            out.writeByte(NODE_PAGE_INF_BLOCK);
                            out.writeVLong(y);
                        } else {
                            out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                            out.writeArray(x.to_array());
                            out.writeVLong(y);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException("Unexpected IOException during node page write preparation", e);
                    }
                });

                out.writeByte(NODE_PAGE_END_BLOCK);

            };

            /*
             * When a checkpoint is in progress (currentBatch != null) dispatch
             * the write asynchronously and record the future on the batch; the
             * outer BLinkKeyToPageIndex.checkpoint joins every future before
             * persisting the IndexStatus. Outside of checkpoint (e.g. DML
             * splits or recovery rebuilds), keep the synchronous path so
             * callers observe the write is durable before returning.
             */
            AsyncIndexWriteBatch batch = currentBatch;
            if (batch != null) {
                try {
                    batch.permits.acquire();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while acquiring BLink async-write permit", ex);
                }
                CompletableFuture<Void> future;
                try {
                    future = dataStorageManager.writeIndexPageAsync(
                            tableSpace, indexName, pageId, writer);
                } catch (RuntimeException ex) {
                    batch.permits.release();
                    throw ex;
                }
                batch.add(future.whenComplete((v, t) -> batch.permits.release()));
            } else {
                dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, writer);
            }

            return pageId;
        }
    }
}
