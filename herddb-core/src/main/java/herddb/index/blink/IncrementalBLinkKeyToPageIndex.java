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

import herddb.core.AbstractIndexManager;
import herddb.core.HerdDBInternalException;
import herddb.core.MemoryManager;
import herddb.core.PostCheckpointAction;
import herddb.index.IndexOperation;
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
import herddb.utils.Bytes;
import herddb.utils.SystemProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Incremental {@link KeyToPageIndex} implementation backed by the existing
 * {@link BLink} tree plus a new on-disk format whose checkpoint cost is
 * proportional to the number of nodes that changed since the previous
 * checkpoint — rather than to the total number of nodes in the tree.
 *
 * <p>The on-disk layout produced by this class is composed of three kinds of
 * pages stored through the standard {@code DataStorageManager.writeIndexPage}
 * entry point, plus one small manifest blob stored inside
 * {@code IndexStatus.indexData}:
 *
 * <ul>
 *   <li><b>Node pages</b> (inner / leaf) — identical format to the legacy
 *       {@link BLinkKeyToPageIndex}, produced by a nested
 *       {@link BLinkIndexDataStorage} implementation. This means node pages
 *       are byte-compatible between the two implementations; only the
 *       checkpoint-metadata pages differ.</li>
 *   <li><b>Snapshot-chunk pages</b> — the full list of
 *       {@link BLinkNodeMetadata} valid at some epoch, split across several
 *       pages to avoid unbounded single-page growth.</li>
 *   <li><b>Delta pages</b> — per-checkpoint deltas recording upserted node
 *       metadata, logically-removed node ids, and the dead store-page ids
 *       that the {@code DataStorageManager} garbage-collector may reclaim.</li>
 * </ul>
 *
 * <p>A running {@link IncrementalBLinkManifest} tracks the latest snapshot
 * reference and the chain of deltas applied on top of it. On recovery, the
 * snapshot is read and deltas are replayed to rebuild the full
 * {@link BLinkMetadata}, which is then passed to the normal {@code BLink}
 * recovery constructor.</p>
 *
 * <p>This class is selected at factory time based on
 * {@link herddb.index.KeyToPageIndexMode}.</p>
 *
 * @see BLinkKeyToPageIndex
 * @see IncrementalBLinkManifest
 * @see IncrementalBLinkPageCodec
 */
public class IncrementalBLinkKeyToPageIndex implements KeyToPageIndex {

    private static final Logger LOGGER = Logger.getLogger(IncrementalBLinkKeyToPageIndex.class.getName());

    /**
     * Number of deltas after which the next checkpoint rewrites a fresh full
     * snapshot instead of appending another delta. Bounded so recovery cost
     * stays bounded even on very long-running indexes.
     */
    public static final String PROP_SNAPSHOT_EVERY =
            "herddb.index.pk.incremental.snapshotEvery";

    /**
     * Target number of {@link BLinkNodeMetadata} entries per snapshot-chunk
     * page. Snapshot pages are split into chunks of at most this size so that
     * an individual snapshot write does not produce a single unbounded page.
     */
    public static final String PROP_SNAPSHOT_CHUNK_SIZE =
            "herddb.index.pk.incremental.snapshotChunkSize";

    private static final int DEFAULT_SNAPSHOT_EVERY =
            SystemProperties.getIntSystemProperty(PROP_SNAPSHOT_EVERY, 64);

    private static final int DEFAULT_SNAPSHOT_CHUNK_SIZE =
            SystemProperties.getIntSystemProperty(PROP_SNAPSHOT_CHUNK_SIZE, 50_000);

    private final String tableSpace;
    private final String indexName;

    private final MemoryManager memoryManager;
    private final DataStorageManager dataStorageManager;

    private final AtomicLong newPageId;

    private final BLinkIndexDataStorage<Bytes, Long> indexDataStorage;

    private volatile BLink<Bytes, Long> tree;

    private final AtomicBoolean closed;

    /**
     * In-memory cache of the node metadata persisted at the previous
     * checkpoint. Used to diff against the metadata produced by the next
     * {@code tree.checkpoint()} call. Maintained in sync with the on-disk
     * snapshot+delta chain.
     */
    private final Map<Long, BLinkNodeMetadata<Bytes>> previousByNodeId = new HashMap<>();

    /**
     * Latest committed manifest (or {@code null} if none has been committed
     * yet for this index).
     */
    private volatile IncrementalBLinkManifest currentManifest;

    /**
     * Preserve set computed by {@link #checkpoint} and supplied to
     * {@code DataStorageManager.indexCheckpoint} as {@code IndexStatus.activePages}.
     * Intentionally small: node pages currently referenced, snapshot chunk
     * pages, and active delta pages. Pages not in this set (and older than
     * the largest known page id) are reclaimed by the standard DSM GC path.
     */
    private Set<Long> lastPreserveSet = Collections.emptySet();

    public IncrementalBLinkKeyToPageIndex(String tableSpace, String tableName,
                                          MemoryManager memoryManager,
                                          DataStorageManager dataStorageManager) {
        super();
        this.tableSpace = tableSpace;
        this.indexName = BLinkKeyToPageIndex.deriveIndexName(tableName);
        this.memoryManager = memoryManager;
        this.dataStorageManager = dataStorageManager;
        this.newPageId = new AtomicLong(1);
        this.indexDataStorage = new NodePageStorageImpl();
        this.closed = new AtomicBoolean(false);
    }

    // ---------------- KeyToPageIndex tree ops ----------------

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
        if (pkTypes.length != 1) {
            return false;
        }
        switch (pkTypes[0]) {
            case ColumnTypes.NOTNULL_STRING:
            case ColumnTypes.STRING:
            case ColumnTypes.BYTEARRAY:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Stream<Entry<Bytes, Long>> scanner(
            IndexOperation operation, StatementEvaluationContext context,
            TableContext tableContext, AbstractIndexManager index
    ) throws DataStorageManagerException, StatementExecutionException {
        if (operation instanceof PrimaryIndexSeek) {
            PrimaryIndexSeek seek = (PrimaryIndexSeek) operation;
            byte[] seekValue = computeKeyValue(seek.value, context, tableContext);
            if (seekValue == null) {
                return Stream.empty();
            }
            Bytes key = Bytes.from_array(seekValue);
            Long pageId = getTree().search(key);
            if (pageId == null) {
                return Stream.empty();
            }
            return Stream.of(new AbstractMap.SimpleImmutableEntry<>(key, pageId));
        }

        if (operation instanceof PrimaryIndexPrefixScan) {
            PrimaryIndexPrefixScan scan = (PrimaryIndexPrefixScan) operation;
            byte[] refvalue = computeKeyValue(scan.value, context, tableContext);
            if (refvalue == null) {
                return Stream.empty();
            }
            Bytes firstKey = Bytes.from_array(refvalue);
            Bytes lastKey = firstKey.next();
            return getTree().scan(firstKey, lastKey);
        }

        if (index != null) {
            return index.recordSetScanner(operation, context, tableContext, this);
        }

        if (operation == null) {
            return getTree().scan(null, null);
        } else if (operation instanceof PrimaryIndexRangeScan) {
            Bytes refminvalue;
            PrimaryIndexRangeScan sis = (PrimaryIndexRangeScan) operation;
            SQLRecordKeyFunction minKey = sis.minValue;
            if (minKey != null) {
                refminvalue = Bytes.from_array(minKey.computeNewValue(null, context, tableContext));
            } else {
                refminvalue = null;
            }
            Bytes refmaxvalue;
            SQLRecordKeyFunction maxKey = sis.maxValue;
            if (maxKey != null) {
                refmaxvalue = Bytes.from_array(maxKey.computeNewValue(null, context, tableContext));
            } else {
                refmaxvalue = null;
            }
            return getTree().scan(refminvalue, refmaxvalue, refmaxvalue != null);
        }

        throw new DataStorageManagerException("operation " + operation + " not implemented on " + this.getClass());
    }

    private static byte[] computeKeyValue(RecordFunction keyFun, StatementEvaluationContext context, TableContext tableContext)
            throws StatementExecutionException {
        try {
            return keyFun.computeNewValue(null, context, tableContext);
        } catch (InvalidNullValueForKeyException invalidKeyValueException) {
            return null;
        }
    }

    // ---------------- lifecycle ----------------

    @Override
    public void close() throws DataStorageManagerException {
        if (closed.compareAndSet(false, true)) {
            final BLink<Bytes, Long> t = this.tree;
            this.tree = null;
            if (t != null) {
                t.close();
            }
            previousByNodeId.clear();
        } else {
            throw new DataStorageManagerException("Index " + indexName + " already closed");
        }
    }

    @Override
    public void truncate() {
        getTree().truncate();
        previousByNodeId.clear();
        currentManifest = null;
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
            LOGGER.log(Level.INFO, "start incremental index {0}", new Object[]{indexName});
        }
        final long pageSize = memoryManager.getMaxLogicalPageSize();

        if (LogSequenceNumber.START_OF_TIME.equals(sequenceNumber)) {
            tree = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                    memoryManager.getPKPageReplacementPolicy(), indexDataStorage);
            currentManifest = null;
            previousByNodeId.clear();
            if (!created) {
                LOGGER.log(Level.INFO, "loaded empty incremental index {0}", new Object[]{indexName});
            }
            return;
        }

        IndexStatus status = dataStorageManager.getIndexStatus(tableSpace, indexName, sequenceNumber);
        IncrementalBLinkManifest manifest;
        try {
            manifest = IncrementalBLinkManifest.deserialize(status.indexData);
        } catch (IncrementalBLinkManifest.LegacyFormatDetectedException err) {
            throw new DataStorageManagerException(
                    "Index " + indexName + " on-disk format is 'legacy' but JVM is configured"
                            + " with herddb.index.pk.mode=incremental. Start the server with"
                            + " -Dherddb.index.pk.mode=legacy, or rebuild the tablespace in the"
                            + " desired mode. Original error: " + err.getMessage(), err);
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }

        Map<Long, BLinkNodeMetadata<Bytes>> byNodeId = new HashMap<>();
        for (IncrementalBLinkManifest.SnapshotChunkRef ref : manifest.snapshotChunks) {
            IncrementalBLinkPageCodec.SnapshotChunkContents chunk =
                    dataStorageManager.readIndexPage(tableSpace, indexName, ref.pageId,
                            IncrementalBLinkPageCodec::readSnapshotChunk);
            for (BLinkNodeMetadata<Bytes> node : chunk.nodes) {
                byNodeId.put(node.id, node);
            }
        }
        for (IncrementalBLinkManifest.DeltaRef deltaRef : manifest.deltaChain) {
            IncrementalBLinkPageCodec.DeltaContents delta =
                    dataStorageManager.readIndexPage(tableSpace, indexName, deltaRef.pageId,
                            IncrementalBLinkPageCodec::readDelta);
            applyDelta(byNodeId, delta);
        }

        BLinkMetadata<Bytes> metadata = IncrementalBLinkPageCodec.rebuild(
                byNodeId,
                status.newPageId,
                manifest.anchorFast, manifest.anchorFastHeight,
                manifest.anchorTop, manifest.anchorTopHeight,
                manifest.anchorFirst, manifest.values);

        try {
            tree = new BLink<>(pageSize, BytesLongSizeEvaluator.INSTANCE,
                    memoryManager.getPKPageReplacementPolicy(), indexDataStorage, metadata);
        } catch (RuntimeException err) {
            throw new DataStorageManagerException(err);
        }

        newPageId.set(status.newPageId);
        currentManifest = manifest;
        previousByNodeId.clear();
        previousByNodeId.putAll(byNodeId);

        LOGGER.log(Level.INFO, "loaded incremental index {0}: {1} keys (epoch {2}, {3} snapshot chunks + {4} deltas)",
                new Object[]{indexName, tree.size(), manifest.epoch,
                        manifest.snapshotChunks.size(), manifest.deltaChain.size()});
    }

    private static void applyDelta(Map<Long, BLinkNodeMetadata<Bytes>> byNodeId,
                                   IncrementalBLinkPageCodec.DeltaContents delta) {
        for (BLinkNodeMetadata<Bytes> node : delta.upserted) {
            byNodeId.put(node.id, node);
        }
        for (long id : delta.removedNodeIds) {
            byNodeId.remove(id);
        }
    }

    // ---------------- checkpoint ----------------

    @Override
    public List<PostCheckpointAction> checkpoint(LogSequenceNumber sequenceNumber, boolean pin)
            throws DataStorageManagerException {
        try {
            final BLink<Bytes, Long> t = this.tree;
            if (t == null) {
                return Collections.emptyList();
            }

            BLinkMetadata<Bytes> metadata = t.checkpoint();

            Map<Long, BLinkNodeMetadata<Bytes>> newByNodeId = new LinkedHashMap<>(metadata.nodes.size());
            for (BLinkNodeMetadata<Bytes> node : metadata.nodes) {
                newByNodeId.put(node.id, node);
            }

            long nextEpoch = (currentManifest == null) ? 1L : currentManifest.epoch + 1;

            boolean rewriteSnapshot = currentManifest == null
                    || currentManifest.deltaChain.size() >= DEFAULT_SNAPSHOT_EVERY;

            List<IncrementalBLinkManifest.SnapshotChunkRef> newSnapshotChunks;
            List<IncrementalBLinkManifest.DeltaRef> newDeltaChain;
            long newSnapshotEpoch;
            List<BLinkNodeMetadata<Bytes>> upserted = new ArrayList<>();
            List<Long> removedIds = new ArrayList<>();
            List<Long> deadStoreIds = new ArrayList<>();

            if (rewriteSnapshot) {
                // A full snapshot supersedes the previous one and all
                // accumulated deltas. We must preserve-set only the new
                // snapshot's pages plus the node pages still reachable;
                // everything else (old snapshot chunks, old delta pages,
                // abandoned node storeIds) is reclaimable.
                newSnapshotChunks = writeSnapshotChunks(nextEpoch, metadata.nodes);
                newDeltaChain = Collections.emptyList();
                newSnapshotEpoch = nextEpoch;
            } else {
                // Diff against previousByNodeId and write a single delta page.
                diff(metadata.nodes, newByNodeId, upserted, removedIds, deadStoreIds);
                if (upserted.isEmpty() && removedIds.isEmpty() && deadStoreIds.isEmpty()
                        && anchorsUnchanged(metadata)) {
                    // Nothing changed; keep the previous manifest but refresh
                    // its LSN/newPageId. We still call indexCheckpoint to make
                    // a new metadata file that the DSM can pin at this LSN.
                    newSnapshotChunks = currentManifest.snapshotChunks;
                    newDeltaChain = currentManifest.deltaChain;
                    newSnapshotEpoch = currentManifest.snapshotEpoch;
                } else {
                    long deltaPageId = allocatePageId();
                    writeDeltaPage(deltaPageId, nextEpoch, upserted, removedIds, deadStoreIds);
                    newSnapshotChunks = currentManifest.snapshotChunks;
                    List<IncrementalBLinkManifest.DeltaRef> chain =
                            new ArrayList<>(currentManifest.deltaChain.size() + 1);
                    chain.addAll(currentManifest.deltaChain);
                    chain.add(new IncrementalBLinkManifest.DeltaRef(nextEpoch, deltaPageId));
                    newDeltaChain = chain;
                    newSnapshotEpoch = currentManifest.snapshotEpoch;
                }
            }

            IncrementalBLinkManifest nextManifest = new IncrementalBLinkManifest(
                    nextEpoch, metadata.values, metadata.nextID,
                    metadata.top, metadata.topheight,
                    metadata.fast, metadata.fastheight,
                    metadata.first,
                    newSnapshotEpoch, newSnapshotChunks, newDeltaChain);

            Set<Long> preserve = buildPreserveSet(metadata.nodes, newSnapshotChunks, newDeltaChain);

            byte[] manifestBytes = nextManifest.serialize();
            IndexStatus indexStatus = new IndexStatus(indexName, sequenceNumber,
                    newPageId.get(), preserve, manifestBytes);

            List<PostCheckpointAction> result = new ArrayList<>(
                    dataStorageManager.indexCheckpoint(tableSpace, indexName, indexStatus, pin));

            currentManifest = nextManifest;
            lastPreserveSet = preserve;
            previousByNodeId.clear();
            previousByNodeId.putAll(newByNodeId);

            LOGGER.log(Level.INFO,
                    "incremental index {0} checkpoint: epoch {1}, nodes {2}, snapshot chunks {3},"
                            + " deltas {4}, upserted {5}, removed {6}, dead pages {7}",
                    new Object[]{indexName, nextEpoch, metadata.nodes.size(),
                            newSnapshotChunks.size(), newDeltaChain.size(),
                            upserted.size(), removedIds.size(), deadStoreIds.size()});

            return result;
        } catch (IOException err) {
            throw new DataStorageManagerException(err);
        }
    }

    private boolean anchorsUnchanged(BLinkMetadata<Bytes> metadata) {
        IncrementalBLinkManifest m = currentManifest;
        return m != null
                && m.anchorTop == metadata.top
                && m.anchorTopHeight == metadata.topheight
                && m.anchorFast == metadata.fast
                && m.anchorFastHeight == metadata.fastheight
                && m.anchorFirst == metadata.first
                && m.values == metadata.values
                && m.nextID == metadata.nextID;
    }

    private void diff(
            List<BLinkNodeMetadata<Bytes>> currentNodes,
            Map<Long, BLinkNodeMetadata<Bytes>> currentByNodeId,
            List<BLinkNodeMetadata<Bytes>> upserted,
            List<Long> removedIds,
            List<Long> deadStoreIds
    ) {
        for (BLinkNodeMetadata<Bytes> current : currentNodes) {
            BLinkNodeMetadata<Bytes> prev = previousByNodeId.get(current.id);
            if (prev == null) {
                upserted.add(current);
            } else if (!sameMetadata(prev, current)) {
                upserted.add(current);
                if (prev.storeId != current.storeId
                        && prev.storeId != BLinkIndexDataStorage.NEW_PAGE) {
                    deadStoreIds.add(prev.storeId);
                }
            }
        }
        for (Map.Entry<Long, BLinkNodeMetadata<Bytes>> e : previousByNodeId.entrySet()) {
            if (!currentByNodeId.containsKey(e.getKey())) {
                removedIds.add(e.getKey());
                long storeId = e.getValue().storeId;
                if (storeId != BLinkIndexDataStorage.NEW_PAGE) {
                    deadStoreIds.add(storeId);
                }
            }
        }
    }

    private static boolean sameMetadata(BLinkNodeMetadata<Bytes> a, BLinkNodeMetadata<Bytes> b) {
        if (a.leaf != b.leaf) {
            return false;
        }
        if (a.storeId != b.storeId) {
            return false;
        }
        if (a.keys != b.keys) {
            return false;
        }
        if (a.bytes != b.bytes) {
            return false;
        }
        if (a.outlink != b.outlink) {
            return false;
        }
        if (a.rightlink != b.rightlink) {
            return false;
        }
        return java.util.Objects.equals(a.rightsep, b.rightsep);
    }

    private List<IncrementalBLinkManifest.SnapshotChunkRef> writeSnapshotChunks(
            long epoch, List<BLinkNodeMetadata<Bytes>> nodes
    ) throws DataStorageManagerException {
        if (nodes.isEmpty()) {
            return Collections.emptyList();
        }
        int chunkSize = Math.max(1, DEFAULT_SNAPSHOT_CHUNK_SIZE);
        int totalChunks = (nodes.size() + chunkSize - 1) / chunkSize;
        List<IncrementalBLinkManifest.SnapshotChunkRef> refs = new ArrayList<>(totalChunks);
        for (int i = 0; i < totalChunks; i++) {
            int from = i * chunkSize;
            int to = Math.min(from + chunkSize, nodes.size());
            List<BLinkNodeMetadata<Bytes>> slice = nodes.subList(from, to);
            long pageId = allocatePageId();
            final int chunkIndex = i;
            dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, out -> {
                IncrementalBLinkPageCodec.writeSnapshotChunk(out, epoch, chunkIndex, totalChunks, slice);
            });
            refs.add(new IncrementalBLinkManifest.SnapshotChunkRef(pageId, i, totalChunks));
        }
        return refs;
    }

    private void writeDeltaPage(long pageId, long epoch,
                                List<BLinkNodeMetadata<Bytes>> upserted,
                                List<Long> removedIds, List<Long> deadStoreIds)
            throws DataStorageManagerException {
        long[] removedArr = toArray(removedIds);
        long[] deadArr = toArray(deadStoreIds);
        dataStorageManager.writeIndexPage(tableSpace, indexName, pageId, out -> {
            IncrementalBLinkPageCodec.writeDelta(out, epoch, upserted, removedArr, deadArr);
        });
    }

    private static long[] toArray(List<Long> list) {
        long[] out = new long[list.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = list.get(i);
        }
        return out;
    }

    private Set<Long> buildPreserveSet(
            List<BLinkNodeMetadata<Bytes>> liveNodes,
            List<IncrementalBLinkManifest.SnapshotChunkRef> snapshotChunks,
            List<IncrementalBLinkManifest.DeltaRef> deltaChain
    ) {
        Set<Long> preserve = new HashSet<>();
        for (BLinkNodeMetadata<Bytes> node : liveNodes) {
            if (node.storeId != BLinkIndexDataStorage.NEW_PAGE) {
                preserve.add(node.storeId);
            }
        }
        for (IncrementalBLinkManifest.SnapshotChunkRef ref : snapshotChunks) {
            preserve.add(ref.pageId);
        }
        for (IncrementalBLinkManifest.DeltaRef ref : deltaChain) {
            preserve.add(ref.pageId);
        }
        return preserve;
    }

    private long allocatePageId() {
        return newPageId.getAndIncrement();
    }

    @Override
    public void unpinCheckpoint(LogSequenceNumber sequenceNumber) throws DataStorageManagerException {
        dataStorageManager.unPinIndexCheckpoint(tableSpace, indexName, sequenceNumber);
    }

    private BLink<Bytes, Long> getTree() {
        final BLink<Bytes, Long> t = this.tree;
        if (t == null) {
            if (closed.get()) {
                throw new DataStorageManagerException("Index " + indexName + " already closed");
            } else {
                throw new DataStorageManagerException("Index " + indexName + " still not started");
            }
        }
        return t;
    }

    // ---------------- node-page storage (same byte layout as legacy) ----------------

    private static final byte INNER_NODE_PAGE = BLinkKeyToPageIndex.INNER_NODE_PAGE;
    private static final byte LEAF_NODE_PAGE = BLinkKeyToPageIndex.LEAF_NODE_PAGE;
    private static final byte NODE_PAGE_END_BLOCK = 0;
    private static final byte NODE_PAGE_KEY_VALUE_BLOCK = 1;
    private static final byte NODE_PAGE_INF_BLOCK = 2;

    private final class NodePageStorageImpl implements BLinkIndexDataStorage<Bytes, Long> {

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
                long flags = in.readVLong();
                if (version != 1 || flags != 0) {
                    throw new IOException("Corrupted index page " + pageId);
                }
                byte rtype = in.readByte();
                if (rtype != type) {
                    throw new IOException("Wrong page type " + rtype + " expected " + type);
                }
                byte block;
                while ((block = in.readByte()) != NODE_PAGE_END_BLOCK) {
                    switch (block) {
                        case NODE_PAGE_KEY_VALUE_BLOCK:
                            map.put(in.readBytes(), in.readVLong());
                            break;
                        case NODE_PAGE_INF_BLOCK:
                            map.put(Bytes.POSITIVE_INFINITY, in.readVLong());
                            break;
                        default:
                            throw new IOException("Wrong node block type " + block);
                    }
                }
                return map;
            });
        }

        @Override
        public long createNodePage(Map<Bytes, Long> data) throws IOException {
            return createPage(NEW_PAGE, data, INNER_NODE_PAGE);
        }

        @Override
        public long createLeafPage(Map<Bytes, Long> data) throws IOException {
            return createPage(NEW_PAGE, data, LEAF_NODE_PAGE);
        }

        @Override
        public void overwriteNodePage(long pageId, Map<Bytes, Long> data) throws IOException {
            createPage(pageId, data, INNER_NODE_PAGE);
        }

        @Override
        public void overwriteLeafPage(long pageId, Map<Bytes, Long> data) throws IOException {
            createPage(pageId, data, LEAF_NODE_PAGE);
        }

        private long createPage(long pageId, Map<Bytes, Long> data, byte type) throws IOException {
            long pid = (pageId == NEW_PAGE) ? allocatePageId() : pageId;
            dataStorageManager.writeIndexPage(tableSpace, indexName, pid, out -> {
                out.writeVLong(1);
                out.writeVLong(0);
                out.writeByte(type);
                data.forEach((x, y) -> {
                    try {
                        if (x == Bytes.POSITIVE_INFINITY) {
                            out.writeByte(NODE_PAGE_INF_BLOCK);
                            out.writeVLong(y);
                        } else {
                            out.writeByte(NODE_PAGE_KEY_VALUE_BLOCK);
                            out.writeArray(x.to_array());
                            out.writeVLong(y);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(
                                "Unexpected IOException during node page write preparation", e);
                    }
                });
                out.writeByte(NODE_PAGE_END_BLOCK);
            });
            return pid;
        }
    }

    // ---------------- test accessors ----------------

    /**
     * Returns a defensive copy of the most recent preserve set sent to
     * {@code DataStorageManager.indexCheckpoint}. Exposed for tests.
     */
    Set<Long> getLastPreserveSet() {
        return new HashSet<>(lastPreserveSet);
    }

    /**
     * Returns the most recently committed manifest, or {@code null} if
     * {@link #checkpoint} has never succeeded.
     */
    IncrementalBLinkManifest getCurrentManifest() {
        return currentManifest;
    }
}
