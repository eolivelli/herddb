# HerdDB BLink Primary-Key Index

HerdDB stores every table's primary-key → page-id map in a B-link tree (see
Lanin & Shasha, "A Symmetric Concurrent B-Tree Algorithm"). This document
describes the two on-disk formats currently supported, how each checkpoints
itself, and how recovery works.

Checkpointing at large is described in [CHECKPOINT.md](./CHECKPOINT.md); this
file zooms into the BLink-specific parts referenced from §5 there.

---

## 1. Two implementations, one interface

Two `KeyToPageIndex` implementations ship with HerdDB:

| Class | On-disk format | Selected when |
|-------|----------------|---------------|
| `herddb.index.blink.BLinkKeyToPageIndex` | legacy (single metadata blob) | `-Dherddb.index.pk.mode=legacy` |
| `herddb.index.blink.IncrementalBLinkKeyToPageIndex` | incremental (snapshot + delta chain) | `-Dherddb.index.pk.mode=incremental` — **default** |

The factory seam is `herddb.index.KeyToPageIndexFactory.create(...)`, called
from every persistent `DataStorageManager.createKeyToPageMap` implementation
(File, BookKeeper, RemoteFile, ReadReplica). `MemoryDataStorageManager` is
not affected — it returns `ConcurrentMapKeyToPageIndex` unconditionally
because it is used only in tests that don't exercise index persistence.

Both implementations share:

- The same BLink tree algorithm (Lanin–Shasha) and the same in-memory node
  objects (`herddb.index.blink.BLink`).
- The same **node-page byte layout** — the bytes stored by
  `writeIndexPage(pageId, …)` for an inner or a leaf node are
  byte-identical between the two implementations. A node page written by
  the legacy implementation can be read by the incremental one and vice
  versa.

They differ only in the **checkpoint metadata** — what gets serialised into
`IndexStatus.indexData` and which sidecar pages the index keeps.

---

## 2. Node-page layout (shared)

Every inner or leaf node is persisted as a single index page via
`DataStorageManager.writeIndexPage(tableSpace, indexName, pageId, writer)`:

```
vlong  version            = 1
vlong  flags              = 0
byte   kind               = 1 (INNER) | 2 (LEAF)

repeat until END:
  byte  block
  case KEY_VALUE_BLOCK (1):
    array key              // variable-length byte array (length-prefixed)
    vlong value            // leaf: data-page-id ; inner: child-node-id
  case INF_BLOCK (2):
    vlong value            // value bound to the +∞ separator (rightmost)
  case END_BLOCK (0):
    break
```

`vlong` is the ZigZag-free variable-length long encoding used throughout
HerdDB; `array` is the length-prefixed byte array (see
`herddb.utils.ExtendedDataOutputStream.writeArray`).

Neither implementation currently embeds per-node metadata (keys count,
outlink, rightlink, rightsep, leaf flag) inside the page. That metadata
lives in the checkpoint sidecar described below.

---

## 3. Legacy format (`BLinkKeyToPageIndex`)

### 3.1 Checkpoint metadata

On every checkpoint the legacy implementation:

1. Calls `BLink.checkpoint()` which returns a `BLinkMetadata<K>` with one
   `BLinkNodeMetadata` entry **per node in the tree**:
   `(leaf, id, storeId, keys, bytes, outlink, rightlink, rightsep)`.
2. Serialises the full `BLinkMetadata` into a single byte array using
   `BLinkKeyToPageIndex.MetadataSerializer`.
3. Collects every live `storeId` into a `Set<Long> activePages`.
4. Builds an `IndexStatus(indexName, lsn, nextPageId, activePages,
   serializedMetadata)` and calls
   `DataStorageManager.indexCheckpoint(tableSpace, indexName, status, pin)`.

The serialized blob starts with
`writeVLong(CURRENT_VERSION=2); writeVLong(NO_FLAGS=0); writeByte(METADATA_PAGE=0);`
followed by anchor pointers and the full node list.

### 3.2 Cost model

| Quantity | Size at checkpoint | Written to |
|----------|--------------------|------------|
| `indexData` (serialized metadata) | O(N) — ~80 bytes × every node in tree | `IndexStatus.indexData` (metadata file) |
| `activePages` set | O(N) — ~8 bytes × every live page | `IndexStatus.activePages` (metadata file) |
| Node pages | O(changes) | `writeIndexPage` (one file per page) |

The **node pages** are already written incrementally — BLink only writes a
node when it is dirty and otherwise reuses its existing `storeId`. The
problem is the **metadata blob** and the **activePages set**, which are
both fully rewritten on every checkpoint regardless of how little the tree
changed. At 20 million nodes the metadata blob is ≈ 2 GB and the page-id
set is ≈ 160 MB — rewritten at every checkpoint, this is why the legacy
format does not scale.

### 3.3 Recovery

`getIndexStatus(lsn)` returns the full metadata blob; the legacy
`BLinkKeyToPageIndex.start` deserialises it and passes the resulting
`BLinkMetadata` to `new BLink<>(…, metadata)`, which rebuilds the in-memory
node map (`ConcurrentMap<Long, Node>`) by creating one `Node` per entry in
the metadata.

Node-page data is loaded lazily on demand through the standard
`BLinkIndexDataStorage` interface.

---

## 4. Incremental format (`IncrementalBLinkKeyToPageIndex`)

### 4.1 Goal

Make **per-checkpoint disk writes proportional to the number of node
entries that changed since the previous checkpoint**, not to the total
number of nodes in the tree.

### 4.2 Pieces on disk

The incremental format writes three kinds of content:

| Kind | Written by | Where it lives |
|------|------------|----------------|
| Node pages (inner / leaf) | `BLinkIndexDataStorage` (same as legacy) | regular index pages (one per node, `writeIndexPage`) |
| **Snapshot-chunk pages** (full node-metadata list) | `IncrementalBLinkPageCodec.writeSnapshotChunk` | regular index pages (chunked, one per chunk) |
| **Delta pages** (per-checkpoint diffs) | `IncrementalBLinkPageCodec.writeDelta` | regular index pages (one per checkpoint) |
| **Manifest** | `IncrementalBLinkManifest.serialize` | `IndexStatus.indexData` (metadata file) |

Sidecar-page layout shares the same three-field header as the node pages
(`vlong version; vlong flags; byte kind`) so the two formats coexist
in the same index directory without confusion. The incremental kinds are
`10 = SNAPSHOT_CHUNK` and `11 = DELTA`.

### 4.3 Manifest (small, O(1) per checkpoint)

Serialised into `IndexStatus.indexData`. Version marker **100** — well
above the legacy range of 0–2, so the legacy reader throws
`"Unknown BLink node metadata version 100"` and the incremental reader
symmetrically refuses any version `< 100` with a
`LegacyFormatDetectedException`. This is the format-mismatch safety net.

```
vlong  version             = 100
vlong  flags               = 0
vlong  epoch               // monotonically increasing
vlong  values              // tree key count
vlong  nextID              // BLink.nextID.get()
zlong  anchorTop           // anchor pointers (copied verbatim from BLink)
vint   anchorTopHeight
zlong  anchorFast
vint   anchorFastHeight
zlong  anchorFirst
vlong  snapshotEpoch       // epoch at which the current snapshot was taken
vint   numSnapshotChunks
repeat numSnapshotChunks × { vlong pageId ; vint chunkIndex ; vint totalChunks }
vint   numDeltas
repeat numDeltas × { vlong epoch ; vlong pageId }
```

Total size: a few hundred bytes plus ~16 bytes per sidecar-page reference.
Always O(1) in tree size.

### 4.4 Snapshot-chunk pages

```
vlong  version = 1  ;  vlong flags = 0  ;  byte kind = 10 (SNAPSHOT_CHUNK)
vlong  epoch
vint   chunkIndex        // 0-based
vint   totalChunks       // total chunks for this snapshot
vint   nodeCount
repeat nodeCount × node-metadata
```

where each node-metadata record is:

```
boolean  leaf
vlong    id               // BLink logical node id
vlong    storeId          // BLink.storeId (data page id)
vint     keys
vlong    bytes
zlong    outlink
zlong    rightlink
byte     rightSepKind     // 0 = +∞ , 1 = explicit key bytes
[array]  rightSep         // only present when rightSepKind == 1
```

One snapshot is split across multiple chunk pages of at most
`herddb.index.pk.incremental.snapshotChunkSize` (default 50 000) node
entries. At recovery, all chunks for the manifest's `snapshotEpoch` are
read and merged into a `Map<nodeId, BLinkNodeMetadata>` baseline.

### 4.5 Delta pages

```
vlong  version = 1  ;  vlong flags = 0  ;  byte kind = 11 (DELTA)
vlong  epoch
vint   upsertedCount
repeat upsertedCount × node-metadata             // full metadata, replaces prior entry with same id
vint   removedCount
repeat removedCount × vlong nodeId               // logical id (not storeId)
vint   deadStoreIdCount
repeat deadStoreIdCount × vlong storeId          // former storeIds now eligible for GC
```

At recovery, deltas from the manifest's `deltaChain` are applied in order
on top of the snapshot baseline: each upserted entry replaces or inserts
its `nodeId` in the map; each removed `nodeId` is deleted. The resulting
map is converted to a list and passed to `new BLink<>(…, metadata)`
exactly like the legacy recovery path — the in-memory BLink is unchanged.

### 4.6 Checkpoint algorithm

```
epoch = (currentManifest == null) ? 1 : currentManifest.epoch + 1
metadata = BLink.checkpoint()                         // returns full node list
newByNodeId = metadata.nodes indexed by node id

rewriteSnapshot = currentManifest == null
               || currentManifest.deltaChain.size() >= SNAPSHOT_EVERY    // default 64

if rewriteSnapshot:
    snapshotChunks = writeSnapshotChunks(epoch, metadata.nodes)
    deltaChain     = []
    snapshotEpoch  = epoch
else:
    diff newByNodeId against previousByNodeId:
        upserted      = nodes added or whose metadata changed
        removedIds    = nodeIds in previous but not in new
        deadStoreIds  = old storeIds of changed or removed nodes
    if everything empty AND anchors unchanged:
        # no-op checkpoint — reuse manifest, bump LSN only
        (snapshotChunks, deltaChain, snapshotEpoch) = (prev.snapshotChunks,
                                                       prev.deltaChain,
                                                       prev.snapshotEpoch)
    else:
        deltaPageId = newPageId++
        writeDelta(deltaPageId, epoch, upserted, removedIds, deadStoreIds)
        snapshotChunks = prev.snapshotChunks
        deltaChain     = prev.deltaChain ++ (epoch, deltaPageId)
        snapshotEpoch  = prev.snapshotEpoch

manifest = IncrementalBLinkManifest(epoch, …, snapshotChunks, deltaChain)
preserve = { node storeIds } ∪ { snapshotChunks.pageIds } ∪ { deltaChain.pageIds }
IndexStatus status = new IndexStatus(indexName, lsn, nextID, preserve,
                                     manifest.serialize())
postActions = dataStorageManager.indexCheckpoint(tableSpace, indexName,
                                                  status, pin)
currentManifest     = manifest
previousByNodeId   := newByNodeId
return postActions
```

Notes:

- **Preserve set** — the `activePages` field of `IndexStatus` is the
  **small** preserve set of "pages we must not delete this round":
  the live node pages, plus our own manifest sidecars. This is
  deliberately not the full activePages set of the tree; the existing
  `FileDataStorageManager.indexCheckpoint` GC logic ([CHECKPOINT.md §10](./CHECKPOINT.md#10-datastorage-backend-differences))
  already deletes any page file not in `activePages` and not pinned,
  so feeding it only the preserve set is what makes GC of superseded
  snapshots and consumed deltas happen automatically.
- **Snapshot-refresh threshold** — tuned via
  `-Dherddb.index.pk.incremental.snapshotEvery` (default 64). After N
  deltas the next checkpoint rewrites a fresh snapshot, bounding the
  recovery cost at snapshot-size + N-deltas.
- **No DSM API change** — the incremental format rides on the same
  `readIndexPage` / `writeIndexPage` / `deleteIndexPage` / `indexCheckpoint`
  / `getIndexStatus` surface as the legacy format. All five
  `DataStorageManager` implementations (File, BookKeeper, RemoteFile,
  ReadReplica, Memory) support it without modification.

### 4.7 Cost model

| Quantity | Size at a non-snapshot checkpoint | Size at a snapshot-refresh checkpoint |
|----------|-----------------------------------|----------------------------------------|
| manifest (`indexData`) | ~O(1) + ~16 B × deltaChain length | ~O(1) + ~16 B × chunk count |
| preserve set (`activePages`) | ~O(changes) + snapshot & delta page-ids | same |
| delta page | O(changes) × node-metadata record size | **not written** |
| snapshot-chunk pages | **not written** | O(N) × node-metadata record size |
| node pages | O(dirty nodes) (same as legacy) | same |

The normal case is a non-snapshot checkpoint: its dominant cost is the
delta page, whose size grows with the number of nodes that actually
changed since the previous checkpoint — not with the total tree size.

### 4.8 Format-mismatch detection

If the JVM is configured with `-Dherddb.index.pk.mode=incremental` (the
default) but the existing on-disk metadata for an index was written by the
legacy implementation, `IncrementalBLinkKeyToPageIndex.start` detects this
inside `IncrementalBLinkManifest.deserialize` (version marker `< 100`) and
throws a `DataStorageManagerException` with a message pointing the
operator to either `-Dherddb.index.pk.mode=legacy` or a fresh tablespace.

Symmetrically, the legacy reader throws a
`"Unknown BLink node metadata version 100"` if fed an incremental
manifest. No silent misreads are possible.

Migration between the two formats is not implemented in this first
iteration — set the mode once per tablespace and keep it.

### 4.9 Garbage collection

Dead store-page IDs are recorded explicitly in each delta's
`deadStoreIds` list so that operators can reason about what was
abandoned at what epoch. Actual file-level deletion happens at
`dataStorageManager.indexCheckpoint` time through the standard
`PostCheckpointAction` mechanism: any index page file whose id is
`< maxPageId`, not pinned, and not in the preserve set is scheduled for
deletion by the `FileDataStorageManager` / `RemoteFileDataStorageManager`
cleanup code (see [CHECKPOINT.md §10](./CHECKPOINT.md#10-datastorage-backend-differences)).

On a snapshot refresh the whole previous snapshot and delta chain become
"not referenced" — the preserve set no longer contains them, so the DSM
reclaims them in the same pass.

---

## 5. In-memory representation (common to both formats)

Both implementations share the same in-memory `BLink` object:

- `ConcurrentMap<Long, Node<K,V>> nodes` — one `Node` instance per live
  tree node. **This is still O(N) in tree size** and is not addressed by
  the incremental format: at 20 million nodes the heap still holds 20
  million `Node` objects. The cost that the incremental format removes is
  the **persisted** O(N), not the in-memory O(N).
- `PageReplacementPolicy` — evicts **node page data** (keys + values) from
  memory when the global PK memory budget fills up; the `Node` object
  itself stays pinned.
- `anchor` — the top / fast / first pointers for concurrent descents.

Moving to true lazy-`Node` allocation (so the heap footprint is
proportional to the working set, not to tree size) is a follow-up that
requires changes to `BLink` itself and is deliberately out of scope for
the first incremental checkpoint release.

---

## 6. Configuration summary

| Property | Default | Effect |
|----------|---------|--------|
| `herddb.index.pk.mode` | `incremental` | `incremental` / `legacy` — which implementation runs |
| `herddb.index.pk.incremental.snapshotEvery` | 64 | Rewrite a fresh snapshot after this many deltas |
| `herddb.index.pk.incremental.snapshotChunkSize` | 50000 | Max node-metadata entries per snapshot-chunk page |

---

## 7. Cross-references

- [CHECKPOINT.md](./CHECKPOINT.md) — overall checkpoint dynamics
- [CHECKPOINT.md §5 BLink Primary-Key Index](./CHECKPOINT.md#blink-primary-key-index) — where the PK index fits into the checkpoint pipeline
- `herddb.index.KeyToPageIndexMode` — property-driven mode selector
- `herddb.index.KeyToPageIndexFactory` — single factory wired into every persistent DSM
- `herddb.index.blink.IncrementalBLinkManifest` — manifest class + serializer
- `herddb.index.blink.IncrementalBLinkPageCodec` — snapshot / delta page codec
- `herddb.index.blink.IncrementalBLinkKeyToPageIndex` — the implementation
