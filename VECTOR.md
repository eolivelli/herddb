# Vector Index in HerdDB

This document describes the vector index feature: its architecture, how to use it, storage format, and implementation details.

---

## Architecture Overview

HerdDB's vector indexing uses a **two-component architecture** where vector index operations are offloaded from the main database server to a standalone **IndexingService**:

```
┌─────────────────────────────────────────────────┐
│  HerdDB Server (DBManager)                      │
│                                                 │
│  VectorIndexManager (thin remote client)        │
│    - DML ops are no-ops (data flows via WAL)    │
│    - Search delegates to IndexingService gRPC   │
│    - Checkpoint waits for IndexingService        │
│      catch-up before WAL truncation             │
│                                                 │
│  CommitLog (WAL)  ──writes──►  .txlog files     │
└─────────────────────────────────────────────────┘
                                    │
                               tails WAL
                                    │
                                    ▼
┌─────────────────────────────────────────────────┐
│  IndexingService (standalone gRPC server)       │
│                                                 │
│  IndexingServiceEngine                          │
│    - CommitLogTailer: reads .txlog files        │
│    - SchemaTracker: tracks DDL from WAL         │
│    - TransactionBuffer: buffers until COMMIT    │
│    - VectorStoreFactory: creates vector stores  │
│    - Apply workers: stripe DML by PK hash       │
│                                                 │
│  Per-index vector stores:                       │
│    ┌───────────────────────────────────────┐    │
│    │ InMemoryVectorStore (brute-force)     │    │
│    │  OR                                   │    │
│    │ PersistentVectorStore (jvector HNSW)  │    │
│    │   - Live graph shards (in-memory)     │    │
│    │   - Frozen shards (during checkpoint) │    │
│    │   - On-disk segments (FusedPQ)        │    │
│    │   - VectorStorage: lock-free array    │    │
│    │   - BLink for PK-to-ordinal mapping   │    │
│    │   - DataStorageManager for persistence│    │
│    │   - MemoryManager for bounded memory  │    │
│    │   - Background compaction thread      │    │
│    └───────────────────────────────────────┘    │
│                                                 │
│  MemoryManager: bounded memory for BLink pages  │
│  DataStorageManager: persistence backend        │
│    - FileDataStorageManager (local disk)        │
│    - MemoryDataStorageManager (testing)         │
└─────────────────────────────────────────────────┘
```

### Key design decisions

1. **Decoupled via CommitLog tailing.** The IndexingService replays the database WAL independently. The main server's `VectorIndexManager` is a thin client — inserts/updates/deletes are no-ops because the IndexingService consumes them asynchronously from the WAL.

2. **DataStorageManager for persistence.** Vector graph chunks and PK-mapping data are stored as pages via `DataStorageManager.writeIndexPage/readIndexPage`. This reuses HerdDB's existing storage infrastructure.

3. **BLink for PK mapping.** On-disk segments use `BLink<Bytes, Long>` for PK-to-ordinal lookups, backed by DataStorageManager pages and evicted via `MemoryManager`'s page replacement policy. This bounds memory usage for large on-disk indexes.

4. **Two store implementations.** `AbstractVectorStore` is the common base class. `InMemoryVectorStore` provides brute-force cosine similarity for small datasets or testing. `PersistentVectorStore` uses jvector for production workloads with on-disk persistence.

5. **DML parallelism via striped workers.** The `IndexingServiceEngine` routes DML from committed transactions to a pool of single-threaded apply workers, striped by PK hash, ensuring per-key ordering while exploiting multi-core throughput.

---

## Modules

| Module | Contains |
|--------|----------|
| `herddb-core` | `AbstractVectorStore`, `PersistentVectorStore`, `VectorIndexManager` (thin remote client), `VectorStorage`, `VectorSegment`, helper classes, SQL planner integration, `DataStorageManager`, `MemoryManager`, `BLink` |
| `herddb-indexing-service` | `IndexingServer` (gRPC), `IndexingServiceEngine`, `IndexingServerConfiguration`, `InMemoryVectorStore`, `VectorStoreFactory`, `CommitLogTailer`, `SchemaTracker`, `TransactionBuffer`, `WatermarkStore`, Prometheus metrics |
| `herddb-services` | `IndexingServiceMain` — standalone server entry point |

---

## Quick Start Guide

### 1. Create a table with a vector column

```sql
CREATE TABLE tblspace1.documents (
    id       INTEGER NOT NULL,
    title    VARCHAR(200),
    vec      floata  NOT NULL,
    PRIMARY KEY (id)
);
```

The column type `floata` (`FLOAT ARRAY`) stores a fixed-dimension array of 32-bit floats.

### 2. Create a vector index

```sql
-- Default settings (cosine similarity, FusedPQ enabled)
CREATE VECTOR INDEX vidx ON tblspace1.documents(vec);

-- Custom hyperparameters via WITH clause
CREATE VECTOR INDEX vidx ON tblspace1.documents(vec)
  WITH m=32 beamWidth=200 similarity=cosine fusedPQ=true;
```

### 3. Insert data

```sql
INSERT INTO tblspace1.documents(id, title, vec) VALUES(1, 'doc one', CAST(? AS FLOAT ARRAY));
-- pass float[] as the JDBC parameter
```

### 4. Query — approximate nearest-neighbour search

```sql
SELECT id, title
FROM tblspace1.documents
ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC
LIMIT 10;
```

When a vector index exists on `vec`, the `ORDER BY ann_of(…) DESC LIMIT k` pattern is automatically routed through the index via `VectorANNScanOp`. Without a vector index, the query falls back to brute-force cosine similarity over a full table scan.

---

## Index Creation Parameters

All parameters are optional. Unspecified parameters use the defaults shown below.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `m` | integer | `16` | Maximum edges per graph node. Higher = better recall, more memory. Range: 8–32. |
| `beamWidth` | integer | `100` | Candidates explored during insertion. Higher = better graph quality, slower inserts. Range: 50–400. |
| `neighborOverflow` | float | `1.2` | Temporary degree overflow factor during construction. Must be ≥ 1.0. |
| `alpha` | float | `1.4` | Diversity criterion. Values > 1.0 allow longer edges (better recall on clustered data). |
| `similarity` | string | `cosine` | Distance metric: `cosine`, `euclidean`, `dot`. |
| `fusedPQ` | boolean | `true` | Use FusedPQ on-disk format when dim ≥ 8 and vectors ≥ 256. |
| `maxSegmentSize` | long | `2147483648` | Maximum on-disk segment size in bytes before segment rotation. |
| `maxLiveGraphSize` | integer | `0` | Maximum vectors per live graph shard before rotation. 0 = auto (see Shard Rotation). |

---

## IndexingServerConfiguration

The `IndexingServerConfiguration` class provides typed access to all IndexingService settings. It follows the `ServerConfiguration` pattern: `Properties`-backed, typed getters, fluent `set()`, `copy()`.

| Property | Key | Default | Description |
|----------|-----|---------|-------------|
| gRPC host | `indexing.grpc.host` | `0.0.0.0` | gRPC server bind address |
| gRPC port | `indexing.grpc.port` | `9850` | gRPC server port |
| HTTP enable | `indexing.http.enable` | `false` | Enable Prometheus metrics HTTP endpoint |
| HTTP host | `indexing.http.host` | `0.0.0.0` | Metrics endpoint bind address |
| HTTP port | `indexing.http.port` | `9851` | Metrics endpoint port |
| Log dir | `indexing.log.dir` | `txlog` | WAL directory to tail |
| Data dir | `indexing.data.dir` | `data` | Data directory for persistence |
| Max vector memory | `indexing.memory.vector.limit` | `0` | Max memory for vector data. 0 = unbounded. |
| Page size | `indexing.memory.page.size` | `1048576` | Logical page size for MemoryManager (1 MB). |
| Vector M | `indexing.vector.m` | `16` | Default M for new vector stores |
| Vector beam width | `indexing.vector.beamWidth` | `100` | Default beam width |
| Vector neighbor overflow | `indexing.vector.neighborOverflow` | `1.2` | Default neighbor overflow |
| Vector alpha | `indexing.vector.alpha` | `1.4` | Default alpha |
| Vector fusedPQ | `indexing.vector.fusedPQ` | `true` | Default FusedPQ enable |
| Max segment size | `indexing.vector.maxSegmentSize` | `2147483648` | Default max segment size |
| Max live graph size | `indexing.vector.maxLiveGraphSize` | `0` | Default max live graph size (0 = auto) |
| Compaction interval | `indexing.compaction.interval` | `60000` | Checkpoint interval in ms |
| Compaction threads | `indexing.compaction.threads` | `2` | Background compaction threads |
| Storage type | `indexing.storage.type` | `file` | `file` (persistent) or `memory` (testing) |
| Memory multiplier | `indexing.vector.memoryMultiplier` | `5.0` | Multiplier for memory estimation |
| Apply parallelism | `indexing.apply.parallelism` | `auto` | Number of DML apply worker threads (default: max(1, availableProcessors/2)) |
| Apply queue capacity | `indexing.apply.queue.capacity` | `1000` | Per-worker bounded queue depth |

### JVM system properties

Some lower-level thresholds are controlled by JVM system properties (not the configuration file):

| Property | Default | Description |
|----------|---------|-------------|
| `herddb.vector.memoryMultiplier` | `5.0` | Memory overhead multiplier |
| `herddb.vectorindex.maxLiveBytesDuringCheckpoint` | `4294967296` (4 GB) | Hard cap on live vector memory during Phase B |
| `herddb.vectorindex.file.usemmap` | `false` | Use `MmapFileBackedVectorValues` instead of channel-based I/O during checkpoint |
| `herddb.vectorindex.dense.arraythreshold` | `10000000` | Switch from `ArrayOffsetIndex` to `BrinOffsetIndex` above this many nodeIds |

---

## CommitLog Tailing & DML Parallelism

The `IndexingServiceEngine` is the core consumer of the database WAL. Its entry processing pipeline is deliberately designed for throughput and ordering correctness.

### Tailer thread

A single dedicated thread (`indexing-service-tailer`) drives `CommitLogTailer` forward. For each log entry it receives a `(LogSequenceNumber, LogEntry)` pair and routes it through `processEntry()`:

- `BEGINTRANSACTION` → `transactionBuffer.beginTransaction(txId)`
- `COMMITTRANSACTION` → collect all buffered entries for `txId` → submit each to an apply worker → `transactionBuffer.rollbackTransaction(txId)` (releases buffer)
- `ROLLBACKTRANSACTION` → `transactionBuffer.rollbackTransaction(txId)` (discard all buffered entries)
- Any other entry with `txId != 0` → `transactionBuffer.bufferEntry(txId, entry)` (defer until COMMIT)
- Any other entry with `txId == 0` → apply immediately in the tailer thread

### Striped apply workers

After COMMIT, each DML entry is routed to one of `N` single-threaded apply workers:

```
stripe = Math.floorMod(entry.key.hashCode(), applyParallelism)
applyWorkers[stripe].submit(applyTask)
```

This gives:
- **Per-key ordering**: all mutations to a given PK always go to the same worker stripe, so insert/update/delete ordering is preserved.
- **Parallel throughput**: independent keys across different stripes are applied concurrently.
- Default parallelism `N = max(1, availableProcessors / 2)`.
- Each worker has a bounded `LinkedBlockingQueue(capacity=1000)` with `CallerRunsPolicy` — when the queue is full, the tailer thread itself executes the task, providing natural backpressure from the WAL consumer to the apply layer.

### DDL synchronization

DDL entries (CREATE/DROP TABLE/INDEX) must be applied atomically with respect to in-flight DML. Before applying any DDL, the engine calls `awaitPendingWork()`, which submits a barrier task to every apply worker via a `CountDownLatch` and waits for all workers to drain their queues. Only then is the DDL applied.

### Watermark persistence

Every `WATERMARK_SAVE_INTERVAL_ENTRIES = 1000` processed entries, the engine calls `awaitPendingWork()` and then persists the current LSN to `WatermarkStore` (atomic write via temp file + rename). On restart, tailing resumes from the saved LSN, preventing duplicate application of already-processed entries.

---

## PersistentVectorStore — In-Memory State

### VectorStorage

`VectorStorage` is a lock-free, resizable array of `VectorFloat<?>` values indexed by integer nodeId:

- Backed by `volatile AtomicReferenceArray<VectorFloat<?>>`.
- `get(nodeId)` — single volatile read, no locking, no boxing.
- `set(nodeId, vec)` — synchronized only when the array must be doubled (capacity growth).
- `remove(nodeId)` — synchronized to avoid races with concurrent `set()`.
- After a checkpoint's Phase C, `compact(highestActiveNodeId)` shrinks the array if fewer than 50% of slots are in use, recovering memory from deleted or remapped nodes.

NodeIds are assigned by a single `AtomicInteger nextNodeId` that monotonically increments across all live shards and persisted checkpoints.

### Live graph shards

In-memory inserts go into **`LiveGraphShard`** instances (inner class of `PersistentVectorStore`):

```
LiveGraphShard:
  pkToNode    ConcurrentHashMap<Bytes, Integer>   // PK → nodeId
  nodeToPk    ConcurrentHashMap<Integer, Bytes>   // nodeId → PK
  mravv       VectorStorageRandomAccessVectorValues // vector accessor (lock-free)
  builder     GraphIndexBuilder                    // mutable HNSW graph (jvector)
  vectorCount AtomicInteger                        // live (non-deleted) count
```

`liveShards` is a `volatile List<LiveGraphShard>`. Only the **last element** accepts new inserts; all earlier shards are sealed (read-only). All live shards are searched in parallel during query time.

`VectorStorageRandomAccessVectorValues` wraps `VectorStorage` with `isValueShared() = false` so jvector graph builders can share the same instance across threads without copying vectors.

### Shard rotation

When the active shard reaches its effective maximum size, it is sealed and a new empty shard is appended:

```
computeEffectiveMaxLiveGraphSize():
  if maxLiveGraphSize > 0: return maxLiveGraphSize
  factor = max(dimension / 128.0, 0.5)
  raw = 50_000 / factor
  return clamp(raw, 10_000, 100_000)
```

For example, a 256-dimension index defaults to ~50,000 vectors per shard. This keeps each shard's graph build time bounded. Rotation is protected by a `synchronized` block.

### Deletions in live shards

`removeVector(pk)` locates the node in `pkToNode`, calls `GraphIndexBuilder.markDeleted(nodeId)`, removes from both maps, decrements `vectorCount`, and nulls the `VectorStorage` slot. Deleted nodes are excluded from search results via jvector's built-in delete mask but their storage is only fully reclaimed during the next checkpoint (Phase B).

---

## Three-Phase Checkpoint

The checkpoint is the mechanism that converts live in-memory HNSW shards into durable on-disk FusedPQ segments. It is designed so that DML is never blocked during the expensive I/O-heavy phase.

The three-phase protocol is protected at the outer level by `checkpointLock` (a `ReentrantLock`) — only one checkpoint runs at a time. State transitions within each phase are guarded by `stateLock` (a `ReentrantReadWriteLock`).

### Phase A — Snapshot & Swap (write lock)

Held for only a few milliseconds.

1. **Snapshot live shards:** `frozenShards = new ArrayList<>(liveShards)`. These shards become read-only from this moment. They continue to serve search queries.
2. **Classify on-disk segments:** segment is "sealed" (≥80% of `maxSegmentSize`) or "mergeable".
3. **Create fresh live state:** `liveShards = [createEmptyLiveShard()]`. New inserts go here; they do not interfere with Phase B.
4. **Configure backpressure:**
   - Compute `totalFrozenVectors` across all frozen shards.
   - Compute `liveVectorCapDuringCheckpoint` — the maximum vectors the new live shards may accumulate before blocking (see Memory and Backpressure section).
   - Create `checkpointPhaseComplete = new CountDownLatch(1)`.
   - Create `pendingCheckpointDeletes = ConcurrentHashMap.newKeySet()` — deletions arriving during Phase B are staged here.
5. `dirty = false` (will be set back to true by any insert/delete during Phase B).

### Phase B — Build Graphs & Write to Disk (no lock)

The most time-consuming phase. The write lock is NOT held. New DML continues to be applied to the fresh live shard created in Phase A.

**Vector collection:**

All vectors from frozen shards and mergeable segments are assembled into a single temporary file-backed buffer (`FileBackedVectorValues` — see File-Backed Vector Storage). Deletions recorded in `pendingCheckpointDeletes` during Phase B are filtered out at this stage. Vectors are remapped to dense sequential ordinals `[0, N)`.

**Segment loop:**

The collected vectors are split into chunks (up to the configured `maxSegmentSize`). For each chunk:

1. Build an in-memory `OnHeapGraphIndex` via `GraphIndexBuilder.addGraphNode(ordinal, vector)`. Graph building is parallelized via `CHECKPOINT_POOL` — a `ForkJoinPool` with `availableProcessors / 2` threads, with thread names `persistent-vector-store-checkpoint-{n}`.
2. If FusedPQ conditions are met (see FusedPQ section), write an `OnDiskGraphIndex` using `OnDiskGraphIndexWriter`. Otherwise, serialize the `OnHeapGraphIndex` directly.
3. Serialize ordinal→PK mapping to a temporary file.
4. Split both files into 1 MB chunks and write each chunk as a DataStorageManager page (`TYPE_VECTOR_GRAPHCHUNK = 12`, `TYPE_VECTOR_MAPCHUNK = 13`).
5. Record `SegmentWriteResult(segmentId, graphPageIds[], mapPageIds[], estimatedSizeBytes)`.

**Error handling:**

If Phase B fails for any reason, `recoverFromPhaseBFailure()` is called. It merges the frozen shards back into the live shard list so no data is lost, and resets the checkpoint state. The checkpoint will be retried at the next interval.

### Phase C — Load & Swap (write lock)

Held briefly again for the final state swap.

1. **Load new segments:** For each `SegmentWriteResult`, read graph and map page chunks back from DataStorageManager into temporary files, then load as `VectorSegment` instances (with `OnDiskGraphIndex` or `OnHeapGraphIndex`, BLink, and ordinal-to-PK cache).
2. **Apply pending deletes:** Any PKs added to `pendingCheckpointDeletes` during Phase B are applied to the new segments (mark their offsets as -1).
3. **Atomic swap:** `segments = sealedSegments + newSegments`. Old mergeable segments are closed and their DataStorageManager pages deleted.
4. **Release frozen shards:** `frozenShards = null`. Close old shard builders and release their resources.
5. **Compact VectorStorage:** Call `vectorStorage.compact(nextNodeId.get())` if in-use fraction is below 50%.
6. **Signal completion:** `checkpointPhaseComplete.countDown()` — any thread blocked in backpressure is unblocked.
7. **Reset backpressure cap:** `liveVectorCapDuringCheckpoint = Integer.MAX_VALUE`.
8. `dirty = (totalLiveSize() > 0)` — true if any inserts arrived during Phase B.

---

## Memory Management

### Estimation formula

`PersistentVectorStore.estimatedMemoryUsageBytes()` returns:

```
sum over all live shards:
  shard.vectorCount.get() * dimension * Float.BYTES * memoryMultiplier
```

The `memoryMultiplier` (default `5.0`, configurable via `herddb.vector.memoryMultiplier`) accounts for the HNSW graph structure: neighbor lists, node arrays, and jvector internal bookkeeping in addition to raw float data. On-disk segments do NOT count toward this estimate — their graph data is file-backed and not included.

### Global memory budget

`VectorMemoryBudget` aggregates memory estimates across all `PersistentVectorStore` instances in the IndexingService. It exposes:

- `totalEstimatedMemoryUsageBytes()` — sum of all stores' estimates.
- `maxMemoryBytes()` — the configured global cap (`indexing.memory.vector.limit`; 0 means unbounded, effectively `Long.MAX_VALUE`).
- `isAboveThreshold(double fraction)` — returns true if total usage exceeds `fraction * maxMemoryBytes`. Used to trigger early checkpoints at 70% utilization.

### Backpressure mechanism

Backpressure prevents Phase B from OOM-ing the JVM when the live shard accumulates vectors faster than the checkpoint can serialize them.

**Computing the cap (Phase A):**

`computeLiveVectorCapDuringCheckpoint(frozenVectorCount, dimension, budget, multiplier, minShardSize)`:

```
frozenBytes = frozenVectorCount * dimension * 4 * multiplier
remainingBudget = budget - frozenBytes
rawCap = remainingBudget / (dimension * 4 * multiplier)

hardCap = herddb.vectorindex.maxLiveBytesDuringCheckpoint / (dimension * 4 * multiplier)
          (default 4 GB / (dim * 4 * multiplier))

cap = min(rawCap, hardCap, Integer.MAX_VALUE)
cap = max(cap, minShardSize)   // always allow at least one shard's worth
```

**Enforcing the cap (addVector):**

```java
if (totalLiveSize() >= liveVectorCapDuringCheckpoint) {
    waitForMemoryPressureRelief();
}
```

`waitForMemoryPressureRelief()`:
1. Wakes the compaction thread immediately (`synchronized(compactionWakeUp) { compactionWakeUp.notifyAll() }`).
2. Atomically increments `backpressureActive`.
3. Records start time.
4. Blocks on `checkpointPhaseComplete.await()` — released only when Phase C calls `countDown()`.
5. Records elapsed time to `totalBackpressureTimeMs` and increments `totalBackpressureCount`.
6. Decrements `backpressureActive`.

This means: **insert threads block inside `addVector` until Phase C completes**. Since the WAL tailer applies DML via the apply worker pool (bounded queue with `CallerRunsPolicy`), backpressure naturally propagates all the way back to the tailer thread, which stops consuming WAL entries until memory is recovered.

**Metrics exposed:**

- `totalBackpressureCount` (AtomicLong) — total number of backpressure events.
- `totalBackpressureTimeMs` (AtomicLong) — cumulative blocking time.
- `backpressureActive` (volatile int) — currently blocked insert threads.
- `lastCheckpointPhaseBDurationMs` — duration of last Phase B.
- `lastCheckpointVectorsProcessed` — vectors processed in last checkpoint.

### Background compaction thread

`PersistentVectorStore` has its own daemon compaction thread that triggers checkpoint when:

- `dirty.get() == true` — any modification since last checkpoint AND the compaction interval has elapsed.
- `memoryBudget.isAboveThreshold(0.7)` — global memory is at 70% of configured limit.

The thread wakes on a timer (polling at 50–100 ms) or immediately via `synchronized(compactionWakeUp) { compactionWakeUp.notifyAll() }`. The `IndexingServiceEngine` also runs a `ScheduledExecutorService` that periodically sweeps all registered stores and triggers checkpoints, providing a second level of compaction scheduling.

---

## On-Disk Storage Format

All vector index data is stored through `DataStorageManager.writeIndexPage / readIndexPage`. No files outside HerdDB's storage directories persist at rest (temporary files used during Phase B graph building are deleted immediately after the corresponding page is written).

### Chunk encoding

Each chunk page begins with a VInt type tag followed by a VInt length and then raw bytes:

```
TYPE_VECTOR_GRAPHCHUNK = 12
  VInt(12) | VInt(chunkLen) | byte[chunkLen]

TYPE_VECTOR_MAPCHUNK = 13
  VInt(13) | VInt(chunkLen) | byte[chunkLen]
```

Chunks are at most `CHUNK_SIZE = 1_048_576` bytes (1 MB). Large graphs or map files are split into multiple consecutive chunks, all of whose page IDs are stored in the segment metadata.

### Graph chunk format

Two sub-formats exist depending on whether FusedPQ was active:

**Simple format** (OnHeapGraphIndex, used when vectors < 256 or dimension < 8):

The `OnHeapGraphIndex` is serialized via its own `save(DataOutputStream)` method. The byte stream is split into 1 MB chunks and written as `TYPE_VECTOR_GRAPHCHUNK` pages. At load time, chunks are reassembled into a single stream and passed to `OnHeapGraphIndex.load(DataInputStream)`.

**FusedPQ format** (OnDiskGraphIndex, used when vectors ≥ 256 and dimension ≥ 8 and fusedPQ=true):

The graph is written via `OnDiskGraphIndexWriter` to a temporary file, then the file is split into 1 MB chunks:

```
OnDiskGraphIndex file layout (jvector native format):
  [graph header]
  [feature data — FUSED_PQ block]
    ProductQuantization codebook (dim/4 subspaces, 256 clusters)
    PQ-encoded neighbor vectors for each node
  [feature data — INLINE_VECTORS block]
    Full-precision float32 vectors for each node (for reranking)
  [adjacency lists for each node]
```

The `FusedPQ` feature embeds PQ-encoded neighbor vectors directly in each HNSW graph node for fast approximate scoring during beam search. The `InlineVectors` feature stores full-precision vectors for exact reranking of the final candidates.

### Map chunk format (ordinal → PK)

```
int entryCount
for each entry:
  int ordinal          // new sequential ordinal [0, N)
  int pkLength         // byte length of PK
  byte[pkLength] pk    // raw primary key bytes
```

Map data is serialized to a temporary file, split into 1 MB chunks, and written as `TYPE_VECTOR_MAPCHUNK` pages. On load, the chunks are reassembled and the entries are used to populate the ordinal-to-PK cache arrays (`pkData`, `pkOffsets`, `pkLengths`) in `VectorSegment`.

### BLink pages (PK → ordinal)

Each on-disk segment creates a `BLink<Bytes, Long>` for reverse lookup (given a PK, find its ordinal). BLink pages are stored as DataStorageManager index pages under a per-segment namespace:

```
index name: {indexUUID}_seg{segmentId}_pktonode
```

The BLink's storage implementation (`BytesLongStorage`) serializes page data as variable-length BLink tree nodes. Page size is `memoryManager.getMaxLogicalPageSize()`. Eviction uses `memoryManager.getIndexPageReplacementPolicy()` (ClockPro by default), bounding resident memory for large indexes.

**Ordinal key serialization** — ordinals are converted to a canonical 4-byte big-endian `Bytes` key:

```java
ordinalToBytes(int ordinal):
  b[0] = (byte)(ordinal >>> 24)
  b[1] = (byte)(ordinal >>> 16)
  b[2] = (byte)(ordinal >>> 8)
  b[3] = (byte) ordinal
```

### VectorSegment in-memory state

After loading, each `VectorSegment` holds:

```
VectorSegment:
  segmentId       int
  onDiskGraph     OnDiskGraphIndex OR OnHeapGraphIndex
  onDiskPkToNode  BLink<Bytes, Long>      // PK → ordinal (evictable pages)
  pkData          byte[]                  // packed PK bytes
  pkOffsets       int[]                   // offset[ordinal] in pkData; -1 = deleted
  pkLengths       int[]                   // length[ordinal]
  graphPageIds    long[]                  // DataStorageManager page IDs for graph
  mapPageIds      long[]                  // DataStorageManager page IDs for map
  estimatedSize   long                    // memory estimate
  liveCount       AtomicInteger           // active (non-deleted) ordinal count
```

`getPkForOrdinal(int ordinal)` reconstructs a `Bytes` from `pkData[pkOffsets[ordinal] .. pkOffsets[ordinal]+pkLengths[ordinal]]` — O(1) array access, no hashing.

Deletion in an on-disk segment sets `pkOffsets[ordinal] = -1` and decrements `liveCount`. The ordinal stays in the graph but is excluded from search via an `acceptBits` mask built from `pkOffsets`.

### Index metadata format

Segment layout is persisted to a metadata page via `dataStorageManager.writeIndexMetadata()`. Three binary versions exist (big-endian `ByteBuffer`):

**Version 1 — Simple OnHeapGraphIndex (single segment):**
```
int  version=1
int  dimension
int  M
int  beamWidth
float neighborOverflow
float alpha
byte addHierarchy
int  nextNodeId
int  numGraphChunks
long[] graphPageIds
int  numMapChunks
long[] mapPageIds
```

**Version 2 — FusedPQ OnDiskGraphIndex (single segment):**
```
Same as v1, with:
byte fusedPQ    // inserted after addHierarchy (1 = FusedPQ, 0 = simple)
```

**Version 3 — Multi-segment:**
```
int  version=3
int  dimension
int  M
int  beamWidth
float neighborOverflow
float alpha
byte addHierarchy
byte fusedPQ
int  nextNodeId
int  numSegments
for each segment:
  int  segmentId
  long estimatedSizeBytes
  int  numGraphChunks
  long[] graphPageIds
  int  numMapChunks
  long[] mapPageIds
```

Segments whose size exceeds ~80% of `maxSegmentSize` are "sealed" — they are not included in future merge rounds and their metadata is preserved verbatim across checkpoints.

---

## FusedPQ On-Disk Format

FusedPQ is activated when ALL three conditions are met at checkpoint time:

1. `fusedPQ = true` in index configuration.
2. `dimension >= MIN_DIM_FOR_FUSED_PQ = 8`.
3. `totalVectors >= MIN_VECTORS_FOR_FUSED_PQ = 256` (jvector requires exactly 256 PQ clusters).

When conditions are not met (small indexes or low dimension), the simpler `OnHeapGraphIndex` path is used without quantization.

### PQ codebook construction

```
pqSubspaces = max(1, dimension / 4)
ProductQuantization.compute(trainingVectors, pqSubspaces, clusters=256)
PQVectors = pq.encodeAll(trainingVectors)
```

The codebook is embedded in the `OnDiskGraphIndex` file and is not stored separately.

### Graph writing with features

```java
OnDiskGraphIndexWriter writer = OnDiskGraphIndexWriter.builder(graph, tmpFile)
    .with(FeatureId.FUSED_PQ, () -> new FusedPQ(maxDegree, pq))
    .with(FeatureId.INLINE_VECTORS, () -> new InlineVectors(dimension))
    .build();
writer.write(PQVectors);
```

`FusedPQ(maxDegree, pq)` encodes each node's neighbor vectors using PQ, storing them inline with the adjacency list for fast approximate distance computation during beam search — no random I/O per neighbor comparison.

`InlineVectors(dimension)` stores full-precision float32 vectors for the final reranking pass (exact distance computation on the top candidates returned by the approximate search).

### Two-phase search on FusedPQ segments

```java
VectorEncoding enc = onDiskGraph.getView();
ScoreFunction approxSF = enc.approximateScoreFunctionFor(queryVector);  // FusedPQ LUT
Reranker reranker = enc.rerankerFor(queryVector);                        // InlineVectors
SearchResult sr = GraphSearcher.search(approxSF, reranker, perSourceK, acceptBits);
```

- **Phase 1 (beam search):** `approxSF` uses the precomputed PQ distance look-up table against each node's inline-encoded neighbors — very cache-friendly, no main vector array access.
- **Phase 2 (reranking):** `reranker` fetches full-precision vectors from `InlineVectors` for the final candidate set and computes exact distances.

---

## File-Backed Vector Storage (Checkpoint Phase B)

During Phase B, the collected vectors from frozen shards and mergeable segments are stored in a temporary file-backed buffer (`FileBackedVectorValues`) rather than heap. This keeps Phase B memory bounded regardless of how many vectors are being compacted.

Two implementations are available, selected by the system property `herddb.vectorindex.file.usemmap` (default: `false`):

### ChannelFileBackedVectorValues (default)

Append-only dense layout. Vectors are written sequentially regardless of their nodeId, so there are no sparse gaps.

**Offset index** maps nodeId → file offset. Two strategies:
- `ArrayOffsetIndex` (nodeIds ≤ 10M): `AtomicLongArray` with O(1) lookup.
- `BrinOffsetIndex` (nodeIds > 10M): `BlockRangeIndex` backed by `PageReplacementPolicy` from `MemoryManager`. Bounds memory usage for extremely large indexes by evicting offset pages.

**Thread safety:** Multiple graph builder threads can read vectors concurrently. Each thread gets its own `FileChannel` via `ThreadLocal<FileChannel>` to avoid channel-level contention. Writes use a shared `AtomicLong appendPosition` to claim unique file regions without locking.

### MmapFileBackedVectorValues (alternative)

Sparse layout. Each vector is written at `offset = nodeId * vectorByteSize`. For nodeIds with gaps (due to deletions), those file regions are simply unread.

The file is memory-mapped in segments of at most 1 GiB (`Integer.MAX_VALUE` bound on `MappedByteBuffer`). Growth is handled by synchronized remapping.

Trade-off: faster random reads (no offset index lookup) but higher virtual memory consumption and sparse disk usage with deleted nodes.

---

## Hybrid Search

`PersistentVectorStore.search(float[] queryVector, int topK)` queries all three sources and merges results.

**Overquery factor:** Each source is queried for `perSourceK = topK * OVERQUERY_FACTOR` candidates. `OVERQUERY_FACTOR = 3`. Querying more candidates per source improves recall when merging across sources with different score scales.

**Source 1 — On-disk segments:**

For each `VectorSegment` where `liveCount > 0`:

```
enc = onDiskGraph.getView()
approxSF = enc.approximateScoreFunctionFor(queryVector)   // FusedPQ
reranker  = enc.rerankerFor(queryVector)                  // InlineVectors
acceptBits = BitSet of ordinals where pkOffsets[ord] != -1
sr = GraphSearcher.search(approxSF, reranker, perSourceK, acceptBits)
// ordinals → PKs via getPkForOrdinal()
```

`GraphSearcher` instances are cached per-thread via a `searcherCache` (ThreadLocal) to avoid repeated allocations.

**Source 2 — Live shards:**

For each `LiveGraphShard` where `nodeToPk` is non-empty:

```
graph = shard.builder.getGraph()
sr = GraphSearcher.search(queryVector, perSourceK, mravv, similarityFunction, graph, Bits.ALL)
// nodeIds → PKs via shard.nodeToPk
```

**Source 3 — Frozen shards (during Phase B only):**

Same as live shards, but wrapped in `try-catch` — frozen shards may be concurrently released by Phase C mid-search. Results from frozen shards are additionally filtered against `pendingCheckpointDeletes`.

**Merge:**

```
all results → sort by score descending → return top K
```

**Single-threaded within the store:** Search over segments and shards is sequential within one `search()` call. Parallelism comes from the IndexingService running one `search()` call per gRPC request, handled by the gRPC thread pool.

---

## IndexingServiceEngine

The engine is the core component of the standalone IndexingService. It:

1. **Tails the CommitLog** via `CommitLogTailer` — reads `.txlog` files from the database's WAL directory.
2. **Buffers transactions** via `TransactionBuffer` — delays DML application until COMMIT.
3. **Tracks schema** via `SchemaTracker` — processes DDL entries (CREATE/DROP TABLE/INDEX).
4. **Manages vector stores** — creates `AbstractVectorStore` instances per vector index via `VectorStoreFactory`.
5. **Persists watermark** via `WatermarkStore` — tracks last processed LSN (`watermark.dat`, atomic write via temp + rename) for restart recovery.
6. **Runs periodic checkpoints** via `ScheduledExecutorService` — triggers `PersistentVectorStore.checkpoint()` on all persistent stores.
7. **Routes DML** via striped apply workers (see CommitLog Tailing & DML Parallelism).

### Storage type selection

On `start()`, the engine reads `indexing.storage.type` from configuration:
- `"file"` (default): creates `PersistentVectorStore` instances backed by `FileDataStorageManager`.
- `"memory"`: creates `InMemoryVectorStore` instances (brute-force, for testing).

### Schema tracking

`SchemaTracker` maintains two maps:
- `tables: HashMap<String, Table>` — current table definitions.
- `indexes: HashMap<String, Index>` — current index definitions.

On CREATE_INDEX: the engine calls `createVectorStoreIfNeeded()` → `factory.create()` → registers the store. On DROP_INDEX or DROP_TABLE: the corresponding store is removed and closed.

---

## SQL Integration

### The `ann_of` function

`ann_of(vectorColumn, queryVector)` is a scalar SQL function returning cosine similarity between two float arrays. Registered as a Calcite `ScalarFunction`.

### Planner interception

For `SELECT … ORDER BY ann_of(col, ?) DESC LIMIT k`:

1. `CalcitePlanner.planSort()` detects the `ORDER BY ann_of()` pattern.
2. Creates `VectorANNScanOp` with compiled query vector expression.
3. `VectorANNScanOp.execute()` calls `VectorIndexManager.search()` (which delegates via gRPC).
4. Fetches rows by PK, applies WHERE filter, projects columns.

### WITH clause parsing

`JSQLParserPlanner.extractIndexWithClause()` pre-processes the SQL to strip `WITH key=value ...` suffix before JSQLParser sees it, storing properties in a `ThreadLocal`. `buildCreateIndexStatement()` reads and applies them to the `Index.Builder`.

---

## Data Flow

### Write path

```
Client INSERT → HerdDB Server → CommitLog (.txlog)
                                      │
                     ┌────────────────┘
                     ▼
            CommitLogTailer thread (single)
                     │ buffers by txId until COMMIT
                     ▼
            TransactionBuffer
                     │ on COMMIT: route each entry
                     ▼
            applyWorkers[hash(pk) % N]    (N = availableProcessors/2)
                     │ per-stripe ordering
                     ▼
            PersistentVectorStore.addVector(pk, float[])
              → backpressure check (block if cap exceeded)
              → VectorStorage.set(nodeId, VectorFloat)
              → GraphIndexBuilder.addGraphNode(nodeId, vec)
              → pkToNode.put(pk, nodeId)
```

### Read path

```
Client SELECT ... ORDER BY ann_of(vec, ?) DESC LIMIT k
  → CalcitePlanner intercepts ORDER BY ann_of()  (LIMIT is mandatory; rejected otherwise)
  → VectorANNScanOp.execute()
  → VectorIndexManager.search(queryVector, topK)
  → IndexingServiceClient.search() — parallel fan-out to ALL IS instances
      ├── gRPC call to IS instance 1 ─┐
      ├── gRPC call to IS instance 2 ─┤ (dispatched concurrently,
      └── gRPC call to IS instance N ─┘  each with its own deadline)
  → each IS: IndexingServiceEngine.search()
           → PersistentVectorStore.search(queryVector, topK)
           → hybrid search: on-disk segments + live shards + frozen shards
  → client waits for ALL futures, merges into a bounded top-K min-heap
      (fails fast and cancels in-flight RPCs if any instance errors)
  → results returned to VectorANNScanOp
  → PK fetch + WHERE filter + projection
```

**LIMIT is required** for `ORDER BY ann_of(...)` queries. The JSQL
planner rejects unbounded `ORDER BY ann_of(...)` with
`StatementExecutionException` so the cluster never fans out an
unbounded search. With the Calcite planner the LIMIT is pushed into
`VectorANNScanOp` whenever possible (no `WHERE` predicate); when a
predicate forces the outer `LimitOp` to stay around, the scan op
still requests the full result set from the index and the outer
`LimitOp` truncates the post-predicate output.

**Parallel fan-out.** Each indexing-service instance holds a subset of
the graph shards (see `IndexingServiceEngine`:
`shardId % numInstances == instanceId`). Partial results would be
incorrect, so the client queries **every** configured instance for
every search, in parallel. If any RPC fails or exceeds its deadline,
the whole query fails fast and the remaining in-flight RPCs are
cancelled. Results from the successful instances are merged into a
top-K min-heap ordered by score ascending: the heap evicts its
weakest entry whenever a better candidate arrives, so it keeps only
the globally highest-scoring `topK` PKs across all instances. After
all futures complete, the heap is drained and sorted descending for
the final response. A single-instance deployment takes a blocking
fast-path that skips the future machinery.

### Checkpoint path

```
compaction thread wakes (timer or memory pressure)
  → PersistentVectorStore.checkpoint()
  → Phase A: snapshot live state (brief write lock)
      - frozenShards = snapshot
      - liveShards = [new empty shard]
      - liveVectorCapDuringCheckpoint = computed cap
      - checkpointPhaseComplete = new CountDownLatch(1)
  → Phase B: build FusedPQ graphs, write to DataStorageManager (no lock)
      - collect vectors from frozen shards + mergeable segments
      - build OnHeapGraphIndex via CHECKPOINT_POOL (ForkJoinPool, availableProcessors/2)
      - if FusedPQ: compute PQ codebook, write OnDiskGraphIndex with FusedPQ+InlineVectors
      - split into 1 MB chunks → writeIndexPage() as TYPE_VECTOR_GRAPHCHUNK/MAPCHUNK
  → Phase C: load new segments, swap (brief write lock)
      - load segments from DataStorageManager pages
      - create BLinks for PK-to-ordinal mapping
      - close frozen shards and merged segments
      - apply pendingCheckpointDeletes to new segments
      - atomic swap: segments = newSegments
      - checkpointPhaseComplete.countDown()  ← unblocks any backpressured insert threads
  → IndexStatus written with metadata + active page IDs
```

---

## jvector Integration

### Library

```xml
<dependency>
    <groupId>io.github.jbellis</groupId>
    <artifactId>jvector</artifactId>
    <version>4.0.0-rc.9-herddb-SNAPSHOT</version>
</dependency>
```

### Graph builder configuration

```java
new GraphIndexBuilder(
    buildScoreProvider,
    dimension,
    M,                  // default 16
    beamWidth,          // default 100
    neighborOverflow,   // default 1.2f
    alpha,              // default 1.4f
    ADD_HIERARCHY=false,
    REFINE_FINAL_GRAPH=false,
    PhysicalCoreExecutor.pool(),   // live insert parallelism
    CHECKPOINT_POOL                // checkpoint graph-build parallelism
)
```

### Key jvector types used

| jvector class | Role |
|---------------|------|
| `GraphIndexBuilder` | Mutable builder for incremental inserts/deletes |
| `OnHeapGraphIndex` | In-memory Vamana HNSW graph |
| `OnDiskGraphIndex` | Immutable on-disk graph with FusedPQ support |
| `OnDiskGraphIndexWriter` | Writes graph to file with feature suppliers |
| `FusedPQ` | Embeds PQ-encoded neighbor vectors for fast approximate scoring |
| `InlineVectors` | Stores full-precision vectors for exact reranking |
| `ProductQuantization` | Computes PQ codebook from training vectors |
| `PQVectors` | PQ-encoded vectors for FusedPQ state |
| `DefaultSearchScoreProvider` | Combines approximate + exact scoring for two-phase reranking |
| `BuildScoreProvider` | Scoring during graph construction |
| `GraphSearcher` | Executes beam search on a graph index |
| `VectorSimilarityFunction` | `COSINE`, `EUCLIDEAN`, `DOT_PRODUCT` |
| `VectorFloat<?>` / `VectorTypeSupport` | Vector data types with SIMD acceleration |

---

## Synchronization Summary

| Operation | Lock | Notes |
|-----------|------|-------|
| `addVector` (normal) | `stateLock.readLock` | Check cap, add to active shard; concurrent with search |
| Shard rotation | `synchronized(rotateLiveShard)` | Append new shard to list |
| Phase A | `stateLock.writeLock` | Brief: snapshot + swap + configure cap |
| Phase B | (none) | Concurrent with live inserts and search |
| Phase C | `stateLock.writeLock` | Brief: swap segments + release frozen shards |
| `search` | `stateLock.readLock` | Concurrent with inserts, blocks during Phase A/C |
| `VectorStorage.set/remove` | `synchronized` | Only during array growth or delete; lock-free otherwise |
| `checkpoint()` | `checkpointLock` | Prevents concurrent checkpoints |
| DDL apply | `awaitPendingWork()` | Drains all apply workers before applying DDL |

---

## Known Limitations

- **LIMIT not pushed into ANN search.** `VectorANNScanOp` currently queries with `topK = Integer.MAX_VALUE` in some code paths.
- **WHERE filtering is post-fetch.** All ANN candidates are fetched by PK before WHERE is tested.
- **FusedPQ requires ≥ 256 vectors.** Smaller indexes use the simpler OnHeapGraphIndex format without quantization.
- **Single sort key only.** Multi-column ORDER BY and joins fall through to brute-force full table scan.
- **Deleted vectors accumulate between checkpoints.** Vectors stay in `VectorStorage` and graph node lists until the next Phase B cleanup; only their ordinals/PKs are masked from results.
- **Search is sequential across segments.** No segment-level parallelism within a single `search()` call.

---

## Performance Benchmark

A standalone benchmark tool lives in `vector-testings/`. It measures ingestion throughput, index build time, ANN query latency/throughput, and recall accuracy against ground truth.

```bash
# Build
mvn -f vector-testings/pom.xml package -DskipTests

# Quick smoke test
./vector-testings/run.sh --password secret -n 1000 --batch-size 100

# Full SIFT-1M benchmark
./vector-testings/run.sh --password secret --dataset sift1m -n 1000000 --ingest-threads 8

# Queries only (data already loaded)
./vector-testings/run.sh --password secret --skip-ingest --skip-index --queries 5000 -k 20
```

### Datasets

| Name | Vectors | Dimensions | Size |
|------|---------|------------|------|
| `sift1m` | 1M | 128 | ~170 MB |
| `sift10m` | 10M | 128 | ~98 GB |
| `bigann` | 1B | 128 | ~98 GB |

---

## indexing-admin — diagnostic CLI

`indexing-admin` is a lightweight gRPC CLI for inspecting the internal state
of a **single** indexing service instance. It's bundled with `herddb-services`
at `bin/indexing-admin.sh` and pre-wired into the k3s tools pod as
`/usr/local/bin/indexing-admin`. Use it when you need to understand what an
indexing replica is actually holding — loaded indexes, live/on-disk node
counts, tailer lag, apply-queue backpressure, or the set of primary keys
backing an index — without scraping Prometheus or reading `sysindexstatus`
from the main HerdDB server.

All commands talk to **one** instance at a time. `list-instances` is the only
command that reads ZooKeeper; every other sub-command takes `--server host:port`
pointing at the specific replica you want to inspect.

### Sub-commands

| Command | Purpose |
|---|---|
| `list-instances` | Read ZooKeeper and print every registered indexing service address |
| `list-indexes` | Enumerate the indexes loaded by one instance (tablespace, table, index, vector count, status) |
| `describe-index` | Full per-index detail: dimension, similarity, live vs on-disk node counts, segments, shards, memory, LSN, FusedPQ/M/beam-width, dirty flag |
| `status` | One-line wrapper over the legacy `GetIndexStatus` RPC |
| `list-pks` | Stream the list of primary keys backing an index (hex or base64 output, optional `--include-ondisk`, `--limit`) |
| `engine-stats` | Tailer watermark, entries processed, apply queue size/capacity/parallelism, loaded index count, total estimated memory, uptime |
| `instance-info` | Instance id, gRPC host:port, storage type, data dir, tablespace name + UUID, ordinal/numInstances, JVM max heap |

### gRPC methods exposed by the indexing service

Five new RPCs were added to `indexing_service.proto` to back the CLI
(`ListIndexes`, `DescribeIndex`, `ListPrimaryKeys` — server-streamed —
`GetEngineStats`, `GetInstanceInfo`). They are strictly additive; the existing
`Search` and `GetIndexStatus` wire format is unchanged.

### Examples

```bash
# Build and run locally against a released zip
mvn -pl herddb-indexing-service,herddb-services -am package -DskipTests
./target/herddb-services-*/bin/indexing-admin.sh list-indexes \
    --server localhost:9850

# Extended view of a single index
./target/herddb-services-*/bin/indexing-admin.sh describe-index \
    --server localhost:9850 \
    --tablespace herd --table docs --index emb_hnsw

# Dump up to 1000 PKs as hex (live graph only)
./target/herddb-services-*/bin/indexing-admin.sh list-pks \
    --server localhost:9850 \
    --table docs --index emb_hnsw --limit 1000

# Engine snapshot in JSON
./target/herddb-services-*/bin/indexing-admin.sh engine-stats \
    --server localhost:9850 --json
```

### In the k3s tools pod

The helm chart mounts the CLI as `indexing-admin` in `$PATH` and injects
`HERDDB_INDEXING_ZK` into the tools container. `list-instances` picks up the
ZK connect string automatically; every other command still requires an
explicit `--server` so the operator is aware which replica they are talking
to.

```bash
kubectl exec -it herddb-tools-0 -- indexing-admin list-instances
kubectl exec -it herddb-tools-0 -- indexing-admin list-indexes \
    --server herddb-indexing-service-0.herddb-indexing-service:9850
kubectl exec -it herddb-tools-0 -- indexing-admin engine-stats \
    --server herddb-indexing-service-0.herddb-indexing-service:9850 --json
```

Setting `HERDDB_INDEXING_SERVER=host:port` in the environment lets the wrapper
auto-fill `--server` for commands that need it — useful for scripted health
checks that target one replica at a time.

### Out of scope (v1)

- Live TUI / dashboards. `watch -n2 indexing-admin engine-stats` covers the
  live case with stdlib tools.
- Write operations (force-checkpoint, drop-segment, etc.). This tool is
  diagnostic; destructive verbs belong to a separate change.
- Automated tuning suggestions. The raw numbers are exposed today; a future
  `indexing-admin advise` sub-command can layer rules on top of
  `describe-index` + `engine-stats`.

