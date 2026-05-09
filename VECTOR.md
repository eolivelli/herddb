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

6. **Pluggable compaction (segmented-v2).** When `indexing.optimizer.enabled=true`, segments are registered in a ZooKeeper registry with mutable per-segment ownership and compaction is offloaded to a singleton `index-optimizer` service. Tombstones live in a per-segment overlay file in remote storage so segments stay byte-immutable across ownership transfers. See [Segmented-v2: external `index-optimizer` service & movable segment ownership](#segmented-v2-external-index-optimizer-service--movable-segment-ownership) for the full design.

---

## Modules

| Module | Contains |
|--------|----------|
| `herddb-core` | `AbstractVectorStore`, `PersistentVectorStore`, `VectorIndexManager` (thin remote client), `VectorStorage`, `VectorSegment`, helper classes, SQL planner integration, `DataStorageManager`, `MemoryManager`, `BLink` |
| `herddb-indexing-service` | `IndexingServer` (gRPC), `IndexingServiceEngine`, `IndexingServerConfiguration`, `InMemoryVectorStore`, `VectorStoreFactory`, `CommitLogTailer`, `SchemaTracker`, `TransactionBuffer`, `WatermarkStore`, Prometheus metrics, **`herddb.indexing.segment`** (`SegmentRegistryClient`, `SegmentMetadata`, `SegmentAssignmentWatcher`, `OwnershipTransfer`, `TombstoneOverlayManager`, `SegmentRegistryPublisher`), **`herddb.indexing.optimizer`** (`IndexOptimizerMain`, `IndexOptimizerEngine`, `MergePolicy`, `SegmentMerger`, `OptimizerConfiguration`) |
| `herddb-services` | `IndexingServiceMain` — standalone server entry point. The same launcher script (`bin/service`) also dispatches `index-optimizer` to `herddb.indexing.optimizer.IndexOptimizerMain`. |

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
| `numShards` | integer | `1` | Number of logical hash buckets within the index (`shardId = XXHash64(pk) % numShards`). Per-index, immutable after CREATE INDEX — it controls bucket granularity, not which replica owns a bucket. |

The number of indexing-service primary replicas across which an index is
sharded — `numInstances` — is **not** an index-level property. It lives
on the engine (initialised from the JVM property
`indexing.cluster.numInstances` and then updated at runtime by every
`INDEXING_SERVICE_REBALANCE` log entry the operator triggers via
`EXECUTE INDEXING_SERVICE_REBALANCE 'tablespace', N`). See [Dynamic
Scale-Up of Indexing Service Replicas](#dynamic-scale-up-of-indexing-service-replicas)
for the full design.

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
| Compaction interval | `indexing.compaction.interval` | `60000` | Checkpoint driver interval in ms (live-shard flush) |
| Compaction threads | `indexing.compaction.threads` | `2` | Background checkpoint threads |
| Vector compaction interval | `vector.index.compaction.intervalMs` | `300000` | Graph-merge compaction cadence (ms) |
| Vector compaction min bytes | `vector.index.compaction.minBytes` | `268435456` | Minimum total size of compaction candidates before firing (256 MB) |
| Vector compaction max bytes | `vector.index.compaction.maxBytes` | `1073741824` | Hard cap on bytes read per compaction run (1 GB) |
| Vector compaction retention | `vector.index.compaction.retentionMs` | `600000` | Retention deadline for old segment files after a compaction swap (10 min) |
| Vector streaming compaction | `vector.index.compaction.streaming.enabled` | `true` | Use jvector's `OnDiskGraphIndexCompactor` (issue #485) for vector-index compaction instead of the in-memory `GraphIndexBuilder` rebuild. Memory cost is bounded by `taskWindowSize × maxDegree` instead of `numTotalNodes × dimension`, so the historical 1 GB cap on `vector.index.compaction.maxBytes` is no longer dictated by heap pressure. Governs both the IS-local path (`VectorIndexCompactor.rebuildSegment`) and the optimizer-pod path (`RemoteSegmentGraphMerger`). Setting to `false` falls back to the legacy in-memory rebuild path (operator escape hatch). The same value can also be set via the JVM system property `herddb.vectorindex.streamingCompactionEnabled` (the config key wins at IS startup). |
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
long indexStatusGeneration
int  numSegments
for each segment:
  int  segmentId
  long estimatedSizeBytes
  utf  graphFilePath
  long graphFileSize
  utf  mapFilePath    // "" if absent
  long mapFileSize
  long generation
int  numPendingDeletes
for each pending delete:
  utf  filePath        // "<segUuid>:<graph|map>"
  long deadlineMs      // wall-clock, System.currentTimeMillis
  long sinceGeneration // deletion gated until all shadows ack > this
```

`indexStatusGeneration` is a monotonic counter bumped on every metadata publish. Each segment is stamped with the generation that produced it: the compactor uses these stamps to pick the authoritative source for a PK, and the retention reaper uses them to gate deletion against shadow replica lag.

Segments whose size exceeds ~80% of `maxSegmentSize` are "sealed" — they are preserved verbatim across checkpoints and are only rewritten by graph-merge compaction.

### Segment Compaction (graph merge)

Large accumulations of small on-disk segments slow queries (search fans out across every segment) and leave storage tied up in tombstoned PKs until a segment fully rotates out. Segment compaction is a periodic background task that picks the smallest mergeable segments, rebuilds a single larger jvector graph from the vectors whose PKs are still authoritative, atomically swaps the merged output for the inputs in `IndexStatus`, and queues the old segment files for retention-aware deletion.

**Policy.** `VectorIndexCompactor.chooseSegmentsToMerge` picks the smallest-first subset of candidates up to `vector.index.compaction.maxBytes` (default 1 GB), firing only when both a minimum count is met and the combined input size crosses `vector.index.compaction.minBytes` (default 256 MB). Compaction is throttled by `vector.index.compaction.intervalMs` (default 5 min) — independent of the checkpoint driver's `indexing.compaction.interval`.

**Live-PK filter.** `VectorIndexCompactor.buildAuthorityMap` drops from the merged output:
- vectors tombstoned inside their input segment (pkOffsets[ord] == -1);
- vectors whose PK is present in a segment with a higher `generation` than any candidate;
- vectors whose PK is held by a live in-memory shard.

This reclaims storage held by deleted or superseded PKs — the previous design could only mask them from search results.

**Shadow Retention Protocol.** Input segment files are NOT deleted immediately after the atomic swap. Each one is appended to the IndexStatus `pendingDeletes` list with `deadlineMs = now + retentionMs` (default 10 min) and `sinceGeneration = indexStatusGeneration at the moment of the swap`. A file becomes eligible for physical deletion when BOTH:
- `System.currentTimeMillis() >= deadlineMs`, AND
- every known shadow replica has acked a generation strictly greater than `sinceGeneration` (aggregated as `min(appliedIndexStatusGeneration)` across shadows; treated as `Long.MAX_VALUE` when no shadows are registered).

`PersistentVectorStore.reapExpiredPendingDeletes(minShadowAcked)` performs one sweep, returning the number of files physically deleted. Entries whose `deadlineMs` has elapsed but whose shadow gate is not yet open remain retained until a later pass.

**Concurrency.** Compaction acquires `checkpointLock` only for the final atomic swap and metadata publish — the same lock checkpoint Phase C uses — so `IndexStatus` updates stay monotonic. The heavy rebuild and write run lock-free. Deletes arriving during a rebuild are tracked in `pendingCompactionDeletes` and replayed against the merged output before it becomes visible.

**Background thread.** `PersistentVectorStore` runs a dedicated `vectorIndexCompactionThread` (separate from the checkpoint driver) that wakes every `vector.index.compaction.intervalMs` and invokes `VectorIndexCompactor.runCompactionIfNeeded(...)`. The thread is started only on primaries — shadow replicas never compact. **When the external `index-optimizer` service is enabled (see next section), this thread becomes a pressure-driven fallback: it stays armed but its cycle body short-circuits below `kickFraction × backpressureThreshold` segments, letting the optimizer drive steady state and only firing locally when accumulation indicates the optimizer is falling behind. See "Pressure-driven IS-local compaction fallback" in the next section for the protocol.**

**Shadow acknowledgement.** Shadow replicas expose their loaded generation via the `GetShadowStatus` RPC; `IndexingServiceEngine` aggregates `min(appliedIndexStatusGeneration)` across all registered shadows. The leader passes that minimum to `reapExpiredPendingDeletes` before every physical delete pass.

---

## Segmented-v2: external `index-optimizer` service & movable segment ownership

Legacy compaction (above) keeps every segment glued to the IS instance that created it: the segment's identity is `(indexUUID, segmentId)` where `segmentId` is a per-store integer counter. There is no way to reassign a segment to another instance, and the heavy graph-merge work runs on the IS hot path — competing with tailing, checkpoint, and search.

**Segmented-v2** introduces three changes that lift those constraints, gated cluster-wide by `indexing.optimizer.enabled=true`:

1. **Segments have a globally-unique UUID and live in a ZooKeeper registry.** Each sealed segment is registered at `/{basePath}/index-segments/{tablespaceUuid}/{indexUuid}/{segmentUuid}` with full metadata: state, owner instance, S3 paths for graph/map/tombstone overlay, LSN watermarks, generation, `replacedBy` lineage, retention deadline.
2. **Segment ownership is mutable.** A CAS protocol on the znode moves a segment from one IS instance to another with no data loss and continuous read availability (a brief read-overlap is tolerated; the server-side `SearchResultMerger` dedups duplicate PKs by keeping the highest score).
3. **Compaction runs in a dedicated singleton service, `index-optimizer`.** Packaged in `herddb-services` and deployed via the Helm chart as a StatefulSet with `replicas: 1` and a `tmp` PVC. The optimizer scans the registry, applies a merge policy, runs the merge, and drives the registry-side state machine. The IS suppresses its in-process compaction loop in this mode.

This is **greenfield-only**: existing legacy indexes continue to use the in-IS compaction path and the `IndexStatus`-based segment list. Indexes created after the upgrade with `indexing.optimizer.enabled=true` opt into the segmented-v2 model.

### Segment lifecycle state machine

```
            createSegment            initiate(Y)
   (none) ─────────────────► ACTIVE ──────────────► TRANSFERRING
                                ▲                          │
                                │                          │ complete(Y)
                                └──────────────────────────┘
                                          ─►
                                  (owner=Y, pending=NONE)

   ACTIVE ──────────► DEPRECATED ──────────► DELETED ──► (znode removed)
            optimizer            retention     remove
            published merge      elapsed       znode + S3 files
            output; sets
            replacedBy[],
            retentionUntilEpochMillis
```

`SegmentState` (in `herddb.indexing.segment.SegmentState`) enumerates: `PROVISIONAL`, `ACTIVE`, `TRANSFERRING`, `DEPRECATED`, `DELETED`. `PROVISIONAL` is reserved as a crash-recovery marker (paired with an ephemeral child znode) for an optimizer that died between uploading merged files and committing the registry CAS.

### Tombstone overlay

Sealed segments stay byte-immutable — but deletes/updates still need to be honored across ownership transfers. Each segment carries a sibling **tombstone overlay** in remote storage (`fileType = "tombstones-{generation}"`) that records the segment-local ordinals of deleted entries plus an LSN watermark.

- The current owner's `TombstoneOverlayManager` accumulates tombstones in memory (`SortedSet<Integer>`) and flushes periodically: serialize → `DataStorageManager.writeMultipartIndexFile` → CAS the segment znode to update `tombstonePath` and `tombstoneLsn`. A failed CAS rolls back the just-uploaded overlay file (best-effort).
- A new owner picks up an in-flight transfer by downloading the latest overlay (`TombstoneOverlayManager.loadOverlay`) and replacing its in-memory state via `replaceFromOverlay`. It then continues from the loaded LSN watermark.
- Wire format v1 is documented in `TombstoneOverlay.java`: `[version, segmentUuid, tombstoneLsn, overlayGeneration, count, ordinals[]]`. The format is intentionally tiny (a few hundred bytes for sparse delete patterns) and bumps the generation on every flush so concurrent readers can pin a stable snapshot.

### Ownership transfer protocol

Two static helpers in `OwnershipTransfer` drive the transfer via CAS:

1. **`OwnershipTransfer.initiate(registry, current, newOwner)`** — moves a segment from `ACTIVE` to `TRANSFERRING`, recording `pendingOwnerInstanceId = newOwner`. The current owner stays unchanged so reads keep flowing.
2. **`OwnershipTransfer.complete(registry, current, newOwner)`** — invoked by the new owner after it has downloaded the artefacts and reloaded the overlay. CAS-flips the znode to `state = ACTIVE, owner = newOwner, pendingOwnerInstanceId = NO_INSTANCE`.

Each IS instance runs a `SegmentAssignmentWatcher` per index that fires `SegmentAssignmentListener` callbacks (`onPendingAssignment`, `onSegmentAssigned`, `onSegmentReleased`) by diffing successive ZK reads against its local view. A 30 s heartbeat refresh defends against missed watcher fires during ZK reconnects.

Under transfer, both the old and the new owner may briefly serve the same PK on a search query. The client-side `SearchResultMerger` (replacing the previous `PriorityQueue`-only merge in `IndexingServiceClient`) groups responses by PK and keeps the highest score before truncating to top-K — the visible duplicate is collapsed.

### `index-optimizer` service

A separate JVM, packaged inside `herddb-services` and launched via:

```bash
/opt/herddb/bin/service index-optimizer console /opt/herddb/conf/indexoptimizer.properties
```

The service is a singleton: at most one optimizer per cluster. Helm enforces this with `replicas: 1` on the StatefulSet (`herddb-kubernetes/.../templates/index-optimizer-statefulset.yaml`); a stray second optimizer cannot corrupt anything (every state transition is a ZK CAS) but will simply lose every race to the leader.

`IndexOptimizerEngine.runOnce()` runs once per scheduled tick:

1. List indexes for the configured tablespace.
2. For each index, partition segments into `ACTIVE` / `DEPRECATED` (others ignored).
3. **Reap** any DEPRECATED segments whose `retentionUntilEpochMillis` has elapsed: CAS to `DELETED`, then delete the znode.
4. **Pick merge candidates** using `MergePolicy.SmallestFirstPolicy` — smallest-first up to `maxBytes`, fired when either segment count ≥ `maxCount` (the issue #285 ceiling, force-fires regardless of size) or count ≥ `minCount` AND aggregate size ≥ `minBytes`.
5. **Run the merger**: `SegmentMerger.merge(inputs, newOwnerInstance)`. The production merger (out of scope for this initial change — TODO) reuses the existing `VectorIndexCompactor` rebuild path; tests use the bundled `InMemorySegmentMerger`.
6. **Publish** the output (`createSegment`) and CAS-deprecate the inputs (`state = DEPRECATED, replacedBy = [output.uuid], retentionUntilEpochMillis`).

Crash-recovery is implicit: the engine is stateless across runs, so a partial state at restart (e.g. output published but inputs not yet deprecated) is observed on the next tick and either re-attempted or healed by the next compaction cycle. ZK CAS prevents corruption.

A pluggable SPI (`ServiceLoader<SegmentMerger>`) lets deployments register a real graph-aware merger; absent any provider, a `NoopMerger` fallback logs and declines every merge — useful for end-to-end registry-lifecycle integration tests.

### Configuration

IS-side (`IndexingServerConfiguration`):

| Property | Default | Notes |
|----------|---------|-------|
| `indexing.optimizer.enabled` | `false` | When `true`, the IS-local `vectorIndexCompactionLoop` becomes pressure-driven (see below). Tailer + checkpoint loops still run. |
| `vector.index.compaction.local.kick.fraction` | `0.7` | Fraction of `vector.index.compaction.backpressure.segments` above which the IS-local compaction fallback runs. Below this threshold the loop short-circuits and lets the optimizer drive steady state. Range: `(0.0, 1.0)`. |
| `vector.index.compaction.local.enabledWithOptimizer` | `true` | Master switch for the IS-local fallback when the optimizer is enabled. Set to `false` to fully delegate compaction to the optimizer (the tailer may then stall on back-pressure if the optimizer cannot keep up). |

#### Pressure-driven IS-local compaction fallback

When `indexing.optimizer.enabled=true`, the IS-local compaction thread no longer disappears — it stays armed but only runs cycles when locally-observed segment count crosses `kickFraction × backpressureThreshold` (default `0.7 × 500 = 350`). Steady state remains optimizer-driven; the IS only kicks in when:

- the optimizer is temporarily down,
- it's leader-locked on a different tablespace,
- or it's processing a long-running merge while a heavy ingest workload accumulates new sealed segments faster than it can drain them.

The local fallback follows the same staged-publish protocol as the checkpoint:

1. **Stage** the merged output via `SegmentRegistryPublisher.stageNewSegments` (PROVISIONAL znode).
2. **Revalidate** every input is still ACTIVE in the registry. If a concurrent compactor (the optimizer or another IS) has already deprecated any input we ABORT — call `unstage` on the staged znode, queue the merged output's multipart files for the existing `pendingDeletes` retention reaper, and skip the in-memory swap.
3. **Persist IndexStatus** locally (the merged output + remaining segments).
4. **Commit** the staged znode (PROVISIONAL → ACTIVE) and **CAS-deprecate** every input (ACTIVE → DEPRECATED with `replacedBy=[mergedUuid]`).

A per-input `VersionMismatch` during deprecate is benign — the optimizer raced us on that specific input; our merged output remains valid for the others, and the next optimizer tick folds the orphan ACTIVE input into a follow-up merge. Both compactors race freely; ZK CAS prevents corruption.

The `addVectorInternal` hot path also pokes the local loop the instant segment count crosses the kick threshold (cheap int compare on the existing fast path), so the fallback responds within milliseconds, not the per-cycle interval.

Operators can monitor whether the optimizer is keeping up via two new counters on `PersistentVectorStore`:

- `getLocalCompactionPressureRunsTotal()` — number of fallback cycles fired. Non-zero, growing = optimizer is falling behind.
- `getLocalCompactionSkippedBelowThresholdTotal()` — number of cycles short-circuited. Steady-state baseline.

Optimizer-side (`OptimizerConfiguration`, `conf/indexoptimizer.properties`):

| Property | Default | Notes |
|----------|---------|-------|
| `indexoptimizer.zookeeper.address` | `localhost:2181` | Must match the cluster's ZK. |
| `indexoptimizer.zookeeper.path` | `/herd` | Must match `server.zookeeper.path`. |
| `indexoptimizer.tablespace.name` | *(required)* | Human-readable tablespace name (e.g. `herd`); UUID resolved from ZooKeeper at startup. Issue #481. |
| `indexoptimizer.interval.ms` | `300000` | Scheduler tick. |
| `indexoptimizer.merge.min.count` | `4` | |
| `indexoptimizer.merge.max.count` | `200` | Force-fire ceiling (issue #285 parity). |
| `indexoptimizer.merge.min.bytes` | `268435456` (256 MiB) | |
| `indexoptimizer.merge.max.bytes` | `1073741824` (1 GiB) | Per-run input cap. |
| `indexoptimizer.retention.ms` | `600000` (10 min) | DEPRECATED → DELETED window. |

Helm values (`indexOptimizer.*`):

```yaml
indexOptimizer:
  enabled: false
  tablespaceName: ""              # human-readable name, e.g. "herd"; UUID resolved at startup (#481)
  intervalMs: 300000
  minCount: 4
  maxCount: 200
  minBytes: "268435456"           # quoted string to prevent YAML float conversion (#480)
  maxBytes: "1073741824"
  retentionMs: 600000
  storage:
    tmp:
      size: 20Gi                  # PVC for merge-intermediate files
      storageClass: ""
  resources:
    requests: { memory: "1Gi", cpu: "2" }
    limits:   { memory: "1Gi", cpu: "2" }
```

The StatefulSet mounts the PVC at `/opt/herddb/optimizer-tmp` and exposes it to the JVM via `-Dindexoptimizer.tmp.dir=…`; the merger uses it for staging files before multipart upload.

### ZK znode shape

Each segment znode stores a JSON-serialized `SegmentMetadata`:

```
segmentUuid, tablespaceUuid, tableName, indexUuid, indexName,
state ∈ {PROVISIONAL, ACTIVE, TRANSFERRING, DEPRECATED, DELETED},
ownerInstanceId, pendingOwnerInstanceId,
graphPath, mapPath, tombstonePath,
tombstoneLsnLedgerId, tombstoneLsnOffset,
baseLsnLedgerId, baseLsnOffset,
sizeBytes, vectorCount, generation,
replacedBy[], retentionUntilEpochMillis, createdAtEpochMillis
```

CRUD operations are exposed by `SegmentRegistryClient` (`createSegment`, `getSegment`, `listSegments`, `casUpdateSegment`, `casDeleteSegment`, plus parent listings `listIndexes`, `listTablespaces`). Watcher arming is supported on both child and data znodes; the registry lazily creates parent znodes on first segment registration.

### Production prerequisites — DO NOT enable `indexOptimizer.enabled=true` until ALL of the following are in place

The current PR ships the registry-side state machine, the staged-publish protocol, the leader-lock, the in-process tombstone overlay, and the optimizer service itself, but it does NOT yet wire them through the IS hot path. Flipping the Helm chart's `indexOptimizer.enabled=true` against an IS that lacks these wirings will silently corrupt indexes (the optimizer would deprecate segments the IS still references, and on the next IS restart the segments would fail to load with file-not-found). Verify each prerequisite before enabling:

1. **Real `SegmentMerger` SPI registered.** The default `IndexOptimizerMain` SPI loader returns a `NoopMerger` that declines every merge. A production deployment must register a `SegmentMerger` ServiceLoader file (see `META-INF/services/herddb.indexing.optimizer.SegmentMerger`) backed by a real graph-aware implementation extracted from `VectorIndexCompactor`. The merger must also implement the `abandon(SegmentMetadata)` callback to clean up multipart files when a revalidate-abort discards an output (review-item R4).
2. **IS-side `SegmentAssignmentWatcher` wired in `IndexingServiceEngine`.** Every IS instance must run a watcher that, on `onSegmentReleased`, closes its local segment handle BEFORE the optimizer reaps the underlying files. Without this, ownership transfers and reaps run blind.
3. **`indexoptimizer.safeMode.fileDeletion=false` opt-in.** The optimizer ships with safe-mode enabled by default; the reaper progresses the znode lifecycle (DEPRECATED → casDelete) but does NOT call `DataStorageManager.deleteMultipartIndexFile`. Disable safe-mode only after #2 above is verified end-to-end. Doing so requires a non-null `DataStorageManager` to be wired into `IndexOptimizerEngine`.
4. **Per-index opt-in plumbed through `IndexingServiceEngine`.** `PROPERTY_INDEX_OPTIMIZER_ENABLED` is currently parsed but never read by the IS engine. Production code must call `PersistentVectorStore.setSegmentPublisher` and `setExternalCompactionEnabled` based on the per-index flag at construction time. Until this lands, only test code exercises the publisher attach path.
5. **Rollback strategy documented.** Indexes written in v4 IndexStatus format cannot be loaded by a binary that only knows v3 — the v3-only loader fails fast with a clear `DataStorageManagerException` (review-item B4). If you need bidirectional compatibility for a phased rollout, gate `indexOptimizer.enabled` per-tenant and keep at least one tier on the v3-only binary.

When all five are in place, follow the validation checklist in the PR description before flipping the production switch.

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
5. **Persists watermark** via `WatermarkStore` — at every successful checkpoint writes a `WatermarkSnapshot` containing the last-applied LSN, `numInstances`, the current table/index schema, and the per-store checkpoint UUID (`_is.store.uuid`) to disk or S3; on restart loads this snapshot to resume from the exact checkpoint state without full log replay.
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

**Duplicate CREATE_INDEX guard.** `createVectorStoreIfNeeded` also tracks the logical `Index.uuid` alongside each store. If the commit-log tailer replays a CREATE_INDEX entry for an index whose store was already created by `installSchemaFromSnapshot` on startup, the duplicate is detected and silently skipped (same UUID → no-op). If a different UUID is seen for the same `(table, index)` key — which should not happen in normal operation but could indicate log divergence — a WARNING is logged and the existing store is kept.

### Schema snapshot in the watermark (issue #368)

When a checkpoint completes, `checkpointAndSaveWatermark` collects the current schema from `SchemaTracker` and embeds it in the `WatermarkSnapshot` before writing it to `WatermarkStore`. Each vector index in the snapshot also carries the property `_is.store.uuid` (key `IndexingServiceEngine.PROP_IS_STORE_UUID`), which is the storage-level UUID of its `PersistentVectorStore` (obtained via `AbstractVectorStore.getStoreUUID()`).

On restart the engine calls `installSchemaFromSnapshot`, which:

1. Pre-populates `SchemaTracker` with the saved tables and indexes so that DML entries can be routed to the correct vector store even when the early BookKeeper ledgers that carried the original `CREATE_TABLE` / `CREATE_INDEX` entries have been trimmed by the server's retention policy.
2. Creates each vector store via the configured `VectorStoreFactory`, passing the saved `_is.store.uuid` in `indexProperties`. The factory reads this value and reuses the same UUID instead of generating a fresh one. `PersistentVectorStore` uses this UUID to construct its S3 / local-disk checkpoint path, so `getIndexStatus()` finds the existing checkpoint from the previous run and loads its segments — no DML replay required.
3. Sets the tailer start position to the **watermark LSN** (not `START_OF_TIME`), so only commit-log entries that arrived *after* the checkpoint need to be replayed.

Stores that have no persistent checkpoint state (e.g. `InMemoryVectorStore`) return `null` from `getStoreUUID()`, so `_is.store.uuid` is omitted from the snapshot for those indexes. A subsequent checkpoint with an in-memory store also clears any stale UUID that was loaded from a previous snapshot.

---

## Dynamic Scale-Up of Indexing Service Replicas

The indexing service supports **online scale-up of primary replicas**
(no scale-down). After a rebalance, EVERY existing vector index
spreads new writes across the new owner set — no per-index
permanence, no static-at-CREATE stamping.

### Routing model

For every entry the tailer applies (INSERT / UPDATE / DELETE), the
engine decides per vector index whether this replica owns the key:

```
owner(pk, index) = (XXHash64(pk) % index.numShards) % engine.currentNumInstances
```

`numShards` is per-index, immutable, set at CREATE INDEX time — it
controls hash-bucket granularity, NOT which replica owns a bucket.
`engine.currentNumInstances` is engine-wide and **mutable**: every
`INDEXING_SERVICE_REBALANCE` log entry updates it on the spot, so
from the entry's LSN onward every routing decision uses the new
value.

INSERT / UPDATE / DELETE differ in how they handle a rebalance:

- **INSERT** is filtered: only the current owner under the new
  mapping installs the vector.
- **UPDATE** is split: the `removeVector` half is **broadcast** to
  every replica (so a stale copy left on the previous owner by an
  earlier write is wiped), then the `addVector` half is filtered
  (only the current owner installs the new value).
- **DELETE** is **broadcast** unconditionally. After a rebalance the
  same primary key may briefly sit on two replicas at once — its
  original-owner copy under the old N, and a new-owner copy under
  the new N (e.g. a re-INSERT after the rebalance). A filtered
  DELETE would leak one of them; broadcast guarantees the key
  disappears from everywhere. `removeVector` is a no-op on replicas
  that never had the key.

### EXECUTE INDEXING_SERVICE_REBALANCE

```sql
EXECUTE INDEXING_SERVICE_REBALANCE 'tablespace', N
```

The leader:

1. Snapshots every `Table` and every vector `Index` in the tablespace
   into an `IndexingServiceRebalanceDescriptor` carrying `epoch`,
   `defaultNumInstances=N`, and the schema.
2. Writes the descriptor in a new `INDEXING_SERVICE_REBALANCE` log
   entry (`LogEntryType=15`) and fsyncs.
3. Returns immediately. There is **no ACK protocol** — log ordering
   is the natural barrier.

Re-running with the same `N` is valid and idempotent: each replica
re-records the descriptor for its `lastObservedRebalance` accessor
and the routing value stays unchanged.

### Operator workflow

Scale-up from K to N primaries:

```bash
# 1. Bring up additional pods. Because they have no local state,
#    they boot in JOINING mode (waiting for the next REBALANCE
#    entry to acquire the schema).
helm upgrade herddb herddb-kubernetes/src/main/helm/herddb/ \
    --set indexingService.replicaCount=N

# 2. Update the engine's effective numInstances on every replica
#    (the existing K AND the new K..N-1) by writing one log entry.
herddb-cli.sh -q "EXECUTE INDEXING_SERVICE_REBALANCE 'herd', $N"

# 3. Done. New writes against EVERY existing vector index now spread
#    across all N pods. Search continues to fan out across all
#    replicas as before.
```

Historical on-disk data is not migrated. The original K replicas
keep their phase-1 vectors and continue serving them on search; the
new K..N-1 replicas start owning their share of new writes against
EVERY existing index immediately.

### Behaviour of HerdDB Followers

The new entry type is meaningful only to indexing-service replicas;
HerdDB Followers in classic cluster mode (BookKeeper + ZooKeeper
replication) silently ignore it via the `default: break` clause in
`TableSpaceManager.apply()`. Operators can read the current value
live from any indexing-service replica via
`indexing-admin engine-stats`.

### Indexing-service restart after a rebalance

Recovery uses three complementary mechanisms:

1. **Watermark snapshot.** Every successful indexing-service checkpoint
   persists a `WatermarkSnapshot` through the configured `WatermarkStore`
   (`LocalWatermarkStore` for persistent local volumes, `S3WatermarkStore`
   for ephemeral pods on shared object storage).  The snapshot contains:

   | Field | Purpose |
   |---|---|
   | `LogSequenceNumber lsn` | Resume point for the commit-log tailer |
   | `int numInstances` | Effective routing fan-out at checkpoint time |
   | `List<Table> tables` | Full table definitions at checkpoint time |
   | `List<Index> vectorIndexes` | Vector index definitions, each carrying the property `_is.store.uuid` |

   The binary format for the **local store** is:
   ```
   byte version=1 | long ledgerId | long offset | int numInstances
   | int tableCount  | for each: int len, byte[len] Table
   | int indexCount  | for each: int len, byte[len] Index
   ```
   The **S3 store** uses the same payload with an additional XXHash64
   footer covering all preceding bytes for integrity detection.

   On engine startup the snapshot is loaded and:
   - `currentNumInstances` is set from it, so a freshly-restarted engine
     starts ALREADY at the correct routing value — even if the BK ledger
     that carried the most recent `INDEXING_SERVICE_REBALANCE` entry has
     been trimmed.
   - `SchemaTracker` is pre-populated from `tables` and `vectorIndexes`
     (see [Schema snapshot in the watermark](#schema-snapshot-in-the-watermark-issue-368)).
   - Each vector store is created with the UUID embedded in `_is.store.uuid`
     (see [The `_is.store.uuid` property](#the-_isstoruuid-property) below).
   - The tailer starts from `lsn` rather than `START_OF_TIME`, so only
     entries newer than the checkpoint need to be replayed.

   A snapshot value of `START_OF_TIME` (ledgerId=−1, offset=−1, numInstances=0)
   means "no recovery state"; the engine falls back to its JVM-property
   bootstrap `indexing.cluster.numInstances` and replays the full log.

2. **Log replay (post-watermark only).** After schema is hydrated from the
   snapshot, the tailer tails the commit log starting at the watermark LSN.
   Only entries that arrived *after* the checkpoint are applied — DDL entries
   (CREATE_TABLE, CREATE_INDEX) and any `INDEXING_SERVICE_REBALANCE` entries
   in that window update the live state as usual.  When no snapshot is
   available the tailer starts from `START_OF_TIME` and replays everything.

3. **`_is.store.uuid` — persistent vector store checkpoint recovery.** Each
   `PersistentVectorStore` instance has a storage-level UUID (obtained via
   `AbstractVectorStore.getStoreUUID()`) that is the key segment of every S3
   path that store has ever written to:

   ```
   {tableSpace}/_indexing/{instanceId}/{indexUUID}/...  ← S3 checkpoint data
   ```

   This UUID is auto-generated the *first* time the store is created:
   ```java
   // IndexingServiceEngine — VectorStoreFactory
   String savedUUID = indexProperties.get("_is.store.uuid");   // from WatermarkSnapshot
   String indexUUID = (savedUUID != null) ? savedUUID
                    : indexName + "_" + tableName + "_" + System.nanoTime();
   ```

   The problem it solves: without this mechanism, every restart would generate
   a fresh `nanoTime()` UUID.  `PersistentVectorStore.start()` calls
   `dataStorageManager.getIndexStatus(tableSpaceUUID, freshUUID, ...)` — finds
   nothing — and starts from an empty store.  If the tailer also starts from
   `START_OF_TIME` the data is rebuilt by full replay; but when early BK
   ledgers have been trimmed *and* the watermark LSN is used as the tailer
   start, there is no DML replay to repopulate the store, and every ANN query
   returns empty results.

   The fix: at each checkpoint `checkpointAndSaveWatermark` reads
   `store.getStoreUUID()` for every `PersistentVectorStore` and stores the
   value in the snapshot index's `properties` map under key `_is.store.uuid`.
   On restart the factory reads that property and reuses the same UUID, so
   `getIndexStatus()` locates the existing S3 checkpoint and loads all
   previously-persisted segments.  Only the DML entries that arrived after the
   watermark LSN need to be replayed.

The only situation where none of the three mechanisms is sufficient is a pod
that has BOTH no persistent watermark AND no BK history available (typically a
brand-new pod added during a scale-up after history was trimmed). That is
exactly what the JOINING fallback covers: the engine enters `JOINING`, drops
every commit-log entry, and waits for the next REBALANCE entry — which the
operator triggers via the same `EXECUTE INDEXING_SERVICE_REBALANCE` SQL.

### Transactions across a rebalance

The engine buffers transactional entries in memory until COMMIT
arrives; the buffered entries are then applied through the same
INSERT/UPDATE/DELETE handlers, using the engine's CURRENT
`numInstances` at COMMIT time. Consequence: a transaction begun
pre-rebalance and committed post-rebalance lands every buffered
INSERT on its post-rebalance owner — this is the user's "no change
is lost (even with transactions open during a rebalance)"
guarantee. ROLLBACK discards the buffered entries.

### Bootstrap of joining replicas

A primary replica that has no local state — typically a freshly
added pod after a scale-up — still boots normally as long as the
BookKeeper history (CREATE_TABLE / CREATE_INDEX entries from
start-of-time) is intact: the tailer replays everything and the
SchemaTracker reconstructs the full schema.

When the BookKeeper first ledger has been trimmed and history replay
is impossible, the engine falls back to a **JOINING bootstrap**:

- Set `indexing.bootstrap.fromRebalance=true` (JVM property) on the
  joining pod.
- The engine boots in `EngineStatus.JOINING`, drops every commit-log
  entry that is **not** an `INDEXING_SERVICE_REBALANCE`.
- The operator runs `EXECUTE INDEXING_SERVICE_REBALANCE 'tablespace',
  N` on the leader; the entry rides the WAL to every replica.
- On observing the entry, the joiner installs the embedded schema
  snapshot (Tables + vector Indexes), creates its vector stores,
  bumps its `currentNumInstances`, transitions to
  `EngineStatus.ACTIVE`, and starts processing subsequent log
  entries normally.

The same `EXECUTE` is therefore the trigger for both "rebalance the
existing cluster" and "bootstrap a fresh replica that lost the
history".

### Shadow replicas across rebalance

Shadows do not tail the commit log and are not affected by routing
changes — they continue to mirror their paired primary
(`shadowOf={primaryOrdinal}`) regardless of any REBALANCE entry. A
shadow whose paired primary has not yet published checkpoint state at
boot (e.g. a shadow deployed for a brand-new primary) waits gracefully
via the existing `exists` watcher in `ZookeeperMetadataStorageManager.
installIndexingServiceStateWatch`, and reloads automatically once the
primary publishes its first state.

### Limitations

- **Scale-up only.** There is no online scale-down: removing a primary
  would orphan the historical data its `instanceId` still owns.
- **Historical data is not redistributed.** Old vectors stay on
  their original owners. Search fans out across all replicas, so
  results are complete; but the new pods don't start owning historical
  data until a write touches it.
- **Operator-driven.** The `EXECUTE` is a manual step; there is no
  auto-rebalance triggered by pod-count changes.

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

### Required JVM flags for SIMD performance

jvector's distance-function hot loops rely on the JDK Panama Vector API (and, on
JDK 22+, a native Panama FFM provider backed by AVX/AVX-512 intrinsics). To reach
its fastest code path, the JVM must be launched with specific flags. The
`herddb-services` distribution ships these defaults in `bin/setenv.sh` and in the
Docker / Helm images, so running HerdDB via `bin/service` or the published
container picks them up automatically.

**Mandatory flags (applied by `setenv.sh`):**

| Flag | Where | Purpose |
|------|-------|---------|
| `--add-modules jdk.incubator.vector` | `JAVA_OPTS` (appended unconditionally) | Loads the incubating Vector API module (Panama). Without it jvector falls back to the scalar `DefaultVectorUtilSupport` implementation and logs a warning at startup. |
| `-XX:CompileCommandFile=conf/jvector-compiler-directives` | `JAVA_OPTS` (appended unconditionally) | Force-inlines jvector's vector-distance implementations (`PanamaVectorUtilSupport`, `NativeVectorUtilSupport`, `DefaultVectorUtilSupport`, `VectorUtil`, `cnative.NativeSimdOps`) at every call site so the SIMD intrinsics stay on the fast path inside graph-search inner loops. |
| `--enable-native-access=ALL-UNNAMED` | `JDK_JAVA_OPTIONS` | Required on JDK 22+ for jvector's `NativeVectorUtilSupport` to call the packaged native SIMD library through the Foreign Function & Memory API. |

The Vector API + compile-command pair lives in `JAVA_OPTS` (appended after the
default expansion) so it lands directly on the `java` command line of the
service processes that pass `JAVA_OPTS` through (`server`, `indexing-service`,
`file-server`, `bookkeeper`). Tools that intentionally don't pass `JAVA_OPTS`
through (`herddb-cli.sh`, `herddb-bench.sh`) skip the flags — the CLI doesn't
load jvector and the extra startup noise can interfere with output capture in
some `kubectl exec` / testcontainers environments.

**Compile-command file** — see
`herddb-services/src/main/resources/conf/jvector-compiler-directives` for the full
list of force-inlined classes. The file is packaged into the release zip and
mounted in Docker and Helm-deployed pods automatically.

**Verifying the flags are active.** At startup `java` prints one
`CompileCommand: inline io/github/jbellis/jvector/vector/*.* bool inline = true`
line per directive. If instead you see a warning like

```
Java vector incubator module is not readable. For optimal vector performance,
pass '--add-modules jdk.incubator.vector'...
```

jvector is running in its scalar fallback path — queries will still be correct
but 5–20× slower depending on dataset dimensionality and CPU.

**Customizing JVM options without losing the jvector baseline.** `setenv.sh`
supports two additive env vars so that deployments can add flags without having
to re-specify the defaults:

- `JAVA_OPTS_EXTRA` — appended to `JAVA_OPTS` (heap, GC, -D properties)
- `JDK_JAVA_OPTIONS_EXTRA` — appended to `JDK_JAVA_OPTIONS` (module opens etc.)

The Helm chart exposes these as `server.javaOptsExtra`,
`fileServer.javaOptsExtra`, `bookkeeper.javaOptsExtra` and
`indexingService.javaOptsExtra`. Prefer these over the full-replace
`*.javaOpts` fields — the latter REPLACE the heap/GC/-D baseline. The
jvector-required flags (`--add-modules jdk.incubator.vector` and
`-XX:CompileCommandFile=...`) survive a full-replace `javaOpts` because
`setenv.sh` appends them after the `${JAVA_OPTS:-...}` expansion. Example:

```yaml
# values.yaml — add a larger heap and -XX:+AlwaysPreTouch without losing
# the baseline or the jvector flags.
indexingService:
  enabled: true
  javaOptsExtra: "-Xmx16g -XX:+AlwaysPreTouch"
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
- **Deleted vectors accumulate between checkpoints.** Vectors stay in `VectorStorage` and graph node lists until the next Phase B cleanup; only their ordinals/PKs are masked from results. The Segment Compaction section above describes the follow-up work that reclaims these.
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

