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
│                                                 │
│  Per-index vector stores:                       │
│    ┌───────────────────────────────────────┐    │
│    │ InMemoryVectorStore (brute-force)     │    │
│    │  OR                                   │    │
│    │ PersistentVectorStore (jvector HNSW)  │    │
│    │   - Live graph shards (in-memory)     │    │
│    │   - On-disk segments (FusedPQ)        │    │
│    │   - BLink for PK-to-ordinal mapping   │    │
│    │   - DataStorageManager for persistence│    │
│    │   - MemoryManager for bounded memory  │    │
│    │   - Background compaction thread      │    │
│    └───────────────────────────────────────┘    │
│                                                 │
│  MemoryManager: bounded memory for BLink pages  │
│  DataStorageManager: persistence backend        │
│    - FileDataStorageManager (local disk)        │
│    - MemoryDataStorageManager (testing)          │
│    - (future: RemoteFileService for remote I/O) │
└─────────────────────────────────────────────────┘
```

### Key design decisions

1. **Decoupled via CommitLog tailing.** The IndexingService replays the database WAL independently. The main server's `VectorIndexManager` is a thin client — inserts/updates/deletes are no-ops because the IndexingService consumes them asynchronously from the WAL.

2. **DataStorageManager for persistence.** Vector graph chunks and PK-mapping data are stored as pages via `DataStorageManager.writeIndexPage/readIndexPage`. This reuses HerdDB's existing storage infrastructure and enables future wiring of `RemoteFileService` for distributed storage without changing the vector store code.

3. **BLink for PK mapping.** On-disk segments use `BLink<Bytes, Long>` for PK-to-ordinal lookups, backed by DataStorageManager pages and evicted via `MemoryManager`'s page replacement policy. This bounds memory usage for large on-disk indexes.

4. **Two store implementations.** `AbstractVectorStore` is the common base class. `InMemoryVectorStore` provides brute-force cosine similarity for small datasets or testing. `PersistentVectorStore` uses jvector for production workloads with on-disk persistence.

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
| `maxLiveGraphSize` | integer | `0` | Maximum vectors per live graph shard before rotation. 0 = unlimited. |

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
| Max vector memory | `indexing.memory.vector.limit` | `0` | Max memory for vector data. 0 = auto (50% of JVM heap). |
| Page size | `indexing.memory.page.size` | `1048576` | Logical page size for MemoryManager (1 MB). |
| Vector M | `indexing.vector.m` | `16` | Default M for new vector stores |
| Vector beam width | `indexing.vector.beamWidth` | `100` | Default beam width |
| Vector neighbor overflow | `indexing.vector.neighborOverflow` | `1.2` | Default neighbor overflow |
| Vector alpha | `indexing.vector.alpha` | `1.4` | Default alpha |
| Vector fusedPQ | `indexing.vector.fusedPQ` | `true` | Default FusedPQ enable |
| Max segment size | `indexing.vector.maxSegmentSize` | `2147483648` | Default max segment size |
| Max live graph size | `indexing.vector.maxLiveGraphSize` | `0` | Default max live graph size |
| Compaction interval | `indexing.compaction.interval` | `60000` | Checkpoint interval in ms |
| Compaction threads | `indexing.compaction.threads` | `2` | Background compaction threads |
| Storage type | `indexing.storage.type` | `file` | `file` (persistent) or `memory` (testing) |
| Memory multiplier | `indexing.vector.memoryMultiplier` | `5.0` | Multiplier for memory estimation |

---

## PersistentVectorStore — jvector Implementation

`PersistentVectorStore` extends `AbstractVectorStore` and implements the full jvector-backed HNSW vector index with on-disk persistence.

### Key source files

| File | Module | Purpose |
|------|--------|---------|
| `AbstractVectorStore.java` | herddb-core | Abstract base: `addVector`, `removeVector`, `search`, `size`, `estimatedMemoryUsageBytes`, `start`, `close` |
| `PersistentVectorStore.java` | herddb-core | Full jvector HNSW implementation (2277 lines) |
| `VectorStorage.java` | herddb-core | Lock-free `AtomicReferenceArray<VectorFloat<?>>` for vector data |
| `VectorStorageRandomAccessVectorValues.java` | herddb-core | jvector `RandomAccessVectorValues` adapter over `VectorStorage` |
| `VectorSegment.java` | herddb-core | On-disk segment: `OnDiskGraphIndex`, `BLink<Bytes,Long>`, ordinal-to-PK cache |
| `SegmentedMappedReader.java` | herddb-core | jvector `ReaderSupplier` for memory-mapped graph file reading |
| `FileBackedVectorValues.java` | herddb-core | Abstract disk-backed vector values |
| `ChannelFileBackedVectorValues.java` | herddb-core | FileChannel-based implementation |
| `MmapFileBackedVectorValues.java` | herddb-core | Memory-mapped implementation |
| `InMemoryVectorStore.java` | herddb-indexing-service | Brute-force cosine similarity (for testing / small datasets) |
| `VectorStoreFactory.java` | herddb-indexing-service | Factory interface for creating vector stores |
| `VectorIndexManager.java` | herddb-core | Thin remote client in DBManager (delegates to IndexingService via gRPC) |
| `RemoteVectorIndexService.java` | herddb-core | Interface for remote vector index operations |

### Live graph shards

In-memory inserts go into **LiveGraphShard** instances:

```
LiveGraphShard:
  pkToNode   : ConcurrentHashMap<Bytes, Integer>   // PK → nodeId
  nodeToPk   : ConcurrentHashMap<Integer, Bytes>   // nodeId → PK
  mravv      : RandomAccessVectorValues             // vector accessor
  builder    : GraphIndexBuilder                     // mutable HNSW graph
  vectorCount: AtomicInteger                         // live count
```

Only the **last shard** accepts new inserts. When `maxLiveGraphSize` is exceeded, the shard is sealed and a new empty shard is created (shard rotation). All live shards are searched in parallel.

Vectors are stored in a global `VectorStorage` (lock-free `AtomicReferenceArray<VectorFloat<?>>`), indexed directly by nodeId for O(1) access without boxing or hashing.

### On-disk segments

Each on-disk segment (`VectorSegment`) contains:

- `OnDiskGraphIndex` — FusedPQ graph loaded via memory-mapped `ReaderSupplier`
- `BLink<Bytes, Long>` — PK-to-ordinal mapping, backed by DataStorageManager pages
- Ordinal-to-PK cache — compact `byte[]` arrays (`pkData`, `pkOffsets`, `pkLengths`)
- `graphPageIds` / `mapPageIds` — DataStorageManager page IDs for graph and map chunks
- `estimatedSizeBytes` — memory usage estimate

Segments are sealed when they reach ~80% of `maxSegmentSize`. Sealed segments are not rewritten during compaction.

### Three-phase checkpoint

The checkpoint process ensures DML is never blocked during the expensive Phase B:

**Phase A — Snapshot + Swap** (brief write lock)
```
stateLock.writeLock()
  1. Snapshot current live shards
  2. Classify segments into sealed vs mergeable
  3. Set frozenShards = snapshot (searched during Phase B)
  4. Set pendingCheckpointDeletes = new set
  5. Calculate DML back-pressure cap
  6. Create new empty live shards
  7. dirty = false
stateLock.writeUnlock()
```

**Phase B — Build Graphs** (no lock, I/O-heavy)
```
  1. Collect vectors from snapshot shards + mergeable segments
  2. Build fresh OnHeapGraphIndex via CHECKPOINT_POOL (ForkJoinPool)
  3. Compute ProductQuantization codebook (dim/4 subspaces, 256 clusters)
  4. Write OnDiskGraphIndex with FusedPQ + InlineVectors features
  5. Split into 1 MB chunks → write as DataStorageManager pages
  6. Write PK mapping data as map chunks
```

**Phase C — Load + Swap** (brief write lock)
```
stateLock.writeLock()
  1. Load new segments from DataStorageManager pages
  2. Create BLinks for PK-to-ordinal mapping
  3. Close old frozen shards and merged segments
  4. Apply pending deletes from Phase B
  5. Atomic swap: segments = newSegments
  6. Clear frozen state
stateLock.writeUnlock()
```

### Hybrid search

Search queries all three sources and merges results:

```
search(queryVector, topK):
  results = []

  1. On-disk segments (FusedPQ two-phase scoring):
     for each segment:
       approxSF = view.approximateScoreFunctionFor(qv)  // FusedPQ
       reranker = view.rerankerFor(qv)                   // InlineVectors
       sr = GraphSearcher.search(approxSF + reranker, k, acceptBits)
       map ordinals → PKs via segment.getPkForOrdinal()
       add to results

  2. Live shards (exact in-memory search):
     for each shard:
       sr = GraphSearcher.search(qv, k, mravv, similarityFunction)
       map nodeIds → PKs via shard.nodeToPk
       add to results

  3. Frozen shards (during Phase B):
     same as live shards, with pendingCheckpointDeletes filter

  sort results by score descending
  return top-K
```

### FusedPQ compaction

FusedPQ is activated when ALL conditions are met:
- `fusedPQ = true` in configuration
- `dimension >= 8` (PQ requires at least 8 subspaces)
- `vectors >= 256` (jvector requires exactly 256 PQ clusters)

The compaction builds:
1. `ProductQuantization` codebook: `pqSubspaces = max(1, dim / 4)`, 256 clusters
2. `PQVectors` from encoded training data
3. `OnDiskGraphIndex` via `OnDiskGraphIndexWriter` with:
   - `FusedPQ(maxDegree, pq)` — embeds PQ-encoded neighbor vectors in each node for fast approximate scoring
   - `InlineVectors(dim)` — stores full-precision vectors for exact reranking

When conditions are not met, a simpler checkpoint path writes the `OnHeapGraphIndex` directly.

### Background compaction thread

`PersistentVectorStore` manages its own background daemon thread that periodically calls `checkpoint()` when the index is dirty. The interval is controlled by `compactionIntervalMs` (default 60 seconds). The `IndexingServiceEngine` also runs a `ScheduledExecutorService` that triggers checkpoints across all persistent stores.

### Memory management

- **MemoryManager** provides `PageReplacementPolicy` (ClockPro by default) for BLink page eviction
- **VectorStorage** uses lock-free `AtomicReferenceArray` — no boxing, single volatile read per access
- **Back-pressure during checkpoint**: when Phase B is active, new inserts are limited to `liveVectorCapDuringCheckpoint` vectors. If exceeded, inserts block until Phase C completes.
- `estimatedMemoryUsageBytes()` = live vectors × dimension × 4 bytes × `memoryMultiplier` (default 5.0)

---

## Data Flow

### Write path

```
Client INSERT → HerdDB Server → CommitLog (.txlog)
                                      │
                     ┌────────────────┘
                     ▼
            CommitLogTailer (IndexingServiceEngine)
                     │
                     ▼
            TransactionBuffer (buffer until COMMIT)
                     │
                     ▼
            SchemaTracker (DDL) + VectorStore (DML)
                     │
                     ▼
            PersistentVectorStore.addVector(pk, float[])
              → VectorStorage.set(nodeId, VectorFloat)
              → GraphIndexBuilder.addGraphNode(nodeId, vec)
              → pkToNode.put(pk, nodeId)
```

### Read path

```
Client SELECT ... ORDER BY ann_of(vec, ?) DESC LIMIT k
  → CalcitePlanner intercepts ORDER BY ann_of()
  → VectorANNScanOp.execute()
  → VectorIndexManager.search(queryVector, topK)
  → gRPC call to IndexingService
  → IndexingServiceEngine.search()
  → PersistentVectorStore.search(queryVector, topK)
  → hybrid search: on-disk segments + live shards
  → results returned via gRPC to VectorANNScanOp
  → PK fetch + WHERE filter + projection
```

### Checkpoint path

```
IndexingServiceEngine checkpoint scheduler
  → PersistentVectorStore.checkpoint()
  → Phase A: snapshot live state (brief write lock)
  → Phase B: build FusedPQ graphs, write to DataStorageManager (no lock)
  → Phase C: load new segments, swap (brief write lock)
  → IndexStatus written with metadata + active page IDs
```

---

## Storage Format

All vector index data is stored through `DataStorageManager.writeIndexPage / readIndexPage`. No files outside HerdDB's storage directories are created at rest (temporary files are used during FusedPQ graph building and deleted immediately).

### Page types

```
TYPE_VECTOR_GRAPHCHUNK = 12
  VInt(12) | VInt(chunkLen) | byte[chunkLen]
  // Simple: raw bytes from OnHeapGraphIndex.save()
  // FusedPQ: raw bytes of OnDiskGraphIndex file

TYPE_VECTOR_MAPCHUNK = 13
  VInt(13) | VInt(chunkLen) | byte[chunkLen]
  // Serialized PK/vector mapping data
```

Each chunk is at most 1 MB (`CHUNK_SIZE = 1_048_576`).

### Metadata (IndexStatus.indexData)

Three versions, all big-endian `ByteBuffer`:

**Version 1 — Simple OnHeapGraphIndex:**
```
int version=1, int dimension, int M, int beamWidth,
float neighborOverflow, float alpha, byte addHierarchy,
int nextNodeId, int numGraphChunks, long[] graphPageIds,
int numMapChunks, long[] mapPageIds
```

**Version 2 — FusedPQ OnDiskGraphIndex:**
```
Same as v1, plus: byte fusedPQ (after addHierarchy)
```

**Version 3 — Multi-segment:**
```
int version=3, int dimension, int M, int beamWidth,
float neighborOverflow, float alpha, byte addHierarchy,
byte fusedPQ, int nextNodeId, int numSegments,
for each segment:
  int segmentId, long estimatedSize,
  int numGraphChunks, long[] graphPageIds,
  int numMapChunks, long[] mapPageIds
```

### Map data format

```
int entryCount
for each entry:
  int ordinal (nodeId or newOrdinal)
  int pkLen
  byte[] pkBytes
  int floatCount (= dimension)
  float[] floats (big-endian IEEE 754)
```

### BLink pages

Each on-disk segment creates a `BLink<Bytes, Long>` for PK-to-ordinal mapping. BLink pages are stored as separate DataStorageManager index pages with a per-segment namespace: `{indexUUID}_seg{segmentId}_pktonode`.

---

## jvector Integration

### Library

```xml
<dependency>
    <groupId>io.github.jbellis</groupId>
    <artifactId>jvector</artifactId>
    <version>4.0.0-rc.9-SNAPSHOT</version>
</dependency>
```

### Key jvector types used

| jvector class | Role |
|---------------|------|
| `GraphIndexBuilder` | Mutable builder for incremental inserts/deletes |
| `OnHeapGraphIndex` | In-memory Vamana graph |
| `OnDiskGraphIndex` | Immutable on-disk graph with FusedPQ support |
| `OnDiskGraphIndexWriter` | Writes graph to file with feature suppliers |
| `FusedPQ` | Embeds PQ-encoded neighbor vectors for fast approximate scoring |
| `InlineVectors` | Stores full-precision vectors for exact reranking |
| `ProductQuantization` | Computes PQ codebook from training vectors |
| `PQVectors` | PQ-encoded vectors for FusedPQ state |
| `DefaultSearchScoreProvider` | Combines approximate + exact scoring for two-phase reranking |
| `BuildScoreProvider` | Scoring during graph construction |
| `VamanaDiversityProvider` | Controls diversity criterion (alpha) |
| `GraphSearcher` | Executes search on a graph index |
| `VectorSimilarityFunction` | `COSINE`, `EUCLIDEAN`, `DOT_PRODUCT` |
| `VectorFloat<?>` / `VectorTypeSupport` | Vector data types with SIMD acceleration |

---

## SQL Integration

### The `ann_of` function

`ann_of(vectorColumn, queryVector)` is a scalar SQL function returning cosine similarity between two float arrays. Registered as a Calcite `ScalarFunction`.

### Planner interception

For `SELECT … ORDER BY ann_of(col, ?) DESC LIMIT k`:

1. `CalcitePlanner.planSort()` detects `ORDER BY ann_of()` pattern
2. Creates `VectorANNScanOp` with compiled query vector expression
3. `VectorANNScanOp.execute()` calls `VectorIndexManager.search()` (which delegates via gRPC)
4. Fetches rows by PK, applies WHERE filter, projects columns

### WITH clause parsing

`JSQLParserPlanner.extractIndexWithClause()` pre-processes the SQL to strip `WITH key=value ...` suffix before JSQLParser sees it, storing properties in a `ThreadLocal`. `buildCreateIndexStatement()` reads and applies them to the `Index.Builder`.

---

## IndexingServiceEngine

The engine is the core component of the standalone IndexingService. It:

1. **Tails the CommitLog** via `CommitLogTailer` — reads `.txlog` files from the database's WAL directory
2. **Buffers transactions** via `TransactionBuffer` — delays DML application until COMMIT
3. **Tracks schema** via `SchemaTracker` — processes DDL entries (CREATE/DROP TABLE/INDEX)
4. **Manages vector stores** — creates `AbstractVectorStore` instances per vector index via `VectorStoreFactory`
5. **Persists watermark** via `WatermarkStore` — tracks last processed LSN for restart recovery
6. **Runs periodic checkpoints** via `ScheduledExecutorService` — triggers `PersistentVectorStore.checkpoint()` on all persistent stores

### Storage type selection

On `start()`, the engine reads `indexing.storage.type` from configuration:
- `"file"` (default): creates `PersistentVectorStore` instances backed by `FileDataStorageManager`
- `"memory"`: creates `InMemoryVectorStore` instances (brute-force, for testing)

---

## Known Limitations

- **LIMIT not pushed into ANN search.** `VectorANNScanOp` currently queries with `topK = Integer.MAX_VALUE`.
- **WHERE filtering is post-fetch.** All ANN candidates are fetched by PK before WHERE is tested.
- **FusedPQ requires ≥ 256 vectors.** Smaller indexes use the simpler OnHeapGraphIndex format.
- **Single sort key only.** Multi-column ORDER BY and joins fall through to brute-force.
- **Deleted vectors accumulate between checkpoints.** Vectors stay in `VectorStorage` until checkpoint cleanup.

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
