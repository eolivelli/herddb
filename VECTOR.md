# Vector Index in HerdDB

This document describes the vector index feature: how to use it, its design, storage format, and implementation details.

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
-- Top-10 most similar documents to a query vector
SELECT id, title
FROM tblspace1.documents
ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC
LIMIT 10;

-- Combined WHERE filter + ANN search
SELECT id, title
FROM tblspace1.documents
WHERE category = ?
ORDER BY ann_of(vec, CAST(? AS FLOAT ARRAY)) DESC
LIMIT 10;
```

When a vector index exists on `vec`, the `ORDER BY ann_of(…) DESC LIMIT k` pattern is automatically routed through the index. Without a vector index, the query falls back to brute-force cosine similarity over a full table scan.

---

## Index Creation Parameters

All parameters are optional. Unspecified parameters use the defaults shown below.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `m` | integer | `16` | Maximum number of edges per graph node. Higher = better recall, more memory, slower build. Typical range: 8–32. |
| `beamWidth` | integer | `100` | Beam width (candidates explored) when inserting a new node during build. Higher = better graph quality, slower inserts. Typical range: 50–400. |
| `neighborOverflow` | float | `1.2` | Temporary degree overflow factor. Each node may temporarily hold `m × neighborOverflow` neighbours during construction before pruning. Must be ≥ 1.0. |
| `alpha` | float | `1.4` | Diversity criterion aggressiveness. Values > 1.0 allow longer edges, which improves recall on clustered data. `1.0` = pure short-edge pruning (HNSW-like). |
| `similarity` | string | `cosine` | Distance metric used for both build and search. Accepted values: `cosine`, `euclidean`, `dot`. Must match the metric used at query time. |
| `fusedPQ` | boolean | `true` | When `true` and the vector dimension is ≥ 8 and the index has ≥ 256 vectors, checkpoints use jvector's FusedPQ on-disk format (faster approximate scoring at search time). Falls back to the legacy `OnHeapGraphIndex` format otherwise. |

**Syntax:**

```sql
CREATE VECTOR INDEX <name> ON <tablespace>.<table>(<column>)
  [WITH <key>=<value> [<key>=<value> …]];
```

Key-value pairs are separated by whitespace or commas. No surrounding parentheses are required.

**Constraints:**
- The indexed column must be of type `floata` (nullable or `NOT NULL`).
- Exactly one column may be indexed per vector index.
- `UNIQUE` is not supported and raises an error at definition time.
- Parameters are stored in the index definition and survive checkpoint/restart.

---

## Overview

HerdDB's vector index is backed by [jvector](https://github.com/jbellis/jvector) (version `4.0.0-rc.9-SNAPSHOT`), a Java library implementing a Vamana-style approximate nearest-neighbour (ANN) graph index (similar in spirit to HNSW but with a more aggressive diversity criterion).

The implementation covers:

- DDL: `CREATE VECTOR INDEX` with optional `WITH` hyperparameter clause
- DML write path: every `INSERT` / `UPDATE` / `DELETE` updates the index
- Checkpoint persistence: serialises the graph to HerdDB's page storage
- Restart recovery: reloads the graph from pages, with two formats (legacy and FusedPQ)
- Query execution: `ann_of(col, ?)` — CalcitePlanner intercepts `ORDER BY ann_of() DESC LIMIT k` and routes through `VectorANNScanOp` when a vector index exists; falls back to brute-force cosine similarity otherwise
- Hybrid search: after a FusedPQ checkpoint, newly inserted rows are held in an in-memory graph alongside the on-disk graph; both are searched and results are merged by score

---

## Key Source Files

| File | Purpose |
|------|---------|
| `herddb-core/src/main/java/herddb/model/Index.java` | Adds `TYPE_VECTOR = "vector"` constant; `Builder.build()` enforces FLOATARRAY / no-UNIQUE constraints; stores `Map<String,String> properties` (serialised as version-2 index format) |
| `herddb-core/src/main/java/herddb/sql/JSQLParserPlanner.java` | `convertIndexType()` accepts `"vector"`; `extractIndexWithClause()` strips the `WITH` suffix before JSQLParser sees it and stores properties in a ThreadLocal; `buildCreateIndexStatement()` reads the ThreadLocal and applies properties to the builder |
| `herddb-core/src/main/java/herddb/index/vector/VectorIndexManager.java` | Full index manager; configurable hyper-parameters from `index.properties`; FusedPQ checkpoint/load/hybrid-search logic |
| `herddb-core/src/main/java/herddb/core/TableSpaceManager.java` | `bootIndex()` switch has a `case Index.TYPE_VECTOR` branch |
| `herddb-core/src/main/java/herddb/sql/functions/BuiltinFunctions.java` | Declares `ANN_OF = "ann_of"` and `NAME_ANN_OF = "ANN_OF"` constants |
| `herddb-core/src/main/java/herddb/sql/CalcitePlanner.java` | Registers `ANN_OF` scalar function; `planSort()` intercepts `ORDER BY ann_of()` to create `VectorANNScanOp` |
| `herddb-core/src/main/java/herddb/sql/expressions/SQLExpressionCompiler.java` | `case BuiltinFunctions.NAME_ANN_OF` dispatches to `CompiledFunction(ANN_OF, …)` |
| `herddb-core/src/main/java/herddb/sql/expressions/CompiledFunction.java` | `case BuiltinFunctions.ANN_OF` evaluates cosine similarity |
| `herddb-core/src/main/java/herddb/model/planner/VectorANNScanOp.java` | `PlannerOp` that performs index-backed ANN search with optional brute-force fallback |
| `herddb-core/src/test/java/herddb/core/indexes/VectorIndexTest.java` | Integration tests (DDL, DML, checkpoint/restart, `search()`, `ann_of`, WITH clause, FusedPQ, hybrid search) |

---

## How jvector Is Used

### Library coordinates

```xml
<dependency>
    <groupId>io.github.jbellis</groupId>
    <artifactId>jvector</artifactId>
    <version>4.0.0-rc.9-SNAPSHOT</version>
</dependency>
```

The jar must be present in the local Maven repository (not published to Maven Central at the snapshot stage).

### Core jvector types used

| jvector class | Role in HerdDB |
|---------------|----------------|
| `GraphIndexBuilder` | Mutable builder for incremental inserts and deletes; wraps `OnHeapGraphIndex` |
| `OnHeapGraphIndex` | In-memory Vamana graph; serialised via `save(DataOutput)` (legacy format) |
| `OnDiskGraphIndex` | Immutable on-disk graph; loaded via `ReaderSupplier → ByteBufferReader`; supports FusedPQ approximate scoring |
| `OnDiskGraphIndexWriter` | Writes an `OnHeapGraphIndex` to a file with optional feature suppliers (`FusedPQ`, `InlineVectors`) |
| `FusedPQ` | On-disk feature that fuses PQ-encoded neighbour vectors into each node's record for fast approximate scoring without random I/O |
| `InlineVectors` | On-disk feature that stores the full-precision vector alongside each node for exact reranking |
| `ProductQuantization` | Computes a 256-cluster PQ codebook from training vectors; used to encode neighbours for FusedPQ |
| `PQVectors` | Holds PQ-encoded vectors; used as a state supplier when writing FusedPQ data |
| `DefaultSearchScoreProvider` | Combines an `ApproximateScoreFunction` (FusedPQ) with an `ExactScoreFunction` (InlineVectors) for two-phase reranking |
| `MapRandomAccessVectorValues` | `ConcurrentHashMap<Integer, VectorFloat<?>>` wrapper for vector access by node ID |
| `BuildScoreProvider` | Provides similarity scoring during graph construction |
| `VamanaDiversityProvider` | Controls the graph's diversity criterion (alpha parameter); required when loading a legacy graph |
| `ByteBufferReader` | In-memory `RandomAccessReader`; used to load graphs from a `ByteBuffer` without touching the filesystem |
| `VectorTypeSupport` / `VectorizationProvider` | Factory for `VectorFloat<?>` objects from `float[]` arrays |
| `VectorSimilarityFunction` | Enum for `COSINE`, `EUCLIDEAN`, `DOT_PRODUCT`; configurable per index |

---

## Lifecycle

### Fresh start (empty index)

```
recordInserted(pk, indexKeyBytes)
  → decode float[] from indexKeyBytes via Bytes.to_float_array()
  → on first insert: create MapRandomAccessVectorValues + GraphIndexBuilder
  → assign nodeId = nextNodeId.getAndIncrement()
  → put vector in: vectors map, pkToNode map, nodeToPk map
  → builder.addGraphNode(nodeId, vectorFloat)
```

### Checkpoint — legacy path (fusedPQ=false, or dim < 8, or < 256 vectors)

```
builder.cleanup()
  → removes marked-deleted nodes; sets allMutationsCompleted=true
OnHeapGraphIndex.save(DataOutputStream)
  → raw bytes split into ≤ 1 MB chunks → written as TYPE_VECTOR_GRAPHCHUNK pages
serializeMapData()
  → (nodeId, pkBytes, floatValues) for each live node
  → split into ≤ 1 MB chunks → written as TYPE_VECTOR_MAPCHUNK pages
write IndexStatus
  → indexData = binary metadata (version=1, see Storage Format)
  → activePages = set of all chunk page IDs
```

### Checkpoint — FusedPQ path (fusedPQ=true, dim ≥ 8, ≥ 256 vectors)

```
Merge all active vectors (on-disk ordinals + live inserts):
  → read on-disk vectors via view.getVector(ordinal) [InlineVectors feature]
  → combine with vectors from live GraphIndexBuilder
Build fresh merged OnHeapGraphIndex from all active vectors
Compute PQ codebook:
  → pqSubspaces = max(1, dim / 4)
  → ProductQuantization.compute(allVectors, pqSubspaces, 256, true)
  → pq.encodeAll(...) → PQVectors
Write to temp file via OnDiskGraphIndexWriter.Builder:
  → .with(new FusedPQ(maxDegree, pq))
  → .with(new InlineVectors(dim))
  → writer.write(suppliers)  // FUSED_PQ + INLINE_VECTORS state per ordinal
Read temp file bytes → split into ≤ 1 MB chunks → TYPE_VECTOR_GRAPHCHUNK pages
Compute sequential renumbering (oldNodeId → newOrdinal)
writeFusedPQMapData():
  → (newOrdinal, pkBytes, floatValues) per active node
  → TYPE_VECTOR_MAPCHUNK pages
write IndexStatus
  → indexData = binary metadata (version=2, fusedPQ flag=1)
Delete temp file
```

### Restart / recovery — legacy format (version=1)

```
getIndexStatus(sequenceNumber)
  → read binary metadata (version=1)
  → reassemble map chunks → deserialise (nodeId, pkBytes, float[]) entries
    → populate vectors, pkToNode, nodeToPk maps
  → reassemble graph chunks → ByteBuffer → ByteBufferReader
    → OnHeapGraphIndex.load(reader, dim, neighborOverflow, diversityProvider)
    → new GraphIndexBuilder(bsp, dim, loadedGraph, ...)
```

### Restart / recovery — FusedPQ format (version=2, fusedPQ flag=1)

```
getIndexStatus(sequenceNumber)
  → read binary metadata (version=2, fusedPQ=true)
  → reassemble map chunks → deserialise (newOrdinal, pkBytes, float[]) entries
    → populate onDiskNodeToPk, onDiskPkToNode maps
  → reassemble graph chunks → byte[]
    → ReaderSupplier: () -> new ByteBufferReader(ByteBuffer.wrap(graphBytes))
    → OnDiskGraphIndex.load(readerSupplier)   → this.onDiskGraph
    → keep graphBytes reference in this.onDiskGraphBytes
  → create empty live GraphIndexBuilder for new inserts
    → nextNodeId set to maxOnDiskOrdinal + 1
```

### Hybrid search (after FusedPQ load)

```
search(queryVector, topK):
  1. If onDiskGraph != null and onDiskNodeToPk is non-empty:
       activeOrdinals = onDiskNodeToPk.keySet()   // excludes deleted on-disk nodes
       acceptBits = activeOrdinals::contains
       approxSF = view.approximateScoreFunctionFor(qv, similarityFunction)  // FusedPQ
       reranker  = view.rerankerFor(qv, similarityFunction)                 // InlineVectors
       ssp = new DefaultSearchScoreProvider(approxSF, reranker)
       sr  = GraphSearcher.search(ssp, k, acceptBits)
       map ordinals → PKs via onDiskNodeToPk
  2. If builder != null and nodeToPk is non-empty:
       GraphSearcher.search(qv, k, mravv, similarityFunction, graph, Bits.ALL)
       map nodeIds → PKs via nodeToPk
  3. Merge both result lists, sort by score descending, return top-K
```

### Delete

```
recordDeleted(pk, indexKeyBytes)
  → check onDiskPkToNode: if found, remove from onDiskPkToNode + onDiskNodeToPk
    // on-disk graph is immutable; deleted ordinals are filtered at search time via acceptBits
  → check pkToNode: if found, remove from pkToNode + nodeToPk
    → builder.markNodeDeleted(nodeId)
    // vector stays in vectors map until next checkpoint cleanup()
```

### Update

```
recordUpdated(pk, oldIndexKey, newIndexKey)
  → recordDeleted(pk, oldIndexKey)
  → recordInserted(pk, newIndexKey)   // assigns a new nodeId
```

---

## WITH Clause Parsing

JSQLParser 3.2 does not support arbitrary trailing syntax on `CREATE INDEX` statements. The `WITH` clause is therefore handled by pre-processing the SQL string in `JSQLParserPlanner.translate()` before JSQLParser is invoked:

1. `extractIndexWithClause(query)` scans the query string for `WITH` appearing outside of parentheses (i.e., after the column list).
2. When found, the key=value pairs are parsed (whitespace around `=` is normalised; tokens are split on whitespace/commas).
3. The parsed properties are stored in a `ThreadLocal<Map<String,String>>` (`PENDING_INDEX_PROPERTIES`).
4. The query is returned with the `WITH …` suffix stripped.
5. JSQLParser parses the clean query.
6. `buildCreateIndexStatement()` reads and clears the ThreadLocal, applying properties to the `Index.Builder`.
7. A `try/finally` in `translate()` guarantees the ThreadLocal is always cleared even on parse errors.

---

## Storage Format

The index is stored entirely through HerdDB's `DataStorageManager` page mechanism (same infrastructure used by BRIN indexes). No files outside HerdDB's tablespace directories are created at rest.

> **Note:** The FusedPQ checkpoint path writes a temporary file (via `Files.createTempFile`) to satisfy `OnDiskGraphIndexWriter.Builder`'s file-path requirement. The file is read immediately after writing and deleted before the checkpoint returns.

### Pages

Two page types, each prefixed with a VInt type tag and a VInt length:

```
TYPE_VECTOR_GRAPHCHUNK = 12
  VInt(12) | VInt(chunkLen) | byte[chunkLen]
  // Legacy: raw bytes from OnHeapGraphIndex.save()
  // FusedPQ: raw bytes of the OnDiskGraphIndex file

TYPE_VECTOR_MAPCHUNK = 13
  VInt(13) | VInt(chunkLen) | byte[chunkLen]
  // Pieces of the serialised pk/vector map (see below)
```

### Metadata (stored in `IndexStatus.indexData`)

A flat big-endian `ByteBuffer`:

**Version 1 (legacy):**
```
int    version           = 1
int    dimension
int    M
int    beamWidth
float  neighborOverflow
float  alpha
byte   addHierarchy      (0 = false, reserved)
int    nextNodeId
int    numGraphChunks
long[] graphChunkPageIds
int    numMapChunks
long[] mapChunkPageIds
```

**Version 2 (FusedPQ):**
```
int    version           = 2
int    dimension
int    M
int    beamWidth
float  neighborOverflow
float  alpha
byte   addHierarchy      (0 = false, reserved)
byte   fusedPQ           (1 = FusedPQ format, 0 = legacy)
int    nextNodeId
int    numGraphChunks
long[] graphChunkPageIds
int    numMapChunks
long[] mapChunkPageIds
```

### Map data — legacy format

```
int    entryCount
for each entry:
  int    nodeId
  int    pkLen
  byte[] pkBytes
  int    floatCount   (= dimension)
  float[] floats      (big-endian IEEE 754)
```

### Map data — FusedPQ format

```
int    entryCount
for each entry:
  int    newOrdinal   // sequential ordinal assigned by OnDiskGraphIndexWriter
  int    pkLen
  byte[] pkBytes
  int    floatCount   (= dimension)
  float[] floats      (big-endian IEEE 754)
```

The vectors are stored here for reference but are also embedded in the on-disk graph via `InlineVectors`. On load, the map data is used only to reconstruct the `onDiskNodeToPk` / `onDiskPkToNode` mappings.

### Graph data

- **Legacy:** raw bytes from `OnHeapGraphIndex.save(DataOutput)`. Internal jvector format (versioned, magic = `OnHeapGraphIndex.MAGIC`).
- **FusedPQ:** raw bytes of a complete `OnDiskGraphIndex` file written by `OnDiskGraphIndexWriter`. Loaded via `OnDiskGraphIndex.load(ReaderSupplier)`.

---

## Index Properties — Serialisation (Index.java version 2)

`Index.properties` is a `Map<String,String>` stored in the index definition byte array. Version 1 index files (written before this feature) have an empty map on deserialisation.

```
Version 1 format (old):
  VLong(1) | VLong(0) | tablespace | name | uuid | table | propertyBits | type | columns...

Version 2 format (current):
  VLong(2) | VLong(0) | tablespace | name | uuid | table | propertyBits | type | columns...
  VInt(numProps)
  for each property: UTF(key) | UTF(value)
```

---

## Node-ID Mapping

jvector uses integer node IDs (0-based). HerdDB primary keys are arbitrary byte sequences (`Bytes`). Two separate mapping domains exist:

**Live nodes** (in-memory, since last checkpoint):
```
pkToNode  : ConcurrentHashMap<Bytes, Integer>   // pk → nodeId
nodeToPk  : ConcurrentHashMap<Integer, Bytes>   // nodeId → pk
vectors   : ConcurrentHashMap<Integer, VectorFloat<?>>
```

**On-disk nodes** (loaded from FusedPQ checkpoint):
```
onDiskPkToNode  : ConcurrentHashMap<Bytes, Integer>   // pk → sequential ordinal
onDiskNodeToPk  : ConcurrentHashMap<Integer, Bytes>   // sequential ordinal → pk
```

Live node IDs are assigned from `AtomicInteger nextNodeId` (starts at `maxOnDiskOrdinal + 1` after a FusedPQ load, to avoid collisions). Node IDs are never reused.

`getNodeCount()` returns `nodeToPk.size() + onDiskNodeToPk.size()`.

---

## Query Execution — `ann_of` and the Vector Index Path

### The `ann_of` function

`ann_of(vectorColumn, queryVector)` is a scalar SQL function that returns the cosine similarity between two float arrays. It is registered as a Calcite `ScalarFunction`. Without a vector index it produces the exact cosine value for every scanned row; with a vector index it triggers the `VectorANNScanOp` path.

The similarity function used at query time always matches the one stored in the index definition (`vector.similarity` property, default `cosine`).

### Calcite plan tree

For `SELECT … ORDER BY ann_of(col, ?) DESC LIMIT k`:

```
EnumerableLimit
  EnumerableProject        ← strips the hidden ann_of column if not in SELECT
    EnumerableSort         ← ORDER BY the ann_of result
      EnumerableProject    ← adds ann_of(col, ?) as an extra column
        BindableTableScan  ← full scan (may carry WHERE filters)
```

### Planner interception — `planSort()`

`CalcitePlanner.planSort()` intercepts the `EnumerableSort` when:
- There is exactly one sort key
- The sort key is a `RexCall` with operator name `ANN_OF`
- The immediate input is a `BindableTableScan`

It then builds a `VectorANNScanOp` with a compiled query-vector expression, an optional compiled WHERE predicate, and a brute-force fallback `SortOp`.

### `VectorANNScanOp` execution

1. Finds the `VectorIndexManager` for the target column (if none exists, delegates to the brute-force fallback).
2. Evaluates the query vector expression.
3. Calls `VectorIndexManager.search(queryVector, Integer.MAX_VALUE)` → ordered `List<(Bytes pk, Float score)>`.
4. For each `(pk, score)`: fetches the row by PK, applies the optional WHERE predicate, projects the row, appends the score.
5. Returns a `ScanResult` in ANN order — no further sort needed.

### WHERE + ANN

`VectorANNScanOp` applies the WHERE predicate after the PK fetch. The index is queried with `topK = Integer.MAX_VALUE` to ensure enough candidates are returned before filtering. A future optimisation could pass the LIMIT (with oversampling) down to reduce the number of PKs fetched.

---

## Known Limitations and Future Work

### LIMIT not pushed into ANN search

`VectorANNScanOp` always calls `vim.search(queryVector, Integer.MAX_VALUE)`. Passing the LIMIT (with an oversampling factor) would reduce unnecessary PK fetches and row reads.

### WHERE filtering is post-fetch

All candidates from the index are fetched by PK before the WHERE predicate is tested. For selective predicates this causes unnecessary row reads.

### Only `ORDER BY ann_of(col, ?) [DESC|ASC]` is intercepted

The pattern requires exactly one sort key, `RexCall("ANN_OF")`, and a `BindableTableScan` directly below the inner project. Multi-table joins, sub-queries, and multi-column ORDER BY fall through to brute-force.

### FusedPQ requires ≥ 256 vectors

jvector's `FusedPQ` currently requires exactly 256 PQ clusters. Checkpoints with fewer than 256 active vectors automatically fall back to the legacy `OnHeapGraphIndex` format. The FusedPQ path becomes active for subsequent checkpoints once the threshold is met.

### Deleted-node accumulation between checkpoints

Deleted live nodes are removed from `pkToNode`/`nodeToPk` immediately but their vectors stay in the `vectors` map until `builder.cleanup()` runs at checkpoint time.

### Concurrent access

`MapRandomAccessVectorValues` is thread-safe. `addGraphNode` is safe to call concurrently. `cleanup()` is `synchronized` in `GraphIndexBuilder` and must not be called concurrently with inserts.

### Java version

The project targets Java 8. The HerdDB integration code avoids `var` and other post-Java-8 constructs even though jvector itself uses them internally.

### @Deprecated / @Experimental jvector APIs

`OnHeapGraphIndex.save()` / `load()` and the `GraphIndexBuilder` constructor that accepts an existing `MutableGraphIndex` are marked deprecated/experimental in jvector 4.x. They are stable within the 4.x series but should be reviewed when upgrading jvector.
