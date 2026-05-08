# Remote File Service

The **Remote File Service** is a storage backend for HerdDB that stores table and index page data on remote object-store-like gRPC servers, while keeping all metadata (checkpoint files, table/index schemas, transaction logs) on local disk. Multiple remote servers are supported with consistent hashing (Murmur3) for load distribution.

---

## Architecture

```
HerdDB Server (DBManager)
  └── RemoteFileDataStorageManager
        ├── Local metadata dir  ← checkpoint files, schemas, transaction records
        │     ├── {tableSpace}.tablespace/
        │     │     ├── checkpoint.{ledger}.{offset}.checkpoint
        │     │     ├── tables.{ledger}.{offset}.tablesmetadata
        │     │     ├── indexes.{ledger}.{offset}.tablesmetadata
        │     │     ├── transactions.{ledger}.{offset}.tx
        │     │     ├── {uuid}.table/
        │     │     │     └── {ledger}.{offset}.checkpoint
        │     │     └── {uuid}.index/
        │     │           └── {ledger}.{offset}.checkpoint
        └── RemoteFileServiceClient
              ├── ConsistentHashRouter (Murmur3)
              ├── gRPC channel → RemoteFileServer A  (port 9846)
              └── gRPC channel → RemoteFileServer B  (port 9847)
```

### Remote path convention

Page data is stored on remote servers using the following path scheme:

| Data type   | Remote path                               |
|-------------|-------------------------------------------|
| Table page  | `{tableSpace}/{uuid}/data/{pageId}.page`  |
| Index page  | `{tableSpace}/{uuid}/index/{pageId}.page` |

The server that stores each file is chosen by hashing the full path with Murmur3 on a consistent hash ring, so routing is deterministic and stable.

### What stays local

All metadata remains on the local filesystem, in the same format as `FileDataStorageManager`:

- **Tablespace checkpoint sequence numbers** — which log position was last checkpointed
- **Table/index schema snapshots** — serialized `Table` and `Index` descriptors at each checkpoint
- **Transaction records** — in-flight transaction state at checkpoint time
- **Table and index checkpoint status files** — the set of active page IDs and next-page-id counter at each checkpoint

This means the local disk must be durable and survive restarts; the remote servers hold only page data and can be considered a large page cache/store.

---

## Maven modules

### `herddb-remote-file-service`

New module containing everything needed to run and use the remote file service:

| Class | Description |
|-------|-------------|
| `RemoteFileServer` | Standalone Netty server (issue #425). Stores files in a local directory or S3. Configurable bind host and port. Hosts both data-plane and admin RPCs on the same socket. |
| `RemoteFileServiceImpl` | Stateless dispatcher: turns inbound file-server PDUs into `ObjectStorage` calls. Writes are atomic (temp file + rename). |
| `RemoteFileServerSideConnection` | Per-connection `ChannelEventListener`. Tracks OIDC SASL auth state and routes inbound PDUs to the data dispatcher or the admin dispatcher. |
| `RemoteFileServiceClient` | Client that manages one read-plane and one write-plane Netty `Channel` per file server (issue #100), routes requests via `ConsistentHashRouter`, and performs an OIDC OAUTHBEARER SASL handshake on every freshly-opened channel when an OIDC token supplier is configured. `listFiles` and `deleteByPrefix` fan out to all servers. |
| `ConsistentHashRouter` | Murmur3-based consistent hash ring with 150 virtual nodes per server for balanced distribution. |
| `RemoteFileDataStorageManager` | `DataStorageManager` implementation. Delegates page I/O to `RemoteFileServiceClient`; delegates all metadata I/O to an internal `FileDataStorageManager`. |

**Dependency:** Java 11, `herddb-net` (length-prefixed Netty wire framework), `herddb-utils` (PDU codec). No gRPC, no protobuf.

---

## Wire protocol

Built on top of the same length-prefixed Netty framing that `herddb-net` uses
for HerdDB core client/server communication (issue #425). Every PDU carries
a 1-byte version, a 1-byte flags field (`FLAGS_ISREQUEST`/`FLAGS_ISRESPONSE`),
a 1-byte type, and an 8-byte messageId for request/response correlation.
File-server PDU types live in the **`50..69`** range so the service the
client is talking to is identifiable from a single type-byte read on the
wire (HerdDB core types occupy `0..25` and `100..104`).

| Type | RPC | Notes |
|------|-----|-------|
| `50` | `TYPE_FS_WRITE_FILE` | request: `path`, `content`. Response: `writtenSize`. |
| `51` | `TYPE_FS_WRITE_FILE_BLOCK` | request: `path`, `blockIndex`, `content`. Response: `writtenSize`. |
| `52` | `TYPE_FS_READ_FILE` | request: `path`. Response: `found`, `content`. |
| `53` | `TYPE_FS_READ_FILE_RANGE` | request: `path`, `offset`, `length`, `blockSize`. Response: `found`, `content`. |
| `54` | `TYPE_FS_DELETE_FILE` | request: `path`. Response: `deleted`. |
| `55` | `TYPE_FS_DELETE_FILES` | request: `paths[]`. Response: per-path `outcomes[]` (issue #398). |
| `56` | `TYPE_FS_LIST_FILES` | request: `prefix`. Response: `paths[]` (single PDU; client dedupes when fanning out). |
| `57` | `TYPE_FS_DELETE_BY_PREFIX` | request: `prefix`. Response: `deletedCount`. |
| `60` | `TYPE_FS_GET_SERVER_INFO` | admin: returns identity + JVM + cache stats (issue #336). |
| `61` | `TYPE_FS_RESIZE_DISK_CACHE` | admin: resize disk-cache LRU at runtime (issue #336). |

The codec lives next to the core PDUs in
`herddb-utils/src/main/java/herddb/proto/PduCodec.java`, so the file
server and HerdDB core share the same framing, message-correlation, and
SASL plumbing. A follow-up issue (#426) will extend `ReadFile` /
`ReadFileRange` to use `DefaultFileRegion` for zero-copy disk reads on
the local-storage backend.

## Authentication

OIDC OAUTHBEARER, mediated by SASL on the new wire protocol:

- Client side: `RemoteFileServiceClient` receives a `Supplier<String>` returning a JWT bearer token. On every freshly-opened `Channel` the client performs a one-round `OAuthBearerSaslClient` handshake (`TYPE_SASL_TOKEN_MESSAGE_REQUEST` / `TYPE_SASL_TOKEN_SERVER_RESPONSE`).
- Server side: `RemoteFileServerSideConnection` instantiates `OAuthBearerSaslServer` with a callback handler that delegates to `OidcTokenValidator`. Until the handshake completes, only SASL PDUs are accepted; data-plane PDUs are rejected with `TYPE_ERROR`.
- Local-VM channel and OIDC-disabled mode bypass the handshake entirely.

---

## Page serialization format

Page data on the remote servers uses the same binary format as `FileDataStorageManager`, ensuring compatibility and making it possible to migrate data:

**Data page:**
```
VLong version = 1
VLong flags   = 0
Int   numRecords
for each record:
    Array key
    Array value
Long  XXHash64 of everything above
```

**Index page:**
```
VLong version = 1
VLong flags   = 0
<index-type-specific bytes written by DataWriter>
Long  XXHash64 of everything above
```

---

## Consistent hashing

`ConsistentHashRouter` builds a sorted hash ring with **150 virtual nodes per server**. For each write or read, the path string is hashed with Murmur3-32 and mapped to the nearest clockwise node on the ring.

- **Deterministic:** the same path always maps to the same server for a given server list.
- **Fan-out for prefix ops:** `listFiles` and `deleteByPrefix` are sent to all servers because files sharing a prefix may live on different servers.
- **No rebalancing:** the ring is built once at startup from the configured server list. Changing the server list requires a data migration.

---

## Configuration

To run HerdDB with remote page storage, set `server.storage.mode=remote` in the server configuration:

```properties
# Server mode can be standalone or cluster
server.mode=standalone

# Use remote file storage for data pages
server.storage.mode=remote

# Comma-separated list of RemoteFileServer addresses (host:port)
# When running in cluster mode with ZooKeeper, this can be left empty
# for automatic ZK-based discovery of file servers.
remote.file.servers=host1:9846,host2:9846

# Local directory for metadata (checkpoint files, schemas, transactions)
server.data.dir=data

# Local tmp directory
server.tmp.dir=tmp
```

All other standalone-mode settings apply (commit log dir, metadata dir, etc.). The remote servers must be started independently before the HerdDB server.

### Starting a RemoteFileServer

```java
RemoteFileServer server = new RemoteFileServer("0.0.0.0", 9846, Paths.get("/data/remote"));
server.start();
// ...
server.stop();
```

Or from the command line (once a launcher is added):

```
java -cp herddb-remote-file-service.jar herddb.remote.RemoteFileServer --port 9846 --dir /data/remote
```

---

## Server.java wiring

`Server.java` resolves `RemoteFileDataStorageManager` via **reflection** to avoid a circular Maven dependency between `herddb-core` and `herddb-remote-file-service`. The `herddb-remote-file-service` JAR must be on the classpath at runtime.

```java
case ServerConfiguration.PROPERTY_STORAGE_MODE_REMOTE: {
    List<String> servers = Arrays.asList(remoteServers.split(","));
    Class<?> clientClass = Class.forName("herddb.remote.RemoteFileServiceClient");
    Object client = clientClass.getConstructor(List.class, Map.class).newInstance(servers, clientConfig);
    Class<?> storageClass = Class.forName("herddb.remote.RemoteFileDataStorageManager");
    return (DataStorageManager) storageClass
            .getConstructor(Path.class, Path.class, int.class, clientClass)
            .newInstance(dataDirectory, tmpDirectory, diskswapThreshold, client);
}
```

---

## Checkpoint and page lifecycle

### Writing pages

1. HerdDB calls `writePage(tableSpace, uuid, pageId, records)`.
2. Records are serialized in-memory (same format as `FileDataStorageManager`).
3. The serialized bytes are sent to the server chosen by `ConsistentHashRouter.getServer(path)`.

### Checkpoint

1. HerdDB calls `tableCheckpoint(tableSpace, uuid, tableStatus, pin)`.
2. `RemoteFileDataStorageManager` delegates local metadata writing to its internal `FileDataStorageManager` (writes `{ledger}.{offset}.checkpoint` in the local table directory).
3. It then lists all remote pages for this table via `client.listFiles("{ts}/{uuid}/data/")`.
4. For each remote page that is no longer in `tableStatus.activePages` (and is not pinned), a `RemoteDeletePageAction` is returned. These are executed after the checkpoint commits.

### Recovery after restart

1. DBManager reads the last checkpoint sequence number from the local metadata dir.
2. It calls `loadTables` / `loadIndexes` / `loadTransactions` — all served from local files.
3. For each table, it calls `getLatestTableStatus` to find the set of active page IDs — also served from local checkpoint files.
4. It calls `cleanupAfterTableBoot` which deletes any remote pages not in the active set (stale pages left by an interrupted checkpoint).
5. Normal page reads go to the remote servers via `readPage` / `readIndexPage`.

---

## Tests

All tests are in `herddb-remote-file-service/src/test/java/herddb/remote/`:

| Test class | What it covers |
|-----------|----------------|
| `RemoteFileServiceTest` | Raw gRPC stub: write/read roundtrip, missing file, delete, list, delete-by-prefix |
| `ConsistentHashRouterTest` | Routes to valid server, consistency, distribution across 2 servers, single-server degenerate case |
| `RemoteFileServiceClientTest` | Client CRUD via single server |
| `MultiServerClientTest` | 2 servers: write/read-back of 50 files, distribution check, delete, list, delete-by-prefix |
| `RemoteFileDataStorageManagerBasicTest` | All DataStorageManager operations: initTablespace/Table/Index, writePage, readPage, writeIndexPage, readIndexPage, tableCheckpoint, indexCheckpoint, fullTableScan, dropTable, truncateIndex, dropIndex, cleanupAfterTableBoot |
| `RemoteFileBrinIndexRecoveryTest` | Full DBManager lifecycle: create tablespace → create table → BRIN index → insert 5 rows → checkpoint → close → reopen → verify data + index recovered |
| `RemoteFileMultiTablespaceTest` | 2 tablespaces × 2 remote servers: insert data, checkpoint, restart DBManager, verify recovery of both tablespaces; BRIN index on tblspace1 |

Run all tests:

```bash
cd herddb-remote-file-service
mvn test
```

---

## On-disk cache layout (S3 mode, issue #475)

When `storage.mode=s3`, the file server fronts the S3-compatible bucket with
a local volatile disk cache (`CachingObjectStorage`). The on-disk layout
is a **two-tier slab** rather than one file per cached object:

- **Small tier** (`slab-small.dat`) — fixed-size cells, default 64 KiB,
  default 25% of `cache.max.bytes`. Holds metadata, transaction records,
  and other sub-block payloads.
- **Large tier** (`slab-large.dat`) — fixed-size cells, default
  `block.size` (4 MiB), default 75% of `cache.max.bytes`. Holds full
  multipart blocks (the dominant ANN/HNSW workload).
- **Per-file fallback** — entries larger than the largest tier's cell
  size fall through to the original one-file-per-object path so
  arbitrarily large objects remain cacheable.

Both slab files are pre-allocated at boot and kept open as
`AsynchronousFileChannel`s for the JVM lifetime, so admit/evict no longer
pay `open`/`close`/`create`/`delete` syscalls per cached object. The
in-memory index (`Map<key, Slot>`) is volatile: the slab files are
deleted on construction and on close, and the index starts empty on every
boot — matching the volatile-cache contract.

Knobs (see `fileserver.properties`):

| Key | Default |
|---|---|
| `cache.slab.enabled` | `true` |
| `cache.slab.small.cell.bytes` | `65536` |
| `cache.slab.small.fraction` | `0.25` |
| `cache.slab.large.cell.bytes` | tracks `block.size` |
| `cache.slab.large.fraction` | `0.75` |

Setting `cache.slab.enabled=false` reverts to the legacy per-file layout.

Per-tier metrics are exposed under `rfs_disk_cache_slab_small_*`,
`rfs_disk_cache_slab_large_*` and `rfs_disk_cache_slab_fallback_*` and
plotted by the bundled Grafana dashboard
(`herddb-kubernetes/.../remote-file-service-dashboard.json`).

---

## Limitations and known constraints

- **No replication.** Each remote server stores a distinct subset of pages. If a server is lost, the pages on that server are lost. Add replication at the infrastructure level (DRBD, replicated block devices, etc.) if durability is required.
- **No server-side resharding.** Changing the `remote.file.servers` list requires a manual data migration because the consistent hash ring changes.
- **Metadata is local.** The node running HerdDB must have a durable local disk for metadata. Page data can be recovered from the remote servers, but the metadata (active page sets, schemas, transaction records) cannot.
- **No TLS.** The Netty channels are plain-text. The underlying `herddb-net` framework supports `SslContext` / `SslHandler`; wiring TLS into `RemoteFileServer` is a follow-up.
- **Single-threaded per-path writes.** No write batching or pipelining; each `writePage` call is a synchronous Netty unary call.
