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
| `RemoteFileServer` | Standalone gRPC server. Stores files in a local directory. Configurable bind host and port. |
| `RemoteFileServiceImpl` | gRPC service implementation backed by local filesystem. Writes are atomic (temp file + rename). |
| `RemoteFileServiceClient` | Client that manages one `ManagedChannel` per server and routes requests via `ConsistentHashRouter`. `listFiles` and `deleteByPrefix` fan out to all servers. |
| `ConsistentHashRouter` | Murmur3-based consistent hash ring with 150 virtual nodes per server for balanced distribution. |
| `RemoteFileDataStorageManager` | `DataStorageManager` implementation. Delegates page I/O to `RemoteFileServiceClient`; delegates all metadata I/O to an internal `FileDataStorageManager`. |

**Dependency:** Java 11, gRPC 1.68.2, protobuf 3.25.5.

---

## gRPC API

Defined in `src/main/proto/remote_file_service.proto`:

```protobuf
service RemoteFileService {
    rpc WriteFile        (WriteFileRequest)        returns (WriteFileResponse);
    rpc ReadFile         (ReadFileRequest)         returns (ReadFileResponse);
    rpc DeleteFile       (DeleteFileRequest)       returns (DeleteFileResponse);
    rpc ListFiles        (ListFilesRequest)        returns (ListFilesResponse);
    rpc DeleteByPrefix   (DeleteByPrefixRequest)   returns (DeleteByPrefixResponse);
}
```

| RPC | Request fields | Response fields | Notes |
|-----|---------------|-----------------|-------|
| `WriteFile` | `path`, `content` | `ok` | Atomic write via tmp+rename |
| `ReadFile` | `path` | `content`, `found` | Returns `found=false` if missing |
| `DeleteFile` | `path` | `deleted` | Returns `false` if file did not exist |
| `ListFiles` | `prefix` | `paths[]` | Returns all files whose relative path starts with `prefix` |
| `DeleteByPrefix` | `prefix` | `deleted_count` | Bulk-delete all files matching the prefix |

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

To run HerdDB with remote page storage, set `server.mode=remote-file-service` in the server configuration:

```properties
server.mode=remote-file-service

# Comma-separated list of RemoteFileServer addresses (host:port)
# Default: localhost:9846
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
case ServerConfiguration.PROPERTY_MODE_REMOTE_FILE_SERVICE: {
    List<String> servers = Arrays.asList(remoteServers.split(","));
    Class<?> clientClass = Class.forName("herddb.remote.RemoteFileServiceClient");
    Object client = clientClass.getConstructor(List.class).newInstance(servers);
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

## Limitations and known constraints

- **No replication.** Each remote server stores a distinct subset of pages. If a server is lost, the pages on that server are lost. Add replication at the infrastructure level (DRBD, replicated block devices, etc.) if durability is required.
- **No server-side resharding.** Changing the `remote.file.servers` list requires a manual data migration because the consistent hash ring changes.
- **Metadata is local.** The node running HerdDB must have a durable local disk for metadata. Page data can be recovered from the remote servers, but the metadata (active page sets, schemas, transaction records) cannot.
- **No TLS.** The gRPC channels are plain-text (`usePlaintext()`). For production use, configure TLS at the network layer or extend the client/server with gRPC TLS support.
- **Single-threaded per-path writes.** No write batching or pipelining; each `writePage` call is a synchronous gRPC unary call.
