# HerdDB + JVector

*A horizontally scalable vector search database built on HerdDB, JVector,
Apache BookKeeper, Apache ZooKeeper and Apache Calcite.*

## Origins

This project started as a fork of [`diennea/herddb`](https://github.com/diennea/herddb)
— an embeddable, SQL-first distributed database — and has since evolved
independently. Focus has shifted toward **vector search at scale on
Kubernetes**: a standalone indexing service, shadow read replicas, and
object-store-backed shared storage have been added on top of the original
tablespace / WAL / checkpoint core.

The upstream project remains a separate codebase; this fork is not a
drop-in replacement and the two are no longer wire-compatible in all
configurations.

## Built on production-grade OSS

HerdDB + JVector does not reinvent the hard parts of a distributed
stateful system. It composes well-known, widely-deployed open-source
components:

| Component | Role | Version | Project |
|---|---|---|---|
| HerdDB core | SQL engine, WAL, tablespaces, checkpointing, BLink PK index | fork base | https://github.com/diennea/herddb |
| JVector (DataStax / IBM) | On-disk HNSW graph + Fused-PQ vector index | `4.0.0-rc.9-herddb` | https://github.com/datastax/jvector |
| Apache BookKeeper | Distributed, replicated, low-latency commit log | `4.17.3` | https://bookkeeper.apache.org |
| Apache ZooKeeper | Cluster metadata, tablespace leader election, instance discovery | `3.9.3` | https://zookeeper.apache.org |
| Apache Calcite | SQL parser and cost-based query planner | `1.40.0` | https://calcite.apache.org |

All of these are battle-tested in production at other projects; this
repository glues them together rather than re-implementing them.

## Cloud-native deployment

HerdDB + JVector is designed to run on **Kubernetes on public clouds**.

- **GKE** is currently the only environment actively tested end-to-end
  (via an in-repo benchmark harness — see [Agentic QA](#agentic-qa)
  below).
- The object-store code path uses the AWS SDK v2 S3 client, so **AWS
  S3** is expected to work with minor configuration changes; other
  S3-compatible stores (MinIO is used in local tests) have also been
  exercised.
- **Docker images** are built from [`herddb-docker/`](./herddb-docker/).
- A **Helm chart** plus ready-to-use `values.yaml` for GKE and for a
  local k3s-in-docker stack ships under
  [`herddb-kubernetes/src/main/helm/herddb/`](./herddb-kubernetes/src/main/helm/herddb/).

See [KUBERNETES.md](./KUBERNETES.md) for image build instructions and
chart usage.

## Architecture at a glance

```mermaid
flowchart LR
  subgraph Clients
    CLI["JDBC / SQL client"]
  end

  subgraph "Control plane"
    ZK[("Apache ZooKeeper<br/>metadata + discovery")]
  end

  subgraph "HerdDB servers"
    Leader["Server<br/>(tablespace leader)"]
    Replica["Server<br/>(read-only replica,<br/>no WAL tailing)"]
  end

  subgraph "Indexing service"
    ISvcP["Primary<br/>(writes index)"]
    ISvcS1["Shadow 1<br/>(read-only)"]
    ISvcS2["Shadow 2<br/>(read-only)"]
  end

  subgraph "Storage tier"
    BK[("Apache BookKeeper<br/>commit log ensemble")]
    RFS["Remote File Service<br/>(gRPC + block cache)"]
    OBJ[("S3 / GCS / MinIO")]
  end

  CLI -->|SQL + DML + vector search| Leader
  CLI -. read-only SQL .-> Replica
  Leader -. vector search (LB) .-> ISvcP
  Leader -. vector search (LB) .-> ISvcS1
  Leader -. vector search (LB) .-> ISvcS2

  Leader -->|append| BK
  ISvcP -->|tail WAL| BK

  Leader --> RFS
  Replica --> RFS
  ISvcP --> RFS
  ISvcS1 --> RFS
  ISvcS2 --> RFS
  RFS --> OBJ

  Leader -. elect / heartbeat .-> ZK
  Replica -. watch checkpoint state .-> ZK
  Leader -. discover indexing instances .-> ZK
  ISvcP -. publish IndexStatus .-> ZK
  ISvcS1 -. watch .-> ZK
  ISvcS2 -. watch .-> ZK
```

The rest of this section zooms in on individual layers.

## Component deep-dives

### 1. Replicated commit log (BookKeeper)

The tablespace leader appends every DML and DDL to a BookKeeper ledger.
In the vector-search deployment profile the **indexing service primary**
tails this ledger to materialise vector indexes on its own schedule —
the SQL read-only replicas described in §2 do **not** tail the WAL;
they read committed data from shared storage instead.

```mermaid
flowchart LR
  Leader["HerdDB leader<br/>(tablespace owner)"]
  subgraph Ensemble["BookKeeper ensemble (E=3, Qw=2, Qa=2)"]
    B1[(Bookie 1)]
    B2[(Bookie 2)]
    B3[(Bookie 3)]
  end
  ISvc["Indexing service primary<br/>(tails WAL → vector index)"]

  Leader -->|fsynced add| B1
  Leader -->|fsynced add| B2
  Leader -->|fsynced add| B3
  B1 -. tail .-> ISvc
  B2 -. tail .-> ISvc
```

Ensemble, write-quorum and ack-quorum are tuned per deployment
(see the example `values.yaml` files). BookKeeper gives strong
durability and low write latency without requiring shared block
storage.

In the classic HerdDB deployment profile (see §5) WAL-tailing
follower replicas also subscribe to this ledger; that mode is not
the recommended topology for vector-search workloads.

Details: [CHECKPOINT.md §2](./CHECKPOINT.md) describes how checkpoints
interact with the WAL and how commit-log truncation is coordinated.

### 2. Read-only replicas on shared storage (vector-search profile)

In the vector-search deployment profile, SQL read scalability is
provided by **read-only replicas** that do *not* tail the commit log.
The leader checkpoints committed pages to the Remote File Service;
replicas read the same pages directly from shared storage and therefore
stay stateless — they can be scaled out horizontally, added and
removed freely, and they never need to replay a WAL.

```mermaid
flowchart LR
  ZK[("ZooKeeper<br/>leader election<br/>+ checkpoint state")]
  subgraph Tablespace["Tablespace 'default'"]
    L["Leader<br/>(writes WAL,<br/>checkpoints pages)"]
    R1["Read-only replica 1"]
    R2["Read-only replica 2"]
  end
  RFS["Remote File Service<br/>(shared pages)"]

  L -->|write pages at checkpoint| RFS
  R1 -. read pages .-> RFS
  R2 -. read pages .-> RFS

  L <-->|heartbeat / lease| ZK
  L -. publish durable LSN .-> ZK
  R1 -. watch checkpoint state .-> ZK
  R2 -. watch checkpoint state .-> ZK
```

Because replicas source their data from the shared file server,
they lag the leader by at most one checkpoint interval — the same
bound that applies to indexing-service shadows (§4). Point-in-time
read-your-writes consistency is **not** provided on these replicas.

**Two distinct read-scaling tiers — don't conflate them:**

- **SQL read-only replicas** transparently scale *plain SQL* reads
  across the cluster by sharing pages through the file-server tier.
- **Indexing-service shadow replicas** (§4 below) transparently scale
  *vector-search* read throughput. The HerdDB server load-balances
  each search query across `{primary, shadow1, shadow2, …}` for the
  target index on behalf of the JDBC client.

For the classic WAL-tailing follower model used in non-vector
deployments, see §5.

### 3. Shared object storage + file-server cache tier

Table pages and vector segments are stored in a shared object store
(S3, GCS, MinIO). The **Remote File Service** is a stateless gRPC
tier that fronts the object store, routing page IDs across backends
with Murmur3 consistent hashing and caching recently-read segment
blocks in an off-heap, byte-weighted Caffeine LRU.

```mermaid
flowchart LR
  subgraph Clients["HerdDB + Indexing servers"]
    H1["Server / Indexer"]
    H2["Server / Indexer"]
  end

  subgraph RFSTier["Remote File Service (stateless, horizontal)"]
    R1["rfs-0<br/>block cache"]
    R2["rfs-1<br/>block cache"]
    R3["rfs-2<br/>block cache"]
  end

  OBJ[("S3 / GCS / MinIO<br/>(durable shared store)")]

  H1 -->|consistent-hash<br/>by page-id| R1
  H1 --> R2
  H1 --> R3
  H2 --> R1
  H2 --> R2
  H2 --> R3

  R1 --> OBJ
  R2 --> OBJ
  R3 --> OBJ
```

Because page content is immutable once written, the cache is trivially
coherent: the file-server tier can be scaled out horizontally, and
shadow replicas that hit the same hot working set benefit from warm
caches.

Details: [REMOTE_FILE_SERVER.md](./REMOTE_FILE_SERVER.md).

### 4. Indexing service: primary + shadow replicas

The indexing service is a standalone gRPC process. It **tails the
HerdDB WAL** independently of the database, materialising vector
indexes on its own schedule.

- The **primary** owns an index: it ingests DML from the WAL, builds
  live JVector shards, freezes them during checkpoint, writes on-disk
  segments (FusedPQ) to the shared Remote File Service, and publishes
  a new `IndexStatus` / durable LSN to ZooKeeper after every successful
  checkpoint.
- **Shadow replicas** are read-only siblings tied to a specific
  primary's `instanceId`. They watch the primary's state znode; when
  a new LSN is published, they reload `IndexStatus` from shared
  storage and serve search traffic against it. Shadows therefore
  lag the primary by **at most one checkpoint interval**.
- The **HerdDB server** discovers indexing-service instances via
  ZooKeeper and, for each vector-search query received over JDBC,
  **load-balances across `{primary, shadow1, shadow2, …}`** for
  the target index, failing over within the pool on `NOT_READY`
  or retryable errors. JDBC clients see a single SQL endpoint.

```mermaid
flowchart LR
  WAL[("BookKeeper WAL")]
  ZK[("ZooKeeper<br/>/indexingServices/instances")]
  OBJ[("Shared object store<br/>(segments + IndexStatus)")]

  subgraph Primary["Indexing service — primary (write)"]
    P["Tails WAL → builds JVector shards → checkpoints"]
  end

  subgraph Shadows["Indexing service — shadows (read-only, horizontal)"]
    S1["Shadow 1"]
    S2["Shadow 2"]
    S3["Shadow N"]
  end

  JDBC["JDBC client"]
  Server["HerdDB server<br/>(vector-search LB)"]

  WAL --> P
  P -->|write segments + status| OBJ
  P -->|publish durable LSN| ZK
  ZK -. notify .-> S1
  ZK -. notify .-> S2
  ZK -. notify .-> S3
  OBJ -. reload IndexStatus .-> S1
  OBJ -. reload IndexStatus .-> S2
  OBJ -. reload IndexStatus .-> S3

  JDBC -->|SQL search| Server
  Server -. gRPC search .-> P
  Server -. gRPC search .-> S1
  Server -. gRPC search .-> S2
  Server -. gRPC search .-> S3
```

Scope is strictly horizontal **read** scalability. Shadow-to-primary
promotion is explicitly out of scope, and shadows require
`indexing.storage.type=remote` (i.e. shared storage) — any other
configuration fails fast at boot.

Details: [VECTOR.md](./VECTOR.md) (architecture, SQL, shard
lifecycle), [VECTOR_SEARCH_METRICS.md](./VECTOR_SEARCH_METRICS.md)
(observability).

### 5. Classic HerdDB replication (WAL-tailing followers)

The original HerdDB replication model is still supported, and is the
right choice for non-vector workloads where an indexing service would
just be dead weight. In this profile each tablespace has a leader and
one or more **follower replicas** that tail the same BookKeeper ledger
the leader writes to, applying every entry locally and keeping a
fully-materialised copy of the tablespace on their own storage.

```mermaid
flowchart LR
  ZK[("ZooKeeper<br/>leader election")]
  Leader["Leader<br/>(writes WAL)"]
  BK[("BookKeeper WAL")]
  F1["Follower 1<br/>(tails WAL,<br/>applies locally)"]
  F2["Follower 2<br/>(tails WAL,<br/>applies locally)"]

  Leader -->|append| BK
  BK -. tail .-> F1
  BK -. tail .-> F2

  Leader <-->|lease| ZK
  F1 <-->|watch| ZK
  F2 <-->|watch| ZK
```

Trade-offs vs §2:

- No dependency on shared object storage or a file-server tier —
  followers keep their own pages, so the deployment works with only
  local disks plus BookKeeper + ZooKeeper.
- Followers can be promoted to leader on failover; they hold the
  full WAL position, not a checkpoint snapshot.
- Does not scale *horizontally* the way §2 does: each added follower
  replays the full WAL and pays its own storage cost.
- There is no vector-indexing service in this profile; the `VECTOR`
  SQL features are not available.

This is the replication model documented extensively in the upstream
HerdDB project and in [CHECKPOINT.md](./CHECKPOINT.md). It is *not*
the recommended topology for vector-search workloads — use §2 + §4
for those.

## Documentation index

All detailed documentation lives alongside the code in the repo root:

| Document | What it covers |
|---|---|
| [VECTOR.md](./VECTOR.md) | Vector index architecture, SQL syntax, indexing service, shard lifecycle, configuration. |
| [VECTOR_SEARCH_METRICS.md](./VECTOR_SEARCH_METRICS.md) | Prometheus metrics and Grafana dashboard for the end-to-end vector read path (server / index server / file server / client). |
| [CHECKPOINT.md](./CHECKPOINT.md) | Checkpoint phases (A / B / C), lock coupling with concurrent DML, per-index specifics, WAL truncation. |
| [BLINK.md](./BLINK.md) | Primary-key B-link tree: legacy vs incremental on-disk formats, recovery, selection flag. |
| [REMOTE_FILE_SERVER.md](./REMOTE_FILE_SERVER.md) | gRPC remote page storage, consistent hashing, metadata vs page layout, block cache. |
| [AUTHENTICATION.md](./AUTHENTICATION.md) | JDBC and gRPC auth mechanisms, including SASL OAUTHBEARER / OIDC JWT. |
| [KUBERNETES.md](./KUBERNETES.md) | Building Docker images, pushing to a registry, deploying via the Helm chart. |
| [CLAUDE.md](./CLAUDE.md) | Contributor guidelines — CI gates, hammer-test regression suite, exception-handling policy. |

## Agentic QA

Cluster-level regressions are caught by two Claude Code agent
definitions checked into the repository. Each one stands up a full
stack (HerdDB + indexing service + Remote File Service + BookKeeper +
ZooKeeper + object store) and runs a vector-search benchmark workload
end-to-end, producing a markdown report — or, on failure, opening a
GitHub issue with pod logs attached.

| Agent | Target | Object store |
|---|---|---|
| [`.claude/agents/herddb-k3s-bench.md`](./.claude/agents/herddb-k3s-bench.md) | local k3s-in-docker | MinIO |
| [`.claude/agents/herddb-gke-bench.md`](./.claude/agents/herddb-gke-bench.md) | existing GKE cluster | Google Cloud Storage |

These are developer-facing regression harnesses, not a user feature.
The underlying shell scripts they drive live under
[`herddb-kubernetes/src/main/helm/herddb/examples/`](./herddb-kubernetes/src/main/helm/herddb/examples/)
and can also be run by hand.

## Getting involved

Join the [mailing list](http://lists.herddb.org/mailman/listinfo).

## License

HerdDB + JVector is distributed under the
[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).
