# VectorBench

VectorBench drives an end-to-end vector-search workload against a HerdDB
server (or cluster): schema setup, bulk ingestion of float-array vectors,
vector-index creation, ANN queries, and recall computation. This document
lists every CLI option and documents the embedded admin HTTP API that
lets you inspect and tune a running bench without a JVM restart.

## CLI parameters

Invocation: `java -jar vector-testings-*.jar [options]`.

### Connection

| Option | Default | Description |
|---|---|---|
| `-u`, `--url <jdbc-url>` | `jdbc:herddb:server:localhost:7000` | HerdDB JDBC URL. |
| `--user <name>` | `sa` | JDBC username. |
| `--password <secret>` | `hdb` | JDBC password. |
| `--table <name>` | `vector_bench` | Table to create and populate. |
| `--client-timeout <seconds>` | `28800` | JDBC client request timeout; appended to the URL as `client.timeout=…ms`. |

### Dataset

| Option | Default | Description |
|---|---|---|
| `--dataset <preset>` | `SIFT1M` | Built-in dataset preset, or `CUSTOM` for a descriptor-driven dataset. |
| `--dataset-dir <path>` | `./datasets` | Local cache directory for dataset files. |
| `--dataset-url <url>` | — | Override the download URL for the selected preset. |
| `--rows <N>` | `100000` | Vectors to ingest. For `CUSTOM`, auto-derived from the descriptor if left at default. For multi-checkpoint custom datasets, `N` must match one of the descriptor's `groundTruthCheckpoints` entries to enable recall — otherwise the bench logs the available counts and continues without recall. Discover the available counts with `./run_describe.sh --descriptor <path-or-url>`. |

### Ingestion

| Option | Default | Description |
|---|---|---|
| `--ingest-threads <N>` | `4` | Concurrent ingestion workers. |
| `--batch-size <N>` | `500` | Rows per JDBC batch commit. |
| `--ingest-max-ops <N>` | `100000` | Global ingestion rate cap in rows/s (`0` = unlimited). **Runtime-tunable** via admin API. |
| `--ingest-commit-retries <N>` | `3` | Retries per failed batch commit (exponential back-off 10s/20s/40s…). |
| `--resume-from <N \| auto>` | `0` | Skip first `N` vectors and start row IDs from `N`. Pass `auto` (case-insensitive) to resolve `N` from `SELECT COUNT(*)` on the table just before ingestion. |
| `--skip-ingest` | off | Skip the ingestion phase. |
| `--drop-table` | off | Drop the table before recreating it. |

### Index

| Option | Default | Description |
|---|---|---|
| `-m`, `--m <N>` | `16` | jVector HNSW fan-out. |
| `--beam-width <N>` | `100` | Build-time beam width. |
| `--index-num-shards <N>` | `4` | Vector index shard count (`1` = unsharded). |
| `--index-before-ingest` | off | Create the index before ingestion rather than after. |
| `--skip-index` | off | Skip the index-creation phase. |
| `--similarity <fn>` | dataset default | One of `euclidean`, `cosine`, `dot`. |

### Query

| Option | Default | Description |
|---|---|---|
| `--query-threads <N>` | `4` | Concurrent query workers. |
| `--queries <N>` | `1000` | Total queries to run. Cycled if the dataset has fewer query vectors. |
| `-k <N>` | `10` | Top-K. **Runtime-tunable** via admin API. |
| `--query-max-ops <N>` | `10` | Global query rate cap in queries/s (`0` = unlimited). **Runtime-tunable** via admin API. |

### Lifecycle / operations

| Option | Default | Description |
|---|---|---|
| `--checkpoint` | off | Run `EXECUTE CHECKPOINT 'herd'` after ingestion and after index creation. |
| `--checkpoint-timeout-seconds <N>` | `300` | Checkpoint timeout. |
| `--wait-for-indexes` | off | Run `EXECUTE WAITFORINDEXES 'herd'` before queries (required for reliable recall with tailer indexes). |
| `--wait-for-indexes-timeout <N>` | `600` | Tailer catch-up timeout. |
| `--skip-verify` | off | Skip the post-ingest `COUNT(*)` verification. |

### Output & telemetry

| Option | Default | Description |
|---|---|---|
| `--output-format <text\|json>` | `text` | `json` emits NDJSON (one object per line) and implies `--no-progress`. |
| `--no-progress` | off | Disable the animated spinner; one line per progress sample. Implicit when `VECTOR_BENCH_NO_PROGRESS=1` or when output is JSON. |
| `--status-interval-seconds <N>` | `60` | Period of the `[status]` dump that queries `syslogstatus`, `systablestats`, `sysindexstatus`. `0` disables. |
| `--config <path>` | — | Load defaults from a properties file (CLI flags still override). |

### Admin HTTP API (system property)

| Property | Default | Description |
|---|---|---|
| `-Dvectorbench.admin.port=<N>` | `8080` | Port for the embedded admin HTTP server. Set `0` or a negative value to disable. |

## Admin HTTP API

When enabled, VectorBench starts an embedded Jetty server on
`-Dvectorbench.admin.port` (default `8080`). The API is JSON; all responses
are `application/json`. Errors return a `{ "error": "<message>" }` body with
HTTP `400` for bad requests or `404` for unknown endpoints.

### `GET /ingestion/config`

Returns the current ingestion configuration. Reflects live values, so after
a `POST` override the new rate is visible here.

```json
{
  "ingest-max-ops": 100000,
  "ingest-threads": 4,
  "batch-size": 500,
  "rows": 100000,
  "resume-from": 0,
  "ingest-commit-retries": 3
}
```

### `POST /ingestion/config/ingest-max-ops`

Override `--ingest-max-ops` while the bench is running. Body can be JSON
`{"value": <int>}` or a bare integer.

```
$ curl -X POST -d '{"value": 5000}' http://localhost:8080/ingestion/config/ingest-max-ops
{"ingest-max-ops":5000,...}
```

- `value >= 0` required; negative values return `400`.
- `0` means unlimited (internally a very-high-rate limiter).
- Internally replaces the Guava `RateLimiter` with a fresh instance so
  workers see the new rate on their next batch without any lingering
  pay-forward reservation from the old rate.

### `GET /query/config`

```json
{
  "query-max-ops": 10,
  "query-threads": 4,
  "queries": 1000,
  "top-k": 10
}
```

### `POST /query/config/query-max-ops`

Same semantics as the ingestion override. Body: JSON `{"value": <int>}` or
bare integer. Negative values return `400`; `0` means unlimited.

```
$ curl -X POST -d '250' http://localhost:8080/query/config/query-max-ops
{"query-max-ops":250,...}
```

### `POST /query/config/top-k`

Override `-k` (top-K) at runtime. Workers re-read `topK` between queries
and re-prepare the SQL statement when the value changes.

```
$ curl -X POST -d '{"value": 32}' http://localhost:8080/query/config/top-k
{"top-k":32,...}
```

- `value > 0` required; `0` and negatives return `400`.
- Note: changing top-K mid-run mixes results of different K across the
  recall computation; use with care.

### `GET /status`

Returns a snapshot of the currently-running phase. Fields depend on phase:

Idle (before ingestion or between phases):

```json
{ "phase": "idle" }
```

During ingestion:

```json
{
  "phase": "ingestion",
  "rows": 42000,
  "total": 100000,
  "ops_per_sec": 12500.3,
  "commits": 84,
  "recovered_commits": 0,
  "heap_used_mb": 512,
  "heap_max_mb": 2048,
  "commit_latency": {
    "mean_ms": 18.2,
    "p50_ms": 15.4,
    "p99_ms": 52.1,
    "max_ms": 78.0
  }
}
```

During queries:

```json
{
  "phase": "query",
  "queries_done": 450,
  "total": 1000,
  "qps": 9.5,
  "top_k": 10,
  "latency": {
    "mean_ms": 104.8,
    "p50_ms": 98.3,
    "p95_ms": 187.5,
    "p99_ms": 205.1,
    "max_ms": 312.0
  }
}
```

## Notes on rate-change semantics

Guava's `RateLimiter` uses pay-forward reservation: a single large
`acquire(N)` call at a low rate can reserve many seconds into the future
and block the *next* caller for that time, even if the rate is raised in
the meantime. VectorBench avoids this by **replacing the limiter** on every
`POST /…/…-max-ops`, so workers that re-read the reference see a fresh
limiter with no lingering reservation. The worst case is a single
already-in-flight `acquire()` finishing its wait on the old limiter; the
next iteration uses the new one.

## Example: throttle ingestion during a production incident

```
# Drop to 1 000 rows/s immediately
curl -X POST -d '1000' http://bench-host:8080/ingestion/config/ingest-max-ops

# …restore full speed once the incident is resolved
curl -X POST -d '0' http://bench-host:8080/ingestion/config/ingest-max-ops
```
