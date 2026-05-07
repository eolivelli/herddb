# VectorBench CLI — Reference for Agents

`vector-bench` is the HerdDB vector-search benchmark client. It runs inside the
`herddb-tools-0` pod at `/opt/herddb/bin/vector-bench.sh`. In GKE the agent
never calls it directly — it always goes through `./scripts/run-bench.sh
[--background] <vector-bench args>`.

---

## Benchmark phases (in order)

1. **Dataset download** — fetches/caches the base + query vectors locally in
   the tools pod's dataset PVC (`$VECTORBENCH_DATASET_DIR`).
2. **Drop table** — only if `--drop-table` is passed.
3. **Index creation (optional)** — `CREATE VECTOR INDEX` DDL. Runs before
   ingest only if `--index-before-ingest` is passed (default: after ingest).
4. **Ingestion** — `N` rows inserted over `--ingest-threads` JDBC connections,
   each committing every `--batch-size` rows (or `--transaction-size` rows when
   set).
5. **Checkpoint** — `EXECUTE CHECKPOINT` SQL, then waits for the Indexing
   Service to acknowledge via timeout. Runs only if `--checkpoint` is passed.
6. **Wait-for-indexes** — `EXECUTE WAITFORINDEXES` SQL. Blocks until all
   external IS tailers report caught-up. Runs only if `--wait-for-indexes`.
7. **Query / recall** — `N` ANN queries, measuring recall@K. Runs unless
   `--skip-index` or the run ends before reaching this phase.
8. **Report** — final latency histogram and recall printed to stdout (JSON or
   TEXT).

**Rule**: never run the query/recall phase before a successful checkpoint
AND a `--wait-for-indexes` barrier. The checkpoint no longer blocks on IS
catch-up, so without `--wait-for-indexes` recall is measured against a
partially-populated index.

---

## CLI flags

All flags can also be supplied via a `.properties` file with `--config <path>`.
CLI flags override the file. Boolean flags that take no value are written as
`key=true` in the properties file.

### Connection

| Flag | Default | Notes |
|------|---------|-------|
| `-u` / `--url` | `jdbc:herddb:server:localhost:7000` | JDBC URL. `client.timeout` is appended automatically from `--client-timeout`. |
| `--user` | `sa` | |
| `--password` | `hdb` | |
| `--client-timeout` | `7200` | Seconds. Appended to JDBC URL as `client.timeout=<ms>`. |
| `--table` | `vector_bench` | Table name for all DDL/DML. |

### Dataset

| Flag | Default | Notes |
|------|---------|-------|
| `--dataset` | `sift1m` | Preset name (see §Presets below). Use `custom` with `--dataset-url`. |
| `--dataset-url` | *(preset default)* | Override download URL. **For GCS paths (`gs://…`) you MUST also pass `--dataset custom`** — otherwise the `gs://` protocol is rejected. |
| `--dataset-dir` | `./datasets` or `$VECTORBENCH_DATASET_DIR` | Local cache directory. In the tools pod, `$VECTORBENCH_DATASET_DIR` is already set to the PVC mount path — do not override it. |

### Ingestion

| Flag | Default | Notes |
|------|---------|-------|
| `-n` / `--rows` | `100000` | Total rows to ingest. Dataset is cycled if `-n` exceeds the dataset size. |
| `--ingest-threads` | `4` | JDBC connection / worker thread count. Runtime-tunable via admin API. Max 1024. |
| `--batch-size` | `500` | Rows per `executeBatch()` flush — also the commit unit unless `--transaction-size` is set. Runtime-tunable. Must be ≥ 1. |
| `--transaction-size` | `0` (= `--batch-size`) | Rows per JDBC commit. When > 0, each commit accumulates multiple `executeBatch()` flushes on one connection. Must be ≥ `--batch-size`. Runtime-tunable. |
| `--ingest-max-ops` | `100000` | Global ingestion rate cap (rows/s across all threads). `0` = unlimited. Runtime-tunable. Must be ≥ effective transaction size (or 0). |
| `--ingest-commit-retries` | `3` | Retries per failed commit. Exponential back-off: 10 s, 20 s, 40 s, … |
| `--resume-from` | `0` | Skip first N vectors; row IDs start from N. `auto` = query `MAX(id)+1` from the table. |
| `--skip-ingest` | false | Skip the ingestion phase entirely. |
| `--skip-verify` | false | Skip post-ingest row count verification. |
| `--drop-table` | false | Drop and recreate the table before starting. |

### Index

| Flag | Default | Notes |
|------|---------|-------|
| `--m` | `16` | HNSW M parameter for `CREATE VECTOR INDEX`. |
| `--beam-width` | `100` | HNSW beamWidth parameter. |
| `--index-num-shards` | `4` | `numShards` emitted in the DDL. Set to `1` to disable sharding (single IS replica). With 2 IS replicas at `numShards=4`, each instance handles 2 shards (~50% of vectors). |
| `--similarity` | *(dataset default)* | Override similarity: `euclidean`, `cosine`, `dot`. |
| `--index-before-ingest` | false | Create the vector index before ingestion instead of after. |
| `--skip-index` | false | Skip index creation and the query phase. |

### Checkpoint / wait-for-indexes

| Flag | Default | Notes |
|------|---------|-------|
| `--checkpoint` | false | Run `EXECUTE CHECKPOINT` after ingestion and after index creation. Required before any recall queries when an IS is in use. |
| `--checkpoint-timeout-seconds` | `300` | **Always use 1800 in automated runs.** Never lower. |
| `--wait-for-indexes` | false | Run `EXECUTE WAITFORINDEXES` before the query phase. Required for reliable recall when IS tailers are used. Always pair with `--checkpoint`. |
| `--wait-for-indexes-timeout` | `600` | Seconds. **Always use 1800 in automated runs.** Never lower. |

### Query / recall

| Flag | Default | Notes |
|------|---------|-------|
| `-k` | `10` | `LIMIT K` for ANN queries. Runtime-tunable via admin API. |
| `--query-threads` | `4` | Parallel query workers. |
| `--queries` | `1000` | Total ANN queries to execute. |
| `--query-max-ops` | `10` | Global query rate cap (queries/s). `0` = unlimited. Runtime-tunable. |

### Output / observability

| Flag | Default | Notes |
|------|---------|-------|
| `--no-progress` | false | Disable the spinner; emit plain `\n`-terminated lines. Always passed by `run-bench.sh` so logs are tail-friendly. |
| `--output-format` | `text` | `text` or `json` (NDJSON, one object per line). JSON implies `--no-progress`. |
| `--status-interval-seconds` | `60` | Seconds between server-status dumps (checkpoint LSN, index tail lag). `0` disables the status thread. |
| `--config` | *(none)* | Path to a `.properties` file; CLI flags override. |

---

## Dataset presets

| Preset name | Aliases | Vectors | Dimensions | Similarity | Source |
|-------------|---------|---------|------------|------------|--------|
| `sift10k` | `siftsmall` | 10,000 | 128 | euclidean | FTP IRISA |
| `sift1m` | `sift` | 1,000,000 | 128 | euclidean | FTP IRISA |
| `sift10m` | — | 10,000,000 | 128 | euclidean | FTP IRISA |
| `gist1m` | `gist` | 1,000,000 | 960 | euclidean | FTP IRISA |
| `bigann` | `sift1b` | 1,000,000,000 | 128 | euclidean | FTP IRISA |
| `glove100` | `glove-100`, `glove` | 1,183,514 | 100 | cosine | HDF5 (ann-benchmarks) |
| `deep-image-96` | `deep-image`, `deepimage` | 9,990,000 | 96 | cosine | HDF5 (ann-benchmarks) |
| `custom` | — | (from descriptor) | (from descriptor) | (from descriptor) | `--dataset-url` |

**Custom (GCS-hosted) datasets:**
```
./scripts/run-bench.sh \
    --dataset custom \
    --dataset-url "gs://herddb-datasets/bigann/published/bigann_descriptor.json" \
    -n 1000000000 -k 10 ...
```
`--dataset custom` activates the GCS download path in `DatasetLoader`. Without
it, `--dataset-url` is silently ignored and the loader falls back to HTTP,
producing `unknown protocol: gs` errors. `$VECTORBENCH_DATASETS_BUCKET` is set
inside the pod but NOT in the local shell — always write the full `gs://` URL
literally.

---

## Admin HTTP API (port 8080, inside `herddb-tools-0`)

Reach it via `kubectl exec`:
```
kubectl exec herddb-tools-0 -- curl -s http://localhost:8080/<endpoint>
```

POST body: bare integer (`-d '40000'`) or JSON `{"value": 40000}`.

### Read endpoints (GET)

| Endpoint | Returns |
|----------|---------|
| `/status` | `phase`, `rows`, `total`, `ops_per_sec`, `commits`, `recovered_commits`, `heap_used_mb`, `heap_max_mb`, `commit_latency` (mean/p50/p99/max ms) |
| `/ingestion/config` | `ingest-max-ops`, `ingest-threads`, `batch-size`, `transaction-size`, `rows`, `resume-from`, `ingest-commit-retries` |
| `/ingestion/config/ingest-threads` | `{"ingest-threads": N}` |
| `/ingestion/config/batch-size` | `{"batch-size": N}` |
| `/ingestion/config/transaction-size` | `{"transaction-size": N}` |
| `/query/config` | `query-max-ops`, `query-threads`, `queries`, `top-k` |
| `/query/config/query-threads` | `{"query-threads": N}` |

### Write endpoints (POST)

| Endpoint | Effect | Validation |
|----------|--------|------------|
| `POST /ingestion/config/ingest-max-ops` | Set global ingestion rate cap (rows/s); 0 = unlimited | Must be ≥ effective transaction size or 0 |
| `POST /ingestion/config/ingest-threads` | Add/remove workers live | 1–1024 |
| `POST /ingestion/config/batch-size` | Change executeBatch() flush size | Must be ≤ transaction-size (if set); effective txn must be ≤ ingest-max-ops |
| `POST /ingestion/config/transaction-size` | Change commit unit | Must be ≥ batch-size; must be ≤ ingest-max-ops (if finite) |
| `POST /query/config/query-max-ops` | Set global query rate cap; 0 = unlimited | ≥ 0 |
| `POST /query/config/query-threads` | Change query parallelism | ≥ 1 |
| `POST /query/config/top-k` | Change LIMIT K for next query | > 0 |

---

## Rate limiter safety rules

**Rule: `ingest-max-ops ≥ effective transaction size` (or 0 for unlimited).**

The rate limiter is acquired **once per commit** with `acquire(effectiveTransactionSize)`.
If `ingest-max-ops < effectiveTransactionSize`, the limiter blocks each worker
for `effectiveTransactionSize / ingestMaxOps` seconds — potentially hours.

Examples:
- `--batch-size 10000 --ingest-max-ops 10` → each acquire blocks **1000 s** per
  worker — effectively a deadlock.
- `--batch-size 10000 --ingest-max-ops 10000` → each acquire takes 1 s (safe).
- `--batch-size 10000 --ingest-max-ops 40000` → each acquire takes 0.25 s (good).

**For ramp tests**: start at `ingest-max-ops = effectiveTransactionSize` and
step up in multiples. Example with batch-size=10000: 10000 → 20000 → 40000 → 100000.

**Note on live rate changes**: swapping the rate via the admin API does NOT
unblock workers already sleeping inside `Thread.sleep()` in the old limiter
(this is fixed in the per-thread limiter for the ingest path — rate changes
push the new rate and unpark sleeping workers immediately). Reducing the rate
to near-zero while a run is in progress is dangerous.

---

## Default workload for GKE benchmarks

```bash
./scripts/run-bench.sh --background \
    --dataset sift10k -n 10000 -k 100 \
    --ingest-max-ops 40000 --ingest-threads 8 --batch-size 10000 \
    --checkpoint --checkpoint-timeout-seconds 1800 \
    --wait-for-indexes --wait-for-indexes-timeout 1800
```

Validated baseline for bigann 1B:
```bash
./scripts/run-bench.sh --background \
    --dataset custom \
    --dataset-url "gs://herddb-datasets/bigann/published/bigann_descriptor.json" \
    -n 1000000000 -k 10 \
    --ingest-max-ops 40000 --ingest-threads 8 --batch-size 10000 \
    --checkpoint --checkpoint-timeout-seconds 1800 \
    --wait-for-indexes --wait-for-indexes-timeout 1800
```

Observed: ~13,870 ops/s sustained at 8 threads / batch=10000 (k3s-local).

Agent defaults (always inject unless the user explicitly overrides):
- `--ingest-max-ops 40000`
- `--ingest-threads 8`
- `--batch-size 10000`
- `--checkpoint-timeout-seconds 1800`
- `--wait-for-indexes-timeout 1800`

---

## Background mode (`--background` in `run-bench.sh`)

When `./scripts/run-bench.sh --background …` is used:
- The script starts the JVM inside the pod as a `nohup` process and **exits immediately** (exit 0).
- The benchmark log lives inside the pod at `/tmp/vector-bench-<TS>.log`.
- The local `$RUN_LOG` file contains only the header + `remote-log:` pointer.
- Progress is monitored via `GET /status` on the admin API.
- To stop: `./scripts/kill-bench.sh`.

To tail the pod log manually:
```
kubectl -n default exec sts/herddb-tools -- tail -f /tmp/vector-bench-<TS>.log
```

---

## Environment variables (inside the pod)

| Variable | Used by | Notes |
|----------|---------|-------|
| `VECTORBENCH_DATASET_DIR` | `DatasetLoader` | Dataset cache PVC mount path. Set by the StatefulSet. Do not override. |
| `VECTORBENCH_DATASETS_BUCKET` | *(not used directly by CLI)* | GCS bucket name (no `gs://` prefix). Set in the pod but CLI always needs the full `gs://` URL in `--dataset-url`. |
| `VECTOR_BENCH_NO_PROGRESS` | `Config` | `1`/`true`/`yes`/`on` → enable `--no-progress`. |
| `VECTOR_BENCH_OUTPUT_FORMAT` | `Config` | `text` or `json`. |
| `vectorbench.admin.port` | `AdminApiServer` | System property (JVM flag `-D`). Default 8080; 0 or negative disables the admin API. |

