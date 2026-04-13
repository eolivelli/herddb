# HerdDB - Development Guidelines

## Git Workflow
Never push commits directly to the `master` branch. Always create a feature branch and
open a pull request so that CI runs and changes can be reviewed before merging.

## Before Sending a PR
Run the code validation checks locally before opening a pull request:
```
mvn -B checkstyle:check apache-rat:check spotbugs:check install -DskipTests -Pjenkins
```
This matches what `.github/workflows/pr-validation.yml` runs in CI (checkstyle, Apache RAT license headers, SpotBugs).

## Test Categories
Tests that require ZooKeeper/BookKeeper infrastructure (cluster mode) must be annotated with
`@Category(ClusterTest.class)` (import `herddb.core.ClusterTest` and `org.junit.experimental.categories.Category`).
This includes any test that:
- Uses `ZKTestEnv`
- Extends `ReplicatedLogtestcase`, `MultiServerBase`, or `BookkeeperFailuresBase`
- Starts multiple `Server` instances with ZooKeeper coordination

CI runs cluster and core tests in separate workflows. Forgetting the annotation means the test
will only run in the core workflow and will likely fail there due to missing infrastructure.

## Testing Code Changes
Always cover code changes with unit or integration tests. Run the new tests locally and ensure
they pass before opening a PR. Also run the existing tests related to the code touched by the
current work to catch regressions.

Never run the full `herddb-core` test suite — it takes a very long time. Instead, run single
tests or small batches using Maven's `-Dtest=...` selector, for example:
```
mvn -pl herddb-core -Dtest=MyChangedTest test
mvn -pl herddb-core -Dtest='Foo*Test,BarTest#someMethod' test
```

## Catching Exceptions
Never catch `Throwable` or bare `Exception` unless strictly necessary. Catch the
narrowest exception type(s) the code in the `try` block can actually throw. If a broad
catch is unavoidable (e.g. isolating a plugin boundary, preventing a background thread
from dying, top-level request handlers), add a comment explaining *why* the broad catch
is required.

## k3s-local Vector Benchmark — Monitoring

### Supervision loop
The agent supervises a running benchmark at ≤60 s cadence. Each tick:
1. `./scripts/check-cluster.sh`
2. `kubectl get pods -o wide` — watch RESTARTS column
3. `kubectl logs --tail=200 <pod>` for each component — scan for OOM/Exception/FATAL
4. `indexing-admin engine-stats --json` per IS replica — watch `apply_queue_size`,
   `total_estimated_memory_bytes`, `tailer_watermark_ledger`
5. File-server metrics via `curl http://localhost:9847/metrics` every few ticks

### indexing-admin
Available as `/usr/local/bin/indexing-admin` in the tools pod. Run via:
```
kubectl exec herddb-tools-0 -- indexing-admin <cmd> --server <IS>:9850 [--json]
```
Commands: `engine-stats`, `describe-index`, `list-indexes`, `instance-info`, `list-pks`.
Use pod DNS `herddb-indexing-service-<N>.herddb-indexing-service:9850`.
`list-instances` (ZK-based) may return empty — use `--server` directly.

### diagnostics.sh
`scripts/diagnostics.sh` handles both heap dumps and async-profiler profiles:
- Heap dump: `./scripts/diagnostics.sh [--pod <pod>] [--analyze]`
- Profiles:  `./scripts/diagnostics.sh --pod <pod> --profile [--profile-duration 30]`
  Collects cpu / wall / alloc / lock HTML flamegraphs from `/opt/profiler/bin/asprof`.
  Downloads to `$HERDDB_TESTS_HOME/profiles-<pod>-<ts>/`.

### File Server Metrics
The file server exposes Prometheus-format metrics on HTTP port **9847**:
```
kubectl --kubeconfig herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/.kubeconfig \
  exec herddb-file-server-0 -- curl -s http://localhost:9847/metrics
```

Key metrics:

| Metric | Meaning |
|---|---|
| `rfs_readrange_bytes` | Total bytes served to IS (from cache or MinIO) |
| `rfs_readrange_requests` | Number of `readFileRange` calls from IS |
| `rfs_writeblock_bytes` | Bytes written by IS during checkpoint |

Growing `rfs_readrange_bytes` during query phases = cache overflow → reads hitting MinIO.

### Verifying the Cache Configuration
The effective `cache.max.bytes` is written into the file server's properties file.
Check it with:
```
kubectl --kubeconfig herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/.kubeconfig \
  exec herddb-file-server-0 -- grep cache.max.bytes /opt/herddb/conf/fileserver.properties
```
The value should be **32212254720** (30 GiB) for the k3s-local benchmark cluster.

### Cache Configuration
The Helm template reads `.Values.fileServer.s3.cacheMaxBytes` (**not**
`.Values.fileServer.cacheMaxBytes`). Wrong placement silently uses 1 GiB default.
```yaml
fileServer:
  s3:
    cacheMaxBytes: 32212254720   # 30 GiB — MUST be under s3:
  storage:
    size: 30Gi
```
