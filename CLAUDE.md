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

### File Server Metrics
The file server exposes Prometheus-format metrics on HTTP port **9847**.  Query them from
inside the cluster with:
```
kubectl --kubeconfig herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/.kubeconfig \
  exec herddb-file-server-0 -- curl -s http://localhost:9847/metrics
```

Key metrics to watch:

| Metric | Meaning |
|---|---|
| `rfs_readrange_bytes` | Total bytes served to the indexing service (read from MinIO or cache) |
| `rfs_readrange_requests` | Number of `readFileRange` calls from the indexing service |
| `rfs_writeblock_bytes` | Total bytes written by the indexing service during checkpoint |

If `rfs_readrange_bytes` keeps growing during query phases (not just during
`--checkpoint`), the disk cache is overflowing and every read falls through to MinIO.

### Verifying the Cache Configuration
The effective `cache.max.bytes` is written into the file server's properties file.
Check it with:
```
kubectl --kubeconfig herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/.kubeconfig \
  exec herddb-file-server-0 -- grep cache.max.bytes /opt/herddb/conf/fileserver.properties
```
The value should be **32212254720** (30 GiB) for the k3s-local benchmark cluster.

### Common Cache Misconfiguration
The Helm template reads `.Values.fileServer.s3.cacheMaxBytes`, **not**
`.Values.fileServer.cacheMaxBytes`.  Placing the value at the wrong YAML level silently
falls back to the 1 GB default (`1073741824`), causing enormous MinIO re-read traffic
and multi-minute query latencies.  The correct structure in `k3s-local/values.yaml` is:
```yaml
fileServer:
  s3:
    cacheMaxBytes: 32212254720   # 30 GiB — MUST be under s3:
  storage:
    size: 30Gi
```
