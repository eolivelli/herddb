# Local HerdDB vector-bench example

End-to-end harness that installs HerdDB on the **local host** (no
Kubernetes, no Docker) from the `herddb-services-*.zip` artefact, runs
the vector-search benchmark, and produces a markdown report — or, on
failure, an issue body draft suitable for `gh issue create`.

The cluster layout matches `herddb-services/test-start-server.sh`: a
single `herddb-server` in standalone mode plus a co-located
`indexing-service`, sharing the commit log on disk. There is no
ZooKeeper, no BookKeeper and no container runtime in the loop.

This example is the local counterpart of the Kubernetes-based
`herddb-kubernetes/.../examples/k3s-local/` and `…/examples/gke/`. It is
the target of the `herddb-local-bench` Claude agent.

## Layout

```
herddb-services/examples/local-bench/
├── install.sh                # unzip + start server + start indexing service
├── teardown.sh               # stop services + delete cluster directory
├── README.md                 # this file
└── scripts/
    ├── analyze-is-checkpoints.sh        # IS checkpoint HTML report
    ├── analyze-server-checkpoints.sh    # server checkpoint HTML report
    ├── check-cluster.sh                 # health check (pids + JDBC ping)
    ├── collect-logs.sh                  # copy service logs into reports/
    ├── common.sh                        # shared helpers (sourced)
    ├── diagnostics.sh                   # heap dump / async-profiler
    ├── kill-bench.sh                    # pkill -f vector-testings
    ├── open-issue.sh                    # gh issue create with attachments
    ├── process-status.sh                # compact PID / RSS table
    ├── report-is-checkpoints.sh         # underlying IS HTML generator
    ├── report-server-checkpoints.sh     # underlying server HTML generator
    ├── run-bench.sh                     # invoke bin/vector-bench.sh
    └── write-report.sh                  # markdown report from a run log
```

## Where things live

- **Cluster install:** `$HERDDB_TESTS_HOME/cluster` (fallback:
  `examples/local-bench/workspace/cluster`)
- **Reports / run logs / heap dumps:** `$HERDDB_TESTS_HOME/reports`
  (fallback: `examples/local-bench/workspace/reports`)

`HERDDB_TESTS_HOME` is the same env var used by
`herddb-services/test-start-*.sh`.

## Quick start

```
# 1. Build the zip (once, or whenever you change the server)
mvn -pl herddb-services install -DskipTests -Dmaven.repo.local=~/dev/repo2

# 2. Install + start
cd herddb-services/examples/local-bench
./install.sh                           # picks up target/herddb-services-*.zip
                                       #   --server-heap 15g  (default)
                                       #   --indexing-heap 40g (default)

# 3. Health check
./scripts/check-cluster.sh

# 4. Run the workload
./scripts/run-bench.sh \
    --dataset sift10k -n 10000 -k 100 \
    --ingest-max-ops 40000 --ingest-threads 8 --batch-size 10000 \
    --checkpoint --wait-for-indexes \
    --checkpoint-timeout-seconds 1800 --wait-for-indexes-timeout 1800

# 5. Render the report
./scripts/write-report.sh "<RUN_LOG path printed in step 4>"

# 6. Tear down (when done)
./teardown.sh
```

## Diagnostics

Heap dump from the indexing-service JVM and Eclipse-MAT analysis:

```
./scripts/diagnostics.sh --service indexing-service --analyze
```

Async-profiler flamegraphs (cpu / wall / alloc / lock — 30 s each):

```
PROFILER_HOME=~/async-profiler ./scripts/diagnostics.sh \
    --service indexing-service --profile --profile-duration 30
```

`PROFILER_HOME` must point at a local async-profiler distribution
containing `bin/asprof` and `lib/jfr-converter.jar`.

## Filing an issue on failure

```
./scripts/collect-logs.sh                                    # → LOGS_DIR=...
./scripts/write-report.sh "<RUN_LOG>"                         # → REPORT=...
./scripts/open-issue.sh \
    --title "[local-bench] <phase> failure on <date>" \
    --body-file <draft.md> \
    --logs-dir <LOGS_DIR>                                     # → ISSUE_URL=...
```

`open-issue.sh` adds the `local-bench` label automatically.
