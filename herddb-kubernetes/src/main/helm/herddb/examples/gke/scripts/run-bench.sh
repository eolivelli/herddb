#!/usr/bin/env bash
#
# Run a vector-bench workload against the HerdDB cluster on GKE.
# All arguments are forwarded to /opt/herddb/bin/vector-bench.sh inside
# the tools pod.
#
# The Java client is always driven with --no-progress so that the
# captured run log is \n-terminated line-per-sample output (rather than
# \r-overwritten spinner frames), making the log tail-friendly for
# supervision agents and much smaller on long runs.
#
# Usage:
#   ./scripts/run-bench.sh [--background] --dataset sift10k -n 10000 -k 100 \
#       --ingest-max-ops 1000 --checkpoint
#   ./scripts/run-bench.sh [--background] \
#       --dataset-url "gs://herddb-datasets/bigann/published/bigann_descriptor.json" \
#       -n 100000 --checkpoint
#
# --background: start the benchmark inside the pod as a pod-resident nohup
#   process and exit immediately.  The JVM writes its log to a file inside
#   the pod (/tmp/vector-bench-<TS>.log) and is fully decoupled from the
#   kubectl connection — a broken pipe, TCP timeout, or output-buffer
#   exhaustion cannot kill it.  Progress is monitored via the admin HTTP
#   API (GET /status) rather than streaming the log.  See issue #325.
#
# On success: writes reports/run-<timestamp>.log and prints its path
# on the last line (prefixed "RUN_LOG="). Exits non-zero on failure.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# ---------------------------------------------------------------------------
# Parse --background flag; collect the remaining vector-bench args.
# ---------------------------------------------------------------------------
BACKGROUND=false
BENCH_ARGS=()
for arg in "$@"; do
    if [[ "$arg" == "--background" ]]; then
        BACKGROUND=true
    else
        BENCH_ARGS+=("$arg")
    fi
done

if [[ ${#BENCH_ARGS[@]} -eq 0 ]]; then
    echo "Usage: $0 [--background] <vector-bench args>" >&2
    echo "Example: $0 --dataset sift10k -n 10000 -k 100 --ingest-max-ops 1000 --checkpoint" >&2
    exit 2
fi

TS="$(timestamp)"
RUN_LOG="$REPORTS_DIR/run-$TS.log"

section "Running vector-bench inside sts/herddb-tools"
echo "  args: ${BENCH_ARGS[*]}"
echo "  log:  $RUN_LOG"
echo ""

{
    echo "# vector-bench run $TS"
    echo "# args: ${BENCH_ARGS[*]}"
    echo "# start: $(date -Iseconds)"
    echo ""
} > "$RUN_LOG"

if [[ "$BACKGROUND" == "true" ]]; then
    # -----------------------------------------------------------------------
    # Background mode (issue #325): start the JVM inside the pod as a nohup
    # process so its lifecycle is fully decoupled from the kubectl connection.
    # A broken kubectl TCP session will NOT send SIGPIPE to the VectorBench
    # JVM.  The benchmark log lives inside the pod at REMOTE_LOG; progress is
    # available at any time via the admin HTTP API at /status.
    # -----------------------------------------------------------------------
    REMOTE_LOG="/tmp/vector-bench-${TS}.log"
    # printf %q safely escapes each arg for embedding inside a bash -c string.
    ESCAPED_ARGS=$(printf ' %q' "${BENCH_ARGS[@]}")
    REMOTE_PID=$(kubectl -n default exec sts/herddb-tools -- bash -c \
        "nohup /opt/herddb/bin/vector-bench.sh --no-progress${ESCAPED_ARGS} > '${REMOTE_LOG}' 2>&1 & echo \$!")
    {
        echo "# mode: background (pod-resident nohup, issue #325)"
        echo "# remote-log: $REMOTE_LOG"
        echo "# remote-pid: $REMOTE_PID"
    } >> "$RUN_LOG"
    echo "  Benchmark started in background inside pod (PID: $REMOTE_PID)."
    echo "  Pod log:    $REMOTE_LOG"
    echo "  Local log:  $RUN_LOG"
    echo ""
    echo "  Check status: kubectl -n default exec herddb-tools-0 -- curl -s http://localhost:8080/status"
    echo "  Follow log:   kubectl -n default exec sts/herddb-tools -- tail -f $REMOTE_LOG"
    echo "  Stop bench:   ./scripts/kill-bench.sh"
    echo ""
    echo "RUN_LOG=$RUN_LOG"
    exit 0
fi

# ---------------------------------------------------------------------------
# Foreground mode: redirect kubectl output directly to the log file (no tee)
# to avoid the automation output buffer (~4 k lines) filling up and sending
# SIGPIPE to the VectorBench JVM mid-run.  Supervision agents read the log
# file via Read; the admin API provides live progress.  See issue #325.
# ---------------------------------------------------------------------------
set +e
kubectl -n default exec sts/herddb-tools -- \
    /opt/herddb/bin/vector-bench.sh --no-progress "${BENCH_ARGS[@]}" >> "$RUN_LOG" 2>&1
status=$?
set -e

{
    echo ""
    echo "# end: $(date -Iseconds)"
    echo "# exit: $status"
} >> "$RUN_LOG"

echo ""
echo "RUN_LOG=$RUN_LOG"
exit "$status"
