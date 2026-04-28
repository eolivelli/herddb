#!/usr/bin/env bash
#
# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Run a vector-bench workload against the local HerdDB cluster.
# All arguments are forwarded to bin/vector-bench.sh inside $CLUSTER_DIR.
#
# The Java client is always driven with --no-progress so that the captured
# run log is \n-terminated line-per-sample output (rather than \r-overwritten
# spinner frames), making the log tail-friendly for supervision agents and
# much smaller on long runs.
#
# Usage:
#   ./scripts/run-bench.sh [--background] --dataset sift10k -n 10000 -k 100 --checkpoint
#   ./scripts/run-bench.sh [--background] --dataset sift1m -n 100000 --checkpoint
#
# --background: launch the VectorBench JVM as a local nohup background
#   process and exit immediately.  Output is appended to the same RUN_LOG.
#   A PID file is written to $REPORTS_DIR/run-<TS>.pid so the process can be
#   found and killed later.  Progress is monitored via the admin HTTP API
#   (GET /status) rather than following the log.  See issue #325.
#
# On success: writes $REPORTS_DIR/run-<timestamp>.log and prints its path
# on the last line (prefixed "RUN_LOG="). Exits non-zero on failure.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

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
    echo "Example: $0 --dataset sift10k -n 10000 -k 100 --checkpoint" >&2
    exit 2
fi

TS="$(timestamp)"
RUN_LOG="$REPORTS_DIR/run-$TS.log"

section "Running vector-bench against local cluster"
echo "  cluster: $CLUSTER_DIR"
echo "  args:    ${BENCH_ARGS[*]}"
echo "  log:     $RUN_LOG"
echo ""

{
    echo "# vector-bench run $TS"
    echo "# args: ${BENCH_ARGS[*]}"
    echo "# start: $(date -Iseconds)"
    echo ""
} > "$RUN_LOG"

# Default to a modest VectorBench heap but let callers override via env.
export VECTORBENCH_HEAP="${VECTORBENCH_HEAP:--Xms1g -Xmx2g}"

# Point dataset cache at $HERDDB_TESTS_HOME so re-runs skip the FTP download.
# The vector-bench jar also honours $VECTORBENCH_DATASET_DIR; prefer whatever
# the caller already set, falling back to $HERDDB_TESTS_HOME when available.
if [[ -z "${VECTORBENCH_DATASET_DIR:-}" && -n "${HERDDB_TESTS_HOME:-}" ]]; then
    export VECTORBENCH_DATASET_DIR="$HERDDB_TESTS_HOME"
fi

if [[ "$BACKGROUND" == "true" ]]; then
    # -----------------------------------------------------------------------
    # Background mode (issue #325): launch the JVM as a local nohup process
    # so it survives shell/terminal closure and is decoupled from any pipe.
    # Output is appended to RUN_LOG.  PID is saved to a .pid file so the
    # process can be stopped later via kill-bench.sh.
    # -----------------------------------------------------------------------
    PID_FILE="$REPORTS_DIR/run-${TS}.pid"
    nohup "$CLUSTER_DIR/bin/vector-bench.sh" --no-progress "${BENCH_ARGS[@]}" >> "$RUN_LOG" 2>&1 &
    BENCH_PID=$!
    echo "$BENCH_PID" > "$PID_FILE"
    {
        echo "# mode: background (local nohup, issue #325)"
        echo "# pid: $BENCH_PID"
        echo "# pid-file: $PID_FILE"
    } >> "$RUN_LOG"
    echo "  Benchmark started in background (PID: $BENCH_PID)."
    echo "  PID file:   $PID_FILE"
    echo "  Log:        $RUN_LOG"
    echo ""
    echo "  Check status: curl -s http://localhost:8080/status"
    echo "  Follow log:   tail -f $RUN_LOG"
    echo "  Stop bench:   ./scripts/kill-bench.sh"
    echo ""
    echo "RUN_LOG=$RUN_LOG"
    exit 0
fi

# ---------------------------------------------------------------------------
# Foreground mode: stream output through tee so the operator can watch live
# progress while also capturing it to the log file.
# ---------------------------------------------------------------------------
set +e
"$CLUSTER_DIR/bin/vector-bench.sh" --no-progress "${BENCH_ARGS[@]}" 2>&1 | tee -a "$RUN_LOG"
status=${PIPESTATUS[0]}
set -e

{
    echo ""
    echo "# end: $(date -Iseconds)"
    echo "# exit: $status"
} >> "$RUN_LOG"

echo ""
echo "RUN_LOG=$RUN_LOG"
exit "$status"
