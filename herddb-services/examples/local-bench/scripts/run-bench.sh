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
#   ./scripts/run-bench.sh --dataset sift10k -n 10000 -k 100 --checkpoint
#   ./scripts/run-bench.sh --dataset sift1m -n 100000 --checkpoint
#
# On success: writes $REPORTS_DIR/run-<timestamp>.log and prints its path
# on the last line (prefixed "RUN_LOG="). Exits non-zero on failure.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <vector-bench args>" >&2
    echo "Example: $0 --dataset sift10k -n 10000 -k 100 --checkpoint" >&2
    exit 2
fi

TS="$(timestamp)"
RUN_LOG="$REPORTS_DIR/run-$TS.log"

section "Running vector-bench against local cluster"
echo "  cluster: $CLUSTER_DIR"
echo "  args:    $*"
echo "  log:     $RUN_LOG"
echo ""

{
    echo "# vector-bench run $TS"
    echo "# args: $*"
    echo "# start: $(date -Iseconds)"
    echo ""
} > "$RUN_LOG"

# Default to a modest VectorBench heap but let callers override via env.
export VECTORBENCH_HEAP="${VECTORBENCH_HEAP:--Xms1g -Xmx2g}"

set +e
"$CLUSTER_DIR/bin/vector-bench.sh" --no-progress "$@" 2>&1 | tee -a "$RUN_LOG"
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
