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
# next-run.sh — convenience wrapper for a follow-up bigann-200M benchmark
# with tuned parameters, based on analysis of the first run where
# batch-size=1000 and server-heap=15g limited effective ingestion rate
# to ~16k rows/s instead of the targeted 30k.
#
# Key changes vs the first run
# ─────────────────────────────
#  --server-heap 25g         (was 15g)  more heap → fewer dirty-page evictions
#                                        → lower mean commit latency
#  --batch-size 10000        (was 1000)  amortises fixed commit overhead over 10x
#                                        more rows → expect 40–60k rows/s
#  --ingest-max-ops 100000   (was 30000) rate limiter kept well above the new
#                                        throughput ceiling
#
# Usage:
#   ./next-run.sh [--dry-run] [--help]
#
#   --dry-run   Print the commands that would be run without executing them.
#   --help      Show this help and exit.
#
# Prerequisites:
#   java, unzip, curl, gh must be on PATH.
#   HERDDB_TESTS_HOME should be set (defaults to workspace/ under this dir).
#   HERDDB_SERVER_HOME may be set to a large-disk path for the service install;
#   when unset, services are installed under $HERDDB_TESTS_HOME/cluster.
#   herddb-services-*.zip must exist under herddb-services/target/.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

DRY_RUN=false

usage() {
    grep '^#' "$0" | grep -v '#!/\|^# shellcheck\|^# ──\|^# ─' | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --help|-h)  usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

run() {
    if $DRY_RUN; then
        echo "[dry-run] $*"
    else
        "$@"
    fi
}

echo "========================================================================"
echo "  next-run.sh — bigann-200M with tuned server heap + batch size"
echo "========================================================================"
echo ""
echo "  server-heap  : 25g  (was 15g)"
echo "  indexing-heap: 40g  (unchanged)"
echo "  batch-size   : 10000 (was 1000)"
echo "  ingest-max-ops: 100000 (was 30000)"
echo ""

# ── Step 1: tear down the old cluster ────────────────────────────────────────
echo ">>> Step 1: teardown"
run "$SCRIPT_DIR/teardown.sh"

# ── Step 2: install with larger server heap ───────────────────────────────────
echo ""
echo ">>> Step 2: install --server-heap 25g"
run "$SCRIPT_DIR/install.sh" --server-heap 25g

# ── Step 3: health check ──────────────────────────────────────────────────────
echo ""
echo ">>> Step 3: health check"
run "$SCRIPT_DIR/scripts/check-cluster.sh"

# ── Step 4: run the benchmark ────────────────────────────────────────────────
echo ""
echo ">>> Step 4: run benchmark"
echo "    (launch with run_in_background for supervised run)"
echo "    command:"
echo ""
echo "    ./scripts/run-bench.sh \\"
echo "      --dataset bigann \\"
echo "      -n 200000000 \\"
echo "      -k 10 \\"
echo "      --ingest-max-ops 100000 \\"
echo "      --ingest-threads 8 \\"
echo "      --batch-size 10000 \\"
echo "      --checkpoint \\"
echo "      --checkpoint-timeout-seconds 1800 \\"
echo "      --wait-for-indexes \\"
echo "      --wait-for-indexes-timeout 1800 \\"
echo "      --index-num-shards 1"
echo ""
echo "Run the command above (with run_in_background: true) to start the benchmark."
echo "========================================================================"
