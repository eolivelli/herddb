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
#   ./scripts/run-bench.sh --dataset sift10k -n 10000 -k 100 \
#       --ingest-max-ops 1000 --checkpoint
#   ./scripts/run-bench.sh --dataset-url "$VECTORBENCH_DATASETS_BUCKET/mycorpus.fvecs.gz" \
#       -n 100000 --checkpoint
#
# On success: writes reports/run-<timestamp>.log and prints its path
# on the last line (prefixed "RUN_LOG="). Exits non-zero on failure.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <vector-bench args>" >&2
    echo "Example: $0 --dataset sift10k -n 10000 -k 100 --ingest-max-ops 1000 --checkpoint" >&2
    exit 2
fi

TS="$(timestamp)"
RUN_LOG="$REPORTS_DIR/run-$TS.log"

section "Running vector-bench inside sts/herddb-tools"
echo "  args: $*"
echo "  log:  $RUN_LOG"
echo ""

{
    echo "# vector-bench run $TS"
    echo "# args: $*"
    echo "# start: $(date -Iseconds)"
    echo ""
} > "$RUN_LOG"

set +e
kubectl -n default exec sts/herddb-tools -- \
    /opt/herddb/bin/vector-bench.sh --no-progress "$@" 2>&1 | tee -a "$RUN_LOG"
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
