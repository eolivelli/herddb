#!/usr/bin/env bash
#
# Kill any running vector-bench Java process inside the herddb-tools pod.
# Safe to call even if no bench is running — exits 0 in that case.
#
# Usage:
#   ./scripts/kill-bench.sh [--help]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \?//'
    exit 0
fi

section "Killing vector-bench process in herddb-tools pod"

if kubectl --kubeconfig "$KUBECONFIG" -n default exec sts/herddb-tools -- \
        pkill -f "vector-testings" 2>&1; then
    echo "  vector-bench process(es) terminated."
else
    echo "  No vector-bench process found (or already stopped) — OK."
fi
