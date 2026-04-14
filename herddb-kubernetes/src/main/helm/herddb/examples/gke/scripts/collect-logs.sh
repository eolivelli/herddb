#!/usr/bin/env bash
#
# Dump logs from every HerdDB pod into reports/logs-<timestamp>/.
# Includes --previous logs if a pod has restarted.
#
# Usage: ./scripts/collect-logs.sh [--tail N]
#
# On success: prints "LOGS_DIR=<path>" on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

TAIL=2000
if [[ "${1:-}" == "--tail" ]]; then
    TAIL="$2"; shift 2
fi

TS="$(timestamp)"
LOGS_DIR="$REPORTS_DIR/logs-$TS"
mkdir -p "$LOGS_DIR"

section "Collecting logs into $LOGS_DIR"

kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
    -o wide > "$LOGS_DIR/pods.txt" 2>&1 || true
kubectl -n default describe pods -l app.kubernetes.io/instance=herddb \
    > "$LOGS_DIR/describe.txt" 2>&1 || true
kubectl -n default get events --sort-by=.lastTimestamp \
    > "$LOGS_DIR/events.txt" 2>&1 || true

while read -r pod; do
    [[ -z "$pod" ]] && continue
    echo "  - $pod"
    kubectl -n default logs "$pod" --all-containers --tail="$TAIL" \
        > "$LOGS_DIR/$pod.log" 2>&1 || true
    kubectl -n default logs "$pod" --all-containers --previous --tail="$TAIL" \
        > "$LOGS_DIR/$pod.previous.log" 2>/dev/null || \
        rm -f "$LOGS_DIR/$pod.previous.log"
done < <(herddb_pods)

echo ""
echo "LOGS_DIR=$LOGS_DIR"
