#!/usr/bin/env bash
# poll-progress.sh — print rows ingested and IS segment count every 60 seconds.
# Usage: ./scripts/poll-progress.sh <pod-log-path> [interval-seconds]
#
# Output format (one line per tick, suitable for Monitor streaming):
#   [HH:MM:SS] rows=N/50000000 (X.X%)  segments=N
set -euo pipefail

POD_LOG="${1:?Usage: $0 <pod-log-path> [interval-seconds]}"
INTERVAL="${2:-60}"
KUBECONFIG_PATH="$(dirname "$0")/../.kubeconfig"

while true; do
    TS="$(date +%H:%M:%S)"

    # Row count from the pod log tail
    ROWS=""
    TOTAL=""
    if kubectl --kubeconfig "$KUBECONFIG_PATH" exec herddb-tools-0 -- \
            tail -n 5 "$POD_LOG" 2>/dev/null | grep -q 'rows='; then
        LAST="$(kubectl --kubeconfig "$KUBECONFIG_PATH" exec herddb-tools-0 -- \
            tail -n 5 "$POD_LOG" 2>/dev/null | grep 'rows=' | tail -n 1)"
        ROWS="$(echo "$LAST"  | grep -oP 'rows=\K[0-9]+'  || echo '?')"
        TOTAL="$(echo "$LAST" | grep -oP 'total=\K[0-9]+' || echo '50000000')"
    fi

    if [ -n "$ROWS" ] && [ "$ROWS" != "?" ] && [ "$TOTAL" != "?" ] && [ "$TOTAL" -gt 0 ]; then
        PCT="$(awk "BEGIN{printf \"%.1f\", $ROWS/$TOTAL*100}")"
    else
        PCT="?"
    fi

    # Segment count and IS vector count from indexing-admin list-indexes
    # Output format: TABLESPACE  TABLE  INDEX  VECTORS  SEGMENTS  STATUS
    LIST="$(kubectl --kubeconfig "$KUBECONFIG_PATH" exec herddb-tools-0 -- \
        indexing-admin list-indexes \
            --server herddb-indexing-service-0.herddb-indexing-service:9850 \
        2>/dev/null | grep -v '^NOTE:' | grep -v '^TABLESPACE' | grep 'vidx' || true)"
    SEGS="$(echo "$LIST" | awk '{print $5}')"
    IS_VECS="$(echo "$LIST" | awk '{print $4}')"
    if [ -z "$SEGS" ]; then SEGS="?"; fi
    if [ -z "$IS_VECS" ]; then IS_VECS="?"; fi

    echo "[$TS] rows=${ROWS:-?}/${TOTAL:-50000000} (${PCT}%)  is_vectors=${IS_VECS:-?}  segments=${SEGS}"

    # Stop if bench process is gone (phase=done)
    if kubectl --kubeconfig "$KUBECONFIG_PATH" exec herddb-tools-0 -- \
            tail -n 5 "$POD_LOG" 2>/dev/null | grep -q 'phase=done'; then
        echo "[$TS] Benchmark complete (phase=done). Stopping poll."
        break
    fi

    sleep "$INTERVAL"
done
