#!/usr/bin/env bash
#
# Pull the Indexing Service pod log from the live k3s cluster and generate
# an HTML checkpoint-dynamics and vector-index-layout report.
#
# Wraps scripts/report-is-checkpoints.sh so you can run a one-shot
# analysis against a running cluster without manually collecting logs first.
# Live indexing-admin queries (engine-stats, describe-index) are also run
# against the cluster unless --no-live is passed.
#
# Usage:
#   ./scripts/analyze-is-checkpoints.sh [OPTIONS]
#
# Options:
#   --replica <N>      IS replica index to inspect (default: 0).  Controls
#                      both the pod name (herddb-indexing-service-<N>) and
#                      the indexing-admin server endpoint.
#   --lines <N>        Maximum log lines to fetch via kubectl logs
#                      (default: 100000; set to 0 for all lines)
#   --tablespace <t>   Tablespace name for indexing-admin queries
#                      (default: herd)
#   --table <t>        Table name (default: vector_bench)
#   --index <t>        Vector index name (default: vidx)
#   --no-live          Skip live indexing-admin queries (log-only mode).
#   --run-log <file>   Optional run-bench log path (not used by IS report
#                      currently, kept for future annotation support).
#   --output <file>    Override the output HTML path.
#   --previous         Fetch previous container logs (use after a pod restart).
#   --help             Show this help and exit.
#
# On success prints "REPORT=<path>" on the last line (stdout).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# ── Defaults ──────────────────────────────────────────────────────────────────
IS_REPLICA=0
LINES=100000
TABLESPACE="herd"
TABLE="vector_bench"
INDEX_NAME="vidx"
LIVE=1
RUN_LOG=""
OUTPUT=""
PREVIOUS_FLAG=""

usage() {
    grep '^#' "$0" | grep -v '#!/\|^# shellcheck\|^# ──\|^# shell' \
        | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --replica)    IS_REPLICA="$2"; shift 2 ;;
        --lines)      LINES="$2";      shift 2 ;;
        --tablespace) TABLESPACE="$2"; shift 2 ;;
        --table)      TABLE="$2";      shift 2 ;;
        --index)      INDEX_NAME="$2"; shift 2 ;;
        --no-live)    LIVE=0;          shift   ;;
        --run-log)    RUN_LOG="$2";    shift 2 ;;
        --output)     OUTPUT="$2";     shift 2 ;;
        --previous)   PREVIOUS_FLAG="--previous"; shift ;;
        --help|-h)    usage ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

POD="herddb-indexing-service-${IS_REPLICA}"

section "Fetching logs from pod $POD"

TAIL_ARG=""
if [[ "$LINES" -gt 0 ]]; then
    TAIL_ARG="--tail=$LINES"
fi

TMPLOG="$(mktemp /tmp/herddb-is-ckpt-XXXXXX.log)"
trap 'rm -f "$TMPLOG"' EXIT

# shellcheck disable=SC2086
kubectl -n default logs "$POD" $TAIL_ARG ${PREVIOUS_FLAG} > "$TMPLOG"

LINE_COUNT=$(wc -l < "$TMPLOG")
echo "  Fetched $LINE_COUNT lines from $POD"

if [[ "$LINE_COUNT" -eq 0 ]]; then
    echo "ERROR: no log lines retrieved from $POD" >&2
    exit 1
fi

section "Generating IS checkpoint report"

EXTRA_ARGS=()
EXTRA_ARGS+=( --is-replica  "$IS_REPLICA"  )
EXTRA_ARGS+=( --tablespace  "$TABLESPACE"  )
EXTRA_ARGS+=( --table       "$TABLE"       )
EXTRA_ARGS+=( --index       "$INDEX_NAME"  )
[[ "$LIVE" -eq 0 ]] && EXTRA_ARGS+=( --no-live )
[[ -n "$OUTPUT"  ]] && EXTRA_ARGS+=( --output "$OUTPUT" )

"$SCRIPT_DIR/report-is-checkpoints.sh" \
    --is-log "$TMPLOG" \
    "${EXTRA_ARGS[@]}"
