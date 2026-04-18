#!/usr/bin/env bash
#
# Pull the HerdDB server pod log from the live k3s cluster and generate
# an HTML checkpoint-dynamics report.
#
# Wraps scripts/report-server-checkpoints.sh so you can run a one-shot
# analysis against a running cluster without manually collecting logs first.
#
# Usage:
#   ./scripts/analyze-server-checkpoints.sh [OPTIONS]
#
# Options:
#   --pod <name>       Server pod to query (default: herddb-server-0)
#   --lines <N>        Maximum log lines to fetch via kubectl logs
#                      (default: 100000; set to 0 for all lines)
#   --run-log <file>   Optional run-bench log; annotates ingest progress on
#                      the report timeline.
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
POD="herddb-server-0"
LINES=100000
RUN_LOG=""
OUTPUT=""
PREVIOUS_FLAG=""

usage() {
    grep '^#' "$0" | grep -v '#!/\|^# shellcheck\|^# ──\|^# Build\|^# shell' \
        | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)        POD="$2";      shift 2 ;;
        --lines)      LINES="$2";    shift 2 ;;
        --run-log)    RUN_LOG="$2";  shift 2 ;;
        --output)     OUTPUT="$2";   shift 2 ;;
        --previous)   PREVIOUS_FLAG="--previous"; shift ;;
        --help|-h)    usage ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

section "Fetching logs from pod $POD"

# Build kubectl logs args
TAIL_ARG=""
if [[ "$LINES" -gt 0 ]]; then
    TAIL_ARG="--tail=$LINES"
fi

TMPLOG="$(mktemp /tmp/herddb-server-ckpt-XXXXXX.log)"
trap 'rm -f "$TMPLOG"' EXIT

# shellcheck disable=SC2086
kubectl -n default logs "$POD" $TAIL_ARG ${PREVIOUS_FLAG} > "$TMPLOG"

LINE_COUNT=$(wc -l < "$TMPLOG")
echo "  Fetched $LINE_COUNT lines from $POD"

if [[ "$LINE_COUNT" -eq 0 ]]; then
    echo "ERROR: no log lines retrieved from $POD" >&2
    exit 1
fi

section "Generating server checkpoint report"

EXTRA_ARGS=()
[[ -n "$RUN_LOG"  ]] && EXTRA_ARGS+=( --run-log  "$RUN_LOG"  )
[[ -n "$OUTPUT"   ]] && EXTRA_ARGS+=( --output   "$OUTPUT"   )

"$SCRIPT_DIR/report-server-checkpoints.sh" \
    --server-log "$TMPLOG" \
    "${EXTRA_ARGS[@]}"
