#!/usr/bin/env bash
#
# Collect a JVM heap dump from a running HerdDB pod and optionally analyse it
# with Eclipse Memory Analyser (MAT).
#
# Usage:
#   ./scripts/heap-dump.sh [--pod <pod>] [--analyze] [--mat-home <path>]
#
# Defaults:
#   --pod       herddb-file-server-0
#   --analyze   disabled (pass --analyze to run MAT after download)
#   --mat-home  $MAT_HOME or ~/mat
#
# Output:
#   Prints  HEAP_DUMP=<local-path>  on the last line on success.
#   If --analyze is passed also prints  MAT_REPORT=<dir>  pointing at the
#   MAT "leak_suspects" report directory.
#
# Requirements:
#   - kubectl on PATH and .kubeconfig present (run ./install.sh first)
#   - jcmd present in the target container  (it ships with the HerdDB JDK image)
#   - Enough ephemeral storage in the pod's /tmp to hold the dump (~heap-size)
#   - MAT ParseHeapDump.sh present at $MAT_HOME (only when --analyze is set)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

POD="herddb-file-server-0"
ANALYZE=false
MAT_HOME="${MAT_HOME:-${HOME}/mat}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)       POD="$2";      shift 2 ;;
        --analyze)   ANALYZE=true;  shift   ;;
        --mat-home)  MAT_HOME="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

section "Heap dump from pod $POD"

# ── 1. Find the JVM PID inside the pod ───────────────────────────────────────
echo "  Locating JVM PID..."
JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
    'jcmd 2>/dev/null | grep -v "^[0-9]* Jcmd$" | awk "NR==1{print \$1}"')

if [[ -z "$JVM_PID" ]]; then
    echo "ERROR: could not determine JVM PID in $POD (is jcmd available?)" >&2
    exit 1
fi
echo "  JVM PID: $JVM_PID"

# ── 2. Trigger the heap dump inside the pod ──────────────────────────────────
REMOTE_DUMP="/tmp/heapdump-$(date +%Y%m%d-%H%M%S).hprof"
echo "  Writing heap dump to $POD:$REMOTE_DUMP ..."
kubectl -n default exec "$POD" -- jcmd "$JVM_PID" GC.heap_dump "$REMOTE_DUMP"

# ── 3. Copy the dump to the local machine ────────────────────────────────────
TS="$(timestamp)"
LOCAL_DUMP="$REPORTS_DIR/heapdump-${POD}-${TS}.hprof"
echo "  Downloading to $LOCAL_DUMP ..."
kubectl -n default cp "${POD}:${REMOTE_DUMP}" "$LOCAL_DUMP"

# Clean up remote copy to free ephemeral storage in the pod
kubectl -n default exec "$POD" -- rm -f "$REMOTE_DUMP" || true

echo ""
echo "HEAP_DUMP=$LOCAL_DUMP"

# ── 4. Optional MAT analysis ─────────────────────────────────────────────────
if $ANALYZE; then
    MAT_PARSE="$MAT_HOME/ParseHeapDump.sh"
    if [[ ! -x "$MAT_PARSE" ]]; then
        echo "ERROR: MAT not found at $MAT_PARSE (set --mat-home or \$MAT_HOME)" >&2
        exit 1
    fi

    section "Analyzing $LOCAL_DUMP with MAT"
    # Run the leak suspects report (writes <dump>.index + reports/ next to the .hprof)
    "$MAT_PARSE" "$LOCAL_DUMP" org.eclipse.mat.api:suspects org.eclipse.mat.api:overview

    MAT_REPORT_DIR="$(dirname "$LOCAL_DUMP")"
    echo ""
    echo "MAT_REPORT=$MAT_REPORT_DIR"
fi
