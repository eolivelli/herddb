#!/usr/bin/env bash
#
# Collect a JVM heap dump or async-profiler profiles from a running HerdDB pod
# and optionally analyse the heap dump with Eclipse Memory Analyser (MAT).
#
# Usage (heap dump):
#   ./scripts/diagnostics.sh [--pod <pod>] [--analyze] [--mat-home <path>]
#
# Usage (async-profiler profiles):
#   ./scripts/diagnostics.sh --pod <pod> --profile [--profile-duration <secs>]
#
# Defaults:
#   --pod               herddb-file-server-0
#   --analyze           disabled (pass --analyze to run MAT after download)
#   --mat-home          $MAT_HOME or ~/mat
#   --profile-duration  30  (seconds per event type)
#
# Output (heap dump):
#   Prints  HEAP_DUMP=<local-path>  on the last line on success.
#   If --analyze is passed also prints  MAT_REPORT=<dir>  pointing at the
#   MAT "leak_suspects" report directory.
#
# Output (profiles):
#   Collects cpu / wall / alloc / lock profiles (HTML flamegraphs via
#   async-profiler at /opt/profiler/bin/asprof) and downloads them.
#   Prints  PROFILES_DIR=<local-dir>  on the last line on success.
#
# Requirements:
#   - kubectl on PATH and .kubeconfig present (run ./install.sh first)
#   - jcmd present in the target container  (it ships with the HerdDB JDK image)
#   - For --profile: /opt/profiler/bin/asprof present in the target container
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
PROFILE=false
PROFILE_DURATION=30

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)               POD="$2";               shift 2 ;;
        --analyze)           ANALYZE=true;            shift   ;;
        --mat-home)          MAT_HOME="$2";           shift 2 ;;
        --profile)           PROFILE=true;            shift   ;;
        --profile-duration)  PROFILE_DURATION="$2";  shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════════
# PROFILE MODE — async-profiler (cpu / wall / alloc / lock)
# ═══════════════════════════════════════════════════════════════════════════════
if $PROFILE; then
    section "async-profiler profiles from pod $POD (${PROFILE_DURATION}s per event)"

    # ── 1. Find the JVM PID inside the pod ─────────────────────────────────────
    echo "  Locating JVM PID..."
    JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
        'jps 2>/dev/null | grep -v "^[0-9]* Jps$" | head -1 | cut -d" " -f1')

    if [[ -z "$JVM_PID" ]]; then
        echo "ERROR: could not determine JVM PID in $POD (is jps available?)" >&2
        exit 1
    fi
    echo "  JVM PID: $JVM_PID"

    # ── 2. Remote output directory (in the pod's /tmp) ──────────────────────────
    TS_POD="$(date +%Y%m%d-%H%M%S)"
    REMOTE_DIR="/tmp/profile-${POD}-${TS_POD}"
    kubectl -n default exec "$POD" -- mkdir -p "$REMOTE_DIR"

    ASPROF="/opt/profiler/bin/asprof"

    # ── 3. Four event types (cpu / wall / alloc / lock) ─────────────────────────
    echo "  [1/4] CPU profile (${PROFILE_DURATION}s)..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" -d "$PROFILE_DURATION" "$JVM_PID" \
        -f "${REMOTE_DIR}/profile_cpu.html"

    echo "  [2/4] Wall-clock profile (${PROFILE_DURATION}s)..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" -d "$PROFILE_DURATION" -e wall "$JVM_PID" \
        -f "${REMOTE_DIR}/profile_wall.html"

    echo "  [3/4] Allocation profile (${PROFILE_DURATION}s)..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" -d "$PROFILE_DURATION" -e alloc "$JVM_PID" \
        -f "${REMOTE_DIR}/profile_mem.html"

    echo "  [4/4] Lock profile (${PROFILE_DURATION}s)..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" -d "$PROFILE_DURATION" -e lock "$JVM_PID" \
        -f "${REMOTE_DIR}/profile_locks.html"

    # ── 4. Download profiles to local machine ────────────────────────────────────
    TS_LOCAL="$(timestamp)"
    LOCAL_DIR="$REPORTS_DIR/profiles-${POD}-${TS_LOCAL}"
    mkdir -p "$LOCAL_DIR"

    echo "  Downloading profiles to $LOCAL_DIR ..."
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile_cpu.html"   "${LOCAL_DIR}/profile_cpu.html"
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile_wall.html"  "${LOCAL_DIR}/profile_wall.html"
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile_mem.html"   "${LOCAL_DIR}/profile_mem.html"
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile_locks.html" "${LOCAL_DIR}/profile_locks.html"

    # Clean up remote copies to free ephemeral storage
    kubectl -n default exec "$POD" -- rm -rf "$REMOTE_DIR" || true

    echo ""
    echo "PROFILES_DIR=$LOCAL_DIR"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════════
# HEAP DUMP MODE
# ═══════════════════════════════════════════════════════════════════════════════
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

# ── 2. Collect JVM command-line and VM info ───────────────────────────────────
echo "  Collecting JVM command-line and VM info..."
JVM_INFO_FILE="$REPORTS_DIR/jvminfo-${POD}-$(timestamp).txt"
{
    echo "=== jcmd VM.command_line ==="
    kubectl -n default exec "$POD" -- jcmd "$JVM_PID" VM.command_line 2>&1 || true
    echo ""
    echo "=== jcmd VM.version ==="
    kubectl -n default exec "$POD" -- jcmd "$JVM_PID" VM.version 2>&1 || true
    echo ""
    echo "=== jcmd VM.flags ==="
    kubectl -n default exec "$POD" -- jcmd "$JVM_PID" VM.flags 2>&1 || true
    echo ""
    echo "=== jcmd VM.info ==="
    kubectl -n default exec "$POD" -- jcmd "$JVM_PID" VM.info 2>&1 || true
} > "$JVM_INFO_FILE"
echo "  JVM info written to $JVM_INFO_FILE"
echo "JVM_INFO=$JVM_INFO_FILE"

# ── 3. Trigger the heap dump inside the pod ──────────────────────────────────
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
