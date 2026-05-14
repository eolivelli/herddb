#!/usr/bin/env bash
#
# Collect a JVM heap dump or async-profiler profiles from a running HerdDB pod
# and optionally analyse the heap dump with Eclipse Memory Analyser (MAT).
#
# Usage (heap dump):
#   ./scripts/diagnostics.sh [--pod <pod>] [--analyze] [--mat-home <path>]
#
# Usage (class histogram):
#   ./scripts/diagnostics.sh --pod <pod> --histo [--histo-live]
#
# Usage (async-profiler profiles):
#   ./scripts/diagnostics.sh --pod <pod> --profile \
#       [--profile-duration <secs>] [--profiler-home <path>]
#
# Defaults:
#   --pod               herddb-file-server-0
#   --analyze           disabled (pass --analyze to run MAT after download)
#   --mat-home          $MAT_HOME or ~/mat
#   --histo-live        disabled (pass --histo-live to force GC before histogram)
#   --profile-duration  30  (total seconds for the single JFR recording)
#   --profiler-home     $PROFILER_HOME  (local async-profiler distribution
#                       containing lib/jfr-converter.jar)
#
# Output (heap dump):
#   Prints  HEAP_DUMP=<local-path>  on the last line on success.
#   If --analyze is passed also prints  MAT_REPORT=<dir>  pointing at the
#   MAT "leak_suspects" report directory.
#
# Output (class histogram):
#   Runs  jcmd <pid> GC.class_histogram  inside the pod, downloads the output
#   to a local text file, and prints HISTO=<local-path> on the last line.
#   With --histo-live, first triggers a full GC so only live objects appear.
#
# Output (profiles):
#   Runs async-profiler once (--all, ${PROFILE_DURATION}s) inside the pod
#   to record cpu / wall / alloc / lock events into a single profile.jfr,
#   downloads the JFR, then runs $PROFILER_HOME/lib/jfr-converter.jar
#   locally to generate profile_cpu.html, profile_wall.html,
#   profile_alloc.html and profile_lock.html.
#   Prints  PROFILES_DIR=<local-dir>  on the last line on success.
#
# Requirements:
#   - kubectl on PATH and .kubeconfig present (run ./install.sh first)
#   - jcmd present in the target container  (it ships with the HerdDB JDK image)
#   - For --profile: /opt/profiler/bin/asprof present in the target container,
#     java on the local PATH, and $PROFILER_HOME/lib/jfr-converter.jar locally
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
HISTO=false
HISTO_LIVE=false
PROFILE=false
PROFILE_DURATION=30
PROFILER_HOME="${PROFILER_HOME:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)               POD="$2";               shift 2 ;;
        --analyze)           ANALYZE=true;            shift   ;;
        --mat-home)          MAT_HOME="$2";           shift 2 ;;
        --histo)             HISTO=true;              shift   ;;
        --histo-live)        HISTO_LIVE=true;         shift   ;;
        --profile)           PROFILE=true;            shift   ;;
        --profile-duration)  PROFILE_DURATION="$2";  shift 2 ;;
        --profiler-home)     PROFILER_HOME="$2";     shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════════
# PROFILE MODE — async-profiler single JFR recording (cpu / wall / alloc / lock)
# ═══════════════════════════════════════════════════════════════════════════════
if $PROFILE; then
    section "async-profiler JFR from pod $POD (${PROFILE_DURATION}s, cpu+wall+alloc+lock)"

    # ── 0. Local converter prerequisites ───────────────────────────────────────
    if [[ -z "$PROFILER_HOME" ]]; then
        echo "ERROR: PROFILER_HOME is not set." >&2
        echo "       Point it at a local async-profiler distribution containing" >&2
        echo "       lib/jfr-converter.jar (or pass --profiler-home <path>)." >&2
        exit 1
    fi
    CONVERTER_JAR="$PROFILER_HOME/lib/jfr-converter.jar"
    if [[ ! -f "$CONVERTER_JAR" ]]; then
        echo "ERROR: jfr-converter.jar not found at $CONVERTER_JAR" >&2
        exit 1
    fi

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

    # ── 3. Single JFR recording with all events (--all) ────────────────────────
    echo "  Recording cpu+wall+alloc+lock for ${PROFILE_DURATION}s into profile.jfr..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" --all -d "$PROFILE_DURATION" "$JVM_PID" \
        -f "${REMOTE_DIR}/profile.jfr"

    # ── 4. Download the JFR to the local machine ───────────────────────────────
    TS_LOCAL="$(timestamp)"
    LOCAL_DIR="$REPORTS_DIR/profiles-${POD}-${TS_LOCAL}"
    mkdir -p "$LOCAL_DIR"

    echo "  Downloading profile.jfr to $LOCAL_DIR ..."
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile.jfr" "${LOCAL_DIR}/profile.jfr"

    # Clean up remote copy to free ephemeral storage
    kubectl -n default exec "$POD" -- rm -rf "$REMOTE_DIR" || true

    # ── 5. Convert JFR into per-event-type flamegraphs locally ─────────────────
    echo "  Generating flamegraphs with $CONVERTER_JAR ..."
    for event in cpu wall alloc lock; do
        echo "    - profile_${event}.html"
        java -jar "$CONVERTER_JAR" "--${event}" \
            "${LOCAL_DIR}/profile.jfr" \
            "${LOCAL_DIR}/profile_${event}.html"
    done

    echo ""
    echo "PROFILES_DIR=$LOCAL_DIR"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════════
# HISTO MODE — jcmd GC.class_histogram (equivalent to jmap -histo)
# ═══════════════════════════════════════════════════════════════════════════════
if $HISTO; then
    section "JVM class histogram from pod $POD"

    # ── 1. Find the JVM PID ──────────────────────────────────────────────────
    echo "  Locating JVM PID..."
    JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
        'jps 2>/dev/null | grep -v "^[0-9]* Jps$" | head -1 | cut -d" " -f1')
    if [[ -z "$JVM_PID" ]]; then
        echo "ERROR: could not determine JVM PID in $POD (is jps available?)" >&2
        exit 1
    fi
    echo "  JVM PID: $JVM_PID"

    # ── 2. Optional full-GC before histogram to show only live objects ────────
    if $HISTO_LIVE; then
        echo "  Triggering full GC (--histo-live) ..."
        kubectl -n default exec "$POD" -- jcmd "$JVM_PID" GC.run 2>&1 || true
        sleep 2
    fi

    # ── 3. Collect histogram inside the pod and write to /tmp ─────────────────
    TS_POD="$(date +%Y%m%d-%H%M%S)"
    REMOTE_HISTO="/tmp/histo-${TS_POD}.txt"
    echo "  Running jcmd GC.class_histogram (top-300 classes) ..."
    kubectl -n default exec "$POD" -- sh -c \
        "jcmd ${JVM_PID} GC.class_histogram 2>&1 | head -310 > ${REMOTE_HISTO}"

    # ── 4. Download and display ───────────────────────────────────────────────
    TS_LOCAL="$(timestamp)"
    LOCAL_HISTO="$REPORTS_DIR/histo-${POD}-${TS_LOCAL}.txt"
    kubectl -n default cp "${POD}:${REMOTE_HISTO}" "$LOCAL_HISTO"
    kubectl -n default exec "$POD" -- rm -f "$REMOTE_HISTO" || true

    echo ""
    echo "=== Top heap consumers (num_instances  num_bytes  class_name) ==="
    cat "$LOCAL_HISTO"
    echo ""
    echo "HISTO=$LOCAL_HISTO"
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
