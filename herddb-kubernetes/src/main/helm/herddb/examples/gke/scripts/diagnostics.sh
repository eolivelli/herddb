#!/usr/bin/env bash
#
# Collect a JVM heap dump or async-profiler profiles from a running
# HerdDB pod and optionally analyse the heap dump with Eclipse Memory
# Analyser (MAT).
#
# Usage (heap dump):
#   ./scripts/diagnostics.sh [--pod <pod>] [--analyze] [--mat-home <path>]
#
# Usage (async-profiler profiles):
#   ./scripts/diagnostics.sh --pod <pod> --profile \
#       [--profile-duration <secs>] [--profiler-home <path>] \
#       [--per-thread]
#
# Usage (thread count):
#   ./scripts/diagnostics.sh --pod <pod> --thread-count [--thread-filter <pattern>]
#
# Usage (jstack):
#   ./scripts/diagnostics.sh --pod <pod> --jstack
#
# Defaults:
#   --pod               herddb-file-server-0
#   --analyze           disabled (pass --analyze to run MAT after download)
#   --mat-home          $MAT_HOME or ~/mat
#   --profile-duration  30  (total seconds for the single JFR recording)
#   --profiler-home     $PROFILER_HOME  (local async-profiler distribution
#                       containing lib/jfr-converter.jar)
#   --per-thread        disabled; when enabled, generates an additional set of
#                       per-thread flamegraphs (profile_cpu_threads.html, etc.)
#                       by passing --threads to jfr-converter for each event.
#                       Useful to diagnose uneven CPU utilisation across a
#                       thread pool (e.g. verifying all 16 IS worker threads
#                       are active vs. a single hot thread doing all the work).
#   --thread-count      print the count of live JVM threads matching --thread-filter.
#                       Uses jcmd Thread.print inside the pod — no profiler needed.
#                       Default filter: '' (counts all threads).
#                       Example: --thread-count --thread-filter indexing-apply-worker
#   --jstack            capture a full thread dump via jcmd Thread.print, download it
#                       to reports/jstack-<pod>-<timestamp>.txt, and print its path
#                       as JSTACK=<path>. Useful for diagnosing deadlocks or hangs.
#
# Output (heap dump):
#   Prints  HEAP_DUMP=<local-path>  on the last line on success.
#   If --analyze is passed also prints  MAT_REPORT=<dir>  pointing at
#   the MAT "leak_suspects" report directory.
#
# Output (profiles):
#   Runs async-profiler once (--all, ${PROFILE_DURATION}s) inside the pod
#   to record cpu / wall / alloc / lock events into a single profile.jfr,
#   downloads the JFR, then runs $PROFILER_HOME/lib/jfr-converter.jar
#   locally to generate profile_cpu.html, profile_wall.html,
#   profile_alloc.html and profile_lock.html.
#   With --per-thread also generates profile_cpu_threads.html, etc.
#   Prints  PROFILES_DIR=<local-dir>  on the last line on success.
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
PROFILER_HOME="${PROFILER_HOME:-}"
PER_THREAD=false
THREAD_COUNT=false
THREAD_FILTER=""
JSTACK=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)               POD="$2";               shift 2 ;;
        --analyze)           ANALYZE=true;            shift   ;;
        --mat-home)          MAT_HOME="$2";           shift 2 ;;
        --profile)           PROFILE=true;            shift   ;;
        --profile-duration)  PROFILE_DURATION="$2";  shift 2 ;;
        --profiler-home)     PROFILER_HOME="$2";     shift 2 ;;
        --per-thread)        PER_THREAD=true;         shift   ;;
        --thread-count)      THREAD_COUNT=true;       shift   ;;
        --thread-filter)     THREAD_FILTER="$2";      shift 2 ;;
        --jstack)            JSTACK=true;             shift   ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if $THREAD_COUNT; then
    section "Live thread count in pod $POD"

    echo "  Locating JVM PID..."
    JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
        'jcmd 2>/dev/null | grep -v "^[0-9]* Jcmd$" | awk "NR==1{print \$1}"')

    if [[ -z "$JVM_PID" ]]; then
        echo "ERROR: could not determine JVM PID in $POD (is jcmd available?)" >&2
        exit 1
    fi
    echo "  JVM PID: $JVM_PID"

    if [[ -n "$THREAD_FILTER" ]]; then
        echo "  Filter: '$THREAD_FILTER'"
        COUNT=$(kubectl -n default exec "$POD" -- sh -c \
            "jcmd $JVM_PID Thread.print 2>/dev/null | grep -c '$THREAD_FILTER'" || true)
        echo ""
        echo "  Threads matching '$THREAD_FILTER': $COUNT"
    else
        echo "  Filter: (none — counting all threads)"
        # jcmd Thread.print prints one '"<thread-name>"' header per thread
        COUNT=$(kubectl -n default exec "$POD" -- sh -c \
            "jcmd $JVM_PID Thread.print 2>/dev/null | grep -c '^\"'" || true)
        echo ""
        echo "  Total live threads: $COUNT"
    fi

    echo ""
    echo "THREAD_COUNT=$COUNT"
    exit 0
fi

if $JSTACK; then
    section "Thread dump (jstack) from pod $POD"

    echo "  Locating JVM PID..."
    JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
        'jcmd 2>/dev/null | grep -v "^[0-9]* Jcmd$" | awk "NR==1{print \$1}"')

    if [[ -z "$JVM_PID" ]]; then
        echo "ERROR: could not determine JVM PID in $POD (is jcmd available?)" >&2
        exit 1
    fi
    echo "  JVM PID: $JVM_PID"

    TS="$(timestamp)"
    LOCAL_JSTACK="$REPORTS_DIR/jstack-${POD}-${TS}.txt"
    mkdir -p "$REPORTS_DIR"

    echo "  Capturing thread dump..."
    kubectl -n default exec "$POD" -- sh -c \
        "jcmd $JVM_PID Thread.print 2>/dev/null" > "$LOCAL_JSTACK"

    echo ""
    echo "JSTACK=$LOCAL_JSTACK"
    exit 0
fi

if $PROFILE; then
    section "async-profiler JFR from pod $POD (${PROFILE_DURATION}s, cpu+wall+alloc+lock)"

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

    echo "  Locating JVM PID..."
    JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
        'jps 2>/dev/null | grep -v "^[0-9]* Jps$" | head -1 | cut -d" " -f1')

    if [[ -z "$JVM_PID" ]]; then
        echo "ERROR: could not determine JVM PID in $POD (is jps available?)" >&2
        exit 1
    fi
    echo "  JVM PID: $JVM_PID"

    TS_POD="$(date +%Y%m%d-%H%M%S)"
    REMOTE_DIR="/tmp/profile-${POD}-${TS_POD}"
    kubectl -n default exec "$POD" -- mkdir -p "$REMOTE_DIR"

    ASPROF="/opt/profiler/bin/asprof"

    echo "  Recording cpu+wall+alloc+lock for ${PROFILE_DURATION}s into profile.jfr..."
    kubectl -n default exec "$POD" -- \
        "$ASPROF" --all -d "$PROFILE_DURATION" "$JVM_PID" \
        -f "${REMOTE_DIR}/profile.jfr"

    TS_LOCAL="$(timestamp)"
    LOCAL_DIR="$REPORTS_DIR/profiles-${POD}-${TS_LOCAL}"
    mkdir -p "$LOCAL_DIR"

    echo "  Downloading profile.jfr to $LOCAL_DIR ..."
    kubectl -n default cp "${POD}:${REMOTE_DIR}/profile.jfr" "${LOCAL_DIR}/profile.jfr"

    kubectl -n default exec "$POD" -- rm -rf "$REMOTE_DIR" || true

    echo "  Generating aggregate flamegraphs with $CONVERTER_JAR ..."
    for event in cpu wall alloc lock; do
        echo "    - profile_${event}.html"
        java -jar "$CONVERTER_JAR" "--${event}" \
            "${LOCAL_DIR}/profile.jfr" \
            "${LOCAL_DIR}/profile_${event}.html"
    done

    if $PER_THREAD; then
        echo "  Generating per-thread flamegraphs (--threads) ..."
        for event in cpu wall alloc lock; do
            echo "    - profile_${event}_threads.html"
            java -jar "$CONVERTER_JAR" "--${event}" --threads \
                "${LOCAL_DIR}/profile.jfr" \
                "${LOCAL_DIR}/profile_${event}_threads.html"
        done
    fi

    echo ""
    echo "PROFILES_DIR=$LOCAL_DIR"
    exit 0
fi

section "Heap dump from pod $POD"

echo "  Locating JVM PID..."
JVM_PID=$(kubectl -n default exec "$POD" -- sh -c \
    'jcmd 2>/dev/null | grep -v "^[0-9]* Jcmd$" | awk "NR==1{print \$1}"')

if [[ -z "$JVM_PID" ]]; then
    echo "ERROR: could not determine JVM PID in $POD (is jcmd available?)" >&2
    exit 1
fi
echo "  JVM PID: $JVM_PID"

REMOTE_DUMP="/tmp/heapdump-$(date +%Y%m%d-%H%M%S).hprof"
echo "  Writing heap dump to $POD:$REMOTE_DUMP ..."
kubectl -n default exec "$POD" -- jcmd "$JVM_PID" GC.heap_dump "$REMOTE_DUMP"

TS="$(timestamp)"
LOCAL_DUMP="$REPORTS_DIR/heapdump-${POD}-${TS}.hprof"
echo "  Downloading to $LOCAL_DUMP ..."
kubectl -n default cp "${POD}:${REMOTE_DUMP}" "$LOCAL_DUMP"

kubectl -n default exec "$POD" -- rm -f "$REMOTE_DUMP" || true

echo ""
echo "HEAP_DUMP=$LOCAL_DUMP"

if $ANALYZE; then
    MAT_PARSE="$MAT_HOME/ParseHeapDump.sh"
    if [[ ! -x "$MAT_PARSE" ]]; then
        echo "ERROR: MAT not found at $MAT_PARSE (set --mat-home or \$MAT_HOME)" >&2
        exit 1
    fi

    section "Analyzing $LOCAL_DUMP with MAT"
    "$MAT_PARSE" "$LOCAL_DUMP" org.eclipse.mat.api:suspects org.eclipse.mat.api:overview

    MAT_REPORT_DIR="$(dirname "$LOCAL_DUMP")"
    echo ""
    echo "MAT_REPORT=$MAT_REPORT_DIR"
fi
