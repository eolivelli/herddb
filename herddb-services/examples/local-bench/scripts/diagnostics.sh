#!/usr/bin/env bash
#
# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Collect a JVM heap dump or async-profiler profiles from a local HerdDB
# service JVM (server or indexing-service). Everything runs on the local
# host — no kubectl, no containers.
#
# Usage (heap dump):
#   ./scripts/diagnostics.sh [--service server|indexing-service] [--analyze] [--mat-home <path>]
#
# Usage (async-profiler profiles):
#   ./scripts/diagnostics.sh --service <name> --profile \
#       [--profile-duration <secs>] [--profiler-home <path>] [--asprof <path>]
#
# Defaults:
#   --service           server
#   --analyze           disabled (pass --analyze to run MAT after the dump)
#   --mat-home          $MAT_HOME or ~/mat
#   --profile-duration  30
#   --profiler-home     $PROFILER_HOME (must contain lib/jfr-converter.jar)
#   --asprof            $ASPROF or $PROFILER_HOME/bin/asprof
#
# Output (heap dump):
#   Prints HEAP_DUMP=<path> on the last line. If --analyze is set also
#   prints MAT_REPORT=<dir>.
#
# Output (profiles):
#   Records one JFR file (--all events: cpu/wall/alloc/lock) for the given
#   duration and converts it to four HTML flamegraphs. Prints
#   PROFILES_DIR=<path> on the last line.
#
# Requirements:
#   - jcmd on PATH (ships with the JDK)
#   - For --profile: asprof binary and $PROFILER_HOME/lib/jfr-converter.jar
#   - For --analyze: Eclipse MAT (ParseHeapDump.sh) at $MAT_HOME
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

SERVICE="server"
ANALYZE=false
MAT_HOME="${MAT_HOME:-${HOME}/mat}"
PROFILE=false
PROFILE_DURATION=30
PROFILER_HOME="${PROFILER_HOME:-}"
ASPROF="${ASPROF:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --service)            SERVICE="$2";             shift 2 ;;
        --analyze)            ANALYZE=true;             shift   ;;
        --mat-home)           MAT_HOME="$2";            shift 2 ;;
        --profile)            PROFILE=true;             shift   ;;
        --profile-duration)   PROFILE_DURATION="$2";    shift 2 ;;
        --profiler-home)      PROFILER_HOME="$2";       shift 2 ;;
        --asprof)             ASPROF="$2";              shift 2 ;;
        --help|-h)            grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

case "$SERVICE" in
    server|indexing-service) : ;;
    *) echo "ERROR: --service must be 'server' or 'indexing-service' (got: $SERVICE)" >&2; exit 2 ;;
esac

JVM_PID="$(service_pid "$SERVICE" || true)"
if [[ -z "$JVM_PID" ]]; then
    echo "ERROR: $SERVICE is not running (no live pid in $CLUSTER_DIR/${SERVICE}.java.pid)" >&2
    exit 1
fi

# ═════════════════════════════════════════════════════════════════════════════
# PROFILE MODE
# ═════════════════════════════════════════════════════════════════════════════
if $PROFILE; then
    section "async-profiler JFR from $SERVICE (pid=$JVM_PID, ${PROFILE_DURATION}s)"

    if [[ -z "$ASPROF" && -n "$PROFILER_HOME" ]]; then
        ASPROF="$PROFILER_HOME/bin/asprof"
    fi
    if [[ -z "$ASPROF" || ! -x "$ASPROF" ]]; then
        echo "ERROR: asprof binary not found." >&2
        echo "       Pass --asprof <path> or set \$ASPROF / \$PROFILER_HOME." >&2
        exit 1
    fi
    if [[ -z "$PROFILER_HOME" ]]; then
        echo "ERROR: PROFILER_HOME is not set (needed for jfr-converter.jar)." >&2
        exit 1
    fi
    CONVERTER_JAR="$PROFILER_HOME/lib/jfr-converter.jar"
    if [[ ! -f "$CONVERTER_JAR" ]]; then
        echo "ERROR: jfr-converter.jar not found at $CONVERTER_JAR" >&2
        exit 1
    fi

    TS="$(timestamp)"
    LOCAL_DIR="$REPORTS_DIR/profiles-${SERVICE}-${TS}"
    mkdir -p "$LOCAL_DIR"

    echo "  Recording cpu+wall+alloc+lock for ${PROFILE_DURATION}s into profile.jfr..."
    "$ASPROF" --all -d "$PROFILE_DURATION" "$JVM_PID" -f "${LOCAL_DIR}/profile.jfr"

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

# ═════════════════════════════════════════════════════════════════════════════
# HEAP DUMP MODE
# ═════════════════════════════════════════════════════════════════════════════
section "Heap dump from $SERVICE (pid=$JVM_PID)"

if ! command -v jcmd >/dev/null 2>&1; then
    echo "ERROR: jcmd not found on PATH." >&2
    exit 1
fi

TS="$(timestamp)"
JVM_INFO_FILE="$REPORTS_DIR/jvminfo-${SERVICE}-${TS}.txt"
{
    echo "=== jcmd VM.command_line ==="
    jcmd "$JVM_PID" VM.command_line 2>&1 || true
    echo ""
    echo "=== jcmd VM.version ==="
    jcmd "$JVM_PID" VM.version 2>&1 || true
    echo ""
    echo "=== jcmd VM.flags ==="
    jcmd "$JVM_PID" VM.flags 2>&1 || true
    echo ""
    echo "=== jcmd VM.info ==="
    jcmd "$JVM_PID" VM.info 2>&1 || true
} > "$JVM_INFO_FILE"
echo "  JVM info written to $JVM_INFO_FILE"
echo "JVM_INFO=$JVM_INFO_FILE"

LOCAL_DUMP="$REPORTS_DIR/heapdump-${SERVICE}-${TS}.hprof"
echo "  Writing heap dump to $LOCAL_DUMP ..."
jcmd "$JVM_PID" GC.heap_dump "$LOCAL_DUMP"

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
