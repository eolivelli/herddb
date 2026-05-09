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
# Copy the server log (and optionally YCSB run log) from $CLUSTER_DIR into
# $REPORTS_DIR/logs-<timestamp>/ so they are preserved across teardown.
#
# Usage: ./scripts/collect-logs.sh [--tail N]
#
# On success: prints "LOGS_DIR=<path>" on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

TAIL=0   # 0 = whole file
if [[ "${1:-}" == "--tail" ]]; then
    TAIL="$2"; shift 2
fi

require_cluster_dir

TS="$(timestamp)"
LOGS_DIR="$REPORTS_DIR/logs-$TS"
mkdir -p "$LOGS_DIR"

section "Collecting logs into $LOGS_DIR"

# Process status snapshot + JVM jcmd info (best-effort).
"$SCRIPT_DIR/process-status.sh" > "$LOGS_DIR/process-status.txt" 2>&1 || true

# Copy the server log.
src="$CLUSTER_DIR/server.service.log"
dst="$LOGS_DIR/server.log"
if [[ -f "$src" ]]; then
    if (( TAIL > 0 )); then
        tail -n "$TAIL" "$src" > "$dst"
    else
        cp "$src" "$dst"
    fi
    echo "  - server ($(wc -l < "$dst") lines)"
else
    echo "  (missing) $src"
fi

# JVM info (best-effort).
pid="$(service_pid "server" || true)"
if [[ -n "$pid" ]] && command -v jcmd >/dev/null 2>&1; then
    {
        echo "=== jcmd VM.command_line ==="
        jcmd "$pid" VM.command_line 2>&1 || true
        echo ""
        echo "=== jcmd VM.flags ==="
        jcmd "$pid" VM.flags 2>&1 || true
    } > "$LOGS_DIR/server.jvm-info.txt" || true
fi

# Most-recent YCSB run log (best-effort: pick the newest run-*.log in REPORTS_DIR).
LATEST_RUN_LOG="$(ls -t "$REPORTS_DIR"/run-*.log 2>/dev/null | head -1 || true)"
if [[ -n "$LATEST_RUN_LOG" && -f "$LATEST_RUN_LOG" ]]; then
    cp "$LATEST_RUN_LOG" "$LOGS_DIR/ycsb-run.log"
    echo "  - ycsb run log ($(wc -l < "$LOGS_DIR/ycsb-run.log") lines) from $LATEST_RUN_LOG"
fi

# Heap dumps dropped by -XX:+HeapDumpOnOutOfMemoryError (best-effort pointer).
for hprof in "$CLUSTER_DIR"/*-heapdump.hprof; do
    [[ -f "$hprof" ]] || continue
    echo "  (heap-dump) $hprof"
    echo "$hprof" >> "$LOGS_DIR/heap-dumps.txt"
done

echo ""
echo "LOGS_DIR=$LOGS_DIR"
