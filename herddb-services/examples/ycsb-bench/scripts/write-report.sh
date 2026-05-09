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
# Turn a YCSB run log into a markdown report.
# Extracts [OVERALL], [INSERT], [READ], [UPDATE], [SCAN],
# [READ-MODIFY-WRITE] stanzas and throughput/latency percentiles.
#
# Usage: ./scripts/write-report.sh <run-log-path>
#
# On success: prints "REPORT=<path>" on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

RUN_LOG="${1:-}"
if [[ -z "$RUN_LOG" || ! -f "$RUN_LOG" ]]; then
    echo "Usage: $0 <run-log-path>" >&2
    echo "Run log not found: $RUN_LOG" >&2
    exit 2
fi

TS="$(timestamp)"
REPORT="$REPORTS_DIR/report-$TS.md"

# Extract metadata from the run log header.
WORKLOAD_LINE=$(grep -m1 '^# workload:' "$RUN_LOG" || echo "# workload: (unknown)")
PHASE_LINE=$(grep -m1 '^# phase:' "$RUN_LOG" || echo "# phase: (unknown)")
THREADS_LINE=$(grep -m1 '^# threads:' "$RUN_LOG" || echo "# threads: (unknown)")
RECORDCOUNT_LINE=$(grep -m1 '^# recordcount:' "$RUN_LOG" || echo "# recordcount: (unknown)")
OPCOUNT_LINE=$(grep -m1 '^# operationcount:' "$RUN_LOG" || echo "# operationcount: (unknown)")
START_LINE=$(grep -m1 '^# start:' "$RUN_LOG" || echo "# start: (unknown)")
END_LINE=$(grep -m1 '^# end:' "$RUN_LOG" || echo "# end: (unknown)")
EXIT_LINE=$(grep -m1 '^# exit:' "$RUN_LOG" || echo "# exit: (unknown)")
EXIT_CODE=$(echo "$EXIT_LINE" | awk '{print $NF}')

# Extract YCSB summary stanzas ([OVERALL], [INSERT], [READ], [UPDATE], etc.)
SUMMARY=$(grep -E '^\[OVERALL\]|^\[INSERT\]|^\[READ\]|^\[UPDATE\]|^\[SCAN\]|^\[READ-MODIFY-WRITE\]' "$RUN_LOG" || true)

# Extract status lines (throughput progress).
STATUS_LINES=$(grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*sec:.*operations.*ops/sec' "$RUN_LOG" | tail -10 || true)

LOG_TAIL=$(tail -n 150 "$RUN_LOG")

{
    echo "# HerdDB YCSB-bench report — $TS"
    echo ""
    if [[ "$EXIT_CODE" == "0" ]]; then
        echo "**Status:** success"
    else
        echo "**Status:** failed (exit=$EXIT_CODE)"
    fi
    echo ""
    echo "## Workload parameters"
    echo ""
    echo '```'
    echo "${WORKLOAD_LINE#\# }"
    echo "${PHASE_LINE#\# }"
    echo "${THREADS_LINE#\# }"
    echo "${RECORDCOUNT_LINE#\# }"
    echo "${OPCOUNT_LINE#\# }"
    echo "${START_LINE#\# }"
    echo "${END_LINE#\# }"
    echo "${EXIT_LINE#\# }"
    echo '```'
    echo ""
    if [[ -n "$SUMMARY" ]]; then
        echo "## YCSB summary"
        echo ""
        echo '```'
        echo "$SUMMARY"
        echo '```'
        echo ""
    fi
    if [[ -n "$STATUS_LINES" ]]; then
        echo "## Last throughput status lines"
        echo ""
        echo '```'
        echo "$STATUS_LINES"
        echo '```'
        echo ""
    fi
    echo "## Run log (last 150 lines)"
    echo ""
    echo '<details><summary>expand</summary>'
    echo ""
    echo '```'
    echo "$LOG_TAIL"
    echo '```'
    echo ""
    echo '</details>'
    echo ""
    echo "---"
    echo ""
    echo "_Full log: \`$RUN_LOG\`_"
} > "$REPORT"

echo ""
echo "REPORT=$REPORT"
