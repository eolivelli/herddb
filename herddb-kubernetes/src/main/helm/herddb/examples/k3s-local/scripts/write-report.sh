#!/usr/bin/env bash
#
# Turn a vector-bench run log into a markdown report.
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

# Extract useful bits from the log.
ARGS_LINE=$(grep -m1 '^# args:' "$RUN_LOG" || echo "# args: (unknown)")
START_LINE=$(grep -m1 '^# start:' "$RUN_LOG" || echo "# start: (unknown)")
END_LINE=$(grep -m1 '^# end:' "$RUN_LOG" || echo "# end: (unknown)")
EXIT_LINE=$(grep -m1 '^# exit:' "$RUN_LOG" || echo "# exit: (unknown)")
EXIT_CODE=$(echo "$EXIT_LINE" | awk '{print $NF}')

# BENCHMARK SUMMARY block (awk: print from the SUMMARY header until
# "Benchmark complete" or EOF).
SUMMARY=$(awk '
    /^========================================$/ && !seen { seen=1; in_summary=1 }
    in_summary { print }
    /^Benchmark complete/ { in_summary=0 }
' "$RUN_LOG" || true)

# phase=... key=value lines, extracted for a compact table.
PHASE_LINES=$(grep -E '^phase=' "$RUN_LOG" || true)

# Cluster state.
CLUSTER_STATE=$("$SCRIPT_DIR/check-cluster.sh" 2>&1 || true)

# Last 200 lines of the run log for an excerpt.
LOG_TAIL=$(tail -n 200 "$RUN_LOG")

{
    echo "# HerdDB vector-bench report — $TS"
    echo ""
    echo "**Status:** $([[ "$EXIT_CODE" == "0" ]] && echo ":white_check_mark: success" || echo ":x: failed (exit=$EXIT_CODE)")"
    echo ""
    echo "## Workload"
    echo ""
    echo '```'
    echo "${ARGS_LINE#\# args: }"
    echo "${START_LINE#\# }"
    echo "${END_LINE#\# }"
    echo "${EXIT_LINE#\# }"
    echo '```'
    echo ""
    echo "## Cluster state"
    echo ""
    echo '```'
    echo "$CLUSTER_STATE"
    echo '```'
    echo ""
    if [[ -n "$PHASE_LINES" ]]; then
        echo "## Phase metrics"
        echo ""
        echo '```'
        echo "$PHASE_LINES"
        echo '```'
        echo ""
    fi
    if [[ -n "$SUMMARY" ]]; then
        echo "## Benchmark summary"
        echo ""
        echo '```'
        echo "$SUMMARY"
        echo '```'
        echo ""
    fi
    echo "## Run log (last 200 lines)"
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
