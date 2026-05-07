#!/usr/bin/env bash
#
# Collect a series of jmap -histo:live snapshots from a pod at regular
# intervals. Useful for spotting accumulating objects over time.
#
# Usage:
#   ./scripts/jmap-histo-series.sh [--pod <pod>] [--count <n>] \
#       [--interval <seconds>]
#
# Defaults:
#   --pod       herddb-server-0
#   --count     10
#   --interval  120   (2 minutes between snapshots)
#
# Output:
#   Prints HISTO_<n>=<path> for each snapshot.
#   Prints HISTO_DIR=<dir>  on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

POD="herddb-server-0"
COUNT=10
INTERVAL=120

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)      POD="$2";      shift 2 ;;
        --count)    COUNT="$2";    shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,/^set -/{ /^set -/q; p }' "$0" | grep -v '^set -'
            exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

TS_SERIES="$(timestamp)"
HISTO_DIR="$REPORTS_DIR/histo-series-${POD}-${TS_SERIES}"
mkdir -p "$HISTO_DIR"

echo "==> jmap -histo:live series on $POD"
echo "    count=$COUNT  interval=${INTERVAL}s"
echo "    output dir: $HISTO_DIR"
echo ""

for i in $(seq 1 "$COUNT"); do
    echo "--- Snapshot $i / $COUNT  ($(date -Iseconds)) ---"
    # Reuse diagnostics.sh --histo; override REPORTS_DIR to land in our dir.
    HERDDB_TESTS_HOME="$HISTO_DIR" \
        "$SCRIPT_DIR/diagnostics.sh" --pod "$POD" --histo

    if [[ $i -lt $COUNT ]]; then
        echo "    sleeping ${INTERVAL}s until next snapshot..."
        sleep "$INTERVAL"
    fi
done

echo ""
echo "HISTO_DIR=$HISTO_DIR"
