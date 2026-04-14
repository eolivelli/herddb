#!/usr/bin/env bash
#
# Open a GitHub issue describing a failed gke-bench run.
# Attaches logs from a directory (produced by collect-logs.sh) as
# collapsible blocks in the issue body. Truncates each log to the last
# 500 lines so the body stays under GitHub's 65k-character limit.
#
# Usage:
#   ./scripts/open-issue.sh --title "<title>" --body-file <path> \
#       [--logs-dir <path>] [--label <label>] [--dry-run]
#
# Requires `gh` to be installed and authenticated.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORTS_DIR="${HERDDB_TESTS_HOME:-$EXAMPLE_DIR/reports}"
mkdir -p "$REPORTS_DIR"

TITLE=""
BODY_FILE=""
LOGS_DIR=""
DRY_RUN=false
LABEL="gke-bench"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --title)      TITLE="$2"; shift 2 ;;
        --body-file)  BODY_FILE="$2"; shift 2 ;;
        --logs-dir)   LOGS_DIR="$2"; shift 2 ;;
        --label)      LABEL="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=true; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$TITLE" || -z "$BODY_FILE" || ! -f "$BODY_FILE" ]]; then
    echo "Usage: $0 --title <title> --body-file <path> [--logs-dir <path>] [--dry-run]" >&2
    exit 2
fi

if ! $DRY_RUN; then
    if ! command -v gh >/dev/null 2>&1; then
        echo "ERROR: 'gh' CLI is not installed." >&2
        exit 1
    fi
    if ! gh auth status >/dev/null 2>&1; then
        echo "ERROR: 'gh' is not authenticated. Run 'gh auth login' first." >&2
        exit 1
    fi
fi

TS="$(date +%Y%m%d-%H%M%S)"
FINAL_BODY="$REPORTS_DIR/issue-body-$TS.md"

{
    cat "$BODY_FILE"
    if [[ -n "$LOGS_DIR" && -d "$LOGS_DIR" ]]; then
        echo ""
        echo "## Attached pod logs"
        echo ""
        echo "_Collected from \`$LOGS_DIR\`. Each log is truncated to the last 500 lines._"
        echo ""
        for f in "$LOGS_DIR"/*.txt "$LOGS_DIR"/*.log; do
            [[ -f "$f" ]] || continue
            name=$(basename "$f")
            echo ""
            echo "<details><summary><code>$name</code></summary>"
            echo ""
            echo '```'
            tail -n 500 "$f"
            echo '```'
            echo ""
            echo '</details>'
        done
    fi
} > "$FINAL_BODY"

echo "==> Issue body written to $FINAL_BODY"

if $DRY_RUN; then
    echo "==> --dry-run set, not creating GH issue."
    echo "ISSUE_BODY=$FINAL_BODY"
    exit 0
fi

echo "==> Creating GitHub issue..."
gh label create "$LABEL" --description "Automated reports from the GKE vector-bench agent" --force >/dev/null 2>&1 || true

URL=$(gh issue create --title "$TITLE" --body-file "$FINAL_BODY" --label "$LABEL")
echo "ISSUE_URL=$URL"
