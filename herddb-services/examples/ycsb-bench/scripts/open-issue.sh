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
# Open a GitHub issue describing a failed ycsb-bench run.
# Attaches logs from a directory (produced by collect-logs.sh) as
# collapsible blocks in the issue body. Truncates each log to the last
# 500 lines so the body stays under GitHub's 65k-character limit.
#
# Usage:
#   ./scripts/open-issue.sh --title "<title>" --body-file <path> [--logs-dir <path>] [--dry-run]
#
# Requires `gh` to be installed and authenticated.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

TITLE=""
BODY_FILE=""
LOGS_DIR=""
DRY_RUN=false
LABEL="ycsb-bench"

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
        echo "## Attached service logs"
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
gh label create "$LABEL" --description "Automated reports from the ycsb-bench agent" --force >/dev/null 2>&1 || true

URL=$(gh issue create --title "$TITLE" --body-file "$FINAL_BODY" --label "$LABEL")
echo "ISSUE_URL=$URL"
