#!/usr/bin/env bash
#
# Re-convert an existing async-profiler JFR recording into per-thread flamegraphs.
#
# Usage:
#   ./scripts/convert-jfr.sh --jfr-file <path> [--profiler-home <path>] [--out-dir <dir>]
#
# Options:
#   --jfr-file      Path to the existing profile.jfr file (required)
#   --profiler-home Path to async-profiler with lib/jfr-converter.jar
#                   (defaults to $PROFILER_HOME env var)
#   --out-dir       Directory to write output HTMLs; defaults to the directory
#                   containing the JFR file
#
# Output:
#   Writes profile_{cpu,wall,alloc,lock}_threads.html into --out-dir.
#   Prints THREADS_DIR=<out-dir> on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

JFR_FILE=""
PROFILER_HOME="${PROFILER_HOME:-}"
OUT_DIR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --jfr-file)      JFR_FILE="$2";      shift 2 ;;
        --profiler-home) PROFILER_HOME="$2"; shift 2 ;;
        --out-dir)       OUT_DIR="$2";       shift 2 ;;
        --help|-h)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$JFR_FILE" ]]; then
    echo "ERROR: --jfr-file is required" >&2
    exit 1
fi
if [[ ! -f "$JFR_FILE" ]]; then
    echo "ERROR: JFR file not found: $JFR_FILE" >&2
    exit 1
fi
if [[ -z "$PROFILER_HOME" ]]; then
    echo "ERROR: PROFILER_HOME is not set." >&2
    echo "       Pass --profiler-home <path> or set the PROFILER_HOME env variable." >&2
    exit 1
fi

CONVERTER_JAR="$PROFILER_HOME/lib/jfr-converter.jar"
if [[ ! -f "$CONVERTER_JAR" ]]; then
    echo "ERROR: jfr-converter.jar not found at $CONVERTER_JAR" >&2
    exit 1
fi

OUT_DIR="${OUT_DIR:-$(dirname "$JFR_FILE")}"
mkdir -p "$OUT_DIR"

section "Per-thread flamegraphs from $(basename "$JFR_FILE")"
echo "  Input:  $JFR_FILE"
echo "  Output: $OUT_DIR"

for event in cpu wall alloc lock; do
    OUT_HTML="${OUT_DIR}/profile_${event}_threads.html"
    echo "  Generating $OUT_HTML ..."
    java -jar "$CONVERTER_JAR" "--${event}" --threads \
        "$JFR_FILE" "$OUT_HTML"
done

echo ""
echo "THREADS_DIR=$OUT_DIR"
