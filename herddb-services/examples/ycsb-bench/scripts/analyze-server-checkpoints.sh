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
# Generate an HTML checkpoint-dynamics report for the local HerdDB server.
# Reads $CLUSTER_DIR/server.service.log by default.
#
# Usage:
#   ./scripts/analyze-server-checkpoints.sh [--server-log <file>] [--run-log <file>] [--output <file>]
#
# On success prints "REPORT=<path>" on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

SERVER_LOG=""
RUN_LOG=""
OUTPUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server-log) SERVER_LOG="$2"; shift 2 ;;
        --run-log)    RUN_LOG="$2";    shift 2 ;;
        --output)     OUTPUT="$2";     shift 2 ;;
        --help|-h)    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$SERVER_LOG" ]]; then
    SERVER_LOG="$CLUSTER_DIR/server.service.log"
fi

if [[ ! -f "$SERVER_LOG" ]]; then
    echo "ERROR: server log not found at $SERVER_LOG" >&2
    exit 1
fi

EXTRA_ARGS=()
[[ -n "$RUN_LOG" ]] && EXTRA_ARGS+=( --run-log "$RUN_LOG" )
[[ -n "$OUTPUT"  ]] && EXTRA_ARGS+=( --output  "$OUTPUT"  )

"$SCRIPT_DIR/report-server-checkpoints.sh" \
    --server-log "$SERVER_LOG" \
    "${EXTRA_ARGS[@]}"
