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
# Generate an HTML IS checkpoint / vector-index-layout report from the local
# indexing service log. Live indexing-admin queries against localhost:9850
# are also run unless --no-live is passed.
#
# Usage:
#   ./scripts/analyze-is-checkpoints.sh [OPTIONS]
#
# Options:
#   --is-log <file>     Override log path (default: $CLUSTER_DIR/indexing-service.service.log)
#   --server <host:port>  indexing-admin endpoint (default: localhost:9850)
#   --tablespace <t>    Tablespace name (default: herd)
#   --table <t>         Table name (default: vector_bench)
#   --index <t>         Vector index name (default: vidx)
#   --no-live           Skip live indexing-admin queries (log-only mode).
#   --output <file>     Override the output HTML path.
#   --help              Show this help and exit.
#
# On success prints "REPORT=<path>" on the last line.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

IS_LOG=""
IS_SERVER="localhost:9850"
TABLESPACE="herd"
TABLE="vector_bench"
INDEX_NAME="vidx"
LIVE_FLAG=""
OUTPUT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --is-log)     IS_LOG="$2";           shift 2 ;;
        --server)     IS_SERVER="$2";        shift 2 ;;
        --tablespace) TABLESPACE="$2";       shift 2 ;;
        --table)      TABLE="$2";            shift 2 ;;
        --index)      INDEX_NAME="$2";       shift 2 ;;
        --no-live)    LIVE_FLAG="--no-live"; shift   ;;
        --output)     OUTPUT="$2";           shift 2 ;;
        --help|-h)    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$IS_LOG" ]]; then
    IS_LOG="$CLUSTER_DIR/indexing-service.service.log"
fi

if [[ ! -f "$IS_LOG" ]]; then
    echo "ERROR: indexing-service log not found at $IS_LOG" >&2
    exit 1
fi

ARGS=(
    --is-log "$IS_LOG"
    --server "$IS_SERVER"
    --tablespace "$TABLESPACE"
    --table "$TABLE"
    --index "$INDEX_NAME"
)
[[ -n "$LIVE_FLAG" ]] && ARGS+=( "$LIVE_FLAG" )
[[ -n "$OUTPUT"    ]] && ARGS+=( --output "$OUTPUT" )

"$SCRIPT_DIR/report-is-checkpoints.sh" "${ARGS[@]}"
