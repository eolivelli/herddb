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
# Run async-profiler captures on server + indexing-service every 30 minutes
# until the cluster goes away or the loop is killed. Designed for unattended
# overnight operation: each round writes its own PROFILES_DIR under
# $REPORTS_DIR and a rolling tracking line into profile-loop.log.
#
# Usage:
#   ./scripts/profile-loop.sh [--interval-s N] [--duration-s N]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

INTERVAL_S=1800       # 30 min
DURATION_S=30
while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval-s) INTERVAL_S="$2"; shift 2 ;;
        --duration-s) DURATION_S="$2"; shift 2 ;;
        --help|-h)    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

LOOP_LOG="$REPORTS_DIR/profile-loop.log"
ROUND_DIR="$REPORTS_DIR/profile-rounds"
mkdir -p "$ROUND_DIR"

echo "==> profile-loop starting at $(date -Iseconds), interval=${INTERVAL_S}s, duration=${DURATION_S}s" | tee -a "$LOOP_LOG"

ROUND=0
while true; do
    ROUND=$((ROUND + 1))
    TS="$(timestamp)"
    echo "" | tee -a "$LOOP_LOG"
    echo "==> Round $ROUND starting at $(date -Iseconds)" | tee -a "$LOOP_LOG"

    # If either service is down, log and skip; the loop survives a teardown.
    SERVER_PID="$(service_pid server || true)"
    IS_PID="$(service_pid indexing-service || true)"
    if [[ -z "$SERVER_PID" || -z "$IS_PID" ]]; then
        echo "  one or both services are not running (server=$SERVER_PID, IS=$IS_PID); skipping round" | tee -a "$LOOP_LOG"
    else
        for svc in server indexing-service; do
            ROUND_LOG="$ROUND_DIR/round-${ROUND}-${svc}.log"
            echo "  - profiling $svc into $ROUND_LOG" | tee -a "$LOOP_LOG"
            if "$SCRIPT_DIR/diagnostics.sh" --service "$svc" --profile --profile-duration "$DURATION_S" \
                    > "$ROUND_LOG" 2>&1; then
                # Capture the PROFILES_DIR pointer back into the loop log.
                grep '^PROFILES_DIR=' "$ROUND_LOG" | tee -a "$LOOP_LOG" || true
            else
                echo "  WARN: $svc profiling failed; see $ROUND_LOG" | tee -a "$LOOP_LOG"
            fi
        done
    fi

    # Drop a marker file the supervisor can poll for.
    touch "$REPORTS_DIR/profile-loop.round-${ROUND}.done"

    echo "==> Round $ROUND complete at $(date -Iseconds); sleeping ${INTERVAL_S}s" | tee -a "$LOOP_LOG"
    sleep "$INTERVAL_S"
done
