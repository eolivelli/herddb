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
# Watchdog: poll server + indexing-service every 60 s, restart any that died
# unexpectedly. Designed for unattended overnight operation.  Logs every
# state transition to $REPORTS_DIR/services-watchdog.log.
#
# Usage: ./scripts/services-watchdog.sh [--interval-s N] [--max-restarts-per-hour N]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

INTERVAL_S=60
MAX_RESTARTS_PER_HOUR=6
while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval-s) INTERVAL_S="$2"; shift 2 ;;
        --max-restarts-per-hour) MAX_RESTARTS_PER_HOUR="$2"; shift 2 ;;
        --help|-h) grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

WD_LOG="$REPORTS_DIR/services-watchdog.log"
echo "==> watchdog starting at $(date -Iseconds), interval=${INTERVAL_S}s, max-restarts/h=${MAX_RESTARTS_PER_HOUR}" | tee -a "$WD_LOG"

declare -A RESTART_COUNT
declare -A WINDOW_START
for svc in server indexing-service; do
    RESTART_COUNT[$svc]=0
    WINDOW_START[$svc]=$(date +%s)
done

while true; do
    NOW=$(date +%s)
    for svc in server indexing-service; do
        # Roll the rate-limiting window every hour.
        if (( NOW - WINDOW_START[$svc] >= 3600 )); then
            RESTART_COUNT[$svc]=0
            WINDOW_START[$svc]=$NOW
        fi

        PID="$(service_pid "$svc" || true)"
        if [[ -z "$PID" ]]; then
            # Either the cluster was torn down or the JVM died.
            # If $CLUSTER_DIR no longer exists, exit cleanly — teardown is intentional.
            if [[ ! -d "$CLUSTER_DIR" ]]; then
                echo "==> $(date -Iseconds) cluster dir gone, exiting watchdog" | tee -a "$WD_LOG"
                exit 0
            fi
            if (( RESTART_COUNT[$svc] >= MAX_RESTARTS_PER_HOUR )); then
                echo "==> $(date -Iseconds) $svc DOWN but rate-limit reached (${RESTART_COUNT[$svc]}/${MAX_RESTARTS_PER_HOUR} this hour), refusing to restart" | tee -a "$WD_LOG"
                continue
            fi
            echo "==> $(date -Iseconds) $svc DOWN — restarting (attempt #$((RESTART_COUNT[$svc] + 1)) this hour)" | tee -a "$WD_LOG"
            ( cd "$CLUSTER_DIR" && ./bin/service "$svc" start ) >> "$WD_LOG" 2>&1 || true
            RESTART_COUNT[$svc]=$((RESTART_COUNT[$svc] + 1))
            sleep 5
            NEW_PID="$(service_pid "$svc" || true)"
            if [[ -n "$NEW_PID" ]]; then
                echo "==> $(date -Iseconds) $svc restarted as pid=$NEW_PID" | tee -a "$WD_LOG"
            else
                echo "==> $(date -Iseconds) $svc restart FAILED — will retry next tick" | tee -a "$WD_LOG"
            fi
        fi
    done
    sleep "$INTERVAL_S"
done
