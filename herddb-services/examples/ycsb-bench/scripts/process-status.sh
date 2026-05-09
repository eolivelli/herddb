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
# Compact process status for the local HerdDB server (ycsb-bench mode).
# Prints a 4-column table: NAME, PID, STATUS, RSS (MB).
#
# Usage: ./scripts/process-status.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

printf '%-20s %-8s %-10s %s\n' NAME PID STATUS RSS_MB
pid="$(service_pid "server" || true)"
if [[ -n "$pid" ]]; then
    rss_kb="$(ps -o rss= -p "$pid" 2>/dev/null | tr -d ' ' || echo 0)"
    rss_mb=$(( rss_kb / 1024 ))
    printf '%-20s %-8s %-10s %s\n' "server" "$pid" RUNNING "$rss_mb"
else
    printf '%-20s %-8s %-10s %s\n' "server" -       DOWN    -
fi
