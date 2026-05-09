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
# Kill any running YCSB Java process (the bench client).
# Safe to call even if no bench is running — exits 0 in that case.
#
# Usage: ./scripts/kill-bench.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
    exit 0
fi

section "Killing YCSB bench process (pattern: site.ycsb)"

if pkill -f 'site.ycsb' 2>/dev/null; then
    echo "  YCSB process(es) terminated."
else
    echo "  No YCSB process found (or already stopped) — OK."
fi

# Also kill any ycsb shell wrapper processes.
pkill -f 'bin/ycsb' 2>/dev/null || true
