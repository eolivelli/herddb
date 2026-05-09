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
# Health check for the local HerdDB server (ycsb-bench mode).
# Prints process status for the server and exits non-zero if it is down
# or not serving JDBC connections.
#
# Usage: ./scripts/check-cluster.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

section "HerdDB server status"
pid="$(service_pid "server" || true)"
if [[ -n "$pid" ]]; then
    echo "  server  pid=$pid  RUNNING"
else
    echo "  server  pid=?    NOT RUNNING" >&2
    echo ""
    echo "ERROR: HerdDB server is not running." >&2
    exit 1
fi

# JDBC smoke test — if this fails the process is alive but not serving.
if ! "$CLUSTER_DIR/bin/herddb-cli.sh" -x jdbc:herddb:server:localhost \
        -u sa -pwd hdb \
        -q 'select * from sysnodes' >/dev/null 2>&1; then
    echo "" >&2
    echo "ERROR: JDBC smoke test failed — server is running but not serving." >&2
    exit 1
fi

echo ""
echo "HerdDB server is running and responding."
