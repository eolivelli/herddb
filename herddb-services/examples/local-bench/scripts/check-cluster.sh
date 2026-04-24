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
# Health check for the local HerdDB cluster.
# Prints process status for each service and exits non-zero if any is down.
#
# Usage: ./scripts/check-cluster.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

section "HerdDB services"
unhealthy=0
for service in server indexing-service; do
    pid="$(service_pid "$service" || true)"
    if [[ -n "$pid" ]]; then
        echo "  $service  pid=$pid  RUNNING"
    else
        echo "  $service  pid=?    NOT RUNNING"
        unhealthy=$((unhealthy + 1))
    fi
done

if (( unhealthy > 0 )); then
    echo ""
    echo "ERROR: $unhealthy service(s) not running." >&2
    exit 1
fi

# JDBC smoke test — if this fails the processes are alive but not serving.
if ! "$CLUSTER_DIR/bin/herddb-cli.sh" -x jdbc:herddb:server:localhost \
        -q 'select * from sysnodes' >/dev/null 2>&1; then
    echo "" >&2
    echo "ERROR: JDBC smoke test failed — server is running but not serving." >&2
    exit 1
fi

echo ""
echo "All HerdDB services are running and responding."
