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
# Drop and recreate the YCSB usertable schema.
# Idempotent: safe to call before every load phase.
#
# The table matches the schema expected by the YCSB JDBC binding:
#   - YCSB_KEY VARCHAR(255) PRIMARY KEY
#   - FIELD0 .. FIELD9 VARCHAR(100)
#
# Usage: ./scripts/create-table.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

CLI="$CLUSTER_DIR/bin/herddb-cli.sh"
JDBC_URL="jdbc:herddb:server:localhost"

section "Recreating YCSB usertable"

"$CLI" -x "$JDBC_URL" -u sa -pwd hdb -q "DROP TABLE IF EXISTS usertable" || true

"$CLI" -x "$JDBC_URL" -u sa -pwd hdb -q "CREATE TABLE usertable (
    YCSB_KEY VARCHAR(255) PRIMARY KEY,
    FIELD0 VARCHAR(100),
    FIELD1 VARCHAR(100),
    FIELD2 VARCHAR(100),
    FIELD3 VARCHAR(100),
    FIELD4 VARCHAR(100),
    FIELD5 VARCHAR(100),
    FIELD6 VARCHAR(100),
    FIELD7 VARCHAR(100),
    FIELD8 VARCHAR(100),
    FIELD9 VARCHAR(100)
)"

echo ""
echo "usertable created successfully."
