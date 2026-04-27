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
# Stop the local HerdDB cluster and remove $CLUSTER_DIR.  When the commit
# log lives outside the cluster dir (i.e. $HERDDB_SERVER_COMMIT_LOG is set)
# that directory is also wiped, unless --keep-dir is passed.
#
# Usage:
#   ./teardown.sh [--keep-dir]
#
# Options:
#   --keep-dir   Stop services but do NOT delete $CLUSTER_DIR or the
#                external commit-log dir (useful when you want to inspect
#                data on disk).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/scripts/common.sh"

KEEP_DIR=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-dir) KEEP_DIR=true; shift ;;
        --help|-h)  grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ ! -d "$CLUSTER_DIR" ]]; then
    echo "Nothing to do — $CLUSTER_DIR does not exist."
    exit 0
fi

section "Stopping indexing service"
( cd "$CLUSTER_DIR" && bin/service indexing-service stop ) || true

section "Stopping HerdDB server"
( cd "$CLUSTER_DIR" && bin/service server stop ) || true

if ! $KEEP_DIR; then
    section "Removing $CLUSTER_DIR"
    rm -rf "$CLUSTER_DIR"
    # If the commit log was relocated outside the cluster dir via
    # $HERDDB_SERVER_COMMIT_LOG, it survived the rm above.  Wipe it too
    # so the next install starts from a clean slate.
    if [[ -n "$COMMIT_LOG_DIR" && -d "$COMMIT_LOG_DIR" ]]; then
        section "Removing external commit-log dir $COMMIT_LOG_DIR"
        rm -rf "${COMMIT_LOG_DIR:?}"/*
        rmdir "$COMMIT_LOG_DIR" 2>/dev/null || true
    fi
else
    echo "--keep-dir set, leaving $CLUSTER_DIR in place."
    if [[ -n "$COMMIT_LOG_DIR" ]]; then
        echo "                also leaving external commit-log dir $COMMIT_LOG_DIR in place."
    fi
fi
