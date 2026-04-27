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
# Shared helpers for the local-bench scripts. Not meant to be executed
# directly.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Base directory for durable artefacts (reports, datasets, profiles, issue drafts).
# Mirrors the convention of herddb-services/test-start-server.sh.
BASEDIR="${HERDDB_TESTS_HOME:-$EXAMPLE_DIR/workspace}"
mkdir -p "$BASEDIR"
BASEDIR="$(cd "$BASEDIR" && pwd)"

# The local HerdDB "cluster" (server + indexing-service binaries and data) lives
# under $HERDDB_SERVER_HOME when that variable is set — use this to point at a
# disk with plenty of space for database files, commit logs, and heap dumps.
# Falls back to $HERDDB_TESTS_HOME/cluster (or workspace/cluster) so that
# existing single-disk setups keep working without any configuration change.
if [[ -n "${HERDDB_SERVER_HOME:-}" ]]; then
    mkdir -p "$HERDDB_SERVER_HOME"
    CLUSTER_DIR="$(cd "$HERDDB_SERVER_HOME" && pwd)"
else
    CLUSTER_DIR="$BASEDIR/cluster"
fi

# Optional: relocate the commit log onto a different drive by setting
# HERDDB_SERVER_COMMIT_LOG to an absolute path. When set, both the server
# (server.log.dir) and the indexing-service (log.dir) write/tail commit-log
# files at that path instead of the default $CLUSTER_DIR/dbdata/txlog. Useful
# when the commit log should live on a faster or larger drive than the rest
# of the cluster data. When unset, the commit log stays under $CLUSTER_DIR.
if [[ -n "${HERDDB_SERVER_COMMIT_LOG:-}" ]]; then
    mkdir -p "$HERDDB_SERVER_COMMIT_LOG"
    COMMIT_LOG_DIR="$(cd "$HERDDB_SERVER_COMMIT_LOG" && pwd)"
else
    COMMIT_LOG_DIR=""
fi

# Reports (run logs, diagnostics, heap dumps, issue drafts) go under
# $HERDDB_TESTS_HOME/reports so they survive teardown and stay outside the
# cluster install.
REPORTS_DIR="$BASEDIR/reports"
mkdir -p "$REPORTS_DIR"

timestamp() { date +%Y%m%d-%H%M%S; }

section() { printf '\n==> %s\n' "$1"; }

# Read a pid from a service pidfile inside $CLUSTER_DIR. Returns empty if the
# pidfile is missing or points at a dead process.
service_pid() {
    local service="$1"
    local pidfile="$CLUSTER_DIR/${service}.java.pid"
    [[ -f "$pidfile" ]] || return 0
    local pid
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    [[ -n "$pid" ]] || return 0
    if ps -p "$pid" >/dev/null 2>&1; then
        echo "$pid"
    fi
}

require_cluster_dir() {
    if [[ ! -d "$CLUSTER_DIR" ]]; then
        echo "ERROR: cluster directory not found at $CLUSTER_DIR" >&2
        echo "       run ./install.sh first." >&2
        exit 1
    fi
}
