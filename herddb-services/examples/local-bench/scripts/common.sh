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

# Base directory for durable artefacts (cluster install + reports).
# Mirrors the convention of herddb-services/test-start-server.sh.
BASEDIR="${HERDDB_TESTS_HOME:-$EXAMPLE_DIR/workspace}"
mkdir -p "$BASEDIR"
BASEDIR="$(cd "$BASEDIR" && pwd)"

# The local HerdDB "cluster" lives under $HERDDB_TESTS_HOME/cluster,
# following the user-facing contract documented in README.md.
CLUSTER_DIR="$BASEDIR/cluster"

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
