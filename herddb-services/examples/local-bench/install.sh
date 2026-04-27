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
# Install and start a local HerdDB "cluster" — a single herddb-server in
# standalone mode plus a herddb-indexing-service, sharing the commit log on
# disk — mirroring test-start-server.sh but driven out of a stable directory
# so the bench scripts can find it.
#
# Usage:
#   ./install.sh [OPTIONS]
#
# Options:
#   --zip <path>                 Path to the herddb-services-*.zip to install.
#                                Default: auto-discover from herddb-services/target.
#   --server-heap <size>         -Xms/-Xmx for the server JVM (default: 17g).
#   --indexing-heap <size>       -Xms/-Xmx for the indexing-service JVM (default: 30g).
#   --indexing-rebuild-threads N Threads for vector index rebuild (default: 8).
#   --reuse                      Do not wipe $CLUSTER_DIR if it already exists;
#                                just (re-)start the services.
#   --help                       Show this help and exit.
#
# Directory layout:
#   Services (server, indexing-service binaries, dbdata, heap dumps) are
#   installed under $HERDDB_SERVER_HOME when that env var is set, allowing
#   them to live on a disk with plenty of space.  Falls back to
#   $HERDDB_TESTS_HOME/cluster (or workspace/cluster) when HERDDB_SERVER_HOME
#   is not set — preserving backward compatibility.
#   Commit-log files (txlog) live under $HERDDB_SERVER_COMMIT_LOG when that
#   env var is set, otherwise they stay inside $CLUSTER_DIR/dbdata/txlog.
#   Both the server (server.log.dir) and the indexing-service (log.dir)
#   are pointed at the chosen path so they share the same commit log.
#   Reports, datasets and profiles always go under $HERDDB_TESTS_HOME.
#   Previous service state is wiped unless --reuse is passed.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=scripts/common.sh
source "$SCRIPT_DIR/scripts/common.sh"

ZIP=""
SERVER_HEAP="17g"
INDEXING_HEAP="30g"
INDEXING_REBUILD_THREADS="8"
REUSE=false

usage() {
    grep '^#' "$0" | grep -v '#!/\|^# shellcheck\|^# ──' | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --zip)                       ZIP="$2"; shift 2 ;;
        --server-heap)               SERVER_HEAP="$2"; shift 2 ;;
        --indexing-heap)             INDEXING_HEAP="$2"; shift 2 ;;
        --indexing-rebuild-threads)  INDEXING_REBUILD_THREADS="$2"; shift 2 ;;
        --reuse)                     REUSE=true; shift ;;
        --help|-h)                   usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# ── Discover the zip if not given ─────────────────────────────────────────────
if [[ -z "$ZIP" ]]; then
    HERDDB_SERVICES_DIR="$(cd "$EXAMPLE_DIR/../.." && pwd)"
    ZIP="$(ls "$HERDDB_SERVICES_DIR"/target/herddb-service*zip 2>/dev/null | head -1 || true)"
fi

if [[ -z "$ZIP" || ! -f "$ZIP" ]]; then
    echo "ERROR: herddb-services zip not found." >&2
    echo "       Pass --zip <path> or run 'mvn -pl herddb-services install' first." >&2
    exit 1
fi

section "Installing HerdDB from $ZIP"
if [[ -n "${HERDDB_SERVER_HOME:-}" ]]; then
    echo "  cluster dir    : $CLUSTER_DIR  (from \$HERDDB_SERVER_HOME)"
else
    echo "  cluster dir    : $CLUSTER_DIR  (from \$HERDDB_TESTS_HOME/cluster fallback)"
fi
if [[ -n "$COMMIT_LOG_DIR" ]]; then
    echo "  commit log dir : $COMMIT_LOG_DIR  (from \$HERDDB_SERVER_COMMIT_LOG)"
else
    echo "  commit log dir : $CLUSTER_DIR/dbdata/txlog  (default, inside cluster dir)"
fi
echo "  reports dir    : $REPORTS_DIR  (from \$HERDDB_TESTS_HOME)"

if [[ -d "$CLUSTER_DIR" ]]; then
    if $REUSE; then
        echo "  --reuse set, keeping $CLUSTER_DIR in place."
    else
        # Stop any services that may still be running in-place before wiping.
        if [[ -x "$CLUSTER_DIR/bin/service" ]]; then
            ( cd "$CLUSTER_DIR" && ./bin/service indexing-service stop >/dev/null 2>&1 || true )
            ( cd "$CLUSTER_DIR" && ./bin/service server           stop >/dev/null 2>&1 || true )
        fi
        rm -rf "$CLUSTER_DIR"
    fi
fi

if [[ ! -d "$CLUSTER_DIR" ]]; then
    section "Unzipping into $CLUSTER_DIR"
    TMPUNZIP="$(mktemp -d)"
    unzip -q "$ZIP" -d "$TMPUNZIP"
    # The zip contains one top-level directory named herddb-*
    INNER="$(find "$TMPUNZIP" -mindepth 1 -maxdepth 1 -type d -name 'herddb*' | head -1)"
    if [[ -z "$INNER" ]]; then
        echo "ERROR: no herddb-* directory inside $ZIP" >&2
        exit 1
    fi
    mv "$INNER" "$CLUSTER_DIR"
    rmdir "$TMPUNZIP"
fi

cd "$CLUSTER_DIR"
mkdir -p tmp

# ── Configure the server ─────────────────────────────────────────────────────
section "Configuring server.properties"
CONFIGFILE="conf/server.properties"
sed -i 's/#http.enable=true/http.enable=true/g' "$CONFIGFILE"
sed -i 's/server.halt.on.tablespace.boot.error=true/server.halt.on.tablespace.boot.error=false/g' "$CONFIGFILE"

# Point the server at the co-located indexing service. This line is
# idempotent: if --reuse is in effect we only append it when missing.
if ! grep -q '^indexing.service.servers=' "$CONFIGFILE"; then
    echo "indexing.service.servers=localhost:9850" >> "$CONFIGFILE"
fi

# Shorten the periodic checkpoint interval from the 15-minute default to
# 7.5 minutes so that dirty data pages are flushed more frequently, keeping
# the in-memory data cache below its cap.  Without this, on long ingest runs
# the data cache reaches 100% of server.memory.data.percentage and every
# subsequent commit incurs a synchronous flush, dragging throughput down.
if ! grep -q '^server.checkpoint.period=' "$CONFIGFILE"; then
    echo "server.checkpoint.period=450000" >> "$CONFIGFILE"
fi

# Optionally relocate the commit log to $HERDDB_SERVER_COMMIT_LOG so that
# txlog writes go to a different (faster / larger) drive than the rest of
# the cluster data.  When unset we leave server.log.dir at its default
# (=txlog, relative to server.base.dir=dbdata).
if [[ -n "$COMMIT_LOG_DIR" ]]; then
    if grep -q '^server.log.dir=' "$CONFIGFILE"; then
        sed -i "s|^server.log.dir=.*|server.log.dir=$COMMIT_LOG_DIR|" "$CONFIGFILE"
    else
        echo "server.log.dir=$COMMIT_LOG_DIR" >> "$CONFIGFILE"
    fi
fi

# ── Configure the indexing service (shared commit-log layout) ────────────────
section "Configuring indexingservice.properties"
INDEXING_CONFIGFILE="conf/indexingservice.properties"
# Shared commit log: the indexing service tails the server's txlog directly.
# When $HERDDB_SERVER_COMMIT_LOG is set we point the IS at the same absolute
# path as the server; otherwise we use the relative path $CLUSTER_DIR/dbdata/txlog
# (server.properties sets server.log.dir=txlog under server.base.dir=dbdata).
if ! grep -q '^log.dir=' "$INDEXING_CONFIGFILE"; then
    if [[ -n "$COMMIT_LOG_DIR" ]]; then
        echo "log.dir=$COMMIT_LOG_DIR" >> "$INDEXING_CONFIGFILE"
    else
        echo "log.dir=dbdata/txlog" >> "$INDEXING_CONFIGFILE"
    fi
fi
if ! grep -q '^server.metadata.dir=' "$INDEXING_CONFIGFILE"; then
    echo "server.metadata.dir=dbdata/metadata" >> "$INDEXING_CONFIGFILE"
fi

# ── Start the server ─────────────────────────────────────────────────────────
section "Starting HerdDB server (heap=${SERVER_HEAP})"
export JAVA_OPTS="-XX:+UseG1GC -Duser.language=en -Xmx${SERVER_HEAP} -Xms${SERVER_HEAP} \
-Djava.net.preferIPv4Stack=true \
-XX:MaxDirectMemorySize=4g -Dio.netty.maxDirectMemory=4294967296 \
-XX:+DisableExplicitGC -Djava.awt.headless=true \
-Djava.util.logging.config.file=conf/logging.properties \
--add-modules jdk.incubator.vector -XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=$CLUSTER_DIR/server-heapdump.hprof \
-Dherddb.file.requirefsync=false \
-Djava.io.tmpdir=$CLUSTER_DIR/tmp"
bin/service server start

sleep 1

# ── Start the indexing service ───────────────────────────────────────────────
section "Starting indexing service (heap=${INDEXING_HEAP}, rebuild-threads=${INDEXING_REBUILD_THREADS})"
export JAVA_OPTS="-XX:+UseG1GC -Duser.language=en \
-Dherddb.vectorindex.rebuild.threads=${INDEXING_REBUILD_THREADS} \
-Djava.net.preferIPv4Stack=true -Xmx${INDEXING_HEAP} -Xms${INDEXING_HEAP} \
-XX:MaxDirectMemorySize=2g -Dio.netty.maxDirectMemory=4294967296 \
-Djava.awt.headless=true \
-Djava.util.logging.config.file=conf/logging.properties \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=$CLUSTER_DIR/indexingservice-heapdump.hprof \
-Dherddb.file.requirefsync=false \
--add-modules jdk.incubator.vector -Djava.io.tmpdir=$CLUSTER_DIR/tmp"
bin/service indexing-service start

sleep 2

# ── Smoke test ───────────────────────────────────────────────────────────────
section "Smoke test (JDBC)"
"$CLUSTER_DIR/bin/herddb-cli.sh" -x jdbc:herddb:server:localhost -q 'select * from sysnodes' || true
"$CLUSTER_DIR/bin/herddb-cli.sh" -x jdbc:herddb:server:localhost -q 'select * from systablespaces' || true

echo ""
echo "CLUSTER_DIR=$CLUSTER_DIR"
echo "REPORTS_DIR=$REPORTS_DIR"
