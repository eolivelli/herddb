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
# Run a YCSB workload against the local HerdDB server using the JDBC binding.
#
# Usage:
#   ./scripts/run-bench.sh [OPTIONS]
#
# Options:
#   --background                    Launch YCSB as a background nohup process
#                                   and exit immediately. A PID file is written
#                                   to $REPORTS_DIR/run-<TS>.pid.
#   --phase load|run|both           Which YCSB phase(s) to run (default: both).
#   --workload <name>               YCSB workload name (default: workloada).
#   --threads N                     YCSB client threads (default: 200).
#   --recordcount N                 Number of records to load (default: 1000000).
#   --operationcount N              Number of operations to run (default: 1000000).
#   --ycsb-args "<extra>"           Extra arguments passed verbatim to ycsb.
#   --help                          Show this help and exit.
#
# Environment:
#   YCSB_HOME     Path to the YCSB binary distribution (must contain bin/ycsb
#                 and workloads/). Required.
#
# On success: last line of stdout is "RUN_LOG=<path>".
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_cluster_dir

BACKGROUND=false
PHASE="both"
WORKLOAD="workloada"
THREADS=200
RECORDCOUNT=1000000
OPERATIONCOUNT=1000000
EXTRA_YCSB_ARGS=""

usage() {
    grep '^#' "$0" | grep -v '#!/\|^# shellcheck' | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --background)      BACKGROUND=true; shift ;;
        --phase)           PHASE="$2"; shift 2 ;;
        --workload)        WORKLOAD="$2"; shift 2 ;;
        --threads)         THREADS="$2"; shift 2 ;;
        --recordcount)     RECORDCOUNT="$2"; shift 2 ;;
        --operationcount)  OPERATIONCOUNT="$2"; shift 2 ;;
        --ycsb-args)       EXTRA_YCSB_ARGS="$2"; shift 2 ;;
        --help|-h)         usage ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# ── Validate YCSB_HOME ────────────────────────────────────────────────────────
if [[ -z "${YCSB_HOME:-}" ]]; then
    echo "ERROR: YCSB_HOME is not set." >&2
    echo "       Set it to the YCSB binary distribution directory." >&2
    exit 1
fi
if [[ ! -x "$YCSB_HOME/bin/ycsb.sh" ]]; then
    echo "ERROR: $YCSB_HOME/bin/ycsb.sh not found or not executable." >&2
    exit 1
fi
WORKLOAD_FILE="$YCSB_HOME/workloads/$WORKLOAD"
if [[ ! -f "$WORKLOAD_FILE" ]]; then
    echo "ERROR: workload file not found: $WORKLOAD_FILE" >&2
    exit 1
fi

# ── Resolve the HerdDB JDBC jar ───────────────────────────────────────────────
JDBC_JAR="$(ls "$CLUSTER_DIR"/herddb-jdbc-*.jar 2>/dev/null | head -1 || true)"
if [[ -z "$JDBC_JAR" ]]; then
    echo "ERROR: herddb-jdbc-*.jar not found in $CLUSTER_DIR" >&2
    exit 1
fi

# ── Build YCSB JDBC properties file ──────────────────────────────────────────
TS="$(timestamp)"
PROPS_FILE="$REPORTS_DIR/ycsb-props-$TS.properties"
RUN_LOG="$REPORTS_DIR/run-$TS.log"
PID_FILE="$REPORTS_DIR/run-$TS.pid"

cat > "$PROPS_FILE" << EOF
db.driver=herddb.jdbc.Driver
db.url=jdbc:herddb:server:localhost
db.user=sa
db.passwd=hdb
db.batchsize=1000
jdbc.fetchsize=10
jdbc.autocommit=true
jdbc.batchupdateapi=false
recordcount=$RECORDCOUNT
operationcount=$OPERATIONCOUNT
EOF

section "Running YCSB workload=$WORKLOAD phase=$PHASE threads=$THREADS"
echo "  cluster:      $CLUSTER_DIR"
echo "  ycsb home:    $YCSB_HOME"
echo "  jdbc jar:     $JDBC_JAR"
echo "  props:        $PROPS_FILE"
echo "  log:          $RUN_LOG"
echo ""

{
    echo "# ycsb-bench run $TS"
    echo "# workload: $WORKLOAD"
    echo "# phase: $PHASE"
    echo "# threads: $THREADS"
    echo "# recordcount: $RECORDCOUNT"
    echo "# operationcount: $OPERATIONCOUNT"
    echo "# extra-ycsb-args: $EXTRA_YCSB_ARGS"
    echo "# start: $(date -Iseconds)"
    echo ""
} > "$RUN_LOG"

# Build the YCSB invocation command. The JDBC binding is selected via `-db
# site.ycsb.db.JdbcDBClient` and the HerdDB JDBC jar is added to the classpath
# with -cp. -s enables status output every 10 seconds.
# Build the extra-args array. Each whitespace-separated token in
# EXTRA_YCSB_ARGS is treated as a key=value property and passed to YCSB
# with a -p prefix, e.g. "jdbc.batchupdateapi=true" becomes
# "-p jdbc.batchupdateapi=true". Tokens that already start with "-" are
# forwarded verbatim so raw flags like "-target 5000" still work.
# shellcheck disable=SC2206
EXTRA_ARGS_ARRAY=()
if [[ -n "$EXTRA_YCSB_ARGS" ]]; then
    read -r -a _raw_extra <<< "$EXTRA_YCSB_ARGS"
    i=0
    while [[ $i -lt ${#_raw_extra[@]} ]]; do
        token="${_raw_extra[$i]}"
        if [[ "$token" == -* ]]; then
            # Raw flag — forward verbatim (and consume its value if next token
            # does not start with '-')
            EXTRA_ARGS_ARRAY+=("$token")
            next=$(( i + 1 ))
            if [[ $next -lt ${#_raw_extra[@]} && "${_raw_extra[$next]}" != -* ]]; then
                EXTRA_ARGS_ARRAY+=("${_raw_extra[$next]}")
                i=$(( next + 1 ))
            else
                i=$(( i + 1 ))
            fi
        else
            # key=value token — wrap with -p
            EXTRA_ARGS_ARRAY+=("-p" "$token")
            i=$(( i + 1 ))
        fi
    done
fi

run_ycsb_phase() {
    local phase="$1"
    local ycsb_command ycsb_class
    if [[ "$phase" == "load" ]]; then
        ycsb_command="-load"
    else
        ycsb_command="-t"
    fi
    ycsb_class="site.ycsb.Client"

    # Build the classpath mirroring what ycsb.sh does, but prepending the
    # HerdDB JDBC driver so it overrides any bundled JDBC driver in the
    # binding jar. ycsb.sh resets CLASSPATH internally so we cannot use env.
    local cp="$JDBC_JAR:$YCSB_HOME/conf"
    for f in "$YCSB_HOME"/lib/*.jar; do
        [[ -r "$f" ]] && cp="$cp:$f"
    done
    [[ -d "$YCSB_HOME/jdbc-binding/conf" ]] && cp="$cp:$YCSB_HOME/jdbc-binding/conf"
    for f in "$YCSB_HOME"/jdbc-binding/lib/*.jar; do
        [[ -r "$f" ]] && cp="$cp:$f"
    done

    echo "# --- phase $phase start: $(date -Iseconds)" >> "$RUN_LOG"
    java -classpath "$cp" \
        "$ycsb_class" "$ycsb_command" \
        -db site.ycsb.db.JdbcDBClient \
        -P "$WORKLOAD_FILE" \
        -P "$PROPS_FILE" \
        -threads "$THREADS" \
        -s \
        "${EXTRA_ARGS_ARRAY[@]+"${EXTRA_ARGS_ARRAY[@]}"}" \
        >> "$RUN_LOG" 2>&1
    echo "# --- phase $phase end: $(date -Iseconds)" >> "$RUN_LOG"
}

if $BACKGROUND; then
    # -----------------------------------------------------------------------
    # Background mode: launch as nohup so it survives shell closure and is
    # decoupled from any pipe. PID is saved for supervision.
    # -----------------------------------------------------------------------
    (
        if [[ "$PHASE" == "load" || "$PHASE" == "both" ]]; then
            run_ycsb_phase "load"
        fi
        if [[ "$PHASE" == "run" || "$PHASE" == "both" ]]; then
            run_ycsb_phase "run"
        fi
        echo "" >> "$RUN_LOG"
        echo "# end: $(date -Iseconds)" >> "$RUN_LOG"
        echo "# exit: 0" >> "$RUN_LOG"
    ) >> "$RUN_LOG" 2>&1 &
    BENCH_PID=$!
    echo "$BENCH_PID" > "$PID_FILE"
    {
        echo "# mode: background"
        echo "# pid: $BENCH_PID"
        echo "# pid-file: $PID_FILE"
    } >> "$RUN_LOG"
    echo "  Benchmark started in background (PID: $BENCH_PID)."
    echo "  PID file: $PID_FILE"
    echo "  Log:      $RUN_LOG"
    echo ""
    echo "RUN_LOG=$RUN_LOG"
    exit 0
fi

# ── Foreground mode ───────────────────────────────────────────────────────────
set +e
if [[ "$PHASE" == "load" || "$PHASE" == "both" ]]; then
    run_ycsb_phase "load"
fi
if [[ "$PHASE" == "run" || "$PHASE" == "both" ]]; then
    run_ycsb_phase "run"
fi
STATUS=$?
set -e

{
    echo ""
    echo "# end: $(date -Iseconds)"
    echo "# exit: $STATUS"
} >> "$RUN_LOG"

echo ""
echo "RUN_LOG=$RUN_LOG"
exit "$STATUS"
