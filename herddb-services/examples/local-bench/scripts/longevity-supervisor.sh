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
# Autonomous overnight supervisor for the local-bench longevity workload.
# Polls the bench /status endpoint every 60 s, computes instantaneous
# ingest rate, and:
#   * if rate stays below --slow-threshold for --slow-ticks consecutive
#     samples, takes a diagnostic snapshot (heap dump + profiles + run-log
#     pointer) and opens a GitHub issue ONCE per supervisor lifetime;
#   * if services-watchdog.log shows --crash-storm-limit restarts within
#     the last 30 minutes, opens a critical GitHub issue, kills the bench
#     client, runs teardown.sh, and exits.
#
# Usage:
#   ./scripts/longevity-supervisor.sh \
#       [--interval-s 60] [--warmup-s 1800] [--slow-threshold 4000] \
#       [--slow-ticks 20] [--crash-storm-limit 3] [--expected-rate 8000]
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

INTERVAL_S=60
WARMUP_S=1800              # ignore first 30 min while rate stabilises
SLOW_THRESHOLD=4000        # rows/s; trigger if instantaneous rate < this
SLOW_TICKS=20              # need this many consecutive slow ticks
CRASH_STORM_LIMIT=3        # restart events within 30 min trigger storm
EXPECTED_RATE=8000         # for reporting only
ADMIN_PORT=8080

while [[ $# -gt 0 ]]; do
    case "$1" in
        --interval-s)        INTERVAL_S="$2"; shift 2 ;;
        --warmup-s)          WARMUP_S="$2"; shift 2 ;;
        --slow-threshold)    SLOW_THRESHOLD="$2"; shift 2 ;;
        --slow-ticks)        SLOW_TICKS="$2"; shift 2 ;;
        --crash-storm-limit) CRASH_STORM_LIMIT="$2"; shift 2 ;;
        --expected-rate)     EXPECTED_RATE="$2"; shift 2 ;;
        --admin-port)        ADMIN_PORT="$2"; shift 2 ;;
        --help|-h)
            grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

SUP_LOG="$REPORTS_DIR/longevity-supervisor.log"
WD_LOG="$REPORTS_DIR/services-watchdog.log"

slowdown_issue_filed=false
start_ts=$(date +%s)

log() { printf '%s %s\n' "$(date -Iseconds)" "$*" | tee -a "$SUP_LOG"; }

log "==> longevity-supervisor starting (interval=${INTERVAL_S}s warmup=${WARMUP_S}s slow_threshold=${SLOW_THRESHOLD}/s slow_ticks=${SLOW_TICKS} crash_storm=${CRASH_STORM_LIMIT}/30m expected_rate=${EXPECTED_RATE}/s)"

# ── Slowdown handler ─────────────────────────────────────────────────────────
handle_slowdown() {
    local cur_rate="$1"
    log "==> SLOWDOWN DETECTED: ${cur_rate} rows/s sustained for ${SLOW_TICKS} ticks (~$((SLOW_TICKS * INTERVAL_S / 60)) min)"

    # Capture run log path for the issue body.
    local run_log
    run_log="$(ls -1t "$REPORTS_DIR"/run-2026*.log 2>/dev/null | head -1 || true)"

    # Take a heap dump + analysis (server side — that's where the bottleneck
    # is, per issue #304). Skip MAT to keep this fast and unattended-safe.
    local hd_log
    hd_log="$REPORTS_DIR/slowdown-heapdump-$(timestamp).log"
    log "    capturing server heap dump (no MAT) → $hd_log"
    "$SCRIPT_DIR/diagnostics.sh" --service server >> "$hd_log" 2>&1 || true
    local heap_dump
    heap_dump="$(grep -m1 '^HEAP_DUMP=' "$hd_log" | sed 's/^HEAP_DUMP=//' || true)"

    # Take async-profiler snapshots of both services.
    local srv_prof_log is_prof_log srv_prof is_prof
    srv_prof_log="$REPORTS_DIR/slowdown-prof-server-$(timestamp).log"
    log "    capturing server profile → $srv_prof_log"
    "$SCRIPT_DIR/diagnostics.sh" --service server --profile --profile-duration 30 \
        >> "$srv_prof_log" 2>&1 || true
    srv_prof="$(grep -m1 '^PROFILES_DIR=' "$srv_prof_log" | sed 's/^PROFILES_DIR=//' || true)"

    is_prof_log="$REPORTS_DIR/slowdown-prof-is-$(timestamp).log"
    log "    capturing IS profile → $is_prof_log"
    "$SCRIPT_DIR/diagnostics.sh" --service indexing-service --profile --profile-duration 30 \
        >> "$is_prof_log" 2>&1 || true
    is_prof="$(grep -m1 '^PROFILES_DIR=' "$is_prof_log" | sed 's/^PROFILES_DIR=//' || true)"

    # Snapshot of /status for the body.
    local status_json mem_json
    status_json="$(curl -s "http://localhost:${ADMIN_PORT}/status" 2>/dev/null || echo '{}')"
    mem_json="$(curl -s http://localhost:9845/metrics 2>/dev/null | grep -E 'jvm_memory_direct_bytes_(used|max)|netty_(used|max)_direct_memory' | grep -v '^#' || true)"

    # Build issue body.
    local body_file
    body_file="$REPORTS_DIR/issue-body-slowdown-$(timestamp).md"
    cat > "$body_file" <<EOF
# bigann 1B-vector longevity bench (batch=1000, max-ops=10000): sustained throughput slowdown

Filed automatically by \`longevity-supervisor.sh\` after detecting that
the instantaneous ingest rate stayed below **${SLOW_THRESHOLD} rows/s**
(half of the expected steady-state rate of ~${EXPECTED_RATE} rows/s)
for **${SLOW_TICKS} consecutive ${INTERVAL_S}-second samples**
(≈ $((SLOW_TICKS * INTERVAL_S / 60)) minutes).

This is a follow-up to #304 with a different workload shape (smaller
batches, lower rate cap) to confirm whether the slowdown reproduces
under producer back-pressure rather than commit-eviction back-pressure.

## Workload

- \`bigann\` 1 B vectors × 128-dim uint8
- \`--ingest-threads 8 --batch-size 1000 --ingest-max-ops 10000\`
- \`--checkpoint --wait-for-indexes --index-num-shards 1\`
- timeouts: 2,147,483,647 s (effectively infinite)
- See \`herddb-services/examples/local-bench/install.sh\` for service heap / direct memory / checkpoint period

## Snapshot at trigger time

\`\`\`json
$status_json
\`\`\`

\`\`\`
$mem_json
\`\`\`

Detected rate at trigger: **${cur_rate} rows/s**

## Artifacts captured

| Path | What |
|---|---|
EOF
    [[ -n "$heap_dump" ]] && echo "| \`$heap_dump\` | server heap dump (live) |" >> "$body_file"
    [[ -n "$srv_prof" ]] && echo "| \`$srv_prof\` | server async-profiler {cpu,wall,alloc,lock} |" >> "$body_file"
    [[ -n "$is_prof" ]] && echo "| \`$is_prof\` | indexing-service async-profiler |" >> "$body_file"
    [[ -n "$run_log" ]] && echo "| \`$run_log\` | full run log |" >> "$body_file"
    {
        echo "| \`$REPORTS_DIR/profile-rounds/\` | rolling 30-min profiles |"
        echo "| \`$REPORTS_DIR/longevity-supervisor.log\` | this supervisor's tick log |"
        echo "| \`$REPORTS_DIR/services-watchdog.log\` | watchdog activity |"
        echo ""
        echo "## Recommendation"
        echo ""
        echo "Compare these flamegraphs against the heap dump from #304 — if the"
        echo "data-cache eviction-flush hot-path is dominant in CPU+wall here too,"
        echo "the root cause is shared with #304. If a different hot-path emerges"
        echo "(e.g. commit-log fsync, BLink lookup, gRPC encoding), this is a new"
        echo "issue mode worth its own fix."
    } >> "$body_file"

    log "    issue body → $body_file"

    # Open the issue (no logs-dir attachment to keep the body small).
    if "$SCRIPT_DIR/open-issue.sh" --title \
            "bigann 1B-vector longevity bench (batch=1000): sustained throughput slowdown" \
            --body-file "$body_file" >> "$SUP_LOG" 2>&1; then
        log "    GitHub issue opened (see longevity-supervisor.log for ISSUE_URL)"
    else
        log "    WARN: failed to open GitHub issue; body remains at $body_file"
    fi
}

# ── Crash-storm handler ──────────────────────────────────────────────────────
handle_crash_storm() {
    local restart_count="$1"
    log "==> CRASH STORM DETECTED: ${restart_count} restart(s) within last 30 min"

    # Collect logs first.
    local logs_dir
    "$SCRIPT_DIR/collect-logs.sh" >> "$SUP_LOG" 2>&1 || true
    logs_dir="$(grep '^LOGS_DIR=' "$SUP_LOG" | tail -1 | sed 's/^LOGS_DIR=//' || true)"

    local body_file
    body_file="$REPORTS_DIR/issue-body-crash-storm-$(timestamp).md"
    cat > "$body_file" <<EOF
# bigann 1B-vector longevity bench: server/IS crash storm

Filed automatically by \`longevity-supervisor.sh\` after detecting that
\`services-watchdog.sh\` had to restart cluster services
**${restart_count} times within the last 30 minutes**, which exceeds the
crash-storm threshold of ${CRASH_STORM_LIMIT}.

Likely a recurrence of the silent server-JVM kill described in #304
(no \`hs_err_pid\`, no kernel OOM-killer, suspected \`systemd-oomd\`
SIGKILL under memory pressure).

## Workload

- \`bigann\` 1 B vectors × 128-dim uint8
- \`--ingest-threads 8 --batch-size 1000 --ingest-max-ops 10000\`

## Watchdog log (last 50 lines)

\`\`\`
$(tail -n 50 "$WD_LOG" 2>/dev/null || echo "(watchdog log not available)")
\`\`\`

## Artifacts

| Path | What |
|---|---|
EOF
    [[ -n "$logs_dir" ]] && echo "| \`$logs_dir\` | service logs collected at storm-detect time |" >> "$body_file"
    {
        echo "| \`$WD_LOG\` | full watchdog log |"
        echo "| \`$REPORTS_DIR/longevity-supervisor.log\` | this supervisor's log |"
        echo ""
        echo "Per the user's instructions for unattended overnight ops, the"
        echo "supervisor is now killing the bench, tearing down the cluster, and"
        echo "exiting.  Investigation continues from #304."
    } >> "$body_file"

    "$SCRIPT_DIR/open-issue.sh" --title \
            "bigann 1B-vector longevity bench: crash storm (${restart_count} restarts/30 min)" \
            --body-file "$body_file" \
            --logs-dir "${logs_dir:-/dev/null}" >> "$SUP_LOG" 2>&1 || true

    log "    killing bench, tearing down cluster"
    "$SCRIPT_DIR/kill-bench.sh" >> "$SUP_LOG" 2>&1 || true
    "$EXAMPLE_DIR/teardown.sh" >> "$SUP_LOG" 2>&1 || true
    log "==> teardown complete; supervisor exiting"
    exit 0
}

# ── Main loop ────────────────────────────────────────────────────────────────
prev_rows=""
prev_ts=""
slow_count=0

while true; do
    NOW=$(date +%s)

    # 1) Slowdown detection (only after warmup).
    if (( NOW - start_ts >= WARMUP_S )); then
        rows="$(curl -s --max-time 5 "http://localhost:${ADMIN_PORT}/status" \
               | python3 -c "import sys,json; print(json.load(sys.stdin).get('rows', 0))" \
               2>/dev/null || echo 0)"
        if [[ -n "$prev_rows" && "$rows" -gt 0 ]]; then
            delta=$((rows - prev_rows))
            dt=$((NOW - prev_ts))
            if (( dt > 0 )); then
                rate=$((delta / dt))
                log "tick rows=$rows rate=${rate}/s slow_count=$slow_count slow_threshold=${SLOW_THRESHOLD} slow_ticks_needed=${SLOW_TICKS}"
                if (( rate < SLOW_THRESHOLD )); then
                    slow_count=$((slow_count + 1))
                else
                    slow_count=0
                fi
                if (( slow_count >= SLOW_TICKS )) && ! $slowdown_issue_filed; then
                    handle_slowdown "$rate"
                    slowdown_issue_filed=true
                    slow_count=0
                fi
            fi
        fi
        prev_rows="$rows"
        prev_ts="$NOW"
    else
        log "tick (in warmup, $((WARMUP_S - (NOW - start_ts)))s left)"
    fi

    # 2) Crash storm detection — count "restarting (attempt" entries from the
    # watchdog log within the last 30 min.
    if [[ -f "$WD_LOG" ]]; then
        cutoff=$(date -d '30 minutes ago' +%s)
        recent_restarts=$(awk -v cutoff="$cutoff" '
            /restarting \(attempt/ {
                # Extract ISO timestamp from the line like:
                # ==> 2026-04-26T22:00:00+02:00 server DOWN — restarting ...
                match($0, /[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}/)
                if (RLENGTH > 0) {
                    ts = substr($0, RSTART, RLENGTH)
                    cmd = "date -d \"" ts "\" +%s"
                    cmd | getline epoch
                    close(cmd)
                    if (epoch >= cutoff) count++
                }
            }
            END { print (count == "" ? 0 : count) }
        ' "$WD_LOG")
        if (( recent_restarts >= CRASH_STORM_LIMIT )); then
            handle_crash_storm "$recent_restarts"
        fi
    fi

    sleep "$INTERVAL_S"
done
