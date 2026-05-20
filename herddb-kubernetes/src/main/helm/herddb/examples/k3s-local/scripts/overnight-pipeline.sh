#!/usr/bin/env bash
# overnight-pipeline.sh — fully autonomous overnight benchmark pipeline.
#
# Usage:
#   ./scripts/overnight-pipeline.sh \
#       --current-run-log <path>   \   # log of the already-running 50M bench
#       --current-pid <N>          \   # PID of the bench process inside the pod
#       --next-n <N>               \   # vectors for the second run (e.g. 100000000)
#       --pod-log <remote-path>        # pod-resident log (e.g. /tmp/vector-bench-*.log)
#
# Steps:
#   1. Wait for current bench to finish (poll every 30s).
#   2. Generate report for run 1.
#   3. Teardown cluster.
#   4. Install fresh cluster.
#   5. Run second benchmark (--next-n vectors).
#   6. Wait for second bench to finish (poll every 60s).
#   7. Generate report for run 2.
#   8. On any failure: collect-logs → write-report → open-issue.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="$(dirname "$SCRIPT_DIR")"
KUBECONFIG="$WORK_DIR/.kubeconfig"
KC="kubectl --kubeconfig $KUBECONFIG"

# ── helpers ─────────────────────────────────────────────────────────────
ts()  { date '+%H:%M:%S'; }
log() { echo "[$(ts)] $*"; }

die() {
    local msg="$1"
    local run_log="${2:-}"
    log "FATAL: $msg"
    log "Collecting logs..."
    local logs_dir
    logs_dir="$( cd "$WORK_DIR" && ./scripts/collect-logs.sh | grep '^LOGS_DIR=' | cut -d= -f2 )"
    local report=""
    if [[ -n "$run_log" && -f "$run_log" ]]; then
        report="$( cd "$WORK_DIR" && ./scripts/write-report.sh "$run_log" | grep '^REPORT=' | cut -d= -f2 )"
    fi
    local body="$WORK_DIR/reports/overnight-failure-$(date +%Y%m%d-%H%M%S).md"
    mkdir -p "$WORK_DIR/reports"
    {
        echo "## Overnight pipeline failure"
        echo ""
        echo "**Message:** $msg"
        echo ""
        echo "**Time:** $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
        echo ""
        if [[ -n "$report" ]]; then
            echo "**Report:** $report"
        fi
        echo ""
        echo "**Logs:** $logs_dir"
    } > "$body"
    # Attach only IS log
    local attach_dir="$WORK_DIR/reports/overnight-failure-attach-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$attach_dir"
    [[ -f "$logs_dir/herddb-indexing-service-0.log" ]] && \
        cp "$logs_dir/herddb-indexing-service-0.log" "$attach_dir/"
    cd "$WORK_DIR"
    ./scripts/open-issue.sh \
        --title "[k3s-bench] overnight pipeline failure: $msg" \
        --body-file "$body" \
        --logs-dir "$attach_dir" || true
    exit 1
}

# ── argument parsing ─────────────────────────────────────────────────────
CURRENT_RUN_LOG=""
CURRENT_PID=""
NEXT_N="100000000"
POD_LOG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --current-run-log) CURRENT_RUN_LOG="$2"; shift 2 ;;
        --current-pid)     CURRENT_PID="$2";     shift 2 ;;
        --next-n)          NEXT_N="$2";           shift 2 ;;
        --pod-log)         POD_LOG="$2";          shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

[[ -z "$CURRENT_RUN_LOG" ]] && { echo "Usage: $0 --current-run-log <path> --current-pid <N> [--next-n N] --pod-log <path>"; exit 1; }
[[ -z "$CURRENT_PID"    ]] && { echo "--current-pid is required"; exit 1; }
[[ -z "$POD_LOG"        ]] && { echo "--pod-log is required"; exit 1; }

cd "$WORK_DIR"

log "=== Overnight pipeline started ==="
log "  Monitoring 50M run (PID=$CURRENT_PID, log=$CURRENT_RUN_LOG)"
log "  Second run: $NEXT_N vectors"

# ── Step 1: wait for current bench to finish ─────────────────────────────
log "--- Step 1: Waiting for current 50M benchmark to complete ---"
TIMEOUT_SECS=7200   # 2 h hard timeout
ELAPSED=0
while true; do
    # Check if process is gone
    if ! $KC exec herddb-tools-0 -- kill -0 "$CURRENT_PID" 2>/dev/null; then
        log "Bench process $CURRENT_PID is gone."
        break
    fi
    # Check pod log for phase=done
    if $KC exec herddb-tools-0 -- tail -n 5 "$POD_LOG" 2>/dev/null | grep -q 'phase=done'; then
        log "phase=done detected in pod log."
        break
    fi
    # Check for fatal error in IS logs
    if $KC logs herddb-indexing-service-0 --tail=10 2>/dev/null | grep -q 'SEVERE.*checkpoint.*Phase B exception'; then
        log "WARNING: IS Phase B exception detected — continuing per policy (no auto-stop)"
    fi
    # Progress snapshot
    ROWS="$( $KC exec herddb-tools-0 -- tail -n 2 "$POD_LOG" 2>/dev/null | grep -oP 'rows=\K[0-9]+' | tail -n 1 || echo '?' )"
    SEGS="$( $KC exec herddb-tools-0 -- \
        indexing-admin list-indexes \
            --server herddb-indexing-service-0.herddb-indexing-service:9850 \
        2>/dev/null | grep -v '^NOTE:\|^TABLESPACE' | grep 'vidx' | awk '{print $5}' || echo '?' )"
    log "[50M run] rows=$ROWS/50000000  segments=$SEGS"

    sleep 30
    ELAPSED=$(( ELAPSED + 30 ))
    if [[ $ELAPSED -ge $TIMEOUT_SECS ]]; then
        die "50M bench did not finish within ${TIMEOUT_SECS}s" "$CURRENT_RUN_LOG"
    fi
done

# ── Step 2: generate report for run 1 ────────────────────────────────────
log "--- Step 2: Generating report for 50M run ---"
REPORT1="$(./scripts/write-report.sh "$CURRENT_RUN_LOG" | grep '^REPORT=' | cut -d= -f2)"
log "Report: $REPORT1"

# Check exit status from pod log
if $KC exec herddb-tools-0 -- tail -n 10 "$POD_LOG" 2>/dev/null | grep -q 'exit=0\|status=success\|phase=done'; then
    log "50M run completed successfully."
else
    log "WARNING: 50M run exit status unclear — proceeding to 100M run anyway."
fi

# ── Step 3: teardown ─────────────────────────────────────────────────────
log "--- Step 3: Tearing down cluster ---"
./teardown.sh

# ── Step 4: install fresh cluster ────────────────────────────────────────
log "--- Step 4: Installing fresh cluster ---"
./install.sh
./scripts/check-cluster.sh || die "Cluster health check failed after install" ""

# ── Step 5: start 100M benchmark ─────────────────────────────────────────
log "--- Step 5: Starting ${NEXT_N}-vector benchmark ---"
RUN2_OUTPUT="$(./scripts/run-bench.sh --background \
    --protocol grpc \
    --grpc-endpoint herddb-indexing-service-0.herddb-indexing-service:9850 \
    --dataset bigann -n "$NEXT_N" --batch-size 10000)"
echo "$RUN2_OUTPUT"
RUN2_LOG="$(echo "$RUN2_OUTPUT" | grep '^RUN_LOG=' | cut -d= -f2)"
RUN2_PID="$(echo "$RUN2_OUTPUT" | grep -oP 'PID: \K[0-9]+')"
RUN2_PODLOG="$(echo "$RUN2_OUTPUT" | grep 'Pod log:' | awk '{print $NF}')"
log "Run 2 started: PID=$RUN2_PID log=$RUN2_LOG pod-log=$RUN2_PODLOG"

# ── Step 6: monitor 100M run ─────────────────────────────────────────────
log "--- Step 6: Monitoring ${NEXT_N}-vector benchmark ---"
TIMEOUT_SECS=21600  # 6 h hard timeout for 100M
ELAPSED=0
while true; do
    if ! $KC exec herddb-tools-0 -- kill -0 "$RUN2_PID" 2>/dev/null; then
        log "100M bench process $RUN2_PID is gone."
        break
    fi
    if $KC exec herddb-tools-0 -- tail -n 5 "$RUN2_PODLOG" 2>/dev/null | grep -q 'phase=done'; then
        log "phase=done detected for 100M run."
        break
    fi
    ROWS="$( $KC exec herddb-tools-0 -- tail -n 2 "$RUN2_PODLOG" 2>/dev/null | grep -oP 'rows=\K[0-9]+' | tail -n 1 || echo '?' )"
    SEGS="$( $KC exec herddb-tools-0 -- \
        indexing-admin list-indexes \
            --server herddb-indexing-service-0.herddb-indexing-service:9850 \
        2>/dev/null | grep -v '^NOTE:\|^TABLESPACE' | grep 'vidx' | awk '{print $5}' || echo '?' )"
    log "[100M run] rows=$ROWS/${NEXT_N}  segments=$SEGS"

    sleep 60
    ELAPSED=$(( ELAPSED + 60 ))
    if [[ $ELAPSED -ge $TIMEOUT_SECS ]]; then
        die "100M bench did not finish within ${TIMEOUT_SECS}s" "$RUN2_LOG"
    fi
done

# ── Step 7: generate report for run 2 ────────────────────────────────────
log "--- Step 7: Generating report for ${NEXT_N}-vector run ---"
REPORT2="$(./scripts/write-report.sh "$RUN2_LOG" | grep '^REPORT=' | cut -d= -f2)"
log "Report: $REPORT2"

log "=== Overnight pipeline complete ==="
log "  50M report:  $REPORT1"
log "  100M report: $REPORT2"
