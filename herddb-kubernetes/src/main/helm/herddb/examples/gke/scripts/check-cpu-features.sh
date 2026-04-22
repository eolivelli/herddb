#!/usr/bin/env bash
#
# Report CPU feature flags for the node(s) hosting HerdDB pods.
# Primarily used to verify AVX-512 availability for jvector acceleration.
#
# Usage:
#   ./scripts/check-cpu-features.sh
#   ./scripts/check-cpu-features.sh --pod herddb-indexing-service-0
#   ./scripts/check-cpu-features.sh --pod herddb-indexing-service-0 --pod herddb-indexing-service-1
#   ./scripts/check-cpu-features.sh --all-pods
#   ./scripts/check-cpu-features.sh --feature avx512f
#
# Flags:
#   --pod <name>      Check the node hosting this specific pod (repeatable).
#                     Defaults to herddb-indexing-service-0 and
#                     herddb-indexing-service-1 when no --pod is given.
#   --all-pods        Check nodes for every HerdDB pod currently running.
#   --feature <flag>  Grep for a specific CPU flag instead of the full
#                     AVX-512 family summary (e.g. avx512f, avx512bw,
#                     avx512vl, avx512vnni).
#
# Output:
#   One block per pod showing:
#     - Pod name and the node it is scheduled on
#     - CPU model name
#     - Presence/absence of key AVX-512 flags
#     - A final AVX512_SUPPORTED=yes|no line
#
# Notes:
#   /proc/cpuinfo inside the pod reflects the host node's CPU since
#   the kernel exposes the same cpuinfo regardless of cgroup isolation.
#   No privileged access or node-exec is required.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# ── Argument parsing ─────────────────────────────────────────────
PODS=()
ALL_PODS=false
FEATURE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)      PODS+=("$2"); shift 2 ;;
        --all-pods) ALL_PODS=true; shift ;;
        --feature)  FEATURE="$2"; shift 2 ;;
        --help|-h)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

# Default: both indexing-service pods
if [[ ${#PODS[@]} -eq 0 ]] && ! $ALL_PODS; then
    PODS=(herddb-indexing-service-0 herddb-indexing-service-1)
fi

if $ALL_PODS; then
    mapfile -t PODS < <(kubectl -n default get pods \
        -l app.kubernetes.io/instance=herddb \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | sort)
fi

if [[ ${#PODS[@]} -eq 0 ]]; then
    echo "ERROR: no HerdDB pods found." >&2
    exit 1
fi

# ── Key AVX-512 feature flags to report ─────────────────────────
# avx512f   : Foundation — required for any AVX-512 path
# avx512bw  : Byte/Word — needed for 8/16-bit vector ops
# avx512vl  : Vector Length Extensions — 128/256-bit registers via 512-bit instructions
# avx512dq  : Doubleword/Quadword — 64-bit lane ops
# avx512vnni: Vector Neural Network Instructions — int8 dot-product acceleration
# avx512fp16: FP16 compute (Intel Sapphire Rapids and later)
AVX512_FLAGS=(avx512f avx512bw avx512vl avx512dq avx512vnni avx512fp16)

overall_supported=true

for POD in "${PODS[@]}"; do
    section "CPU features on node hosting $POD"

    # Find which node this pod landed on.
    NODE=$(kubectl -n default get pod "$POD" \
        -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
    if [[ -z "$NODE" ]]; then
        echo "  WARNING: pod $POD not found or not scheduled — skipping."
        continue
    fi
    echo "  Node: $NODE"

    # /proc/cpuinfo is the same inside the container as on the host.
    CPUINFO=$(kubectl -n default exec "$POD" -- sh -c \
        'cat /proc/cpuinfo 2>/dev/null || echo "ERROR: /proc/cpuinfo not readable"' \
        2>/dev/null || echo "ERROR: exec failed")

    # CPU model
    MODEL=$(echo "$CPUINFO" | grep -m1 "^model name" | cut -d: -f2- | xargs || echo "unknown")
    echo "  CPU model: $MODEL"
    echo ""

    # Extract all flags into a newline-separated list for reliable word matching.
    # /proc/cpuinfo flags line uses tabs before the colon, so we strip everything
    # up to and including the colon, then split on whitespace.
    FLAGS_LIST=$(echo "$CPUINFO" | grep "^flags" | head -1 | sed 's/^flags[^:]*://' | tr -s ' \t' '\n')

    has_flag() {
        # grep -c exits 0 (count≥1) or 1 (count=0); never causes SIGPIPE.
        [[ $(echo "$FLAGS_LIST" | grep -cx "$1") -gt 0 ]]
    }

    if [[ -n "$FEATURE" ]]; then
        # Single-flag mode
        if has_flag "$FEATURE"; then
            echo "  $FEATURE: PRESENT ✓"
        else
            echo "  $FEATURE: ABSENT  ✗"
        fi
    else
        # AVX-512 family summary
        avx512_present=false
        for flag in "${AVX512_FLAGS[@]}"; do
            if has_flag "$flag"; then
                echo "  $flag: PRESENT ✓"
                [[ "$flag" == "avx512f" ]] && avx512_present=true
            else
                echo "  $flag: absent"
            fi
        done

        # Also show avx2 for reference (jvector fallback path)
        echo ""
        if has_flag "avx2"; then
            echo "  avx2 (fallback): PRESENT ✓"
        else
            echo "  avx2 (fallback): absent"
        fi

        echo ""
        if $avx512_present; then
            echo "  AVX512_SUPPORTED=yes"
        else
            echo "  AVX512_SUPPORTED=no  ← jvector will use scalar/AVX2 fallback"
            overall_supported=false
        fi
    fi
    echo ""
done

if [[ -z "$FEATURE" ]]; then
    echo "────────────────────────────────────────────"
    if $overall_supported; then
        echo "RESULT: all checked pods run on AVX-512 capable nodes ✓"
    else
        echo "RESULT: one or more pods run on nodes WITHOUT AVX-512 ✗"
        echo "        jvector will fall back to scalar/AVX2 — indexing will be slower."
    fi
fi
