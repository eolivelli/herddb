#!/usr/bin/env bash
#
# Reset the HerdDB cluster to an empty state between benchmark runs.
#
# Scales down every HerdDB StatefulSet except the tools pod, deletes
# their PVCs, empties the file-server pages bucket in GCS, and then
# scales the StatefulSets back up to their original replica count.
# The Helm release, the datasets bucket, the tools pod, and the tools
# dataset cache PVC are all preserved.
#
# Usage:
#   ./scripts/reset-cluster.sh
#   ./scripts/reset-cluster.sh --pages-bucket herddb-pages --yes
#   ./scripts/reset-cluster.sh --keep-pvc herddb-tools --keep-pvc herddb-server --yes
#
# Flags:
#   --pages-bucket <name>   GCS bucket to empty (default: read from
#                           `helm get values herddb`, else env
#                           PAGES_BUCKET, else herddb-pages).
#   --keep-pvc <sts-name>   Do not delete PVCs belonging to this
#                           StatefulSet (repeatable). herddb-tools is
#                           always kept; add more here to preserve
#                           additional state.
#   --yes                   Skip the interactive confirmation prompt.
#
# On success prints RESET_STATE=<path> pointing at a small JSON-ish
# file under reports/ recording the original replica counts.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# The tools StatefulSet is always preserved — it holds the downloaded
# dataset cache, which we want to reuse across runs.
KEEP_PVCS=("herddb-tools")
PAGES_BUCKET=""
ASSUME_YES=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pages-bucket) PAGES_BUCKET="$2"; shift 2 ;;
        --keep-pvc)     KEEP_PVCS+=("$2"); shift 2 ;;
        --yes)          ASSUME_YES=true; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

fatal() { echo "ERROR: $*" >&2; exit 1; }

# Resolve pages bucket. Priority: --pages-bucket > env > helm values
# > literal default. We never want to accidentally hit the datasets
# bucket, so guard against that at several layers.
if [[ -z "$PAGES_BUCKET" && -n "${PAGES_BUCKET_ENV:-}" ]]; then
    PAGES_BUCKET="$PAGES_BUCKET_ENV"
fi
if [[ -z "$PAGES_BUCKET" ]]; then
    PAGES_BUCKET=$(helm get values herddb -a -o json 2>/dev/null \
        | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("fileServer",{}).get("s3",{}).get("bucket","") or "")' 2>/dev/null || true)
fi
PAGES_BUCKET="${PAGES_BUCKET:-herddb-pages}"

# ── Hard safety guards. ────────────────────────────────────────
if [[ "$PAGES_BUCKET" == "herddb-datasets" || "$PAGES_BUCKET" == *datasets* ]]; then
    fatal "refusing to reset with pages-bucket='$PAGES_BUCKET' — looks like a datasets bucket."
fi
if [[ -z "$PAGES_BUCKET" ]]; then
    fatal "could not resolve the pages bucket name; pass --pages-bucket."
fi

# The StatefulSets that hold durable state and that we DO want to
# wipe. Order matters for a clean shutdown (dependents first).
ALL_STS=(
    "herddb-server"
    "herddb-indexing-service"
    "herddb-file-server"
    "herddb-bookkeeper"
    "herddb-zookeeper"
)

# Helper: is $1 in the KEEP_PVCS array?
is_kept() {
    local name="$1" kept
    for kept in "${KEEP_PVCS[@]}"; do
        [[ "$name" == "$kept" ]] && return 0
    done
    return 1
}

# Filter targets so we never scale the tools pod or any --keep-pvc entry.
TARGET_STS=()
for s in "${ALL_STS[@]}"; do
    if ! is_kept "$s"; then
        if kubectl -n default get sts "$s" >/dev/null 2>&1; then
            TARGET_STS+=("$s")
        fi
    fi
done

if [[ ${#TARGET_STS[@]} -eq 0 ]]; then
    fatal "no StatefulSets to reset (are you pointed at the right cluster?)."
fi

section "Reset plan"
echo "  Pages bucket:   gs://$PAGES_BUCKET   (will be emptied)"
echo "  StatefulSets:   ${TARGET_STS[*]}"
echo "  Keep PVCs for:  ${KEEP_PVCS[*]}"
echo ""

if ! $ASSUME_YES; then
    read -rp "Proceed with reset? This will DELETE PVCs and EMPTY the pages bucket. [y/N] " ans
    if [[ ! "$ans" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
fi

TS="$(timestamp)"
STATE_FILE="$REPORTS_DIR/reset-$TS.state"
: > "$STATE_FILE"

# ── 1. Record the current replica counts so scale-up can restore
#       them even if the script is interrupted and re-run.
section "Recording current replica counts"
for s in "${TARGET_STS[@]}"; do
    r=$(kubectl -n default get sts "$s" -o jsonpath='{.spec.replicas}')
    echo "$s=$r" >> "$STATE_FILE"
    echo "  $s: $r replicas"
done

# ── 2. Scale StatefulSets to 0 and wait for pod termination.
section "Scaling StatefulSets to 0"
for s in "${TARGET_STS[@]}"; do
    kubectl -n default scale sts "$s" --replicas=0
done

echo "  Waiting for pods to terminate (up to 5m)..."
for s in "${TARGET_STS[@]}"; do
    # Wait per-StatefulSet so a single slow shutdown doesn't mask the
    # others. `--for=delete` watches for the pod objects to disappear.
    kubectl -n default wait --for=delete pod \
        -l "app.kubernetes.io/instance=herddb,statefulset.kubernetes.io/pod-name" \
        --timeout=300s >/dev/null 2>&1 || true
done
kubectl -n default get pods -l app.kubernetes.io/instance=herddb -o wide || true

# ── 3. Delete PVCs for the target StatefulSets.
section "Deleting PVCs"
for s in "${TARGET_STS[@]}"; do
    # StatefulSet PVCs follow the pattern <volume-name>-<sts>-<ordinal>.
    # We filter by sts-name substring so we never touch PVCs of the
    # tools pod or anything else.
    PVCS=$(kubectl -n default get pvc -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' \
        | grep -E -- "-${s}-[0-9]+$" || true)
    if [[ -z "$PVCS" ]]; then
        echo "  $s: no PVCs to delete"
        continue
    fi
    while read -r pvc; do
        [[ -z "$pvc" ]] && continue
        # Double-check it doesn't belong to any --keep-pvc target.
        skip=false
        for kept in "${KEEP_PVCS[@]}"; do
            if [[ "$pvc" == *"-${kept}-"* ]]; then
                skip=true; break
            fi
        done
        if $skip; then
            echo "  SKIP $pvc (kept)"
            continue
        fi
        echo "  delete pvc/$pvc"
        kubectl -n default delete pvc "$pvc" --wait=true --timeout=120s
    done <<< "$PVCS"
done

# ── 4. Empty the pages bucket (but keep the bucket itself so IAM
#       bindings and HMAC keys remain valid).
section "Emptying gs://$PAGES_BUCKET"
# Re-assert the guard right before the destructive call.
if [[ "$PAGES_BUCKET" == "herddb-datasets" || "$PAGES_BUCKET" == *datasets* ]]; then
    fatal "BUG: pages bucket '$PAGES_BUCKET' looks like a datasets bucket — refusing."
fi
gcloud storage rm "gs://${PAGES_BUCKET}/**" --recursive --quiet 2>/dev/null || {
    echo "  (bucket was already empty)"
}

# ── 5. Scale StatefulSets back up to the recorded replica counts.
section "Scaling StatefulSets back up"
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    name="${line%%=*}"
    replicas="${line##*=}"
    echo "  $name -> $replicas"
    kubectl -n default scale sts "$name" --replicas="$replicas"
done < "$STATE_FILE"

# ── 6. Wait for ready.
section "Waiting for pods to become Ready (up to 5m)"
kubectl -n default wait --for=condition=ready pod \
    -l app.kubernetes.io/instance=herddb \
    --all --timeout=300s || {
    echo "WARNING: not all pods became Ready within 5m; run scripts/check-cluster.sh" >&2
}

echo ""
echo "RESET_STATE=$STATE_FILE"
