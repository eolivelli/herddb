#!/usr/bin/env bash
#
# Rotate the GCS HMAC key for the HerdDB service account and update
# the herddb-gcs-credentials Kubernetes Secret, then restart the
# file-server StatefulSet so the new credentials are picked up.
#
# This script is idempotent: it deletes ALL existing active HMAC keys
# for the service account, creates one fresh key, replaces the Secret,
# and rolls the file-server pods.
#
# Usage:
#   ./scripts/fix-gcs-credentials.sh [--dry-run]
#
#   --dry-run   Print the commands that would be run without executing them.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

DRY_RUN=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--dry-run]"
            echo "  Rotate HMAC keys for the HerdDB GCS service account and update the k8s secret."
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

run() {
    if $DRY_RUN; then
        echo "[dry-run] $*"
    else
        "$@"
    fi
}

# ── Detect project and service account ───────────────────────────
PROJECT="$(gcloud config get-value project 2>/dev/null)"
[[ -n "$PROJECT" ]] || { echo "ERROR: no GCP project configured" >&2; exit 1; }
SA="herddb-gcs@${PROJECT}.iam.gserviceaccount.com"

section "Rotating HMAC keys for ${SA} (project: ${PROJECT})"

# ── 1. Delete all existing active HMAC keys ──────────────────────
echo "==> Listing existing HMAC keys..."
EXISTING_KEYS="$(gcloud storage hmac list \
    --service-account="$SA" \
    --project="$PROJECT" \
    --format='value(accessId)' 2>/dev/null || true)"

if [[ -n "$EXISTING_KEYS" ]]; then
    while IFS= read -r key_id; do
        [[ -z "$key_id" ]] && continue
        echo "==> Deactivating HMAC key ${key_id}..."
        run gcloud storage hmac update "$key_id" \
            --deactivate --project="$PROJECT"
        echo "==> Deleting HMAC key ${key_id}..."
        run gcloud storage hmac delete "$key_id" \
            --project="$PROJECT" --quiet
    done <<< "$EXISTING_KEYS"
else
    echo "==> No existing HMAC keys found."
fi

# ── 2. Create a fresh HMAC key ────────────────────────────────────
echo "==> Creating new HMAC key for ${SA}..."
if $DRY_RUN; then
    echo "[dry-run] gcloud storage hmac create ${SA} --project=${PROJECT} --format=json"
    NEW_ACCESS_ID="DRY_RUN_ACCESS_ID"
    NEW_SECRET="DRY_RUN_SECRET"
else
    HMAC_OUTPUT="$(gcloud storage hmac create "$SA" \
        --project="$PROJECT" --format=json)"
    NEW_ACCESS_ID="$(echo "$HMAC_OUTPUT" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d['metadata']['accessId'])")"
    NEW_SECRET="$(echo "$HMAC_OUTPUT" | \
        python3 -c "import sys,json; d=json.load(sys.stdin); print(d['secret'])")"
    [[ -n "$NEW_ACCESS_ID" && -n "$NEW_SECRET" ]] || \
        { echo "ERROR: failed to parse HMAC key from gcloud output" >&2; exit 1; }
    echo "==> New access ID: ${NEW_ACCESS_ID}"
fi

# ── 3. Replace the Kubernetes Secret ─────────────────────────────
echo "==> Replacing herddb-gcs-credentials Secret..."
run kubectl delete secret herddb-gcs-credentials --ignore-not-found
if $DRY_RUN; then
    echo "[dry-run] kubectl create secret generic herddb-gcs-credentials ..."
else
    kubectl create secret generic herddb-gcs-credentials \
        --from-literal=S3_ACCESS_KEY="$NEW_ACCESS_ID" \
        --from-literal=S3_SECRET_KEY="$NEW_SECRET"
fi

# ── 4. Force-delete any stuck file-server pods then roll the StatefulSet ──
# The pods use secretKeyRef env vars resolved at creation time.  If a pod
# was created in the narrow window before the secret was swapped it will
# have stale credentials and the readiness probe will never pass.  The
# StatefulSet controller will not re-terminate a Running-but-not-Ready pod
# on its own, so we delete the pods explicitly first.
echo "==> Force-deleting any existing file-server pods so they are recreated with new credentials..."
run kubectl delete pod -l app.kubernetes.io/component=file-server --ignore-not-found
if ! $DRY_RUN; then
    echo "==> Waiting for file-server pods to be recreated..."
    kubectl rollout status statefulset/herddb-file-server --timeout=300s
fi

echo ""
echo "==> GCS credentials rotated successfully."
echo "    New HMAC access ID: ${NEW_ACCESS_ID}"
