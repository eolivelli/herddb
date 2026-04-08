#!/usr/bin/env bash
#
# Install HerdDB on a GKE cluster using Google Cloud Storage.
#
# Prerequisites:
#   - KUBECONFIG is set and points to a GKE cluster
#   - A Kubernetes Secret with GCS HMAC credentials exists:
#       kubectl create secret generic herddb-gcs-credentials \
#         --from-literal=S3_ACCESS_KEY=<access-id> \
#         --from-literal=S3_SECRET_KEY=<secret>
#
# Usage:
#   ./install.sh                                           # install chart
#   ./install.sh --set fileServer.s3.bucket=my-bucket      # pass extra Helm values
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Collect any extra arguments to forward to helm install/upgrade.
EXTRA_ARGS=("$@")

# ── 1. Verify cluster connectivity ────────────────────────────────
echo "==> Verifying cluster connectivity..."
if ! kubectl get nodes >/dev/null 2>&1; then
    echo "ERROR: Cannot reach the Kubernetes cluster. Is KUBECONFIG set correctly?"
    exit 1
fi

# ── 2. Check that the credentials Secret exists ───────────────────
if ! kubectl get secret herddb-gcs-credentials >/dev/null 2>&1; then
    echo "WARNING: Secret 'herddb-gcs-credentials' not found."
    echo "Create it with:"
    echo "  kubectl create secret generic herddb-gcs-credentials \\"
    echo "    --from-literal=S3_ACCESS_KEY=<access-id> \\"
    echo "    --from-literal=S3_SECRET_KEY=<secret>"
    echo ""
    read -rp "Continue anyway? [y/N] " answer
    if [[ ! "$answer" =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# ── 3. Install or upgrade the Helm chart ──────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Helm release 'herddb' already exists, upgrading..."
    helm upgrade herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${EXTRA_ARGS[@]}"
else
    echo "==> Installing Helm chart..."
    helm install herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${EXTRA_ARGS[@]}"
fi

# ── 4. Wait for pods ─────────────────────────────────────────────
echo "==> Waiting for all pods to become ready (timeout 3m)..."
kubectl wait --for=condition=ready pod --all --timeout=180s

echo ""
echo "==> HerdDB is ready!"
echo ""
echo "Connect with the CLI:"
echo "  kubectl exec -it deploy/herddb-tools -- herddb-cli"
