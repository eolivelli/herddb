#!/usr/bin/env bash
#
# Teardown HerdDB from a GKE cluster.
#
# Usage:
#   ./teardown.sh   # uninstall Helm release and delete PVCs
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

# ── 1. Uninstall Helm release ────────────────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Uninstalling Helm release 'herddb'..."
    helm uninstall herddb
else
    echo "==> Helm release 'herddb' not found, skipping."
fi

# ── 2. Delete PersistentVolumeClaims ─────────────────────────────
# Preserve the tools pod's dataset cache PVC so that large benchmark datasets
# (e.g. bigann 100M/200M) do not need to be re-downloaded on the next install.
echo "==> Deleting PersistentVolumeClaims (preserving tools dataset cache)..."
kubectl get pvc -l app.kubernetes.io/instance=herddb -o name \
    | grep -v "datasets-herddb-tools" \
    | xargs --no-run-if-empty kubectl delete --ignore-not-found

echo ""
echo "==> Teardown complete."
echo ""
echo "Note: The GCS bucket and credentials Secret are NOT deleted."
echo "To remove the Secret:"
echo "  kubectl delete secret herddb-gcs-credentials"
