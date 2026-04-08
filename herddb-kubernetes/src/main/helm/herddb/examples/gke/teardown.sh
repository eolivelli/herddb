#!/usr/bin/env bash
#
# Teardown HerdDB from a GKE cluster.
#
# Usage:
#   ./teardown.sh   # uninstall Helm release and delete PVCs
#
set -euo pipefail

# ── 1. Uninstall Helm release ────────────────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Uninstalling Helm release 'herddb'..."
    helm uninstall herddb
else
    echo "==> Helm release 'herddb' not found, skipping."
fi

# ── 2. Delete PersistentVolumeClaims ─────────────────────────────
echo "==> Deleting PersistentVolumeClaims..."
kubectl delete pvc -l app.kubernetes.io/instance=herddb --ignore-not-found

echo ""
echo "==> Teardown complete."
echo ""
echo "Note: The GCS bucket and credentials Secret are NOT deleted."
echo "To remove the Secret:"
echo "  kubectl delete secret herddb-gcs-credentials"
