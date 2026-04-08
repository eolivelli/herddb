#!/usr/bin/env bash
#
# Teardown HerdDB from a local k3s cluster.
#
# Usage:
#   ./teardown.sh              # uninstall Helm release and delete PVCs
#   ./teardown.sh --remove-k3s # also uninstall k3s entirely
#
set -euo pipefail

KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
export KUBECONFIG

# Fall back to the k3s default if the user kubeconfig doesn't exist yet.
if [[ ! -f "$KUBECONFIG" ]] && [[ -f /etc/rancher/k3s/k3s.yaml ]]; then
    KUBECONFIG=/etc/rancher/k3s/k3s.yaml
    export KUBECONFIG
fi

REMOVE_K3S=false
if [[ "${1:-}" == "--remove-k3s" ]]; then
    REMOVE_K3S=true
fi

# ── 1. Uninstall Helm release ───────────────────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Uninstalling Helm release 'herddb'..."
    helm uninstall herddb
else
    echo "==> Helm release 'herddb' not found, skipping."
fi

# ── 2. Delete PersistentVolumeClaims ────────────────────────────────
echo "==> Deleting PersistentVolumeClaims..."
kubectl delete pvc -l app.kubernetes.io/instance=herddb --ignore-not-found

# ── 3. Optionally uninstall k3s ─────────────────────────────────────
if $REMOVE_K3S; then
    if [[ -x /usr/local/bin/k3s-uninstall.sh ]]; then
        echo "==> Uninstalling k3s..."
        /usr/local/bin/k3s-uninstall.sh
    else
        echo "==> k3s uninstall script not found at /usr/local/bin/k3s-uninstall.sh"
    fi
else
    echo ""
    echo "k3s is still running. To remove it entirely:"
    echo "  /usr/local/bin/k3s-uninstall.sh"
fi

echo ""
echo "==> Teardown complete."
