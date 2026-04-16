#!/usr/bin/env bash
#
# Tear down HerdDB from a local k3s-in-docker cluster.
#
# Usage:
#   ./teardown.sh                   # uninstall chart AND remove k3s container
#   ./teardown.sh --keep-container  # only uninstall chart, leave k3s running
#   ./teardown.sh --name mycluster  # use a non-default container name
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"

CONTAINER_NAME="herddb-k3s"
KEEP_CONTAINER=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --keep-container) KEEP_CONTAINER=true; shift ;;
        --name)           CONTAINER_NAME="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

KUBECONFIG_FILE="$SCRIPT_DIR/.kubeconfig"
if [[ -f "$KUBECONFIG_FILE" ]]; then
    export KUBECONFIG="$KUBECONFIG_FILE"
fi

# ── 1. Uninstall Helm release ───────────────────────────────────────
if [[ -f "$KUBECONFIG_FILE" ]] && helm status herddb >/dev/null 2>&1; then
    echo "==> Uninstalling Helm release 'herddb'..."
    helm uninstall herddb || true
    echo "==> Deleting PersistentVolumeClaims..."
    kubectl delete pvc -l app.kubernetes.io/instance=herddb --ignore-not-found || true
else
    echo "==> Helm release 'herddb' not found, skipping."
fi

# ── 2. Stop and remove k3s container ────────────────────────────────
if $KEEP_CONTAINER; then
    echo "==> Leaving k3s container '$CONTAINER_NAME' running (--keep-container)."
elif docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "==> Removing k3s container '$CONTAINER_NAME'..."
    docker rm -f "$CONTAINER_NAME" >/dev/null
    rm -f "$KUBECONFIG_FILE"
else
    echo "==> k3s container '$CONTAINER_NAME' not found."
fi

# ── 3. Prune unused Docker volumes ──────────────────────────────────
# k3s stores containerd image layers and PVC data in anonymous Docker
# volumes; pruning them reclaims disk space after each benchmark run.
echo "==> Pruning unused Docker volumes..."
docker volume prune -f

echo ""
echo "==> Teardown complete."
