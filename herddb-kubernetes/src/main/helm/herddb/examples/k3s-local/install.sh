#!/usr/bin/env bash
#
# Install HerdDB on a local k3s cluster.
#
# Usage:
#   ./install.sh                     # detect k3s, prompt if missing, install chart
#   ./install.sh --build             # also rebuild Docker images with Maven first
#   ./install.sh --install-k3s       # install k3s without prompting
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$CHART_DIR/../../../.." && pwd)"

IMAGE="herddb/herddb-server:0.30.0-SNAPSHOT"

BUILD=false
INSTALL_K3S=false
for arg in "$@"; do
    case "$arg" in
        --build)       BUILD=true ;;
        --install-k3s) INSTALL_K3S=true ;;
    esac
done

# ── 1. Build Docker images ──────────────────────────────────────────
if $BUILD; then
    echo "==> Building Docker images with Maven..."
    (cd "$REPO_ROOT" && mvn clean install -DskipTests -Pdocker)
fi

# ── 2. Ensure k3s is running ────────────────────────────────────────
if command -v k3s >/dev/null 2>&1 && k3s kubectl get nodes >/dev/null 2>&1; then
    echo "==> k3s is already running."
else
    if ! $INSTALL_K3S; then
        echo "k3s is not installed or not running."
        read -rp "Do you want to install k3s now? [y/N] " answer
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            INSTALL_K3S=true
        else
            echo "Aborting. Install k3s manually or re-run with --install-k3s."
            exit 1
        fi
    fi
    echo "==> Installing k3s..."
    curl -sfL https://get.k3s.io | sh -
    echo "==> Waiting for k3s to be ready..."
    sleep 5
    sudo k3s kubectl wait --for=condition=ready node --all --timeout=60s
fi

# ── 3. Configure KUBECONFIG ─────────────────────────────────────────
# Copy the k3s kubeconfig so that helm/kubectl work without sudo.
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown "$(id -u):$(id -g)" ~/.kube/config
export KUBECONFIG=~/.kube/config

# ── 4. Import image into k3s containerd ─────────────────────────────
echo "==> Importing image $IMAGE into k3s..."
docker save "$IMAGE" | sudo k3s ctr images import -

# ── 5. Install Helm chart ───────────────────────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Helm release 'herddb' already exists, upgrading..."
    helm upgrade herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml"
else
    echo "==> Installing Helm chart..."
    helm install herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml"
fi

# ── 6. Wait for pods ────────────────────────────────────────────────
echo "==> Waiting for all pods to become ready (timeout 3m)..."
kubectl wait --for=condition=ready pod --all --timeout=180s

echo ""
echo "==> HerdDB is ready!"
echo ""
echo "Connect with the CLI:"
echo "  kubectl exec -it deploy/herddb-tools -- herddb-cli"
