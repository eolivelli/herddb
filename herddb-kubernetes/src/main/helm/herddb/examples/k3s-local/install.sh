#!/usr/bin/env bash
#
# Install HerdDB on a local k3s cluster running inside a Docker container.
#
# Instead of installing k3s on the host, this script runs the official
# `rancher/k3s` image in a privileged Docker container, imports the
# HerdDB image into its containerd, and installs the Helm chart using a
# kubeconfig pulled out of the container. Nothing is written to the
# host's /etc/rancher or ~/.kube/config.
#
# Usage:
#   ./install.sh                      # start k3s container and install chart
#   ./install.sh --build              # also rebuild Docker images with Maven first
#   ./install.sh --k3s-version vX.Y.Z # pin a different rancher/k3s tag
#   ./install.sh --name mycluster     # use a different container name
#   ./install.sh --no-wait            # skip waiting for pods to become ready
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$CHART_DIR/../../../../.." && pwd)"

IMAGE="herddb/herddb-server:0.30.0-SNAPSHOT"
K3S_VERSION="v1.31.4-k3s1"
CONTAINER_NAME="herddb-k3s"
BUILD=false
WAIT=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --build)        BUILD=true; shift ;;
        --k3s-version)  K3S_VERSION="$2"; shift 2 ;;
        --name)         CONTAINER_NAME="$2"; shift 2 ;;
        --no-wait)      WAIT=false; shift ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

KUBECONFIG_FILE="$SCRIPT_DIR/.kubeconfig"
export KUBECONFIG="$KUBECONFIG_FILE"

# ── 1. Build Docker images ──────────────────────────────────────────
if $BUILD; then
    echo "==> Building Docker images with Maven..."
    (cd "$REPO_ROOT" && mvn clean install -DskipTests -Pdocker)
fi

# Ensure the image exists locally — it will be side-loaded into k3s.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "ERROR: Docker image $IMAGE not found locally." >&2
    echo "Build it first with: ./install.sh --build" >&2
    exit 1
fi

# ── 2. Start the k3s container ──────────────────────────────────────
if docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "==> k3s container '$CONTAINER_NAME' is already running."
elif docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "==> Starting existing k3s container '$CONTAINER_NAME'..."
    docker start "$CONTAINER_NAME" >/dev/null
else
    echo "==> Starting k3s container '$CONTAINER_NAME' (rancher/k3s:$K3S_VERSION)..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        --privileged \
        --tmpfs /run --tmpfs /var/run \
        -p 6443:6443 \
        -e K3S_KUBECONFIG_MODE=666 \
        "rancher/k3s:$K3S_VERSION" \
        server --disable traefik --disable metrics-server >/dev/null
fi

echo "==> Waiting for k3s API to be reachable..."
for i in {1..60}; do
    if docker exec "$CONTAINER_NAME" k3s kubectl get --raw=/readyz >/dev/null 2>&1; then
        break
    fi
    sleep 2
    if [[ $i -eq 60 ]]; then
        echo "ERROR: k3s API did not become ready within 120s." >&2
        docker logs --tail=100 "$CONTAINER_NAME" >&2 || true
        exit 1
    fi
done

# ── 3. Extract kubeconfig ───────────────────────────────────────────
# The kubeconfig inside the container points to https://127.0.0.1:6443,
# which also works from the host because we published port 6443 above.
docker cp "$CONTAINER_NAME:/etc/rancher/k3s/k3s.yaml" "$KUBECONFIG_FILE" 2>/dev/null
chmod 600 "$KUBECONFIG_FILE"
echo "==> Wrote kubeconfig to $KUBECONFIG_FILE"

echo "==> Waiting for node to become Ready..."
kubectl wait --for=condition=ready node --all --timeout=120s >/dev/null

# ── 4. Import the HerdDB image into k3s containerd ──────────────────
echo "==> Importing image $IMAGE into k3s..."
IMAGE_TAR="$(mktemp --suffix=.tar)"
trap 'rm -f "$IMAGE_TAR"' EXIT
docker save "$IMAGE" -o "$IMAGE_TAR"
docker cp "$IMAGE_TAR" "$CONTAINER_NAME:/tmp/herddb.tar"
docker exec "$CONTAINER_NAME" \
    ctr --address /run/k3s/containerd/containerd.sock \
        --namespace k8s.io images import /tmp/herddb.tar >/dev/null
docker exec "$CONTAINER_NAME" rm -f /tmp/herddb.tar

# ── 5. Install Helm chart ───────────────────────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Helm release 'herddb' already exists, upgrading..."
    helm upgrade herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml"
else
    echo "==> Installing Helm chart..."
    helm install herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml"
fi

# ── 6. Wait for pods ────────────────────────────────────────────────
if $WAIT; then
    echo "==> Waiting for all pods to become ready (timeout 5m)..."
    kubectl wait --for=condition=ready pod --all --timeout=300s || {
        echo "WARNING: not all pods became ready in time." >&2
        kubectl get pods
        exit 1
    }
fi

cat <<EOF

==> HerdDB is ready!

Use this kubeconfig for subsequent commands (scripts already do this):
  export KUBECONFIG=$KUBECONFIG_FILE

Connect with the CLI:
  kubectl exec -it sts/herddb-tools -- herddb-cli

Run a vector benchmark:
  ./scripts/run-bench.sh --dataset sift10k -n 10000 -k 100 --checkpoint

Tear down:
  ./teardown.sh
EOF
