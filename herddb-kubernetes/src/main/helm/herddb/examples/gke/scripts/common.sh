#!/usr/bin/env bash
#
# Shared helpers sourced by the other GKE scripts. Not meant to be
# executed directly.
#
# Unlike the k3s-local variant, this file does NOT manage a local
# .kubeconfig — it relies on the caller's ambient $KUBECONFIG (or
# ~/.kube/config if unset) pointing at a reachable GKE cluster.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# If $HERDDB_TESTS_HOME is set, keep reports there (outside the chart
# tree so they are never bundled into the Helm release Secret).
# Otherwise fall back to the in-tree reports/ directory.
REPORTS_DIR="${HERDDB_TESTS_HOME:-$EXAMPLE_DIR/reports}"
mkdir -p "$REPORTS_DIR"

# Honour ambient KUBECONFIG. A missing kubeconfig means the user hasn't
# selected a GKE cluster yet — fail fast.
if [[ -z "${KUBECONFIG:-}" && ! -f "$HOME/.kube/config" ]]; then
    echo "ERROR: no kubeconfig available. Export KUBECONFIG or run 'gcloud container clusters get-credentials ...' first." >&2
    exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "ERROR: kubectl cannot reach the cluster. Check KUBECONFIG / current context." >&2
    exit 1
fi

timestamp() { date +%Y%m%d-%H%M%S; }

herddb_pods() {
    kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

section() { printf '\n==> %s\n' "$1"; }
