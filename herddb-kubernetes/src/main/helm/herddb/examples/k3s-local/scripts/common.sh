#!/usr/bin/env bash
#
# Shared helpers sourced by the other scripts. Not meant to be executed
# directly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
EXAMPLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPORTS_DIR="$EXAMPLE_DIR/reports"

KUBECONFIG_FILE="$EXAMPLE_DIR/.kubeconfig"
if [[ -f "$KUBECONFIG_FILE" ]]; then
    export KUBECONFIG="$KUBECONFIG_FILE"
else
    echo "ERROR: kubeconfig not found at $KUBECONFIG_FILE — run ./install.sh first." >&2
    exit 1
fi

mkdir -p "$REPORTS_DIR"

timestamp() { date +%Y%m%d-%H%M%S; }

herddb_pods() {
    kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'
}

section() { printf '\n==> %s\n' "$1"; }
