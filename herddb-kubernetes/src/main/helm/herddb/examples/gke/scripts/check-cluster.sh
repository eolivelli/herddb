#!/usr/bin/env bash
#
# Health check for a HerdDB deployment on a GKE cluster.
# Prints pod status and exits non-zero if any HerdDB pod is not Ready.
#
# Usage: ./scripts/check-cluster.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

section "HerdDB pods"
kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
    -o wide

unhealthy=0
while read -r pod; do
    [[ -z "$pod" ]] && continue
    ready=$(kubectl -n default get pod "$pod" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "Unknown")
    phase=$(kubectl -n default get pod "$pod" \
        -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    if [[ "$ready" != "True" ]]; then
        echo "  [NOT READY] $pod (phase=$phase, ready=$ready)" >&2
        unhealthy=$((unhealthy + 1))
    fi
done < <(herddb_pods)

if [[ $unhealthy -gt 0 ]]; then
    echo ""
    echo "ERROR: $unhealthy pod(s) not Ready." >&2
    exit 1
fi

echo ""
echo "All HerdDB pods are Ready."
