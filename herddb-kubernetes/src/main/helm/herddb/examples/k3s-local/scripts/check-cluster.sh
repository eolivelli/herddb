#!/usr/bin/env bash
#
# Health check for a HerdDB deployment on the local k3s cluster.
# Prints pod status and exits non-zero if any HerdDB pod is not Ready.
#
# Usage: ./scripts/check-cluster.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

section "HerdDB pods"
unhealthy=0
while IFS=$'\t' read -r pod phase ready; do
    [[ -z "$pod" ]] && continue
    echo "  $pod  $phase  $ready"
    if [[ "$ready" != "True" ]]; then
        echo "  [NOT READY] $pod" >&2
        unhealthy=$((unhealthy + 1))
    fi
done < <(kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.phase}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}')

if [[ $unhealthy -gt 0 ]]; then
    echo ""
    echo "ERROR: $unhealthy pod(s) not Ready." >&2
    exit 1
fi

echo ""
echo "All HerdDB pods are Ready."
