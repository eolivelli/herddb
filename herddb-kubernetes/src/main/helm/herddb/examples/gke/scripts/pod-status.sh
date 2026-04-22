#!/usr/bin/env bash
#
# Print compact pod status (4-column table: NAME, READY, STATUS, RESTARTS).
# Used by supervision loops to reduce token usage.
#
# Usage: ./scripts/pod-status.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

kubectl -n default get pods -l app.kubernetes.io/instance=herddb \
    -o custom-columns=NAME:.metadata.name,READY:.status.containerStatuses[0].ready,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount
