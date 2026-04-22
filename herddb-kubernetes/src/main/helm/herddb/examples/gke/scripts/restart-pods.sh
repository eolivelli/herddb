#!/usr/bin/env bash
# restart-pods.sh — Rolling-restart all HerdDB StatefulSets.
#
# Usage:
#   ./scripts/restart-pods.sh [--namespace <ns>]
#
# Use this after a Helm upgrade that changes pod-level settings that do not
# trigger an automatic rollout (e.g. imagePullSecrets added while pods were
# already in a Pending/ImagePullBackOff state).
#
# Pre-requisites:
#   kubectl pointing at the target cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

NAMESPACE="default"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,14p' "$0" | sed 's/^# //'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

section "Rolling-restart all HerdDB StatefulSets in namespace $NAMESPACE"
for sts in $(kubectl get sts -n "$NAMESPACE" -l app.kubernetes.io/instance=herddb \
               -o jsonpath='{.items[*].metadata.name}'); do
  echo "  Restarting sts/$sts ..."
  kubectl rollout restart sts/"$sts" -n "$NAMESPACE"
done

section "Waiting for rollout to complete (timeout 10m)"
for sts in $(kubectl get sts -n "$NAMESPACE" -l app.kubernetes.io/instance=herddb \
               -o jsonpath='{.items[*].metadata.name}'); do
  echo "  Waiting for sts/$sts ..."
  kubectl rollout status sts/"$sts" -n "$NAMESPACE" --timeout=600s
done

timestamp "All HerdDB StatefulSets restarted and ready."
