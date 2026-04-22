#!/usr/bin/env bash
# setup-ghcr-secret.sh — Create or replace the ghcr-credentials docker-registry Secret.
#
# Usage:
#   ./scripts/setup-ghcr-secret.sh --username <github-user> --token <pat>
#
# Options:
#   --username <user>     GitHub username                   (required)
#   --token    <pat>      GitHub Personal Access Token      (required)
#                         Minimum scope: read:packages
#   --secret-name <name>  Kubernetes Secret name            (default: ghcr-credentials)
#   --namespace <ns>      Kubernetes namespace              (default: default)
#   --dry-run             Print what would be created, do not create
#   --help                Show this help
#
# Pre-requisites:
#   - kubectl pointing at the target cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

USERNAME=""
TOKEN=""
SECRET_NAME="ghcr-credentials"
NAMESPACE="default"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --username)    USERNAME="$2";    shift 2 ;;
    --token)       TOKEN="$2";       shift 2 ;;
    --secret-name) SECRET_NAME="$2"; shift 2 ;;
    --namespace)   NAMESPACE="$2";   shift 2 ;;
    --dry-run)     DRY_RUN=true;     shift   ;;
    --help|-h)
      sed -n '2,18p' "$0" | sed 's/^# //'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$USERNAME" || -z "$TOKEN" ]]; then
  echo "ERROR: --username and --token are required." >&2
  echo "Usage: $0 --username <github-user> --token <pat>" >&2
  exit 1
fi

section "GHCR pull secret setup"
echo "  Registry:    ghcr.io"
echo "  Username:    $USERNAME"
echo "  Secret name: $SECRET_NAME"
echo "  Namespace:   $NAMESPACE"

if $DRY_RUN; then
  echo ""
  echo "  [DRY RUN] Would create docker-registry secret/$SECRET_NAME for ghcr.io"
  exit 0
fi

section "Creating Kubernetes Secret $SECRET_NAME in namespace $NAMESPACE"
kubectl create secret docker-registry "$SECRET_NAME" \
  --namespace="$NAMESPACE" \
  --docker-server=ghcr.io \
  --docker-username="$USERNAME" \
  --docker-password="$TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

section "Verifying Secret"
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" \
  -o jsonpath='Secret {.metadata.name} created at {.metadata.creationTimestamp}{"\n"}' 2>/dev/null || true

timestamp "ghcr-credentials Secret ready."
