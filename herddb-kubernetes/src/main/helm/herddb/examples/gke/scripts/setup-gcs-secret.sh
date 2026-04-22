#!/usr/bin/env bash
# setup-gcs-secret.sh — Create or rotate the herddb-gcs-credentials Secret.
#
# Usage:
#   ./scripts/setup-gcs-secret.sh [OPTIONS]
#
# Options:
#   --service-account <email>   GCS service account to generate HMAC keys for
#                               (default: herddb-gcs@<project>.iam.gserviceaccount.com)
#   --project <id>              GCP project ID  (default: from gcloud config)
#   --secret-name <name>        Kubernetes Secret name  (default: herddb-gcs-credentials)
#   --namespace <ns>            Kubernetes namespace    (default: default)
#   --dry-run                   Print what would be created, but do not create
#   --help                      Show this help
#
# The script:
#   1. Creates a new HMAC key for the given GCS service account.
#   2. Creates (or replaces) the Kubernetes Secret in the target namespace.
#   3. Verifies the Secret was created successfully.
#
# The HMAC secret is printed to stdout once during creation and then lost —
# gcloud cannot retrieve it again.  The Kubernetes Secret is the only copy.
#
# Pre-requisites:
#   - gcloud CLI authenticated with roles/storage.hmacKeyAdmin (or Owner)
#   - kubectl pointing at the target cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
SERVICE_ACCOUNT=""
SECRET_NAME="herddb-gcs-credentials"
NAMESPACE="default"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --service-account) SERVICE_ACCOUNT="$2"; shift 2 ;;
    --project)         PROJECT="$2";         shift 2 ;;
    --secret-name)     SECRET_NAME="$2";     shift 2 ;;
    --namespace)       NAMESPACE="$2";       shift 2 ;;
    --dry-run)         DRY_RUN=true;         shift   ;;
    --help|-h)
      sed -n '2,22p' "$0" | sed 's/^# //'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$PROJECT" ]]; then
  echo "ERROR: GCP project not set. Pass --project <id> or run: gcloud config set project <id>" >&2
  exit 1
fi

if [[ -z "$SERVICE_ACCOUNT" ]]; then
  SERVICE_ACCOUNT="herddb-gcs@${PROJECT}.iam.gserviceaccount.com"
fi

section "GCS credentials setup"
echo "  Project:         $PROJECT"
echo "  Service account: $SERVICE_ACCOUNT"
echo "  Secret name:     $SECRET_NAME"
echo "  Namespace:       $NAMESPACE"

if $DRY_RUN; then
  echo ""
  echo "  [DRY RUN] Would create HMAC key for $SERVICE_ACCOUNT and store in secret/$SECRET_NAME"
  exit 0
fi

# ── Create HMAC key ───────────────────────────────────────────────────────────
section "Creating HMAC key for $SERVICE_ACCOUNT"
HMAC_JSON=$(gcloud storage hmac create "$SERVICE_ACCOUNT" \
  --project="$PROJECT" \
  --format=json)

ACCESS_KEY=$(echo "$HMAC_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['metadata']['accessId'])")
SECRET_KEY=$(echo "$HMAC_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['secret'])")

echo "  Access ID: $ACCESS_KEY"
echo "  Secret:    (stored in Secret/$SECRET_NAME only — not shown again)"

# ── Create / replace Kubernetes Secret ───────────────────────────────────────
section "Creating Kubernetes Secret $SECRET_NAME in namespace $NAMESPACE"
kubectl create secret generic "$SECRET_NAME" \
  --namespace="$NAMESPACE" \
  --from-literal=S3_ACCESS_KEY="$ACCESS_KEY" \
  --from-literal=S3_SECRET_KEY="$SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

# ── Verify ───────────────────────────────────────────────────────────────────
section "Verifying Secret"
kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" \
  -o jsonpath='Secret {.metadata.name} created at {.metadata.creationTimestamp}, keys: {.data}' 2>/dev/null \
  | python3 -c "import sys; line=sys.stdin.read(); print(line)" || true
echo ""

timestamp "herddb-gcs-credentials Secret ready."
