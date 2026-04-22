#!/usr/bin/env bash
# create-cluster.sh — Create a GKE Autopilot cluster for HerdDB benchmarks
#
# Usage:
#   ./scripts/create-cluster.sh [OPTIONS]
#
# Options:
#   --cluster  <name>        Cluster name                   (default: cluster1)
#   --location <region>      GCP region                     (default: europe-west4)
#   --project  <id>          GCP project ID                 (default: from gcloud config)
#   --network  <name>        VPC network name               (default: default)
#   --subnetwork <name>      Subnetwork name                (default: default)
#   --channel  <channel>     Release channel                (default: REGULAR)
#   --configure-kubeconfig   Run gcloud get-credentials after creation (default: true)
#   --no-configure-kubeconfig  Skip kubeconfig update
#   --help                   Show this help
#
# After creation the script prints the gcloud get-credentials command so you
# can update $KUBECONFIG manually if you passed --no-configure-kubeconfig.
#
# The cluster is created as Autopilot with Workload Identity enabled (matching
# the previous cluster1 in europe-west12).  Node provisioning is left entirely
# to Autopilot; machine family nodeSelectors in values.yaml will guide
# scheduling towards AVX-512-capable nodes (c3/c3d in europe-west4).
#
# Pre-requisites:
#   gcloud CLI authenticated with roles/container.admin on the project

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# create-cluster.sh runs before the cluster exists, so we cannot source
# common.sh (which checks kubectl connectivity). Define the helpers inline.
timestamp() { date +%Y%m%d-%H%M%S; }
section()   { printf '\n==> %s\n' "$1"; }

# ── Defaults ─────────────────────────────────────────────────────────────────
CLUSTER_NAME="cluster1"
LOCATION="europe-west4"
PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
NETWORK="default"
SUBNETWORK="default"
CHANNEL="regular"
CONFIGURE_KUBECONFIG=true

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster)                CLUSTER_NAME="$2";           shift 2 ;;
    --location)               LOCATION="$2";               shift 2 ;;
    --project)                PROJECT="$2";                shift 2 ;;
    --network)                NETWORK="$2";                shift 2 ;;
    --subnetwork)             SUBNETWORK="$2";             shift 2 ;;
    --channel)                CHANNEL="$2";                shift 2 ;;
    --configure-kubeconfig)   CONFIGURE_KUBECONFIG=true;  shift   ;;
    --no-configure-kubeconfig) CONFIGURE_KUBECONFIG=false; shift  ;;
    --help|-h)
      sed -n '2,24p' "$0" | sed 's/^# //'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Validation ───────────────────────────────────────────────────────────────
if [[ -z "$PROJECT" ]]; then
  echo "ERROR: GCP project not set. Pass --project <id> or run: gcloud config set project <id>" >&2
  exit 1
fi

# ── Summary ──────────────────────────────────────────────────────────────────
section "GKE Autopilot cluster to create"
echo "  Project:    $PROJECT"
echo "  Location:   $LOCATION"
echo "  Name:       $CLUSTER_NAME"
echo "  Network:    $NETWORK / $SUBNETWORK"
echo "  Channel:    $CHANNEL"
echo "  Workload Identity: enabled by default (Autopilot)"
echo ""
echo "  AVX-512 note: europe-west4 has C3 (Intel Sapphire Rapids) and C3D (AMD EPYC Genoa)."
echo "  Both support AVX-512.  Set indexingService.nodeSelector in values.yaml:"
echo "    cloud.google.com/machine-family: c3"

# ── Create cluster ───────────────────────────────────────────────────────────
section "Creating Autopilot cluster $CLUSTER_NAME in $LOCATION"
gcloud container clusters create-auto "$CLUSTER_NAME" \
  --location="$LOCATION" \
  --project="$PROJECT" \
  --network="$NETWORK" \
  --subnetwork="$SUBNETWORK" \
  --release-channel="$CHANNEL"

timestamp "Cluster $CLUSTER_NAME created."

# ── Configure kubeconfig ─────────────────────────────────────────────────────
GET_CREDS_CMD="gcloud container clusters get-credentials $CLUSTER_NAME --location=$LOCATION --project=$PROJECT"

if $CONFIGURE_KUBECONFIG; then
  section "Updating kubeconfig"
  $GET_CREDS_CMD
  timestamp "kubeconfig updated. Active context: $(kubectl config current-context)"
else
  echo ""
  echo "Skipped kubeconfig update. To configure kubectl, run:"
  echo "  $GET_CREDS_CMD"
fi

# ── Next steps ───────────────────────────────────────────────────────────────
section "Next steps"
cat <<EOF
1. Ensure the GCS credentials secret exists:
     kubectl create secret generic herddb-gcs-credentials \\
       --from-literal=S3_ACCESS_KEY=<hmac-access-key> \\
       --from-literal=S3_SECRET_KEY=<hmac-secret-key>

2. Update values.yaml:
   - Set indexingService.nodeSelector.cloud.google.com/machine-family to "c3"
     (AVX-512 Intel Sapphire Rapids) or "c3d" (AVX-512 AMD EPYC Genoa)
   - Set fileServer.storage.pages.bucket to your pages GCS bucket name
   - Set tools.gcs.datasetsBucket to your datasets bucket name

3. Install HerdDB:
     ./install.sh --non-interactive

4. Check cluster health:
     ./scripts/check-cluster.sh
EOF
