#!/usr/bin/env bash
# delete-cluster.sh — Delete a GKE Autopilot cluster
#
# Usage:
#   ./scripts/delete-cluster.sh [--cluster <name>] [--location <region>] [--project <id>] [--yes]
#
# Defaults to the cluster discovered from the current kubeconfig context.
# Requires explicit --yes (or interactive confirmation) to proceed.
#
# This script ONLY deletes the GKE cluster itself.  It does NOT touch:
#   - GCS buckets (herddb-pages, herddb-datasets)
#   - Docker images in the registry
#   - Any IAM service accounts or roles
#
# Pre-requisites:
#   gcloud CLI authenticated with roles/container.admin on the project

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

# ── Defaults derived from the active kubeconfig context ─────────────────────
CURRENT_CONTEXT=$(kubectl config current-context 2>/dev/null || true)
# Context format: gke_<project>_<location>_<cluster>
if [[ "$CURRENT_CONTEXT" =~ ^gke_([^_]+)_([^_]+)_(.+)$ ]]; then
  DEFAULT_PROJECT="${BASH_REMATCH[1]}"
  DEFAULT_LOCATION="${BASH_REMATCH[2]}"
  DEFAULT_CLUSTER="${BASH_REMATCH[3]}"
else
  DEFAULT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
  DEFAULT_LOCATION=$(gcloud config get-value compute/region 2>/dev/null || echo "")
  DEFAULT_CLUSTER=""
fi

CLUSTER_NAME="${DEFAULT_CLUSTER}"
LOCATION="${DEFAULT_LOCATION}"
PROJECT="${DEFAULT_PROJECT}"
YES=false

# ── Argument parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster)   CLUSTER_NAME="$2"; shift 2 ;;
    --location)  LOCATION="$2";     shift 2 ;;
    --project)   PROJECT="$2";      shift 2 ;;
    --yes)       YES=true;          shift   ;;
    --help|-h)
      sed -n '2,12p' "$0" | sed 's/^# //'
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# ── Validation ───────────────────────────────────────────────────────────────
if [[ -z "$CLUSTER_NAME" ]]; then
  echo "ERROR: Could not determine cluster name. Pass --cluster <name>." >&2
  exit 1
fi
if [[ -z "$LOCATION" ]]; then
  echo "ERROR: Could not determine cluster location. Pass --location <region>." >&2
  exit 1
fi
if [[ -z "$PROJECT" ]]; then
  echo "ERROR: Could not determine GCP project. Pass --project <id>." >&2
  exit 1
fi

# ── Confirmation ─────────────────────────────────────────────────────────────
section "Cluster to delete"
echo "  Project:  $PROJECT"
echo "  Location: $LOCATION"
echo "  Name:     $CLUSTER_NAME"
echo ""
echo "  WARNING: This is irreversible. All node pools will be deleted."
echo "  GCS buckets, IAM accounts, and Docker images are NOT affected."

if ! $YES; then
  read -r -p "  Type the cluster name to confirm deletion: " CONFIRM
  if [[ "$CONFIRM" != "$CLUSTER_NAME" ]]; then
    echo "Confirmation did not match. Aborting." >&2
    exit 1
  fi
fi

# ── Delete ───────────────────────────────────────────────────────────────────
section "Deleting cluster $CLUSTER_NAME in $LOCATION ($PROJECT)"
gcloud container clusters delete "$CLUSTER_NAME" \
  --location="$LOCATION" \
  --project="$PROJECT" \
  --quiet

timestamp "Cluster $CLUSTER_NAME deleted."
