#!/usr/bin/env bash
#
# Install HerdDB on a GKE cluster using Google Cloud Storage.
#
# Prerequisites:
#   - KUBECONFIG is set and points to a GKE cluster
#   - A Kubernetes Secret with GCS HMAC credentials exists:
#       kubectl create secret generic herddb-gcs-credentials \
#         --from-literal=S3_ACCESS_KEY=<access-id> \
#         --from-literal=S3_SECRET_KEY=<secret>
#
# Usage:
#   ./install.sh                                           # install chart
#   ./install.sh --set fileServer.s3.bucket=my-bucket      # pass extra Helm values
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Collect any extra arguments to forward to helm install/upgrade.
EXTRA_ARGS=("$@")

# ── 0. Ask for Docker image name and version ─────────────────────
read -rp "Docker image repository [eolivelli/herddb]: " IMAGE_REPO
IMAGE_REPO="${IMAGE_REPO:-eolivelli/herddb}"

read -rp "Docker image tag [0.30.0-SNAPSHOT]: " IMAGE_TAG
IMAGE_TAG="${IMAGE_TAG:-0.30.0-SNAPSHOT}"

EXTRA_ARGS+=(--set "image.repository=$IMAGE_REPO" --set "image.tag=$IMAGE_TAG")

# ── 0b. Ask for GCS bucket name ──────────────────────────────────
read -rp "GCS bucket name [herddb-pages]: " GCS_BUCKET
GCS_BUCKET="${GCS_BUCKET:-herddb-pages}"

EXTRA_ARGS+=(--set "fileServer.s3.bucket=$GCS_BUCKET")

# ── 1. Verify cluster connectivity ────────────────────────────────
echo "==> Verifying cluster connectivity..."
if ! kubectl get nodes >/dev/null 2>&1; then
    echo "ERROR: Cannot reach the Kubernetes cluster. Is KUBECONFIG set correctly?"
    exit 1
fi

# ── 2. Check that the GCS bucket exists ───────────────────────────
echo "==> Checking GCS bucket gs://${GCS_BUCKET}..."
if ! gcloud storage buckets describe "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
    echo "Bucket 'gs://${GCS_BUCKET}' does not exist."
    read -rp "Create it now? [Y/n] " create_bucket
    if [[ ! "$create_bucket" =~ ^[Nn]$ ]]; then
        read -rp "Bucket location [us-central1]: " BUCKET_LOCATION
        BUCKET_LOCATION="${BUCKET_LOCATION:-us-central1}"
        echo "==> Creating bucket gs://${GCS_BUCKET} in ${BUCKET_LOCATION}..."
        gcloud storage buckets create "gs://${GCS_BUCKET}" --location="${BUCKET_LOCATION}"
    else
        echo "WARNING: Proceeding without a bucket — the file server will fail to start."
    fi
else
    echo "Bucket gs://${GCS_BUCKET} exists."
fi

# ── 2b. Optionally clean up the GCS bucket ───────────────────────
read -rp "Clean up (delete all objects in) bucket gs://${GCS_BUCKET}? [y/N] " clean_bucket
if [[ "$clean_bucket" =~ ^[Yy]$ ]]; then
    echo "==> Deleting all objects in gs://${GCS_BUCKET}..."
    gcloud storage rm "gs://${GCS_BUCKET}/**" --recursive 2>/dev/null || true
    echo "Bucket cleaned."
fi

# ── 3. Check that the credentials Secret exists ──────────────────
if ! kubectl get secret herddb-gcs-credentials >/dev/null 2>&1; then
    echo "Secret 'herddb-gcs-credentials' not found."
    read -rp "Create HMAC credentials and Secret now? [Y/n] " create_creds
    if [[ ! "$create_creds" =~ ^[Nn]$ ]]; then
        # HMAC keys require a GCP service account (not a user account).
        GCP_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
        if [[ -z "$GCP_PROJECT" ]]; then
            echo "ERROR: No GCP project configured. Run 'gcloud config set project PROJECT_ID'."
            exit 1
        fi
        DEFAULT_SA="herddb-gcs@${GCP_PROJECT}.iam.gserviceaccount.com"
        echo "HMAC keys require a GCP service account (not a personal Google account)."
        read -rp "Service account email [${DEFAULT_SA}]: " SA_EMAIL
        SA_EMAIL="${SA_EMAIL:-$DEFAULT_SA}"
        # Create the service account if it does not exist yet
        SA_NAME="${SA_EMAIL%%@*}"
        if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
            echo "==> Service account '${SA_EMAIL}' not found. Creating it..."
            gcloud iam service-accounts create "$SA_NAME" \
                --display-name="HerdDB GCS access"
        fi
        # Grant the service account objectAdmin on the bucket
        echo "==> Granting roles/storage.objectAdmin on gs://${GCS_BUCKET} to ${SA_EMAIL}..."
        gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
            --member="serviceAccount:${SA_EMAIL}" \
            --role="roles/storage.objectAdmin" --quiet
        echo "==> Creating HMAC key for ${SA_EMAIL}..."
        HMAC_OUTPUT="$(gcloud storage hmac create "$SA_EMAIL" --format=json)"
        HMAC_ACCESS_ID="$(echo "$HMAC_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['metadata']['accessId'])")"
        HMAC_SECRET="$(echo "$HMAC_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['secret'])")"
        if [[ -z "$HMAC_ACCESS_ID" || -z "$HMAC_SECRET" ]]; then
            echo "ERROR: Failed to parse HMAC credentials from gcloud output."
            exit 1
        fi
        echo "==> Creating Kubernetes secret 'herddb-gcs-credentials'..."
        kubectl create secret generic herddb-gcs-credentials \
            --from-literal=S3_ACCESS_KEY="$HMAC_ACCESS_ID" \
            --from-literal=S3_SECRET_KEY="$HMAC_SECRET"
        echo "Secret created successfully."
    else
        echo "WARNING: Secret 'herddb-gcs-credentials' is missing."
        echo "The file server will fail to start without valid credentials."
        echo "You can create it manually with:"
        echo "  kubectl create secret generic herddb-gcs-credentials \\"
        echo "    --from-literal=S3_ACCESS_KEY=<access-id> \\"
        echo "    --from-literal=S3_SECRET_KEY=<secret>"
        echo ""
        read -rp "Continue anyway? [y/N] " answer
        if [[ ! "$answer" =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
else
    echo "==> Secret 'herddb-gcs-credentials' found."
fi

# ── 4. Install or upgrade the Helm chart ──────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Helm release 'herddb' already exists, upgrading..."
    helm upgrade herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${EXTRA_ARGS[@]}"
else
    echo "==> Installing Helm chart..."
    helm install herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${EXTRA_ARGS[@]}"
fi

# ── 5. Wait for pods ─────────────────────────────────────────────
echo "==> Waiting for all pods to become ready (timeout 3m)..."
kubectl wait --for=condition=ready pod --all --timeout=180s

echo ""
echo "==> HerdDB is ready!"
echo ""
echo "Connect with the CLI:"
echo "  kubectl exec -it deploy/herddb-tools -- herddb-cli"
