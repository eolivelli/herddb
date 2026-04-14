#!/usr/bin/env bash
#
# Install HerdDB on a GKE cluster using Google Cloud Storage.
#
# Two modes:
#
#   ./install.sh                       # interactive — prompts for
#                                      # image, bucket, credentials,
#                                      # optionally builds and pushes
#                                      # the Docker image.
#
#   ./install.sh --non-interactive \   # scripted — consumes flags /
#       --image-tag 0.30.0-SNAPSHOT    # env vars only, never prompts,
#                                      # never builds or pushes.
#
# Non-interactive flags (all optional unless noted):
#
#   --image-repo <repo>        default: $IMAGE_REPO or
#                                       ghcr.io/eolivelli/herddb
#   --image-tag <tag>          default: $IMAGE_TAG or 0.30.0-SNAPSHOT
#   --bucket <gcs-bucket>      default: $GCS_BUCKET or herddb-pages
#   --location <region>        default: $GCS_LOCATION or us-central1
#   --no-create-bucket         fail instead of creating the file-server bucket
#   --no-create-secret         fail instead of creating herddb-gcs-credentials
#   --no-create-pull-secret    fail instead of creating ghcr-credentials
#   --no-wait                  skip the kubectl wait for pods
#
# Prerequisites for non-interactive mode:
#   - KUBECONFIG is set and points at a reachable GKE cluster
#   - The image at <image-repo>:<image-tag> is already pushed
#   - gh, gcloud, helm, kubectl on PATH
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Parse flags ──────────────────────────────────────────────────
NON_INTERACTIVE=false
IMAGE_REPO="${IMAGE_REPO:-}"
IMAGE_TAG="${IMAGE_TAG:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_LOCATION="${GCS_LOCATION:-}"
CREATE_BUCKET=true
CREATE_SECRET=true
CREATE_PULL_SECRET=true
WAIT_FOR_PODS=true
# Any remaining --set k=v etc. go through to helm.
HELM_EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --non-interactive)      NON_INTERACTIVE=true; shift ;;
        --image-repo)           IMAGE_REPO="$2"; shift 2 ;;
        --image-tag)            IMAGE_TAG="$2"; shift 2 ;;
        --bucket)               GCS_BUCKET="$2"; shift 2 ;;
        --location)             GCS_LOCATION="$2"; shift 2 ;;
        --no-create-bucket)     CREATE_BUCKET=false; shift ;;
        --no-create-secret)     CREATE_SECRET=false; shift ;;
        --no-create-pull-secret) CREATE_PULL_SECRET=false; shift ;;
        --no-wait)              WAIT_FOR_PODS=false; shift ;;
        --)                     shift; HELM_EXTRA_ARGS+=("$@"); break ;;
        *)                      HELM_EXTRA_ARGS+=("$1"); shift ;;
    esac
done

fatal() { echo "ERROR: $*" >&2; exit 1; }

if $NON_INTERACTIVE; then
    IMAGE_REPO="${IMAGE_REPO:-ghcr.io/eolivelli/herddb}"
    IMAGE_TAG="${IMAGE_TAG:-0.30.0-SNAPSHOT}"
    GCS_BUCKET="${GCS_BUCKET:-herddb-pages}"
    GCS_LOCATION="${GCS_LOCATION:-us-central1}"
else
    # ── 0. Interactive prompts ─────────────────────────────────
    read -rp "Docker image repository [ghcr.io/eolivelli/herddb]: " in_repo
    IMAGE_REPO="${in_repo:-${IMAGE_REPO:-ghcr.io/eolivelli/herddb}}"

    read -rp "Docker image tag [0.30.0-SNAPSHOT]: " in_tag
    IMAGE_TAG="${in_tag:-${IMAGE_TAG:-0.30.0-SNAPSHOT}}"

    read -rp "GCS bucket name [herddb-pages]: " in_bucket
    GCS_BUCKET="${in_bucket:-${GCS_BUCKET:-herddb-pages}}"

    read -rp "GCS bucket location [us-central1]: " in_loc
    GCS_LOCATION="${in_loc:-${GCS_LOCATION:-us-central1}}"
fi

HELM_EXTRA_ARGS+=(--set "image.repository=$IMAGE_REPO" \
                  --set "image.tag=$IMAGE_TAG" \
                  --set "fileServer.s3.bucket=$GCS_BUCKET")

echo "==> Image:    ${IMAGE_REPO}:${IMAGE_TAG}"
echo "==> Bucket:   gs://${GCS_BUCKET} (${GCS_LOCATION})"
echo "==> Mode:     $( $NON_INTERACTIVE && echo non-interactive || echo interactive )"

# ── 0b. Optional image build & push (interactive only) ──────────
if ! $NON_INTERACTIVE; then
    read -rp "Build and push Docker image ${IMAGE_REPO}:${IMAGE_TAG}? [y/N] " build_image
    if [[ "$build_image" =~ ^[Yy]$ ]]; then
        REPO_ROOT="$(cd "$CHART_DIR/../../../../.." && pwd)"
        echo "==> Building Docker image (this may take a few minutes)..."
        (cd "$REPO_ROOT" && mvn clean package -Ddocker -DskipTests)
        BUILD_VERSION="$(cd "$REPO_ROOT" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
        SOURCE_IMAGE="herddb/herddb-server:${BUILD_VERSION}"
        if [[ "$SOURCE_IMAGE" != "${IMAGE_REPO}:${IMAGE_TAG}" ]]; then
            echo "==> Tagging ${SOURCE_IMAGE} as ${IMAGE_REPO}:${IMAGE_TAG}..."
            docker tag "$SOURCE_IMAGE" "${IMAGE_REPO}:${IMAGE_TAG}"
        fi
        echo "==> Pushing ${IMAGE_REPO}:${IMAGE_TAG}..."
        docker push "${IMAGE_REPO}:${IMAGE_TAG}"
    fi
fi

# ── 0c. Check / create GHCR image-pull secret ────────────────────
if ! kubectl get secret ghcr-credentials >/dev/null 2>&1; then
    if $NON_INTERACTIVE; then
        if $CREATE_PULL_SECRET; then
            fatal "Secret 'ghcr-credentials' not found. In non-interactive mode you must create it yourself (or pass --no-create-pull-secret to proceed without one)."
        else
            echo "WARNING: Secret 'ghcr-credentials' not found; proceeding without it (--no-create-pull-secret)."
        fi
    else
        echo "Secret 'ghcr-credentials' not found (needed to pull from ghcr.io)."
        read -rp "Create it now? [Y/n] " create_ghcr
        if [[ ! "$create_ghcr" =~ ^[Nn]$ ]]; then
            read -rp "GitHub username: " GHCR_USER
            read -rsp "GitHub Personal Access Token (with read:packages scope): " GHCR_TOKEN
            echo ""
            kubectl create secret docker-registry ghcr-credentials \
                --docker-server=ghcr.io \
                --docker-username="$GHCR_USER" \
                --docker-password="$GHCR_TOKEN"
            echo "Secret 'ghcr-credentials' created."
        else
            echo "WARNING: Without 'ghcr-credentials' pods may fail to pull from ghcr.io."
        fi
    fi
else
    echo "==> Secret 'ghcr-credentials' found."
fi

# ── 1. Verify cluster connectivity ────────────────────────────────
echo "==> Verifying cluster connectivity..."
if ! kubectl cluster-info >/dev/null 2>&1; then
    fatal "Cannot reach the Kubernetes cluster. Is KUBECONFIG set correctly?"
fi

# ── 2. Check that the GCS bucket exists ───────────────────────────
echo "==> Checking GCS bucket gs://${GCS_BUCKET}..."
if ! gcloud storage buckets describe "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
    if $NON_INTERACTIVE; then
        if $CREATE_BUCKET; then
            echo "==> Creating bucket gs://${GCS_BUCKET} in ${GCS_LOCATION}..."
            gcloud storage buckets create "gs://${GCS_BUCKET}" --location="${GCS_LOCATION}"
        else
            fatal "Bucket gs://${GCS_BUCKET} does not exist and --no-create-bucket was set."
        fi
    else
        echo "Bucket 'gs://${GCS_BUCKET}' does not exist."
        read -rp "Create it now? [Y/n] " create_bucket
        if [[ ! "$create_bucket" =~ ^[Nn]$ ]]; then
            read -rp "Bucket location [${GCS_LOCATION}]: " in_loc
            GCS_LOCATION="${in_loc:-$GCS_LOCATION}"
            echo "==> Creating bucket gs://${GCS_BUCKET} in ${GCS_LOCATION}..."
            gcloud storage buckets create "gs://${GCS_BUCKET}" --location="${GCS_LOCATION}"
        else
            echo "WARNING: Proceeding without a bucket — the file server will fail to start."
        fi
    fi
else
    echo "Bucket gs://${GCS_BUCKET} exists."
fi

# ── 2b. Optional interactive bucket cleanup ─────────────────────
if ! $NON_INTERACTIVE; then
    read -rp "Clean up (delete all objects in) bucket gs://${GCS_BUCKET}? [y/N] " clean_bucket
    if [[ "$clean_bucket" =~ ^[Yy]$ ]]; then
        if [[ "$GCS_BUCKET" == "herddb-datasets" ]]; then
            fatal "Refusing to wipe gs://herddb-datasets."
        fi
        echo "==> Deleting all objects in gs://${GCS_BUCKET}..."
        gcloud storage rm "gs://${GCS_BUCKET}/**" --recursive 2>/dev/null || true
    fi
fi

# ── 3. Check that the credentials Secret exists ──────────────────
if ! kubectl get secret herddb-gcs-credentials >/dev/null 2>&1; then
    if $NON_INTERACTIVE; then
        if $CREATE_SECRET; then
            fatal "Secret 'herddb-gcs-credentials' not found. In non-interactive mode you must create it yourself (or pass --no-create-secret)."
        else
            echo "WARNING: Secret 'herddb-gcs-credentials' not found; proceeding without it."
        fi
    else
        echo "Secret 'herddb-gcs-credentials' not found."
        read -rp "Create HMAC credentials and Secret now? [Y/n] " create_creds
        if [[ ! "$create_creds" =~ ^[Nn]$ ]]; then
            GCP_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
            if [[ -z "$GCP_PROJECT" ]]; then
                fatal "No GCP project configured. Run 'gcloud config set project PROJECT_ID'."
            fi
            DEFAULT_SA="herddb-gcs@${GCP_PROJECT}.iam.gserviceaccount.com"
            echo "HMAC keys require a GCP service account (not a personal Google account)."
            read -rp "Service account email [${DEFAULT_SA}]: " SA_EMAIL
            SA_EMAIL="${SA_EMAIL:-$DEFAULT_SA}"
            SA_NAME="${SA_EMAIL%%@*}"
            if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
                echo "==> Service account '${SA_EMAIL}' not found. Creating it..."
                gcloud iam service-accounts create "$SA_NAME" \
                    --display-name="HerdDB GCS access"
            fi
            echo "==> Granting roles/storage.objectAdmin on gs://${GCS_BUCKET} to ${SA_EMAIL}..."
            gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
                --member="serviceAccount:${SA_EMAIL}" \
                --role="roles/storage.objectAdmin" --quiet
            echo "==> Granting roles/storage.objectViewer on gs://herddb-datasets to ${SA_EMAIL}..."
            gcloud storage buckets add-iam-policy-binding "gs://herddb-datasets" \
                --member="serviceAccount:${SA_EMAIL}" \
                --role="roles/storage.objectViewer" --quiet || \
                echo "WARNING: could not bind objectViewer on gs://herddb-datasets. Custom datasets may be inaccessible."
            echo "==> Creating HMAC key for ${SA_EMAIL}..."
            HMAC_OUTPUT="$(gcloud storage hmac create "$SA_EMAIL" --format=json)"
            HMAC_ACCESS_ID="$(echo "$HMAC_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['metadata']['accessId'])")"
            HMAC_SECRET="$(echo "$HMAC_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['secret'])")"
            if [[ -z "$HMAC_ACCESS_ID" || -z "$HMAC_SECRET" ]]; then
                fatal "Failed to parse HMAC credentials from gcloud output."
            fi
            echo "==> Creating Kubernetes secret 'herddb-gcs-credentials'..."
            kubectl create secret generic herddb-gcs-credentials \
                --from-literal=S3_ACCESS_KEY="$HMAC_ACCESS_ID" \
                --from-literal=S3_SECRET_KEY="$HMAC_SECRET"
            echo "Secret created successfully."
        else
            echo "WARNING: Secret 'herddb-gcs-credentials' is missing."
            echo "You can create it manually with:"
            echo "  kubectl create secret generic herddb-gcs-credentials \\"
            echo "    --from-literal=S3_ACCESS_KEY=<access-id> \\"
            echo "    --from-literal=S3_SECRET_KEY=<secret>"
            read -rp "Continue anyway? [y/N] " answer
            if [[ ! "$answer" =~ ^[Yy]$ ]]; then
                exit 1
            fi
        fi
    fi
else
    echo "==> Secret 'herddb-gcs-credentials' found."
fi

# ── 4. Install or upgrade the Helm chart ──────────────────────────
if helm status herddb >/dev/null 2>&1; then
    echo "==> Helm release 'herddb' already exists, upgrading..."
    helm upgrade herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${HELM_EXTRA_ARGS[@]}"
else
    echo "==> Installing Helm chart..."
    helm install herddb "$CHART_DIR" -f "$SCRIPT_DIR/values.yaml" "${HELM_EXTRA_ARGS[@]}"
fi

# ── 5. Wait for pods ─────────────────────────────────────────────
if $WAIT_FOR_PODS; then
    echo "==> Waiting for all pods to become ready (timeout 5m)..."
    kubectl wait --for=condition=ready pod \
        -l app.kubernetes.io/instance=herddb \
        --all --timeout=300s || {
        echo "WARNING: not all pods became ready within 5m; run scripts/check-cluster.sh" >&2
        exit 1
    }
fi

cat <<EOF

==> HerdDB is ready!

Connect with the CLI:
  kubectl exec -it sts/herddb-tools -- herddb-cli

Run a vector benchmark:
  ./scripts/run-bench.sh --dataset sift10k -n 10000 -k 100 \\
      --ingest-max-ops 1000 --checkpoint
EOF
