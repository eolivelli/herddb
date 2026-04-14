#!/usr/bin/env bash
#
# Build the HerdDB Docker image from the current source tree and push
# it to the configured container registry.
#
# This is the same build logic that ./install.sh exposes in interactive
# mode, extracted into a standalone non-interactive script so that the
# GKE bench agent (and CI) can call it without requiring a human at a
# terminal.
#
# Usage:
#   ./scripts/build-and-push.sh
#   ./scripts/build-and-push.sh --image-repo ghcr.io/eolivelli/herddb
#   ./scripts/build-and-push.sh --image-tag 0.30.0-SNAPSHOT
#   ./scripts/build-and-push.sh --image-repo <repo> --image-tag <tag> \
#       [--skip-tests] [--mvn-opts "-Pfoo -Pbar"]
#
# Flags:
#   --image-repo <repo>   Registry + image name (default: $IMAGE_REPO or
#                         ghcr.io/eolivelli/herddb)
#   --image-tag  <tag>    Tag to apply (default: $IMAGE_TAG or the Maven
#                         project.version, e.g. 0.30.0-SNAPSHOT)
#   --skip-tests          Pass -DskipTests to Maven (default: true, since
#                         this script is for build-and-push only)
#   --no-skip-tests       Run tests during the build (slow)
#   --mvn-opts <opts>     Extra Maven options, e.g. "-Pfoo". Appended
#                         after the fixed flags.
#
# On success prints:
#   PUSHED_IMAGE=<repo>:<tag>
#
# Requirements:
#   - Maven (mvn) on PATH
#   - Docker on PATH and the daemon running
#   - Write access to the target registry (docker login beforehand)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# scripts/ → gke/ → examples/ → helm/herddb/  (the Helm chart root)
CHART_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
# helm/herddb/ → helm/ → main/ → src/ → herddb-kubernetes/ → repo root
REPO_ROOT="$(cd "$CHART_DIR/../../../../.." && pwd)"

fatal() { echo "ERROR: $*" >&2; exit 1; }
section() { printf '\n==> %s\n' "$1"; }

# ── Defaults ──────────────────────────────────────────────────────────
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/eolivelli/herddb}"
IMAGE_TAG="${IMAGE_TAG:-}"       # resolved later from Maven if empty
SKIP_TESTS=true
MVN_OPTS=""

# ── Parse flags ───────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --image-repo)    IMAGE_REPO="$2"; shift 2 ;;
        --image-tag)     IMAGE_TAG="$2";  shift 2 ;;
        --skip-tests)    SKIP_TESTS=true;  shift ;;
        --no-skip-tests) SKIP_TESTS=false; shift ;;
        --mvn-opts)      MVN_OPTS="$2";   shift 2 ;;
        --help|-h)
            sed -n '2,/^set -/p' "$0" | grep '^#' | sed 's/^# \?//'
            exit 0 ;;
        *) fatal "Unknown argument: $1" ;;
    esac
done

# ── Prerequisite checks ───────────────────────────────────────────────
command -v mvn    >/dev/null 2>&1 || fatal "mvn not found on PATH"
command -v docker >/dev/null 2>&1 || fatal "docker not found on PATH"

section "Build configuration"
echo "  Repo root:  $REPO_ROOT"
echo "  Image repo: $IMAGE_REPO"
echo "  Skip tests: $SKIP_TESTS"
[[ -n "$MVN_OPTS" ]] && echo "  Extra opts: $MVN_OPTS"

# ── Step 1: Maven build with Docker image creation ────────────────────
SKIP_TESTS_FLAG=""
$SKIP_TESTS && SKIP_TESTS_FLAG="-DskipTests"

section "Building Docker image via Maven (-Ddocker)"
# shellcheck disable=SC2086  # MVN_OPTS is intentionally unquoted for word-splitting
(cd "$REPO_ROOT" && mvn clean package -Ddocker $SKIP_TESTS_FLAG $MVN_OPTS)

# ── Step 2: Resolve the built image name from Maven ───────────────────
BUILD_VERSION="$(cd "$REPO_ROOT" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout)"
[[ -z "$BUILD_VERSION" ]] && fatal "Could not determine Maven project.version"
SOURCE_IMAGE="herddb/herddb-server:${BUILD_VERSION}"

# If the caller didn't supply --image-tag, default to the Maven version.
IMAGE_TAG="${IMAGE_TAG:-$BUILD_VERSION}"
TARGET_IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"

section "Tagging and pushing"
echo "  Source: $SOURCE_IMAGE"
echo "  Target: $TARGET_IMAGE"

if [[ "$SOURCE_IMAGE" != "$TARGET_IMAGE" ]]; then
    echo "==> docker tag $SOURCE_IMAGE $TARGET_IMAGE"
    docker tag "$SOURCE_IMAGE" "$TARGET_IMAGE"
fi

echo "==> docker push $TARGET_IMAGE"
docker push "$TARGET_IMAGE"

echo ""
echo "PUSHED_IMAGE=$TARGET_IMAGE"
