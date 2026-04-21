#!/usr/bin/env bash
#
# Remove the custom-dataset cache from the tools pod so that the next
# run of run-bench.sh will re-download all dataset files from GCS.
#
# Usage:
#   ./scripts/clean-dataset-cache.sh [--yes]
#
# Without --yes the script will prompt for confirmation before deleting.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

DATASET_CACHE_DIR="/opt/herddb/vector-datasets/custom"
AUTO_YES=false

for arg in "$@"; do
    case "$arg" in
        --yes) AUTO_YES=true ;;
        --help|-h)
            echo "Usage: $0 [--yes]"
            echo "  --yes  Skip confirmation prompt"
            exit 0
            ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

section "Custom-dataset cache cleanup"
echo "  Pod:  herddb-tools-0"
echo "  Dir:  $DATASET_CACHE_DIR"
echo ""

# Show current cache contents
echo "Current cache contents:"
kubectl exec herddb-tools-0 -- ls -lh "$DATASET_CACHE_DIR" 2>/dev/null || echo "  (directory does not exist)"
echo ""

if [ "$AUTO_YES" = false ]; then
    read -r -p "Delete entire cache directory? [y/N] " CONFIRM
    case "$CONFIRM" in
        [yY]*) ;;
        *) echo "Aborted."; exit 0 ;;
    esac
fi

section "Deleting cache directory"
kubectl exec herddb-tools-0 -- rm -rf "$DATASET_CACHE_DIR"
echo "  Done. Cache directory deleted."
echo ""
echo "The next run-bench.sh invocation will re-download all dataset files from GCS."
