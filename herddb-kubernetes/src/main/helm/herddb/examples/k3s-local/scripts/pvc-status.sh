#!/usr/bin/env bash
#
# Print PVC status and actual disk usage for the HerdDB StatefulSet pods.
#
# Two sections:
#   1. `kubectl get pvc` summary (name, status, capacity, storage-class)
#   2. Per-pod `df -h` on the data mount points, so the user can see how
#      much of each PVC is actually in use — important during long
#      benchmark runs to catch creeping disk pressure.
#
# Usage: ./scripts/pvc-status.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

section "PVCs"
kubectl -n default get pvc \
    -o custom-columns=NAME:.metadata.name,STATUS:.status.phase,CAPACITY:.status.capacity.storage,STORAGECLASS:.spec.storageClassName

section "Disk usage per pod"
# Note: on k3s-local's local-path provisioner every PVC is a bind-mount
# onto the same host filesystem, so `df` reports the host-fs totals for
# all mount points (useful for overall "am I running out of host disk?").
# We additionally run `du -sh` on each mount to show the actual per-PVC
# footprint, which is what matters during long benchmarks.

# pod -> mount-path pairs. Keep this list in sync with the StatefulSet
# volumeMounts used by the Helm chart.
declare -a TARGETS=(
    "herddb-server-0:/opt/herddb/data"
    "herddb-server-0:/opt/herddb/commitlog"
    "herddb-file-server-0:/opt/herddb/filedata"
    "herddb-indexing-service-0:/opt/herddb/indexdata"
    "herddb-indexing-service-0:/opt/herddb/indexlog"
    "herddb-bookkeeper-0:/opt/herddb/bookie-data/journal"
    "herddb-bookkeeper-0:/opt/herddb/bookie-data/ledgers"
    "herddb-minio-0:/data"
    "herddb-zookeeper-0:/opt/herddb/zk-data"
    "herddb-tools-0:/opt/herddb/vector-datasets"
)

# Disable pipefail inside this loop: some mount paths may not exist on every
# pod (e.g. when components are disabled) and we don't want one missing path
# to abort the whole report.
set +e +o pipefail

for t in "${TARGETS[@]}"; do
    pod="${t%%:*}"
    path="${t#*:}"
    # Some pods may not exist (e.g. if a component is disabled). Skip gracefully.
    if ! kubectl -n default get pod "$pod" >/dev/null 2>&1; then
        printf '  %-30s %-40s (pod not present)\n' "$pod" "$path"
        continue
    fi
    raw="$(kubectl -n default exec "$pod" -- df -P "$path" 2>/dev/null)"
    df_line="$(printf '%s\n' "$raw" | awk 'NR==2 {printf "hostfs_use=%s avail=%s", $5, $4}')"
    # du -sh = actual size consumed by this mount only. Use -x to stay on
    # the same fs (avoids wandering into /proc etc if the mount is bind-nested).
    du_line="$(kubectl -n default exec "$pod" -- du -sxh "$path" 2>/dev/null | awk '{printf "pvc_used=%s", $1}')"
    if [[ -z "$df_line" && -z "$du_line" ]]; then
        info="(mount not found)"
    else
        info="${du_line} ${df_line}"
    fi
    printf '  %-30s %-40s %s\n' "$pod" "$path" "$info"
done

set -e -o pipefail
