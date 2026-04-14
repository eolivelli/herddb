## Summary

When HerdDB Server is configured in `storageMode: remote` (using BookKeeper as the
durable write-ahead log), the `commitlog` PVC created by the Helm chart for the
`herddb-server` StatefulSet is unnecessary.

In BookKeeper-backed mode, **every write is journaled by BookKeeper** before the server
acknowledges it. BookKeeper's own journal PVCs (`journal-herddb-bookkeeper-*`) provide
the durability guarantee. The local `commitlog` volume on the server pod is either empty
or redundant, adding unnecessary storage cost and complicating cluster resets.

## Expected behaviour

When `server.storageMode: remote` (the default for cluster/GKE deployments), the Helm
chart should **not** provision a `commitlog` PVC for `herddb-server`. The volume and
`volumeClaimTemplates` entry should only be created when `storageMode: local` (standalone
mode without BookKeeper).

## Observed behaviour

`values.yaml` (GKE example):
```yaml
server:
  mode: cluster
  storageMode: remote   # <-- BookKeeper is the WAL
  storage:
    data:
      size: 10Gi
    commitlog:           # <-- this PVC is provisioned but serves no purpose
      size: 5Gi
```

This results in a `commitlog-herddb-server-0` PVC being created even though the server
delegates durability to BookKeeper. During cluster resets this PVC needs to be deleted
alongside the others, and it was the root cause of a reset timeout in the first GKE
benchmark run (the PVC was marked for deletion while the pod was still mounting it,
causing `kubectl delete pvc --wait=true --timeout=120s` to time out).

## Proposed fix

In `herddb-kubernetes/src/main/helm/herddb/templates/statefulset-server.yaml`,
wrap the `commitlog` `volumeClaimTemplate` and the corresponding `volumeMount` in a
conditional:

```yaml
{{- if eq .Values.server.storageMode "local" }}
- name: commitlog
  ...
{{- end }}
```

Also update `values.yaml` to document that `storage.commitlog` is only relevant for
`storageMode: local`.

## Context

- Cluster: GKE Autopilot, `europe-west12`
- Helm chart revision: 2
- Image: `ghcr.io/eolivelli/herddb:0.30.0-SNAPSHOT`
- Discovered during the first automated GKE benchmark run on 2026-04-14
