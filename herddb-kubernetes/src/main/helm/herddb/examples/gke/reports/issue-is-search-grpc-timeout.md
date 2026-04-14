## Summary

The gRPC timeout used when the HerdDB server calls `IndexingService/Search` is
currently **hardcoded at 30 seconds** (`IndexingServiceClient.java:270`). This is
exposed as a user-visible error when the Indexing Service is temporarily busy (e.g.
writing large FusedPQ segments to GCS) and cannot respond to search requests within
that window.

## Observed failure

During the first GKE gist1m benchmark run (2026-04-14, 1M vectors, dim=960,
FusedPQ index), the query phase started immediately after the post-ingest checkpoint.
At that point both IS replicas were still writing ~1.4 GB segments to GCS as part of
their ongoing checkpoint cycle.

**All 1,000 queries failed** with:

```
herddb.model.StatementExecutionException: remote vector index search failed:
Search timed out waiting for indexing-service instances after 30s
  at herddb.index.vector.VectorIndexManager.search(VectorIndexManager.java:205)
...
Caused by: java.lang.RuntimeException:
  Search timed out waiting for indexing-service instances after 30s
  at herddb.indexing.IndexingServiceClient.search(IndexingServiceClient.java:286)
...
Caused by: java.util.concurrent.TimeoutException:
  Waited 30 seconds (plus ~170000 nanoseconds delay)
  for GrpcFuture[status=PENDING, indexing.IndexingService/Search]
  at herddb.indexing.IndexingServiceClient.search(IndexingServiceClient.java:270)
```

The benchmark produced **Recall@100 = 0.000 / 0 qps** on a dataset that had been
fully ingested and checkpointed.

## Proposed fix

Make the IS search gRPC timeout configurable via a server-side or Helm values
property, e.g.:

```
# herddb-server configuration (e.g. broker.properties or ENV)
indexing.search.timeout.seconds = 120   # default: raise from 30s to 120s

# Or expose via Helm values.yaml
server:
  indexingSearchTimeoutSeconds: 120
```

A 2-minute timeout is a reasonable default for large datasets where the IS may be
performing background segment flushes. For smaller datasets (e.g. sift10k, dim=128)
30s is usually sufficient.

## Additional context

- **Dataset**: GIST1M, 1M vectors, dim=960, FusedPQ index
- **IS segment size**: ~1.4 GB per segment (graph) + ~700 MB (map) at M=16
- **GCS upload rate**: ~50 MB/s → a single segment takes ~20-40s to upload
- **IS replicas**: 2 (IS-0 and IS-1 writing simultaneously)
- **Effect on benchmark**: the IS was non-responsive to search queries for the full
  30s wait, then returned a timeout instead of queuing the request or waiting for
  the ongoing GCS write to finish
- **Related**: this issue would be partially mitigated by adding a "wait for IS idle"
  step between the checkpoint phase and the query phase in the benchmark, but the
  server-side timeout should also be raised to be safe for production workloads

## Cluster config

Image: `ghcr.io/eolivelli/herddb:0.30.0-SNAPSHOT`
IS resources: 32 Gi / 16 CPU, `-Xms24g -Xmx24g`

Run date: 2026-04-14
