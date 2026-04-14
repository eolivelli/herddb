## Summary

The `IndexingService/Search` gRPC call times out after 30 seconds whenever the
Indexing Service is busy flushing large FusedPQ segments (~1.4 GB each for
GIST1M dim=960) to GCS through the file server.  The root cause is **I/O
contention on the file-server pod** between concurrent background segment writes
(during checkpoint/catch-up) and the gRPC read requests from the search path.

This issue has been reproduced on two consecutive GKE benchmark runs with 1M
GIST1M vectors:

| Run | Ingest rate | Ingest time | Checkpoint | Query result |
|-----|-------------|-------------|------------|--------------|
| 1   | 1,000 ops/s | 1000.4 s    | 387.7 s    | **0 qps / Recall=0** |
| 2   | 10,000 ops/s (actual ~3,250) | 307.7 s | 0.0 s | **0 qps / Recall=0** |

Both runs saw **all 1,000 queries fail** with:
```
Search timed out waiting for indexing-service instances after 30s
  at IndexingServiceClient.search(IndexingServiceClient.java:270)
Caused by: java.util.concurrent.TimeoutException:
  Waited 30 seconds for GrpcFuture[indexing.IndexingService/Search, status=PENDING]
```

---

## Root-cause chain

### 1. IS background GCS segment writes (heavy I/O on the file server)

During ingest, the IS continuously checkpoints its in-memory FusedPQ shards by
writing large segment files to GCS via the file server gRPC service:

```
writeMultipartIndexFile: .../vidx_vector_bench_.../multipart/graph
    written 1,434,367,888 bytes in blocks of 4,194,304
writeMultipartIndexFile: .../vidx_vector_bench_.../multipart/map
    written 712,500,116 bytes in blocks of 4,194,304
checkpoint vidx Phase B: segment 1/2 (184,777 nodes) written in 163,067 ms (multipart)
```

Each phase-B cycle for GIST1M pushes ~2.1 GB to GCS through the single
file-server pod. Parallelism=2 means two segments are written simultaneously,
leading to sustained ~4 GB/min peak I/O on the FS pod.

### 2. File-server saturation → DEADLINE_EXCEEDED on IS reads

While the segment writes are in flight, the IS also calls `readFileRange` on
the file server (to load checkpoint state for tailing). These read calls
exceed their gRPC deadline because the FS gRPC thread pool is saturated
handling concurrent write RPCs:

```
IS-0: remote file readFileRange retry 1/10 for path
  .../vidx_vector_bench_2295499842506_seg1/multipart/graph
  after 1,000ms (error: DEADLINE_EXCEEDED: context timed out)
IS-1: remote file readFileRange retry 1/10 for path
  .../vidx_vector_bench_3833264521526_seg2/multipart/graph
  after 1,000ms (error: CANCELLED: RPC cancelled)
```

The FS metrics confirm writes **succeeded** (0 `rfs_writeblock_errors`, 0
`rfs_readrange_errors`) and reads complete fast when the FS is free
(`rfs_readrange_latency` p99 = 15 ms). The bottleneck is gRPC thread
exhaustion, not GCS throughput.

### 3. IS search gRPC blocked by the same FS contention

When the benchmark triggers `IndexingService/Search`, the IS must scan its
in-memory + on-disk FusedPQ index.  The on-disk scan calls `readFileRange` to
stream node pages from the FS.  Because the FS is still writing segment files,
these search-path `readFileRange` calls also queue behind write RPCs and time
out — causing the server-side IS search future to remain `PENDING` for 30 s,
hitting the hardcoded `IndexingServiceClient` timeout.

**All 4 query threads (ranges [0,250), [250,500), [500,750), [750,1000)) fail
concurrently** once the 30 s wall-clock deadline fires.

---

## Flamegraph evidence

Async-profiler flamegraphs (cpu/wall/alloc/lock) collected on all three pods
**during the live contention window** (concurrent ingest writes + query
timeouts):

- IS-0 profiles: `/home/eolivelli/dev/tests/profiles-herddb-indexing-service-0-20260414-130939/`
- IS-1 profiles: `/home/eolivelli/dev/tests/profiles-herddb-indexing-service-1-20260414-130946/`
- FS-0 profiles: `/home/eolivelli/dev/tests/profiles-herddb-file-server-0-20260414-130953/`

**Expected hot-paths to investigate in the flamegraphs:**

For the IS (cpu/wall):
- `RemoteFileServiceClient.retryAsync` / `readFileRange` blocking threads
- `AbstractFuture.blockingGet` (holding `IndexingServiceClient.search` threads)
- `writeFusedPQGraphToTempFile` writing large in-memory graphs (CPU heavy)

For the file server (cpu/wall/lock):
- gRPC `ServerCallHandler` / `ServerImpl` — thread pool saturation under
  concurrent read+write RPCs
- `MultiPartS3Manager` / `GCSUploadStream` — GCS upload bandwidth
- Any lock contention between read and write I/O paths

---

## Suggested fixes

1. **Separate IS read and write I/O through the file server** — give checkpoint
   segment writes lower priority than search-path reads, or use separate gRPC
   channels / thread pools.

2. **Raise (or make configurable) the IS search deadline** — the 30 s
   hardcoded deadline in `IndexingServiceClient.java:270` should be a
   configurable property (see issue #99).

3. **Pause IS background checkpoint flush during the query phase** — the
   benchmark could call `CHECKPOINT` and then wait for the IS to go idle
   (no active segment writes) before issuing the first query.  The vector-bench
   `--checkpoint` flow does not currently have such a gate.

4. **Add file-server write admission control** — when the FS write queue
   exceeds a threshold, accept new writeBlock RPCs asynchronously (queue +
   back-pressure) rather than blocking all gRPC threads.

---

## Cluster config at time of profiling

| Component | Image | JVM heap | CPU |
|-----------|-------|----------|-----|
| IndexingService | `ghcr.io/eolivelli/herddb:0.30.0-SNAPSHOT` | 24 GB | 16 |
| FileServer | `ghcr.io/eolivelli/herddb:0.30.0-SNAPSHOT` | 12 GB | 8 |

Dataset: GIST1M, 1M vectors, dim=960, FusedPQ (M=16, beamWidth=100)
Run date: 2026-04-14, GKE `europe-west12`
