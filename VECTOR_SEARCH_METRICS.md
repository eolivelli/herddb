# Vector Search Latency Monitoring

This document describes the comprehensive Prometheus metrics and Grafana dashboard for monitoring vector search performance across all layers of the read path.

## Overview

The vector search read path instrumentation provides end-to-end latency, throughput, and error monitoring across:
- HerdDB server query execution
- Index server search operations
- File server reads (disk and S3)
- Client-side read operations

## Available Metrics

### 1. Vector Query Execution (HerdDB Server)

**Scope:** `tablespace_<tablespace>_vector_query_*`

| Metric | Type | Description |
|--------|------|-------------|
| `vector_query_latency` | OpStatsLogger | Query execution latency (p50, p95, p99, p999) |
| `vector_query_requests` | Counter | Total query requests |
| `vector_query_errors` | Counter | Failed queries |

**Location:** `herddb-core/src/main/java/herddb/index/vector/VectorIndexManager.java`

### 2. Index Server Search Operations

**Scope:** `indexing_*`

| Metric | Type | Description |
|--------|------|-------------|
| `indexing_search_latency` | OpStatsLogger | Search RPC latency (p50, p95, p99, p999) |
| `indexing_search_requests` | Counter | Total search requests |
| `indexing_search_errors` | Counter | Failed searches |
| `indexing_search_bytes` | Counter | Response size in bytes |

**Location:** `herddb-indexing-service/src/main/java/herddb/indexing/IndexingServiceImpl.java`

### 3. File Server Range Reads (Server-Side)

**Scope:** `rfs_readrange_*`

| Metric | Type | Description |
|--------|------|-------------|
| `rfs_readrange_latency` | OpStatsLogger | Read latency (p50, p95, p99, p999) |
| `rfs_readrange_requests` | Counter | Total range read requests |
| `rfs_readrange_bytes` | Counter | Bytes read |

**Location:** `herddb-remote-file-service/src/main/java/herddb/remote/RemoteFileServiceImpl.java` (existing)

### 4. File Server Client Reads (Indexing Service → File Server)

**Scope:** `rfs_client_read_*`

| Metric | Type | Description |
|--------|------|-------------|
| `rfs_client_read_latency` | OpStatsLogger | Client read latency (p50, p95, p99, p999) |
| `rfs_client_read_requests` | Counter | Total client read requests |
| `rfs_client_read_bytes` | Counter | Bytes read |

**Location:** `herddb-remote-file-service/src/main/java/herddb/remote/RemoteRandomAccessReader.java`

### 5. File Server Disk Reads

**Scope:** `rfs_disk_read_*`

| Metric | Type | Description |
|--------|------|-------------|
| `rfs_disk_read_latency` | OpStatsLogger | Disk read latency (p50, p95, p99, p999) |
| `rfs_disk_read_requests` | Counter | Total disk read requests |
| `rfs_disk_read_bytes` | Counter | Bytes read from disk |

**Location:** `herddb-remote-file-service/src/main/java/herddb/remote/storage/LocalObjectStorage.java`

### 6. File Server S3 Reads

**Scope:** `rfs_s3_read_*`

| Metric | Type | Description |
|--------|------|-------------|
| `rfs_s3_read_latency` | OpStatsLogger | S3 read latency (p50, p95, p99, p999) |
| `rfs_s3_read_requests` | Counter | Total S3 read requests |
| `rfs_s3_read_bytes` | Counter | Bytes read from S3 |

**Location:** `herddb-remote-file-service/src/main/java/herddb/remote/storage/S3ObjectStorage.java`

## Grafana Dashboards

### Dedicated Vector Search Latency Dashboard

**File:** `herddb-kubernetes/src/main/helm/herddb/monitor/grafana/dashboards/vector-search-latency-dashboard.json`

This comprehensive dashboard provides 6 rows of monitoring covering the complete vector search read path:

1. **Vector Query Execution (HerdDB Server)** - Query latency, rate, errors
2. **Index Server Search (Indexing Service)** - Search latency, rate, response throughput
3. **File Server Client Reads** - Client latency, rate, throughput
4. **File Server Range Reads** - Server-side latency, rate, throughput
5. **File Server Disk Reads** - Disk latency, rate, throughput
6. **File Server S3 Reads** - S3 latency, rate, throughput

Each row includes:
- Latency panel with p50, p95, p99, p99.9 quantiles and average
- Rate panel showing operations/second
- Throughput panel showing bytes/second

### Integrating with Existing Dashboards (Optional)

The metrics can also be integrated into existing dashboards:

- **herddb-dashboard.json**: Add vector query execution metrics as a new row
- **indexing-service-dashboard.json**: Add search bytes counter to the "Search RPC" row
- **remote-file-service-dashboard.json**: Add disk/S3 read metrics as new rows

See the metric names and scopes above to add relevant panels to these dashboards.

## Accessing Metrics

### Via HTTP /metrics Endpoint

Each service exposes metrics on its HTTP metrics endpoint:

```bash
# HerdDB Server metrics
curl http://localhost:9845/metrics | grep vector_query

# Indexing Service metrics
curl http://localhost:9851/metrics | grep indexing_search

# File Server metrics
curl http://localhost:9847/metrics | grep rfs_
```

### Via Prometheus

Configure Prometheus to scrape the `/metrics` endpoints:

```yaml
scrape_configs:
  - job_name: 'herddb-server'
    static_configs:
      - targets: ['localhost:9845']
  
  - job_name: 'indexing-service'
    static_configs:
      - targets: ['localhost:9851']
  
  - job_name: 'file-server'
    static_configs:
      - targets: ['localhost:9847']
```

### Via Grafana Dashboard

Open the "HerdDB Vector Search Latency" dashboard in Grafana to view all metrics in one place.

## Testing Metrics

Run the included unit tests:

```bash
# Test storage layer metrics (disk/S3 reads)
mvn -pl herddb-remote-file-service -Dtest=RemoteFileServiceStorageMetricsTest test

# Test IndexingService search bytes counter
mvn -pl herddb-indexing-service -Dtest=IndexingServiceSearchBytesTest test
```

## Implementation Notes

### Backward Compatibility

All metric instrumentation is backward compatible:
- StatsLogger parameters are optional (nullable)
- Constructor overloads provided for existing callers
- No breaking changes to public APIs

### Async-Aware Instrumentation

Storage layer reads (disk and S3) use async APIs (CompletableFuture):
- Metrics are recorded via CompletionHandlers
- Success and failure paths are tracked separately
- Latency includes full async operation time

### Quantiles

BookKeeper OpStatsLogger provides:
- p50 (median)
- p95 (90th percentile proxy)
- p99 (99th percentile)
- p999 (99.9th percentile / "pmax" proxy)

Plus computed average: `sum(rate(metric_sum[1m])) / sum(rate(metric_count[1m]))`

## Future Enhancements

- Add per-tablespace latency metrics
- Add per-vector-index granularity
- Integrate metrics into existing dashboards
- Add alerting rules for latency thresholds
- Add more test coverage for edge cases

## References

- Prometheus Metrics: `/metrics` HTTP endpoint
- BookKeeper StatsLogger: `org.apache.bookkeeper.stats`
- Grafana Dashboards: `herddb-kubernetes/src/main/helm/herddb/monitor/grafana/dashboards/`
