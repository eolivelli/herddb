# HerdDB on GKE with Google Cloud Storage

Deploy HerdDB on Google Kubernetes Engine using Google Cloud Storage (GCS) as
the object store via its S3-compatible endpoint.

## Architecture

| Component        | Replicas | Purpose                                          |
|------------------|----------|--------------------------------------------------|
| HerdDB server    | 1        | JDBC endpoint, metadata (ZooKeeper), commit log (BookKeeper) |
| File server      | 1        | gRPC page storage, backed by GCS (S3 endpoint)   |
| ZooKeeper        | 1        | Cluster coordination and metadata storage         |
| BookKeeper       | 1        | Distributed commit log                            |
| Indexing service | 1        | Vector indexing service                           |
| Tools pod        | 1        | Pre-configured CLI for interactive queries        |

MinIO is **not** deployed; the file server connects directly to GCS.

## Prerequisites

- A running GKE cluster with `kubectl` configured
- [Helm 3](https://helm.sh/docs/intro/install/)
- `gcloud` CLI authenticated
- The HerdDB Docker image pushed to a registry accessible from GKE
  (e.g. Artifact Registry)

## Setup

### 1. Create a GCS bucket

```bash
gcloud storage buckets create gs://my-herddb-pages --location=us-central1
```

### 2. Create HMAC keys

GCS exposes an S3-compatible API that uses HMAC credentials for
authentication. Create a pair for a service account that has
`roles/storage.objectAdmin` on the bucket:

```bash
gcloud storage hmac create SERVICE_ACCOUNT_EMAIL
```

Save the **Access ID** and **Secret** from the output.

### 3. Create a Kubernetes Secret

```bash
kubectl create secret generic herddb-gcs-credentials \
  --from-literal=S3_ACCESS_KEY=<access-id> \
  --from-literal=S3_SECRET_KEY=<secret>
```

The Helm chart references this Secret by name
(`fileServer.s3.credentialsSecret`) and injects the values at pod startup.
Credentials never appear in ConfigMaps or Helm values stored in the cluster.

### 4. Install the Helm chart

From the `herddb-kubernetes/src/main/helm/herddb/` directory:

```bash
helm install herddb . -f examples/gke/values.yaml \
  --set fileServer.s3.bucket=my-herddb-pages
```

If the Docker image is in a private registry, also set:

```bash
--set image.repository=us-docker.pkg.dev/MY_PROJECT/herddb/herddb-server
```

### 5. Wait for pods

```bash
kubectl get pods -w
```

All pods (server, file-server, zookeeper, bookkeeper, indexing-service, tools)
should reach `Running` / `Ready`.

### 6. Verify

```bash
kubectl exec -it deploy/herddb-tools -- herddb-cli
```

```sql
SELECT * FROM SYSTABLES;
```

Check the file server logs to confirm a successful connection to GCS:

```bash
kubectl logs statefulset/herddb-file-server
```

## Vector search example

HerdDB supports vector search with approximate nearest neighbor (ANN) indexes.
Here is a quick walkthrough using the CLI's Groovy scripting support.

### Create a table with a vector column and an index

```bash
kubectl exec deploy/herddb-tools -- herddb-cli \
  -q "CREATE TABLE documents (id int primary key, content string, embedding floata not null)"

kubectl exec deploy/herddb-tools -- herddb-cli \
  -q "CREATE VECTOR INDEX vidx ON documents(embedding)"
```

### Insert and search via a Groovy script

Vector data must be inserted through JDBC prepared statements. The CLI's
`-g` flag runs a Groovy script with a pre-configured `connection` object.

Create a file called `vector_demo.groovy` on the tools pod:

```bash
kubectl exec deploy/herddb-tools -- bash -c 'cat > /tmp/vector_demo.groovy << '\''SCRIPT'\''
import java.sql.*

// Insert sample documents with 3-dimensional embeddings
def insertSql = "INSERT INTO documents(id, content, embedding) VALUES(?, ?, CAST(? AS FLOAT ARRAY))"
def ps = connection.prepareStatement(insertSql)

def data = [
  [1, "The cat sat on the mat",       [0.1f, 0.9f, 0.0f] as float[]],
  [2, "The dog chased the ball",      [0.8f, 0.1f, 0.0f] as float[]],
  [3, "A kitten played with yarn",    [0.2f, 0.8f, 0.1f] as float[]],
  [4, "The puppy ran in the park",    [0.7f, 0.2f, 0.1f] as float[]],
  [5, "Birds were singing at dawn",   [0.0f, 0.1f, 0.9f] as float[]],
]

data.each { row ->
  ps.setInt(1, row[0])
  ps.setString(2, row[1])
  ps.setObject(3, row[2])
  ps.executeUpdate()
}
ps.close()
println "Inserted ${data.size()} documents"

// Cosine similarity search
def searchVec = [0.15f, 0.85f, 0.05f] as float[]
def searchSql = """SELECT id, content,
  cosine_similarity(embedding, CAST(? AS FLOAT ARRAY)) AS score
  FROM documents
  ORDER BY cosine_similarity(embedding, CAST(? AS FLOAT ARRAY)) DESC
  LIMIT 3"""
def query = connection.prepareStatement(searchSql)
query.setObject(1, searchVec)
query.setObject(2, searchVec)
def rs = query.executeQuery()

println ""
println "Top 3 documents by cosine similarity to [0.15, 0.85, 0.05]:"
println "-------------------------------------------------------------"
while (rs.next()) {
  printf "  id=%d  score=%.4f  \"%s\"%n", rs.getInt("id"), rs.getFloat("score"), rs.getString("content")
}
rs.close()
query.close()

// ANN search (uses the vector index)
println ""
def annSql = "SELECT id, content FROM documents ORDER BY ann_of(embedding, CAST(? AS FLOAT ARRAY)) DESC LIMIT 3"
def annQuery = connection.prepareStatement(annSql)
annQuery.setObject(1, searchVec)
def annRs = annQuery.executeQuery()
println "ANN search results (uses vector index):"
println "-----------------------------------------"
while (annRs.next()) {
  printf "  id=%d  \"%s\"%n", annRs.getInt("id"), annRs.getString("content")
}
annRs.close()
annQuery.close()
SCRIPT
'
```

Run the script:

```bash
kubectl exec deploy/herddb-tools -- herddb-cli -g /tmp/vector_demo.groovy
```

Expected output:

```
Inserted 5 documents

Top 3 documents by cosine similarity to [0.15, 0.85, 0.05]:
-------------------------------------------------------------
  id=1  score=0.9963  "The cat sat on the mat"
  id=3  score=0.9956  "A kitten played with yarn"
  id=4  score=0.4407  "The puppy ran in the park"

ANN search results (uses vector index):
-----------------------------------------
  id=1  "The cat sat on the mat"
  id=3  "A kitten played with yarn"
  id=4  "The puppy ran in the park"
```

### Vector search SQL reference

| Function | Description |
|----------|-------------|
| `cosine_similarity(col, vec)` | Cosine similarity (brute-force, higher = more similar) |
| `ann_of(col, vec)` | Approximate nearest neighbor (uses vector index if available) |
| `dot_product(col, vec)` | Dot product distance |
| `euclidean_distance(col, vec)` | Euclidean distance (lower = more similar) |

Vectors are passed as JDBC parameters with `CAST(? AS FLOAT ARRAY)`.
The column type for vector data is `floata` (float array).

## Running the vector benchmark

Once HerdDB is up and the `herddb-tools` StatefulSet is Ready, you
can run the `vector-bench` workload with the helper scripts under
`scripts/`. They mirror the k3s-local benchmark helpers and are used
by the `herddb-gke-bench` Claude Code agent (see
`.claude/agents/herddb-gke-bench.md`).

### Scripted install

For agent-driven workflows, `install.sh` accepts a `--non-interactive`
flag that consumes flags or environment variables instead of
prompting:

```bash
./install.sh --non-interactive \
    --image-tag 0.30.0-SNAPSHOT \
    --bucket my-herddb-pages \
    --location us-central1
```

All three flags have sensible defaults; the image, HMAC secret, and
GHCR pull secret must already exist in the cluster (the script
refuses to create them in non-interactive mode).

### Running a workload

```bash
./scripts/check-cluster.sh    # health check

./scripts/run-bench.sh \
    --dataset sift10k -n 10000 -k 100 \
    --ingest-max-ops 1000 --checkpoint
```

`run-bench.sh` executes `vector-bench.sh` inside the tools pod and
tees plain-text output (`--no-progress` is always on) to
`reports/run-<timestamp>.log`. The last line of stdout is
`RUN_LOG=<path>`. Turn it into a markdown report with:

```bash
./scripts/write-report.sh reports/run-20260414-101530.log
```

### Custom datasets from gs://herddb-datasets

Standard presets (`sift10k`, `sift1m`, `gist1m`, …) are resolved via
the built-in public URLs in `DatasetLoader`. Custom datasets live in
the shared bucket `gs://herddb-datasets`. The GKE values set
`tools.gcs.datasetsBucket=herddb-datasets`, which causes the tools
StatefulSet to export `VECTORBENCH_DATASETS_BUCKET=gs://herddb-datasets`.
Pass an explicit `--dataset-url` through `run-bench.sh` to fetch one:

```bash
./scripts/run-bench.sh \
    --dataset-url "$VECTORBENCH_DATASETS_BUCKET/my-corpus.fvecs.gz" \
    -n 100000 --checkpoint
```

The service account backing `herddb-gcs-credentials` needs
`roles/storage.objectViewer` on `gs://herddb-datasets` in addition
to `roles/storage.objectAdmin` on the pages bucket.

### Resetting between runs

`scripts/reset-cluster.sh` scales every HerdDB StatefulSet except
the tools pod down to zero, deletes their PVCs, empties the
file-server pages bucket (**never** touching `gs://herddb-datasets`),
and scales back up. The tools pod and its dataset cache PVC are
preserved so downloaded datasets don't need to be re-fetched.

```bash
./scripts/reset-cluster.sh --yes
```

The script reads the pages bucket name from `helm get values herddb`
and refuses to run if the resolved name looks like a datasets
bucket.

### Diagnostics

`scripts/diagnostics.sh` collects either a JVM heap dump (default)
or async-profiler flamegraphs (`--profile`) from a running pod. See
the script's `--help` header for details. `scripts/collect-logs.sh`
dumps per-pod logs into a timestamped directory, and
`scripts/open-issue.sh` publishes them as a GitHub issue via `gh`.

## Teardown

```bash
helm uninstall herddb
```

To also remove persistent volumes:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=herddb
```

The GCS bucket and its contents are **not** deleted by Helm.
