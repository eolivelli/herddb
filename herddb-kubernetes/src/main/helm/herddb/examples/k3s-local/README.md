# HerdDB on k3s (local development, in Docker)

Deploy HerdDB with the full distributed stack on a local
[k3s](https://k3s.io/) cluster that runs **inside a Docker container**.
Nothing is installed on the host: no `sudo`, no `/etc/rancher`, no
system service. Tearing down removes the container and all state.

## Architecture

| Component        | Replicas | Purpose                                         |
|------------------|----------|-------------------------------------------------|
| HerdDB server    | 1        | JDBC endpoint, metadata (ZooKeeper), commit log (BookKeeper), remote page storage |
| File server      | 1        | gRPC page storage, backed by MinIO (S3)         |
| MinIO            | 1        | S3-compatible object store for data and indexes  |
| ZooKeeper        | 1        | Cluster coordination and metadata storage        |
| BookKeeper       | 1        | Distributed commit log                           |
| Indexing service | 1        | Vector indexing service                          |
| Tools pod        | 1        | Pre-configured CLI and `vector-bench.sh`         |
| Prometheus       | 1        | Metrics scraping                                 |
| Grafana          | 1        | Dashboards                                       |

The values mirror `examples/gke/values.yaml` (cluster mode, remote
storage, indexing service enabled) but with resources and PVCs sized for
a laptop — no PVC exceeds 10 GiB.

## Prerequisites

- Docker (to run the k3s container and build the HerdDB image)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm 3](https://helm.sh/docs/intro/install/)

## Quick start

Run the install script from this directory:

```bash
./install.sh          # start k3s container, import image, install chart
./install.sh --build  # also rebuild Docker images with Maven first
```

The script:

1. (optional) runs `mvn clean install -DskipTests -Pdocker` at the repo root
2. starts a privileged `rancher/k3s` container named `herddb-k3s` with
   port 6443 published on localhost
3. extracts the kubeconfig to `./.kubeconfig` (gitignored) — the other
   scripts pick it up automatically
4. `docker save`s the HerdDB image and imports it into the k3s
   container's containerd via `ctr images import`
5. `helm install`s the chart against `values.yaml`
6. waits for all pods to become Ready

All subsequent `kubectl` / `helm` commands in this directory need
`KUBECONFIG=./.kubeconfig`. The helper scripts under `scripts/` set this
automatically.

Connect with the CLI:

```bash
export KUBECONFIG=$PWD/.kubeconfig
kubectl exec -it sts/herddb-tools -- herddb-cli
```

Run a test query:

```sql
SELECT * FROM SYSTABLES;
```

## Helper scripts

The `scripts/` directory contains small, independently-callable shell
scripts intended to be driven either by a human or by the
`herddb-k3s-bench` Claude subagent:

| Script | Purpose |
|--------|---------|
| `scripts/check-cluster.sh` | Print pod status, exit non-zero if any HerdDB pod is not Ready. |
| `scripts/run-bench.sh <args>` | Forward args to `vector-bench.sh` inside the tools pod, tee output to `reports/run-<ts>.log`. |
| `scripts/collect-logs.sh` | Dump `kubectl logs` + describe + events into `reports/logs-<ts>/`. |
| `scripts/write-report.sh <run-log>` | Turn a run log into a markdown report with metrics + cluster state. |
| `scripts/open-issue.sh --title … --body-file … --logs-dir …` | Open a GH issue with logs attached as collapsible blocks. `--dry-run` skips the API call. |

Example end-to-end benchmark run:

```bash
./install.sh --build
./scripts/check-cluster.sh
./scripts/run-bench.sh --dataset sift10k -n 10000 -k 100 --checkpoint
./scripts/write-report.sh reports/run-*.log
```

## Vector search example

HerdDB supports vector search with approximate nearest neighbor (ANN) indexes.
Here is a quick walkthrough using the CLI's Groovy scripting support.

### Create a table with a vector column and an index

```bash
kubectl exec sts/herddb-tools -- herddb-cli \
  -q "CREATE TABLE documents (id int primary key, content string, embedding floata not null)"

kubectl exec sts/herddb-tools -- herddb-cli \
  -q "CREATE VECTOR INDEX vidx ON documents(embedding)"
```

### Insert and search via a Groovy script

Vector data must be inserted through JDBC prepared statements. The CLI's
`-g` flag runs a Groovy script with a pre-configured `connection` object.

Create a file called `vector_demo.groovy` on the tools pod:

```bash
kubectl exec sts/herddb-tools -- bash -c 'cat > /tmp/vector_demo.groovy << '\''SCRIPT'\''
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
kubectl exec sts/herddb-tools -- herddb-cli -g /tmp/vector_demo.groovy
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

## Teardown

Remove the Helm release, delete PVCs, and destroy the k3s container:

```bash
./teardown.sh
```

Keep the k3s container around (e.g., to re-install quickly):

```bash
./teardown.sh --keep-container
```

Manual teardown:

```bash
export KUBECONFIG=$PWD/.kubeconfig
helm uninstall herddb
kubectl delete pvc -l app.kubernetes.io/instance=herddb
docker rm -f herddb-k3s
rm -f .kubeconfig
```
