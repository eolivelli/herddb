# HerdDB on Kind (local development)

Deploy HerdDB with the full distributed stack on a local
[Kind](https://kind.sigs.k8s.io/) cluster.

## Architecture

| Component        | Replicas | Purpose                                         |
|------------------|----------|-------------------------------------------------|
| HerdDB server    | 1        | JDBC endpoint, metadata (ZooKeeper), commit log (BookKeeper) |
| File server      | 1        | gRPC page storage, backed by MinIO (S3)         |
| MinIO            | 1        | S3-compatible object store for data and indexes  |
| ZooKeeper        | 1        | Cluster coordination and metadata storage        |
| BookKeeper       | 1        | Distributed commit log                           |
| Indexing service | 1        | Vector indexing service                          |
| Tools pod        | 1        | Pre-configured CLI for interactive queries       |

## Prerequisites

- [Kind](https://kind.sigs.k8s.io/) installed
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured
- [Helm 3](https://helm.sh/docs/intro/install/)
- Docker image available in the cluster (see below)

## Quick start

### 1. Create a Kind cluster

```bash
kind create cluster --name herddb
```

### 2. Load the HerdDB Docker image

If you built the image locally:

```bash
kind load docker-image herddb/herddb-server:0.30.0-SNAPSHOT --name herddb
```

### 3. Install the Helm chart

From the `herddb-kubernetes/src/main/helm/herddb/` directory:

```bash
helm install herddb . -f examples/kind-local/values.yaml
```

### 4. Wait for all pods to become ready

```bash
kubectl get pods -w
```

You should see 7 pods (server, file-server, minio, zookeeper, bookkeeper,
indexing-service, and tools) all reach `Running` / `Ready` status.

### 5. Connect with the CLI

```bash
kubectl exec -it deploy/herddb-tools -- herddb-cli
```

Run a test query:

```sql
SELECT * FROM SYSTABLES;
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

## Teardown

```bash
helm uninstall herddb
kind delete cluster --name herddb
```

> **Note:** PersistentVolumeClaims are not deleted by `helm uninstall`.
> Deleting the Kind cluster removes everything.
