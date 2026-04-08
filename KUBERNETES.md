# HerdDB on Kubernetes

## Building Docker images

### Local build

Build the Docker image locally using the `docker` Maven profile:

```bash
mvn clean install -DskipTests -Pdocker
```

This creates a local Docker image named `herddb/herddb-server:0.30.0-SNAPSHOT`.

### Push to Docker Hub

To build and push directly to a Docker Hub repository, use the Jib `build`
goal with the `image.server.image.name` property:

```bash
mvn clean install -DskipTests -pl herddb-docker -am jib:build@build \
  -Dimage.server.image.name=eolivelli/herddb:0.30.0-SNAPSHOT
```

Replace `eolivelli/herddb` with your Docker Hub repository and adjust the tag
as needed.

**Authentication:** Jib uses credentials from `docker login`. Run
`docker login` before pushing, or configure a
[Jib credential helper](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin#authentication-methods).

### Custom image name

The image name is controlled by the Maven property `image.server.image.name`
(default: `herddb/herddb-server:<version>`). You can override it on any build:

```bash
# Local build with a custom name
mvn clean install -DskipTests -Pdocker \
  -Dimage.server.image.name=myregistry/herddb:latest

# Push to a private registry
mvn clean install -DskipTests -pl herddb-docker -am jib:build@build \
  -Dimage.server.image.name=us-docker.pkg.dev/my-project/herddb/herddb-server:latest
```

## Helm chart

The Helm chart is located at:

```
herddb-kubernetes/src/main/helm/herddb/
```

Install with default values:

```bash
helm install herddb herddb-kubernetes/src/main/helm/herddb/
```

To use a custom Docker image, override `image.repository` and `image.tag`:

```bash
helm install herddb herddb-kubernetes/src/main/helm/herddb/ \
  --set image.repository=eolivelli/herddb \
  --set image.tag=0.30.0-SNAPSHOT
```

See [`values.yaml`](herddb-kubernetes/src/main/helm/herddb/values.yaml) for
all configurable options.

## Deployment examples

### Local development with k3s

Full distributed stack on a local k3s cluster (server, file server, MinIO,
ZooKeeper, BookKeeper, indexing service, tools).

See [`examples/k3s-local/README.md`](herddb-kubernetes/src/main/helm/herddb/examples/k3s-local/README.md)

Quick start:

```bash
cd herddb-kubernetes/src/main/helm/herddb/
helm install herddb . -f examples/k3s-local/values.yaml
```

### Google Kubernetes Engine (GKE)

Production deployment using Google Cloud Storage as the object store via its
S3-compatible endpoint. MinIO is not needed.

See [`examples/gke/README.md`](herddb-kubernetes/src/main/helm/herddb/examples/gke/README.md)

Quick start:

```bash
cd herddb-kubernetes/src/main/helm/herddb/
helm install herddb . -f examples/gke/values.yaml \
  --set fileServer.s3.bucket=my-herddb-pages
```
