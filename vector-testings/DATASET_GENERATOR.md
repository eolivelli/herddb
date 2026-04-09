<!--
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Dataset Generator

A tool for generating synthetic vector datasets using Ollama embeddings.
It constructs random English sentences from bundled word lists and embeds them
via the Ollama API, producing SIFT-compatible files (FVECS/IVECS) that can be
used directly with VectorBench.

## Prerequisites

### Install Ollama

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

### Pull an Embedding Model

The default model is `all-minilm` (384 dimensions, fast):

```bash
ollama pull all-minilm
```

Other supported models (any Ollama embedding model works):

```bash
ollama pull nomic-embed-text    # 768 dimensions
ollama pull mxbai-embed-large   # 1024 dimensions
```

### Start Ollama

Ollama typically runs as a background service after installation. Verify it is running:

```bash
ollama list
```

If not running, start it:

```bash
ollama serve
```

By default Ollama listens on `http://localhost:11434`.

## Building

```bash
mvn -f vector-testings/pom.xml package -DskipTests
```

Or let the run script build automatically:

```bash
cd vector-testings
./run_generate.sh --help
```

## Usage

```bash
./run_generate.sh --total <N> [options]
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--total` | (required) | Total number of sentences/vectors to generate |
| `--name` | `generated` | Dataset name (used in file prefix and descriptor) |
| `--output-dir` | `./datasets/generated` | Output directory |
| `--model` | `all-minilm` | Ollama embedding model name |
| `--ollama-url` | `http://localhost:11434` | Ollama server URL |
| `--num-queries` | `1000` | First N sentences used as query vectors |
| `--ground-truth-k` | `100` | K nearest neighbors per query in ground truth |
| `--csv` | off | Also generate CSV with id, sentence, vector |
| `--zip` | off | Compress output into a ZIP archive and remove individual files |
| `--batch-size` | `100` | Sentences per Ollama API call |
| `--similarity` | `euclidean` | Distance function: `euclidean` or `cosine` |

### Examples

**Minimal — generate 10,000 vectors:**

```bash
./run_generate.sh --total 10000 --name my-dataset
```

**With CSV output for debugging:**

```bash
./run_generate.sh --total 5000 --csv
```

**Full options with ZIP and custom model:**

```bash
./run_generate.sh \
    --total 100000 \
    --name cosine-100k \
    --model nomic-embed-text \
    --num-queries 500 \
    --ground-truth-k 50 \
    --batch-size 200 \
    --similarity cosine \
    --csv --zip \
    --output-dir ./datasets/cosine-100k
```

**Using a remote Ollama instance:**

```bash
./run_generate.sh --total 50000 --ollama-url http://gpu-server:11434
```

## Output Files

All files are written to the `--output-dir` directory, prefixed with `--name`
(default: `generated`):

| File | Format | Description |
|------|--------|-------------|
| `{name}_descriptor.json` | JSON | Dataset metadata (dimensions, similarity, file names, etc.) |
| `{name}_base.fvecs` | FVECS | All base vectors. IDs are implicit (position 0, 1, 2, ...) |
| `{name}_query.fvecs` | FVECS | First N query vectors (also included in base) |
| `{name}_groundtruth.ivecs` | IVECS | K nearest neighbor IDs per query, sorted nearest-first |
| `{name}_sentences.csv` | CSV | (if `--csv`) Columns: id, sentence, vector |
| `{name}_dataset.zip` | ZIP | (if `--zip`) All above files compressed; individual files are removed |

### Dataset Descriptor (JSON)

The descriptor file contains all metadata needed to auto-configure VectorBench:

```json
{
  "name": "my-dataset",
  "format": "fvecs",
  "dimensions": 384,
  "similarity": "euclidean",
  "totalVectors": 10000,
  "numQueries": 1000,
  "groundTruthK": 100,
  "embeddingModel": "all-minilm",
  "baseFile": "my-dataset_base.fvecs",
  "queryFile": "my-dataset_query.fvecs",
  "groundTruthFile": "my-dataset_groundtruth.ivecs",
  "createdAt": "2026-04-09T10:00:00Z"
}
```

### SIFT Binary Format

**FVECS** (float vectors): each record is `[4-byte LE int: dim][dim * 4-byte LE float: values]`

**IVECS** (integer vectors / ground truth): each record is `[4-byte LE int: count][count * 4-byte LE int: neighbor IDs]`

All integers and floats are little-endian. This is the standard format used by
SIFT1M, GIST1M, and other ANN benchmark datasets.

## Pushing Datasets to Google Cloud Storage

Use the provided script to upload a dataset to GCS:

```bash
# Prerequisites: gcloud CLI installed and authenticated
gcloud auth login

# Push a ZIP dataset (recommended)
./push_dataset_gcs.sh ./datasets/my-dataset gs://my-bucket/datasets/my-dataset

# Or push individual files
./push_dataset_gcs.sh ./datasets/my-dataset gs://my-bucket/datasets/my-dataset
```

When using `--zip`, the output directory contains only the ZIP file, which is
the single artifact to push. The bucket must be publicly readable (or accessible
to the VectorBench runner) for the pull to work.

## Using Generated Datasets with VectorBench

### Local datasets

Point VectorBench at the generated dataset using `--dataset custom`:

```bash
./run.sh --dataset custom \
    --dataset-dir ./datasets \
    --dataset-url file:///absolute/path/to/my-dataset/my-dataset_descriptor.json \
    --rows 10000
```

When using `--dataset custom`, VectorBench loads the descriptor and
auto-configures:
- **Similarity function** from the descriptor (no need for `--similarity`)
- **Row count** from `totalVectors` (if you didn't override `--rows`)

### Remote datasets (Google Cloud Storage)

Pull a dataset ZIP from GCS and run the benchmark:

```bash
./run.sh --dataset custom \
    --dataset-url gs://my-bucket/datasets/my-dataset/my-dataset_dataset.zip
```

VectorBench will:
1. Download the ZIP file
2. Extract all files (descriptor, base vectors, query vectors, ground truth)
3. Auto-configure similarity and row count from the descriptor
4. Run the benchmark

You can also point to the descriptor JSON directly (for non-ZIP datasets):

```bash
./run.sh --dataset custom \
    --dataset-url gs://my-bucket/datasets/my-dataset/my-dataset_descriptor.json
```

In this case, VectorBench downloads the descriptor first, then each data file
listed in it from the same URL path.

GCS URLs (`gs://...`) are downloaded using `gsutil`, which inherits your
`gcloud auth` credentials and supports private buckets. Make sure gcloud is
installed and authenticated:

```bash
gcloud auth login
```

## How It Works

1. **Sentence Construction**: Random sentences are built from 4 word lists
   (nouns, verbs, objects, time adverbs) bundled as classpath resources.
   Pattern: `"The {noun} {verb} a {object} {adverb}"`

2. **Embedding**: Sentences are sent in batches to Ollama's `/api/embed`
   endpoint. The embedding dimension is auto-detected at startup.

3. **Ground Truth**: The first N sentences become query vectors. As each base
   vector is generated, its distance to all query vectors is computed. A bounded
   max-heap per query maintains the K nearest neighbors. At the end, heaps are
   drained to produce the IVECS ground truth file.

## Performance Notes

- **Ollama throughput**: With `all-minilm` on GPU, expect ~1000+ embeddings/sec.
  On CPU, expect ~20-100 embeddings/sec depending on hardware and batch size.
- **Ground truth computation**: O(total * numQueries * dim) distance operations.
  For 100K vectors with 1000 queries, this takes a few seconds.
- **Memory**: Only query vectors (~numQueries * dim * 4 bytes) are kept in
  memory. All other vectors stream to disk.
