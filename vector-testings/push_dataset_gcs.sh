#!/bin/bash
# Licensed to Diennea S.r.l. under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. Diennea S.r.l. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Pushes a generated dataset directory to a Google Cloud Storage bucket.
#
# Usage:
#   ./push_dataset_gcs.sh <dataset-dir> <gs-bucket-path>
#
# Examples:
#   ./push_dataset_gcs.sh ./datasets/generated gs://my-bucket/datasets/my-dataset
#   ./push_dataset_gcs.sh ./datasets/generated gs://my-bucket/datasets/  (uses dir name)
#
# Prerequisites:
#   - Google Cloud SDK installed (gcloud / gsutil)
#   - Authenticated: gcloud auth login
#   - Bucket exists and you have write access
#
set -e

if [ $# -lt 2 ]; then
    echo "Usage: $0 <dataset-dir> <gs-bucket-path>"
    echo ""
    echo "Examples:"
    echo "  $0 ./datasets/generated gs://my-bucket/datasets/my-dataset"
    echo "  $0 ./datasets/generated gs://my-bucket/datasets/"
    exit 1
fi

DATASET_DIR="$1"
GS_PATH="$2"

if [ ! -d "$DATASET_DIR" ]; then
    echo "ERROR: Dataset directory not found: $DATASET_DIR"
    exit 1
fi

# Check for descriptor file
DESCRIPTOR=$(find "$DATASET_DIR" -maxdepth 1 -name "*_descriptor.json" | head -1)
if [ -z "$DESCRIPTOR" ]; then
    echo "WARNING: No descriptor JSON found in $DATASET_DIR"
    echo "The dataset may not be auto-configurable by VectorBench."
fi

# Verify gsutil is available
if ! command -v gsutil &> /dev/null; then
    echo "ERROR: gsutil not found. Install Google Cloud SDK:"
    echo "  https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# If GS_PATH ends with /, append the directory name
if [[ "$GS_PATH" == */ ]]; then
    DIR_NAME=$(basename "$DATASET_DIR")
    GS_PATH="${GS_PATH}${DIR_NAME}"
fi

echo "Uploading dataset to $GS_PATH ..."
echo "Files:"
ls -lh "$DATASET_DIR"/*.fvecs "$DATASET_DIR"/*.ivecs "$DATASET_DIR"/*_descriptor.json 2>/dev/null || true
ls -lh "$DATASET_DIR"/*.csv "$DATASET_DIR"/*.zip 2>/dev/null || true
echo ""

# Upload all dataset files (parallel composite uploads for large files)
gsutil -m cp -r "$DATASET_DIR"/* "$GS_PATH/"

echo ""
echo "Upload complete."
echo "GCS path: $GS_PATH"
if [ -n "$DESCRIPTOR" ]; then
    DESCRIPTOR_NAME=$(basename "$DESCRIPTOR")
    echo "Descriptor: $GS_PATH/$DESCRIPTOR_NAME"
    echo ""
    echo "Use with VectorBench:"
    echo "  ./run.sh --dataset-url $GS_PATH/$DESCRIPTOR_NAME --dataset custom ..."
fi
