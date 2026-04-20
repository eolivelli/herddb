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
# Prepares BIGANN or GIST1M for VectorBench "custom" mode: stages the native
# files with a generated descriptor JSON and (optionally) pushes everything to
# a Google Cloud Storage path using push_dataset_gcs.sh.
#
# Usage examples:
#   ./run_publish_standard_datasets.sh --dataset gist1m
#   ./run_publish_standard_datasets.sh --dataset bigann --gs-path gs://herddb-datasets/bigann/
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

JAR=$(ls "$SCRIPT_DIR"/target/vector-testings-*.jar 2>/dev/null | grep -v original | head -1)
if [ -z "$JAR" ]; then
    echo "Building vector-testings..."
    mvn -f "$SCRIPT_DIR/pom.xml" package -DskipTests -q
    JAR=$(ls "$SCRIPT_DIR"/target/vector-testings-*.jar 2>/dev/null | grep -v original | head -1)
fi

if [ -z "$JAR" ]; then
    echo "ERROR: Could not find uber-jar. Run: mvn -f $SCRIPT_DIR/pom.xml package -DskipTests"
    exit 1
fi

JAVA_HEAP="${VECTORBENCH_HEAP:--Xms1g -Xmx2g}"

cd "$SCRIPT_DIR"
java $JAVA_HEAP -cp "$JAR" herddb.vectortesting.StandardDatasetPublisher "$@"
