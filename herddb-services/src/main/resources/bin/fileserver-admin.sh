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

set -e

### BASE_DIR discovery
if [ -z "$BASE_DIR" ]; then
  BASE_DIR="`dirname \"$0\"`"
  BASE_DIR="`( cd \"$BASE_DIR\" && pwd )`"
fi
BASE_DIR=$BASE_DIR/..
BASE_DIR="`( cd \"$BASE_DIR\" && pwd )`"

. $BASE_DIR/bin/setenv.sh

JAVA="$JAVA_HOME/bin/java"
JAR=$(ls "$BASE_DIR"/fileserver-admin/herddb-fileserver-admin-*.jar 2>/dev/null | head -1)

if [ -z "$JAR" ]; then
    echo "ERROR: herddb-fileserver-admin jar not found under $BASE_DIR/fileserver-admin/" >&2
    exit 1
fi

JAVA_HEAP="${FILESERVER_ADMIN_HEAP:--Xms64m -Xmx256m}"

# When HERDDB_FILESERVER_SERVER is set (typically injected by the Helm chart
# inside the k3s/GKE tools pod), auto-wire --server for sub-commands that
# accept it unless the user already passed it.
EXTRA_ARGS=()
if [ -n "$HERDDB_FILESERVER_SERVER" ] && [ -n "$1" ]; then
    HAS_SERVER=false
    for arg in "$@"; do
        if [ "$arg" = "--server" ]; then
            HAS_SERVER=true
            break
        fi
    done
    if [ "$HAS_SERVER" = false ]; then
        EXTRA_ARGS+=("--server" "$HERDDB_FILESERVER_SERVER")
    fi
fi

exec $JAVA $JAVA_HEAP -jar "$JAR" "$@" "${EXTRA_ARGS[@]}"
