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
JAR=$(ls "$BASE_DIR"/vector-testings/vector-testings-*.jar 2>/dev/null | head -1)

if [ -z "$JAR" ]; then
    echo "ERROR: vector-testings jar not found under $BASE_DIR/vector-testings/" >&2
    exit 1
fi

JAVA_HEAP="${VECTORBENCH_HEAP:--Xms1g -Xmx2g}"

exec $JAVA $JAVA_HEAP -jar "$JAR" "$@"
