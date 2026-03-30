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
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Build if jar doesn't exist
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
LOG_FILE="$SCRIPT_DIR/vectorbench.log"
PID_FILE="$SCRIPT_DIR/vectorbench.pid"

# Check for --background flag
BACKGROUND=false
ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--background" ]; then
        BACKGROUND=true
    else
        ARGS+=("$arg")
    fi
done

if [ "$BACKGROUND" = true ]; then
    echo "Starting VectorBench in background..."
    echo "Log file: $LOG_FILE"
    echo "PID file: $PID_FILE"
    nohup java $JAVA_HEAP -jar "$JAR" "${ARGS[@]}" > "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "PID: $(cat "$PID_FILE")"
    echo "Use $SCRIPT_DIR/stop.sh to stop the benchmark."
    echo "Use 'tail -f $LOG_FILE' to follow progress."
else
    echo "Logging output to $LOG_FILE"
    java $JAVA_HEAP -jar "$JAR" "${ARGS[@]}" 2>&1 | tee "$LOG_FILE"
fi
