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
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="$SCRIPT_DIR/vectorbench.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "No PID file found at $PID_FILE — is VectorBench running?"
    exit 1
fi

PID=$(cat "$PID_FILE")

if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping VectorBench (PID $PID)..."
    kill "$PID"
    # Wait up to 10 seconds for graceful shutdown
    for i in $(seq 1 10); do
        if ! kill -0 "$PID" 2>/dev/null; then
            echo "VectorBench stopped."
            rm -f "$PID_FILE"
            exit 0
        fi
        sleep 1
    done
    echo "Process did not stop gracefully, sending SIGKILL..."
    kill -9 "$PID" 2>/dev/null
    rm -f "$PID_FILE"
    echo "VectorBench killed."
else
    echo "Process $PID is not running. Cleaning up stale PID file."
    rm -f "$PID_FILE"
fi
