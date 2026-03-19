#!/bin/bash
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

exec java -jar "$JAR" "$@"
