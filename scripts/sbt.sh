#!/usr/bin/env bash
# =============================================================
# SparkShield — sbt wrapper (uses local sbt-launch.jar)
# Ensures JDK 8 compatibility by using sbt 1.x launcher.
#
# Usage from scala/ directory:
#   ../scripts/sbt.sh compile
#   ../scripts/sbt.sh test
#   ../scripts/sbt.sh assembly
# Or directly from project root:
#   scripts/sbt.sh -C scala compile
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SBT_LAUNCH="$SCRIPT_DIR/sbt-launch.jar"
JAVA_HOME="${JAVA_HOME:-/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home}"
JAVA_CMD="$JAVA_HOME/bin/java"

if [ ! -f "$SBT_LAUNCH" ]; then
  echo "ERROR: sbt-launch.jar not found at $SBT_LAUNCH"
  echo "  Download it with:"
  echo "    curl -L https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar \\"
  echo "         -o scripts/sbt-launch.jar"
  exit 1
fi

if [ ! -x "$JAVA_CMD" ]; then
  echo "WARN: $JAVA_CMD not found, falling back to 'java' on PATH"
  JAVA_CMD=java
fi

exec "$JAVA_CMD" \
  -Xmx2g -Xms512m \
  -XX:+UseG1GC \
  -jar "$SBT_LAUNCH" \
  "$@"
