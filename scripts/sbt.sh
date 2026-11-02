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

# Resolve Java executable
JAVA_CMD=""
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA_CMD="$JAVA_HOME/bin/java"
elif [ -d "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home" ]; then
  JAVA_CMD="/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home/bin/java"
elif command -v java &>/dev/null; then
  JAVA_CMD="$(command -v java)"
fi

if [ -z "$JAVA_CMD" ] || [ ! -x "$JAVA_CMD" ]; then
  echo "ERROR: Java not found. Please install Zulu OpenJDK 8 and set JAVA_HOME." >&2
  exit 1
fi

if [ ! -f "$SBT_LAUNCH" ]; then
  echo "INFO: sbt-launch.jar not found at $SBT_LAUNCH. Attempting bootstrap download..."
  mkdir -p "$SCRIPT_DIR"
  if command -v curl &>/dev/null; then
    curl -fSL "https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar" -o "$SBT_LAUNCH" || {
      echo "ERROR: Failed to download sbt-launch.jar via curl." >&2
      exit 1
    }
  else
    echo "ERROR: curl not found. Please manually download:" >&2
    echo "  curl -L https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar -o scripts/sbt-launch.jar" >&2
    exit 1
  fi
fi

exec "$JAVA_CMD" \
  -Xmx2g -Xms512m \
  -XX:+UseG1GC \
  -jar "$SBT_LAUNCH" \
  "$@"
