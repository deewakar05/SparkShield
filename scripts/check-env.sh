#!/usr/bin/env bash
# =============================================================
# SparkShield — Environment Validation Script
# Checks all required tools and paths before building.
# Usage: bash scripts/check-env.sh
# =============================================================

set -euo pipefail

PASS=0; WARN=0; FAIL=0

ok()   { echo "  [OK]   $*"; ((PASS+=1)); }
warn() { echo "  [WARN] $*"; ((WARN+=1)); }
fail() { echo "  [FAIL] $*"; ((FAIL+=1)); }

echo ""
echo "============================================================"
echo "  SparkShield — Environment Validation"
echo "  $(date)"
echo "============================================================"

# ── Java ──────────────────────────────────────────────────────
echo ""
echo "── Java ──────────────────────────────────────────────────"
if java -version 2>&1 | grep -q "1\.8\|11\|17\|21"; then
  JAVA_VER=$(java -version 2>&1 | head -1)
  ok "Java found: $JAVA_VER"
  if java -version 2>&1 | grep -q "1\.8"; then
    ok "Java 8 — compatible with Spark 3.5.9 + sbt 1.x"
  else
    warn "Java version is not 8; sbt 1.x prefers Java 8/11. Proceed with care."
  fi
else
  fail "Java not found. Install Zulu JDK 8."
fi

# ── Scala ─────────────────────────────────────────────────────
echo ""
echo "── Scala ─────────────────────────────────────────────────"
SCALAC_BIN=""
if [ -n "${SCALA_HOME:-}" ] && [ -x "$SCALA_HOME/bin/scalac" ]; then
  SCALAC_BIN="$SCALA_HOME/bin/scalac"
elif command -v scalac &>/dev/null; then
  SCALAC_BIN="$(command -v scalac)"
elif [ -x "/opt/scala-2.12.18/bin/scalac" ]; then
  SCALAC_BIN="/opt/scala-2.12.18/bin/scalac"
fi

if [ -n "$SCALAC_BIN" ]; then
  SCALA_VER=$($SCALAC_BIN -version 2>&1)
  ok "scalac found ($SCALAC_BIN): $SCALA_VER"
else
  warn "Standalone scalac not found on PATH (Optional: sbt manages Scala 2.12.18 compilation automatically)"
fi

# ── Spark ─────────────────────────────────────────────────────
echo ""
echo "── Spark ─────────────────────────────────────────────────"
SPARK_SUBMIT_BIN=""
if [ -n "${SPARK_SUBMIT:-}" ] && [ -x "$SPARK_SUBMIT" ]; then
  SPARK_SUBMIT_BIN="$SPARK_SUBMIT"
elif [ -n "${SPARK_HOME:-}" ] && [ -x "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_SUBMIT_BIN="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit &>/dev/null; then
  SPARK_SUBMIT_BIN="$(command -v spark-submit)"
elif [ -x "$HOME/spark-python/bin/spark-submit" ]; then
  SPARK_SUBMIT_BIN="$HOME/spark-python/bin/spark-submit"
fi

if [ -n "$SPARK_SUBMIT_BIN" ]; then
  ok "spark-submit found: $SPARK_SUBMIT_BIN"
else
  fail "spark-submit not found. Set SPARK_SUBMIT or SPARK_HOME, or install Spark 3.5.9."
fi

SPARK_JARS_DIR=""
if [ -n "${SPARK_HOME:-}" ] && [ -d "$SPARK_HOME/jars" ]; then
  SPARK_JARS_DIR="$SPARK_HOME/jars"
elif [ -n "${PYSPARK_JARS:-}" ] && [ -d "$PYSPARK_JARS/jars" ]; then
  SPARK_JARS_DIR="$PYSPARK_JARS/jars"
elif [ -d "$HOME/spark-python/lib/python3.10/site-packages/pyspark/jars" ]; then
  SPARK_JARS_DIR="$HOME/spark-python/lib/python3.10/site-packages/pyspark/jars"
elif [ -n "$SPARK_SUBMIT_BIN" ]; then
  CANDIDATE_DIR="$(cd "$(dirname "$SPARK_SUBMIT_BIN")/.." 2>/dev/null && pwd)/jars"
  if [ -d "$CANDIDATE_DIR" ]; then
    SPARK_JARS_DIR="$CANDIDATE_DIR"
  fi
fi

if [ -n "$SPARK_JARS_DIR" ] && [ -d "$SPARK_JARS_DIR" ]; then
  CORE_JAR=$(ls "$SPARK_JARS_DIR/spark-core_2.12-"*.jar 2>/dev/null | head -1)
  if [ -n "$CORE_JAR" ]; then
    ok "Spark jars directory: $SPARK_JARS_DIR ($(basename "$CORE_JAR"))"
  else
    warn "No spark-core_2.12 jar found in $SPARK_JARS_DIR"
  fi
else
  warn "Spark jars directory not found directly (spark-submit will locate them at runtime)"
fi

# Test spark-submit --version
if [ -n "$SPARK_SUBMIT_BIN" ]; then
  if SPARK_LOCAL_IP=127.0.0.1 "$SPARK_SUBMIT_BIN" --version 2>&1 | grep -q "3\.5\."; then
    SPARK_VER_LINE=$(SPARK_LOCAL_IP=127.0.0.1 "$SPARK_SUBMIT_BIN" --version 2>&1 | grep "version 3\.5" | head -1 || echo "Spark 3.5.x")
    ok "Spark version confirmed: $SPARK_VER_LINE"
  else
    warn "Could not confirm Spark 3.5.x from spark-submit --version"
  fi
fi

# ── sbt ───────────────────────────────────────────────────────
echo ""
echo "── sbt ────────────────────────────────────────────────────"
SBT_LAUNCHER="$(cd "$(dirname "$0")"; pwd)/sbt-launch.jar"
if [ -f "$SBT_LAUNCHER" ]; then
  ok "sbt launcher jar found: $SBT_LAUNCHER"
else
  fail "sbt launcher jar not found at $SBT_LAUNCHER — run: curl -L https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch/1.10.7/sbt-launch-1.10.7.jar -o scripts/sbt-launch.jar"
fi

SBT_PROPS="$(cd "$(dirname "$0")"; pwd)/../scala/project/build.properties"
if [ -f "$SBT_PROPS" ]; then
  SBT_VER=$(grep sbt.version "$SBT_PROPS" | cut -d= -f2)
  ok "build.properties sbt.version=$SBT_VER"
  if echo "$SBT_VER" | grep -q "^1\."; then
    ok "sbt 1.x — JDK 8 compatible"
  else
    fail "sbt.version is not 1.x — sbt 2.x requires JDK 17. Edit scala/project/build.properties."
  fi
else
  fail "scala/project/build.properties not found"
fi

# ── Docker ────────────────────────────────────────────────────
echo ""
echo "── Docker ─────────────────────────────────────────────────"
if command -v docker &>/dev/null; then
  ok "Docker found: $(docker --version)"
  if docker compose version &>/dev/null; then
    ok "Docker Compose: $(docker compose version)"
  else
    warn "docker compose (v2) not found; Kafka stack may not start"
  fi
else
  warn "Docker not found — Kafka streaming demo will not run"
fi

# Check protected containers
PROTECTED=("autoheal-web" "autoheal-ansible" "petfood_flask" "petfood_springboot" "petfood_postgres")
echo "  Checking protected containers are still running..."
for NAME in "${PROTECTED[@]}"; do
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${NAME}$"; then
    ok "Protected container running: $NAME"
  else
    STATUS=$(docker ps -a --format '{{.Names}}\t{{.Status}}' 2>/dev/null | grep "^${NAME}" | awk '{print $2}' || echo "not found")
    warn "Protected container not running: $NAME (status: $STATUS)"
  fi
done

# ── Dataset ───────────────────────────────────────────────────
echo ""
echo "── Dataset ────────────────────────────────────────────────"
DATASET_DIR="${SPARKSHIELD_DATASET_DIR:-$HOME/Downloads/MachineLearningCVE}"
if [ -d "$DATASET_DIR" ]; then
  ok "Dataset directory: $DATASET_DIR"
  CSV_COUNT=$(ls "$DATASET_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
  if [ "$CSV_COUNT" -gt 0 ]; then
    ok "Found $CSV_COUNT CSV file(s) in dataset directory"
    ls "$DATASET_DIR"/*.csv 2>/dev/null | while read f; do
      SIZE=$(du -sh "$f" 2>/dev/null | cut -f1)
      echo "    $SIZE  $(basename "$f")"
    done
  else
    warn "No CSV files found in $DATASET_DIR"
  fi
else
  fail "Dataset directory not found: $DATASET_DIR"
  fail "Set SPARKSHIELD_DATASET_DIR env var to point to CICIDS2017 CSVs"
fi

# ── Python (Auxiliary: Dashboard & Validation) ────────────────
echo ""
echo "── Python ────────────────────────────────────────────────"
PYTHON_BIN="${PYTHON:-python3}"
if command -v "$PYTHON_BIN" &>/dev/null; then
  PY_VER=$("$PYTHON_BIN" --version 2>&1)
  ok "Python found ($PYTHON_BIN): $PY_VER"
else
  warn "Python 3 not found on PATH. Required for web dashboard (scripts/dashboard.sh)."
fi

# ── Project build files ───────────────────────────────────────
echo ""
echo "── Project Build Files ────────────────────────────────────"
PROJECT_ROOT="$(cd "$(dirname "$0")"; pwd)/.."
BUILD_FILES=(
  "scala/build.sbt"
  "scala/project/build.properties"
  "scala/project/plugins.sbt"
  "scripts/sbt-launch.jar"
  "scripts/sbt.sh"
  "scripts/run.sh"
  "scripts/dashboard.sh"
  "scripts/streaming-pipeline.sh"
)
for f in "${BUILD_FILES[@]}"; do
  if [ -f "$PROJECT_ROOT/$f" ]; then
    ok "$f"
  else
    fail "Missing: $f"
  fi
done

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  Results: $PASS passed, $WARN warnings, $FAIL failures"
echo "============================================================"

if [ "$FAIL" -gt 0 ]; then
  echo "  ACTION REQUIRED: Fix failures above before building."
  exit 1
elif [ "$WARN" -gt 0 ]; then
  echo "  Warnings present — review before running full pipeline."
  exit 0
else
  echo "  All checks passed. Ready to build."
  exit 0
fi
