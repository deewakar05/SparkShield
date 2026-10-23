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
SCALAC=/opt/scala-2.12.18/bin/scalac
if [ -x "$SCALAC" ]; then
  SCALA_VER=$($SCALAC -version 2>&1)
  ok "scalac found: $SCALA_VER"
else
  if command -v scalac &>/dev/null; then
    ok "scalac on PATH: $(scalac -version 2>&1)"
  else
    fail "scalac not found at $SCALAC and not on PATH"
  fi
fi

# ── Spark (via PySpark install) ───────────────────────────────
echo ""
echo "── Spark ─────────────────────────────────────────────────"
SPARK_SUBMIT=/Users/deewakarkumar/spark-python/bin/spark-submit
PYSPARK_JARS=/Users/deewakarkumar/spark-python/lib/python3.10/site-packages/pyspark
DERIVED_SPARK_HOME="$PYSPARK_JARS"

if [ -x "$SPARK_SUBMIT" ]; then
  ok "spark-submit found: $SPARK_SUBMIT"
else
  fail "spark-submit not found at $SPARK_SUBMIT"
fi

if [ -d "$PYSPARK_JARS/jars" ]; then
  CORE_JAR=$(ls "$PYSPARK_JARS/jars/spark-core_2.12-"*.jar 2>/dev/null | head -1)
  if [ -n "$CORE_JAR" ]; then
    ok "Spark jars found: $(basename "$CORE_JAR")"
  else
    fail "No spark-core jar found in $PYSPARK_JARS/jars"
  fi
else
  fail "PySpark jars directory not found: $PYSPARK_JARS/jars"
fi

# Test spark-submit --version
if SPARK_LOCAL_IP=127.0.0.1 "$SPARK_SUBMIT" --version 2>&1 | grep -q "3\.5\.9"; then
  ok "Spark version confirmed: 3.5.9"
else
  warn "Could not confirm Spark 3.5.9 from spark-submit --version"
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

# ── Project build files ───────────────────────────────────────
echo ""
echo "── Project Build Files ────────────────────────────────────"
PROJECT_ROOT="$(cd "$(dirname "$0")"; pwd)/.."
BUILD_FILES=(
  "scala/build.sbt"
  "scala/project/build.properties"
  "scala/project/plugins.sbt"
  "scripts/sbt-launch.jar"
  "scripts/run.sh"
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
