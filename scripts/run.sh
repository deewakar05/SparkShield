#!/usr/bin/env bash
# =============================================================
# SparkShield — spark-submit helper
#
# Usage:
#   ./scripts/run.sh <MainClass> [extra spark-submit args...]
#
# Examples:
#   ./scripts/run.sh sparkshield.RDDBasics
#   ./scripts/run.sh sparkshield.ModelTraining --conf spark.executor.memory=4g
#
# Environment variables (optional overrides):
#   SPARKSHIELD_DATASET_DIR   path to CICIDS2017 CSV directory
#   SPARK_SUBMIT              path to spark-submit binary
#   SPARK_LOCAL_IP            local IP for Spark (default: 127.0.0.1)
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SCALA_DIR="$PROJECT_ROOT/scala"

# ── Resolve spark-submit ──────────────────────────────────────
SPARK_SUBMIT="${SPARK_SUBMIT:-/Users/deewakarkumar/spark-python/bin/spark-submit}"
if [ ! -x "$SPARK_SUBMIT" ]; then
  echo "ERROR: spark-submit not found at '$SPARK_SUBMIT'."
  echo "  Set the SPARK_SUBMIT environment variable, e.g.:"
  echo "    export SPARK_SUBMIT=/path/to/spark-submit"
  exit 1
fi

# ── Resolve SPARK_LOCAL_IP to avoid loopback warnings ─────────
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

# ── Resolve SPARK_HOME so spark-class can find jars ──────────
PYSPARK_HOME=/Users/deewakarkumar/spark-python/lib/python3.10/site-packages/pyspark
if [ -d "$PYSPARK_HOME/jars" ]; then
  export SPARK_HOME="$PYSPARK_HOME"
else
  echo "ERROR: Cannot locate Spark jars directory."
  echo "  Expected: $PYSPARK_HOME/jars"
  exit 1
fi

# ── Require a main class argument ─────────────────────────────
if [ $# -lt 1 ]; then
  echo "Usage: $0 <MainClass> [spark-submit args...]"
  echo ""
  echo "Available main classes:"
  echo "  sparkshield.RDDBasics"
  echo "  sparkshield.RDDAnalytics"
  echo "  sparkshield.DataIngestion"
  echo "  sparkshield.DataCleaning"
  echo "  sparkshield.SparkSQLAnalytics"
  echo "  sparkshield.GraphAnalytics"
  echo "  sparkshield.KafkaProducer"
  echo "  sparkshield.KafkaStreamProcessor"
  echo "  sparkshield.ModelTraining"
  echo "  sparkshield.MetricsReporter"
  exit 1
fi

MAIN_CLASS="$1"
shift

# ── Locate assembled jar ──────────────────────────────────────
JAR="$PROJECT_ROOT/sparkshield-assembly-1.0.0.jar"
if [ ! -f "$JAR" ]; then
  echo "ERROR: Assembly jar not found at $JAR"
  echo "  Build it first with:"
  echo "    cd scala && java -jar ../scripts/sbt-launch.jar assembly"
  exit 1
fi

# ── Dataset path ──────────────────────────────────────────────
export SPARKSHIELD_DATASET_DIR="${SPARKSHIELD_DATASET_DIR:-$HOME/Downloads/MachineLearningCVE}"

# ── Run ───────────────────────────────────────────────────────
echo ""
echo "┌──────────────────────────────────────────────────────────"
echo "│  SparkShield — spark-submit"
echo "│  Class : $MAIN_CLASS"
echo "│  Jar   : $JAR"
echo "│  Dataset: $SPARKSHIELD_DATASET_DIR"
echo "└──────────────────────────────────────────────────────────"
echo ""

# ── Separate Spark options and application arguments ─────────
SPARK_OPTS=()
APP_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --conf|--driver-memory|--executor-memory|--driver-cores|--executor-cores|--master)
      SPARK_OPTS+=("$1" "$2")
      shift 2
      ;;
    --conf=*|--driver-*|--executor-*)
      SPARK_OPTS+=("$1")
      shift 1
      ;;
    --)
      shift
      APP_ARGS+=("$@")
      break
      ;;
    *)
      APP_ARGS+=("$1")
      shift 1
      ;;
  esac
done

exec "$SPARK_SUBMIT" \
  --master "local[*]" \
  --class "$MAIN_CLASS" \
  --driver-memory 2g \
  --conf "spark.ui.enabled=false" \
  --conf "spark.driver.extraJavaOptions=-Dsparkshield.dataset.dir=$SPARKSHIELD_DATASET_DIR" \
  ${SPARK_OPTS[@]+"${SPARK_OPTS[@]}"} \
  "$JAR" \
  ${APP_ARGS[@]+"${APP_ARGS[@]}"}
