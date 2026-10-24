#!/usr/bin/env bash

# SparkShield — Spark application launcher
# Usage: ./scripts/run.sh <MainClass> [Spark options] [-- application args]

set -euo pipefail

# ── Project paths ─────────────────────────────────────────────
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
SCALA_DIR="${PROJECT_ROOT}/scala"

# ── Help ──────────────────────────────────────────────────────
show_help() {
  cat <<'EOF'
SparkShield — Spark application launcher

Usage:
  ./scripts/run.sh <MainClass> [Spark options] [-- application args]
  ./scripts/run.sh --help
  ./scripts/run.sh -h

Available main classes:
  sparkshield.RDDBasics
  sparkshield.DataIngestion
  sparkshield.DataCleaning
  sparkshield.SparkSQLAnalytics
  sparkshield.NetworkFlowProducer
  sparkshield.NetworkFlowStreaming
  sparkshield.NetworkFlowMLStreaming
  sparkshield.IntrusionDetectionModel

Spark options:
  --master <URL>
  --driver-memory <memory>
  --executor-memory <memory>
  --driver-cores <number>
  --executor-cores <number>
  --conf <key=value>
  --key=value

Examples:
  ./scripts/run.sh sparkshield.RDDBasics
  ./scripts/run.sh sparkshield.DataIngestion
  ./scripts/run.sh sparkshield.SparkSQLAnalytics
  ./scripts/run.sh sparkshield.NetworkFlowProducer
  ./scripts/run.sh sparkshield.NetworkFlowStreaming
  ./scripts/run.sh sparkshield.RDDBasics --master 'local[2]'
  ./scripts/run.sh sparkshield.NetworkFlowProducer -- --limit 500

Environment overrides:
  SPARK_SUBMIT       Path to spark-submit
  PYSPARK_HOME       Path to the PySpark installation
  SPARK_LOCAL_IP     Local IP address used by Spark
  DATASET_DIR        Path to the dataset directory
EOF
}

# Handle help before checking Spark installation.
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  show_help
  exit 0
fi

# ── Validate arguments ────────────────────────────────────────
if [[ $# -lt 1 ]]; then
  show_help
  exit 1
fi

MAIN_CLASS="$1"
shift

# ── Resolve Spark installation ────────────────────────────────
DEFAULT_SPARK_SUBMIT="${HOME}/spark-python/bin/spark-submit"
SPARK_SUBMIT="${SPARK_SUBMIT:-${DEFAULT_SPARK_SUBMIT}}"

if [[ ! -x "${SPARK_SUBMIT}" ]]; then
  echo "ERROR: spark-submit was not found or is not executable:" >&2
  echo "  ${SPARK_SUBMIT}" >&2
  echo "Set SPARK_SUBMIT to the correct executable path." >&2
  exit 1
fi

DEFAULT_PYSPARK_HOME="${HOME}/spark-python/lib/python3.10/site-packages/pyspark"
PYSPARK_HOME="${PYSPARK_HOME:-${DEFAULT_PYSPARK_HOME}}"

if [[ -d "${PYSPARK_HOME}/jars" ]]; then
  export PYSPARK_HOME
  export SPARK_HOME="${PYSPARK_HOME}"
else
  echo "ERROR: PySpark jars directory was not found:" >&2
  echo "  ${PYSPARK_HOME}/jars" >&2
  echo "Set PYSPARK_HOME to your PySpark installation." >&2
  exit 1
fi

export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

# ── Resolve assembly JAR ──────────────────────────────────────
JAR_PATH="${PROJECT_ROOT}/sparkshield-assembly-1.0.0.jar"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "ERROR: Assembly JAR not found:" >&2
  echo "  ${JAR_PATH}" >&2
  echo "Build it first with:" >&2
  echo "  cd \"${SCALA_DIR}\" && ../scripts/sbt.sh assembly" >&2
  exit 1
fi

# ── Dataset directory ─────────────────────────────────────────
DATASET_DIR="${DATASET_DIR:-${HOME}/Downloads/MachineLearningCVE}"

# ── Spark options and application arguments ───────────────────
SPARK_OPTS=()
APP_ARGS=()
SPARK_MASTER="local[*]"
PARSING_APP_ARGS=false

while [[ $# -gt 0 ]]; do
  if [[ "${PARSING_APP_ARGS}" == true ]]; then
    APP_ARGS+=("$1")
    shift
    continue
  fi

  case "$1" in
    --)
      PARSING_APP_ARGS=true
      shift
      ;;
    --master)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Missing value for --master." >&2
        exit 2
      fi
      SPARK_MASTER="$2"
      shift 2
      ;;
    --conf|--driver-memory|--executor-memory|--driver-cores|--executor-cores)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: Missing value for option '$1'." >&2
        exit 2
      fi
      SPARK_OPTS+=("$1" "$2")
      shift 2
      ;;
    --*=*)
      SPARK_OPTS+=("$1")
      shift
      ;;

    *)
      # Forward unrecognized options to the Spark application.
      APP_ARGS+=("$1")
      shift
      ;;

  esac
done

# ── Prepare Spark-submit arguments ────────────────────────────
echo "=========================================="
echo " SparkShield"
echo "=========================================="
echo " Project      : ${PROJECT_ROOT}"
echo " Main class   : ${MAIN_CLASS}"
echo " Assembly JAR : ${JAR_PATH}"
echo " Spark submit : ${SPARK_SUBMIT}"
echo " Spark home   : ${SPARK_HOME}"
echo " Dataset dir  : ${DATASET_DIR}"
echo " Spark master : ${SPARK_MASTER}"
echo "=========================================="

SPARK_DEFAULT_OPTS=(
  --master "${SPARK_MASTER}"
  --class "${MAIN_CLASS}"
  --driver-memory 2g
  --conf spark.ui.enabled=false
  --conf "spark.driver.extraJavaOptions=-Dsparkshield.dataset.dir=${DATASET_DIR}"
)

FINAL_ARGS=()
FINAL_ARGS+=("${SPARK_DEFAULT_OPTS[@]}")

if [[ ${#SPARK_OPTS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${SPARK_OPTS[@]}")
fi

FINAL_ARGS+=("${JAR_PATH}")

if [[ ${#APP_ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${APP_ARGS[@]}")
fi

# Replace the shell process with spark-submit.
exec "${SPARK_SUBMIT}" "${FINAL_ARGS[@]}"
