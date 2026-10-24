#!/usr/bin/env bash
# =============================================================
# SparkShield — Machine Learning Intrusion Detection Helper
# Milestone 5 | Apache Spark MLlib 3.5.9
#
# Usage:
#   # 1. Run ML unit tests
#   ./scripts/train-model.sh test
#
#   # 2. Train model with default configuration (80/20 split, seed 42)
#   ./scripts/train-model.sh train
#
#   # 3. Train with custom arguments
#   ./scripts/train-model.sh train --max-iter 100 --reg-param 0.05
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ACTION="${1:-train}"

case "$ACTION" in
  test)
    echo "── Running ML Unit Tests (IntrusionDetectionModelSpec) ──"
    cd "$PROJECT_ROOT/scala"
    "$SCRIPT_DIR/sbt.sh" "testOnly sparkshield.IntrusionDetectionModelSpec"
    ;;

  train)
    shift || true
    echo "── Training Logistic Regression Intrusion Detection Pipeline ──"
    "$SCRIPT_DIR/run.sh" sparkshield.IntrusionDetectionModel --driver-memory 3g -- "$@"
    ;;

  *)
    echo "Usage: $0 {test|train} [extra args...]"
    exit 1
    ;;
esac
