#!/usr/bin/env bash
# =============================================================
# SparkShield — Dataset Ingestion and Cleaning Helper
# Milestone 2 | Apache Spark 3.5.9
#
# Usage:
#   # 1. Run unit tests
#   ./scripts/clean-dataset.sh test
#
#   # 2. Inspect a single CSV file (schema, sample, row count, labels)
#   ./scripts/clean-dataset.sh inspect [path/to/file.csv]
#
#   # 3. Clean a single CSV file and output to Parquet
#   ./scripts/clean-dataset.sh clean [path/to/file.csv] [optional_output_dir]
#
#   # 4. Clean all 8 CSV files in dataset directory
#   ./scripts/clean-dataset.sh all [optional_dataset_dir] [optional_output_dir]
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DATASET_DIR="${SPARKSHIELD_DATASET_DIR:-$HOME/Downloads/MachineLearningCVE}"
DEFAULT_FILE="$DATASET_DIR/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"
ACTION="${1:-inspect}"

case "$ACTION" in
  test)
    echo "── Running Unit Tests (RDDBasicsSpec + DataCleaningSpec) ──"
    cd "$PROJECT_ROOT/scala"
    "$SCRIPT_DIR/sbt.sh" test
    ;;

  inspect)
    TARGET_FILE="${2:-$DEFAULT_FILE}"
    echo "── Inspecting CSV File: $TARGET_FILE ──"
    "$SCRIPT_DIR/run.sh" sparkshield.DataIngestion "$TARGET_FILE"
    ;;

  clean)
    TARGET_FILE="${2:-$DEFAULT_FILE}"
    OUTPUT_PATH="${3:-data/processed/cleaned_$(date +%Y-%m-%d)}"
    echo "── Cleaning CSV File: $TARGET_FILE ──"
    echo "── Output Directory: $OUTPUT_PATH ──"
    "$SCRIPT_DIR/run.sh" sparkshield.DataCleaning "$TARGET_FILE" "$OUTPUT_PATH"
    ;;

  all)
    INPUT_DIR="${2:-$DATASET_DIR}"
    OUTPUT_PATH="${3:-data/processed/all_cleaned_$(date +%Y-%m-%d)}"
    echo "── Running Batch Cleaning for All CSVs in: $INPUT_DIR ──"
    for csv_file in "$INPUT_DIR"/*.csv; do
      if [ -f "$csv_file" ]; then
        fname="$(basename "$csv_file" .csv)"
        file_output="$OUTPUT_PATH/$fname"
        echo ""
        echo "=========================================================="
        echo " Processing: $fname"
        echo " Output to : $file_output"
        echo "=========================================================="
        "$SCRIPT_DIR/run.sh" sparkshield.DataCleaning "$csv_file" "$file_output"
      fi
    done
    echo ""
    echo "All files processed successfully."
    ;;

  *)
    echo "Usage: $0 {test|inspect [file]|clean [file] [output_dir]|all [dir] [output_dir]}"
    exit 1
    ;;
esac
