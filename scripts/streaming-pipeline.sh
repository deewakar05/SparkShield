
#!/usr/bin/env bash
# ==============================================================================
# SparkShield — Milestone 4: Kafka Integration & Streaming Analytics Operator
#
# Safe, isolated management commands for Kafka and Spark Structured Streaming.
#
# Commands:
#   ./scripts/streaming-pipeline.sh start-kafka
#   ./scripts/streaming-pipeline.sh check-kafka
#   ./scripts/streaming-pipeline.sh create-topic
#   ./scripts/streaming-pipeline.sh produce [--max-records N] [--delay-ms MS]
#   ./scripts/streaming-pipeline.sh consume [--duration-seconds SEC]
#   ./scripts/streaming-pipeline.sh run-demo [--records N] [--duration SEC]
#   ./scripts/streaming-pipeline.sh stop-kafka
#   ./scripts/streaming-pipeline.sh test
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.kafka.yml"
PROJECT_NAME="sparkshield"
TOPIC_NAME="network-flows"
BOOTSTRAP_SERVERS="localhost:9092"
CONTAINER_NAME="sparkshield-kafka"

# ── Colors ────────────────────────────────────────────────────────────────────
BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
RED="\033[0;31m"
NC="\033[0m"

log_info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ── Start Kafka ───────────────────────────────────────────────────────────────
cmd_start_kafka() {
  log_info "Starting isolated Kafka service via $COMPOSE_FILE..."
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d

  log_info "Waiting for Kafka container ($CONTAINER_NAME) to become healthy..."

  for _ in $(seq 1 30); do
    local STATUS
    STATUS="$(docker inspect --format='{{json .State.Health.Status}}' \
      "$CONTAINER_NAME" 2>/dev/null || echo '"not-running"')"

    if [[ "$STATUS" == '"healthy"' ]]; then
      log_ok "Kafka is healthy on $BOOTSTRAP_SERVERS!"
      cmd_create_topic
      return 0
    fi

    sleep 1
  done

  log_err "Kafka failed to report healthy within 30 seconds."
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --tail 20
  return 1
}

# ── Check Kafka ───────────────────────────────────────────────────────────────
cmd_check_kafka() {
  log_info "Checking Kafka status..."
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" ps

  if docker exec "$CONTAINER_NAME" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null; then
    log_ok "Broker is reachable and responding to topic queries."
  else
    log_err "Broker $CONTAINER_NAME is not responding."
    return 1
  fi
}

# ── Create or inspect the default topic ───────────────────────────────────────
cmd_create_topic() {
  log_info "Checking topic '$TOPIC_NAME'..."

  if docker exec "$CONTAINER_NAME" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list 2>/dev/null |
    grep -Fxq "$TOPIC_NAME"; then
    log_ok "Topic '$TOPIC_NAME' already exists."
  else
    log_info "Creating topic '$TOPIC_NAME'..."

    docker exec "$CONTAINER_NAME" \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create \
      --if-not-exists \
      --topic "$TOPIC_NAME" \
      --partitions 3 \
      --replication-factor 1

    log_ok "Topic '$TOPIC_NAME' created."
  fi

  docker exec "$CONTAINER_NAME" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic "$TOPIC_NAME"
}

# ── Create an isolated topic for a single demo run ────────────────────────────
cmd_create_demo_topic() {
  local topic="$1"

  log_info "Creating isolated demo topic '$topic'..."

  docker exec "$CONTAINER_NAME" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1

  docker exec "$CONTAINER_NAME" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe --topic "$topic"

  log_ok "Demo topic '$topic' is ready."
}

# ── Stop Kafka without deleting volumes ───────────────────────────────────────
cmd_stop_kafka() {
  log_info "Stopping ONLY the isolated SparkShield Kafka service..."
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" stop
  log_ok "SparkShield Kafka service stopped safely. Volumes preserved."
}

# ── Run producer ──────────────────────────────────────────────────────────────
cmd_produce() {
  log_info "Launching NetworkFlowProducer..."
  "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowProducer "$@"
}

# ── Run consumer ──────────────────────────────────────────────────────────────
cmd_consume() {
  log_info "Launching NetworkFlowStreaming consumer..."
  "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowStreaming "$@"
}

# ── Run ML inference consumer ─────────────────────────────────────────────────
cmd_consume_ml() {
  log_info "Launching NetworkFlowMLStreaming consumer..."
  "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowMLStreaming "$@"
}

# ── Run tests ─────────────────────────────────────────────────────────────────
cmd_test() {
  log_info "Running Scala test suite..."
  (
    cd "$PROJECT_ROOT/scala"
    "$PROJECT_ROOT/scripts/sbt.sh" test
  )
}

# ── End-to-end streaming demo ─────────────────────────────────────────────────
cmd_run_demo() {
  local RECORDS=1000
  local DURATION=60

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --records)
        [[ $# -ge 2 ]] || {
          log_err "--records requires a value"
          return 2
        }
        RECORDS="$2"
        shift 2
        ;;
      --duration)
        [[ $# -ge 2 ]] || {
          log_err "--duration requires a value"
          return 2
        }
        DURATION="$2"
        shift 2
        ;;
      *)
        log_err "Unknown run-demo option: $1"
        return 2
        ;;
    esac
  done

  [[ "$RECORDS" =~ ^[1-9][0-9]*$ ]] || {
    log_err "--records must be a positive integer"
    return 2
  }

  [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || {
    log_err "--duration must be a positive integer"
    return 2
  }

  local RUN_ID
  RUN_ID="$(date +%Y%m%d-%H%M%S)"

  local DEMO_TOPIC="network-flows-demo-$RUN_ID"
  local CHECKPOINT_PATH="checkpoints/demo-$RUN_ID"
  local OUTPUT_PATH="results/streaming/demo-$RUN_ID"
  local LOG_FILE="results/streaming/logs/demo-$RUN_ID.log"
  local CONSUMER_PID=""

  mkdir -p "$PROJECT_ROOT/results/streaming/logs"

  echo "=============================================================="
  echo " SparkShield Milestone 4: End-to-End Streaming Demo"
  echo " Records:    $RECORDS"
  echo " Duration:   $DURATION seconds"
  echo " Kafka topic: $DEMO_TOPIC"
  echo " Checkpoint: $CHECKPOINT_PATH"
  echo " Output:     $OUTPUT_PATH"
  echo "=============================================================="

  # Ensure Kafka is running and responding.
  cmd_check_kafka || return 1

  # A new topic isolates this run from previous demo messages.
  cmd_create_demo_topic "$DEMO_TOPIC" || return 1

  # Start the consumer before publishing messages.
  log_info "Starting consumer before producing events..."

  "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowStreaming \
    --topic "$DEMO_TOPIC" \
    --checkpoint "$CHECKPOINT_PATH" \
    --output "$OUTPUT_PATH" \
    --duration-seconds "$DURATION" \
    > "$PROJECT_ROOT/$LOG_FILE" 2>&1 &

  CONSUMER_PID=$!

  # Wait until the streaming queries are ready.
  log_info "Waiting for Spark streaming queries to start..."
  local READY=0

  for _ in $(seq 1 90); do
    if grep -q "Streaming queries started successfully" \
      "$PROJECT_ROOT/$LOG_FILE" 2>/dev/null; then
      READY=1
      break
    fi

    if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
      log_err "Consumer exited before becoming ready."
      tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
      wait "$CONSUMER_PID" || true
      return 1
    fi

    sleep 1
  done

  if [[ "$READY" -ne 1 ]]; then
    log_err "Consumer did not become ready within 90 seconds."
    tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" || true
    return 1
  fi

  log_ok "Consumer is ready. Publishing historical flow events..."

  if ! "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowProducer \
    --input "data/processed/cleaned_2026-10-24" \
    --max-records "$RECORDS" \
    --delay-ms 0 \
    --topic "$DEMO_TOPIC"; then

    log_err "Producer failed. Stopping the demo consumer."
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" || true
    tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
    return 1
  fi

  log_info "Waiting for the consumer to finish..."

  if ! wait "$CONSUMER_PID"; then
    log_err "Consumer exited with an error."
    tail -n 80 "$PROJECT_ROOT/$LOG_FILE"
    return 1
  fi

  log_ok "Streaming demo completed."
  echo "Kafka topic:   $DEMO_TOPIC"
  echo "Consumer log: $LOG_FILE"
  echo "Parquet output: $OUTPUT_PATH/parquet/"
  echo "CSV output:     $OUTPUT_PATH/csv/"

  tail -n 35 "$PROJECT_ROOT/$LOG_FILE"
}

# ── End-to-end streaming ML demo ──────────────────────────────────────────────
cmd_run_ml_demo() {
  local RECORDS=100
  local DURATION=60
  local MODEL_PATH="${SPARKSHIELD_MODEL_PATH:-}"

  # Auto-discover model if not explicitly specified via environment
  if [[ -z "$MODEL_PATH" ]]; then
    if [[ -f "$PROJECT_ROOT/results/ml/latest_run.txt" ]]; then
      local LATEST_RUN
      LATEST_RUN="$(cat "$PROJECT_ROOT/results/ml/latest_run.txt" | tr -d '[:space:]')"
      if [[ -d "$PROJECT_ROOT/$LATEST_RUN/model" ]]; then
        MODEL_PATH="$LATEST_RUN/model"
      fi
    fi
  fi
  if [[ -z "$MODEL_PATH" ]]; then
    local LATEST_RUN_DIR
    LATEST_RUN_DIR="$(find "$PROJECT_ROOT/results/ml" -maxdepth 1 -type d -name "run-*" 2>/dev/null | sort | tail -1)"
    if [[ -n "$LATEST_RUN_DIR" && -d "$LATEST_RUN_DIR/model" ]]; then
      MODEL_PATH="${LATEST_RUN_DIR#$PROJECT_ROOT/}/model"
    fi
  fi
  MODEL_PATH="${MODEL_PATH:-results/ml/run-20261024-115612/model}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --records)
        [[ $# -ge 2 ]] || {
          log_err "--records requires a value"
          return 2
        }
        RECORDS="$2"
        shift 2
        ;;
      --duration)
        [[ $# -ge 2 ]] || {
          log_err "--duration requires a value"
          return 2
        }
        DURATION="$2"
        shift 2
        ;;
      --model)
        [[ $# -ge 2 ]] || {
          log_err "--model requires a value"
          return 2
        }
        MODEL_PATH="$2"
        shift 2
        ;;
      *)
        log_err "Unknown run-ml-demo option: $1"
        return 2
        ;;
    esac
  done

  [[ "$RECORDS" =~ ^[1-9][0-9]*$ ]] || {
    log_err "--records must be a positive integer"
    return 2
  }

  [[ "$DURATION" =~ ^[1-9][0-9]*$ ]] || {
    log_err "--duration must be a positive integer"
    return 2
  }

  # Validate model exists before proceeding
  local RESOLVED_MODEL_DIR="$MODEL_PATH"
  if [[ ! -d "$RESOLVED_MODEL_DIR" && -d "$PROJECT_ROOT/$MODEL_PATH" ]]; then
    RESOLVED_MODEL_DIR="$PROJECT_ROOT/$MODEL_PATH"
  fi

  if [[ ! -d "$RESOLVED_MODEL_DIR" ]]; then
    log_err "ML model directory not found: $MODEL_PATH"
    log_err "Please train a model first using: ./scripts/train-model.sh train"
    return 1
  fi
  MODEL_PATH="$RESOLVED_MODEL_DIR"

  local RUN_ID
  RUN_ID="$(date +%Y%m%d-%H%M%S)"

  local DEMO_TOPIC="network-flows-ml-demo-$RUN_ID"
  local CHECKPOINT_PATH="checkpoints/ml-demo-$RUN_ID"
  local OUTPUT_PATH="results/streaming-ml/demo-$RUN_ID"
  local LOG_FILE="results/streaming-ml/logs/demo-$RUN_ID.log"
  local CONSUMER_PID=""

  mkdir -p "$PROJECT_ROOT/results/streaming-ml/logs"

  echo "=============================================================="
  echo " SparkShield Milestone 6: End-to-End Real-Time ML Streaming Demo"
  echo " Records:     $RECORDS"
  echo " Duration:    $DURATION seconds"
  echo " Model path:  $MODEL_PATH"
  echo " Kafka topic: $DEMO_TOPIC"
  echo " Checkpoint:  $CHECKPOINT_PATH"
  echo " Output:      $OUTPUT_PATH"
  echo "=============================================================="

  # 1. Ensure Kafka is running and healthy
  cmd_check_kafka || return 1

  # 2. Create isolated topic
  cmd_create_demo_topic "$DEMO_TOPIC" || return 1

  # 3. Start the ML streaming consumer in background
  log_info "Starting ML inference consumer before producing events..."

  "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowMLStreaming \
    --topic "$DEMO_TOPIC" \
    --checkpoint "$CHECKPOINT_PATH" \
    --output "$OUTPUT_PATH" \
    --model "$MODEL_PATH" \
    --duration-seconds "$DURATION" \
    > "$PROJECT_ROOT/$LOG_FILE" 2>&1 &

  CONSUMER_PID=$!

  # 4. Wait until streaming consumer reports ready
  log_info "Waiting for Spark ML streaming query to start..."
  local READY=0

  for _ in $(seq 1 90); do
    if grep -q "Streaming queries started successfully" \
      "$PROJECT_ROOT/$LOG_FILE" 2>/dev/null; then
      READY=1
      break
    fi

    if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
      log_err "ML consumer exited before becoming ready."
      tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
      wait "$CONSUMER_PID" || true
      return 1
    fi

    sleep 1
  done

  if [[ "$READY" -ne 1 ]]; then
    log_err "ML consumer did not become ready within 90 seconds."
    tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" || true
    return 1
  fi

  log_ok "ML Consumer is ready. Publishing historical flow events..."

  # 5. Publish known flow events
  if ! "$PROJECT_ROOT/scripts/run.sh" sparkshield.NetworkFlowProducer \
    --input "data/processed/cleaned_2026-10-24" \
    --max-records "$RECORDS" \
    --delay-ms 0 \
    --topic "$DEMO_TOPIC"; then

    log_err "Producer failed. Stopping the ML demo consumer."
    kill "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" || true
    tail -n 60 "$PROJECT_ROOT/$LOG_FILE"
    return 1
  fi

  log_info "Waiting for ML consumer to finish inference processing..."

  if ! wait "$CONSUMER_PID"; then
    log_err "ML consumer exited with an error."
    tail -n 80 "$PROJECT_ROOT/$LOG_FILE"
    return 1
  fi

  log_ok "Streaming ML inference demo completed successfully."
  echo "Kafka topic:    $DEMO_TOPIC"
  echo "Consumer log:   $LOG_FILE"
  echo "Parquet output: $OUTPUT_PATH/predictions/parquet/"
  echo "CSV output:     $OUTPUT_PATH/predictions/csv/"
  echo "Metrics file:   $OUTPUT_PATH/metrics/streaming-metrics.json"

  tail -n 35 "$PROJECT_ROOT/$LOG_FILE"

  # 6. Verify output files exist
  if [[ -d "$PROJECT_ROOT/$OUTPUT_PATH/predictions/parquet" ]]; then
    local NUM_PARQUET_FILES
    NUM_PARQUET_FILES=$(find "$PROJECT_ROOT/$OUTPUT_PATH/predictions/parquet" -name "*.parquet" | wc -l | tr -d ' ')
    log_ok "Generated $NUM_PARQUET_FILES Parquet prediction partitions."
  fi
}

# ── Help ──────────────────────────────────────────────────────────────────────
show_help() {
  cat <<'EOF'
SparkShield — Kafka Integration & Streaming Analytics

Usage:
  ./scripts/streaming-pipeline.sh <command> [options]

Commands:
  start-kafka     Start isolated Kafka and ensure the default topic exists
  check-kafka     Check broker status and list topics
  create-topic    Create or inspect the network-flows topic
  produce         Run NetworkFlowProducer
  consume         Run NetworkFlowStreaming
  consume-ml      Run NetworkFlowMLStreaming (ML inference consumer)
  run-demo        Run an isolated end-to-end streaming demo
  run-ml-demo     Run an isolated end-to-end ML streaming inference demo
  stop-kafka      Stop Kafka while preserving volumes
  test            Run the Scala test suite
  help, --help    Display this help

Examples:
  ./scripts/streaming-pipeline.sh start-kafka
  ./scripts/streaming-pipeline.sh check-kafka
  ./scripts/streaming-pipeline.sh produce --max-records 100
  ./scripts/streaming-pipeline.sh consume --duration-seconds 30
  ./scripts/streaming-pipeline.sh consume-ml --duration-seconds 30
  ./scripts/streaming-pipeline.sh run-demo --records 100 --duration 60
  ./scripts/streaming-pipeline.sh run-ml-demo --records 100 --duration 60
EOF
}

# ── Main dispatcher ───────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
  show_help
  exit 1
fi

ACTION="$1"
shift

case "$ACTION" in
  help|--help|-h)
    show_help
    ;;
  start-kafka)
    cmd_start_kafka
    ;;
  check-kafka)
    cmd_check_kafka
    ;;
  create-topic)
    cmd_create_topic
    ;;
  produce)
    cmd_produce "$@"
    ;;
  consume)
    cmd_consume "$@"
    ;;
  consume-ml)
    cmd_consume_ml "$@"
    ;;
  run-demo)
    cmd_run_demo "$@"
    ;;
  run-ml-demo)
    cmd_run_ml_demo "$@"
    ;;
  stop-kafka)
    cmd_stop_kafka
    ;;
  test)
    cmd_test
    ;;
  *)
    log_err "Unknown command: $ACTION"
    show_help >&2
    exit 2
    ;;
esac
