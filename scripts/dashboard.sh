#!/usr/bin/env bash

# SparkShield — Analytics Dashboard Controller
# Usage: ./scripts/dashboard.sh {start|stop|status|restart|browse} [--port <PORT>]

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DASHBOARD_DIR="${PROJECT_ROOT}/dashboard"
PID_FILE="${DASHBOARD_DIR}/.dashboard.pid"
LOG_FILE="${DASHBOARD_DIR}/dashboard.log"
PORT=8088

show_help() {
  cat <<'EOF'
SparkShield — Analytics Dashboard Controller

Usage:
  ./scripts/dashboard.sh start [--port <PORT>]
  ./scripts/dashboard.sh stop
  ./scripts/dashboard.sh status
  ./scripts/dashboard.sh restart [--port <PORT>]
  ./scripts/dashboard.sh browse

Commands:
  start     Start the dashboard HTTP server in the background
  stop      Stop the running dashboard server
  status    Check if dashboard server is currently active
  restart   Restart dashboard server
  browse    Open the dashboard in default system browser

Options:
  --port <PORT>   Port number to bind (default: 8088)
EOF
}

is_running() {
  if [[ -f "${PID_FILE}" ]]; then
    local pid
    pid="$(cat "${PID_FILE}")"
    if kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
  fi
  return 1
}

cmd_start() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "[INFO] Dashboard is already running (PID: ${pid}) at http://127.0.0.1:${PORT}"
    return 0
  fi

  local python_cmd="${PYTHON:-python3}"
  echo "Starting SparkShield Analytics Dashboard on port ${PORT} with ${python_cmd}..."
  nohup "${python_cmd}" "${DASHBOARD_DIR}/server.py" "${PORT}" > "${LOG_FILE}" 2>&1 &
  local new_pid=$!
  echo "${new_pid}" > "${PID_FILE}"

  sleep 1
  if is_running; then
    echo "[OK] Dashboard successfully started (PID: ${new_pid})."
    echo "     URL: http://127.0.0.1:${PORT}"
    echo "     Log: ${LOG_FILE}"
  else
    echo "[ERROR] Failed to start dashboard. Check log:"
    cat "${LOG_FILE}"
    exit 1
  fi
}

cmd_stop() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "Stopping SparkShield Dashboard (PID: ${pid})..."
    kill "${pid}" 2>/dev/null || true
    rm -f "${PID_FILE}"
    echo "[OK] Dashboard stopped."
  else
    echo "[INFO] Dashboard is not running."
    rm -f "${PID_FILE}"
  fi
}

cmd_status() {
  if is_running; then
    local pid
    pid="$(cat "${PID_FILE}")"
    echo "[ONLINE] Dashboard is running (PID: ${pid})."
    echo "         URL: http://127.0.0.1:${PORT}"
  else
    echo "[OFFLINE] Dashboard is not running."
  fi
}

cmd_browse() {
  if is_running; then
    echo "Opening http://127.0.0.1:${PORT} in default browser..."
    open "http://127.0.0.1:${PORT}"
  else
    echo "[WARN] Dashboard is not running. Starting it first..."
    cmd_start
    open "http://127.0.0.1:${PORT}"
  fi
}

# ── Argument Parsing ──────────────────────────────────────────
ACTION="${1:-status}"
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="$2"
      shift 2
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      show_help >&2
      exit 1
      ;;
  esac
done

case "${ACTION}" in
  start)
    cmd_start
    ;;
  stop)
    cmd_stop
    ;;
  status)
    cmd_status
    ;;
  restart)
    cmd_stop
    sleep 1
    cmd_start
    ;;
  browse)
    cmd_browse
    ;;
  -h|--help)
    show_help
    ;;
  *)
    echo "Invalid command: ${ACTION}" >&2
    show_help >&2
    exit 1
    ;;
esac
