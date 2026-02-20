#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"
PID_FILE="${RALPH_ROOT}/runner.pid"
LOG_FILE="${RALPH_ROOT}/logs/runner.out"

CMD="${1:-status}"
TAIL_LINES="${TAIL_LINES:-120}"

is_running() {
  if [ ! -f "${PID_FILE}" ]; then
    return 1
  fi
  local pid
  pid="$(awk 'NR==1{print $1; exit}' "${PID_FILE}" 2>/dev/null || true)"
  [ -n "${pid}" ] || return 1
  kill -0 "${pid}" >/dev/null 2>&1
}

start() {
  "${SCRIPT_DIR}/ralph-init.sh" >/dev/null
  "${SCRIPT_DIR}/ralph-control.sh" on >/dev/null

  if is_running; then
    echo "ralph-loop already running (pid=$(cat "${PID_FILE}"))"
    return 0
  fi

  nohup "${SCRIPT_DIR}/ralph-loop.sh" >> "${LOG_FILE}" 2>&1 < /dev/null &
  echo "$!" > "${PID_FILE}"
  echo "ralph-loop started (pid=$!)"
}

stop() {
  "${SCRIPT_DIR}/ralph-control.sh" off >/dev/null || true

  if ! is_running; then
    rm -f "${PID_FILE}"
    echo "ralph-loop already stopped"
    return 0
  fi

  pid="$(cat "${PID_FILE}")"
  kill "${pid}" >/dev/null 2>&1 || true
  sleep 1
  if kill -0 "${pid}" >/dev/null 2>&1; then
    kill -9 "${pid}" >/dev/null 2>&1 || true
  fi
  rm -f "${PID_FILE}"
  echo "ralph-loop stopped"
}

status() {
  "${SCRIPT_DIR}/ralph-status.sh"
}

run_once() {
  "${SCRIPT_DIR}/ralph-init.sh" >/dev/null
  MAX_LOOPS="${MAX_LOOPS:-1}" "${SCRIPT_DIR}/ralph-loop.sh"
}

case "${CMD}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    status
    ;;
  tail)
    mkdir -p "${RALPH_ROOT}/logs"
    touch "${LOG_FILE}"
    tail -n "${TAIL_LINES}" -f "${LOG_FILE}"
    ;;
  run)
    run_once
    ;;
  *)
    echo "usage: ralph-daemon.sh start|stop|restart|status|tail|run" >&2
    exit 1
    ;;
esac
