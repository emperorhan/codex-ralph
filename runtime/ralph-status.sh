#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"
STATE_FILE="${RALPH_ROOT}/state.env"
PROFILE_FILE="${RALPH_ROOT}/profile.env"
PID_FILE="${RALPH_ROOT}/runner.pid"
ISSUES_DIR="${RALPH_ROOT}/issues"
IN_PROGRESS_DIR="${RALPH_ROOT}/in-progress"
DONE_DIR="${RALPH_ROOT}/done"
BLOCKED_DIR="${RALPH_ROOT}/blocked"

count_files() {
  local dir="$1"
  if [ -d "${dir}" ]; then
    find "${dir}" -maxdepth 1 -type f -name 'I-*.md' | wc -l | awk '{print $1}'
  else
    echo 0
  fi
}

read_state() {
  awk -F= '/^RALPH_LOCAL_ENABLED=/{print $2; exit}' "${STATE_FILE}" 2>/dev/null || true
}

read_plugin() {
  awk -F= '/^RALPH_PLUGIN_NAME=/{print $2; exit}' "${PROFILE_FILE}" 2>/dev/null || true
}

is_running() {
  if [ ! -f "${PID_FILE}" ]; then
    return 1
  fi
  local pid
  pid="$(awk 'NR==1{print $1; exit}' "${PID_FILE}" 2>/dev/null || true)"
  [ -n "${pid}" ] || return 1
  kill -0 "${pid}" >/dev/null 2>&1
}

next_ready() {
  local f status id role title
  for f in "${ISSUES_DIR}"/I-*.md; do
    [ -f "${f}" ] || continue
    status="$(awk -F': ' '/^status:/{print $2; exit}' "${f}" 2>/dev/null || true)"
    [ -n "${status}" ] || status="ready"
    if [ "${status}" = "ready" ]; then
      id="$(awk -F': ' '/^id:/{print $2; exit}' "${f}" 2>/dev/null || true)"
      role="$(awk -F': ' '/^role:/{print $2; exit}' "${f}" 2>/dev/null || true)"
      title="$(awk -F': ' '/^title:/{print $2; exit}' "${f}" 2>/dev/null || true)"
      echo "${id:-unknown} | ${role:-unknown} | ${title:-untitled}"
      return 0
    fi
  done
  echo "none"
}

echo "## Ralph Status"
echo "- updated_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "- project: ${PROJECT_DIR}"
echo "- plugin: $(read_plugin)"
echo "- enabled: $(read_state)"
if is_running; then
  echo "- daemon: running"
else
  echo "- daemon: stopped"
fi
echo "- queue_ready: $(count_files "${ISSUES_DIR}")"
echo "- in_progress: $(count_files "${IN_PROGRESS_DIR}")"
echo "- done: $(count_files "${DONE_DIR}")"
echo "- blocked: $(count_files "${BLOCKED_DIR}")"
echo "- next_ready: $(next_ready)"
