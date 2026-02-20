#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"
STATE_FILE="${RALPH_ROOT}/state.env"
PROFILE_FILE="${RALPH_ROOT}/profile.env"
PROFILE_LOCAL_FILE="${RALPH_ROOT}/profile.local.env"

QUEUE_DIR="${RALPH_ROOT}/issues"
IN_PROGRESS_DIR="${RALPH_ROOT}/in-progress"
DONE_DIR="${RALPH_ROOT}/done"
BLOCKED_DIR="${RALPH_ROOT}/blocked"
LOGS_DIR="${RALPH_ROOT}/logs"

MAX_LOOPS="${MAX_LOOPS:-0}"

mkdir -p "${QUEUE_DIR}" "${IN_PROGRESS_DIR}" "${DONE_DIR}" "${BLOCKED_DIR}" "${LOGS_DIR}"
[ -f "${STATE_FILE}" ] || printf 'RALPH_LOCAL_ENABLED=true\n' > "${STATE_FILE}"

if [ -f "${PROFILE_FILE}" ]; then
  # shellcheck source=/dev/null
  . "${PROFILE_FILE}"
fi
if [ -f "${PROFILE_LOCAL_FILE}" ]; then
  # shellcheck source=/dev/null
  . "${PROFILE_LOCAL_FILE}"
fi

RALPH_CODEX_MODEL="${RALPH_CODEX_MODEL:-gpt-5.3-codex}"
RALPH_CODEX_SANDBOX="${RALPH_CODEX_SANDBOX:-workspace-write}"
RALPH_CODEX_APPROVAL="${RALPH_CODEX_APPROVAL:-never}"
RALPH_REQUIRE_CODEX="${RALPH_REQUIRE_CODEX:-true}"
RALPH_IDLE_SLEEP_SEC="${RALPH_IDLE_SLEEP_SEC:-20}"
RALPH_EXIT_ON_IDLE="${RALPH_EXIT_ON_IDLE:-false}"
RALPH_NO_READY_MAX_LOOPS="${RALPH_NO_READY_MAX_LOOPS:-0}"
RALPH_VALIDATE_ROLES="${RALPH_VALIDATE_ROLES:-developer,qa}"
RALPH_VALIDATE_CMD="${RALPH_VALIDATE_CMD:-echo \"skip validation\"}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 2
  fi
}

require_cmd awk
require_cmd find
require_cmd mv
require_cmd date
require_cmd bash

if [ "${RALPH_REQUIRE_CODEX}" = "true" ]; then
  require_cmd codex
fi

local_enabled() {
  local v
  v="$(awk -F= '/^RALPH_LOCAL_ENABLED=/{print $2; exit}' "${STATE_FILE}" 2>/dev/null || true)"
  [ "${v}" = "true" ]
}

meta_value() {
  local file="$1"
  local key="$2"
  awk -F': ' -v k="${key}" '$1==k{print $2; exit}' "${file}" 2>/dev/null || true
}

set_status() {
  local file="$1"
  local status="$2"
  local tmp
  tmp="$(mktemp "${RALPH_ROOT}/tmp.status.XXXXXX")"
  awk -v s="${status}" '
    BEGIN { replaced=0 }
    /^status:[[:space:]]*/ {
      print "status: " s
      replaced=1
      next
    }
    { print }
    END {
      if (replaced==0) {
        print "status: " s
      }
    }
  ' "${file}" > "${tmp}"
  mv "${tmp}" "${file}"
}

mark_result() {
  local file="$1"
  local result="$2"
  local reason="$3"
  local log_file="$4"
  cat >> "${file}" <<EOF_RESULT

## Ralph Result
- status: ${result}
- reason: ${reason}
- log_file: ${log_file}
- updated_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF_RESULT
}

should_validate_role() {
  local role="$1"
  case ",${RALPH_VALIDATE_ROLES}," in
    *",${role},"*) return 0 ;;
    *) return 1 ;;
  esac
}

pick_next_issue() {
  local f status
  for f in "${QUEUE_DIR}"/I-*.md; do
    [ -f "${f}" ] || continue
    status="$(meta_value "${f}" "status")"
    [ -n "${status}" ] || status="ready"
    if [ "${status}" = "ready" ]; then
      echo "${f}"
      return 0
    fi
  done
  return 1
}

recover_in_progress() {
  local f id dst
  for f in "${IN_PROGRESS_DIR}"/I-*.md; do
    [ -f "${f}" ] || continue
    id="$(basename "${f}")"
    dst="${QUEUE_DIR}/${id}"
    if [ -f "${dst}" ]; then
      dst="${QUEUE_DIR}/recovered-${id}"
    fi
    set_status "${f}" "ready"
    mv "${f}" "${dst}"
  done
}

run_issue() {
  local issue_file="$1"
  local id role title in_progress_file log_file prompt codex_rc

  id="$(meta_value "${issue_file}" "id")"
  role="$(meta_value "${issue_file}" "role")"
  title="$(meta_value "${issue_file}" "title")"
  [ -n "${id}" ] || id="$(basename "${issue_file}" .md)"
  [ -n "${role}" ] || role="developer"

  in_progress_file="${IN_PROGRESS_DIR}/${id}.md"
  mv "${issue_file}" "${in_progress_file}"
  set_status "${in_progress_file}" "in-progress"

  log_file="${LOGS_DIR}/${id}-$(date -u +%Y%m%dT%H%M%SZ).log"

  prompt="You are executing a local Ralph issue in project ${PROJECT_DIR}.\n\nIssue:\n$(cat "${in_progress_file}")\n\nRules:\n- Keep edits inside project root.\n- Follow acceptance criteria.\n- Do not open PR or remote automation."

  if [ "${RALPH_REQUIRE_CODEX}" = "true" ]; then
    set +e
    codex --ask-for-approval "${RALPH_CODEX_APPROVAL}" exec \
      --model "${RALPH_CODEX_MODEL}" \
      --sandbox "${RALPH_CODEX_SANDBOX}" \
      --cd "${PROJECT_DIR}" \
      "${prompt}" > "${log_file}" 2>&1
    codex_rc=$?
    set -e

    if [ "${codex_rc}" -ne 0 ]; then
      set_status "${in_progress_file}" "blocked"
      mark_result "${in_progress_file}" "blocked" "codex_exit_${codex_rc}" "${log_file}"
      mv "${in_progress_file}" "${BLOCKED_DIR}/${id}.md"
      echo "[ralph-loop] blocked ${id}: codex rc=${codex_rc}"
      return 1
    fi
  else
    echo "codex execution skipped (RALPH_REQUIRE_CODEX=false)" > "${log_file}"
  fi

  if should_validate_role "${role}"; then
    set +e
    bash -lc "${RALPH_VALIDATE_CMD}" >> "${log_file}" 2>&1
    validate_rc=$?
    set -e
    if [ "${validate_rc}" -ne 0 ]; then
      set_status "${in_progress_file}" "blocked"
      mark_result "${in_progress_file}" "blocked" "validate_exit_${validate_rc}" "${log_file}"
      mv "${in_progress_file}" "${BLOCKED_DIR}/${id}.md"
      echo "[ralph-loop] blocked ${id}: validation rc=${validate_rc}"
      return 1
    fi
  fi

  set_status "${in_progress_file}" "done"
  mark_result "${in_progress_file}" "done" "completed" "${log_file}"
  mv "${in_progress_file}" "${DONE_DIR}/${id}.md"
  echo "[ralph-loop] done ${id} (${title:-untitled})"
  return 0
}

recover_in_progress

loop_count=0
idle_count=0

while true; do
  if ! local_enabled; then
    echo "[ralph-loop] disabled; stopping"
    break
  fi

  if [ "${MAX_LOOPS}" -gt 0 ] && [ "${loop_count}" -ge "${MAX_LOOPS}" ]; then
    echo "[ralph-loop] max loops reached (${MAX_LOOPS})"
    break
  fi

  next_issue="$(pick_next_issue || true)"
  if [ -z "${next_issue}" ]; then
    idle_count=$((idle_count + 1))
    if [ "${RALPH_EXIT_ON_IDLE}" = "true" ]; then
      echo "[ralph-loop] no ready issues; exit_on_idle=true"
      break
    fi
    if [ "${RALPH_NO_READY_MAX_LOOPS}" -gt 0 ] && [ "${idle_count}" -ge "${RALPH_NO_READY_MAX_LOOPS}" ]; then
      echo "[ralph-loop] no ready issues; reached no_ready_max_loops=${RALPH_NO_READY_MAX_LOOPS}"
      break
    fi
    echo "[ralph-loop] no ready issues; sleeping ${RALPH_IDLE_SLEEP_SEC}s"
    sleep "${RALPH_IDLE_SLEEP_SEC}"
    continue
  fi

  idle_count=0
  run_issue "${next_issue}" || true
  loop_count=$((loop_count + 1))
done
