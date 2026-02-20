#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"
STATE_FILE="${RALPH_ROOT}/state.env"
CMD="${1:-status}"

mkdir -p "${RALPH_ROOT}"
if [ ! -f "${STATE_FILE}" ]; then
  printf 'RALPH_LOCAL_ENABLED=true\n' > "${STATE_FILE}"
fi

current() {
  awk -F= '/^RALPH_LOCAL_ENABLED=/{print $2; exit}' "${STATE_FILE}" 2>/dev/null || true
}

set_state() {
  local value="$1"
  printf 'RALPH_LOCAL_ENABLED=%s\n' "${value}" > "${STATE_FILE}"
}

case "${CMD}" in
  on)
    set_state true
    echo "ralph_local_enabled=true"
    ;;
  off)
    set_state false
    echo "ralph_local_enabled=false"
    ;;
  status)
    echo "ralph_local_enabled=$(current)"
    ;;
  *)
    echo "usage: ralph-control.sh on|off|status" >&2
    exit 1
    ;;
esac
