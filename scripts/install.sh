#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: install.sh <target-project-dir> [plugin-name]" >&2
  exit 1
fi

TARGET_INPUT="$1"
mkdir -p "${TARGET_INPUT}"
TARGET_DIR="$(cd "${TARGET_INPUT}" && pwd)"
PLUGIN_NAME="${2:-universal-default}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="${ROOT_DIR}/runtime"
PLUGIN_FILE="${ROOT_DIR}/plugins/${PLUGIN_NAME}/plugin.env"

if [ ! -d "${RUNTIME_DIR}" ]; then
  echo "runtime directory missing: ${RUNTIME_DIR}" >&2
  exit 1
fi
if [ ! -f "${PLUGIN_FILE}" ]; then
  echo "plugin not found: ${PLUGIN_NAME}" >&2
  exit 1
fi

RALPH_DIR="${TARGET_DIR}/.ralph"
BIN_DIR="${RALPH_DIR}/bin"
mkdir -p "${BIN_DIR}"

cp "${RUNTIME_DIR}"/*.sh "${BIN_DIR}/"
chmod +x "${BIN_DIR}"/*.sh

for dir_name in issues in-progress done blocked logs reports; do
  mkdir -p "${RALPH_DIR}/${dir_name}"
done

if [ ! -f "${RALPH_DIR}/state.env" ]; then
  printf 'RALPH_LOCAL_ENABLED=true\n' > "${RALPH_DIR}/state.env"
fi
cp "${PLUGIN_FILE}" "${RALPH_DIR}/profile.env"

WRAPPER="${TARGET_DIR}/ralph"
cat > "${WRAPPER}" <<'WRAP'
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${PROJECT_DIR}/.ralph/bin"
CMD="${1:-status}"
shift || true

case "${CMD}" in
  init)
    exec "${BIN_DIR}/ralph-init.sh" "$@"
    ;;
  on|off)
    exec "${BIN_DIR}/ralph-control.sh" "${CMD}"
    ;;
  new)
    exec "${BIN_DIR}/ralph-new-issue.sh" "$@"
    ;;
  run)
    exec "${BIN_DIR}/ralph-daemon.sh" run "$@"
    ;;
  start|stop|restart|status|tail)
    exec "${BIN_DIR}/ralph-daemon.sh" "${CMD}" "$@"
    ;;
  *)
    echo "usage: ./ralph init|on|off|new|run|start|stop|restart|status|tail" >&2
    exit 1
    ;;
esac
WRAP
chmod +x "${WRAPPER}"

echo "installed codex-ralph runtime"
echo "- target: ${TARGET_DIR}"
echo "- plugin: ${PLUGIN_NAME}"
echo "- run: cd ${TARGET_DIR} && ./ralph status"
