#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$PWD}"
NOTE="${2:-manual rollback via scripts/cp_rollback.sh}"

if ! command -v ralphctl >/dev/null 2>&1; then
  echo "error: ralphctl not found in PATH" >&2
  exit 1
fi

echo "[cp-rollback] project_dir=${PROJECT_DIR}"
ralphctl --project-dir "${PROJECT_DIR}" cp cutover disable-v2 --note "${NOTE}"
ralphctl --project-dir "${PROJECT_DIR}" cp doctor --repair
ralphctl --project-dir "${PROJECT_DIR}" cp cutover status
