#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$PWD}"

if ! command -v ralphctl >/dev/null 2>&1; then
  echo "error: ralphctl not found in PATH" >&2
  exit 1
fi

echo "[cp-migrate-verify] project_dir=${PROJECT_DIR}"
ralphctl --project-dir "${PROJECT_DIR}" cp init
ralphctl --project-dir "${PROJECT_DIR}" cp migrate-v1 --apply --verify --strict-verify
ralphctl --project-dir "${PROJECT_DIR}" cp doctor --repair --strict
ralphctl --project-dir "${PROJECT_DIR}" cp cutover evaluate
