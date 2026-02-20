#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGINS_DIR="${ROOT_DIR}/plugins"

if [ ! -d "${PLUGINS_DIR}" ]; then
  exit 0
fi

find "${PLUGINS_DIR}" -maxdepth 1 -mindepth 1 -type d -exec basename {} \; | sort
