#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: apply-plugin.sh <target-project-dir> <plugin-name>" >&2
  exit 1
fi

TARGET_DIR="$(cd "$1" && pwd)"
PLUGIN_NAME="$2"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGIN_FILE="${ROOT_DIR}/plugins/${PLUGIN_NAME}/plugin.env"
RALPH_DIR="${TARGET_DIR}/.ralph"
PROFILE_FILE="${RALPH_DIR}/profile.env"

if [ ! -f "${PLUGIN_FILE}" ]; then
  echo "plugin not found: ${PLUGIN_NAME}" >&2
  exit 1
fi

mkdir -p "${RALPH_DIR}"
if [ -f "${PROFILE_FILE}" ]; then
  cp "${PROFILE_FILE}" "${PROFILE_FILE}.bak.$(date -u +%Y%m%dT%H%M%SZ)"
fi
cp "${PLUGIN_FILE}" "${PROFILE_FILE}"

echo "applied plugin: ${PLUGIN_NAME}"
echo "profile: ${PROFILE_FILE}"
