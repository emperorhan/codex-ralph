#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$PWD}"
DURATION_SEC="${2:-300}"
INTERVAL_SEC="${3:-30}"
MAX_SOAK_AGE_SEC="${4:-$((DURATION_SEC + INTERVAL_SEC + 120))}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT_REL=".ralph-v2/reports/cutover-evaluate-${STAMP}.json"
REPORT_PATH="${PROJECT_DIR}/${REPORT_REL}"
AUTO_REPORT_REL=".ralph-v2/reports/cutover-auto-${STAMP}.json"
AUTO_REPORT_PATH="${PROJECT_DIR}/${AUTO_REPORT_REL}"

if ! command -v ralphctl >/dev/null 2>&1; then
  echo "error: ralphctl not found in PATH" >&2
  exit 1
fi

echo "[cp-canary] project_dir=${PROJECT_DIR} duration_sec=${DURATION_SEC} interval_sec=${INTERVAL_SEC} max_soak_age_sec=${MAX_SOAK_AGE_SEC}"
BASELINE_OUT="$(ralphctl --project-dir "${PROJECT_DIR}" cp baseline show || true)"
if echo "${BASELINE_OUT}" | grep -q "baseline not found"; then
  echo "[cp-canary] baseline not found; capturing baseline"
  ralphctl --project-dir "${PROJECT_DIR}" cp baseline capture --note "captured by cp_canary.sh before canary gate"
fi
ralphctl --project-dir "${PROJECT_DIR}" cp doctor --repair
ralphctl --project-dir "${PROJECT_DIR}" cp soak --duration-sec "${DURATION_SEC}" --interval-sec "${INTERVAL_SEC}" --strict=true
ralphctl --project-dir "${PROJECT_DIR}" cp cutover evaluate --require-baseline=true --require-soak-pass=true --max-soak-age-sec "${MAX_SOAK_AGE_SEC}" --output "${REPORT_REL}"
ralphctl --project-dir "${PROJECT_DIR}" cp cutover auto --require-baseline=true --require-soak-pass=true --max-soak-age-sec "${MAX_SOAK_AGE_SEC}" --disable-on-fail=true --rollback-on=all --pre-repair=true --pre-repair-reset-circuit=true --output "${AUTO_REPORT_REL}" --note "canary gate via scripts/cp_canary.sh"
ralphctl --project-dir "${PROJECT_DIR}" cp cutover status
echo "[cp-canary] evaluation_report=${REPORT_PATH}"
echo "[cp-canary] auto_decision_report=${AUTO_REPORT_PATH}"
