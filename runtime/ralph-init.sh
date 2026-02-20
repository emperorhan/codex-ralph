#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"

mkdir -p \
  "${RALPH_ROOT}/issues" \
  "${RALPH_ROOT}/in-progress" \
  "${RALPH_ROOT}/done" \
  "${RALPH_ROOT}/blocked" \
  "${RALPH_ROOT}/reports" \
  "${RALPH_ROOT}/logs"

STATE_FILE="${RALPH_ROOT}/state.env"
PROFILE_FILE="${RALPH_ROOT}/profile.env"
ISSUE_TEMPLATE="${RALPH_ROOT}/issue-template.md"

if [ ! -f "${STATE_FILE}" ]; then
  cat > "${STATE_FILE}" <<'STATE'
RALPH_LOCAL_ENABLED=true
STATE
fi

if [ ! -f "${PROFILE_FILE}" ]; then
  cat > "${PROFILE_FILE}" <<'PROFILE'
# Default profile (override by plugins)
RALPH_PLUGIN_NAME=universal-default
RALPH_CODEX_MODEL=gpt-5.3-codex
RALPH_CODEX_SANDBOX=workspace-write
RALPH_CODEX_APPROVAL=never
RALPH_CODEX_EXEC_TIMEOUT_SEC=900
RALPH_CODEX_RETRY_MAX_ATTEMPTS=3
RALPH_CODEX_RETRY_BACKOFF_SEC=10
RALPH_REQUIRE_CODEX=true
RALPH_ROLE_RULES_ENABLED=true
RALPH_HANDOFF_REQUIRED=true
RALPH_HANDOFF_SCHEMA=universal

RALPH_IDLE_SLEEP_SEC=20
RALPH_EXIT_ON_IDLE=false
RALPH_NO_READY_MAX_LOOPS=0

RALPH_BUSYWAIT_DETECT_LOOPS=3
RALPH_BUSYWAIT_SELF_HEAL_ENABLED=true
RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED=true
RALPH_BUSYWAIT_SELF_HEAL_COOLDOWN_SEC=120
RALPH_BUSYWAIT_SELF_HEAL_MAX_ATTEMPTS=20
RALPH_INPROGRESS_WATCHDOG_ENABLED=true
RALPH_INPROGRESS_WATCHDOG_STALE_SEC=1800
RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS=1
RALPH_SUPERVISOR_ENABLED=true
RALPH_SUPERVISOR_RESTART_DELAY_SEC=5

RALPH_VALIDATE_ROLES=developer,qa
RALPH_VALIDATE_CMD='echo "skip validation"'
PROFILE
fi

if [ ! -f "${ISSUE_TEMPLATE}" ]; then
  cat > "${ISSUE_TEMPLATE}" <<'TEMPLATE'
id: I-0000
role: developer
status: ready
title: Example issue
created_at_utc: 2026-01-01T00:00:00Z

## Objective
- Describe the required output.

## Acceptance Criteria
- [ ] Tests or checks required by this project pass.
- [ ] Scope is limited to related files.
TEMPLATE
fi

touch "${RALPH_ROOT}/logs/runner.out"

echo "initialized: ${RALPH_ROOT}"
