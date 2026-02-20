#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: ralph-new-issue.sh <planner|developer|qa|manager> \"<title>\"" >&2
  exit 1
fi

ROLE="$1"
shift
TITLE="$*"

case "${ROLE}" in
  planner|developer|qa|manager) ;;
  *)
    echo "invalid role: ${ROLE}" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RALPH_ROOT="${PROJECT_DIR}/.ralph"
ISSUES_DIR="${RALPH_ROOT}/issues"

mkdir -p "${ISSUES_DIR}" "${RALPH_ROOT}/in-progress" "${RALPH_ROOT}/done" "${RALPH_ROOT}/blocked"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RAND="$(printf '%04d' "$((RANDOM % 10000))")"
ID="I-${TS}-${RAND}"
OUT_FILE="${ISSUES_DIR}/${ID}.md"

cat > "${OUT_FILE}" <<EOF_ISSUE
id: ${ID}
role: ${ROLE}
status: ready
title: ${TITLE}
created_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)

## Objective
- ${TITLE}

## Acceptance Criteria
- [ ] Required changes are implemented.
- [ ] Validation command passes if this role requires validation.
EOF_ISSUE

echo "created: ${OUT_FILE}"
