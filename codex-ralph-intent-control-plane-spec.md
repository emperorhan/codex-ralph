# Codex-Ralph Intent Control Plane Specification (v2 Runtime)

Updated: 2026-03-08 (Asia/Seoul)

## 1. Purpose
This document defines the implemented Control Plane v2 runtime for codex-ralph.
It describes the executable contract for:

- Intent ingestion and validation
- Task graph planning
- Orchestrated execution and recovery
- Verification policy enforcement
- Observability, doctor checks, and cutover governance

The v1 `.ralph/` issue loop remains available for compatibility and migration input.

## 2. Scope
### In scope (v2)
- Single-project control plane
- SQLite source of truth (`.ralph-v2/controlplane.db`)
- JSONL event/learning journals
- DAG planning, task state machine, lease/heartbeat orchestration
- Verification policy engine (`unit|integration|lint|custom`)
- Doctor/metrics/cutover/migration/fault-injection workflows
- Read-only status API

### Out of scope (current)
- Multi-project fleet as v2 default runtime
- External issue tracker bidirectional sync
- Semantic memory subsystem
- Cross-project global scheduler

## 3. Runtime Layout
```
intent/
graph/
tasks/
.ralph-v2/
  controlplane.db
  events.jsonl
  learning.jsonl
  cutover.json
  reports/
    metrics-baseline.json
    soak-*.json
    cutover-evaluate-*.json
    cutover-auto-*.json
```

## 4. Core Data Contracts
### IntentSpecV1
- `id`, `version`, `goal`
- `constraints[]`, `success_criteria[]`, `non_goals[]`
- `epics[]`

### TaskNodeV1
- `id`, `epic_id`, `title`, `role`, `priority`
- `deps[]`, `acceptance[]`, `verify_cmd`, `risk_level`
- optional execution fields: `execute_cmd`, `codex_objective`

### TaskRunV1
- `task_id`, `attempt`, `state`
- `started_at_utc`, `ended_at_utc`
- `failure_reason`, `artifacts[]`

### VerificationResultV1
- `task_id`, `checks[]`, `pass`
- `evidence[]`, `failure_reason`, `verified_at_utc`

### LearningEventV1
- `time_utc`, `task_id`, `category`, `lesson`, `action_item`

## 5. State Machine
Primary path:
```
draft -> planned -> ready -> running -> verifying -> done
```

Failure and recovery path:
```
running|verifying -> blocked
blocked -> ready (recover policy pass or force recover)
```

Transition validity is enforced and audited by doctor/event replay checks.

## 6. Planning and Validation
### Intent import (`cp import-intent`)
- Validates required fields and schema shape
- Persists intent record and import event

### Graph planning (`cp plan`)
- Compiles intent tasks to DAG
- Hard-fails on:
  - cycle dependencies
  - orphan dependencies
  - missing/empty required fields (`acceptance`, `verify_cmd`)
- Deterministic ordering for equal priority cases

## 7. Execution and Recovery
### Orchestrator (`cp run`)
- Ready task claim with lease ownership
- Heartbeat-based lease extension
- Worker result application via state machine
- Verification gate before `done`

### Recovery (`cp recover`)
- Recovers blocked tasks when policy allows
- Supports forced recovery override

### Recovery policy controls
- Retry budget
- Backoff window (`next_recover_at_utc`)
- Circuit-open cooldown (`circuit_open_until_utc`)

## 8. Verification Policy Engine
`verify_cmd` supports policy-prefixed checks:

- `unit: ...`
- `integration: ...`
- `lint: ...`
- `custom: ...`

If no prefix is given, command is treated as `custom`.

`done` is blocked when mandatory verification evidence is missing.
False completion prevention is tracked as `false_done_prevented` metric/event.

## 9. Observability and Ledgers
### Journals
- `events.jsonl` (event stream)
- `learning.jsonl` (learning stream)

### SQLite ledgers
- `cp_event_ledger`
- `cp_learning_ledger`

If legacy DB rows exist in `cp_events`/`cp_learnings` while ledgers are empty,
schema ensure step automatically backfills ledger rows.

### API (`cp api`)
- `GET /health`
- `GET /v2/status`
- `GET /v2/metrics`
- `GET /v2/metrics?with_baseline=true`
- `GET /v2/metrics/summary`
- `GET /v2/doctor`
- `GET /v2/cutover`
- `GET /v2/events?limit=N`
- `GET /v2/events/stream` (SSE-style)

## 10. Doctor and Integrity Gates
Representative doctor checks:

- `task_graph`
- `task_state_validity`
- `event_replay_consistency`
- `event_transition_consistency`
- `task_json_consistency`
- `event_journal_consistency`
- `learning_journal_consistency`
- `event_ledger_consistency`
- `learning_ledger_consistency`
- KPI checks (`kpi_blocked_rate`, `kpi_recovery_success_rate`, `kpi_mttr_seconds`)

Cutover evaluation classifies critical integrity failures into `data_integrity` category.

## 11. CLI Surface (Control Plane)
Primary commands:

- `cp init`
- `cp import-intent --file <path>`
- `cp plan --intent-id <id>`
- `cp run --max-workers <n> [--execute-with-codex]`
- `cp verify --task-id <id>`
- `cp recover [--force]`
- `cp status --json`
- `cp metrics [--with-baseline] [--json]`
- `cp baseline <capture|show>`
- `cp doctor [--strict] [--repair ...]`
- `cp soak --duration-sec <n> --interval-sec <n> [--strict]`
- `cp cutover <status|evaluate|auto|enable-v2|disable-v2>`
- `cp fault-inject --task-id <id> --mode <lease-expire|verify-fail|execute-fail|permission-denied>`
- `cp migrate-v1 [--dry-run|--apply] [--verify] [--strict-verify]`
- `cp api --listen <host:port>`

## 12. Cutover Policy
Cutover evaluation uses:

- Doctor failures
- KPI thresholds
- Optional baseline requirement
- Optional soak pass requirement with max age bound

Rollback policy in `cp cutover auto` supports:

- `all`
- `critical`
- `doctor`
- `kpi`
- `baseline`
- `soak`
- `data_integrity`

## 13. KPI Targets
Operational targets currently enforced by policy:

- `blocked_rate <= 0.25`
- `recovery_success_rate >= 0.70` (when recovery events exist)
- `mttr_seconds <= 300`

Program-level goals still require real workload validation:

- blocked-rate improvement vs baseline
- sustained soak stability window
- false done = 0 in production flow

## 14. Operational Runbook (Minimal)
1. `cp init`
2. `cp import-intent` / `cp plan`
3. `cp run` + `cp recover` loop
4. `cp baseline capture`
5. `cp soak`
6. `cp cutover evaluate`
7. `cp cutover auto`
8. Promote to default (`cp cutover enable-v2 --canary=false`) when policy gates are satisfied

## 15. Backward Compatibility
- v1 queue is retained for migration input and fallback operation.
- v2 does not require full v1 behavioral parity at runtime.
- Migration tooling preserves state counts and verifies parity.
