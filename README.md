<p align="center">
  <img src="assets/ralph-hero.jpg" alt="Ralph hero" width="180" />
</p>

<h1 align="center">codex-ralph</h1>

<p align="center"><strong>manager / planner / developer / qa</strong> 자율 에이전트 루프 CLI</p>

## 세팅

### 1) Codex 연결

`ralph` 루프는 내부적으로 `codex exec`를 사용합니다.

```bash
codex --version
codex login status
```

로그인이 안 되어 있으면:

```bash
codex login
```

API 키 로그인:

```bash
printenv OPENAI_API_KEY | codex login --with-api-key
```

### 2) ralphctl 설치

macOS / Ubuntu:

```bash
curl -fsSL https://raw.githubusercontent.com/emperorhan/codex-ralph/main/scripts/install-ralphctl.sh | bash
```

설치 확인:

```bash
ralphctl --help
```

- 기본 control dir: `~/.ralph-control`
- 첫 실행 시 필요한 기본 plugin/registry가 자동 준비됩니다.

### 3) 프로젝트 연결 (권장)

프로젝트 루트에서:

```bash
ralphctl --project-dir "$PWD" setup
```

이후:

- `./ralph` 실행 헬퍼가 생성됩니다.
- 로컬 Git 저장소가 없으면 자동 초기화됩니다.
- 기본 설정 파일:
  - `.ralph/profile.yaml`
  - `.ralph/profile.local.yaml`
- 안정성 기본값(timeout/retry/watchdog/supervisor)이 적용됩니다.
- 완료(`done`)된 이슈 단위로 자동 커밋이 누적됩니다(임시/런타임 파일 제외).
- 루프 daemon이 자동 시작됩니다.

### 4) 첫 동작 확인

```bash
./ralph doctor
./ralph status
./ralph tail
./ralph stop
./ralph start
```

문제가 있으면:

```bash
./ralph doctor --repair
./ralph recover
./ralph retry-blocked --reason codex_failed_after
```

## 활용방법

### 1) 작업 투입

수동 이슈 생성:

```bash
./ralph new developer "health endpoint 구현"
./ralph new --priority 10 --story-id US-001 developer "결제 API 에러 처리 개선"
```

PRD JSON 일괄 생성:

```bash
./ralph import-prd --file prd.json --default-role developer
```

### 2) 루프 운영

```bash
./ralph start
./ralph tail
./ralph status
./ralph stop
```

단건/역할 지정 실행:

```bash
./ralph run --max-loops 1
./ralph run --max-loops 0 --roles developer,qa
```

### 3) 결과 확인

주요 산출물:

- `.ralph/done/`: 완료 이슈
- `.ralph/blocked/`: 실패/검토 필요 이슈
- `.ralph/logs/`: 실행 로그

blocked 이슈 재시도:

```bash
./ralph retry-blocked
./ralph retry-blocked --reason codex_failed_after
./ralph retry-blocked --reason codex_permission_denied --limit 1
```

## Control Plane v2 (Intent -> Graph -> Execution)

v2는 `cp` 네임스페이스로 실행됩니다.

### 1) 초기화 및 Intent 입력

```bash
ralphctl --project-dir "$PWD" cp init
ralphctl --project-dir "$PWD" cp import-intent --file intent.json
ralphctl --project-dir "$PWD" cp plan --intent-id <intent-id>
```

### 2) 실행/검증

```bash
# 기본 실행 (verify 중심)
ralphctl --project-dir "$PWD" cp run --max-workers 1

# Codex 실행 + verify
ralphctl --project-dir "$PWD" cp run --max-workers 1 --execute-with-codex

# blocked 복구 (policy 존중: retry budget/backoff/circuit)
ralphctl --project-dir "$PWD" cp recover

# 즉시 강제 복구 (cooldown/circuit 우회)
ralphctl --project-dir "$PWD" cp recover --force

ralphctl --project-dir "$PWD" cp status --json
ralphctl --project-dir "$PWD" cp metrics --json
ralphctl --project-dir "$PWD" cp metrics --with-baseline --json
ralphctl --project-dir "$PWD" cp doctor --repair --repair-recover-limit 10 --strict
# cooldown/circuit 우회가 필요할 때
ralphctl --project-dir "$PWD" cp doctor --repair --repair-force-recover --strict
# blocked circuit/재시도 예산을 수동 초기화할 때
ralphctl --project-dir "$PWD" cp doctor --repair --repair-reset-circuit --repair-reset-retry-budget --strict
# fault injection (신뢰성 테스트)
ralphctl --project-dir "$PWD" cp fault-inject --task-id task-1 --mode lease-expire
ralphctl --project-dir "$PWD" cp fault-inject --task-id task-1 --mode verify-fail
ralphctl --project-dir "$PWD" cp fault-inject --task-id task-1 --mode execute-fail
ralphctl --project-dir "$PWD" cp fault-inject --task-id task-1 --mode permission-denied
```

`cp doctor` 핵심 data integrity 체크:

- `task_json_consistency`
- `event_journal_consistency`
- `learning_journal_consistency`
- `event_ledger_consistency`
- `learning_ledger_consistency`

검증 정책 엔진(`unit|integration|lint|custom`) 사용 예시:

```json
{
  "verify_cmd": "lint: golangci-lint run\nunit: go test ./...\ncustom: ./scripts/verify_extra.sh"
}
```

접두어가 없으면 `custom`으로 처리됩니다.

### 3) KPI baseline 및 canary cutover

```bash
ralphctl --project-dir "$PWD" cp baseline capture --note "v1 baseline before canary"
ralphctl --project-dir "$PWD" cp soak --duration-sec 300 --interval-sec 30 --strict
ralphctl --project-dir "$PWD" cp cutover evaluate --require-baseline=true --require-soak-pass=true --max-soak-age-sec 600 --output .ralph-v2/reports/cutover-evaluate.json
ralphctl --project-dir "$PWD" cp cutover auto --require-baseline=true --require-soak-pass=true --max-soak-age-sec 600 --disable-on-fail=true --output .ralph-v2/reports/cutover-auto.json
# 실패 타입별 롤백 정책 분리
ralphctl --project-dir "$PWD" cp cutover auto --disable-on-fail=true --rollback-on=critical,data_integrity
# auto 평가 전에 repair+recover 선행
ralphctl --project-dir "$PWD" cp cutover auto --pre-repair --pre-repair-force-recover --disable-on-fail=true --rollback-on=all
# pre-repair에서 circuit 창도 초기화
ralphctl --project-dir "$PWD" cp cutover auto --pre-repair --pre-repair-reset-circuit --disable-on-fail=true --rollback-on=all
# 실제 전환 없이 의사결정만 미리 확인
ralphctl --project-dir "$PWD" cp cutover auto --dry-run --rollback-on=all
# JSON으로 바로 확인
ralphctl --project-dir "$PWD" cp cutover auto --dry-run --rollback-on=all --json
# keep-current을 실패로 보지 않고 성공 종료
ralphctl --project-dir "$PWD" cp cutover auto --rollback-on=doctor --allow-keep-current
# canary 통과 후 v2를 기본 모드로 승격
ralphctl --project-dir "$PWD" cp cutover enable-v2 --canary=false --note "promote v2 default"
```

헬퍼 스크립트:

```bash
./scripts/cp_canary.sh "$PWD" 300 30
# 4번째 인자로 soak 최대 허용 age(sec) 지정 가능
./scripts/cp_canary.sh "$PWD" 300 30 600
./scripts/cp_rollback.sh "$PWD" "manual rollback"
./scripts/cp_migrate_verify.sh "$PWD"
```

마이그레이션 리포트(JSON) 저장:

```bash
# dry-run 요약 리포트
ralphctl --project-dir "$PWD" cp migrate-v1 --dry-run=true --output .ralph-v2/reports/migrate-v1-dry-run.json

# 실제 적용 + parity 검증 + 리포트 저장
ralphctl --project-dir "$PWD" cp migrate-v1 --apply --verify --strict-verify --output .ralph-v2/reports/migrate-v1-apply.json

# stdout으로 JSON 출력
ralphctl --project-dir "$PWD" cp migrate-v1 --apply --verify --json
```

참고:

- ledger(`cp_event_ledger`, `cp_learning_ledger`)가 비어 있고 기존 `cp_events`/`cp_learnings` 데이터가 있으면,
  스키마 보장 단계에서 자동 backfill 됩니다.

상태 조회 API 서버:

```bash
ralphctl --project-dir "$PWD" cp api --listen 127.0.0.1:8787
curl -s http://127.0.0.1:8787/health
curl -s http://127.0.0.1:8787/v2/status
curl -s http://127.0.0.1:8787/v2/metrics
curl -s "http://127.0.0.1:8787/v2/metrics?with_baseline=true"
curl -s http://127.0.0.1:8787/v2/metrics/summary
curl -s http://127.0.0.1:8787/v2/doctor
curl -s http://127.0.0.1:8787/v2/cutover
curl -s "http://127.0.0.1:8787/v2/events?limit=50"
curl -N http://127.0.0.1:8787/v2/events/stream
```

## advanced

### 1) 원격/장시간 실행

서비스 등록(systemd/launchd):

```bash
ralphctl --project-dir "$PWD" service install --start
ralphctl --project-dir "$PWD" service status
ralphctl --project-dir "$PWD" service uninstall
```

### 2) 멀티 프로젝트 오케스트레이션

대화형(권장):

```bash
ralphctl fleet
```

명령형 예시:

```bash
ralphctl fleet register --id wallet --project-dir <wallet-project-dir> --plugin universal-default --prd PRD.md
ralphctl fleet start --all
ralphctl fleet status --all
ralphctl fleet stop --all
```

### 3) Telegram 채널 (선택)

텔레그램은 필수 설정이 아닙니다.

필요한 값:

- `bot token`
- `chat-ids`
- `user-ids` (특히 그룹 제어 시 권장/필수)

값 수집 방법 (웹):

1. `@BotFather`에서 봇 생성 후 토큰 발급 (`/newbot`).
2. 봇을 사용할 대화방에 추가하고 메시지 1개 전송.
3. 브라우저에서 아래 URL 열기:

```bash
https://api.telegram.org/bot<bot-token>/getUpdates
```

JSON에서 다음 값을 복사:

- `message.chat.id` -> `chat-ids`
- `message.from.id` -> `user-ids`
- 그룹/슈퍼그룹 `chat.id`는 음수(보통 `-100...`)
- 결과가 비어 있으면 봇/그룹에 메시지를 한 번 더 보내고 새로고침

원격 머신 세팅 시에는 로컬 브라우저에서 값을 확인한 뒤, 서버에서 아래처럼 입력하면 됩니다:

```bash
ralphctl --project-dir "$PWD" telegram setup --non-interactive \
  --token "<bot-token>" \
  --chat-ids "<chat-id>" \
  --user-ids "<user-id>" \
  --allow-control=false \
  --notify=true \
  --notify-scope auto \
  --command-timeout-sec 300 \
  --command-concurrency 4
```

권장:

```bash
ralphctl --project-dir "$PWD" telegram setup
ralphctl --project-dir "$PWD" telegram run
ralphctl --project-dir "$PWD" telegram status
ralphctl --project-dir "$PWD" telegram tail
ralphctl --project-dir "$PWD" telegram stop
```

- `telegram run`은 기본적으로 백그라운드 daemon으로 실행됩니다.
- 터미널을 종료해도 계속 동작합니다.
- 포그라운드로 실행하려면 `telegram run --foreground`를 사용하세요.

비대화형:

```bash
ralphctl --project-dir "$PWD" telegram setup --non-interactive \
  --token "<bot-token>" \
  --chat-ids "<allowed-chat-id-1>,<allowed-chat-id-2>" \
  --user-ids "<allowed-user-id-1>,<allowed-user-id-2>" \
  --allow-control=false \
  --notify=true \
  --notify-scope auto \
  --command-timeout-sec 300 \
  --command-concurrency 4
```

보안 규칙:

- `allow-control=true` + 그룹/슈퍼그룹 chat id(음수) 조합에서는 `user-ids`가 필수입니다.
- 제어 명령을 열 때는 `user-ids` 설정을 권장합니다.
- 기본 정책은 `1 bot = 1 project` 입니다. 같은 bot token을 다른 프로젝트에서 실행하면 차단됩니다.
- bot token을 다른 프로젝트로 이동하려면: `telegram run --rebind-bot`
- telegram offset은 프로젝트별로 자동 분리되어 `~/.ralph-control/telegram-offsets/*.offset`에 저장됩니다.

주요 명령:

- `/status [all|<project_id>]`
- `/doctor [all|<project_id>]`
- `/fleet [all|<project_id>]`
- 평문 메시지: 프로젝트 컨텍스트 Codex 대화 (예: `결제 PRD 초안 만들어줘`)
- `/chat <message>`: Codex 대화를 명시적으로 실행
- `/chat status`, `/chat reset`: Codex 대화 컨텍스트 확인/초기화
- (`--allow-control`일 때) `/start|/stop|/restart|/doctor_repair|/recover|/retry_blocked [all|<project_id>]`
- `/doctor_repair`는 현재 프로젝트 기준으로 `repair + recover + codex blocked 재큐잉 + 필요 시 circuit reset + daemon 자동 시작`까지 한 번에 수행합니다.
- (`--allow-control`일 때) `/new [manager|planner|developer|qa] <title>` (role 생략 시 developer)
- (`--allow-control`일 때) `/task <자연어 요청>` (Codex가 role/title/objective/acceptance를 구조화해 이슈 생성)
- (`--allow-control`일 때) `/prd help` (대화형 PRD wizard + clarity refine)

PRD wizard 빠른 흐름:

1. `/prd start`
2. `/prd refine` (부족한 컨텍스트를 질문으로 채움)
3. 스토리 입력: `title -> description -> role -> priority`
4. `/prd score` 또는 `/prd preview`
5. `/prd apply` (점수 미달이면 `/prd refine` 유도)

대화형 입력 팁:

- refine 중에 질문형 입력(`포함 범위가 뭐야?`)을 보내면 단계를 유지한 채 설명을 반환합니다.
- 추천 요청(`제외 범위 추천해줘`)을 보내면 현재 단계 기준 추천안을 반환합니다.
- refine 입력 의도(`답변/설명/추천`)는 Codex가 우선 판단합니다.
- `/prd score`, `/prd apply`의 게이트 점수는 Codex 점수를 우선 사용합니다(불가 시 heuristic 폴백).
- PRD 세션이 활성화된 채팅에서는 평문 입력이 우선 `/prd` 세션 입력으로 처리됩니다.

### 4) 실행 중 graceful 설정 변경

`profile.local.yaml`을 수정하면 실행 중인 루프가 자동으로 재로딩합니다(다음 loop부터 반영).

자주 쓰는 값:

```yaml
codex_model: auto
codex_home: .codex-home
codex_exec_timeout_sec: 900
codex_retry_max_attempts: 3
codex_retry_backoff_sec: 10
codex_require_exit_signal: true
codex_exit_signal: "EXIT_SIGNAL: DONE"
codex_context_summary_enabled: true
codex_context_summary_lines: 8
codex_circuit_breaker_enabled: true
codex_circuit_breaker_failures: 3
codex_circuit_breaker_cooldown_sec: 120
idle_sleep_sec: 20
inprogress_watchdog_enabled: true
inprogress_watchdog_stale_sec: 1800
inprogress_watchdog_scan_loops: 1
```

`codex_home` 기본값은 프로젝트 로컬 `./.codex-home`입니다.
로그인/설정 파일(`auth.json`, `config.toml`)은 필요 시 자동 시드됩니다.

반영 확인:

```bash
./ralph status
```

`last_profile_reload_at`, `profile_reload_count`가 업데이트됩니다.

Codex 네트워크 문제가 의심되면:

```bash
./ralph doctor
```

`network:dns:chatgpt.com`, `network:codex-api`, `codex-home` 체크를 확인하세요.

주의:

- `supervisor_enabled`, `supervisor_restart_delay_sec` 변경은 daemon 재시작 후 반영됩니다.

### 5) 바이너리 업데이트 후 일괄 반영

새 `ralphctl` 바이너리 설치 후:

```bash
ralphctl reload
```

- 연결된 프로젝트(현재 프로젝트 + fleet 등록 프로젝트)의 `./ralph` wrapper를 새 바이너리로 갱신
- 기존에 실행 중이던 loop/role worker/telegram daemon만 자동 재시작
- 현재 프로젝트만 반영하려면: `ralphctl reload --current-only`

## License

MIT (`LICENSE`)
