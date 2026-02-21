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

주요 명령:

- `/status [all|<project_id>]`
- `/doctor [all|<project_id>]`
- `/fleet [all|<project_id>]`
- 평문 메시지: 프로젝트 컨텍스트 Codex 대화 (예: `결제 PRD 초안 만들어줘`)
- `/chat <message>`: Codex 대화를 명시적으로 실행
- `/chat status`, `/chat reset`: Codex 대화 컨텍스트 확인/초기화
- (`--allow-control`일 때) `/start|/stop|/restart|/doctor_repair|/recover [all|<project_id>]`
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

반영 확인:

```bash
./ralph status
```

`last_profile_reload_at`, `profile_reload_count`가 업데이트됩니다.

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
