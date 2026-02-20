# codex-ralph

`codex-ralph`는 프로젝트 안에서 `manager / planner / developer / qa` 자율 에이전트 루프를 운영하는 CLI입니다.

## 세팅

### 0) Codex 연결 확인

`ralph` 루프는 내부적으로 `codex exec`를 호출하므로, 먼저 로그인 상태를 확인하세요.

```bash
codex --version
codex login status
```

로그인이 안 되어 있으면:

```bash
codex login
```

API 키로 로그인하려면:

```bash
printenv OPENAI_API_KEY | codex login --with-api-key
```

### 1) GitHub Releases 바이너리 설치 (권장)

macOS/Ubuntu 공통:

```bash
curl -fsSL https://raw.githubusercontent.com/emperorhan/codex-ralph/main/scripts/install-ralphctl.sh | bash
```

특정 버전 설치:

```bash
curl -fsSL https://raw.githubusercontent.com/emperorhan/codex-ralph/main/scripts/install-ralphctl.sh | \
  RALPH_VERSION=v0.1.0 bash
```

설치 경로 지정:

```bash
curl -fsSL https://raw.githubusercontent.com/emperorhan/codex-ralph/main/scripts/install-ralphctl.sh | \
  RALPH_INSTALL_DIR=/usr/local/bin bash
```

검증:

```bash
ralphctl --help
```

설치 스크립트는 release checksum(`checksums.txt`)을 자동 검증합니다.

릴리즈에 `checksums.txt.sig`, `cosign.pub`가 포함되어 있고 로컬에 `cosign`이 있으면 서명 검증도 자동으로 수행합니다.

### 릴리즈 발행 (maintainer)

`v*` 태그를 push하면 GitHub Actions가 아래 아티팩트를 자동 업로드합니다.

- `darwin-amd64`
- `darwin-arm64`
- `linux-amd64`
- `linux-arm64`
- `checksums.txt`

```bash
git tag v0.1.0
git push origin v0.1.0
```

선택 사항: `COSIGN_PRIVATE_KEY`, `COSIGN_PASSWORD` 시크릿을 설정하면 `checksums.txt.sig`, `cosign.pub`도 함께 배포됩니다.

### 3) Control Dir 기본값

- 기본 control dir: `~/.ralph-control`
- 첫 실행 시 기본 plugin/registry가 자동으로 준비됩니다.
- 다른 경로를 쓰고 싶으면 `--control-dir <DIR>`로 override 하세요.

### 4) 프로젝트 연결 (권장: setup)

```bash
ralphctl --project-dir "$PWD" setup
```

- 위 명령을 실행하면 프로젝트 루트에 `./ralph` 실행 헬퍼가 생성됩니다.
- 이후부터는 프로젝트에서 `./ralph ...` 형태로 짧게 사용하면 됩니다.
- 기본 설정은 `.ralph/profile.yaml`(base), `.ralph/profile.local.yaml`(local override)에 저장됩니다.
- `RALPH_*` env 값(`profile.env`, `profile.local.env`, 프로세스 env)은 YAML보다 우선 적용됩니다.

빠른 시작(질문 없이 기본값 적용):

```bash
ralphctl --project-dir "$PWD" setup --mode quickstart
```

원격/장시간 운영 preset(무중단 튜닝 기본값 적용):

```bash
ralphctl --project-dir "$PWD" setup --mode remote
# 원샷 온보딩(세팅 + 즉시 데몬 시작)
ralphctl --project-dir "$PWD" setup --mode remote --start
```

비대화형 기본 세팅:

```bash
ralphctl --project-dir "$PWD" setup --non-interactive
```

`setup`에서 묻는 항목:
- plugin 선택
- role rules/handoff 활성화 여부
- handoff schema(`universal`/`strict`)
- busy-wait 시 자동 doctor 복구 여부
- validation 모드(plugin default/skip/custom)

### 5) 플러그인 선택 가이드

- `universal-default`: 언어/프레임워크 무관 기본값 (권장 시작점)
- `go-default`: Go 프로젝트 기본 검증 커맨드 포함
- `node-default`: Node/TypeScript 프로젝트 기본 검증 커맨드 포함

대화형 `setup`에서 선택하거나, 직접 지정할 수 있습니다.

```bash
ralphctl --project-dir "$PWD" install --plugin universal-default
```

### 6) 세팅 점검

```bash
cd <project-dir>
./ralph doctor
./ralph status
```

`doctor` 출력에서 `auth:codex`가 `pass`인지 확인하세요.

## 활용방법

### 1) 작업 투입

수동 이슈 생성:

```bash
./ralph new developer "health endpoint 구현"
./ralph new --priority 10 --story-id US-001 developer "결제 API 에러 처리 개선"
```

PRD JSON에서 일괄 생성:

```bash
./ralph import-prd --file prd.json --default-role developer
./ralph import-prd --file prd.json --dry-run
```

### 2) 루프 실행

데몬 실행:

```bash
./ralph start
# start 전 doctor 복구 + 권한 보정
./ralph start --doctor-repair --fix-perms
./ralph status
./ralph tail
./ralph stop
```

- 기본값으로 `start`는 supervisor 모드로 실행되어 worker 비정상 종료 시 자동 재시작합니다.
- 기본값으로 `start`는 실행 전에 `doctor --repair` preflight를 수행합니다(`--doctor-repair=false`로 비활성화).
- 권한 이슈가 잦은 환경에서는 `--fix-perms`로 `.ralph`/control-dir 권한을 정규화할 수 있습니다.
- 비활성화하려면 `supervisor_enabled: false`를 설정하세요.

1회/지정 루프 실행:

```bash
./ralph run --max-loops 1
./ralph run --max-loops 0
./ralph run --max-loops 0 --roles manager
./ralph run --max-loops 0 --roles developer,qa
```

### 3) 결과 확인

```bash
./ralph status
./ralph doctor
```

- `status`에서 `last_profile_reload_at`, `profile_reload_count`로 실행 중 설정 반영 시점을 확인할 수 있습니다.

주요 산출물 위치:

- `.ralph/done/`: 완료된 이슈
- `.ralph/blocked/`: 실패/검토 필요 이슈
- `.ralph/logs/`: 실행 로그
- `.ralph/reports/progress-journal.log`: 처리 이력
- `.ralph/reports/handoffs/*.json`: role handoff 결과

## advanced

### 0) 안전하게 운영하기

- 기본은 `universal-default`로 시작하고, 프로젝트별로 검증 명령만 명시적으로 설정하세요.
- 루프 시작 전 `./ralph doctor`를 실행해 `auth:codex`, `command:codex` 상태를 확인하세요.
- 장시간 실행 중에는 주기적으로 `./ralph doctor --repair`를 실행해 stale pid/큐 이상 상태를 정리하세요.
- 대규모 실험 전에는 `./ralph run --max-loops 1`로 먼저 단건 검증 후 데몬 모드로 전환하세요.

### 1) role 규칙 커스터마이징

각 role worker는 아래 파일을 프롬프트에 주입받습니다.

- `.ralph/rules/common.md`
- `.ralph/rules/manager.md`
- `.ralph/rules/planner.md`
- `.ralph/rules/developer.md`
- `.ralph/rules/qa.md`

프로젝트 특성에 맞춰 직접 수정하세요.

### 2) handoff 정책

`profile.local.yaml`에서 제어:

```yaml
role_rules_enabled: true
handoff_required: true
handoff_schema: universal
busywait_doctor_repair_enabled: true
# optional: handoff_schema: strict
```

- `universal`: 어떤 프로젝트에도 적용 가능한 최소 공통 스키마
- `strict`: role별 상세 스키마 강제

### 3) 검증 명령 커스터마이징

```yaml
validate_cmd: "make test && make lint"
```

`setup`에서 `custom command`를 선택해 설정해도 됩니다.

### 4) 운영 안정화

```bash
./ralph doctor --strict
./ralph doctor --repair
./ralph recover
```

- `doctor --repair`: 안전한 자동 복구(룰 파일 보정, stale pid 정리, in-progress 복구)
- `recover`: in-progress 이슈를 ready로 복구
- busy-wait 감지 시에는 내부적으로 `doctor --repair` 동등 복구가 자동 실행됩니다(기본값).
- `doctor`에는 `security:*` 점검(샌드박스/승인 정책/검증 명령/권한)과 `plugin-registry` 무결성 점검이 포함됩니다.
- 루프 시작 시 `project/control/issues/in-progress/done/blocked/logs` 쓰기 권한 preflight를 수행해 권한 문제를 조기 감지합니다.
- 처리 중 권한 에러가 반복되면 지수 백오프(+busywait event 기록)로 과도한 재시도를 방지합니다.

자동 복구 on/off:

```yaml
busywait_doctor_repair_enabled: true
```

env override 예시:

```bash
export RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED=false
```

### 5) 안정성 튜닝

`profile.local.yaml`에서 timeout/retry/watchdog/supervisor를 조절할 수 있습니다.

```yaml
codex_model: auto
# optional: role별 override
codex_model_developer: gpt-5.3-codex-spark
codex_exec_timeout_sec: 900
codex_retry_max_attempts: 3
codex_retry_backoff_sec: 10
codex_skip_git_repo_check: true
codex_output_last_message_enabled: true
inprogress_watchdog_enabled: true
inprogress_watchdog_stale_sec: 1800
inprogress_watchdog_scan_loops: 1
supervisor_enabled: true
supervisor_restart_delay_sec: 5
```

- `codex_exec_timeout_sec`: codex 1회 실행 타임아웃(초, `0`이면 무제한)
- `codex_retry_max_attempts`: codex 실패 시 최대 재시도 횟수(총 시도 횟수)
- `codex_retry_backoff_sec`: 재시도 기본 대기(지수 백오프)
- `codex_skip_git_repo_check`: git 저장소가 아닌 디렉터리에서도 codex 실행 허용
- `codex_output_last_message_enabled`: codex 마지막 응답을 `.ralph/logs/*.last.txt`로 저장
- `codex_model`: `auto`면 Codex CLI 기본 모델 사용(업그레이드 시 자동 추종)
- `codex_model_*`: role별 모델 override (`manager/planner/developer/qa`), 비어있으면 `codex_model` 사용
- `inprogress_watchdog_*`: 오래된 `in-progress` 자동 복구
- `supervisor_*`: 백그라운드 worker 자동 재시작
- 루프 실행 중 `profile*.yaml/.env`를 변경하면 재시작 없이 다음 이슈부터 반영됩니다.

env로 일시 override:

```bash
export RALPH_CODEX_MODEL_DEVELOPER=gpt-5.3-codex-spark
```

### 6) 멀티 프로젝트(Fleet)

대화형 관리(권장):

```bash
ralphctl fleet
```

- 메뉴에서 프로젝트 `register/unregister/start/stop/start all/stop all/status`를 바로 관리할 수 있습니다.
- 신규 프로젝트 등록 시 즉시 시작할지 선택할 수 있습니다.

명령형 관리:

```bash
ralphctl fleet register \
  --id wallet \
  --project-dir <wallet-project-dir> \
  --plugin universal-default \
  --prd PRD.md

ralphctl fleet start --all
ralphctl fleet status --all
ralphctl fleet stop --all
ralphctl fleet dashboard --all
ralphctl fleet dashboard --all --watch --interval-sec 5
```

- `fleet dashboard --watch`에는 `last_failure`, `codex_retries`, `perm_streak`가 함께 표시됩니다.

### 7) 서비스 자동기동(systemd/launchd)

```bash
ralphctl --project-dir "$PWD" service install --start
ralphctl --project-dir "$PWD" service status
ralphctl --project-dir "$PWD" service uninstall
```

### 8) Telegram Bot 채널(선택)

텔레그램은 필수 설정이 아닙니다. 필요할 때만 설정해서 사용하면 됩니다.

권장: 1회 setup 후 run

```bash
# 대화형 설정(토큰/chat-id/알림/제어권한)
ralphctl --project-dir "$PWD" telegram setup

# 실행(기본 설정 파일 자동 사용)
ralphctl --project-dir "$PWD" telegram run
```

비대화형 설정:

```bash
ralphctl --project-dir "$PWD" telegram setup --non-interactive \
  --token "<bot-token>" \
  --chat-ids "<allowed-chat-id-1>,<allowed-chat-id-2>" \
  --allow-control=false \
  --notify=true
```

`telegram setup`은 기본적으로 `<control-dir>/telegram.env`에 저장됩니다. `run`은 같은 파일을 자동으로 읽고, 환경변수/flag로 override할 수 있습니다.

실행 시 override 예시:

```bash
# 알림만 끄기
ralphctl --project-dir "$PWD" telegram run --notify=false

# 제어 명령 허용(/start, /stop, /restart, /doctor_repair, /recover)
ralphctl --project-dir "$PWD" telegram run --allow-control
```

기본 알림(`--notify=true`):

- `blocked` 증가 감지
- `codex_retries` 임계치 초과(기본 `2`)
- `busy-wait(stuck)` 감지
- `permission streak` 임계치 초과(기본 `3`)

알림 튜닝:

```bash
ralphctl --project-dir "$PWD" telegram run \
  --notify-interval-sec 30 \
  --notify-retry-threshold 3 \
  --notify-perm-streak-threshold 4
```

지원 명령:

- `/help`, `/ping`
- `/status`, `/doctor`, `/fleet`
- (`--allow-control`일 때) `/start`, `/stop`, `/restart`, `/doctor_repair`, `/recover`

### 9) Plugin Registry(무결성)

manifest 생성/검증:

```bash
ralphctl registry generate
ralphctl registry list
ralphctl registry verify
```

- `plugins/registry.json`의 SHA256으로 plugin 파일 무결성을 검사합니다.
- registry가 존재하면 `install/apply-plugin/setup` 시 선택 plugin이 registry 검증을 통과해야 적용됩니다.

## License

MIT (`LICENSE`)
