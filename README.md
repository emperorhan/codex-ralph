# codex-ralph

[![Test](https://github.com/emperorhan/codex-ralph/actions/workflows/test.yml/badge.svg)](https://github.com/emperorhan/codex-ralph/actions/workflows/test.yml)

`codex-ralph`는 프로젝트 안에서 `manager / planner / developer / qa` 자율 에이전트 루프를 운영하는 CLI입니다.

이 문서는 **랄프 루프 사용자 관점**에서만 설명합니다.

## 테스트 가시성

- CI 테스트 상태: `Test` 워크플로우 배지(상단)
- 커버리지 산출물: 각 `Test` 실행의 artifact(`coverage.out`, `coverage.txt`)
- 로컬 확인:

```bash
go test ./... -covermode=atomic -coverprofile=coverage.out
go tool cover -func=coverage.out
```

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

### 3) 제어 디렉터리 1회 지정

`ralphctl`은 공용 플러그인/설정을 담는 control dir이 필요합니다.

```bash
export RALPH_CONTROL_DIR="$HOME/.ralph-control"
mkdir -p "$RALPH_CONTROL_DIR"
```

### 4) 프로젝트 연결 (권장: setup)

```bash
ralphctl --control-dir "$RALPH_CONTROL_DIR" --project-dir "$PWD" setup
```

- 위 명령을 실행하면 프로젝트 루트에 `./ralph` 실행 헬퍼가 생성됩니다.
- 이후부터는 프로젝트에서 `./ralph ...` 형태로 짧게 사용하면 됩니다.
- 기본 설정은 `.ralph/profile.yaml`(base), `.ralph/profile.local.yaml`(local override)에 저장됩니다.
- `RALPH_*` env 값(`profile.env`, `profile.local.env`, 프로세스 env)은 YAML보다 우선 적용됩니다.

비대화형 기본 세팅:

```bash
ralphctl --control-dir "$RALPH_CONTROL_DIR" --project-dir "$PWD" setup --non-interactive
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
ralphctl --control-dir "$RALPH_CONTROL_DIR" --project-dir "$PWD" install --plugin universal-default
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
./ralph status
./ralph tail
./ralph stop
```

- 기본값으로 `start`는 supervisor 모드로 실행되어 worker 비정상 종료 시 자동 재시작합니다.
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
codex_exec_timeout_sec: 900
codex_retry_max_attempts: 3
codex_retry_backoff_sec: 10
inprogress_watchdog_enabled: true
inprogress_watchdog_stale_sec: 1800
inprogress_watchdog_scan_loops: 1
supervisor_enabled: true
supervisor_restart_delay_sec: 5
```

- `codex_exec_timeout_sec`: codex 1회 실행 타임아웃(초, `0`이면 무제한)
- `codex_retry_max_attempts`: codex 실패 시 최대 재시도 횟수(총 시도 횟수)
- `codex_retry_backoff_sec`: 재시도 기본 대기(지수 백오프)
- `inprogress_watchdog_*`: 오래된 `in-progress` 자동 복구
- `supervisor_*`: 백그라운드 worker 자동 재시작

### 6) 멀티 프로젝트(Fleet)

대화형 관리(권장):

```bash
ralphctl --control-dir "$RALPH_CONTROL_DIR" fleet
```

- 메뉴에서 프로젝트 `register/unregister/start/stop/start all/stop all/status`를 바로 관리할 수 있습니다.
- 신규 프로젝트 등록 시 즉시 시작할지 선택할 수 있습니다.

명령형 관리:

```bash
ralphctl --control-dir "$RALPH_CONTROL_DIR" fleet register \
  --id wallet \
  --project-dir <wallet-project-dir> \
  --plugin universal-default \
  --prd PRD.md

ralphctl --control-dir "$RALPH_CONTROL_DIR" fleet start --all
ralphctl --control-dir "$RALPH_CONTROL_DIR" fleet status --all
ralphctl --control-dir "$RALPH_CONTROL_DIR" fleet stop --all
```

## License

MIT (`LICENSE`)
