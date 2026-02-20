# codex-ralph

`codex-ralph`는 프로젝트 안에서 `manager / planner / developer / qa` 자율 에이전트 루프를 운영하는 CLI입니다.

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
ralphctl --project-dir "$PWD" setup --mode quickstart
```

이후:

- `./ralph` 실행 헬퍼가 생성됩니다.
- 기본 설정 파일:
  - `.ralph/profile.yaml`
  - `.ralph/profile.local.yaml`

### 4) 첫 동작 확인

```bash
./ralph doctor
./ralph start
./ralph status
./ralph stop
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

무중단 운영 preset:

```bash
ralphctl --project-dir "$PWD" setup --mode remote --start
```

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

권장:

```bash
ralphctl --project-dir "$PWD" telegram setup
ralphctl --project-dir "$PWD" telegram run
```

비대화형:

```bash
ralphctl --project-dir "$PWD" telegram setup --non-interactive \
  --token "<bot-token>" \
  --chat-ids "<allowed-chat-id-1>,<allowed-chat-id-2>" \
  --user-ids "<allowed-user-id-1>,<allowed-user-id-2>" \
  --allow-control=false \
  --notify=true \
  --notify-scope auto
```

보안 규칙:

- `allow-control=true` + 그룹/슈퍼그룹 chat id(음수) 조합에서는 `user-ids`가 필수입니다.
- 제어 명령을 열 때는 `user-ids` 설정을 권장합니다.

주요 명령:

- `/status [all|<project_id>]`
- `/doctor [all|<project_id>]`
- `/fleet [all|<project_id>]`
- (`--allow-control`일 때) `/start|/stop|/restart|/doctor_repair|/recover [all|<project_id>]`

### 4) 안정성 최소 튜닝

`profile.local.yaml` 예시:

```yaml
codex_model: auto
codex_exec_timeout_sec: 900
codex_retry_max_attempts: 3
codex_retry_backoff_sec: 10
inprogress_watchdog_enabled: true
supervisor_enabled: true
```

## License

MIT (`LICENSE`)
