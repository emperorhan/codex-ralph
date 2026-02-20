package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"codex-ralph/internal/ralph"
)

func TestParseTelegramCommandLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       string
		wantCmd  string
		wantArgs string
	}{
		{in: "/status", wantCmd: "/status", wantArgs: ""},
		{in: "/status@ralphbot", wantCmd: "/status", wantArgs: ""},
		{in: "status", wantCmd: "/status", wantArgs: ""},
		{in: "/doctor_repair now", wantCmd: "/doctor_repair", wantArgs: "now"},
		{in: "   ", wantCmd: "", wantArgs: ""},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			gotCmd, gotArgs := parseTelegramCommandLine(tt.in)
			if gotCmd != tt.wantCmd || gotArgs != tt.wantArgs {
				t.Fatalf("parseTelegramCommandLine(%q)=(%q,%q) want=(%q,%q)", tt.in, gotCmd, gotArgs, tt.wantCmd, tt.wantArgs)
			}
		})
	}
}

func TestEnvBoolDefault(t *testing.T) {
	t.Setenv("RALPH_TELEGRAM_ALLOW_CONTROL", "true")
	if !envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", false) {
		t.Fatalf("expected true")
	}
	t.Setenv("RALPH_TELEGRAM_ALLOW_CONTROL", "invalid")
	if !envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", true) {
		t.Fatalf("invalid value should fallback to default")
	}
}

func TestEnvIntDefault(t *testing.T) {
	t.Setenv("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", "45")
	if got := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", 30); got != 45 {
		t.Fatalf("envIntDefault mismatch: got=%d want=45", got)
	}
	t.Setenv("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", "invalid")
	if got := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", 30); got != 30 {
		t.Fatalf("envIntDefault invalid fallback mismatch: got=%d want=30", got)
	}
}

func TestParseTelegramTargetSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        string
		wantAll   bool
		wantID    string
		wantError bool
	}{
		{name: "empty", in: "", wantAll: false, wantID: ""},
		{name: "all", in: "all", wantAll: true, wantID: ""},
		{name: "star", in: "*", wantAll: true, wantID: ""},
		{name: "id", in: "wallet", wantAll: false, wantID: "wallet"},
		{name: "invalid multi", in: "all wallet", wantError: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			spec, err := parseTelegramTargetSpec(tt.in)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error for %q", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if spec.All != tt.wantAll || spec.ProjectID != tt.wantID {
				t.Fatalf("parseTelegramTargetSpec(%q)=(all=%t,id=%q) want=(all=%t,id=%q)", tt.in, spec.All, spec.ProjectID, tt.wantAll, tt.wantID)
			}
		})
	}
}

func TestNormalizeNotifyScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: "auto"},
		{in: "auto", want: "auto"},
		{in: "project", want: "project"},
		{in: "fleet", want: "fleet"},
		{in: "invalid", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeNotifyScope(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("normalizeNotifyScope(%q)=%q want=%q", tt.in, got, tt.want)
			}
		})
	}
}

func TestRequiresUserAllowlistForControl(t *testing.T) {
	t.Parallel()

	if requiresUserAllowlistForControl(map[int64]struct{}{12345: {}}) {
		t.Fatalf("private chat should not require user allowlist")
	}
	if !requiresUserAllowlistForControl(map[int64]struct{}{-10012345: {}}) {
		t.Fatalf("group chat should require user allowlist")
	}
	if !requiresUserAllowlistForControl(map[int64]struct{}{12345: {}, -200: {}}) {
		t.Fatalf("mixed chats should require user allowlist")
	}
}

func TestTelegramConfigFileFromArgs(t *testing.T) {
	t.Parallel()

	controlDir := "/tmp/ralph-control"
	if got := telegramConfigFileFromArgs(controlDir, nil); got != filepath.Join(controlDir, "telegram.env") {
		t.Fatalf("default config path mismatch: got=%q", got)
	}
	if got := telegramConfigFileFromArgs(controlDir, []string{"--config-file=/tmp/custom.env"}); got != "/tmp/custom.env" {
		t.Fatalf("inline config path mismatch: got=%q", got)
	}
	if got := telegramConfigFileFromArgs(controlDir, []string{"--config-file", "/tmp/custom2.env"}); got != "/tmp/custom2.env" {
		t.Fatalf("split config path mismatch: got=%q", got)
	}
}

func TestSaveLoadTelegramCLIConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "telegram.env")
	want := telegramCLIConfig{
		Token:                     "123456:ABC-DEF",
		ChatIDs:                   "1001,1002",
		UserIDs:                   "2001,2002",
		AllowControl:              true,
		Notify:                    true,
		NotifyScope:               "fleet",
		NotifyIntervalSec:         45,
		NotifyRetryThreshold:      3,
		NotifyPermStreakThreshold: 5,
		CommandTimeoutSec:         180,
		CommandConcurrency:        6,
	}
	if err := saveTelegramCLIConfig(path, want); err != nil {
		t.Fatalf("saveTelegramCLIConfig failed: %v", err)
	}
	got, err := loadTelegramCLIConfig(path)
	if err != nil {
		t.Fatalf("loadTelegramCLIConfig failed: %v", err)
	}

	if got.Token != want.Token {
		t.Fatalf("token mismatch: got=%q want=%q", got.Token, want.Token)
	}
	if got.ChatIDs != want.ChatIDs {
		t.Fatalf("chat ids mismatch: got=%q want=%q", got.ChatIDs, want.ChatIDs)
	}
	if got.UserIDs != want.UserIDs {
		t.Fatalf("user ids mismatch: got=%q want=%q", got.UserIDs, want.UserIDs)
	}
	if got.AllowControl != want.AllowControl {
		t.Fatalf("allow control mismatch: got=%t want=%t", got.AllowControl, want.AllowControl)
	}
	if got.Notify != want.Notify {
		t.Fatalf("notify mismatch: got=%t want=%t", got.Notify, want.Notify)
	}
	if got.NotifyScope != want.NotifyScope {
		t.Fatalf("notify scope mismatch: got=%q want=%q", got.NotifyScope, want.NotifyScope)
	}
	if got.NotifyIntervalSec != want.NotifyIntervalSec {
		t.Fatalf("notify interval mismatch: got=%d want=%d", got.NotifyIntervalSec, want.NotifyIntervalSec)
	}
	if got.NotifyRetryThreshold != want.NotifyRetryThreshold {
		t.Fatalf("notify retry mismatch: got=%d want=%d", got.NotifyRetryThreshold, want.NotifyRetryThreshold)
	}
	if got.NotifyPermStreakThreshold != want.NotifyPermStreakThreshold {
		t.Fatalf("notify perm mismatch: got=%d want=%d", got.NotifyPermStreakThreshold, want.NotifyPermStreakThreshold)
	}
	if got.CommandTimeoutSec != want.CommandTimeoutSec {
		t.Fatalf("command timeout mismatch: got=%d want=%d", got.CommandTimeoutSec, want.CommandTimeoutSec)
	}
	if got.CommandConcurrency != want.CommandConcurrency {
		t.Fatalf("command concurrency mismatch: got=%d want=%d", got.CommandConcurrency, want.CommandConcurrency)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat telegram config failed: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("telegram config mode mismatch: got=%#o want=%#o", info.Mode().Perm(), 0o600)
	}
}

func TestBuildStatusAlerts(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir:             "/tmp/p",
		Blocked:                1,
		LastFailureUpdatedAt:   "2026-02-20T08:00:00Z",
		LastCodexRetryCount:    0,
		LastBusyWaitDetectedAt: "",
		LastPermissionStreak:   0,
	}
	curr := ralph.Status{
		ProjectDir:             "/tmp/p",
		QueueReady:             1,
		InProgress:             1,
		Blocked:                2,
		LastFailureCause:       "codex_failed_after_3_attempts",
		LastFailureUpdatedAt:   "2026-02-20T08:10:00Z",
		LastCodexRetryCount:    3,
		LastBusyWaitDetectedAt: "2026-02-20T08:11:00Z",
		LastBusyWaitIdleCount:  9,
		LastPermissionStreak:   4,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	if len(alerts) < 4 {
		t.Fatalf("expected multiple alerts, got=%d", len(alerts))
	}
	joined := strings.Join(alerts, "\n")
	if !strings.Contains(joined, "[blocked]") {
		t.Fatalf("missing blocked alert: %q", joined)
	}
	if !strings.Contains(joined, "[retry]") {
		t.Fatalf("missing retry alert: %q", joined)
	}
	if !strings.Contains(joined, "[stuck]") {
		t.Fatalf("missing stuck alert: %q", joined)
	}
	if !strings.Contains(joined, "[permission]") {
		t.Fatalf("missing permission alert: %q", joined)
	}
}

func TestBuildStatusAlertsSkipsStuckWhenNoWork(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir:             "/tmp/p",
		LastBusyWaitDetectedAt: "",
	}
	curr := ralph.Status{
		ProjectDir:             "/tmp/p",
		QueueReady:             0,
		InProgress:             0,
		LastBusyWaitDetectedAt: "2026-02-20T10:00:00Z",
		LastBusyWaitIdleCount:  12,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	joined := strings.Join(alerts, "\n")
	if strings.Contains(joined, "[stuck]") {
		t.Fatalf("stuck alert should be suppressed when queue is empty: %q", joined)
	}
}

func TestBuildStatusAlertsInputRequiredTransition(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir: "/tmp/p",
		QueueReady: 1,
		InProgress: 0,
		Blocked:    0,
	}
	curr := ralph.Status{
		ProjectDir: "/tmp/p",
		QueueReady: 0,
		InProgress: 0,
		Blocked:    0,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	joined := strings.Join(alerts, "\n")
	if !strings.Contains(joined, "[input_required]") {
		t.Fatalf("input_required alert should be emitted on transition: %q", joined)
	}
}

func TestParseTelegramNewIssueArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        string
		wantRole  string
		wantTitle string
		wantErr   bool
	}{
		{
			name:      "default role",
			in:        "health endpoint 구현",
			wantRole:  "developer",
			wantTitle: "health endpoint 구현",
		},
		{
			name:      "explicit role",
			in:        "qa 결제 시나리오 검증",
			wantRole:  "qa",
			wantTitle: "결제 시나리오 검증",
		},
		{
			name:    "missing title with role",
			in:      "planner",
			wantErr: true,
		},
		{
			name:    "empty args",
			in:      "   ",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			role, title, err := parseTelegramNewIssueArgs(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if role != tt.wantRole || title != tt.wantTitle {
				t.Fatalf("parseTelegramNewIssueArgs(%q)=(%q,%q) want=(%q,%q)", tt.in, role, title, tt.wantRole, tt.wantTitle)
			}
		})
	}
}

func TestEnsureTelegramForegroundArg(t *testing.T) {
	t.Parallel()

	original := []string{"--config-file", "/tmp/telegram.env"}
	got := ensureTelegramForegroundArg(original)
	if len(got) != len(original)+1 {
		t.Fatalf("length mismatch: got=%d want=%d", len(got), len(original)+1)
	}
	if got[len(got)-1] != "--foreground" {
		t.Fatalf("last arg mismatch: got=%q want=--foreground", got[len(got)-1])
	}
	if len(original) != 2 {
		t.Fatalf("original slice should not be mutated")
	}
}

func TestTelegramPIDState(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "telegram.pid")

	pid, running, stale := telegramPIDState(pidFile)
	if pid != 0 || running || stale {
		t.Fatalf("missing pid file should be stopped/non-stale: pid=%d running=%t stale=%t", pid, running, stale)
	}

	if err := os.WriteFile(pidFile, []byte("invalid\n"), 0o644); err != nil {
		t.Fatalf("write invalid pid file: %v", err)
	}
	_, running, stale = telegramPIDState(pidFile)
	if running || !stale {
		t.Fatalf("invalid pid file should be stale")
	}

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatalf("write running pid file: %v", err)
	}
	pid, running, stale = telegramPIDState(pidFile)
	if pid != os.Getpid() || !running || stale {
		t.Fatalf("running pid mismatch: pid=%d running=%t stale=%t", pid, running, stale)
	}

	if err := os.WriteFile(pidFile, []byte("999999\n"), 0o644); err != nil {
		t.Fatalf("write stale pid file: %v", err)
	}
	_, running, stale = telegramPIDState(pidFile)
	if running || !stale {
		t.Fatalf("non-running pid file should be stale")
	}
}

func TestParseTelegramPRDStoryRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "developer", want: "developer"},
		{in: "1", want: "manager"},
		{in: "4", want: "qa"},
		{in: "invalid", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := parseTelegramPRDStoryRole(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseTelegramPRDStoryRole(%q)=%q want=%q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseTelegramPRDStoryPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: "", want: telegramPRDDefaultPriority},
		{in: "default", want: telegramPRDDefaultPriority},
		{in: "25", want: 25},
		{in: "0", wantErr: true},
		{in: "x", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := parseTelegramPRDStoryPriority(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseTelegramPRDStoryPriority(%q)=%d want=%d", tt.in, got, tt.want)
			}
		})
	}
}

func TestAdvanceTelegramPRDSessionFlow(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitProduct,
	}
	var err error
	if s, _, err = advanceTelegramPRDSession(s, "Wallet"); err != nil {
		t.Fatalf("set product failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitProblem {
		t.Fatalf("stage mismatch after product: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "결제 실패율이 높다"); err != nil {
		t.Fatalf("set problem failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitGoal {
		t.Fatalf("stage mismatch after problem: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "실패율을 30%% 낮춘다"); err != nil {
		t.Fatalf("set goal failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitInScope {
		t.Fatalf("stage mismatch after goal: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "결제 실패 재시도"); err != nil {
		t.Fatalf("set in-scope failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitOutOfScope {
		t.Fatalf("stage mismatch after in-scope: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "신규 PG 연동 제외"); err != nil {
		t.Fatalf("set out-of-scope failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitAcceptance {
		t.Fatalf("stage mismatch after out-of-scope: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "핵심 시나리오 3개 통과"); err != nil {
		t.Fatalf("set acceptance failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryTitle {
		t.Fatalf("stage mismatch after acceptance: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "결제 API 개선"); err != nil {
		t.Fatalf("set title failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryDesc {
		t.Fatalf("stage mismatch after title: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "사용자 결제 실패율을 줄인다"); err != nil {
		t.Fatalf("set description failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryRole {
		t.Fatalf("stage mismatch after desc: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "developer"); err != nil {
		t.Fatalf("set role failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryPrio {
		t.Fatalf("stage mismatch after role: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(s, "10"); err != nil {
		t.Fatalf("set priority failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryTitle {
		t.Fatalf("stage mismatch after priority: %s", s.Stage)
	}
	if len(s.Stories) != 1 {
		t.Fatalf("story count mismatch: got=%d want=1", len(s.Stories))
	}
	if s.Stories[0].Role != "developer" || s.Stories[0].Priority != 10 {
		t.Fatalf("story fields mismatch: role=%s priority=%d", s.Stories[0].Role, s.Stories[0].Priority)
	}
}

func TestEvaluateTelegramPRDClarityReady(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		ProductName: "Wallet",
		Stories: []telegramPRDStory{
			{
				ID:          "US-001",
				Title:       "결제 실패 복구",
				Description: "실패 시 자동 재시도로 사용자 이탈을 줄인다",
				Role:        "developer",
				Priority:    10,
			},
		},
		Context: telegramPRDContext{
			Problem:    "결제 실패 원인 파악이 느리다",
			Goal:       "실패 재현/복구 시간을 50% 단축한다",
			InScope:    "결제 실패 감지와 재시도 로직",
			OutOfScope: "신규 결제수단 도입",
			Acceptance: "실패 시나리오 3종 자동 복구 및 알림",
		},
	}

	status := evaluateTelegramPRDClarity(s)
	if !status.ReadyToApply {
		t.Fatalf("expected ready, got=%+v", status)
	}
	if status.Score < telegramPRDClarityMinScore {
		t.Fatalf("score should meet gate: got=%d gate=%d", status.Score, telegramPRDClarityMinScore)
	}
}

func TestEvaluateTelegramPRDClarityNeedsInput(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		ProductName: "Wallet",
		Stories: []telegramPRDStory{
			{
				ID:          "US-001",
				Title:       "결제 실패 복구",
				Description: "설명",
				Role:        "developer",
				Priority:    10,
			},
		},
		Context: telegramPRDContext{
			Problem: "",
		},
	}

	status := evaluateTelegramPRDClarity(s)
	if status.ReadyToApply {
		t.Fatalf("status should not be ready")
	}
	if status.NextStage != telegramPRDStageAwaitProblem {
		t.Fatalf("next stage mismatch: got=%s want=%s", status.NextStage, telegramPRDStageAwaitProblem)
	}
}

func TestEvaluateTelegramPRDClarityAssumedValueRequiresRefine(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		ProductName: "Wallet",
		Stories: []telegramPRDStory{
			{
				ID:          "US-001",
				Title:       "결제 실패 복구",
				Description: "설명",
				Role:        "developer",
				Priority:    10,
			},
		},
		Context: telegramPRDContext{
			Problem:    "[assumed] pain point",
			Goal:       "목표",
			InScope:    "범위",
			OutOfScope: "비범위",
			Acceptance: "검증",
		},
	}

	status := evaluateTelegramPRDClarity(s)
	if status.ReadyToApply {
		t.Fatalf("assumed value should keep session below gate")
	}
	if status.NextStage != telegramPRDStageAwaitProblem {
		t.Fatalf("expected first assumed field to be asked again: got=%s", status.NextStage)
	}
}

func TestAdvanceTelegramPRDSessionQuestionDoesNotAdvance(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitInScope,
	}
	updated, reply, err := advanceTelegramPRDSession(s, "포함 범위가 뭐지?")
	if err != nil {
		t.Fatalf("advance failed: %v", err)
	}
	if updated.Stage != telegramPRDStageAwaitInScope {
		t.Fatalf("stage should stay same on question input: got=%s", updated.Stage)
	}
	if !strings.Contains(reply, "in-scope 설명") {
		t.Fatalf("expected help reply for in-scope question, got=%q", reply)
	}
}

func TestIsLikelyTelegramPRDRecommendationRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want bool
	}{
		{in: "추천해줘", want: true},
		{in: "제안 부탁해", want: true},
		{in: "what should be in scope?", want: false},
		{in: "best practice로 알려줘", want: true},
		{in: "그냥 값 입력", want: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got := isLikelyTelegramPRDRecommendationRequest(tt.in)
			if got != tt.want {
				t.Fatalf("isLikelyTelegramPRDRecommendationRequest(%q)=%t want=%t", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseTelegramPRDCodexAssistResponse(t *testing.T) {
	t.Parallel()

	raw := "```json\n{\"intent\":\"recommend\",\"reply\":\"추천 1\\n추천 2\",\"normalized_answer\":\"\"}\n```"
	got, err := parseTelegramPRDCodexAssistResponse(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got.Intent != "recommend" {
		t.Fatalf("intent mismatch: got=%q want=%q", got.Intent, "recommend")
	}
	if !strings.Contains(got.Reply, "추천 1") {
		t.Fatalf("reply mismatch: %q", got.Reply)
	}
}

func TestParseTelegramPRDCodexScoreResponse(t *testing.T) {
	t.Parallel()

	raw := "{\"score\":91,\"ready_to_apply\":true,\"missing\":[\"none\"],\"summary\":\"완성도가 높음\"}"
	got, err := parseTelegramPRDCodexScoreResponse(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got.Score != 91 {
		t.Fatalf("score mismatch: got=%d want=91", got.Score)
	}
	if !got.ReadyToApply {
		t.Fatalf("ready_to_apply mismatch")
	}
	if got.Summary == "" {
		t.Fatalf("summary should not be empty")
	}
}

func TestFormatTelegramPRDCodexScore(t *testing.T) {
	t.Parallel()

	s := telegramPRDSession{
		CodexScore:      85,
		CodexReady:      true,
		CodexMissing:    nil,
		CodexSummary:    "적용 가능",
		CodexScoredAtUT: "2026-02-20T12:00:00Z",
	}
	out := formatTelegramPRDCodexScore(s)
	if !strings.Contains(out, "scoring_mode: codex") {
		t.Fatalf("missing codex scoring mode: %q", out)
	}
	if !strings.Contains(out, "status: ready_to_apply") {
		t.Fatalf("missing ready status: %q", out)
	}
}

func TestTelegramPRDSessionStoreRoundTrip(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	session := telegramPRDSession{
		ChatID:      42,
		Stage:       telegramPRDStageAwaitStoryTitle,
		ProductName: "Wallet",
		Stories: []telegramPRDStory{
			{ID: "US-001", Title: "결제", Description: "설명", Role: "developer", Priority: 10},
		},
	}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		t.Fatalf("upsert session failed: %v", err)
	}
	got, found, err := telegramLoadPRDSession(paths, 42)
	if err != nil {
		t.Fatalf("load session failed: %v", err)
	}
	if !found {
		t.Fatalf("session should exist")
	}
	if got.ProductName != "Wallet" || len(got.Stories) != 1 {
		t.Fatalf("loaded session mismatch: %+v", got)
	}
	if err := telegramDeletePRDSession(paths, 42); err != nil {
		t.Fatalf("delete session failed: %v", err)
	}
	_, found, err = telegramLoadPRDSession(paths, 42)
	if err != nil {
		t.Fatalf("reload after delete failed: %v", err)
	}
	if found {
		t.Fatalf("session should be deleted")
	}
}

func TestWriteTelegramPRDFile(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	target, err := resolveTelegramPRDFilePath(paths, 100, "")
	if err != nil {
		t.Fatalf("resolve prd file path failed: %v", err)
	}
	session := telegramPRDSession{
		ChatID:      100,
		ProductName: "Wallet",
		Context: telegramPRDContext{
			Problem:    "결제 실패율이 높다",
			Goal:       "실패율 감소",
			InScope:    "재시도 로직",
			OutOfScope: "신규 PG",
			Acceptance: "핵심 시나리오 통과",
		},
		Stories: []telegramPRDStory{
			{ID: "US-001", Title: "결제", Description: "설명", Role: "developer", Priority: 10},
		},
	}
	if err := writeTelegramPRDFile(target, session); err != nil {
		t.Fatalf("write prd file failed: %v", err)
	}
	content, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read prd file failed: %v", err)
	}
	if !strings.Contains(string(content), "\"userStories\"") {
		t.Fatalf("prd file should include userStories: %s", string(content))
	}
	if !strings.Contains(string(content), "\"clarity_score\"") {
		t.Fatalf("prd file should include clarity_score metadata: %s", string(content))
	}
	if !strings.Contains(string(content), "\"problem\"") {
		t.Fatalf("prd file should include context metadata: %s", string(content))
	}
}

func TestTelegramPRDConversationTail(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}

	if err := appendTelegramPRDConversation(paths, 99, "user", "첫 질문"); err != nil {
		t.Fatalf("append conversation #1 failed: %v", err)
	}
	if err := appendTelegramPRDConversation(paths, 99, "assistant", "첫 응답"); err != nil {
		t.Fatalf("append conversation #2 failed: %v", err)
	}
	tail := readTelegramPRDConversationTail(paths, 99, 200)
	if !strings.Contains(tail, "첫 질문") || !strings.Contains(tail, "첫 응답") {
		t.Fatalf("conversation tail should contain both entries: %q", tail)
	}
}

func TestTelegramPRDSessionStoreLegacyMigration(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}

	legacyPath := legacyTelegramPRDSessionFile(paths)
	legacyPayload := `{"sessions":{"42":{"chat_id":42,"stage":"await_story_title","product_name":"Legacy Wallet"}}}`
	if err := os.WriteFile(legacyPath, []byte(legacyPayload+"\n"), 0o600); err != nil {
		t.Fatalf("write legacy session file failed: %v", err)
	}

	session, found, err := telegramLoadPRDSession(paths, 42)
	if err != nil {
		t.Fatalf("load with legacy migration failed: %v", err)
	}
	if !found {
		t.Fatalf("legacy session should be loaded")
	}
	if session.ProductName != "Legacy Wallet" {
		t.Fatalf("legacy session content mismatch: %+v", session)
	}
	if _, err := os.Stat(telegramPRDSessionFile(paths)); err != nil {
		t.Fatalf("migrated session file missing: %v", err)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy session file should be removed after migration: %v", err)
	}
}

func TestTelegramPRDSessionLockRecoveryFromStaleInvalidOwner(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}

	lockPath := telegramPRDSessionFile(paths) + ".lock"
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("invalid-owner\n"), 0o600); err != nil {
		t.Fatalf("write lock file: %v", err)
	}
	old := time.Now().Add(-(telegramPRDSessionLockStale + 5*time.Second))
	if err := os.Chtimes(lockPath, old, old); err != nil {
		t.Fatalf("set stale mtime: %v", err)
	}

	session := telegramPRDSession{ChatID: 7, Stage: telegramPRDStageAwaitStoryTitle, ProductName: "lock-recovery"}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		t.Fatalf("upsert with stale lock should recover: %v", err)
	}
	loaded, found, err := telegramLoadPRDSession(paths, 7)
	if err != nil {
		t.Fatalf("load after lock recovery failed: %v", err)
	}
	if !found || loaded.ProductName != "lock-recovery" {
		t.Fatalf("unexpected session after recovery: found=%t session=%+v", found, loaded)
	}
}

func TestBuildTelegramPRDAssistPromptIncludesConversation(t *testing.T) {
	t.Parallel()

	session := telegramPRDSession{
		ChatID:      1,
		Stage:       telegramPRDStageAwaitProblem,
		ProductName: "Ralph",
	}
	prompt := buildTelegramPRDAssistPrompt(session, "문제는 멈춤", "### 2026-02-20T00:00:00Z | user\n이전 입력")
	if !strings.Contains(prompt, "Recent conversation (markdown):") {
		t.Fatalf("assist prompt should include conversation section: %q", prompt)
	}
	if !strings.Contains(prompt, "이전 입력") {
		t.Fatalf("assist prompt should include conversation content: %q", prompt)
	}
}
