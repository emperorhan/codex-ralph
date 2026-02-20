package main

import (
	"fmt"
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
	oldRefine := telegramPRDRefineAnalyzer
	t.Cleanup(func() { telegramPRDRefineAnalyzer = oldRefine })
	telegramPRDRefineAnalyzer = func(_ ralph.Paths, s telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
		status := evaluateTelegramPRDClarity(s)
		return telegramPRDCodexRefineResponse{
			Score:          status.Score,
			ReadyToApply:   status.ReadyToApply,
			Ask:            "test question",
			Missing:        status.Missing,
			SuggestedStage: status.NextStage,
			Reason:         "test refine",
		}, nil
	}

	s := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitProduct,
	}
	var err error
	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "Wallet"); err != nil {
		t.Fatalf("set product failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitProblem {
		t.Fatalf("stage mismatch after product: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "결제 실패율이 높다"); err != nil {
		t.Fatalf("set problem failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitGoal {
		t.Fatalf("stage mismatch after problem: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "실패율을 30%% 낮춘다"); err != nil {
		t.Fatalf("set goal failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitInScope {
		t.Fatalf("stage mismatch after goal: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "결제 실패 재시도"); err != nil {
		t.Fatalf("set in-scope failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitOutOfScope {
		t.Fatalf("stage mismatch after in-scope: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "신규 PG 연동 제외"); err != nil {
		t.Fatalf("set out-of-scope failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitAcceptance {
		t.Fatalf("stage mismatch after out-of-scope: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "핵심 시나리오 3개 통과"); err != nil {
		t.Fatalf("set acceptance failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryTitle {
		t.Fatalf("stage mismatch after acceptance: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "결제 API 개선"); err != nil {
		t.Fatalf("set title failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryDesc {
		t.Fatalf("stage mismatch after title: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "사용자 결제 실패율을 줄인다"); err != nil {
		t.Fatalf("set description failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryRole {
		t.Fatalf("stage mismatch after desc: %s", s.Stage)
	}

	if s, _, err = advanceTelegramPRDSession(ralph.Paths{}, s, "developer 10"); err != nil {
		t.Fatalf("set role failed: %v", err)
	}
	if s.Stage != telegramPRDStageAwaitStoryTitle {
		t.Fatalf("stage mismatch after role add: %s", s.Stage)
	}
	if len(s.Stories) != 1 {
		t.Fatalf("story count mismatch: got=%d want=1", len(s.Stories))
	}
	if s.Stories[0].Role != "developer" || s.Stories[0].Priority != 10 {
		t.Fatalf("story fields mismatch: role=%s priority=%d", s.Stories[0].Role, s.Stories[0].Priority)
	}
}

func TestParseTelegramPRDStoryRoleAndPriorityInput(t *testing.T) {
	t.Parallel()

	session := telegramPRDSession{
		Context: telegramPRDContext{
			AgentPriority: map[string]int{
				"manager":   910,
				"planner":   920,
				"developer": 930,
				"qa":        940,
			},
		},
	}

	role, priority, explicit, err := parseTelegramPRDStoryRoleAndPriorityInput(session, "developer", "")
	if err != nil {
		t.Fatalf("parse role only failed: %v", err)
	}
	if role != "developer" || priority != 0 || explicit {
		t.Fatalf("role-only parse mismatch: role=%s priority=%d explicit=%t", role, priority, explicit)
	}

	role, priority, explicit, err = parseTelegramPRDStoryRoleAndPriorityInput(session, "qa 777", "")
	if err != nil {
		t.Fatalf("parse role+priority failed: %v", err)
	}
	if role != "qa" || priority != 777 || !explicit {
		t.Fatalf("role+priority parse mismatch: role=%s priority=%d explicit=%t", role, priority, explicit)
	}

	role, priority, explicit, err = parseTelegramPRDStoryRoleAndPriorityInput(session, "manager", "default")
	if err != nil {
		t.Fatalf("parse explicit default failed: %v", err)
	}
	if role != "manager" || priority != 0 || explicit {
		t.Fatalf("explicit default parse mismatch: role=%s priority=%d explicit=%t", role, priority, explicit)
	}
}

func TestParseTelegramPRDQuickStoryInput(t *testing.T) {
	t.Parallel()

	session := telegramPRDSession{
		Context: telegramPRDContext{
			AgentPriority: map[string]int{
				"developer": 1200,
			},
		},
	}

	story, quick, err := parseTelegramPRDQuickStoryInput(session, "결제 실패 자동 복구 | 실패시 재시도와 알림 | developer")
	if err != nil {
		t.Fatalf("quick parse failed: %v", err)
	}
	if !quick {
		t.Fatalf("quick flag should be true")
	}
	if story.Role != "developer" || story.Priority != 0 {
		t.Fatalf("quick parse role/priority mismatch: role=%s priority=%d", story.Role, story.Priority)
	}

	story, quick, err = parseTelegramPRDQuickStoryInput(session, "알림 개선 | 상태 가시성 강화 | qa | 555")
	if err != nil {
		t.Fatalf("quick parse with explicit priority failed: %v", err)
	}
	if !quick {
		t.Fatalf("quick flag should be true")
	}
	if story.Role != "qa" || story.Priority != 555 {
		t.Fatalf("quick parse explicit priority mismatch: role=%s priority=%d", story.Role, story.Priority)
	}
}

func TestParseTelegramPRDAgentPriorityArgs(t *testing.T) {
	t.Parallel()

	got, err := parseTelegramPRDAgentPriorityArgs("manager=900 planner:950 developer=1000 qa=1100")
	if err != nil {
		t.Fatalf("parse agent priority failed: %v", err)
	}
	if got["manager"] != 900 || got["planner"] != 950 || got["developer"] != 1000 || got["qa"] != 1100 {
		t.Fatalf("agent priority parse mismatch: %+v", got)
	}

	if _, err := parseTelegramPRDAgentPriorityArgs("invalid=1"); err == nil {
		t.Fatalf("invalid role should fail")
	}
	if _, err := parseTelegramPRDAgentPriorityArgs("developer=0"); err == nil {
		t.Fatalf("non-positive priority should fail")
	}
}

func TestResolveTelegramPRDStoryPriorityUsesCodexEstimator(t *testing.T) {
	old := telegramPRDStoryPriorityEstimator
	t.Cleanup(func() { telegramPRDStoryPriorityEstimator = old })
	telegramPRDStoryPriorityEstimator = func(_ ralph.Paths, _ telegramPRDSession, _ telegramPRDStory) (int, string, error) {
		return 777, "codex_auto", nil
	}

	session := telegramPRDSession{
		Context: telegramPRDContext{
			AgentPriority: map[string]int{
				"developer": 1000,
			},
		},
	}
	story := telegramPRDStory{Role: "developer"}
	priority, source := resolveTelegramPRDStoryPriority(ralph.Paths{}, session, story)
	if priority != 777 || source != "codex_auto" {
		t.Fatalf("priority resolve mismatch: priority=%d source=%s", priority, source)
	}
}

func TestResolveTelegramPRDStoryPriorityFallsBackOnEstimatorError(t *testing.T) {
	old := telegramPRDStoryPriorityEstimator
	t.Cleanup(func() { telegramPRDStoryPriorityEstimator = old })
	telegramPRDStoryPriorityEstimator = func(_ ralph.Paths, _ telegramPRDSession, _ telegramPRDStory) (int, string, error) {
		return 0, "", fmt.Errorf("codex unavailable")
	}

	session := telegramPRDSession{
		Context: telegramPRDContext{
			AgentPriority: map[string]int{
				"developer": 1234,
			},
		},
	}
	story := telegramPRDStory{Role: "developer"}
	priority, source := resolveTelegramPRDStoryPriority(ralph.Paths{}, session, story)
	if priority != 1234 || source != "fallback_role_profile" {
		t.Fatalf("fallback resolve mismatch: priority=%d source=%s", priority, source)
	}
}

func TestAdvanceTelegramPRDSessionRoleWithoutPriorityUsesEstimator(t *testing.T) {
	old := telegramPRDStoryPriorityEstimator
	t.Cleanup(func() { telegramPRDStoryPriorityEstimator = old })
	telegramPRDStoryPriorityEstimator = func(_ ralph.Paths, _ telegramPRDSession, _ telegramPRDStory) (int, string, error) {
		return 888, "codex_auto", nil
	}

	s := telegramPRDSession{
		ChatID:      1,
		Stage:       telegramPRDStageAwaitStoryRole,
		ProductName: "Wallet",
		DraftTitle:  "결제 실패 자동 복구",
		DraftDesc:   "실패 시 자동 재시도와 알림",
		Context: telegramPRDContext{
			Problem:    "실패율 높음",
			Goal:       "복구 시간 단축",
			InScope:    "재시도/알림",
			OutOfScope: "신규 PG",
			Acceptance: "핵심 시나리오 통과",
		},
	}
	updated, reply, err := advanceTelegramPRDSession(ralph.Paths{}, s, "developer")
	if err != nil {
		t.Fatalf("advance failed: %v", err)
	}
	if updated.Stage != telegramPRDStageAwaitStoryTitle {
		t.Fatalf("stage should return to title: %s", updated.Stage)
	}
	if len(updated.Stories) != 1 || updated.Stories[0].Priority != 888 {
		t.Fatalf("story priority should come from estimator: %+v", updated.Stories)
	}
	if !strings.Contains(reply, "priority_source: codex_auto") {
		t.Fatalf("reply should include codex priority source: %q", reply)
	}
}

func TestParseTelegramPRDCodexStoryPriorityResponse(t *testing.T) {
	t.Parallel()

	raw := "```json\n{\"priority\":95,\"reason\":\"운영 영향도가 높음\"}\n```"
	parsed, err := parseTelegramPRDCodexStoryPriorityResponse(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if parsed.Priority != 100 {
		t.Fatalf("priority should be clamped to minimum 100: %d", parsed.Priority)
	}
	if parsed.Reason == "" {
		t.Fatalf("reason should not be empty")
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

func TestAdvanceTelegramPRDSessionQuestionInputAdvancesWithoutAssist(t *testing.T) {
	t.Parallel()
	oldRefine := telegramPRDRefineAnalyzer
	t.Cleanup(func() { telegramPRDRefineAnalyzer = oldRefine })
	telegramPRDRefineAnalyzer = func(_ ralph.Paths, s telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
		status := evaluateTelegramPRDClarity(s)
		return telegramPRDCodexRefineResponse{
			Score:          status.Score,
			ReadyToApply:   status.ReadyToApply,
			Ask:            "test question",
			Missing:        status.Missing,
			SuggestedStage: status.NextStage,
			Reason:         "test refine",
		}, nil
	}

	s := telegramPRDSession{
		ChatID:      1,
		Stage:       telegramPRDStageAwaitInScope,
		ProductName: "Ralph",
		Context: telegramPRDContext{
			Problem: "문제",
			Goal:    "목표",
		},
	}
	updated, reply, err := advanceTelegramPRDSession(ralph.Paths{}, s, "포함 범위가 뭐지?")
	if err != nil {
		t.Fatalf("advance failed: %v", err)
	}
	if updated.Stage == telegramPRDStageAwaitInScope {
		t.Fatalf("stage should advance once value is submitted: got=%s", updated.Stage)
	}
	if strings.TrimSpace(updated.Context.InScope) != "포함 범위가 뭐지?" {
		t.Fatalf("in-scope should keep raw input when assist is bypassed: %q", updated.Context.InScope)
	}
	if !strings.Contains(reply, "prd refine question") {
		t.Fatalf("expected refine reply, got=%q", reply)
	}
}

func TestTelegramPRDAssistInputUsesCodexRecommendForStoryTitle(t *testing.T) {
	old := telegramPRDCodexAssistAnalyzer
	t.Cleanup(func() { telegramPRDCodexAssistAnalyzer = old })
	telegramPRDCodexAssistAnalyzer = func(_ ralph.Paths, _ telegramPRDSession, _ string) (telegramPRDCodexAssistResponse, error) {
		return telegramPRDCodexAssistResponse{
			Intent: "recommend",
			Reply:  "story title 추천\n- 결제 실패 자동 복구",
		}, nil
	}

	session := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitStoryTitle,
	}
	assist, err := telegramPRDAssistInput(ralph.Paths{}, session, "추천해줘")
	if err != nil {
		t.Fatalf("assist failed: %v", err)
	}
	if !assist.Handled {
		t.Fatalf("recommend intent should be handled")
	}
	if !strings.Contains(assist.Reply, "story title 추천") {
		t.Fatalf("unexpected assist reply: %q", assist.Reply)
	}
	if assist.InputOverride != "" {
		t.Fatalf("recommend intent should not override input: %q", assist.InputOverride)
	}
}

func TestTelegramPRDAssistInputUsesCodexNormalizedAnswer(t *testing.T) {
	old := telegramPRDCodexAssistAnalyzer
	t.Cleanup(func() { telegramPRDCodexAssistAnalyzer = old })
	telegramPRDCodexAssistAnalyzer = func(_ ralph.Paths, _ telegramPRDSession, _ string) (telegramPRDCodexAssistResponse, error) {
		return telegramPRDCodexAssistResponse{
			Intent:           "answer",
			NormalizedAnswer: "결제 실패 자동 복구",
		}, nil
	}

	session := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitStoryTitle,
	}
	assist, err := telegramPRDAssistInput(ralph.Paths{}, session, "제목은 결제 실패 자동 복구")
	if err != nil {
		t.Fatalf("assist failed: %v", err)
	}
	if assist.Handled {
		t.Fatalf("answer intent should not be handled as reply")
	}
	if assist.InputOverride != "결제 실패 자동 복구" {
		t.Fatalf("normalized answer should be used as override: %q", assist.InputOverride)
	}
}

func TestTelegramPRDAssistInputFallbackReplyWhenCodexReplyEmpty(t *testing.T) {
	old := telegramPRDCodexAssistAnalyzer
	t.Cleanup(func() { telegramPRDCodexAssistAnalyzer = old })
	telegramPRDCodexAssistAnalyzer = func(_ ralph.Paths, _ telegramPRDSession, _ string) (telegramPRDCodexAssistResponse, error) {
		return telegramPRDCodexAssistResponse{
			Intent: "clarify",
			Reply:  "",
		}, nil
	}

	session := telegramPRDSession{
		ChatID: 1,
		Stage:  telegramPRDStageAwaitStoryRole,
	}
	assist, err := telegramPRDAssistInput(ralph.Paths{}, session, "무슨 role?")
	if err != nil {
		t.Fatalf("assist failed: %v", err)
	}
	if !assist.Handled {
		t.Fatalf("clarify intent should be handled")
	}
	if !strings.Contains(assist.Reply, "manager|planner|developer|qa") {
		t.Fatalf("fallback guide should include role options: %q", assist.Reply)
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

func TestParseTelegramPRDCodexRefineResponse(t *testing.T) {
	t.Parallel()

	raw := "```json\n{\"score\":72,\"ready_to_apply\":false,\"ask\":\"핵심 성공 지표를 한 줄로 써주세요\",\"missing\":[\"success metric\"],\"suggested_stage\":\"await_goal\",\"reason\":\"목표 정량화가 부족\"}\n```"
	got, err := parseTelegramPRDCodexRefineResponse(raw)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if got.Score != 72 {
		t.Fatalf("score mismatch: got=%d want=72", got.Score)
	}
	if got.ReadyToApply {
		t.Fatalf("ready_to_apply should be false")
	}
	if got.Ask == "" || got.SuggestedStage != telegramPRDStageAwaitGoal {
		t.Fatalf("parsed refine response mismatch: %+v", got)
	}
}

func TestTelegramPRDRefineSessionUsesCodexDynamicQuestion(t *testing.T) {
	old := telegramPRDRefineAnalyzer
	t.Cleanup(func() { telegramPRDRefineAnalyzer = old })
	telegramPRDRefineAnalyzer = func(_ ralph.Paths, _ telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
		return telegramPRDCodexRefineResponse{
			Score:          68,
			ReadyToApply:   false,
			Ask:            "이번 배포에서 반드시 만족해야 할 성공 지표를 한 줄로 입력하세요",
			Missing:        []string{"success metric"},
			SuggestedStage: telegramPRDStageAwaitGoal,
			Reason:         "goal이 정량화되지 않아 우선 보강 필요",
		}, nil
	}

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
		ChatID:      77,
		Stage:       telegramPRDStageAwaitProblem,
		ProductName: "Wallet",
		Context: telegramPRDContext{
			Problem: "실패율이 높다",
		},
	}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		t.Fatalf("upsert session failed: %v", err)
	}

	reply, err := telegramPRDRefineSession(paths, 77)
	if err != nil {
		t.Fatalf("refine session failed: %v", err)
	}
	if !strings.Contains(reply, "scoring_mode: codex") {
		t.Fatalf("refine reply should use codex scoring mode: %q", reply)
	}
	if !strings.Contains(reply, "성공 지표") {
		t.Fatalf("refine reply should contain codex ask question: %q", reply)
	}

	updated, found, err := telegramLoadPRDSession(paths, 77)
	if err != nil {
		t.Fatalf("load updated session failed: %v", err)
	}
	if !found {
		t.Fatalf("updated session not found")
	}
	if updated.Stage != telegramPRDStageAwaitGoal {
		t.Fatalf("session stage should follow codex suggested_stage: %s", updated.Stage)
	}
	if updated.CodexScore != 68 {
		t.Fatalf("codex score should be stored: %d", updated.CodexScore)
	}
}

func TestTelegramPRDRefineSessionCodexUnavailableNoHeuristicQuestion(t *testing.T) {
	oldRefine := telegramPRDRefineAnalyzer
	oldScore := telegramPRDScoreAnalyzer
	t.Cleanup(func() {
		telegramPRDRefineAnalyzer = oldRefine
		telegramPRDScoreAnalyzer = oldScore
	})
	telegramPRDRefineAnalyzer = func(_ ralph.Paths, _ telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("could not resolve host: api.openai.com")
	}
	telegramPRDScoreAnalyzer = func(_ ralph.Paths, _ telegramPRDSession) (telegramPRDCodexScoreResponse, error) {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("could not resolve host: api.openai.com")
	}

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
		ChatID:      88,
		Stage:       telegramPRDStageAwaitProblem,
		ProductName: "Wallet",
		Context: telegramPRDContext{
			Problem: "실패율이 높다",
		},
	}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		t.Fatalf("upsert session failed: %v", err)
	}

	reply, err := telegramPRDRefineSession(paths, 88)
	if err != nil {
		t.Fatalf("refine session failed: %v", err)
	}
	if !strings.Contains(reply, "prd refine unavailable") {
		t.Fatalf("reply should indicate codex refine unavailable: %q", reply)
	}
	if strings.Contains(reply, "- ask:") {
		t.Fatalf("reply should not include heuristic fixed ask: %q", reply)
	}
	if !strings.Contains(reply, "codex_error: network") {
		t.Fatalf("reply should include codex error category: %q", reply)
	}

	updated, found, err := telegramLoadPRDSession(paths, 88)
	if err != nil {
		t.Fatalf("load updated session failed: %v", err)
	}
	if !found {
		t.Fatalf("updated session not found")
	}
	if updated.Stage != telegramPRDStageAwaitProblem {
		t.Fatalf("stage should remain unchanged when codex is unavailable: %s", updated.Stage)
	}
}

func TestClassifyTelegramCodexFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "network", err: fmt.Errorf("could not resolve host: api.openai.com"), want: "network"},
		{name: "timeout", err: fmt.Errorf("codex exec timeout: context deadline exceeded"), want: "timeout"},
		{name: "permission", err: fmt.Errorf("operation not permitted"), want: "permission"},
		{name: "not installed", err: fmt.Errorf("codex command not found"), want: "not_installed"},
		{name: "invalid response", err: fmt.Errorf("parse codex refine json: invalid character"), want: "invalid_response"},
		{name: "other", err: fmt.Errorf("exit status 1"), want: "exec_failure"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, _ := classifyTelegramCodexFailure(tt.err)
			if got != tt.want {
				t.Fatalf("classify mismatch: got=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestFormatTelegramPRDRefineUnavailableIncludesCodexReason(t *testing.T) {
	t.Parallel()

	out := formatTelegramPRDRefineUnavailable(telegramPRDStageAwaitProblem, 42, fmt.Errorf("could not resolve host: api.openai.com"))
	if !strings.Contains(out, "codex_error: network") {
		t.Fatalf("expected network codex_error in fallback output: %q", out)
	}
	if !strings.Contains(out, "codex_detail:") {
		t.Fatalf("expected codex_detail in fallback output: %q", out)
	}
	if strings.Contains(out, "- ask:") {
		t.Fatalf("fallback output should not include heuristic ask: %q", out)
	}
	if !strings.Contains(out, "next: codex 상태 복구 후") {
		t.Fatalf("fallback output should guide retry after codex recovery: %q", out)
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
			AgentPriority: map[string]int{
				"manager":   900,
				"planner":   950,
				"developer": 1000,
				"qa":        1100,
			},
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
	if !strings.Contains(string(content), "\"agent_priority\"") {
		t.Fatalf("prd file should include agent priority metadata: %s", string(content))
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
	if !strings.Contains(prompt, "Expected answer format:") {
		t.Fatalf("assist prompt should include expected answer format: %q", prompt)
	}
}
